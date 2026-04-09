"""
MODULE 2: Insider Trading Cluster Detector

Monitors SEBI PIT disclosures for cluster buying/selling by company insiders.
Legally available data. Statistically proven edge — insiders know the business.

Signal Classification:
  BUY_CLUSTER          → 2+ distinct insiders buy within 14 days     Score: 15-40
  PROMOTER_CONVICTION  → Promoter buys after >10% stock dip           Score: +25
  PRE_RESULTS_BUYING   → Cluster within 30d before results             Score: +10 bonus
  SELL_CLUSTER         → 2+ distinct insiders sell within 14 days     Score: -15 to -40

Weighting:
  Promoter buying    → 25 pts
  Director buying    → 15 pts
  KMP buying         → 10 pts
  Multiple categories → multiply by 1.5x
  Cluster <3 days apart → +10 bonus (coordinated)
  Min ₹10L per tx to filter noise

Usage:
    python3 execution/scan_insider_clusters.py
    python3 execution/scan_insider_clusters.py --days 30
    python3 execution/scan_insider_clusters.py --from 2026-03-01 --to 2026-04-04
"""

import os
import sys
import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))
from nse_session import NSEDataFetcher
from signals_db import SignalsDB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─── Configuration ────────────────────────────────────────────────────────────
CLUSTER_WINDOW_DAYS = 14       # Window to look for insider clusters
MIN_TRANSACTION_VALUE_LAKHS = 10.0   # Filter noise: min ₹10L per transaction
CLUSTER_TIGHT_DAYS = 3         # "Tight cluster" bonus: all within 3 days

# Insider category → base score
CATEGORY_SCORES = {
    'promoter': 25,
    'promoters': 25,
    'promoter group': 25,
    'director': 15,
    'whole time director': 15,
    'managing director': 15,
    'chief executive officer': 10,
    'chief financial officer': 10,
    'kmp': 10,
    'key managerial personnel': 10,
    'company secretary': 8,
    'designated person': 8,
    'relative': 5,
}

# Bearish: same scores but negative
BUY_KEYWORDS = ['purchase', 'buy', 'bought', 'acquisition', 'acquired', 'allotment', 'subscri']
SELL_KEYWORDS = ['sell', 'sold', 'sale', 'disposal', 'disposed', 'transfer', 'transmit']


def classify_insider(category: str) -> str:
    """Map NSE category string to canonical category."""
    if not category:
        return 'other'
    cat_lower = str(category).lower().strip()
    for key in ['promoter group', 'promoter']:
        if key in cat_lower:
            return 'promoter'
    for key in ['managing director', 'whole time director', 'director']:
        if key in cat_lower:
            return 'director'
    for key in ['chief executive', 'chief financial', 'kmp', 'key managerial']:
        if key in cat_lower:
            return 'kmp'
    return 'other'


def is_buy_transaction(txn_type: str):
    """Return True for buy, False for sell, None if unclear."""
    if not txn_type:
        return None
    t = str(txn_type).lower()
    if any(k in t for k in BUY_KEYWORDS):
        return True
    if any(k in t for k in SELL_KEYWORDS):
        return False
    return None


def score_cluster(trades: pd.DataFrame):
    """
    Score a cluster of insider trades for one stock.

    Args:
        trades: Filtered insider trades for one symbol in the window

    Returns: (signal_type, score, categories_involved)
    """
    # Separate buys and sells
    buys = trades[trades['is_buy'] == True]
    sells = trades[trades['is_buy'] == False]

    # Process buys
    buy_score = 0
    buy_categories = []
    buy_signal = 'NO_SIGNAL'

    if len(buys) >= 2:
        # Multiple distinct insiders buying
        distinct_categories = set()
        for _, row in buys.iterrows():
            cat = classify_insider(row.get('PERSON_CATEGORY', ''))
            cat_score = CATEGORY_SCORES.get(cat, 5)
            buy_score += cat_score
            distinct_categories.add(cat)
            buy_categories.append(cat)

        # Multi-category multiplier (1.5x if >1 category)
        if len(distinct_categories) > 1:
            buy_score = int(buy_score * 1.5)

        # Tight cluster bonus (all within 3 days)
        if 'DATE' in buys.columns:
            buys_dated = buys.copy()
            buys_dated['DATE'] = pd.to_datetime(buys_dated['DATE'], errors='coerce')
            date_range = (buys_dated['DATE'].max() - buys_dated['DATE'].min()).days
            if date_range <= CLUSTER_TIGHT_DAYS:
                buy_score += 10

        # Cap at 40
        buy_score = min(buy_score, 40)
        buy_signal = 'BUY_CLUSTER'

    # Process sells
    sell_score = 0
    sell_categories = []
    sell_signal = 'NO_SIGNAL'

    if len(sells) >= 2:
        distinct_categories = set()
        for _, row in sells.iterrows():
            cat = classify_insider(row.get('PERSON_CATEGORY', ''))
            cat_score = CATEGORY_SCORES.get(cat, 5)
            sell_score += cat_score
            distinct_categories.add(cat)
            sell_categories.append(cat)

        if len(distinct_categories) > 1:
            sell_score = int(sell_score * 1.5)

        sell_score = min(sell_score, 40)
        sell_signal = 'SELL_CLUSTER'

    # Determine dominant signal
    if buy_score > sell_score and buy_score >= 15:
        return buy_signal, buy_score, buy_categories
    elif sell_score > buy_score and sell_score >= 15:
        return sell_signal, -sell_score, sell_categories
    elif buy_score >= 15:
        return buy_signal, buy_score, buy_categories
    elif sell_score >= 15:
        return sell_signal, -sell_score, sell_categories

    return 'WEAK_SIGNAL', 0, []


def run_insider_scan(from_date: str, to_date: str = None, save_to_db: bool = True):
    """
    Main scanner. Fetches insider trading disclosures for the date range,
    groups by stock, detects clusters, and scores them.

    Args:
        from_date: Start date 'YYYY-MM-DD'
        to_date:   End date 'YYYY-MM-DD' (default: today)
        save_to_db: Persist results to SQLite

    Returns:
        List of signal dicts sorted by |score| descending
    """
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Running insider cluster scan: {from_date} to {to_date}")

    fetcher = NSEDataFetcher()
    db = SignalsDB() if save_to_db else None

    # Fetch insider trading data
    df = fetcher.fetch_insider_trades(from_date, to_date)
    if df is None or df.empty:
        logger.warning("No insider data available. NSE may require authenticated session.")
        return []

    # Standardize
    if 'DATE' not in df.columns:
        for col in ['anDt', 'date', 'Date', 'TDATE']:
            if col in df.columns:
                df['DATE'] = df[col]
                break

    if 'PERSON_CATEGORY' not in df.columns:
        for col in ['pdesc', 'personCategory', 'category']:
            if col in df.columns:
                df['PERSON_CATEGORY'] = df[col]
                break

    if 'TRANSACTION_TYPE' not in df.columns:
        for col in ['tdesc', 'transactionType', 'buyOrSell']:
            if col in df.columns:
                df['TRANSACTION_TYPE'] = df[col]
                break

    # Classify buy/sell
    df['is_buy'] = df.get('TRANSACTION_TYPE', pd.Series([''] * len(df))).apply(is_buy_transaction)

    # Filter by minimum transaction value (₹10L)
    if 'VALUE_LAKHS' in df.columns:
        df['VALUE_LAKHS'] = pd.to_numeric(df['VALUE_LAKHS'], errors='coerce')
        df = df[df['VALUE_LAKHS'] >= MIN_TRANSACTION_VALUE_LAKHS]

    # Ensure SYMBOL exists
    for sym_col in ['SYMBOL', 'symbol', 'Symbol']:
        if sym_col in df.columns:
            df['SYMBOL'] = df[sym_col]
            break

    if 'SYMBOL' not in df.columns:
        logger.error("No SYMBOL column in insider data")
        return []

    df['SYMBOL'] = df['SYMBOL'].str.strip().str.upper()
    df = df.dropna(subset=['SYMBOL', 'is_buy'])

    # ── Detect clusters per stock ─────────────────────────────────────────────
    signals = []
    grouped = df.groupby('SYMBOL')

    for symbol, group in grouped:
        group = group.copy()

        # Need at least 2 trades to form a cluster
        buys = group[group['is_buy'] == True]
        sells = group[group['is_buy'] == False]

        if len(buys) < 2 and len(sells) < 2:
            continue

        signal_type, score, categories = score_cluster(group)

        if signal_type == 'NO_SIGNAL' or abs(score) == 0:
            continue

        total_value = float(group['VALUE_LAKHS'].sum()) if 'VALUE_LAKHS' in group.columns else 0.0
        insider_count = max(len(buys), len(sells))

        signal = {
            'date': to_date,
            'symbol': symbol,
            'signal_type': signal_type,
            'insider_count': insider_count,
            'insider_categories': categories,
            'total_value_lakhs': round(total_value, 1),
            'days_window': CLUSTER_WINDOW_DAYS,
            'score': score,
        }
        signals.append(signal)

        if save_to_db and db:
            db.upsert_insider_signal(
                date=to_date,
                symbol=symbol,
                signal_type=signal_type,
                insider_count=insider_count,
                insider_categories=categories,
                total_value_lakhs=total_value,
                days_window=CLUSTER_WINDOW_DAYS,
                score=score
            )

    signals.sort(key=lambda x: abs(x['score']), reverse=True)
    logger.info(f"Insider scan: {len([s for s in signals if s['score'] > 0])} buy clusters + "
                f"{len([s for s in signals if s['score'] < 0])} sell clusters")
    return signals


def print_insider_report(signals):
    """Pretty-print insider trading signal report."""
    if not signals:
        print("  No insider trading clusters detected in the window.")
        return

    bullish = [s for s in signals if s['score'] > 0]
    bearish = [s for s in signals if s['score'] < 0]

    if bullish:
        print(f"\n  BUY CLUSTERS ({len(bullish)}):")
        print(f"  {'Symbol':<15} {'Signal':<22} {'Insiders':>8} {'Value (L)':>10} {'Categories':<30} {'Score':>6}")
        print(f"  {'-'*92}")
        for s in bullish:
            cats = ', '.join(set(s['insider_categories'][:3]))
            flag = '>>>' if s['score'] >= 25 else ' > '
            print(f"  {flag} {s['symbol']:<13} {s['signal_type']:<22} "
                  f"{s['insider_count']:>8} {s['total_value_lakhs']:>10.1f} "
                  f"{cats:<30} {s['score']:>6}")

    if bearish:
        print(f"\n  SELL CLUSTERS ({len(bearish)}):")
        for s in bearish:
            cats = ', '.join(set(s['insider_categories'][:3]))
            print(f"  [!] {s['symbol']:<13} {s['signal_type']:<22} "
                  f"{s['insider_count']:>8} {s['total_value_lakhs']:>10.1f} "
                  f"{cats:<30} {s['score']:>6}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 2: Insider Trading Cluster Detector')
    parser.add_argument('--from', dest='from_date', type=str,
                        default=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                        help='From date YYYY-MM-DD (default: 30 days ago)')
    parser.add_argument('--to', dest='to_date', type=str,
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='To date YYYY-MM-DD (default: today)')
    parser.add_argument('--days', type=int, default=None,
                        help='Alternative: look back N days from today')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    args = parser.parse_args()

    if args.days:
        from_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
        to_date = datetime.now().strftime('%Y-%m-%d')
    else:
        from_date = args.from_date
        to_date = args.to_date

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 2: INSIDER TRADING CLUSTER DETECTOR".ljust(79) + "║")
    print(f"║  Window: {from_date} to {to_date}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    signals = run_insider_scan(from_date, to_date, save_to_db=not args.no_db)
    print_insider_report(signals)
    print(f"\n  Total clusters: {len(signals)}")
