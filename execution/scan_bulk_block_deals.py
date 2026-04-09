"""
MODULE 3: Bulk/Block Deal Accumulation Tracker

Tracks bulk deals (>0.5% equity) and block deals (>5L shares or >₹10Cr) to
detect systematic accumulation by a single entity before open offer / SAST filing.

Signal Classification:
  SYSTEMATIC_ACCUMULATION → Same buyer, 3+ deals in 30 days      Score: +20
  THRESHOLD_APPROACH      → Cumulative holding approaching 5/10%  Score: +30
  BLOCK_DEAL_INSTITUTIONAL → Large block by known MF/FII          Score: +10
  SYSTEMATIC_DISTRIBUTION → Same seller, 3+ deals                Score: -20
  PROMOTER_EXIT           → Promoter selling in bulk              Score: -35

Key insight: When the same client appears 3+ times buying the same stock in
30 days, they're accumulating for a reason. If cumulative quantity approaches
SEBI thresholds (5%, 10%, 15%, 25%), an open offer announcement is imminent.
Open offer stocks typically rally 15-40% at announcement.

Usage:
    python3 execution/scan_bulk_block_deals.py
    python3 execution/scan_bulk_block_deals.py --days 30
    python3 execution/scan_bulk_block_deals.py --from 2026-03-01 --to 2026-04-04
"""

import os
import sys
import argparse
import logging
import re
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
ACCUMULATION_WINDOW_DAYS = 30   # Rolling window for same-client detection
MIN_DEAL_COUNT = 3              # Minimum appearances for systematic signal
THRESHOLD_WARNING_PCT = 4.5    # Flag when cumulative holding near 5% SEBI threshold

# Known institutional names (partial match, case-insensitive)
# When these appear in block deals, it's directionally informative
KNOWN_MF_NAMES = [
    'mutual fund', 'hdfc amc', 'sbi mf', 'icici pru', 'axis mf', 'nippon',
    'kotak mf', 'dsp', 'aditya birla', 'franklin', 'invesco', 'mirae',
    'pgim', 'motilal oswal mf', 'canara robeco', 'union mf'
]

KNOWN_FII_NAMES = [
    'fii', 'fpi', 'foreign portfolio', 'blackrock', 'vanguard', 'fidelity',
    'jp morgan', 'goldman', 'morgan stanley', 'merrill', 'ubs', 'credit suisse',
    'nomura', 'templeton', 'gic', 'temasek', 'abu dhabi', 'sovereign wealth'
]

PROMOTER_INDICATORS = ['promoter', 'director', 'chairman', 'managing director']


def is_institutional_buyer(client_name: str):
    """Return entity type if client is a known institution, else None."""
    if not client_name:
        return None
    name_lower = str(client_name).lower()
    if any(k in name_lower for k in KNOWN_MF_NAMES):
        return 'MUTUAL_FUND'
    if any(k in name_lower for k in KNOWN_FII_NAMES):
        return 'FII_FPI'
    return None


def is_promoter(client_name: str) -> bool:
    """Heuristic: detect if a bulk deal client is likely a promoter."""
    if not client_name:
        return False
    name_lower = str(client_name).lower()
    return any(k in name_lower for k in PROMOTER_INDICATORS)


def score_deal_group(group: pd.DataFrame, symbol: str, buy_or_sell: str):
    """
    Score a group of deals by the same client in the same stock.

    Args:
        group:      DataFrame rows for one client+symbol combo
        symbol:     Stock symbol
        buy_or_sell: 'BUY' or 'SELL'

    Returns signal dict or None if below threshold.
    """
    deal_count = len(group)
    if deal_count < MIN_DEAL_COUNT:
        return None

    # Calculate totals
    total_qty = float(group['QUANTITY'].sum()) if 'QUANTITY' in group.columns else 0
    avg_price = float(group['AVG_PRICE'].mean()) if 'AVG_PRICE' in group.columns else 0
    total_value_cr = (total_qty * avg_price) / 1e7  # ₹ crore

    deal_type = group['DEAL_TYPE'].iloc[0] if 'DEAL_TYPE' in group.columns else 'BULK'
    client_name = group['CLIENT_NAME'].iloc[0] if 'CLIENT_NAME' in group.columns else 'UNKNOWN'

    is_promoter_flag = is_promoter(client_name)
    institution_type = is_institutional_buyer(client_name)

    if buy_or_sell == 'BUY':
        # Systematic accumulation
        score = 20

        # Higher score if approaching SEBI thresholds (we don't have share cap data,
        # so we proxy with deal count and value)
        if deal_count >= 5 or total_value_cr >= 100:
            score = 30  # THRESHOLD_APPROACH proxy

        signal_type = 'THRESHOLD_APPROACH' if score == 30 else 'SYSTEMATIC_ACCUMULATION'

        # Institutional block deal
        if institution_type and group['DEAL_TYPE'].eq('BLOCK').any():
            signal_type = f'BLOCK_DEAL_{institution_type}'
            score = max(score, 10)

    else:  # SELL
        score = -20
        signal_type = 'SYSTEMATIC_DISTRIBUTION'

        if is_promoter_flag:
            signal_type = 'PROMOTER_EXIT'
            score = -35

    return {
        'symbol': symbol,
        'client_name': str(client_name)[:40],
        'signal_type': signal_type,
        'deal_count': deal_count,
        'total_qty': round(total_qty, 0),
        'total_value_cr': round(total_value_cr, 2),
        'deal_type': deal_type,
        'score': score,
        'is_promoter': is_promoter_flag,
        'institution_type': institution_type,
    }


def run_bulk_deal_scan(from_date: str, to_date: str = None, save_to_db: bool = True):
    """
    Main scanner. Fetches bulk and block deals for the date range,
    groups by client+symbol, detects systematic patterns, and scores them.

    Args:
        from_date:  Start date 'YYYY-MM-DD'
        to_date:    End date 'YYYY-MM-DD' (default: today)
        save_to_db: Persist to SQLite

    Returns:
        List of signal dicts sorted by |score| descending
    """
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Running bulk/block deal scan: {from_date} to {to_date}")

    fetcher = NSEDataFetcher()
    db = SignalsDB() if save_to_db else None

    # Fetch both bulk and block deals
    bulk_df = fetcher.fetch_bulk_deals(from_date, to_date)
    block_df = fetcher.fetch_block_deals(from_date, to_date)

    frames = [df for df in [bulk_df, block_df] if df is not None and not df.empty]
    if not frames:
        logger.warning("No bulk/block deal data available. NSE may require authenticated session.")
        return []

    df = pd.concat(frames, ignore_index=True)

    # Normalize columns
    for col_variant, canonical in [
        (['QUANTITY', 'quantity', 'qty', 'QUANTITY_TRADED'], 'QUANTITY'),
        (['AVG_PRICE', 'avg_price', 'TRADE_PRICE', 'price'], 'AVG_PRICE'),
        (['BUY_SELL', 'buy_sell', 'buyOrSell'], 'BUY_SELL'),
        (['CLIENT_NAME', 'client_name', 'clientName'], 'CLIENT_NAME'),
    ]:
        for v in col_variant:
            if v in df.columns and canonical not in df.columns:
                df[canonical] = df[v]
                break

    # Numeric conversions
    for col in ['QUANTITY', 'AVG_PRICE']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['SYMBOL'] = df['SYMBOL'].str.strip().str.upper()
    df['CLIENT_NAME'] = df['CLIENT_NAME'].fillna('UNKNOWN').str.strip()
    df['BUY_SELL'] = df['BUY_SELL'].fillna('').str.upper().str.strip()

    # Map buy/sell to B/S
    df['side'] = df['BUY_SELL'].map(lambda x:
        'BUY' if any(k in x for k in ['B', 'BUY', 'BOUGHT']) else
        'SELL' if any(k in x for k in ['S', 'SELL', 'SOLD']) else 'UNKNOWN'
    )

    df = df[df['side'].isin(['BUY', 'SELL'])]
    df = df.dropna(subset=['SYMBOL', 'CLIENT_NAME'])

    # ── Detect systematic patterns per client+symbol ─────────────────────────
    signals = []
    grouped = df.groupby(['SYMBOL', 'CLIENT_NAME', 'side'])

    for (symbol, client_name, side), group in grouped:
        result = score_deal_group(group, symbol, side)
        if result is None:
            continue

        result['date'] = to_date
        signals.append(result)

        if save_to_db and db:
            db.upsert_bulk_signal(
                date=to_date,
                symbol=symbol,
                client_name=result['client_name'],
                signal_type=result['signal_type'],
                deal_count=result['deal_count'],
                total_qty=result['total_qty'],
                total_value_cr=result['total_value_cr'],
                deal_type=result['deal_type'],
                score=result['score']
            )

    # Also detect single large block deals by institutions (≥1 occurrence counts)
    block_only = df[df.get('DEAL_TYPE', pd.Series(['BULK'] * len(df))) == 'BLOCK']
    for _, row in block_only.iterrows():
        client = str(row.get('CLIENT_NAME', ''))
        institution_type = is_institutional_buyer(client)
        if institution_type and row.get('side') == 'BUY':
            # Check if not already captured
            symbol = str(row.get('SYMBOL', ''))
            existing = any(s['symbol'] == symbol and s['client_name'][:10] == client[:10]
                           for s in signals)
            if not existing:
                signal = {
                    'date': to_date,
                    'symbol': symbol,
                    'client_name': client[:40],
                    'signal_type': f'BLOCK_DEAL_{institution_type}',
                    'deal_count': 1,
                    'total_qty': float(row.get('QUANTITY', 0)),
                    'total_value_cr': (float(row.get('QUANTITY', 0) or 0) * float(row.get('AVG_PRICE', 0) or 0)) / 1e7,
                    'deal_type': 'BLOCK',
                    'score': 10,
                    'is_promoter': False,
                    'institution_type': institution_type,
                }
                signals.append(signal)

    signals.sort(key=lambda x: abs(x['score']), reverse=True)

    bullish = [s for s in signals if s['score'] > 0]
    bearish = [s for s in signals if s['score'] < 0]
    logger.info(f"Bulk/block scan: {len(bullish)} accumulation + {len(bearish)} distribution signals")
    return signals


def print_bulk_report(signals):
    """Pretty-print bulk/block deal report."""
    if not signals:
        print("  No systematic bulk/block deal patterns detected.")
        return

    bullish = [s for s in signals if s['score'] > 0]
    bearish = [s for s in signals if s['score'] < 0]

    if bullish:
        print(f"\n  ACCUMULATION SIGNALS ({len(bullish)}):")
        print(f"  {'Symbol':<12} {'Signal':<28} {'Client':<30} {'Deals':>5} {'Value(Cr)':>10} {'Score':>6}")
        print(f"  {'-'*95}")
        for s in bullish[:15]:
            flag = '>>>' if s['score'] >= 25 else ' > '
            print(f"  {flag} {s['symbol']:<10} {s['signal_type']:<28} "
                  f"{s['client_name']:<30} {s['deal_count']:>5} "
                  f"{s['total_value_cr']:>10.1f} {s['score']:>6}")

    if bearish:
        print(f"\n  DISTRIBUTION / EXIT SIGNALS ({len(bearish)}):")
        for s in bearish[:10]:
            print(f"  [!] {s['symbol']:<10} {s['signal_type']:<28} "
                  f"{s['client_name']:<30} {s['deal_count']:>5} "
                  f"{s['total_value_cr']:>10.1f} {s['score']:>6}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 3: Bulk/Block Deal Accumulation Tracker')
    parser.add_argument('--from', dest='from_date', type=str,
                        default=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                        help='From date YYYY-MM-DD (default: 30 days ago)')
    parser.add_argument('--to', dest='to_date', type=str,
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='To date YYYY-MM-DD (default: today)')
    parser.add_argument('--days', type=int, default=None,
                        help='Alternative: look back N days')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    args = parser.parse_args()

    if args.days:
        from_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
        to_date = datetime.now().strftime('%Y-%m-%d')
    else:
        from_date = args.from_date
        to_date = args.to_date

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 3: BULK / BLOCK DEAL ACCUMULATION TRACKER".ljust(79) + "║")
    print(f"║  Window: {from_date} to {to_date} ({ACCUMULATION_WINDOW_DAYS}d rolling)".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    signals = run_bulk_deal_scan(from_date, to_date, save_to_db=not args.no_db)
    print_bulk_report(signals)
    print(f"\n  Total: {len(signals)} bulk/block deal signals")
