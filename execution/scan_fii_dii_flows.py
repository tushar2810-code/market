"""
MODULE 5: FII/DII Flow Momentum Score

India-specific edge: FII/DII data is published by NSE daily by ~7 PM IST.
Most retail traders misread FII activity — this module reads it correctly.

Key hidden pattern:
  FII SELLING CASH + BUYING FUTURES = HEDGING, NOT EXITING (bullish signal)
  This is the most misread signal in Indian markets.

Composite FII Score:
  fii_score = (FII_cash_net × 0.4) + (FII_index_futures_net × 0.35) + (FII_options_net × 0.25)
  5-day rolling sum of fii_score determines regime

Signal Classification:
  FII_REGIME_BULLISH  → 5-day flow flips positive          Score: +15
  FII_REGIME_BEARISH  → 5-day flow flips negative          Score: -15
  FII_HEDGE_SIGNAL    → Cash selling + Futures buying      Score: +10 (contrarian bullish)
  FII_CAPITULATION    → Heavy selling both cash + futures  Score: -25

Usage:
    python3 execution/scan_fii_dii_flows.py
    python3 execution/scan_fii_dii_flows.py --history 10
"""

import os
import sys
import argparse
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(__file__))
from nse_session import NSEDataFetcher
from signals_db import SignalsDB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
# Weights for composite FII score (must sum to 1.0)
FII_CASH_WEIGHT = 0.40
FII_FUTURES_WEIGHT = 0.35
FII_OPTIONS_WEIGHT = 0.25

ROLLING_WINDOW = 5          # 5-day rolling sum for regime detection
CAPITULATION_THRESHOLD = 5000   # ₹ Cr — both cash+futures selling beyond this = capitulation
HEDGE_THRESHOLD = 1000          # ₹ Cr — futures buying above this while cash selling


def calculate_composite_fii_score(cash_net: float, fut_net: float, opt_net: float) -> float:
    """
    Weighted composite FII score for a single day.

    Rationale for weights:
    - Cash (0.4): Actual stock buying/selling — most directional
    - Futures (0.35): Forward commitment — directional but leveraged
    - Options (0.25): Least clear (can be hedging) — lowest weight
    """
    return (cash_net * FII_CASH_WEIGHT) + (fut_net * FII_FUTURES_WEIGHT) + (opt_net * FII_OPTIONS_WEIGHT)


def classify_fii_signal(cash_net: float, fut_net: float,
                          rolling_5d: float, prev_rolling_5d: float):
    """
    Classify FII flow pattern and return (signal_type, score).

    The hedge pattern (cash sell + futures buy) is the key contrarian edge.
    Most retail traders see FII cash selling and panic. But if futures are
    being bought simultaneously, FIIs are simply hedging their portfolio
    (protecting gains), not actually exiting. This is subtly bullish.
    """
    # CAPITULATION: Heavy selling in BOTH cash AND futures
    if cash_net < -CAPITULATION_THRESHOLD and fut_net < -CAPITULATION_THRESHOLD / 2:
        return 'FII_CAPITULATION', -25

    # HEDGE SIGNAL: Cash selling + Futures buying (the key hidden pattern)
    if cash_net < -HEDGE_THRESHOLD and fut_net > HEDGE_THRESHOLD:
        return 'FII_HEDGE_SIGNAL', 10

    # REGIME FLIP: 5-day rolling score crosses from negative to positive
    if rolling_5d > 0 and prev_rolling_5d <= 0:
        return 'FII_REGIME_BULLISH', 15

    # REGIME FLIP: 5-day rolling score crosses from positive to negative
    if rolling_5d < 0 and prev_rolling_5d >= 0:
        return 'FII_REGIME_BEARISH', -15

    # Ongoing regime (no flip, just reporting current state)
    if rolling_5d > 0:
        return 'FII_REGIME_BULLISH', 10
    elif rolling_5d < 0:
        return 'FII_REGIME_BEARISH', -10

    return 'FII_NEUTRAL', 0


def run_fii_scan(save_to_db: bool = True) -> dict:
    """
    Fetch FII/DII data, compute composite scores and regime, save to DB.

    Returns:
        Dict with: date, regime, signal_type, score, fii_cash_net, fii_fut_net,
                   rolling_5d_score, dii_cash_net, history
    """
    today = datetime.now().strftime('%Y-%m-%d')

    fetcher = NSEDataFetcher()
    db = SignalsDB() if save_to_db else None

    df = fetcher.fetch_fii_dii_flows()

    if df is None or df.empty:
        logger.warning("No FII/DII data fetched. Falling back to database history.")
        if db:
            history = db.get_fii_history(days=10)
            regime_row = db.get_fii_regime()
            if regime_row:
                return {
                    'date': regime_row['date'],
                    'regime': regime_row['regime'],
                    'signal_type': regime_row['signal_type'],
                    'score': regime_row['score'],
                    'fii_cash_net': regime_row['fii_cash_net'],
                    'fii_fut_net': regime_row.get('fii_fut_net', 0),
                    'rolling_5d_score': regime_row['rolling_5d_score'],
                    'dii_cash_net': regime_row.get('dii_cash_net', 0),
                    'history': history,
                    'source': 'DB_CACHE',
                }
        return {'regime': 'UNKNOWN', 'score': 0, 'signal_type': 'NO_DATA', 'history': []}

    # Standardize columns
    col_map_attempts = [
        # NSE format 1: separate category rows
        ('FII_CASH_NET', 'FII_CASH_NET'),
        ('fiiBuySell', 'FII_CASH_NET'),
        ('fii_cash', 'FII_CASH_NET'),
    ]

    for src, dst in col_map_attempts:
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

    # If data is in wide format (separate row per category), process accordingly
    # NSE's typical format has these columns: date, category (FII/DII), buy, sell, net
    if 'FII_CASH_NET' not in df.columns and len(df) > 0:
        # Try to extract from row-based format
        df = _extract_from_row_format(df)

    if df is None or 'FII_CASH_NET' not in df.columns:
        logger.warning("Could not parse FII/DII data structure")
        return {'regime': 'UNKNOWN', 'score': 0, 'signal_type': 'NO_DATA', 'history': []}

    # Fill missing futures/options with 0
    df['FII_FUT_NET'] = pd.to_numeric(df.get('FII_FUT_NET', 0), errors='coerce').fillna(0)
    df['FII_OPT_NET'] = pd.to_numeric(df.get('FII_OPT_NET', 0), errors='coerce').fillna(0)
    df['DII_CASH_NET'] = pd.to_numeric(df.get('DII_CASH_NET', 0), errors='coerce').fillna(0)

    # Compute composite score for each day
    df['composite_score'] = df.apply(
        lambda r: calculate_composite_fii_score(
            float(r.get('FII_CASH_NET', 0) or 0),
            float(r.get('FII_FUT_NET', 0) or 0),
            float(r.get('FII_OPT_NET', 0) or 0)
        ), axis=1
    )

    # Sort chronologically if DATE exists
    if 'DATE' in df.columns:
        df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        df = df.sort_values('DATE').dropna(subset=['DATE'])

    # 5-day rolling sum
    df['rolling_5d'] = df['composite_score'].rolling(ROLLING_WINDOW, min_periods=1).sum()

    # Get latest and previous
    if len(df) < 1:
        return {'regime': 'UNKNOWN', 'score': 0, 'signal_type': 'NO_DATA', 'history': []}

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else latest

    cash_net = float(latest.get('FII_CASH_NET', 0) or 0)
    fut_net = float(latest.get('FII_FUT_NET', 0) or 0)
    opt_net = float(latest.get('FII_OPT_NET', 0) or 0)
    dii_net = float(latest.get('DII_CASH_NET', 0) or 0)
    rolling_5d = float(latest.get('rolling_5d', 0) or 0)
    prev_rolling_5d = float(prev.get('rolling_5d', 0) or 0)
    composite = float(latest.get('composite_score', 0) or 0)

    signal_type, score = classify_fii_signal(cash_net, fut_net, rolling_5d, prev_rolling_5d)

    # Regime determination
    regime = ('BULLISH' if rolling_5d > 0 else
              'BEARISH' if rolling_5d < 0 else 'NEUTRAL')

    date_str = latest['DATE'].strftime('%Y-%m-%d') if 'DATE' in df.columns and not pd.isna(latest.get('DATE')) else today

    # Save to DB
    if save_to_db and db:
        db.upsert_fii_signal(
            date=date_str,
            fii_cash_net=cash_net,
            fii_fut_net=fut_net,
            fii_opt_net=opt_net,
            dii_cash_net=dii_net,
            composite_score=composite,
            rolling_5d_score=rolling_5d,
            regime=regime,
            signal_type=signal_type,
            score=score
        )

    # Build history for report
    history = []
    for _, row in df.tail(10).iterrows():
        history.append({
            'date': row['DATE'].strftime('%Y-%m-%d') if 'DATE' in df.columns and not pd.isna(row.get('DATE')) else '',
            'fii_cash_net': round(float(row.get('FII_CASH_NET', 0) or 0), 0),
            'fii_fut_net': round(float(row.get('FII_FUT_NET', 0) or 0), 0),
            'dii_cash_net': round(float(row.get('DII_CASH_NET', 0) or 0), 0),
            'composite_score': round(float(row.get('composite_score', 0) or 0), 0),
            'rolling_5d': round(float(row.get('rolling_5d', 0) or 0), 0),
        })

    logger.info(f"FII regime: {regime} | Signal: {signal_type} | Score: {score} | "
                f"Cash: {cash_net:.0f}Cr | Fut: {fut_net:.0f}Cr | 5d Rolling: {rolling_5d:.0f}")

    return {
        'date': date_str,
        'regime': regime,
        'signal_type': signal_type,
        'score': score,
        'fii_cash_net': round(cash_net, 0),
        'fii_fut_net': round(fut_net, 0),
        'fii_opt_net': round(opt_net, 0),
        'dii_cash_net': round(dii_net, 0),
        'composite_score_today': round(composite, 0),
        'rolling_5d_score': round(rolling_5d, 0),
        'history': history,
        'source': 'LIVE',
    }


def _extract_from_row_format(df: pd.DataFrame):
    """
    Handle NSE's row-based FII/DII format where each category is a separate row.
    Pivots into wide format with one row per date.
    """
    # Look for category column
    cat_col = None
    for c in ['category', 'Category', 'TYPE', 'type']:
        if c in df.columns:
            cat_col = c
            break

    if cat_col is None:
        return df  # Assume already wide format, return as-is

    # Look for net column
    net_col = None
    for c in ['netVal', 'NET', 'net', 'NetValue', 'NET_VALUE']:
        if c in df.columns:
            net_col = c
            break

    if net_col is None:
        return df

    # Pivot
    date_col = 'DATE' if 'DATE' in df.columns else None
    if date_col is None:
        for c in ['date', 'Date', 'TDATE']:
            if c in df.columns:
                date_col = c
                df['DATE'] = df[c]
                break

    if date_col is None:
        return df

    result_rows = []
    for date, group in df.groupby('DATE'):
        row = {'DATE': date}
        for _, r in group.iterrows():
            cat = str(r.get(cat_col, '')).strip().upper()
            val = pd.to_numeric(r.get(net_col, 0), errors='coerce')
            if 'FII' in cat or 'FPI' in cat:
                row['FII_CASH_NET'] = val
            elif 'DII' in cat:
                row['DII_CASH_NET'] = val
        result_rows.append(row)

    if result_rows:
        return pd.DataFrame(result_rows)
    return df


def print_fii_report(result: dict):
    """Pretty-print FII/DII regime report."""
    regime = result.get('regime', 'UNKNOWN')
    signal = result.get('signal_type', 'UNKNOWN')
    score = result.get('score', 0)

    regime_icon = '▲' if regime == 'BULLISH' else ('▼' if regime == 'BEARISH' else '─')

    print(f"\n  FII REGIME: {regime_icon} {regime}  |  Signal: {signal}  |  Score: {score:+d}")
    print(f"\n  Today's Flows (₹ Crore):")
    print(f"    FII Cash:    {result.get('fii_cash_net', 0):>+10,.0f}")
    print(f"    FII Futures: {result.get('fii_fut_net', 0):>+10,.0f}")
    print(f"    FII Options: {result.get('fii_opt_net', 0):>+10,.0f}")
    print(f"    DII Cash:    {result.get('dii_cash_net', 0):>+10,.0f}")
    print(f"    5-Day Rolling Score: {result.get('rolling_5d_score', 0):>+8,.0f}")

    if result.get('signal_type') == 'FII_HEDGE_SIGNAL':
        print(f"\n  *** HEDGE SIGNAL: FII selling cash but buying futures.")
        print(f"  *** This is HEDGING, not exiting. Contrarian BULLISH. Most retail will misread this.")

    if result.get('history'):
        print(f"\n  10-Day FII Flow History:")
        print(f"  {'Date':<12} {'Cash(Cr)':>10} {'Fut(Cr)':>10} {'DII(Cr)':>10} {'Composite':>10} {'5D Roll':>10}")
        print(f"  {'-'*62}")
        for h in result['history'][-10:]:
            print(f"  {h['date']:<12} {h['fii_cash_net']:>+10,.0f} {h['fii_fut_net']:>+10,.0f} "
                  f"{h['dii_cash_net']:>+10,.0f} {h['composite_score']:>+10,.0f} "
                  f"{h['rolling_5d']:>+10,.0f}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 5: FII/DII Flow Momentum Score')
    parser.add_argument('--history', type=int, default=10, help='Days of history to show')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 5: FII/DII FLOW MOMENTUM SCORE".ljust(79) + "║")
    print("║  India-specific edge — reading institutional flows correctly".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    result = run_fii_scan(save_to_db=not args.no_db)
    print_fii_report(result)
