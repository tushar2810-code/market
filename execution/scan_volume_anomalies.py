"""
MODULE 1: Volume Anomaly + Delivery Scanner

The #1 retail edge on NSE. Scans all stocks daily for:
  - Unusual volume vs 20-day rolling average
  - Cross-referenced with delivery percentage
  - Detects institutional accumulation BEFORE price moves

Signal Classification:
  STEALTH_ACCUMULATION  → Vol >3x, Delivery >50%, |Price| <1%    Score: +30
  BREAKOUT_BUYING       → Vol >3x, Delivery >50%, Price >2%       Score: +20
  DISTRIBUTION          → Vol >3x, Delivery >40%, Price <-2%      Score: -25
  OPERATOR_NOISE        → Vol >3x, Delivery <30%                  Score:   0
  SYSTEMATIC_BUILDUP    → 5 consecutive days vol rising, avg >1.5x Score: +15

Usage:
    python3 execution/scan_volume_anomalies.py
    python3 execution/scan_volume_anomalies.py --days 25 --min-score 15
    python3 execution/scan_volume_anomalies.py --date 2026-04-04
"""

import os
import sys
import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(__file__))
from nse_session import NSEDataFetcher
from signals_db import SignalsDB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─── Signal thresholds (statistically derived) ────────────────────────────────
VOL_SPIKE_THRESHOLD = 2.5     # Volume must be >2.5x 20-day average
VOL_STRONG_THRESHOLD = 3.0    # Strong signal: >3x
DELIVERY_HIGH = 50.0          # Genuine buying: delivery >50%
DELIVERY_VERY_HIGH = 60.0     # Stealth accumulation: >60%
DELIVERY_LOW = 30.0           # Speculative noise: delivery <30%
DELIVERY_MEDIUM = 40.0        # Distribution signal: >40%
PRICE_STEALTH = 1.0           # Stealth accumulation: |price| <1%
PRICE_BREAKOUT = 2.0          # Breakout: price >2%
PRICE_DISTRIBUTION = -2.0     # Distribution: price <-2%
BUILDUP_DAYS = 5              # Systematic buildup: 5 consecutive days
BUILDUP_RATIO = 1.5           # Average ratio for buildup
LOOKBACK_DAYS = 20            # Rolling average window


def score_volume_signal(vol_ratio: float, delivery_pct: float, price_change_pct: float):
    """
    Classify and score a volume anomaly based on delivery % and price action.

    Returns: (signal_type, score)

    Logic mirrors Simons: let the data define the pattern. These thresholds are
    empirically observed — institutional behavior leaves specific fingerprints:
    - Large buyers distribute across sessions → delivery high, price flat
    - Operators churn intraday → delivery low (positions squared same day)
    - Distribution → high volume, some delivery (selling into demand), price drops
    """
    if vol_ratio < VOL_SPIKE_THRESHOLD:
        return 'NO_ANOMALY', 0

    # STEALTH_ACCUMULATION: Someone large absorbing supply without moving price
    # This is the strongest signal — they're building a position covertly
    if (vol_ratio >= VOL_STRONG_THRESHOLD
            and delivery_pct >= DELIVERY_VERY_HIGH
            and abs(price_change_pct) < PRICE_STEALTH):
        return 'STEALTH_ACCUMULATION', 30

    # BREAKOUT_BUYING: Genuine institutional buying on price breakout
    if (vol_ratio >= VOL_STRONG_THRESHOLD
            and delivery_pct >= DELIVERY_HIGH
            and price_change_pct >= PRICE_BREAKOUT):
        return 'BREAKOUT_BUYING', 20

    # DISTRIBUTION: Smart money selling into rally or causing decline
    if (vol_ratio >= VOL_STRONG_THRESHOLD
            and delivery_pct >= DELIVERY_MEDIUM
            and price_change_pct <= PRICE_DISTRIBUTION):
        return 'DISTRIBUTION', -25

    # Standard volume spike + high delivery (less extreme)
    if vol_ratio >= VOL_SPIKE_THRESHOLD and delivery_pct >= DELIVERY_HIGH:
        if price_change_pct >= 1.0:
            return 'BREAKOUT_BUYING', 12
        elif abs(price_change_pct) < PRICE_STEALTH:
            return 'STEALTH_ACCUMULATION', 18

    # OPERATOR_NOISE: High volume, low delivery = intraday speculation, no signal
    if vol_ratio >= VOL_STRONG_THRESHOLD and delivery_pct < DELIVERY_LOW:
        return 'OPERATOR_NOISE', 0

    # Moderate anomaly — watchlist
    if VOL_SPIKE_THRESHOLD <= vol_ratio < VOL_STRONG_THRESHOLD and delivery_pct >= DELIVERY_HIGH:
        return 'MODERATE_ACCUMULATION', 8

    return 'VOLUME_SPIKE_UNCLASSIFIED', 5


def detect_systematic_buildup(symbol_history: pd.DataFrame):
    """
    Detect gradual, multi-session accumulation (operator avoiding detection).
    Returns: (is_buildup, avg_vol_ratio)

    Pattern: Volume increasing for 5+ consecutive days AND avg ratio >1.5x
    This is the "someone is building a position across days to avoid moving price" signal.
    """
    if len(symbol_history) < BUILDUP_DAYS + LOOKBACK_DAYS:
        return False, 0.0

    # Last 5 trading days
    recent = symbol_history.tail(BUILDUP_DAYS + LOOKBACK_DAYS)
    recent_vol = recent['TOTAL_TRADED_QTY'].tail(BUILDUP_DAYS)
    baseline = recent['TOTAL_TRADED_QTY'].iloc[:LOOKBACK_DAYS]
    avg_vol = baseline.mean()

    if avg_vol == 0:
        return False, 0.0

    # Check if volume has been increasing for 5 consecutive days
    vol_increasing = all(
        recent_vol.iloc[i] < recent_vol.iloc[i + 1]
        for i in range(len(recent_vol) - 1)
    )

    avg_ratio = recent_vol.mean() / avg_vol

    is_buildup = vol_increasing and avg_ratio >= BUILDUP_RATIO
    return is_buildup, round(avg_ratio, 2)


def run_volume_scan(date_str: str = None, lookback_days: int = 25,
                    min_score: int = 10, save_to_db: bool = True):
    """
    Main scanner. Downloads N days of bhavcopy, computes rolling averages,
    flags anomalies, and returns scored signals.

    Args:
        date_str:     Scan date (default: today/last trading day)
        lookback_days: Days of history to load (need >20 for rolling avg)
        min_score:    Only return signals with score >= this
        save_to_db:   Persist results to SQLite

    Returns:
        List of signal dicts, sorted by score descending
    """
    if date_str is None:
        # Find last trading day
        today = datetime.now()
        if today.weekday() >= 5:  # weekend
            date_str = (today - timedelta(days=today.weekday() - 4)).strftime('%Y-%m-%d')
        else:
            date_str = today.strftime('%Y-%m-%d')

    logger.info(f"Running volume anomaly scan for: {date_str} (loading {lookback_days} days)")

    fetcher = NSEDataFetcher()
    db = SignalsDB() if save_to_db else None

    # Fetch N days of bhavcopy
    df_all = fetcher.fetch_bhavcopy_range(days=lookback_days)
    if df_all is None or df_all.empty:
        logger.error("No bhavcopy data available. Check NSE_COOKIES or network.")
        return []

    # Ensure required columns
    required_cols = ['SYMBOL', 'CLOSE', 'TOTAL_TRADED_QTY', 'DELIVERY_PCT', 'TIMESTAMP']
    missing = [c for c in required_cols if c not in df_all.columns]
    if missing:
        logger.warning(f"Missing columns: {missing}. Available: {list(df_all.columns)}")
        # Try to recover
        if 'DELIVERABLE_QTY' in df_all.columns and 'TOTAL_TRADED_QTY' in df_all.columns:
            df_all['DELIVERY_PCT'] = (df_all['DELIVERABLE_QTY'] / df_all['TOTAL_TRADED_QTY'] * 100).clip(0, 100)
        for col in ['CLOSE', 'LAST_PRICE', 'CLOSE_PRICE']:
            if col in df_all.columns and 'CLOSE' not in df_all.columns:
                df_all['CLOSE'] = df_all[col]
                break

    df_all['TIMESTAMP'] = pd.to_datetime(df_all['TIMESTAMP'], errors='coerce')
    df_all = df_all.dropna(subset=['SYMBOL', 'TOTAL_TRADED_QTY', 'TIMESTAMP'])
    df_all = df_all.sort_values('TIMESTAMP')

    # Filter to EQ series only
    if 'SERIES' in df_all.columns:
        df_all = df_all[df_all['SERIES'].str.strip() == 'EQ']

    # Filter to FNO universe only — remove ETFs, index funds, bonds
    # Use the 3Y futures data directory as the canonical FNO symbol list
    fno_data_dir = Path('.tmp/3y_data')
    if fno_data_dir.exists():
        fno_symbols = {f.stem.replace('_3Y', '') for f in fno_data_dir.glob('*_5Y.csv')}
        before = len(df_all['SYMBOL'].unique())
        df_all = df_all[df_all['SYMBOL'].isin(fno_symbols)]
        after = len(df_all['SYMBOL'].unique())
        logger.info(f"FNO universe filter: {before} → {after} symbols (removed ETFs/non-FNO)")

    # Find the scan date in the data
    scan_dt = pd.Timestamp(date_str)
    available_dates = df_all['TIMESTAMP'].dt.date.unique()

    if scan_dt.date() not in available_dates:
        # Use the most recent available date
        latest_date = max(available_dates)
        logger.warning(f"{date_str} not in data, using {latest_date}")
        scan_dt = pd.Timestamp(latest_date)
        date_str = latest_date.strftime('%Y-%m-%d')

    # Today's snapshot
    today_df = df_all[df_all['TIMESTAMP'].dt.date == scan_dt.date()].copy()
    if today_df.empty:
        logger.error(f"No data for scan date {date_str}")
        return []

    # Historical window for rolling average (exclude today)
    hist_df = df_all[df_all['TIMESTAMP'].dt.date < scan_dt.date()].copy()

    # Calculate 20-day rolling avg volume per symbol
    vol_avgs = (hist_df.groupby('SYMBOL')['TOTAL_TRADED_QTY']
                .apply(lambda x: x.tail(LOOKBACK_DAYS).mean())
                .rename('avg_vol_20d'))
    vol_avgs = vol_avgs[vol_avgs > 0]

    # Merge with today's data
    today_df = today_df.merge(vol_avgs, on='SYMBOL', how='left')
    today_df = today_df.dropna(subset=['avg_vol_20d'])
    today_df['vol_ratio'] = today_df['TOTAL_TRADED_QTY'] / today_df['avg_vol_20d']

    # Calculate prev close for price change
    if 'PREV_CLOSE' in today_df.columns and today_df['PREV_CLOSE'].notna().any():
        today_df['price_change_pct'] = (
            (today_df['CLOSE'] - today_df['PREV_CLOSE']) / today_df['PREV_CLOSE'] * 100
        )
    else:
        # PREV_CLOSE missing — derive from yesterday's CLOSE in historical data
        prev_day = hist_df[hist_df['TIMESTAMP'].dt.date == hist_df['TIMESTAMP'].dt.date.max()]
        if not prev_day.empty:
            prev_close_map = prev_day.set_index('SYMBOL')['CLOSE']
            today_df['price_change_pct'] = today_df.apply(
                lambda r: ((r['CLOSE'] - prev_close_map[r['SYMBOL']]) / prev_close_map[r['SYMBOL']] * 100)
                if r['SYMBOL'] in prev_close_map.index else np.nan, axis=1
            )
            missing = today_df['price_change_pct'].isna().sum()
            if missing > 0:
                logger.warning(f"price_change_pct unavailable for {missing} symbols (no prior day data)")
            today_df = today_df.dropna(subset=['price_change_pct'])
        else:
            logger.warning("PREV_CLOSE missing and no prior day in hist_df — dropping all rows")
            return []

    # Ensure delivery_pct
    if 'DELIVERY_PCT' not in today_df.columns:
        if 'DELIVERABLE_QTY' in today_df.columns:
            today_df['DELIVERY_PCT'] = (
                today_df['DELIVERABLE_QTY'] / today_df['TOTAL_TRADED_QTY'] * 100
            ).clip(0, 100)
        else:
            today_df['DELIVERY_PCT'] = 50.0  # Default assumption

    today_df['DELIVERY_PCT'] = today_df['DELIVERY_PCT'].fillna(50.0).clip(0, 100)
    # Drop rows where price_change_pct is still NaN — don't fabricate 0% price change
    today_df = today_df.dropna(subset=['price_change_pct'])

    # ── Score each stock ──────────────────────────────────────────────────────
    signals = []
    for _, row in today_df.iterrows():
        symbol = str(row['SYMBOL']).strip()
        vol_ratio = float(row['vol_ratio'])
        delivery_pct = float(row['DELIVERY_PCT'])
        price_change_pct = float(row['price_change_pct'])
        close = float(row.get('CLOSE', 0))

        signal_type, score = score_volume_signal(vol_ratio, delivery_pct, price_change_pct)

        if signal_type == 'NO_ANOMALY':
            continue

        # Check for systematic buildup (multi-day pattern)
        if signal_type not in ('DISTRIBUTION', 'OPERATOR_NOISE'):
            sym_hist = hist_df[hist_df['SYMBOL'] == symbol].sort_values('TIMESTAMP')
            is_buildup, buildup_ratio = detect_systematic_buildup(sym_hist)
            if is_buildup:
                score = max(score, 15)
                signal_type = f"SYSTEMATIC_BUILDUP+{signal_type}"

        if score < min_score and score > -20:
            continue

        signal = {
            'date': date_str,
            'symbol': symbol,
            'signal_type': signal_type,
            'vol_ratio': round(vol_ratio, 2),
            'delivery_pct': round(delivery_pct, 1),
            'price_change_pct': round(price_change_pct, 2),
            'close_price': round(close, 2),
            'score': score,
        }
        signals.append(signal)

        if save_to_db and db:
            db.upsert_volume_signal(
                date=date_str, symbol=symbol, signal_type=signal_type,
                vol_ratio=vol_ratio, delivery_pct=delivery_pct,
                price_change_pct=price_change_pct, close_price=close, score=score
            )

    # Sort: highest score first, then most negative (bearish alerts)
    signals.sort(key=lambda x: abs(x['score']), reverse=True)
    positive = [s for s in signals if s['score'] > 0]
    negative = [s for s in signals if s['score'] < 0]

    logger.info(f"Volume scan complete: {len(positive)} bullish + {len(negative)} bearish signals")
    return signals


def print_volume_report(signals):
    """Pretty-print volume anomaly signals."""
    if not signals:
        print("  No volume anomaly signals above threshold.")
        return

    bullish = [s for s in signals if s['score'] > 0]
    bearish = [s for s in signals if s['score'] < 0]

    if bullish:
        print(f"\n  BULLISH VOLUME SIGNALS ({len(bullish)}):")
        print(f"  {'Symbol':<15} {'Signal':<28} {'Vol Ratio':>9} {'Delivery':>9} {'Price Chg':>10} {'Score':>6}")
        print(f"  {'-'*80}")
        for s in bullish[:15]:
            flag = '>>>' if s['score'] >= 20 else ' > '
            print(f"  {flag} {s['symbol']:<13} {s['signal_type']:<28} "
                  f"{s['vol_ratio']:>8.1f}x {s['delivery_pct']:>8.1f}% "
                  f"{s['price_change_pct']:>+9.2f}% {s['score']:>6}")

    if bearish:
        print(f"\n  BEARISH VOLUME SIGNALS ({len(bearish)}):")
        print(f"  {'Symbol':<15} {'Signal':<28} {'Vol Ratio':>9} {'Delivery':>9} {'Price Chg':>10} {'Score':>6}")
        print(f"  {'-'*80}")
        for s in bearish[:10]:
            print(f"  [!] {s['symbol']:<13} {s['signal_type']:<28} "
                  f"{s['vol_ratio']:>8.1f}x {s['delivery_pct']:>8.1f}% "
                  f"{s['price_change_pct']:>+9.2f}% {s['score']:>6}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 1: Volume Anomaly + Delivery Scanner')
    parser.add_argument('--date', type=str, help='Scan date YYYY-MM-DD (default: today)')
    parser.add_argument('--days', type=int, default=25, help='Lookback days for rolling avg (min 22)')
    parser.add_argument('--min-score', type=int, default=10, help='Minimum score to show')
    parser.add_argument('--no-db', action='store_true', help='Skip database storage')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 1: VOLUME ANOMALY + DELIVERY SCANNER".ljust(79) + "║")
    print("║  The #1 NSE retail edge — detecting institutional footprints".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    signals = run_volume_scan(
        date_str=args.date,
        lookback_days=max(args.days, 22),
        min_score=args.min_score,
        save_to_db=not args.no_db
    )

    print_volume_report(signals)

    if signals:
        top = [s for s in signals if s['score'] >= 20]
        print(f"\n  Total: {len(signals)} signals | High conviction (score>=20): {len(top)}")
