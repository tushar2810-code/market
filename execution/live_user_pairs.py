"""
Live Z-score monitor for a specific pair.

Usage:
    python3 execution/live_user_pairs.py --symA ICICIBANK --symB HDFCBANK
    python3 execution/live_user_pairs.py --symA AXISBANK --symB BANKBARODA --window 30

Uses cash-neutral spread (lot_a × price_a − lot_b × price_b) — same as scan_proven_pairs.py.
Applies data freshness check (≤3 trading days stale).
"""

import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime
from shoonya_client import ShoonyaClient


DATA_DIR = '.tmp/3y_data'
MAX_STALENESS_TRADING_DAYS = 3


def load_continuous(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        print(f"ERROR: No data file for {symbol}")
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

    last_date = df['FH_TIMESTAMP'].max()
    stale = int(np.busday_count(last_date.date(), datetime.now().date()))
    if stale > MAX_STALENESS_TRADING_DAYS:
        print(f"WARNING: {symbol} data is {stale} trading days stale (last: {last_date.date()}) — results may be wrong")

    df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT'].idxmin()].copy()
    continuous = continuous.sort_values('FH_TIMESTAMP')
    # Replace 0/NaN lot sizes with nearest valid
    continuous['FH_MARKET_LOT'] = continuous['FH_MARKET_LOT'].replace(0, np.nan).ffill().bfill()
    return continuous[['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_MARKET_LOT']].set_index('FH_TIMESTAMP')


def get_live_price_and_lot(api, symbol):
    ret = api.searchscrip(exchange='NFO', searchtext=symbol)
    if not ret or 'values' not in ret:
        return None, None
    futs = [s for s in ret['values'] if 'FUT' in s.get('dname', '') and s.get('dname', '').split()[0] == symbol]
    if not futs:
        print(f"WARNING: No exact FUT match for {symbol}")
        futs = [s for s in ret['values'] if 'FUT' in s.get('dname', '')]
    if not futs:
        return None, None
    futs.sort(key=lambda x: pd.to_datetime(x.get('dname', '').split()[-1], format='%d%b%Y', errors='coerce'))
    selected = futs[0]
    q = api.get_quotes(exchange='NFO', token=selected['token'])
    if q and 'lp' in q:
        return float(q['lp']), int(selected.get('ls', 1))
    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symA', required=True)
    parser.add_argument('--symB', required=True)
    parser.add_argument('--window', type=int, default=30, help='Lookback window in trading days')
    args = parser.parse_args()

    sym_a, sym_b = args.symA.upper(), args.symB.upper()

    hist_a = load_continuous(sym_a)
    hist_b = load_continuous(sym_b)
    if hist_a is None or hist_b is None:
        return

    merged = hist_a.join(hist_b, how='inner', lsuffix='_A', rsuffix='_B')
    for col in ['FH_MARKET_LOT_A', 'FH_MARKET_LOT_B']:
        merged[col] = merged[col].replace(0, np.nan).ffill().bfill()

    # Cash-neutral spread per row using the lot size in effect on each date
    merged['SPREAD'] = (merged['FH_CLOSING_PRICE_A'] * merged['FH_MARKET_LOT_A']
                        - merged['FH_CLOSING_PRICE_B'] * merged['FH_MARKET_LOT_B'])
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']

    recent = merged.tail(args.window)
    mean_spread = recent['SPREAD'].mean()
    std_spread = recent['SPREAD'].std()

    client = ShoonyaClient()
    api = client.login()
    if not api:
        print("Login failed")
        return

    live_a, lot_a = get_live_price_and_lot(api, sym_a)
    live_b, lot_b = get_live_price_and_lot(api, sym_b)

    if live_a is None or live_b is None:
        print("Could not fetch live prices")
        return

    live_spread = live_a * lot_a - live_b * lot_b
    live_ratio = live_a / live_b
    z = (live_spread - mean_spread) / std_spread if std_spread > 0 else 0.0

    status = "NEUTRAL"
    if z >= 2.0:
        status = f"SIGNAL: SELL {sym_a} / BUY {sym_b}"
    elif z <= -2.0:
        status = f"SIGNAL: BUY {sym_a} / SELL {sym_b}"
    elif z > 1.5:
        status = f"WATCH: approaching SHORT {sym_a}"
    elif z < -1.5:
        status = f"WATCH: approaching LONG {sym_a}"

    print(f"\n{'='*55}")
    print(f"  PAIR: {sym_a} / {sym_b}  |  Window: {args.window}d")
    print(f"{'='*55}")
    print(f"  Live {sym_a}: ₹{live_a:.2f}  (lot {lot_a})")
    print(f"  Live {sym_b}: ₹{live_b:.2f}  (lot {lot_b})")
    print(f"  Live Spread (cash-neutral): ₹{live_spread:,.0f}")
    print(f"  Hist Spread mean: ₹{mean_spread:,.0f}  std: ₹{std_spread:,.0f}")
    print(f"  Live Ratio: {live_ratio:.4f}")
    print(f"  Z-Score ({args.window}d): {z:+.2f}")
    print(f"  STATUS: {status}")
    if abs(z) >= 2.0:
        print(f"  Stop loss: |Z| > 3.5 → close both legs")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
