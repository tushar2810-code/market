"""
DATA VERIFICATION + EXACT P&L CALCULATION
==========================================
1. Cross-checks historical CSV closing prices vs live API prev_close
2. Calculates exact P&L for both positions with clear math
3. Checks data freshness for all relevant scrips
"""

import pandas as pd
import numpy as np
import os
import sys
import time
import warnings
import logging

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.ERROR)

sys.path.insert(0, os.path.dirname(__file__))
from shoonya_client import ShoonyaClient

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '.tmp', '3y_data')


def load_continuous(symbol):
    """Load historical data and return continuous series."""
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None, f"FILE NOT FOUND: {path}"

    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    # Parse dates
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    n_total = len(df)
    n_parsed = df['FH_TIMESTAMP'].notna().sum()
    n_failed = n_total - n_parsed

    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

    # Continuous front-month
    df['FH_EXPIRY_DT_parsed'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT_parsed'].idxmin()].copy()
    continuous = continuous.sort_values('FH_TIMESTAMP')

    info = {
        'total_rows': n_total,
        'parsed_ok': n_parsed,
        'parse_failed': n_failed,
        'continuous_rows': len(continuous),
        'date_range': f"{continuous['FH_TIMESTAMP'].min().strftime('%d-%b-%Y')} to {continuous['FH_TIMESTAMP'].max().strftime('%d-%b-%Y')}",
        'last_close': float(continuous.iloc[-1]['FH_CLOSING_PRICE']),
        'last_date': continuous.iloc[-1]['FH_TIMESTAMP'].strftime('%d-%b-%Y'),
        'second_last_close': float(continuous.iloc[-2]['FH_CLOSING_PRICE']) if len(continuous) > 1 else None,
        'second_last_date': continuous.iloc[-2]['FH_TIMESTAMP'].strftime('%d-%b-%Y') if len(continuous) > 1 else None,
    }

    return continuous, info


def get_live_price(api, symbol):
    """Fetch live futures price for a symbol."""
    ret = api.searchscrip(exchange='NFO', searchtext=symbol)
    if ret and 'values' in ret:
        futures = [x for x in ret['values']
                   if x.get('instname') == 'FUTSTK' and x.get('symname') == symbol]
        futures.sort(key=lambda x: pd.to_datetime(x['exd'], format='%d-%b-%Y'))
        if futures:
            token = futures[0]['token']
            tsym = futures[0]['tsym']
            q = api.get_quotes(exchange='NFO', token=token)
            if q and 'lp' in q:
                return {
                    'lp': float(q['lp']),
                    'prev_close': float(q.get('c', 0)),
                    'open': float(q.get('o', 0)),
                    'high': float(q.get('h', 0)),
                    'low': float(q.get('l', 0)),
                    'volume': int(q.get('v', 0)),
                    'oi': int(q.get('oi', 0)),
                    'tsym': tsym,
                    'expiry': futures[0]['exd'],
                }
    return None


def main():
    print("=" * 80)
    print("  DATA VERIFICATION & P&L REPORT")
    print(f"  Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ── Step 1: Login ──
    client = ShoonyaClient()
    api = client.login()
    if not api:
        print("FATAL: Shoonya login failed")
        return
    print("\n[OK] Shoonya API connected\n")

    # ── Step 2: Fetch Live Prices ──
    all_symbols = ['CIPLA', 'SUNPHARMA', 'PFC', 'RECLTD', 'LUPIN', 'POWERGRID']
    live = {}

    print("─" * 80)
    print("  SECTION 1: LIVE FUTURES PRICES")
    print("─" * 80)

    for sym in all_symbols:
        p = get_live_price(api, sym)
        if p:
            live[sym] = p
            chg = ((p['lp'] - p['prev_close']) / p['prev_close'] * 100) if p['prev_close'] > 0 else 0
            print(f"  {sym:>12s}:  LTP = {p['lp']:>10.2f}  |  Prev Close = {p['prev_close']:>10.2f}  |  Chg = {chg:+.2f}%  |  Exp: {p['expiry']}  |  Contract: {p['tsym']}")
        else:
            print(f"  {sym:>12s}:  ** FAILED TO FETCH **")
        time.sleep(0.3)

    # ── Step 3: Data Verification ──
    print("\n" + "─" * 80)
    print("  SECTION 2: HISTORICAL DATA VERIFICATION")
    print("─" * 80)

    for sym in all_symbols:
        cont, info = load_continuous(sym)
        if cont is None:
            print(f"\n  {sym}: {info}")
            continue

        print(f"\n  {sym}:")
        print(f"    CSV Rows: {info['total_rows']} total, {info['parsed_ok']} parsed OK, {info['parse_failed']} FAILED")
        print(f"    Continuous Series: {info['continuous_rows']} trading days")
        print(f"    Date Range: {info['date_range']}")
        print(f"    Last CSV Close: {info['last_close']:.2f} on {info['last_date']}")

        if sym in live:
            api_prev = live[sym]['prev_close']
            csv_close = info['last_close']

            # The CSV last close should match or be very close to the API's prev_close
            # (if CSV was last updated yesterday) or match the live price (if updated today)
            diff_pct = abs(csv_close - api_prev) / api_prev * 100 if api_prev > 0 else 0

            match_status = "MATCH" if diff_pct < 1.0 else "CLOSE" if diff_pct < 3.0 else "** MISMATCH **"
            print(f"    API Prev Close: {api_prev:.2f}")
            print(f"    CSV vs API Prev: Diff = {abs(csv_close - api_prev):.2f} ({diff_pct:.2f}%) → {match_status}")

            if info['second_last_close']:
                diff_pct2 = abs(info['second_last_close'] - api_prev) / api_prev * 100 if api_prev > 0 else 0
                if diff_pct2 < 1.0:
                    print(f"    Note: 2nd-last CSV close ({info['second_last_close']:.2f} on {info['second_last_date']}) matches API prev_close → CSV likely includes today's data")

    # ── Step 4: P&L Calculations ──
    print("\n" + "─" * 80)
    print("  SECTION 3: POSITION P&L (SIMPLE DIFFERENCE)")
    print("─" * 80)

    # Position 1: Long CIPLA / Short SUNPHARMA
    # Entry: SUN - CIPLA = 435 (i.e., you shorted SUNPHARMA at X, bought CIPLA at X-435)
    # P&L on difference trades:
    #   If you're LONG A, SHORT B, and entry diff = B - A = D_entry
    #   Current diff = B - A = D_now
    #   P&L per share = D_entry - D_now  (you want the diff to shrink)

    if 'CIPLA' in live and 'SUNPHARMA' in live:
        cipla_ltp = live['CIPLA']['lp']
        sun_ltp = live['SUNPHARMA']['lp']

        entry_diff_cs = 435.0  # SUN - CIPLA at entry
        current_diff_cs = sun_ltp - cipla_ltp
        pnl_per_share_cs = entry_diff_cs - current_diff_cs  # Positive = profit

        print(f"\n  TRADE 1: Long CIPLA / Short SUNPHARMA")
        print(f"  {'─' * 50}")
        print(f"    Position: LONG CIPLA + SHORT SUNPHARMA")
        print(f"    Entry Diff (SUN - CIPLA):     {entry_diff_cs:.2f}")
        print(f"    CIPLA  now:  {cipla_ltp:.2f}")
        print(f"    SUNPHARMA now: {sun_ltp:.2f}")
        print(f"    Current Diff (SUN - CIPLA):   {current_diff_cs:.2f}")
        print(f"    P&L per share = Entry - Current = {entry_diff_cs:.2f} - {current_diff_cs:.2f} = {pnl_per_share_cs:.2f}")

        if pnl_per_share_cs > 0:
            print(f"    >>> PROFIT: +{pnl_per_share_cs:.2f} per share")
        else:
            print(f"    >>> LOSS: {pnl_per_share_cs:.2f} per share")

        # Which leg is causing the loss?
        # Load entry-day-ish prices from historical (around when diff was 435)
        # We know entry diff = 435, so let's find when that was historically
        print(f"\n    Breakdown of what moved:")
        print(f"    - If CIPLA dropped → LOSS (you are long CIPLA)")
        print(f"    - If SUNPHARMA rose → LOSS (you are short SUNPHARMA)")
        print(f"    - Current diff widened from {entry_diff_cs} to {current_diff_cs:.2f}")
        if current_diff_cs > entry_diff_cs:
            print(f"    - Spread WIDENED by {current_diff_cs - entry_diff_cs:.2f} → LOSS")
            print(f"    - This means SUNPHARMA outperformed CIPLA since entry")
        else:
            print(f"    - Spread NARROWED by {entry_diff_cs - current_diff_cs:.2f} → PROFIT")

    # Position 2: PFC / RECLTD
    if 'PFC' in live and 'RECLTD' in live:
        pfc_ltp = live['PFC']['lp']
        rec_ltp = live['RECLTD']['lp']

        # User says entry diff was 33 "before merger" and is at a loss due to PFC
        # Need to determine direction. If loss is due to PFC:
        # - If short PFC and PFC rose → loss
        # - If long PFC and PFC dropped → loss

        # Let's show both directions and let the user confirm
        current_diff_pr = pfc_ltp - rec_ltp
        entry_diff_pr = 33.0

        print(f"\n  TRADE 2: PFC / RECLTD")
        print(f"  {'─' * 50}")
        print(f"    Entry Diff (PFC - REC):     {entry_diff_pr:.2f}")
        print(f"    PFC    now:  {pfc_ltp:.2f}")
        print(f"    RECLTD now:  {rec_ltp:.2f}")
        print(f"    Current Diff (PFC - REC):   {current_diff_pr:.2f}")

        # Scenario A: Long PFC / Short REC (entry diff PFC-REC = 33, now wider → profit)
        pnl_a = current_diff_pr - entry_diff_pr
        # Scenario B: Short PFC / Long REC (entry diff PFC-REC = 33, now wider → loss)
        pnl_b = entry_diff_pr - current_diff_pr

        print(f"\n    >>> NEED TO CONFIRM YOUR DIRECTION:")
        print(f"    Scenario A (Long PFC / Short REC): P&L = {current_diff_pr:.2f} - {entry_diff_pr:.2f} = {pnl_a:+.2f} per share {'(PROFIT)' if pnl_a > 0 else '(LOSS)'}")
        print(f"    Scenario B (Short PFC / Long REC): P&L = {entry_diff_pr:.2f} - {current_diff_pr:.2f} = {pnl_b:+.2f} per share {'(PROFIT)' if pnl_b > 0 else '(LOSS)'}")

        print(f"\n    Since you said LOSS due to PFC:")
        if pfc_ltp > rec_ltp + entry_diff_pr:
            print(f"    - PFC has risen significantly (now {pfc_ltp:.2f})")
            print(f"    - If you're SHORT PFC / LONG REC → you're losing because PFC rose")
            print(f"    - That would be Scenario B: Loss = {abs(pnl_b):.2f} per share")
        else:
            print(f"    - PFC at {pfc_ltp:.2f}, REC at {rec_ltp:.2f}")

    # ── Step 5: Data freshness check for swap candidates ──
    print("\n" + "─" * 80)
    print("  SECTION 4: DATA FRESHNESS FOR SWAP CANDIDATES")
    print("─" * 80)

    swap_symbols = ['LUPIN', 'POWERGRID', 'DRREDDY', 'AUROPHARMA', 'IRFC', 'NHPC', 'NTPC']
    for sym in swap_symbols:
        path = os.path.join(DATA_DIR, f"{sym}_3Y.csv")
        if not os.path.exists(path):
            print(f"  {sym:>12s}: ** NO DATA FILE **")
            continue
        cont, info = load_continuous(sym)
        if cont is None:
            print(f"  {sym:>12s}: {info}")
            continue

        stale_flag = ""
        last_dt = pd.to_datetime(info['last_date'], format='%d-%b-%Y')
        days_old = (pd.Timestamp.now() - last_dt).days
        if days_old > 5:
            stale_flag = f" ** STALE ({days_old} days old) — NEEDS UPDATE **"

        print(f"  {sym:>12s}: Last = {info['last_close']:>10.2f} on {info['last_date']} | {info['continuous_rows']} days{stale_flag}")

        # Cross-check with live if available
        if sym in live:
            api_prev = live[sym]['prev_close']
            diff_pct = abs(info['last_close'] - api_prev) / api_prev * 100 if api_prev > 0 else 0
            status = "OK" if diff_pct < 1.0 else f"DIFF {diff_pct:.1f}%"
            print(f"  {'':>12s}  Live prev_close = {api_prev:.2f} → {status}")


if __name__ == '__main__':
    main()
