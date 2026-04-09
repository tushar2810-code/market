"""
SUNPHARMA/CIPLA Pair Trade — Live Analysis
Fetches live futures prices and runs full Z-score analysis across timeframes.
Also runs historical backtest grid to show optimal configs.
"""

import pandas as pd
import numpy as np
import os
import sys
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'execution'))
from shoonya_client import ShoonyaClient

DATA_DIR = os.path.join(os.path.dirname(__file__), '3y_data')

def load_historical(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        print(f"  ❌ Data file not found: {path}")
        return None, None

    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

    last_date = df['FH_TIMESTAMP'].max()
    trading_days_stale = int(np.busday_count(last_date.date(), datetime.now().date()))
    
    # Build continuous series (nearest expiry per date)
    df['FH_EXPIRY_DT_parsed'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT_parsed'].idxmin()].copy()
    continuous = continuous.sort_values('FH_TIMESTAMP')
    
    result = continuous[['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_MARKET_LOT']].set_index('FH_TIMESTAMP')
    result['pct_change'] = result['FH_CLOSING_PRICE'].pct_change(fill_method=None)
    
    return result, trading_days_stale


def get_live_price(api, symbol):
    """Fetch current month futures price."""
    try:
        ret = api.searchscrip(exchange='NFO', searchtext=symbol)
        if not ret or 'values' not in ret:
            return None, None, None

        futs = []
        for s in ret['values']:
            dname = s.get('dname', '')
            if 'FUT' not in dname:
                continue
            contract_symbol = dname.split()[0] if dname else ''
            if contract_symbol == symbol:
                futs.append(s)

        if not futs:
            return None, None, None

        def parse_expiry(x):
            try:
                return pd.to_datetime(x.get('dname', '').split()[-1], format='%d%b%Y')
            except:
                return pd.Timestamp.max

        futs.sort(key=parse_expiry)
        selected = futs[0]
        token = selected['token']
        lot_size = int(selected.get('ls', 1))
        contract_name = selected.get('dname', '')

        q = api.get_quotes(exchange='NFO', token=token)
        if q and 'lp' in q:
            price = float(q['lp'])
            if price <= 0 or np.isnan(price):
                return None, None, None
            return price, lot_size, contract_name

        return None, None, None
    except Exception as e:
        print(f"  Error fetching {symbol}: {e}")
        return None, None, None


def run_analysis():
    SYM_A = "SUNPHARMA"
    SYM_B = "CIPLA"
    
    print("=" * 80)
    print(f"  SUNPHARMA / CIPLA PAIR TRADE — LIVE ANALYSIS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 80)

    # Check market hours
    now = datetime.now()
    if now.weekday() >= 5:
        print(f"\n  ⚠️  Markets closed (weekend)")
    elif now.hour < 9 or (now.hour == 9 and now.minute < 15):
        print(f"\n  ⚠️  Pre-market — prices may be from previous close")
    elif now.hour > 15 or (now.hour == 15 and now.minute > 30):
        print(f"\n  ⚠️  Post-market — prices are closing prices")

    # =========================================================================
    # 1. LOAD HISTORICAL DATA
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 1: HISTORICAL DATA")
    print(f"{'─'*80}")
    
    hist_a, stale_a = load_historical(SYM_A)
    hist_b, stale_b = load_historical(SYM_B)
    
    if hist_a is None or hist_b is None:
        print("  ❌ Cannot proceed without historical data.")
        return
    
    print(f"  {SYM_A}: {len(hist_a)} data points, {stale_a} trading days stale")
    print(f"         Range: {hist_a.index.min().date()} → {hist_a.index.max().date()}")
    print(f"  {SYM_B}: {len(hist_b)} data points, {stale_b} trading days stale")
    print(f"         Range: {hist_b.index.min().date()} → {hist_b.index.max().date()}")
    
    if stale_a > 3 or stale_b > 3:
        print(f"\n  ⚠️  DATA IS STALE ({max(stale_a, stale_b)} trading days) — Z-scores may be unreliable!")

    # Merge
    merged = hist_a[['FH_CLOSING_PRICE', 'pct_change']].join(
        hist_b[['FH_CLOSING_PRICE', 'pct_change']],
        how='inner', lsuffix='_A', rsuffix='_B'
    )
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']
    
    print(f"\n  Merged: {len(merged)} common trading days")
    print(f"  Date range: {merged.index.min().date()} → {merged.index.max().date()}")
    print(f"  Ratio range: {merged['RATIO'].min():.4f} → {merged['RATIO'].max():.4f}")
    print(f"  Current hist ratio: {merged['RATIO'].iloc[-1]:.4f}")
    
    # =========================================================================
    # 2. FETCH LIVE PRICES
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 2: LIVE FUTURES PRICES")
    print(f"{'─'*80}")
    
    import logging
    logging.basicConfig(level=logging.ERROR)
    
    client = ShoonyaClient()
    api = client.login()
    
    if not api:
        print("  ❌ Shoonya login failed. Using last historical close instead.")
        live_a = merged['FH_CLOSING_PRICE_A'].iloc[-1]
        live_b = merged['FH_CLOSING_PRICE_B'].iloc[-1]
        lot_a = int(hist_a['FH_MARKET_LOT'].iloc[-1])
        lot_b = int(hist_b['FH_MARKET_LOT'].iloc[-1])
        print(f"  {SYM_A}: ₹{live_a:.2f} (last close)")
        print(f"  {SYM_B}: ₹{live_b:.2f} (last close)")
    else:
        live_a, lot_a, contract_a = get_live_price(api, SYM_A)
        live_b, lot_b, contract_b = get_live_price(api, SYM_B)
        
        if live_a is None or live_b is None:
            print("  ❌ Could not fetch live prices. Falling back to last close.")
            live_a = merged['FH_CLOSING_PRICE_A'].iloc[-1]
            live_b = merged['FH_CLOSING_PRICE_B'].iloc[-1]
            lot_a = int(hist_a['FH_MARKET_LOT'].iloc[-1])
            lot_b = int(hist_b['FH_MARKET_LOT'].iloc[-1])
        else:
            print(f"  {SYM_A}: ₹{live_a:.2f} ({contract_a}) × {lot_a} lot = ₹{live_a * lot_a:,.0f}")
            print(f"  {SYM_B}: ₹{live_b:.2f} ({contract_b}) × {lot_b} lot = ₹{live_b * lot_b:,.0f}")
    
    live_ratio = live_a / live_b
    value_a = live_a * lot_a
    value_b = live_b * lot_b
    cash_imbal = abs(value_a - value_b) / max(value_a, value_b) * 100
    
    print(f"\n  Live Ratio: {live_ratio:.4f}")
    print(f"  Cash Imbalance: {cash_imbal:.1f}%")
    
    # =========================================================================
    # 3. MULTI-TIMEFRAME Z-SCORE ANALYSIS
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 3: Z-SCORE ANALYSIS (Multi-Timeframe)")
    print(f"{'─'*80}")
    
    # Append live ratio to series
    last_date = merged.index[-1] + pd.Timedelta(days=1)
    new_row = pd.DataFrame({
        'FH_CLOSING_PRICE_A': [live_a],
        'FH_CLOSING_PRICE_B': [live_b],
        'RATIO': [live_ratio]
    }, index=[last_date])
    combined = pd.concat([merged[['FH_CLOSING_PRICE_A', 'FH_CLOSING_PRICE_B', 'RATIO']], new_row])
    
    windows = [10, 20, 30, 45, 60, 90, 120, 180, 250]
    
    print(f"\n  {'Window':<10} {'Mean':>10} {'Std':>10} {'Z-Score':>10} {'SSS':>8} {'Status':<20}")
    print(f"  {'─'*68}")
    
    active_signals = []
    
    for w in windows:
        if len(combined) < w:
            continue
        
        subset = combined['RATIO'].tail(w + 1)  # +1 for the live point
        hist_subset = subset.iloc[:-1]  # Exclude live point for stats
        
        mean = hist_subset.mean()
        std = hist_subset.std()
        
        if std == 0 or np.isnan(std):
            continue
        
        z = (live_ratio - mean) / std
        
        # Calculate correlation for this window
        ret_a = combined['FH_CLOSING_PRICE_A'].tail(w).pct_change(fill_method=None).dropna()
        ret_b = combined['FH_CLOSING_PRICE_B'].tail(w).pct_change(fill_method=None).dropna()
        corr = ret_a.corr(ret_b)
        
        # SSS from directive
        sss = abs(z) * (1 + max(corr, 0))
        
        status = "NEUTRAL"
        if abs(z) > 3.0: status = "⛔ EXTREME"
        elif abs(z) > 2.5: status = "🔴 STRONG SIGNAL"
        elif abs(z) > 2.0: status = "🟡 SIGNAL"
        elif abs(z) > 1.5: status = "🟠 WATCH"
        
        print(f"  {w:>3}d       {mean:>10.4f} {std:>10.4f} {z:>+10.2f} {sss:>8.2f} {status:<20}")
        
        if abs(z) >= 2.0:
            direction = "BUY A / SELL B" if z < 0 else "SELL A / BUY B"
            active_signals.append({
                'window': w, 'z': z, 'sss': sss, 'corr': corr,
                'mean': mean, 'std': std, 'direction': direction
            })
    
    # =========================================================================
    # 4. CORRELATION ANALYSIS
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 4: CORRELATION ANALYSIS")
    print(f"{'─'*80}")
    
    returns_a = merged['FH_CLOSING_PRICE_A'].pct_change(fill_method=None)
    returns_b = merged['FH_CLOSING_PRICE_B'].pct_change(fill_method=None)
    
    corr_windows = [10, 20, 30, 60, 90, 120, 250]
    print(f"\n  {'Window':<10} {'Return Corr':>12} {'Price Corr':>12} {'Status':<15}")
    print(f"  {'─'*52}")
    
    for w in corr_windows:
        if len(merged) < w:
            continue
        ret_corr = returns_a.tail(w).corr(returns_b.tail(w))
        price_corr = merged['FH_CLOSING_PRICE_A'].tail(w).corr(merged['FH_CLOSING_PRICE_B'].tail(w))
        
        status = "✅ STRONG" if ret_corr > 0.5 else ("🟡 MODERATE" if ret_corr > 0.3 else "❌ WEAK")
        print(f"  {w:>3}d       {ret_corr:>+12.3f} {price_corr:>+12.3f} {status:<15}")
    
    # =========================================================================
    # 5. SAFETY GATES
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 5: SAFETY GATES")
    print(f"{'─'*80}")
    
    # Gate 1: Data Freshness
    max_stale = max(stale_a, stale_b)
    g1_pass = max_stale <= 3
    print(f"\n  Gate 1 — Data Freshness:     {'✅ PASS' if g1_pass else '❌ FAIL'} ({max_stale} trading days stale)")
    
    # Gate 2: 20D Return Correlation
    corr_20 = returns_a.tail(20).corr(returns_b.tail(20))
    g2_pass = corr_20 > 0.3
    print(f"  Gate 2 — 20D Return Corr:    {'✅ PASS' if g2_pass else '❌ FAIL'} ({corr_20:.3f}, threshold: >0.3)")
    
    # Gate 3: Historical Range
    hist_min = merged['RATIO'].min()
    hist_max = merged['RATIO'].max()
    margin = (hist_max - hist_min) * 0.05
    g3_pass = hist_min - margin <= live_ratio <= hist_max + margin
    print(f"  Gate 3 — Historical Range:   {'✅ PASS' if g3_pass else '❌ FAIL'} (ratio {live_ratio:.4f} in [{hist_min:.4f}, {hist_max:.4f}])")
    
    # Gate 5: Z-Score Cap (using 30d window as default)
    if len(combined) >= 31:
        z_30 = (live_ratio - combined['RATIO'].tail(31).iloc[:-1].mean()) / combined['RATIO'].tail(31).iloc[:-1].std()
        g5_pass = abs(z_30) <= 4.0
        print(f"  Gate 5 — Z-Score Cap:        {'✅ PASS' if g5_pass else '❌ FAIL'} (30d Z={z_30:.2f}, cap: ±4.0)")
    
    all_gates = g1_pass and g2_pass and g3_pass
    print(f"\n  Overall: {'✅ ALL GATES PASS' if all_gates else '⚠️  NOT ALL GATES PASS — Trade with caution'}")

    # =========================================================================
    # 6. HISTORICAL BACKTEST SUMMARY (Using cached results if available)
    # =========================================================================
    print(f"\n{'─'*80}")
    print(f"  SECTION 6: QUICK BACKTEST (Best Configs)")
    print(f"{'─'*80}")
    
    best_configs = []
    for w in [20, 30, 45, 60]:
        df = merged.copy()
        df['Mean'] = df['RATIO'].rolling(window=w).mean()
        df['Std'] = df['RATIO'].rolling(window=w).std()
        df['Z'] = (df['RATIO'] - df['Mean']) / df['Std']
        df = df.dropna()
        
        for z_e in [1.5, 2.0, 2.5]:
            for ts in [20, 30, 45]:
                trades = []
                position = 0
                entry_ratio = 0
                entry_date = None
                
                for i in range(len(df)):
                    row = df.iloc[i]
                    z = row['Z']
                    ratio = row['RATIO']
                    current_date = df.index[i]
                    
                    if position == 0:
                        if z < -z_e:
                            position = 1; entry_ratio = ratio; entry_date = current_date
                        elif z > z_e:
                            position = -1; entry_ratio = ratio; entry_date = current_date
                    elif position != 0:
                        days_held = (current_date - entry_date).days
                        exit_signal = False
                        
                        if position == 1:
                            if z > 0: exit_signal = True
                            elif z < -3.5: exit_signal = True
                            elif days_held >= ts: exit_signal = True
                            if exit_signal:
                                pnl = (ratio - entry_ratio) / entry_ratio
                        elif position == -1:
                            if z < 0: exit_signal = True
                            elif z > 3.5: exit_signal = True
                            elif days_held >= ts: exit_signal = True
                            if exit_signal:
                                pnl = (entry_ratio - ratio) / entry_ratio
                        
                        if exit_signal:
                            trades.append({'Return': pnl, 'Duration': days_held})
                            position = 0
                
                if len(trades) >= 5:
                    wins = [t for t in trades if t['Return'] > 0]
                    wr = len(wins) / len(trades) * 100
                    avg_ret = np.mean([t['Return'] for t in trades]) * 100
                    avg_days = np.mean([t['Duration'] for t in trades])
                    max_dd = min([t['Return'] for t in trades]) * 100
                    
                    best_configs.append({
                        'w': w, 'z_e': z_e, 'ts': ts,
                        'trades': len(trades), 'wr': wr,
                        'avg_ret': avg_ret, 'avg_days': avg_days, 'max_dd': max_dd
                    })
    
    if best_configs:
        best_configs.sort(key=lambda x: (x['wr'] >= 80, x['wr'], x['avg_ret']), reverse=True)
        
        print(f"\n  {'Config':<25} {'Trades':>7} {'WinRate':>8} {'AvgRet':>8} {'AvgDays':>8} {'MaxDD':>8}")
        print(f"  {'─'*72}")
        
        for c in best_configs[:10]:
            config_str = f"W{c['w']}/Z{c['z_e']}/T{c['ts']}"
            tier = "🏆" if c['wr'] >= 90 else ("✅" if c['wr'] >= 75 else "⚠️")
            print(f"  {tier} {config_str:<22} {c['trades']:>7} {c['wr']:>7.1f}% {c['avg_ret']:>+7.2f}% {c['avg_days']:>7.1f}d {c['max_dd']:>+7.2f}%")
    
    # =========================================================================
    # 7. VERDICT
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  VERDICT")
    print(f"{'='*80}")
    
    if active_signals:
        best_sig = max(active_signals, key=lambda x: x['sss'])
        print(f"\n  🎯 ACTIVE SIGNAL DETECTED")
        print(f"     Window: {best_sig['window']}d")
        print(f"     Z-Score: {best_sig['z']:+.2f}")
        print(f"     SSS: {best_sig['sss']:.2f} ({'STRONG' if best_sig['sss'] > 6 else 'MODERATE' if best_sig['sss'] > 4 else 'WEAK'})")
        print(f"     Correlation: {best_sig['corr']:.3f}")
        print(f"     Direction: {best_sig['direction']}")
        
        if best_sig['z'] < -2.0:
            print(f"     Action: BUY {SYM_A} FUT + SELL {SYM_B} FUT")
        elif best_sig['z'] > 2.0:
            print(f"     Action: SELL {SYM_A} FUT + BUY {SYM_B} FUT")
        
        # Risk note from historical lesson
        print(f"\n  ⚠️  HISTORICAL NOTE:")
        print(f"     Feb 2026 lesson: CIPLA/SUNPHARMA had -₹10k drawdown at Z=2.2")
        print(f"     This pair is NOT in the 'Proven Pairs' list (90%+ WR required)")
        print(f"     Proceed with caution — lower conviction than Tier 1 pairs")
        
        if not all_gates:
            print(f"\n  ❌ SAFETY GATES FAILED — Signal is VOID per directive")
    else:
        print(f"\n  ✅ NO ACTIVE SIGNAL")
        print(f"     Z-scores across all timeframes are within ±2.0")
        print(f"     No trade warranted at this time.")
    
    # Recent ratio trend
    print(f"\n  📊 Recent Ratio Trend (last 10 trading days):")
    for i in range(-min(10, len(merged)), 0):
        idx = merged.index[i]
        r = merged['RATIO'].iloc[i]
        print(f"     {idx.date()}: {r:.4f}")
    print(f"     {'LIVE':>10}: {live_ratio:.4f}")
    
    print(f"\n{'='*80}")


if __name__ == "__main__":
    run_analysis()
