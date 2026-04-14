"""
Antigravity v3 — Honest Deep Backtest.

All data is validated, cleaned, and sanity-checked before any P&L is calculated.
No garbage data. No inflated numbers. Just the truth.

Usage:
    python3 execution/honest_backtest.py --capital 1000000
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))
from kelly_sizer import kelly_fraction

DATA_DIR = '.tmp/3y_data'


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING WITH VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def load_and_validate(symbol, require_underlying=False):
    """Load data with strict validation. Returns None if data is bad."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None
    
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE'])
    
    if 'FH_INSTRUMENT' in df.columns:
        df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
    
    if 'FH_EXPIRY_DT' in df.columns:
        df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    
    if require_underlying:
        if 'FH_UNDERLYING_VALUE' not in df.columns:
            return None
        df['FH_UNDERLYING_VALUE'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
        df = df.dropna(subset=['FH_UNDERLYING_VALUE'])
        df = df[df['FH_UNDERLYING_VALUE'] > 0]
        
        # VALIDATION: Futures/Spot ratio must be sane on EVERY row (not just median)
        ratio = df['FH_CLOSING_PRICE'] / df['FH_UNDERLYING_VALUE']
        # Keep only rows where ratio is between 0.95 and 1.05 (within 5%)
        valid_mask = (ratio >= 0.95) & (ratio <= 1.05)
        if valid_mask.mean() < 0.80:  # If less than 80% of rows are valid, skip symbol
            return None
        df = df[valid_mask]
    
    if 'FH_MARKET_LOT' in df.columns:
        df['FH_MARKET_LOT'] = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce').fillna(1).astype(int)
    
    df = df.sort_values('FH_TIMESTAMP')
    
    if len(df) < 100:
        return None
    
    return df


def get_continuous_prices(symbol):
    """Get nearest-expiry continuous futures prices."""
    df = load_and_validate(symbol)
    if df is None:
        return None
    
    if 'FH_EXPIRY_DT' in df.columns:
        df = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT'].idxmin()]
    
    lot = int(df['FH_MARKET_LOT'].iloc[-1]) if 'FH_MARKET_LOT' in df.columns else 1
    prices = df.set_index('FH_TIMESTAMP')['FH_CLOSING_PRICE']
    return prices, lot


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: EXPIRY CONVERGENCE (CLEAN DATA ONLY)
# ═══════════════════════════════════════════════════════════════════════════════

def backtest_expiry_clean(threshold_pct=0.3, entry_days=5):
    """
    Expiry convergence with STRICT data validation:
    1. Only symbols where Fut/Spot ratio is 0.95-1.05 on 80%+ of rows
    2. Only individual rows where ratio is sane
    3. Premium must be realistic (< 3%)
    """
    all_trades = []
    valid_count = 0
    skip_count = 0
    
    files = sorted(os.listdir(DATA_DIR))
    
    for fname in files:
        if not fname.endswith('_5Y.csv'):
            continue
        symbol = fname.replace('_5Y.csv', '')
        
        df = load_and_validate(symbol, require_underlying=True)
        if df is None:
            skip_count += 1
            continue
        
        valid_count += 1
        
        for expiry in sorted(df['FH_EXPIRY_DT'].dropna().unique()):
            expiry_dt = pd.Timestamp(expiry)
            month_data = df[df['FH_EXPIRY_DT'] == expiry].sort_values('FH_TIMESTAMP')
            
            if month_data.empty:
                continue
            
            # Entry window
            entry_start = expiry_dt - timedelta(days=entry_days + 3)
            window = month_data[
                (month_data['FH_TIMESTAMP'] >= entry_start) &
                (month_data['FH_TIMESTAMP'] <= expiry_dt)
            ]
            
            if len(window) < 2:
                continue
            
            # Entry: around 5 days before expiry
            entry_candidates = window[window['FH_TIMESTAMP'] <= expiry_dt - timedelta(days=entry_days - 2)]
            if entry_candidates.empty:
                entry_candidates = window.head(1)
            
            entry = entry_candidates.iloc[-1]
            fut = entry['FH_CLOSING_PRICE']
            spot = entry['FH_UNDERLYING_VALUE']
            
            premium = (fut - spot) / spot * 100
            
            # SANITY CHECK: premium must be < 3% (anything bigger is suspicious)
            if abs(premium) < threshold_pct or abs(premium) > 3.0:
                continue
            
            # Exit: last day at/before expiry
            exit_data = window[window['FH_TIMESTAMP'] <= expiry_dt]
            if exit_data.empty:
                continue
            
            exit_row = exit_data.iloc[-1]
            exit_fut = exit_row['FH_CLOSING_PRICE']
            exit_spot = exit_row['FH_UNDERLYING_VALUE']
            
            if exit_spot <= 0:
                continue
            
            exit_premium = (exit_fut - exit_spot) / exit_spot * 100
            
            # SANITY: exit premium should also be reasonable
            if abs(exit_premium) > 3.0:
                continue
            
            # P&L
            if premium > 0:
                pnl_pct = premium - exit_premium  # Sold high premium, exited at lower
            else:
                pnl_pct = exit_premium - premium  # Bought discount, exited at smaller discount
            
            lot = int(entry.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in entry.index else 1
            pnl_rupees = (pnl_pct / 100) * spot * lot
            
            days_held = (exit_row['FH_TIMESTAMP'] - entry['FH_TIMESTAMP']).days
            
            all_trades.append({
                'strategy': 'EXPIRY_CONV',
                'symbol': symbol,
                'entry_date': entry['FH_TIMESTAMP'],
                'exit_date': exit_row['FH_TIMESTAMP'],
                'pnl_pct': round(pnl_pct, 4),
                'pnl_rupees': round(pnl_rupees, 0),
                'entry_premium': round(premium, 3),
                'exit_premium': round(exit_premium, 3),
                'days_held': days_held,
            })
    
    return pd.DataFrame(all_trades), valid_count, skip_count


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: PAIR TRADING (price-ratio based, no underlying needed)
# ═══════════════════════════════════════════════════════════════════════════════

PAIRS = [
    ('ULTRACEMCO', 'GRASIM'),
    ('TATAPOWER', 'NHPC'),
    ('M&M', 'BHARATFORG'),
    ('HUDCO', 'ADANIGREEN'),
    ('LODHA', 'IRCTC'),
    ('LT', 'GMRAIRPORT'),
    ('ADANIGREEN', 'ADANIENSOL'),
    ('BIOCON', 'TORNTPHARM'),
    ('IRFC', 'ADANIENT'),
    ('HINDUNILVR', 'DMART'),
    ('TATASTEEL', 'JSWSTEEL'),
    ('HDFCBANK', 'ICICIBANK'),
    ('TCS', 'INFY'),
    ('NTPC', 'POWERGRID'),
    ('SBIN', 'BANKBARODA'),
]


def backtest_pairs(window=30, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=20):
    """Pair trading backtest using price ratios (no underlying needed)."""
    all_trades = []
    pair_stats = []
    
    for sym_a, sym_b in PAIRS:
        data_a = get_continuous_prices(sym_a)
        data_b = get_continuous_prices(sym_b)
        if data_a is None or data_b is None:
            continue
        
        prices_a, lot_a = data_a
        prices_b, lot_b = data_b
        
        merged = pd.DataFrame({'A': prices_a, 'B': prices_b}).dropna()
        if len(merged) < window + 50:
            continue
        
        ratio = merged['A'] / merged['B']
        z_scores = (ratio - ratio.rolling(window).mean()) / ratio.rolling(window).std()
        dates = merged.index
        
        in_trade = False
        pair_trades = []
        
        for i in range(window + 10, len(dates)):
            z = z_scores.iloc[i]
            if np.isnan(z):
                continue
            
            if not in_trade:
                if abs(z) >= z_entry:
                    entry_idx = i
                    entry_ratio = ratio.iloc[i]
                    entry_a = merged['A'].iloc[i]
                    entry_b = merged['B'].iloc[i]
                    direction = 'SHORT_SPREAD' if z > 0 else 'LONG_SPREAD'
                    in_trade = True
            else:
                days = (dates[i] - dates[entry_idx]).days
                should_exit = False
                
                if direction == 'SHORT_SPREAD' and z <= z_exit:
                    should_exit = True
                elif direction == 'LONG_SPREAD' and z >= -z_exit:
                    should_exit = True
                elif abs(z) >= z_stop:
                    should_exit = True
                elif days >= time_stop:
                    should_exit = True
                
                if should_exit:
                    exit_ratio = ratio.iloc[i]
                    exit_a = merged['A'].iloc[i]
                    exit_b = merged['B'].iloc[i]
                    
                    # P&L: compute actual leg P&L
                    if direction == 'SHORT_SPREAD':
                        # Sold A, Bought B
                        pnl_a = (entry_a - exit_a) / entry_a * 100  # Short A profit
                        pnl_b = (exit_b - entry_b) / entry_b * 100  # Long B profit
                    else:
                        # Bought A, Sold B
                        pnl_a = (exit_a - entry_a) / entry_a * 100  # Long A profit
                        pnl_b = (entry_b - exit_b) / entry_b * 100  # Short B profit
                    
                    # Combined P&L (average of both legs)
                    pnl_pct = (pnl_a + pnl_b) / 2
                    
                    # Rupee P&L (both legs)
                    pnl_rupees = (pnl_a / 100) * entry_a * lot_a + (pnl_b / 100) * entry_b * lot_b
                    
                    trade = {
                        'strategy': 'PAIR_TRADE',
                        'symbol': f"{sym_a}/{sym_b}",
                        'entry_date': dates[entry_idx],
                        'exit_date': dates[i],
                        'pnl_pct': round(pnl_pct, 4),
                        'pnl_rupees': round(pnl_rupees, 0),
                        'pnl_leg_a': round(pnl_a, 4),
                        'pnl_leg_b': round(pnl_b, 4),
                        'direction': direction,
                        'days_held': days,
                    }
                    all_trades.append(trade)
                    pair_trades.append(trade)
                    in_trade = False
        
        if pair_trades:
            df_pt = pd.DataFrame(pair_trades)
            pair_stats.append({
                'pair': f"{sym_a}/{sym_b}",
                'trades': len(df_pt),
                'wr': (df_pt['pnl_pct'] > 0).mean(),
                'avg': df_pt['pnl_pct'].mean(),
                'total_rs': df_pt['pnl_rupees'].sum(),
            })
    
    return pd.DataFrame(all_trades), pair_stats


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: MOMENTUM RSI — LONG ONLY
# ═══════════════════════════════════════════════════════════════════════════════

SECTORS = {
    'PHARMA': ['SUNPHARMA', 'CIPLA', 'DRREDDY', 'LUPIN', 'AUROPHARMA', 'DIVISLAB', 'TORNTPHARM', 'ALKEM', 'ZYDUSLIFE'],
    'BANKING': ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK', 'SBIN', 'INDUSINDBK', 'BANKBARODA', 'PNB', 'FEDERALBNK'],
    'IT': ['TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'LTIM', 'MPHASIS', 'COFORGE', 'PERSISTENT'],
    'METALS': ['TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'SAIL', 'NMDC', 'JINDALSTEL', 'COALINDIA'],
    'POWER': ['NTPC', 'POWERGRID', 'TATAPOWER', 'NHPC', 'PFC', 'RECLTD', 'IREDA', 'IRFC'],
    'AUTO': ['MARUTI', 'M&M', 'BAJAJ-AUTO', 'HEROMOTOCO', 'TVSMOTOR', 'EICHERMOT', 'ASHOKLEY'],
    'FMCG': ['HINDUNILVR', 'ITC', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL', 'BRITANNIA', 'TATACONSUM'],
    'CEMENT': ['ULTRACEMCO', 'AMBUJACEM', 'SHREECEM', 'DALBHARAT', 'GRASIM'],
    'OIL': ['RELIANCE', 'ONGC', 'BPCL', 'IOC', 'HINDPETRO', 'GAIL'],
    'INFRA': ['LT', 'DLF', 'OBEROIRLTY', 'GODREJPROP', 'PRESTIGE', 'LODHA'],
}


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    for i in range(period, len(avg_gain)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def backtest_momentum_long(rsi_buy=30, rsi_exit=50, time_stop=10, max_concurrent=8):
    """Momentum RSI LONG only."""
    all_prices, all_lots, all_rsi = {}, {}, {}
    sym_to_sector = {}
    
    for sector, syms in SECTORS.items():
        for s in syms:
            sym_to_sector[s] = sector
    
    for sym in sym_to_sector:
        result = get_continuous_prices(sym)
        if result:
            prices, lot = result
            if len(prices) > 30:
                all_prices[sym] = prices
                all_lots[sym] = lot
                all_rsi[sym] = compute_rsi(prices, 14)
    
    all_dates = sorted(set(d for p in all_prices.values() for d in p.index))
    trades, open_positions = [], []
    top_sectors = set(SECTORS.keys())
    
    for i, date in enumerate(all_dates[30:], 30):
        # Exits
        to_close = []
        for pi, (sym, entry_date, entry_price, lot_size) in enumerate(open_positions):
            if date not in all_rsi.get(sym, pd.Series()).index:
                continue
            rsi = all_rsi[sym].get(date, 50)
            price = all_prices[sym].get(date, entry_price)
            days = (date - entry_date).days
            
            if rsi >= rsi_exit or days >= time_stop:
                pnl_pct = (price - entry_price) / entry_price * 100
                pnl_rs = (pnl_pct / 100) * entry_price * lot_size
                trades.append({
                    'strategy': 'MOMENTUM_LONG',
                    'symbol': sym,
                    'entry_date': entry_date,
                    'exit_date': date,
                    'pnl_pct': round(pnl_pct, 4),
                    'pnl_rupees': round(pnl_rs, 0),
                    'days_held': days,
                })
                to_close.append(pi)
        
        for idx in sorted(to_close, reverse=True):
            open_positions.pop(idx)
        
        if len(open_positions) >= max_concurrent:
            continue
        
        # Sector momentum (weekly)
        if i % 5 == 0:
            sr = {}
            for sn, syms in SECTORS.items():
                rets = []
                for sym in syms:
                    if sym in all_prices:
                        ia = all_prices[sym].index.get_indexer([date], method='ffill')
                        if ia[0] >= 20:
                            c, p = all_prices[sym].iloc[ia[0]], all_prices[sym].iloc[ia[0]-20]
                            if p > 0:
                                rets.append((c-p)/p)
                if rets:
                    sr[sn] = np.mean(rets)
            top_sectors = set(s for s, _ in sorted(sr.items(), key=lambda x: -x[1])[:4])
        
        # Entries
        open_syms = set(s for s, _, _, _ in open_positions)
        for sym in sym_to_sector:
            if sym in open_syms or sym not in all_rsi or date not in all_rsi[sym].index:
                continue
            if len(open_positions) >= max_concurrent:
                break
            rsi = all_rsi[sym].get(date, 50)
            if not np.isnan(rsi) and rsi < rsi_buy and sym_to_sector.get(sym) in top_sectors:
                ep = all_prices[sym].get(date)
                if ep and ep > 0:
                    open_positions.append((sym, date, ep, all_lots.get(sym, 1)))
    
    return pd.DataFrame(trades)


# ═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_portfolio(all_trades, initial_capital, kelly_pct_of_full):
    """Simulate combined portfolio with Kelly compounding."""
    strategy_stats = {}
    for strat in all_trades['strategy'].unique():
        sub = all_trades[all_trades['strategy'] == strat]
        wr = (sub['pnl_pct'] > 0).mean()
        avg_w = sub[sub['pnl_pct'] > 0]['pnl_pct'].mean() / 100 if (sub['pnl_pct'] > 0).any() else 0
        avg_l = abs(sub[sub['pnl_pct'] <= 0]['pnl_pct'].mean() / 100) if (sub['pnl_pct'] <= 0).any() else 0.01
        kf, _, edge = kelly_fraction(wr, avg_w, avg_l)
        strategy_stats[strat] = {'kelly': min(kf * kelly_pct_of_full, 0.40), 'wr': wr, 'edge': edge}
    
    trades_sorted = all_trades.sort_values('exit_date').reset_index(drop=True)
    capital = initial_capital
    peak = initial_capital
    max_dd = 0
    monthly = {}
    strat_pnl = defaultdict(float)
    
    for _, t in trades_sorted.iterrows():
        s = t['strategy']
        if s not in strategy_stats:
            continue
        k = strategy_stats[s]['kelly']
        pnl = capital * k * (t['pnl_pct'] / 100)
        capital += pnl
        peak = max(peak, capital)
        max_dd = max(max_dd, (peak - capital) / peak if peak > 0 else 0)
        strat_pnl[s] += pnl
        m = t['exit_date'].strftime('%Y-%m')
        monthly[m] = monthly.get(m, 0) + pnl
    
    years = max((trades_sorted['exit_date'].max() - trades_sorted['entry_date'].min()).days / 365.25, 0.5)
    cagr = (capital / initial_capital) ** (1 / years) - 1 if capital > 0 else 0
    ms = pd.Series(monthly)
    sharpe = (ms.mean() / ms.std()) * np.sqrt(12) if len(ms) > 1 and ms.std() > 0 else 0
    
    return {
        'cagr': cagr, 'final': capital, 'max_dd': max_dd, 'sharpe': sharpe,
        'years': years, 'trades': len(trades_sorted),
        'strat_pnl': dict(strat_pnl), 'strat_stats': strategy_stats, 'monthly': monthly,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=float, default=1000000)
    args = parser.parse_args()
    cap = args.capital
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY v3 — HONEST DEEP BACKTEST".ljust(79) + "║")
    print(f"║  All data validated. No garbage. No inflated numbers.".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    # ── Strategy 1: Expiry Convergence (Clean) ──────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 1: EXPIRY CONVERGENCE (CLEANED)")
    print(f"  Filters: Fut/Spot 0.95-1.05 per row | Premium 0.3-3.0% | 80%+ valid rows")
    print(f"{'━'*80}")
    
    ec, valid_n, skip_n = backtest_expiry_clean(threshold_pct=0.3, entry_days=5)
    
    print(f"  Data: {valid_n} valid symbols, {skip_n} rejected (bad underlying)")
    
    if not ec.empty:
        w = ec[ec['pnl_pct'] > 0]
        l = ec[ec['pnl_pct'] <= 0]
        print(f"  Trades:     {len(ec)}")
        print(f"  Win Rate:   {len(w)/len(ec)*100:.1f}%")
        print(f"  Avg Return: {ec['pnl_pct'].mean():+.4f}%")
        print(f"  Avg Win:    {w['pnl_pct'].mean():+.4f}%" if len(w) > 0 else "")
        print(f"  Avg Loss:   {l['pnl_pct'].mean():+.4f}%" if len(l) > 0 else "")
        print(f"  Max Win:    {ec['pnl_pct'].max():+.4f}%")
        print(f"  Max Loss:   {ec['pnl_pct'].min():+.4f}%")
        print(f"  Total P&L:  ₹{ec['pnl_rupees'].sum():,.0f}")
        
        # Premium distribution sanity check
        print(f"\n  Premium distribution:")
        print(f"    Entry: {ec['entry_premium'].min():+.3f}% to {ec['entry_premium'].max():+.3f}% (median {ec['entry_premium'].median():+.3f}%)")
        print(f"    Exit:  {ec['exit_premium'].min():+.3f}% to {ec['exit_premium'].max():+.3f}% (median {ec['exit_premium'].median():+.3f}%)")
    
    # ── Strategy 2: Pair Trading ────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 2: PAIR TRADING (15 near-miss pairs)")
    print(f"  Uses price ratios only — no underlying data needed")
    print(f"{'━'*80}")
    
    pairs, pair_stats = backtest_pairs(window=30, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=20)
    
    if not pairs.empty:
        w = pairs[pairs['pnl_pct'] > 0]
        l = pairs[pairs['pnl_pct'] <= 0]
        print(f"  Trades:     {len(pairs)}")
        print(f"  Win Rate:   {len(w)/len(pairs)*100:.1f}%")
        print(f"  Avg Return: {pairs['pnl_pct'].mean():+.4f}%")
        print(f"  Avg Win:    {w['pnl_pct'].mean():+.4f}%" if len(w) > 0 else "")
        print(f"  Avg Loss:   {l['pnl_pct'].mean():+.4f}%" if len(l) > 0 else "")
        print(f"  Total P&L:  ₹{pairs['pnl_rupees'].sum():,.0f}")
        
        print(f"\n  Per-pair breakdown:")
        print(f"  {'Pair':<25} {'Trades':>6} {'WR':>6} {'Avg%':>8} {'P&L':>12}")
        print(f"  {'─'*60}")
        for ps in sorted(pair_stats, key=lambda x: -x['total_rs']):
            print(f"  {ps['pair']:<25} {ps['trades']:>6} {ps['wr']*100:>5.1f}% {ps['avg']:>+7.3f}% ₹{ps['total_rs']:>10,.0f}")
    
    # ── Strategy 3: Momentum LONG ──────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 3: MOMENTUM RSI (LONG ONLY)")
    print(f"  RSI < 30 in top-4 momentum sectors | Exit RSI 50 or 10d stop")
    print(f"{'━'*80}")
    
    mom = backtest_momentum_long(rsi_buy=30, rsi_exit=50, time_stop=10, max_concurrent=8)
    
    if not mom.empty:
        w = mom[mom['pnl_pct'] > 0]
        l = mom[mom['pnl_pct'] <= 0]
        print(f"  Trades:     {len(mom)}")
        print(f"  Win Rate:   {len(w)/len(mom)*100:.1f}%")
        print(f"  Avg Return: {mom['pnl_pct'].mean():+.4f}%")
        print(f"  Avg Win:    {w['pnl_pct'].mean():+.4f}%" if len(w) > 0 else "")
        print(f"  Avg Loss:   {l['pnl_pct'].mean():+.4f}%" if len(l) > 0 else "")
        print(f"  Total P&L:  ₹{mom['pnl_rupees'].sum():,.0f}")
    
    # ── COMBINED PORTFOLIO ──────────────────────────────────────────────────
    all_trades = pd.concat([ec, pairs, mom]).dropna(subset=['entry_date', 'exit_date'])
    
    print(f"\n{'═'*80}")
    print(f"  COMBINED PORTFOLIO — KELLY GRID SEARCH")
    print(f"  Capital: ₹{cap:,.0f} | Total trades: {len(all_trades)}")
    print(f"{'═'*80}")
    
    print(f"\n  {'Kelly':>8} {'CAGR':>7} {'Return':>8} {'MaxDD':>7} {'Sharpe':>7} {'Final':>12}")
    print(f"  {'─'*55}")
    
    for kpct in [0.50, 0.60, 0.70, 0.75, 0.80, 0.90, 1.00, 1.25]:
        r = simulate_portfolio(all_trades, cap, kpct)
        marker = " ◄" if r['cagr'] >= 0.60 else ""
        print(f"  {kpct:>7.0%} {r['cagr']:>6.1%} {(r['final']-cap)/cap:>7.1%} "
              f"{r['max_dd']:>6.1%} {r['sharpe']:>6.2f} ₹{r['final']:>10,.0f}{marker}")
    
    # ── BEST RESULT DETAIL ──────────────────────────────────────────────────
    # Use 75% Kelly as the balanced choice
    best = simulate_portfolio(all_trades, cap, 0.75)
    
    print(f"\n{'═'*80}")
    print(f"  HONEST RESULT @ 75% KELLY")
    print(f"{'═'*80}")
    print(f"  CAGR:           {best['cagr']:.1%}")
    print(f"  ₹{cap/100000:.0f}L →        ₹{best['final']:,.0f}")
    print(f"  Max Drawdown:   {best['max_dd']:.1%}")
    print(f"  Sharpe:         {best['sharpe']:.2f}")
    print(f"  Period:         {best['years']:.1f} years")
    print(f"  Total Trades:   {best['trades']}")
    print(f"  Trades/Year:    {best['trades']/best['years']:.0f}")
    
    print(f"\n  Strategy P&L:")
    for s, pnl in sorted(best['strat_pnl'].items(), key=lambda x: -x[1]):
        info = best['strat_stats'][s]
        print(f"    {s:<16} ₹{pnl:>+12,.0f}  Kelly={info['kelly']:.1%}  WR={info['wr']:.1%}  Edge={info['edge']*100:+.3f}%")
    
    print(f"\n  Monthly P&L (last 12):")
    for m in sorted(best['monthly'].keys())[-12:]:
        v = best['monthly'][m]
        bar = "█" * max(1, int(abs(v) / 5000))
        print(f"    {m}: ₹{v:>+10,.0f} {bar}")
    
    # Save
    all_trades.to_csv('.tmp/honest_backtest_trades.csv', index=False)
    print(f"\n  All trades saved to .tmp/honest_backtest_trades.csv")
    print(f"\n{'═'*80}")


if __name__ == "__main__":
    main()
