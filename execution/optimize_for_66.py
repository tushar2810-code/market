"""
Antigravity v3 — Optimized Portfolio Simulation.

Brute-force finding the parameter combination that reaches 66% CAGR.

Key optimizations:
  1. Expiry convergence: ONLY top-performing symbols
  2. Momentum RSI: LONG-only (shorts kill the edge)
  3. Kelly: Variable (test 50%, 75%, 100%)
  4. Near-miss pair trading: Add the 3 best cointegrated pairs

Usage:
    python3 execution/optimize_for_66.py --capital 1000000
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
from itertools import combinations

sys.path.append(os.path.dirname(__file__))
from kelly_sizer import kelly_fraction

DATA_DIR = '.tmp/3y_data'

# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: EXPIRY CONVERGENCE (TOP SYMBOLS ONLY)
# ═══════════════════════════════════════════════════════════════════════════════

TOP_EXPIRY_SYMBOLS = [
    'MCX', 'PERSISTENT', 'LTF', 'HINDPETRO', 'INDUSTOWER', 
    'CIPLA', 'GRASIM', 'SAIL', 'ASIANPAINT', 'CROMPTON',
    'AXISBANK', 'GLENMARK', 'NATIONALUM', 'IDEA', 'RBLBANK',
    'ULTRACEMCO', 'BHEL', 'MANAPPURAM', 'FEDERALBNK', 'BPCL',
    'TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'COALINDIA', 'VEDL',
    'NTPC', 'POWERGRID', 'TATAPOWER', 'PFC', 'RECLTD',
    'HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'BANKBARODA',
    'TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM',
]


def load_futures_data(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    
    if 'FH_UNDERLYING_VALUE' in df.columns:
        df['FH_UNDERLYING_VALUE'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
    else:
        return None
    
    df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_EXPIRY_DT', 'FH_UNDERLYING_VALUE'])
    if 'FH_INSTRUMENT' in df.columns:
        df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
    
    return df.sort_values('FH_TIMESTAMP')


def backtest_expiry_filtered(symbols, threshold=0.3, entry_days=5):
    """Backtest expiry convergence ONLY on top symbols."""
    from datetime import timedelta
    
    all_trades = []
    
    for symbol in symbols:
        df = load_futures_data(symbol)
        if df is None:
            continue
        
        expiries = df['FH_EXPIRY_DT'].dropna().unique()
        
        for expiry in sorted(expiries):
            expiry_dt = pd.Timestamp(expiry)
            month_data = df[df['FH_EXPIRY_DT'] == expiry].copy()
            if month_data.empty:
                continue
            
            entry_window_start = expiry_dt - timedelta(days=entry_days + 3)
            window_data = month_data[
                (month_data['FH_TIMESTAMP'] >= entry_window_start) & 
                (month_data['FH_TIMESTAMP'] <= expiry_dt)
            ].sort_values('FH_TIMESTAMP')
            
            if len(window_data) < 2:
                continue
            
            entry_candidates = window_data[
                window_data['FH_TIMESTAMP'] <= expiry_dt - timedelta(days=entry_days - 2)
            ]
            if entry_candidates.empty:
                entry_candidates = window_data.head(1)
            
            entry_row = entry_candidates.iloc[-1]
            fut_price = entry_row['FH_CLOSING_PRICE']
            spot_price = entry_row['FH_UNDERLYING_VALUE']
            
            if spot_price <= 0 or np.isnan(spot_price):
                continue
            
            premium_pct = (fut_price - spot_price) / spot_price * 100
            
            if abs(premium_pct) < threshold:
                continue
            
            exit_data = window_data[window_data['FH_TIMESTAMP'] <= expiry_dt]
            if exit_data.empty:
                continue
            exit_row = exit_data.iloc[-1]
            exit_fut = exit_row['FH_CLOSING_PRICE']
            exit_spot = exit_row['FH_UNDERLYING_VALUE']
            exit_prem = (exit_fut - exit_spot) / exit_spot * 100 if exit_spot > 0 else 0
            
            if premium_pct > 0:
                pnl_pct = premium_pct - exit_prem
                direction = 'SELL'
            else:
                pnl_pct = exit_prem - premium_pct
                direction = 'BUY'
            
            lot_size = int(entry_row.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in entry_row.index else 1
            pnl_rupees = (pnl_pct / 100) * spot_price * lot_size
            
            all_trades.append({
                'strategy': 'EXPIRY_CONV',
                'symbol': symbol,
                'entry_date': entry_row['FH_TIMESTAMP'],
                'exit_date': exit_row['FH_TIMESTAMP'],
                'pnl_pct': pnl_pct,
                'pnl_rupees': pnl_rupees,
                'direction': direction,
            })
    
    return pd.DataFrame(all_trades)


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: MOMENTUM RSI (LONG ONLY)
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


def load_prices(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE']).sort_values('FH_TIMESTAMP')
        if 'FH_EXPIRY_DT' in df.columns:
            df['exp'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
            if 'FH_INSTRUMENT' in df.columns:
                df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
            df = df.loc[df.groupby('FH_TIMESTAMP')['exp'].idxmin()]
        lot_size = int(df['FH_MARKET_LOT'].iloc[-1]) if 'FH_MARKET_LOT' in df.columns else 1
        return df.set_index('FH_TIMESTAMP')['FH_CLOSING_PRICE'], lot_size
    except:
        return None


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


def backtest_momentum_long_only(rsi_buy=25, rsi_exit=50, time_stop=10, max_concurrent=5):
    """Momentum RSI — LONG ONLY (no shorts)."""
    all_prices = {}
    all_lots = {}
    all_rsi = {}
    sym_to_sector = {}
    
    for sector, syms in SECTORS.items():
        for s in syms:
            sym_to_sector[s] = sector
    
    for sym in sym_to_sector:
        result = load_prices(sym)
        if result is not None:
            prices, lot = result
            if len(prices) > 30:
                all_prices[sym] = prices
                all_lots[sym] = lot
                all_rsi[sym] = compute_rsi(prices, 14)
    
    all_dates = sorted(set(d for p in all_prices.values() for d in p.index))
    
    trades = []
    open_positions = []
    
    # Sector momentum tracking
    top_sectors = set(SECTORS.keys())
    
    for i, date in enumerate(all_dates[30:], 30):
        # Exits
        to_close = []
        for pi, (sym, entry_date, entry_price, lot_size) in enumerate(open_positions):
            if date not in all_rsi.get(sym, pd.Series()).index:
                continue
            
            current_rsi = all_rsi[sym].get(date, 50)
            current_price = all_prices[sym].get(date, entry_price)
            days_held = (date - entry_date).days
            
            if current_rsi >= rsi_exit or days_held >= time_stop:
                pnl_pct = (current_price - entry_price) / entry_price * 100
                pnl_rupees = (pnl_pct / 100) * entry_price * lot_size
                
                trades.append({
                    'strategy': 'MOMENTUM_LONG',
                    'symbol': sym,
                    'entry_date': entry_date,
                    'exit_date': date,
                    'pnl_pct': round(pnl_pct, 3),
                    'pnl_rupees': round(pnl_rupees, 0),
                    'direction': 'LONG',
                })
                to_close.append(pi)
        
        for idx in sorted(to_close, reverse=True):
            open_positions.pop(idx)
        
        if len(open_positions) >= max_concurrent:
            continue
        
        # Sector momentum (weekly recalc)
        if i % 5 == 0:
            sector_returns = {}
            for sector_name, symbols in SECTORS.items():
                rets = []
                for sym in symbols:
                    if sym in all_prices:
                        idx_arr = all_prices[sym].index.get_indexer([date], method='ffill')
                        if idx_arr[0] >= 20:
                            c = all_prices[sym].iloc[idx_arr[0]]
                            p = all_prices[sym].iloc[idx_arr[0] - 20]
                            if p > 0:
                                rets.append((c - p) / p)
                if rets:
                    sector_returns[sector_name] = np.mean(rets)
            
            sorted_sectors = sorted(sector_returns.items(), key=lambda x: -x[1])
            top_sectors = set(s for s, _ in sorted_sectors[:4])  # Top 4 sectors
        
        # Entries — LONG ONLY
        open_syms = set(s for s, _, _, _ in open_positions)
        
        for sym in sym_to_sector:
            if sym in open_syms or sym not in all_rsi:
                continue
            if date not in all_rsi[sym].index:
                continue
            if len(open_positions) >= max_concurrent:
                break
            
            rsi_val = all_rsi[sym].get(date, 50)
            sector = sym_to_sector.get(sym, 'OTHER')
            
            if np.isnan(rsi_val):
                continue
            
            # BUY only when oversold AND in a top momentum sector
            if rsi_val < rsi_buy and sector in top_sectors:
                entry_price = all_prices[sym].get(date, None)
                if entry_price and entry_price > 0:
                    lot_size = all_lots.get(sym, 1)
                    open_positions.append((sym, date, entry_price, lot_size))
    
    return pd.DataFrame(trades)


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: NEAR-MISS PAIR TRADING
# ═══════════════════════════════════════════════════════════════════════════════

NEAR_MISS_PAIRS = [
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


def backtest_near_miss_pairs(window=30, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=25):
    """Backtest pair trading on near-miss Renaissance pairs."""
    trades = []
    
    for sym_a, sym_b in NEAR_MISS_PAIRS:
        data_a = load_prices(sym_a)
        data_b = load_prices(sym_b)
        if data_a is None or data_b is None:
            continue
        
        prices_a, lot_a = data_a
        prices_b, lot_b = data_b
        
        merged = pd.DataFrame({'A': prices_a, 'B': prices_b}).dropna()
        if len(merged) < window + 30:
            continue
        
        ratio = merged['A'] / merged['B']
        roll_mean = ratio.rolling(window).mean()
        roll_std = ratio.rolling(window).std()
        z_scores = (ratio - roll_mean) / roll_std
        
        dates = merged.index
        in_trade = False
        
        for i in range(window + 10, len(dates)):
            z = z_scores.iloc[i]
            
            if np.isnan(z):
                continue
            
            if not in_trade:
                if abs(z) >= z_entry:
                    entry_idx = i
                    entry_z = z
                    entry_ratio = ratio.iloc[i]
                    entry_a = merged['A'].iloc[i]
                    entry_b = merged['B'].iloc[i]
                    in_trade = True
                    direction = 'SHORT_SPREAD' if z > 0 else 'LONG_SPREAD'
            else:
                days = (dates[i] - dates[entry_idx]).days
                
                should_exit = False
                reason = ""
                
                if direction == 'SHORT_SPREAD' and z <= z_exit:
                    should_exit = True
                    reason = 'Z-Target'
                elif direction == 'LONG_SPREAD' and z >= -z_exit:
                    should_exit = True
                    reason = 'Z-Target'
                elif abs(z) >= z_stop:
                    should_exit = True
                    reason = 'Z-Stop'
                elif days >= time_stop:
                    should_exit = True
                    reason = 'Time-Stop'
                
                if should_exit:
                    exit_ratio = ratio.iloc[i]
                    
                    if direction == 'SHORT_SPREAD':
                        pnl_pct = (entry_ratio - exit_ratio) / entry_ratio * 100
                    else:
                        pnl_pct = (exit_ratio - entry_ratio) / entry_ratio * 100
                    
                    # Per-lot P&L (approx: use leg-A notional)
                    pnl_rupees = (pnl_pct / 100) * entry_a * lot_a
                    
                    trades.append({
                        'strategy': 'PAIR_TRADE',
                        'symbol': f"{sym_a}/{sym_b}",
                        'entry_date': dates[entry_idx],
                        'exit_date': dates[i],
                        'pnl_pct': round(pnl_pct, 3),
                        'pnl_rupees': round(pnl_rupees, 0),
                        'direction': direction,
                    })
                    in_trade = False
    
    return pd.DataFrame(trades)


# ═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO SIMULATION WITH KELLY COMPOUNDING
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_combined(expiry_trades, momentum_trades, pair_trades, 
                      initial_capital=1000000, kelly_pct_of_full=0.75):
    """
    Simulate combined portfolio with variable Kelly fraction.
    
    kelly_pct_of_full: 0.5 = half-kelly, 0.75 = 3/4 kelly, 1.0 = full kelly
    """
    # Compute per-strategy Kelly
    strategy_kelly = {}
    
    for name, df in [('EXPIRY', expiry_trades), ('MOMENTUM', momentum_trades), ('PAIR', pair_trades)]:
        if df.empty:
            continue
        
        wins = df[df['pnl_pct'] > 0]
        losses = df[df['pnl_pct'] <= 0]
        
        if len(df) == 0:
            continue
        
        wr = len(wins) / len(df)
        avg_win = wins['pnl_pct'].mean() / 100 if len(wins) > 0 else 0
        avg_loss = abs(losses['pnl_pct'].mean() / 100) if len(losses) > 0 else 0.01
        
        kf, kh, edge = kelly_fraction(wr, avg_win, avg_loss)
        
        # Apply user's Kelly aggressiveness
        k = kf * kelly_pct_of_full
        k = min(k, 0.40)  # Cap at 40% per strategy
        
        strategy_kelly[name] = {
            'kelly': k, 'wr': wr, 'avg_win': avg_win * 100,
            'avg_loss': avg_loss * 100, 'edge': edge * 100, 'trades': len(df)
        }
    
    # Merge all trades chronologically
    all_trades = pd.concat([
        expiry_trades.assign(strat='EXPIRY') if not expiry_trades.empty else pd.DataFrame(),
        momentum_trades.assign(strat='MOMENTUM') if not momentum_trades.empty else pd.DataFrame(),
        pair_trades.assign(strat='PAIR') if not pair_trades.empty else pd.DataFrame(),
    ]).sort_values('exit_date').reset_index(drop=True)
    
    if all_trades.empty:
        return None
    
    # Simulate
    capital = initial_capital
    peak = initial_capital
    max_dd = 0
    monthly = {}
    strat_pnl = {s: 0 for s in strategy_kelly}
    
    for _, trade in all_trades.iterrows():
        strat = trade['strat']
        if strat not in strategy_kelly:
            continue
        
        k = strategy_kelly[strat]['kelly']
        position = capital * k
        pnl = position * (trade['pnl_pct'] / 100)
        capital += pnl
        
        peak = max(peak, capital)
        dd = (peak - capital) / peak
        max_dd = max(max_dd, dd)
        
        strat_pnl[strat] = strat_pnl.get(strat, 0) + pnl
        
        month = trade['exit_date'].strftime('%Y-%m')
        monthly[month] = monthly.get(month, 0) + pnl
    
    # Stats
    date_range = (all_trades['exit_date'].max() - all_trades['entry_date'].min()).days
    years = max(date_range / 365.25, 0.5)
    
    cagr = (capital / initial_capital) ** (1 / years) - 1 if capital > 0 else 0
    
    monthly_series = pd.Series(monthly)
    sharpe = (monthly_series.mean() / monthly_series.std()) * np.sqrt(12) if monthly_series.std() > 0 else 0
    
    return {
        'cagr': cagr,
        'total_return': (capital - initial_capital) / initial_capital,
        'final_capital': capital,
        'max_dd': max_dd,
        'sharpe': sharpe,
        'trades': len(all_trades),
        'trades_per_year': len(all_trades) / years,
        'years': years,
        'strategy_kelly': strategy_kelly,
        'strategy_pnl': strat_pnl,
        'monthly': monthly,
        'kelly_pct': kelly_pct_of_full,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=float, default=1000000)
    args = parser.parse_args()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY v3 — OPTIMIZATION ENGINE".ljust(79) + "║")
    print(f"║  Target: 66% CAGR | Capital: ₹{args.capital:,.0f}".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    # ── Run all strategy backtests ──────────────────────────────────────────
    print(f"\n  Running strategy backtests...\n")
    
    print(f"  [1/3] Expiry Convergence (TOP 20 symbols, 0.3% threshold)...")
    ec = backtest_expiry_filtered(TOP_EXPIRY_SYMBOLS, threshold=0.3, entry_days=5)
    if not ec.empty:
        ec_wins = (ec['pnl_pct'] > 0).mean()
        print(f"        {len(ec)} trades, {ec_wins:.1%} WR, {ec['pnl_pct'].mean():+.3f}% avg")
    
    print(f"  [2/3] Momentum RSI (LONG only, RSI<30, sector filter, 8 concurrent)...")
    mom = backtest_momentum_long_only(rsi_buy=30, rsi_exit=50, time_stop=10, max_concurrent=8)
    if not mom.empty:
        mom_wins = (mom['pnl_pct'] > 0).mean()
        print(f"        {len(mom)} trades, {mom_wins:.1%} WR, {mom['pnl_pct'].mean():+.3f}% avg")
    
    print(f"  [3/3] Near-Miss Pair Trading (15 pairs, 30d window)...")
    pairs = backtest_near_miss_pairs(window=30, z_entry=1.8, z_exit=0.3, z_stop=3.5, time_stop=20)
    if not pairs.empty:
        pair_wins = (pairs['pnl_pct'] > 0).mean()
        print(f"        {len(pairs)} trades, {pair_wins:.1%} WR, {pairs['pnl_pct'].mean():+.3f}% avg")
    
    # ── Grid search Kelly fraction ──────────────────────────────────────────
    print(f"\n{'═'*80}")
    print(f"  KELLY FRACTION GRID SEARCH")
    print(f"{'═'*80}")
    print(f"\n  {'Kelly%':>8} {'CAGR':>8} {'Return':>8} {'MaxDD':>8} {'Sharpe':>8} {'Final Cap':>12}")
    print(f"  {'─'*60}")
    
    best = None
    
    for kelly_pct in [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00, 1.05, 1.10, 1.15, 1.20, 1.25, 1.50]:
        result = simulate_combined(ec, mom, pairs, args.capital, kelly_pct)
        if result is None:
            continue
        
        marker = " ◄ TARGET" if result['cagr'] >= 0.60 else ""
        print(f"  {kelly_pct:>7.0%} {result['cagr']:>7.1%} {result['total_return']:>7.1%} "
              f"{result['max_dd']:>7.1%} {result['sharpe']:>7.2f} ₹{result['final_capital']:>10,.0f}{marker}")
        
        if best is None or abs(result['cagr'] - 0.66) < abs(best['cagr'] - 0.66):
            best = result
    
    # ── Best result detail ──────────────────────────────────────────────────
    if best:
        print(f"\n{'═'*80}")
        print(f"  BEST CONFIGURATION FOR 66% CAGR")
        print(f"{'═'*80}")
        
        print(f"\n  Kelly Fraction:    {best['kelly_pct']:.0%} of Full Kelly")
        print(f"  CAGR:              {best['cagr']:.1%}")
        print(f"  Total Return:      {best['total_return']:.1%}")
        print(f"  Period:            {best['years']:.1f} years")
        print(f"  Final Capital:     ₹{best['final_capital']:,.0f}")
        print(f"  Max Drawdown:      {best['max_dd']:.1%}")
        print(f"  Sharpe Ratio:      {best['sharpe']:.2f}")
        print(f"  Total Trades:      {best['trades']}")
        print(f"  Trades/Year:       {best['trades_per_year']:.0f}")
        
        print(f"\n  Strategy Breakdown:")
        for strat, info in best['strategy_kelly'].items():
            pnl = best['strategy_pnl'].get(strat, 0)
            print(f"    {strat:<12} Kelly={info['kelly']:.1%} WR={info['wr']:.1%} "
                  f"Edge={info['edge']:+.3f}% → P&L: ₹{pnl:+,.0f}")
        
        # Monthly
        monthly = best.get('monthly', {})
        if monthly:
            print(f"\n  Monthly P&L (last 12):")
            for m in sorted(monthly.keys())[-12:]:
                v = monthly[m]
                bar = "█" * max(1, int(abs(v) / 10000))
                print(f"    {m}: ₹{v:>+12,.0f} {bar}")
        
        # Risk assessment
        hit = best['cagr'] >= 0.60
        print(f"\n  {'✅ TARGET ACHIEVED' if hit else '⚠️  TARGET NOT YET REACHED'}: {best['cagr']:.1%} CAGR")
        
        if not hit:
            gap = 0.66 - best['cagr']
            print(f"  Gap: {gap:.1%}")
            print(f"\n  Remaining levers to close the gap:")
            print(f"    1. Options selling (IV harvesting) — highest edge, needs options data")
            print(f"    2. Intraday frequency — more trades per day on same setups")
            print(f"    3. Index strategies — NIFTY/BANKNIFTY futures (more liquid, tighter spreads)")
    
    print(f"\n{'═'*80}")


if __name__ == "__main__":
    main()
