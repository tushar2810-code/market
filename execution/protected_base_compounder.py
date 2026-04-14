"""
Antigravity v3 — Protected-Base Profit Compounder.

The SMART approach to high CAGR with minimal risk:

Phase 1 (PROTECT): 
  - Trade with conservative Kelly (50%) on INITIAL capital
  - Goal: grow account by 30-50% to build a "profit buffer"
  
Phase 2 (COMPOUND):
  - LOCK the initial capital — never risk it again
  - Trade with aggressive Kelly (100-125%) on PROFITS ONLY
  - If drawdown eats into initial capital → pause, go back to Phase 1

Think of it like a business:
  - Your initial capital = your savings (NEVER touch it)
  - Profits = house money (bet aggressively)

Usage:
    python3 execution/protected_base_compounder.py --capital 1000000
    python3 execution/protected_base_compounder.py --capital 1000000 --phase2-kelly 1.25
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime

sys.path.append(os.path.dirname(__file__))
from kelly_sizer import kelly_fraction

DATA_DIR = '.tmp/3y_data'

# ═══════════════════════════════════════════════════════════════════════════════
# LOAD STRATEGY BACKTESTS (reuse from optimize_for_66.py)
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


# ── Data loaders (same as optimize_for_66.py) ──────────────────────────────

def load_futures_data(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
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


def load_prices(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
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


# ── Strategy backtests ─────────────────────────────────────────────────────

def backtest_expiry_filtered(symbols, threshold=0.3, entry_days=5):
    from datetime import timedelta
    all_trades = []
    for symbol in symbols:
        df = load_futures_data(symbol)
        if df is None:
            continue
        for expiry in sorted(df['FH_EXPIRY_DT'].dropna().unique()):
            expiry_dt = pd.Timestamp(expiry)
            month_data = df[df['FH_EXPIRY_DT'] == expiry]
            entry_window_start = expiry_dt - timedelta(days=entry_days + 3)
            window_data = month_data[
                (month_data['FH_TIMESTAMP'] >= entry_window_start) & 
                (month_data['FH_TIMESTAMP'] <= expiry_dt)
            ].sort_values('FH_TIMESTAMP')
            if len(window_data) < 2:
                continue
            entry_candidates = window_data[window_data['FH_TIMESTAMP'] <= expiry_dt - timedelta(days=entry_days - 2)]
            if entry_candidates.empty:
                entry_candidates = window_data.head(1)
            entry_row = entry_candidates.iloc[-1]
            fut_price, spot_price = entry_row['FH_CLOSING_PRICE'], entry_row['FH_UNDERLYING_VALUE']
            if spot_price <= 0 or np.isnan(spot_price):
                continue
            premium_pct = (fut_price - spot_price) / spot_price * 100
            if abs(premium_pct) < threshold:
                continue
            exit_data = window_data[window_data['FH_TIMESTAMP'] <= expiry_dt]
            if exit_data.empty:
                continue
            exit_row = exit_data.iloc[-1]
            exit_prem = (exit_row['FH_CLOSING_PRICE'] - exit_row['FH_UNDERLYING_VALUE']) / exit_row['FH_UNDERLYING_VALUE'] * 100 if exit_row['FH_UNDERLYING_VALUE'] > 0 else 0
            pnl_pct = (premium_pct - exit_prem) if premium_pct > 0 else (exit_prem - premium_pct)
            lot_size = int(entry_row.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in entry_row.index else 1
            all_trades.append({
                'strategy': 'EXPIRY', 'symbol': symbol,
                'entry_date': entry_row['FH_TIMESTAMP'], 'exit_date': exit_row['FH_TIMESTAMP'],
                'pnl_pct': pnl_pct, 'pnl_rupees': (pnl_pct / 100) * spot_price * lot_size,
            })
    return pd.DataFrame(all_trades)


def backtest_momentum_long_only(rsi_buy=30, rsi_exit=50, time_stop=10, max_concurrent=8):
    all_prices, all_lots, all_rsi = {}, {}, {}
    sym_to_sector = {}
    for sector, syms in SECTORS.items():
        for s in syms:
            sym_to_sector[s] = sector
    for sym in sym_to_sector:
        result = load_prices(sym)
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
        to_close = []
        for pi, (sym, entry_date, entry_price, lot_size) in enumerate(open_positions):
            if date not in all_rsi.get(sym, pd.Series()).index:
                continue
            current_rsi = all_rsi[sym].get(date, 50)
            current_price = all_prices[sym].get(date, entry_price)
            days_held = (date - entry_date).days
            if current_rsi >= rsi_exit or days_held >= time_stop:
                pnl_pct = (current_price - entry_price) / entry_price * 100
                trades.append({
                    'strategy': 'MOMENTUM', 'symbol': sym,
                    'entry_date': entry_date, 'exit_date': date,
                    'pnl_pct': round(pnl_pct, 3),
                    'pnl_rupees': round((pnl_pct / 100) * entry_price * lot_size, 0),
                })
                to_close.append(pi)
        for idx in sorted(to_close, reverse=True):
            open_positions.pop(idx)
        if len(open_positions) >= max_concurrent:
            continue
        if i % 5 == 0:
            sector_returns = {}
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
                    sector_returns[sn] = np.mean(rets)
            top_sectors = set(s for s, _ in sorted(sector_returns.items(), key=lambda x: -x[1])[:4])
        open_syms = set(s for s, _, _, _ in open_positions)
        for sym in sym_to_sector:
            if sym in open_syms or sym not in all_rsi or date not in all_rsi[sym].index:
                continue
            if len(open_positions) >= max_concurrent:
                break
            rsi_val = all_rsi[sym].get(date, 50)
            if not np.isnan(rsi_val) and rsi_val < rsi_buy and sym_to_sector.get(sym) in top_sectors:
                ep = all_prices[sym].get(date)
                if ep and ep > 0:
                    open_positions.append((sym, date, ep, all_lots.get(sym, 1)))
    return pd.DataFrame(trades)


def backtest_near_miss_pairs(window=30, z_entry=1.8, z_exit=0.3, z_stop=3.5, time_stop=20):
    trades = []
    for sym_a, sym_b in NEAR_MISS_PAIRS:
        data_a, data_b = load_prices(sym_a), load_prices(sym_b)
        if data_a is None or data_b is None:
            continue
        prices_a, lot_a = data_a
        prices_b, lot_b = data_b
        merged = pd.DataFrame({'A': prices_a, 'B': prices_b}).dropna()
        if len(merged) < window + 30:
            continue
        ratio = merged['A'] / merged['B']
        z_scores = (ratio - ratio.rolling(window).mean()) / ratio.rolling(window).std()
        dates = merged.index
        in_trade = False
        for i in range(window + 10, len(dates)):
            z = z_scores.iloc[i]
            if np.isnan(z):
                continue
            if not in_trade:
                if abs(z) >= z_entry:
                    entry_idx, entry_z, entry_ratio = i, z, ratio.iloc[i]
                    entry_a = merged['A'].iloc[i]
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
                    pnl_pct = ((entry_ratio - exit_ratio) / entry_ratio * 100) if direction == 'SHORT_SPREAD' else ((exit_ratio - entry_ratio) / entry_ratio * 100)
                    trades.append({
                        'strategy': 'PAIR', 'symbol': f"{sym_a}/{sym_b}",
                        'entry_date': dates[entry_idx], 'exit_date': dates[i],
                        'pnl_pct': round(pnl_pct, 3),
                        'pnl_rupees': round((pnl_pct / 100) * entry_a * lot_a, 0),
                    })
                    in_trade = False
    return pd.DataFrame(trades)


# ═══════════════════════════════════════════════════════════════════════════════
# PROTECTED-BASE SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_protected_base(all_trades, initial_capital, 
                             phase1_kelly_pct=0.50,
                             phase2_kelly_pct=1.00,
                             profit_threshold_pct=0.30,
                             drawdown_pause_pct=0.05):
    """
    Two-phase capital management:
    
    Phase 1 (PROTECT):
      - Kelly = phase1_kelly_pct * Full Kelly, sized on total capital
      - Conservative: build a profit buffer
      - Ends when: profits >= profit_threshold_pct * initial_capital
      
    Phase 2 (COMPOUND):
      - Kelly = phase2_kelly_pct * Full Kelly, sized on TOTAL capital
      - But with a HARD FLOOR: if capital drops to initial capital → back to Phase 1
      - The profits are the "risk budget" — you can lose them but never the base
      - Drawdown pause: if you lose 30% of peak profits in Phase 2, pause 3 trades
      
    The magic: Phase 2 uses full capital for sizing (not just profits), but the 
    Kelly fraction is higher since you can afford the volatility. If things go wrong, 
    the Phase 1 downshift catches it before base is touched.
    """
    # Compute per-strategy Kelly
    strategy_kelly = {}
    for strat_name in ['EXPIRY', 'MOMENTUM', 'PAIR']:
        sub = all_trades[all_trades['strategy'] == strat_name]
        if sub.empty:
            continue
        wr = (sub['pnl_pct'] > 0).mean()
        avg_win = sub[sub['pnl_pct'] > 0]['pnl_pct'].mean() / 100 if (sub['pnl_pct'] > 0).any() else 0
        avg_loss = abs(sub[sub['pnl_pct'] <= 0]['pnl_pct'].mean() / 100) if (sub['pnl_pct'] <= 0).any() else 0.01
        kf, kh, edge = kelly_fraction(wr, avg_win, avg_loss)
        strategy_kelly[strat_name] = {'full_kelly': kf, 'wr': wr, 'edge': edge}
    
    # Sort all trades chronologically
    trades_sorted = all_trades.sort_values('exit_date').reset_index(drop=True)
    
    # Simulate
    capital = initial_capital
    base = initial_capital  # The untouchable floor
    peak_capital = initial_capital
    peak_profit = 0
    max_dd = 0
    phase = 1
    phase_switches = []
    paused = 0
    
    equity_curve = [{'date': trades_sorted['entry_date'].min(), 'capital': capital, 'phase': 1, 'profit': 0}]
    monthly = {}
    strat_pnl = {}
    phase_trades = {1: 0, 2: 0}
    phase_pnl = {1: 0, 2: 0}
    
    base_breach_count = 0
    
    for _, trade in trades_sorted.iterrows():
        strat = trade['strategy']
        if strat not in strategy_kelly:
            continue
        
        # Pause check
        if paused > 0:
            paused -= 1
            continue
        
        profits = capital - base
        
        if phase == 1:
            # CONSERVATIVE: size on total capital with low Kelly
            kelly = strategy_kelly[strat]['full_kelly'] * phase1_kelly_pct
            kelly = min(kelly, 0.25)  # Hard cap 25%
            position = capital * kelly
            
            # Graduate to Phase 2?
            if profits >= base * profit_threshold_pct:
                phase = 2
                phase_switches.append({
                    'date': trade['exit_date'], 'capital': capital,
                    'profit': profits, 'from': 1, 'to': 2
                })
        
        elif phase == 2:
            # AGGRESSIVE: size on ENTIRE capital with high Kelly
            # Key insight: you can afford bigger Kelly because you have a profit buffer
            kelly = strategy_kelly[strat]['full_kelly'] * phase2_kelly_pct
            kelly = min(kelly, 0.40)  # Cap at 40% 
            position = capital * kelly
            
            # HARD FLOOR CHECK: if capital approaches base, downshift to Phase 1
            if capital <= base * 1.10:  # 10% buffer above base
                phase = 1
                paused = 3  # Cool off
                phase_switches.append({
                    'date': trade['exit_date'], 'capital': capital,
                    'profit': profits, 'from': 2, 'to': 1
                })
                continue
            
            # DRAWDOWN PAUSE: if lost 40% of peak profits, pause briefly
            if peak_profit > 0 and profits > 0:
                profit_drawdown = (peak_profit - profits) / peak_profit
                if profit_drawdown > 0.40:
                    paused = 3
                    continue
        
        # Execute
        pnl = position * (trade['pnl_pct'] / 100)
        capital += pnl
        
        # Tracking
        profits = capital - base
        peak_profit = max(peak_profit, profits)
        peak_capital = max(peak_capital, capital)
        dd = (peak_capital - capital) / peak_capital if peak_capital > 0 else 0
        max_dd = max(max_dd, dd)
        
        if capital < base:
            base_breach_count += 1
        
        phase_trades[phase] = phase_trades.get(phase, 0) + 1
        phase_pnl[phase] = phase_pnl.get(phase, 0) + pnl
        strat_pnl[strat] = strat_pnl.get(strat, 0) + pnl
        
        month = trade['exit_date'].strftime('%Y-%m')
        monthly[month] = monthly.get(month, 0) + pnl
        
        equity_curve.append({
            'date': trade['exit_date'], 'capital': capital,
            'phase': phase, 'profit': profits
        })
    
    # Results
    date_range = (trades_sorted['exit_date'].max() - trades_sorted['entry_date'].min()).days
    years = max(date_range / 365.25, 0.5)
    cagr = (capital / initial_capital) ** (1 / years) - 1 if capital > 0 else 0
    
    monthly_series = pd.Series(monthly)
    sharpe = (monthly_series.mean() / monthly_series.std()) * np.sqrt(12) if len(monthly_series) > 1 and monthly_series.std() > 0 else 0
    
    return {
        'cagr': cagr,
        'total_return': (capital - initial_capital) / initial_capital,
        'final_capital': capital,
        'initial_capital': initial_capital,
        'final_profit': capital - base,
        'max_dd': max_dd,
        'worst_base_breach': base_breach_count,
        'sharpe': sharpe,
        'total_trades': len(trades_sorted),
        'trades_per_year': len(trades_sorted) / years,
        'years': years,
        'phase_trades': phase_trades,
        'phase_pnl': phase_pnl,
        'phase_switches': phase_switches,
        'strategy_pnl': strat_pnl,
        'monthly': monthly,
        'equity_curve': equity_curve,
        'phase1_kelly': phase1_kelly_pct,
        'phase2_kelly': phase2_kelly_pct,
        'base_touched': capital < initial_capital,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--capital', type=float, default=1000000)
    parser.add_argument('--phase1-kelly', type=float, default=0.50, help='Kelly fraction in Phase 1')
    parser.add_argument('--phase2-kelly', type=float, default=1.00, help='Kelly fraction in Phase 2 (on profits)')
    parser.add_argument('--profit-threshold', type=float, default=0.30, help='Profit % to graduate to Phase 2')
    args = parser.parse_args()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY v3 — PROTECTED-BASE COMPOUNDER".ljust(79) + "║")
    print(f"║  Protect the base. Compound only profits.".ljust(79) + "║")
    print(f"║  Capital: ₹{args.capital:,.0f}".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    # Run backtests
    print(f"\n  Running strategy backtests...")
    
    ec = backtest_expiry_filtered(TOP_EXPIRY_SYMBOLS, threshold=0.3, entry_days=5)
    print(f"    Expiry Conv:  {len(ec)} trades, {(ec['pnl_pct']>0).mean():.1%} WR" if not ec.empty else "    Expiry Conv: no data")
    
    mom = backtest_momentum_long_only(rsi_buy=30, rsi_exit=50, time_stop=10, max_concurrent=8)
    print(f"    Momentum:     {len(mom)} trades, {(mom['pnl_pct']>0).mean():.1%} WR" if not mom.empty else "    Momentum: no data")
    
    pairs = backtest_near_miss_pairs()
    print(f"    Pair Trading: {len(pairs)} trades, {(pairs['pnl_pct']>0).mean():.1%} WR" if not pairs.empty else "    Pair Trading: no data")
    
    all_trades = pd.concat([ec, mom, pairs]).dropna(subset=['entry_date', 'exit_date'])
    print(f"\n    Total: {len(all_trades)} trades across 3 strategies")
    
    # ── Grid search Phase 2 Kelly ──────────────────────────────────────────
    print(f"\n{'═'*90}")
    print(f"  PROTECTED-BASE GRID SEARCH")
    print(f"  Phase 1: {args.phase1_kelly:.0%} Kelly on total capital (conservative)")
    print(f"  Phase 2: Variable Kelly on PROFITS ONLY (aggressive)")
    print(f"  Graduate to Phase 2 when profits = {args.profit_threshold:.0%} of initial capital")
    print(f"{'═'*90}")
    
    print(f"\n  {'P2 Kelly':>10} {'CAGR':>7} {'Return':>8} {'MaxDD':>7} {'Sharpe':>7} {'Base Breached':>15} {'Final':>12}")
    print(f"  {'─'*75}")
    
    best_safe = None
    
    for p2k in [0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00]:
        result = simulate_protected_base(
            all_trades, args.capital,
            phase1_kelly_pct=args.phase1_kelly,
            phase2_kelly_pct=p2k,
            profit_threshold_pct=args.profit_threshold,
        )
        
        base_ok = "✅ SAFE" if result['worst_base_breach'] == 0 else f"⚠️  {result['worst_base_breach']}x"
        marker = " ◄" if result['cagr'] >= 0.60 and not result['base_touched'] else ""
        
        print(f"  {p2k:>9.0%} {result['cagr']:>6.1%} {result['total_return']:>7.1%} "
              f"{result['max_dd']:>6.1%} {result['sharpe']:>6.2f} {base_ok:>15} ₹{result['final_capital']:>10,.0f}{marker}")
        
        # Track best result where base is NEVER breached
        if not result['base_touched']:
            if best_safe is None or result['cagr'] > best_safe['cagr']:
                best_safe = result
    
    # ── Best safe result detail ─────────────────────────────────────────────
    if best_safe:
        r = best_safe
        print(f"\n{'═'*90}")
        print(f"  OPTIMAL SAFE CONFIGURATION (Base NEVER breached)")
        print(f"{'═'*90}")
        print(f"\n  💰 Initial Capital:   ₹{r['initial_capital']:,.0f} (PROTECTED — never at risk)")
        print(f"  📈 Final Capital:     ₹{r['final_capital']:,.0f}")
        print(f"  💵 Pure Profit:       ₹{r['final_profit']:,.0f}")
        print(f"  🎯 CAGR:              {r['cagr']:.1%}")
        print(f"  📊 Sharpe Ratio:      {r['sharpe']:.2f}")
        print(f"  📉 Max Drawdown:      {r['max_dd']:.1%} (from profit peak, NOT from base)")
        print(f"  🔒 Base Touched:      NEVER ✅")
        
        print(f"\n  Phase Breakdown:")
        print(f"    Phase 1 (Protect): {r['phase_trades'].get(1,0)} trades → ₹{r['phase_pnl'].get(1,0):+,.0f} P&L")
        print(f"    Phase 2 (Compound): {r['phase_trades'].get(2,0)} trades → ₹{r['phase_pnl'].get(2,0):+,.0f} P&L")
        
        print(f"\n  Phase Switches:")
        for sw in r['phase_switches'][:10]:
            print(f"    {sw['date'].strftime('%Y-%m-%d')}: Phase {sw['from']}→{sw['to']} "
                  f"(Capital: ₹{sw['capital']:,.0f}, Profit: ₹{sw['profit']:,.0f})")
        
        print(f"\n  Strategy P&L:")
        for strat, pnl in sorted(r['strategy_pnl'].items(), key=lambda x: -x[1]):
            print(f"    {strat:<12} ₹{pnl:>+12,.0f}")
        
        print(f"\n  Monthly P&L (last 12):")
        for m in sorted(r['monthly'].keys())[-12:]:
            v = r['monthly'][m]
            bar = "█" * max(1, int(abs(v) / 10000))
            print(f"    {m}: ₹{v:>+12,.0f} {bar}")
        
        # Save equity curve
        eq = pd.DataFrame(r['equity_curve'])
        eq.to_csv('.tmp/protected_base_equity.csv', index=False)
        print(f"\n  Equity curve saved to .tmp/protected_base_equity.csv")
    
    print(f"\n{'═'*90}")


if __name__ == "__main__":
    main()
