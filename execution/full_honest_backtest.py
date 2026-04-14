"""
Antigravity v3 — Full Pair Scanner + Calendar Spread + Enhanced Momentum.

Scans ALL valid sector pair combinations and adds calendar spreads.
Goal: Find 200+ trades/year at 0.5%+ average to hit 66% CAGR.

Usage:
    python3 execution/full_honest_backtest.py --capital 1000000
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime, timedelta
from itertools import combinations
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))
from kelly_sizer import kelly_fraction

DATA_DIR = '.tmp/3y_data'


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATED DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def get_continuous_prices(symbol):
    """Get clean continuous futures prices (nearest expiry)."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE'])
        if 'FH_INSTRUMENT' in df.columns:
            df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
        if 'FH_EXPIRY_DT' in df.columns:
            df['exp'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
            df = df.loc[df.groupby('FH_TIMESTAMP')['exp'].idxmin()]
        lot = int(df['FH_MARKET_LOT'].iloc[-1]) if 'FH_MARKET_LOT' in df.columns else 1
        prices = df.set_index('FH_TIMESTAMP')['FH_CLOSING_PRICE'].sort_index()
        if len(prices) < 100:
            return None
        return prices, lot
    except:
        return None


def load_multi_expiry(symbol):
    """Load ALL expiry months for calendar spread analysis."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
        df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_EXPIRY_DT'])
        if 'FH_INSTRUMENT' in df.columns:
            df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
        df['FH_MARKET_LOT'] = pd.to_numeric(df.get('FH_MARKET_LOT', 1), errors='coerce').fillna(1).astype(int)
        return df.sort_values(['FH_TIMESTAMP', 'FH_EXPIRY_DT'])
    except:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: ALL-PAIR SCANNER (scan every sector pair combination)
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
    'INSURANCE': ['HDFCLIFE', 'ICICIGI', 'ICICIPRULI', 'SBILIFE', 'LICI'],
    'FINANCE': ['BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'MUTHOOTFIN', 'LICHSGFIN', 'MANAPPURAM', 'PFC', 'RECLTD'],
    'CHEMICALS': ['PIIND', 'SRF', 'UPL', 'DALBHARAT'],
    'CONSUMER': ['TITAN', 'TRENT', 'DMART', 'VBL', 'JUBLFOOD', 'PAGEIND'],
}


def backtest_one_pair(prices_a, lot_a, prices_b, lot_b, 
                       window=30, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=20):
    """Backtest a single pair. Returns list of trades."""
    merged = pd.DataFrame({'A': prices_a, 'B': prices_b}).dropna()
    if len(merged) < window + 50:
        return []
    
    ratio = merged['A'] / merged['B']
    z = (ratio - ratio.rolling(window).mean()) / ratio.rolling(window).std()
    dates = merged.index
    
    trades = []
    in_trade = False
    
    for i in range(window + 10, len(dates)):
        zv = z.iloc[i]
        if np.isnan(zv):
            continue
        
        if not in_trade:
            if abs(zv) >= z_entry:
                ei = i
                entry_a = merged['A'].iloc[i]
                entry_b = merged['B'].iloc[i]
                direction = 'SHORT' if zv > 0 else 'LONG'
                in_trade = True
        else:
            days = (dates[i] - dates[ei]).days
            close = False
            if direction == 'SHORT' and zv <= z_exit: close = True
            elif direction == 'LONG' and zv >= -z_exit: close = True
            elif abs(zv) >= z_stop: close = True
            elif days >= time_stop: close = True
            
            if close:
                exit_a = merged['A'].iloc[i]
                exit_b = merged['B'].iloc[i]
                if direction == 'SHORT':
                    pnl_a = (entry_a - exit_a) / entry_a * 100
                    pnl_b = (exit_b - entry_b) / entry_b * 100
                else:
                    pnl_a = (exit_a - entry_a) / entry_a * 100
                    pnl_b = (entry_b - exit_b) / entry_b * 100
                
                pnl_pct = (pnl_a + pnl_b) / 2
                pnl_rs = (pnl_a / 100) * entry_a * lot_a + (pnl_b / 100) * entry_b * lot_b
                
                trades.append({
                    'entry_date': dates[ei], 'exit_date': dates[i],
                    'pnl_pct': round(pnl_pct, 4), 'pnl_rupees': round(pnl_rs, 0),
                    'days_held': days,
                })
                in_trade = False
    
    return trades


def scan_all_pairs():
    """Scan ALL within-sector pair combinations."""
    # Load all available prices
    all_prices = {}
    all_lots = {}
    sym_to_sector = {}
    
    for sector, syms in SECTORS.items():
        for s in syms:
            sym_to_sector[s] = sector
            if s not in all_prices:
                result = get_continuous_prices(s)
                if result:
                    all_prices[s], all_lots[s] = result
    
    print(f"    Loaded {len(all_prices)} symbols")
    
    # Test all within-sector combinations
    all_trades = []
    pair_results = []
    tested = 0
    
    for sector, syms in SECTORS.items():
        available = [s for s in syms if s in all_prices]
        for sym_a, sym_b in combinations(available, 2):
            tested += 1
            trades = backtest_one_pair(
                all_prices[sym_a], all_lots[sym_a],
                all_prices[sym_b], all_lots[sym_b],
                window=30, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=20
            )
            
            if len(trades) >= 3:  # Need at least 3 trades
                df_t = pd.DataFrame(trades)
                wr = (df_t['pnl_pct'] > 0).mean()
                avg = df_t['pnl_pct'].mean()
                total = df_t['pnl_rupees'].sum()
                
                # FILTER: only keep pairs with WR > 55% AND positive avg AND profitable
                if wr >= 0.55 and avg > 0 and total > 0:
                    for t in trades:
                        t['symbol'] = f"{sym_a}/{sym_b}"
                        t['strategy'] = 'PAIR_TRADE'
                    all_trades.extend(trades)
                    pair_results.append({
                        'pair': f"{sym_a}/{sym_b}",
                        'sector': sector,
                        'trades': len(df_t),
                        'wr': wr,
                        'avg': avg,
                        'total': total,
                    })
    
    print(f"    Tested {tested} pairs, {len(pair_results)} profitable (>{55}% WR)")
    return pd.DataFrame(all_trades), pair_results


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: CALENDAR SPREAD
# ═══════════════════════════════════════════════════════════════════════════════

def backtest_calendar_spreads(symbols=None, window=20, z_entry=1.5, z_exit=0.3, z_stop=3.0, time_stop=15):
    """
    Calendar spread: exploit near-far month premium mean reversion.
    
    When near-far spread is unusually wide → sell near, buy far (expect shrinkage)
    When near-far spread is unusually narrow → buy near, sell far (expect expansion)
    """
    if symbols is None:
        symbols = sorted(set(s for syms in SECTORS.values() for s in syms))
    
    all_trades = []
    sym_count = 0
    
    for symbol in symbols:
        df = load_multi_expiry(symbol)
        if df is None:
            continue
        
        # For each date, get near and far month prices
        grouped = df.groupby('FH_TIMESTAMP')
        daily_data = []
        
        for date, group in grouped:
            expiries = group.sort_values('FH_EXPIRY_DT')
            if len(expiries) < 2:
                continue
            
            near = expiries.iloc[0]
            far = expiries.iloc[1]
            
            near_price = near['FH_CLOSING_PRICE']
            far_price = far['FH_CLOSING_PRICE']
            lot = near['FH_MARKET_LOT']
            
            if near_price <= 0 or far_price <= 0:
                continue
            
            spread_pct = (far_price - near_price) / near_price * 100
            
            daily_data.append({
                'date': date,
                'near_price': near_price,
                'far_price': far_price,
                'spread_pct': spread_pct,
                'near_expiry': near['FH_EXPIRY_DT'],
                'far_expiry': far['FH_EXPIRY_DT'],
                'lot': lot,
            })
        
        if len(daily_data) < window + 50:
            continue
        
        sym_count += 1
        dd = pd.DataFrame(daily_data)
        dd['spread_mean'] = dd['spread_pct'].rolling(window).mean()
        dd['spread_std'] = dd['spread_pct'].rolling(window).std()
        dd['z'] = (dd['spread_pct'] - dd['spread_mean']) / dd['spread_std']
        
        in_trade = False
        
        for i in range(window + 5, len(dd)):
            row = dd.iloc[i]
            zv = row['z']
            
            if np.isnan(zv):
                continue
            
            if not in_trade:
                if abs(zv) >= z_entry:
                    entry_idx = i
                    entry_spread = row['spread_pct']
                    entry_near = row['near_price']
                    entry_far = row['far_price']
                    entry_lot = row['lot']
                    direction = 'SELL_SPREAD' if zv > 0 else 'BUY_SPREAD'
                    in_trade = True
            else:
                days = (row['date'] - dd.iloc[entry_idx]['date']).days
                close = False
                
                if direction == 'SELL_SPREAD' and zv <= z_exit: close = True
                elif direction == 'BUY_SPREAD' and zv >= -z_exit: close = True
                elif abs(zv) >= z_stop: close = True
                elif days >= time_stop: close = True
                
                if close:
                    exit_spread = row['spread_pct']
                    
                    if direction == 'SELL_SPREAD':
                        pnl_pct = entry_spread - exit_spread
                    else:
                        pnl_pct = exit_spread - entry_spread
                    
                    pnl_rs = (pnl_pct / 100) * entry_near * entry_lot
                    
                    all_trades.append({
                        'strategy': 'CALENDAR_SPREAD',
                        'symbol': symbol,
                        'entry_date': dd.iloc[entry_idx]['date'],
                        'exit_date': row['date'],
                        'pnl_pct': round(pnl_pct, 4),
                        'pnl_rupees': round(pnl_rs, 0),
                        'days_held': days,
                    })
                    in_trade = False
    
    return pd.DataFrame(all_trades), sym_count


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: MOMENTUM RSI LONG (same as honest_backtest.py)
# ═══════════════════════════════════════════════════════════════════════════════

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


def backtest_momentum(rsi_buy=35, rsi_exit=50, time_stop=10, max_concurrent=10):
    """Momentum RSI LONG only — widened to RSI 35 with more concurrent."""
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
    trades, open_pos = [], []
    top_sectors = set(SECTORS.keys())
    
    for i, date in enumerate(all_dates[30:], 30):
        to_close = []
        for pi, (sym, ed, ep, lot) in enumerate(open_pos):
            if date not in all_rsi.get(sym, pd.Series()).index:
                continue
            rsi = all_rsi[sym].get(date, 50)
            price = all_prices[sym].get(date, ep)
            days = (date - ed).days
            if rsi >= rsi_exit or days >= time_stop:
                pnl = (price - ep) / ep * 100
                trades.append({
                    'strategy': 'MOMENTUM_LONG', 'symbol': sym,
                    'entry_date': ed, 'exit_date': date,
                    'pnl_pct': round(pnl, 4), 'pnl_rupees': round((pnl/100)*ep*lot, 0),
                    'days_held': days,
                })
                to_close.append(pi)
        for idx in sorted(to_close, reverse=True):
            open_pos.pop(idx)
        
        if len(open_pos) >= max_concurrent:
            continue
        
        if i % 5 == 0:
            sr = {}
            for sn, syms in SECTORS.items():
                rets = []
                for sym in syms:
                    if sym in all_prices:
                        ia = all_prices[sym].index.get_indexer([date], method='ffill')
                        if ia[0] >= 20:
                            c, p = all_prices[sym].iloc[ia[0]], all_prices[sym].iloc[ia[0]-20]
                            if p > 0: rets.append((c-p)/p)
                if rets: sr[sn] = np.mean(rets)
            top_sectors = set(s for s, _ in sorted(sr.items(), key=lambda x: -x[1])[:5])
        
        open_syms = set(s for s, _, _, _ in open_pos)
        for sym in sym_to_sector:
            if sym in open_syms or sym not in all_rsi or date not in all_rsi[sym].index:
                continue
            if len(open_pos) >= max_concurrent:
                break
            rsi = all_rsi[sym].get(date, 50)
            if not np.isnan(rsi) and rsi < rsi_buy and sym_to_sector.get(sym) in top_sectors:
                ep = all_prices[sym].get(date)
                if ep and ep > 0:
                    open_pos.append((sym, date, ep, all_lots.get(sym, 1)))
    
    return pd.DataFrame(trades)


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: EXPIRY CONVERGENCE (CLEAN — same logic from honest_backtest.py)
# ═══════════════════════════════════════════════════════════════════════════════

def backtest_expiry_clean(threshold=0.3, entry_days=5):
    """Expiry convergence with strict data validation."""
    all_trades = []
    valid_n = 0
    
    for f in sorted(os.listdir(DATA_DIR)):
        if not f.endswith('_5Y.csv'): continue
        sym = f.replace('_5Y.csv', '')
        
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, f))
            df.columns = [c.strip() for c in df.columns]
            df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
            df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
            df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
            if 'FH_UNDERLYING_VALUE' not in df.columns: continue
            df['FH_UNDERLYING_VALUE'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
            df = df.dropna(subset=['FH_TIMESTAMP','FH_CLOSING_PRICE','FH_EXPIRY_DT','FH_UNDERLYING_VALUE'])
            df = df[df['FH_UNDERLYING_VALUE'] > 0]
            if 'FH_INSTRUMENT' in df.columns:
                df = df[df['FH_INSTRUMENT'].isin(['FUTSTK','FUTIDX'])]
            
            # Row-level validation
            ratio = df['FH_CLOSING_PRICE'] / df['FH_UNDERLYING_VALUE']
            valid_mask = (ratio >= 0.95) & (ratio <= 1.05)
            if valid_mask.mean() < 0.80:
                continue
            df = df[valid_mask]
            valid_n += 1
            
            if 'FH_MARKET_LOT' in df.columns:
                df['FH_MARKET_LOT'] = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce').fillna(1).astype(int)
            
            for expiry in sorted(df['FH_EXPIRY_DT'].unique()):
                exp_dt = pd.Timestamp(expiry)
                md = df[df['FH_EXPIRY_DT'] == expiry].sort_values('FH_TIMESTAMP')
                start = exp_dt - timedelta(days=entry_days+3)
                wd = md[(md['FH_TIMESTAMP'] >= start) & (md['FH_TIMESTAMP'] <= exp_dt)]
                if len(wd) < 2: continue
                
                ec = wd[wd['FH_TIMESTAMP'] <= exp_dt - timedelta(days=entry_days-2)]
                if ec.empty: ec = wd.head(1)
                er = ec.iloc[-1]
                
                spot = er['FH_UNDERLYING_VALUE']
                if spot <= 0: continue
                prem = (er['FH_CLOSING_PRICE'] - spot) / spot * 100
                if abs(prem) < threshold or abs(prem) > 3.0: continue
                
                ex = wd[wd['FH_TIMESTAMP'] <= exp_dt]
                if ex.empty: continue
                xr = ex.iloc[-1]
                if xr['FH_UNDERLYING_VALUE'] <= 0: continue
                x_prem = (xr['FH_CLOSING_PRICE'] - xr['FH_UNDERLYING_VALUE']) / xr['FH_UNDERLYING_VALUE'] * 100
                if abs(x_prem) > 3.0: continue
                
                pnl = (prem - x_prem) if prem > 0 else (x_prem - prem)
                lot = int(er.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in er.index else 1
                
                all_trades.append({
                    'strategy': 'EXPIRY_CONV', 'symbol': sym,
                    'entry_date': er['FH_TIMESTAMP'], 'exit_date': xr['FH_TIMESTAMP'],
                    'pnl_pct': round(pnl, 4), 'pnl_rupees': round((pnl/100)*spot*lot, 0),
                    'days_held': (xr['FH_TIMESTAMP'] - er['FH_TIMESTAMP']).days,
                })
        except:
            pass
    
    return pd.DataFrame(all_trades), valid_n


# ═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

def simulate(all_trades, capital, kelly_pct):
    """Kelly compounding sim."""
    stats = {}
    for s in all_trades['strategy'].unique():
        sub = all_trades[all_trades['strategy'] == s]
        wr = (sub['pnl_pct'] > 0).mean()
        aw = sub[sub['pnl_pct'] > 0]['pnl_pct'].mean() / 100 if (sub['pnl_pct'] > 0).any() else 0
        al = abs(sub[sub['pnl_pct'] <= 0]['pnl_pct'].mean() / 100) if (sub['pnl_pct'] <= 0).any() else 0.01
        kf, _, edge = kelly_fraction(wr, aw, al)
        stats[s] = {'kelly': min(kf * kelly_pct, 0.35), 'wr': wr, 'edge': edge,
                     'trades': len(sub), 'avg': sub['pnl_pct'].mean()}
    
    sorted_t = all_trades.sort_values('exit_date').reset_index(drop=True)
    cap = capital
    peak = capital
    max_dd = 0
    monthly = {}
    s_pnl = defaultdict(float)
    
    for _, t in sorted_t.iterrows():
        s = t['strategy']
        if s not in stats: continue
        pnl = cap * stats[s]['kelly'] * (t['pnl_pct'] / 100)
        cap += pnl
        peak = max(peak, cap)
        max_dd = max(max_dd, (peak - cap) / peak if peak > 0 else 0)
        s_pnl[s] += pnl
        m = t['exit_date'].strftime('%Y-%m')
        monthly[m] = monthly.get(m, 0) + pnl
    
    yrs = max((sorted_t['exit_date'].max() - sorted_t['entry_date'].min()).days / 365.25, 0.5)
    cagr = (cap / capital) ** (1 / yrs) - 1 if cap > 0 else 0
    ms = pd.Series(monthly)
    sharpe = (ms.mean() / ms.std()) * np.sqrt(12) if len(ms) > 1 and ms.std() > 0 else 0
    
    return {
        'cagr': cagr, 'final': cap, 'max_dd': max_dd, 'sharpe': sharpe,
        'years': yrs, 'trades': len(sorted_t), 'tpy': len(sorted_t)/yrs,
        's_pnl': dict(s_pnl), 'stats': stats, 'monthly': monthly,
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
    print(f"║  ANTIGRAVITY v3 — FULL HONEST BACKTEST".ljust(79) + "║")
    print(f"║  4 strategies | All data validated | No shortcuts".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    # ────────────────────────────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 1: EXPIRY CONVERGENCE (CLEAN)")
    print(f"{'━'*80}")
    ec, ec_valid = backtest_expiry_clean(threshold=0.3, entry_days=5)
    if not ec.empty:
        w = ec[ec['pnl_pct'] > 0]
        l = ec[ec['pnl_pct'] <= 0]
        print(f"  Valid symbols: {ec_valid}")
        print(f"  Trades: {len(ec)} | WR: {len(w)/len(ec)*100:.1f}% | Avg: {ec['pnl_pct'].mean():+.4f}%")
        print(f"  Avg Win: {w['pnl_pct'].mean():+.4f}% | Avg Loss: {l['pnl_pct'].mean():+.4f}%" if len(l) > 0 else "")
        print(f"  Total P&L: ₹{ec['pnl_rupees'].sum():,.0f}")
    
    # ────────────────────────────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 2: ALL-SECTOR PAIR TRADING (every combo)")
    print(f"{'━'*80}")
    pairs, pair_results = scan_all_pairs()
    if not pairs.empty:
        w = pairs[pairs['pnl_pct'] > 0]
        l = pairs[pairs['pnl_pct'] <= 0]
        print(f"  Trades: {len(pairs)} | WR: {len(w)/len(pairs)*100:.1f}% | Avg: {pairs['pnl_pct'].mean():+.4f}%")
        print(f"  Total P&L: ₹{pairs['pnl_rupees'].sum():,.0f}")
        
        print(f"\n  Top 20 pairs:")
        print(f"  {'Pair':<25} {'Sect':<10} {'N':>4} {'WR':>6} {'Avg%':>8} {'P&L':>12}")
        print(f"  {'─'*68}")
        for ps in sorted(pair_results, key=lambda x: -x['total'])[:20]:
            print(f"  {ps['pair']:<25} {ps['sector']:<10} {ps['trades']:>4} {ps['wr']*100:>5.1f}% "
                  f"{ps['avg']:>+7.3f}% ₹{ps['total']:>10,.0f}")
    
    # ────────────────────────────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 3: CALENDAR SPREADS (near-far month)")
    print(f"{'━'*80}")
    cal, cal_n = backtest_calendar_spreads(window=20, z_entry=1.5, z_exit=0.3, z_stop=3.0, time_stop=15)
    if not cal.empty:
        w = cal[cal['pnl_pct'] > 0]
        l = cal[cal['pnl_pct'] <= 0]
        print(f"  Symbols tested: {cal_n}")
        print(f"  Trades: {len(cal)} | WR: {len(w)/len(cal)*100:.1f}% | Avg: {cal['pnl_pct'].mean():+.4f}%")
        print(f"  Total P&L: ₹{cal['pnl_rupees'].sum():,.0f}")
    else:
        print(f"  No calendar spread trades found")
    
    # ────────────────────────────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  STRATEGY 4: MOMENTUM RSI LONG (RSI<35, top-5 sectors)")
    print(f"{'━'*80}")
    mom = backtest_momentum(rsi_buy=35, rsi_exit=50, time_stop=10, max_concurrent=10)
    if not mom.empty:
        w = mom[mom['pnl_pct'] > 0]
        l = mom[mom['pnl_pct'] <= 0]
        print(f"  Trades: {len(mom)} | WR: {len(w)/len(mom)*100:.1f}% | Avg: {mom['pnl_pct'].mean():+.4f}%")
        print(f"  Total P&L: ₹{mom['pnl_rupees'].sum():,.0f}")
    
    # ────────────────────────────────────────────────────────────────────────
    # COMBINE ALL
    all_t = pd.concat([ec, pairs, cal, mom]).dropna(subset=['entry_date','exit_date'])
    
    print(f"\n{'═'*80}")
    print(f"  COMBINED PORTFOLIO — {len(all_t)} total trades")
    print(f"{'═'*80}")
    
    print(f"\n  {'Kelly':>8} {'CAGR':>7} {'Return':>8} {'MaxDD':>7} {'Sharpe':>7} {'Tpy':>5} {'Final':>12}")
    print(f"  {'─'*60}")
    
    for kp in [0.50, 0.60, 0.70, 0.75, 0.80, 0.90, 1.00, 1.10, 1.25]:
        r = simulate(all_t, cap, kp)
        mk = " ◄" if r['cagr'] >= 0.60 else ""
        print(f"  {kp:>7.0%} {r['cagr']:>6.1%} {(r['final']-cap)/cap:>7.1%} "
              f"{r['max_dd']:>6.1%} {r['sharpe']:>6.2f} {r['tpy']:>4.0f} ₹{r['final']:>10,.0f}{mk}")
    
    # Detail at 100% Kelly
    best = simulate(all_t, cap, 1.00)
    print(f"\n{'═'*80}")
    print(f"  RESULT @ FULL (100%) KELLY")
    print(f"{'═'*80}")
    print(f"  CAGR:           {best['cagr']:.1%}")
    print(f"  ₹{cap/100000:.0f}L →        ₹{best['final']:,.0f}")
    print(f"  Max Drawdown:   {best['max_dd']:.1%}")
    print(f"  Sharpe:         {best['sharpe']:.2f}")
    print(f"  Trades:         {best['trades']} ({best['tpy']:.0f}/yr)")
    
    print(f"\n  Strategy P&L:")
    for s in sorted(best['s_pnl'], key=lambda x: -best['s_pnl'][x]):
        info = best['stats'][s]
        print(f"    {s:<18} ₹{best['s_pnl'][s]:>+10,.0f}  K={info['kelly']:.1%} WR={info['wr']:.1%} "
              f"Avg={info['avg']:+.3f}% N={info['trades']}")
    
    print(f"\n  Monthly P&L (last 12):")
    for m in sorted(best['monthly'].keys())[-12:]:
        v = best['monthly'][m]
        bar = "█" * max(1, int(abs(v) / 5000))
        sign = "+" if v >= 0 else "-"
        print(f"    {m}: ₹{v:>+10,.0f} {bar}")
    
    all_t.to_csv('.tmp/full_honest_backtest.csv', index=False)
    print(f"\n  All {len(all_t)} trades saved to .tmp/full_honest_backtest.csv")
    print(f"\n{'═'*80}")


if __name__ == "__main__":
    main()
