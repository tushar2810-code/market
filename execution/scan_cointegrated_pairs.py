"""
Fast Cointegration Scanner — Find True Mean-Reverting Pairs Across FNO Universe.

Runs ADF + Cointegration + Hurst + Half-Life on all same-sector pair combinations.
Much faster than full Renaissance deep dive — used as a PRE-SCREEN.

Pairs that pass: H < 0.5, ADF p < 0.05, Coint p < 0.10, HL < 40d
These get promoted to the "True Proven Pairs" list for live trading.

Usage:
    python3 execution/scan_cointegrated_pairs.py
    python3 execution/scan_cointegrated_pairs.py --sector pharma
    python3 execution/scan_cointegrated_pairs.py --all  # Cross-sector too (slow)
"""

import json
import pandas as pd
import numpy as np
import os
import sys
import argparse
import warnings
import time
from datetime import datetime
from itertools import combinations

warnings.filterwarnings('ignore')

from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

DATA_DIR = '.tmp/3y_data'

# ─── Sector Classification ───────────────────────────────────────────────────
SECTORS = {
    'PHARMA': ['SUNPHARMA', 'CIPLA', 'DRREDDY', 'LUPIN', 'AUROPHARMA', 'BIOCON', 
               'DIVISLAB', 'TORNTPHARM', 'ALKEM', 'GLENMARK', 'LAURUSLABS', 
               'ZYDUSLIFE', 'SYNGENE', 'PPLPHARMA', 'MANKIND'],
    'BANKING': ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK', 'SBIN', 
                'INDUSINDBK', 'BANKBARODA', 'PNB', 'FEDERALBNK', 'IDFCFIRSTB',
                'CANBK', 'BANDHANBNK', 'AUBANK', 'RBLBANK', 'INDIANB', 'YESBANK',
                'BANKINDIA', 'UNIONBANK'],
    'NBFC': ['BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'SHRIRAMFIN', 'MUTHOOTFIN',
             'LTF', 'LICHSGFIN', 'MANAPPURAM', 'PNBHOUSING', 'MFSL', 'SBICARD',
             'HDFCAMC', 'JIOFIN', '360ONE', 'ANGELONE', 'NUVAMA', 'POLICYBZR'],
    'IT': ['TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'LTIM', 'MPHASIS',
           'COFORGE', 'PERSISTENT', 'KPITTECH', 'TATAELXSI', 'OFSS'],
    'METALS': ['TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'SAIL', 'NMDC',
               'JINDALSTEL', 'NATIONALUM', 'HINDZINC', 'COALINDIA'],
    'POWER': ['NTPC', 'POWERGRID', 'TATAPOWER', 'NHPC', 'PFC', 'RECLTD', 
              'IREDA', 'IRFC', 'HUDCO', 'TORNTPOWER', 'JSWENERGY', 'ADANIENT',
              'ADANIGREEN', 'ADANIENSOL'],
    'INFRA': ['LT', 'RVNL', 'NBCC', 'CONCOR', 'GMRAIRPORT', 'DLF', 
              'OBEROIRLTY', 'GODREJPROP', 'PRESTIGE', 'LODHA', 'PHOENIXLTD', 'IRCTC'],
    'AUTO': ['MARUTI', 'M&M', 'TATAMOTORS', 'BAJAJ-AUTO', 'HEROMOTOCO', 'TVSMOTOR',
             'EICHERMOT', 'ASHOKLEY', 'MOTHERSON', 'BHARATFORG', 'UNOMINDA', 'EXIDEIND'],
    'FMCG': ['HINDUNILVR', 'ITC', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL',
             'BRITANNIA', 'GODREJCP', 'TATACONSUM', 'VBL', 'DMART', 'JUBLFOOD',
             'UNITDSPR', 'PATANJALI'],
    'CEMENT': ['ULTRACEMCO', 'AMBUJACEM', 'SHREECEM', 'DALBHARAT', 'GRASIM'],
    'OIL': ['RELIANCE', 'ONGC', 'BPCL', 'IOC', 'HINDPETRO', 'GAIL', 'PETRONET', 'OIL'],
    'TELECOM': ['BHARTIARTL', 'IDEA', 'INDUSTOWER'],
    'INSURANCE': ['LICI', 'SBILIFE', 'HDFCLIFE', 'ICICIPRULI', 'ICICIGI', 'MAXHEALTH'],
    'DEFENCE': ['HAL', 'BEL', 'BDL', 'MAZDOCK'],
    'CAPITAL_GOODS': ['ABB', 'SIEMENS', 'CGPOWER', 'BHEL', 'CUMMINSIND', 'HAVELLS',
                      'POLYCAB', 'CROMPTON', 'VOLTAS', 'BLUESTARCO', 'DIXON', 'KEI'],
}


def load_continuous_prices(symbol):
    """Load continuous futures prices (nearest expiry). Returns Series or None."""
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
        
        series = df.set_index('FH_TIMESTAMP')['FH_CLOSING_PRICE']
        if len(series) < 200:
            return None
        return series
    except Exception:
        return None


def hurst_fast(ts, max_lag=50):
    """Fast Hurst exponent calculation."""
    n = len(ts)
    if n < 100:
        return None
    
    lags = range(2, min(max_lag, n // 4))
    tau, rs_values = [], []
    
    for lag in lags:
        chunks = n // lag
        rs_list = []
        for i in range(chunks):
            chunk = ts[i * lag:(i + 1) * lag]
            mean_c = np.mean(chunk)
            dev = chunk - mean_c
            cum = np.cumsum(dev)
            R = max(cum) - min(cum)
            S = np.std(chunk, ddof=1)
            if S > 0:
                rs_list.append(R / S)
        if rs_list:
            tau.append(lag)
            rs_values.append(np.mean(rs_list))
    
    if len(tau) < 2:
        return None
    
    H, _ = np.polyfit(np.log(tau), np.log(rs_values), 1)
    return H


def half_life_calc(spread):
    """Calculate half-life of mean reversion."""
    spread = spread.dropna()
    if len(spread) < 30:
        return None
    
    lag = spread.shift(1)
    delta = spread - lag
    valid = ~(lag.isna() | delta.isna())
    
    if valid.sum() < 20:
        return None
    
    y = delta[valid].values
    x = lag[valid].values
    x_c = add_constant(x)
    
    try:
        result = OLS(y, x_c).fit()
        theta = result.params[1]
        if theta >= 0:
            return -1  # Diverging
        return -np.log(2) / np.log(1 + theta)
    except:
        return None


def test_pair(sym_a, sym_b, prices_a, prices_b):
    """
    Fast pair validation: ADF + Cointegration + Hurst + Half-Life.
    Returns dict of results or None if data insufficient.
    """
    # Align
    merged = pd.DataFrame({
        'A': prices_a, 'B': prices_b
    }).dropna()
    
    if len(merged) < 200:
        return None
    
    ratio = merged['A'] / merged['B']
    
    # 1. ADF on ratio
    try:
        adf_stat, adf_p, *_ = adfuller(ratio, maxlag=20)
    except:
        return None
    
    # 2. Cointegration
    try:
        coint_stat, coint_p, _ = coint(merged['A'], merged['B'])
    except:
        return None
    
    # 3. Hurst
    H = hurst_fast(ratio.values)
    
    # 4. Half-Life
    hl = half_life_calc(ratio)
    
    # 5. Correlation
    corr = merged['A'].pct_change().corr(merged['B'].pct_change())
    
    # 6. Current Z-Score (30d)
    mean_30 = ratio.rolling(30).mean()
    std_30 = ratio.rolling(30).std()
    z_30 = ((ratio - mean_30) / std_30).iloc[-1]
    
    # 7. OLS R²
    try:
        X = add_constant(merged['B'].values)
        ols = OLS(merged['A'].values, X).fit()
        r_sq = ols.rsquared
    except:
        r_sq = None
    
    return {
        'Pair': f"{sym_a}/{sym_b}",
        'SymA': sym_a,
        'SymB': sym_b,
        'Days': len(merged),
        'ADF_p': round(adf_p, 4),
        'Coint_p': round(coint_p, 4),
        'Hurst': round(H, 4) if H else None,
        'HalfLife': round(hl, 1) if hl and hl > 0 else ('DIV' if hl == -1 else None),
        'Corr': round(corr, 3) if corr else None,
        'R2': round(r_sq, 3) if r_sq else None,
        'Z_30d': round(z_30, 2) if not np.isnan(z_30) else None,
        'Passes': (
            (adf_p < 0.05) and 
            (coint_p < 0.10) and 
            (H is not None and H < 0.5) and 
            (hl is not None and isinstance(hl, (int, float)) and 0 < hl < 40)
        )
    }


def scan_sector(sector_name, symbols, all_prices):
    """Scan all pair combinations within a sector."""
    available = [(s, all_prices[s]) for s in symbols if s in all_prices]
    
    if len(available) < 2:
        return []
    
    results = []
    pairs = list(combinations(available, 2))
    
    for (sym_a, pa), (sym_b, pb) in pairs:
        result = test_pair(sym_a, sym_b, pa, pb)
        if result:
            result['Sector'] = sector_name
            results.append(result)
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Cointegration Universe Scanner')
    parser.add_argument('--sector', help='Scan specific sector only (e.g., pharma, banking)')
    parser.add_argument('--all', action='store_true', help='Cross-sector scan (very slow)')
    parser.add_argument('--min-data', type=int, default=200, help='Minimum trading days required')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print(f"║  COINTEGRATION UNIVERSE SCANNER — Antigravity v3".ljust(79) + "║")
    print(f"║  Finding True Mean-Reverting Pairs".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    # Load all available prices
    print(f"\n  Loading price data...")
    all_prices = {}
    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('_5Y.csv')]
    
    for f in all_files:
        sym = f.replace('_5Y.csv', '')
        prices = load_continuous_prices(sym)
        if prices is not None and len(prices) >= args.min_data:
            all_prices[sym] = prices
    
    print(f"  Loaded {len(all_prices)} symbols with {args.min_data}+ trading days")

    # Determine which sectors to scan
    if args.sector:
        sector_key = args.sector.upper()
        if sector_key in SECTORS:
            sectors_to_scan = {sector_key: SECTORS[sector_key]}
        else:
            print(f"  ❌ Unknown sector: {args.sector}")
            print(f"  Available: {', '.join(SECTORS.keys())}")
            return
    else:
        sectors_to_scan = SECTORS

    # Scan
    all_results = []
    total_pairs = 0
    
    for sector_name, symbols in sectors_to_scan.items():
        avail = [s for s in symbols if s in all_prices]
        n_pairs = len(list(combinations(avail, 2)))
        total_pairs += n_pairs
        
        if n_pairs == 0:
            continue
            
        print(f"\n  Scanning {sector_name}: {len(avail)} stocks, {n_pairs} pairs...")
        t0 = time.time()
        results = scan_sector(sector_name, symbols, all_prices)
        elapsed = time.time() - t0
        
        passing = [r for r in results if r['Passes']]
        print(f"    Done in {elapsed:.1f}s — {len(results)} tested, {len(passing)} PASS")
        
        all_results.extend(results)

    if not all_results:
        print("\n  No results generated.")
        return

    # Sort and display
    df = pd.DataFrame(all_results)
    
    # Passing pairs
    passing = df[df['Passes'] == True].sort_values('Hurst')
    
    print(f"\n{'═'*100}")
    print(f"  RESULTS: {total_pairs} pairs tested, {len(passing)} PASS all criteria")
    print(f"  Criteria: ADF p<0.05 | Coint p<0.10 | Hurst<0.5 | HalfLife<40d")
    print(f"{'═'*100}")
    
    if len(passing) > 0:
        print(f"\n  ✅ TRUE MEAN-REVERTING PAIRS (Renaissance-Grade):")
        print(f"  {'Pair':<25} {'Sector':<12} {'Hurst':>6} {'ADF_p':>7} {'Coint_p':>8} {'HL':>6} {'Corr':>6} {'Z_30d':>7} {'R²':>5}")
        print(f"  {'─'*90}")
        
        for _, row in passing.iterrows():
            hl_str = f"{row['HalfLife']:.0f}d" if isinstance(row['HalfLife'], (int, float)) else str(row['HalfLife'])
            z_str = f"{row['Z_30d']:+.1f}" if row['Z_30d'] is not None else 'N/A'
            print(f"  {row['Pair']:<25} {row['Sector']:<12} {row['Hurst']:>6.3f} {row['ADF_p']:>7.4f} {row['Coint_p']:>8.4f} {hl_str:>6} {row['Corr']:>6.3f} {z_str:>7} {row['R2']:>5.3f}")
        
        # Active signals among passing pairs
        active = passing[(passing['Z_30d'].abs() > 2.0)]
        if len(active) > 0:
            print(f"\n  ELEVATED Z (|Z| > 2.0) — PRE-SCREEN ONLY, price-ratio based:")
            print(f"  WARNING: Do NOT trade directly from this Z. Verify via scan_proven_pairs.py (cash-neutral spread).")
            for _, row in active.iterrows():
                direction = "SELL A / BUY B" if row['Z_30d'] > 0 else "BUY A / SELL B"
                print(f"    {row['Pair']}: Z={row['Z_30d']:+.2f} (ratio) → candidate direction: {direction}")
    else:
        print(f"\n  ❌ No pairs passed all criteria in the scanned sectors.")
    
    # Near-misses (pass 3 out of 4)
    near = df[df['Passes'] == False].copy()
    if len(near) > 0:
        near['score'] = 0
        near.loc[near['ADF_p'] < 0.05, 'score'] += 1
        near.loc[near['Coint_p'] < 0.10, 'score'] += 1
        near.loc[near['Hurst'].apply(lambda x: x is not None and x < 0.5), 'score'] += 1
        near.loc[near['HalfLife'].apply(lambda x: isinstance(x, (int, float)) and 0 < x < 40), 'score'] += 1
        
        near_3 = near[near['score'] >= 3].sort_values('score', ascending=False).head(10)
        if len(near_3) > 0:
            print(f"\n  🟡 NEAR MISSES (3/4 criteria passed):")
            print(f"  {'Pair':<25} {'Sector':<12} {'Hurst':>6} {'ADF_p':>7} {'Coint_p':>8} {'HL':>6} {'Corr':>6}")
            print(f"  {'─'*70}")
            for _, row in near_3.iterrows():
                hl_str = f"{row['HalfLife']:.0f}d" if isinstance(row['HalfLife'], (int, float)) and row['HalfLife'] > 0 else str(row['HalfLife'])
                h_icon = "✅" if row['Hurst'] is not None and row['Hurst'] < 0.5 else "❌"
                a_icon = "✅" if row['ADF_p'] < 0.05 else "❌"
                c_icon = "✅" if row['Coint_p'] < 0.10 else "❌"
                hl_icon = "✅" if isinstance(row['HalfLife'], (int, float)) and 0 < row['HalfLife'] < 40 else "❌"
                print(f"  {row['Pair']:<25} {row['Sector']:<12} {h_icon}{row['Hurst']:>5.3f} {a_icon}{row['ADF_p']:>6.4f} {c_icon}{row['Coint_p']:>7.4f} {hl_icon}{hl_str:>5} {row['Corr']:>6.3f}")
    
    # Save full results
    out_path = '.tmp/cointegration_scan.csv'
    df.to_csv(out_path, index=False)
    print(f"\n  Full results saved to {out_path}")
    
    # Save passing pairs as JSON for other scripts
    if len(passing) > 0:
        proven = passing[['SymA', 'SymB', 'Sector', 'Hurst', 'ADF_p', 'Coint_p', 'HalfLife', 'Corr']].to_dict('records')
        with open('.tmp/true_proven_pairs.json', 'w') as f:
            json.dump(proven, f, indent=2, default=str)
        print(f"  True proven pairs saved to .tmp/true_proven_pairs.json")
    
    print(f"\n{'═'*100}")


if __name__ == "__main__":
    main()
