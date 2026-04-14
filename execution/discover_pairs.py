"""
Pair Discovery Scanner — Hunt for new proven pairs across the FNO universe.

Pairs come and go. This scanner continuously discovers new candidates by:
1. Loading ALL available 3Y data files (208+ symbols)
2. Grouping by sector (using NSE sector classification)
3. Testing ALL intra-sector pairs for correlation + cointegration
4. Running quick backtests on promising pairs (corr > 0.5)
5. Outputting a ranked list of candidates meeting 90%+ win rate threshold

This runs OFFLINE (no live prices needed) — purely historical analysis.
Results feed into the proven pairs config of scan_proven_pairs.py.

Usage:
    python3 execution/discover_pairs.py                    # Full scan
    python3 execution/discover_pairs.py --sector "Metals"  # Sector only
    python3 execution/discover_pairs.py --fast              # Top sectors only
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
import logging
from datetime import datetime
from itertools import combinations
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = ".tmp/3y_data"

# =============================================================================
# SECTOR CLASSIFICATION — Group FNO symbols by sector for pair candidates
# Only pairs within the same sector are meaningful for mean reversion
# =============================================================================

SECTOR_MAP = {
    "Private Banks": ["HDFCBANK", "ICICIBANK", "KOTAKBANK", "AXISBANK", "INDUSINDBK",
                       "FEDERALBNK", "BANDHANBNK", "IDFCFIRSTB", "AUBANK", "RBLBANK"],
    "PSU Banks": ["SBIN", "PNB", "BANKBARODA", "CANBK", "BANKINDIA", "INDIANB",
                   "UNIONBANK", "CENTRALBK", "IOB"],
    "NBFCs": ["BAJFINANCE", "BAJAJFINSV", "CHOLAFIN", "SHRIRAMFIN", "MANAPPURAM",
              "MUTHOOTFIN", "LICHSGFIN", "PFC", "RECLTD", "POONAWALLA"],
    "IT Services": ["TCS", "INFY", "WIPRO", "HCLTECH", "TECHM", "LTIM", "PERSISTENT",
                     "COFORGE", "MPHASIS"],
    "Pharma": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "AUROPHARMA", "BIOCON",
               "LUPIN", "TORNTPHARM", "ALKEM", "LAURUSLABS", "GRANULES", "IPCALAB"],
    "Cement": ["ULTRACEMCO", "AMBUJACEM", "SHREECEM", "ACC", "DALMIACEM", "RAMCOCEM",
               "JKCEMENT", "BIRLASOFT"],
    "Metals - Non Ferrous": ["HINDALCO", "VEDL", "NMDC", "COALINDIA", "NATIONALUM"],
    "Metals - Steel": ["TATASTEEL", "JSWSTEEL", "JINDALSTEL", "SAIL", "HINDZINC", "APLAPOLLO"],
    "Oil & Gas": ["RELIANCE", "BPCL", "IOC", "HINDPETRO", "GAIL", "ONGC", "PETRONET",
                   "GUJGASLTD", "IGL", "MGL"],
    "Auto - OEM": ["MARUTI", "TATAMOTORS", "M&M", "BAJAJ-AUTO", "HEROMOTOCO",
                    "TVSMOTOR", "EICHERMOT", "ASHOKLEY"],
    "Auto - Ancillary": ["BOSCHLTD", "MOTHERSON", "BHARATFORG", "EXIDEIND",
                          "APOLLOTYRE", "MRF", "BALKRISIND", "CEAT"],
    "Capital Goods": ["ABB", "SIEMENS", "HAVELLS", "POLYCAB", "CGPOWER",
                       "CUMMINSIND", "THERMAX", "BLUESTARCO", "VOLTAS", "CROMPTON"],
    "Defence": ["BEL", "HAL", "BDL"],
    "Power & Utilities": ["NTPC", "POWERGRID", "TATAPOWER", "ADANIGREEN", "ADANIENENT",
                           "NHPC", "SJVN", "IREDA", "JSWENERGY"],
    "FMCG": ["HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "DABUR", "MARICO",
              "COLPAL", "GODREJCP", "TATACONSUM"],
    "Exchanges & Fintech": ["CDSL", "CAMS", "KFINTECH", "BSE", "MCX", "ANGELONE"],
    "Telecom": ["BHARTIARTL", "IDEA"],
    "Infra & Realty": ["LTFINANCE", "LT", "DLF", "GODREJPROP", "OBEROIRLTY",
                        "PRESTIGE", "LODHA"],
    "Chemicals": ["PIIND", "UPL", "SRF", "DEEPAKNTR", "ATUL", "CLEAN"],
    "Healthcare Services": ["APOLLOHOSP", "MAXHEALTH", "FORTIS"],
    "Insurance": ["SBILIFE", "HDFCLIFE", "ICICIPRULI", "LICI", "NIACL"],
}


def load_continuous_series(symbol):
    """Load historical data and build continuous futures series."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None

    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df['FH_EXPIRY_DT_p'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

        # Build continuous series (nearest expiry per date)
        continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT_p'].idxmin()].copy()
        continuous = continuous.sort_values('FH_TIMESTAMP')

        result = continuous[['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_MARKET_LOT']].set_index('FH_TIMESTAMP')
        return result
    except Exception as e:
        logger.debug(f"Error loading {symbol}: {e}")
        return None


def quick_backtest(merged, lookback=60, z_entry=2.0, z_exit=0.5, z_stop=3.5, time_stop=30):
    """
    Quick mean-reversion backtest on merged price series.
    Returns (win_rate, num_trades, avg_return, avg_days, max_drawdown)
    """
    if len(merged) < lookback + 60:
        return None

    merged = merged.copy()
    merged['RATIO'] = merged['CLOSE_A'] / merged['CLOSE_B']
    merged['ROLL_MEAN'] = merged['RATIO'].rolling(lookback).mean()
    merged['ROLL_STD'] = merged['RATIO'].rolling(lookback).std()
    merged['Z'] = (merged['RATIO'] - merged['ROLL_MEAN']) / merged['ROLL_STD']

    # Drop rows where Z couldn't be calculated
    merged = merged.dropna(subset=['Z'])
    if len(merged) < 30:
        return None

    trades = []
    in_trade = False

    for i in range(len(merged)):
        z = merged.iloc[i]['Z']
        ratio = merged.iloc[i]['RATIO']
        date = merged.index[i]

        if not in_trade:
            if abs(z) >= z_entry:
                entry_z = z
                entry_ratio = ratio
                entry_date = date
                entry_idx = i
                in_trade = True
        else:
            days_held = (date - entry_date).days
            current_z = z

            # Exit conditions
            exit_reason = None
            if abs(current_z) < z_exit:
                exit_reason = "mean_revert"
            elif abs(current_z) > z_stop:
                exit_reason = "z_stop"
            elif days_held > time_stop:
                exit_reason = "time_stop"

            if exit_reason:
                # Compute return (we're always trading ratio back to mean)
                if entry_z > 0:  # Short ratio: sell A, buy B
                    pnl_pct = (entry_ratio - ratio) / entry_ratio * 100
                else:  # Long ratio: buy A, sell B
                    pnl_pct = (ratio - entry_ratio) / entry_ratio * 100

                trades.append({
                    'entry': entry_date,
                    'exit': date,
                    'days': days_held,
                    'return': pnl_pct,
                    'reason': exit_reason,
                    'entry_z': entry_z,
                    'exit_z': current_z,
                })
                in_trade = False

    if len(trades) < 3:
        return None

    wins = sum(1 for t in trades if t['return'] > 0)
    wr = wins / len(trades) * 100
    avg_ret = np.mean([t['return'] for t in trades])
    avg_days = np.mean([t['days'] for t in trades])
    max_dd = min(t['return'] for t in trades)

    return {
        'win_rate': round(wr, 1),
        'trades': len(trades),
        'avg_return': round(avg_ret, 2),
        'avg_days': round(avg_days, 1),
        'max_drawdown': round(max_dd, 2),
    }


def analyze_pair_candidate(sym_a, sym_b, min_overlap=200, grid_search=True):
    """
    Full analysis of a pair candidate.
    Returns best backtest config if the pair qualifies (90%+ WR, 5+ trades).
    """
    hist_a = load_continuous_series(sym_a)
    hist_b = load_continuous_series(sym_b)

    if hist_a is None or hist_b is None:
        return None

    # Merge
    merged = hist_a[['FH_CLOSING_PRICE']].join(
        hist_b[['FH_CLOSING_PRICE']],
        how='inner', lsuffix='_A', rsuffix='_B'
    )
    merged.columns = ['CLOSE_A', 'CLOSE_B']

    if len(merged) < min_overlap:
        return None

    # Quick return correlation check (reject if < 0.3)
    returns_a = merged['CLOSE_A'].pct_change(fill_method=None)
    returns_b = merged['CLOSE_B'].pct_change(fill_method=None)
    overall_corr = returns_a.corr(returns_b)

    if pd.isna(overall_corr) or overall_corr < 0.3:
        return None

    # Recent correlation (last 60 days)
    recent_corr = returns_a.tail(60).corr(returns_b.tail(60))
    if pd.isna(recent_corr) or recent_corr < 0.3:
        return None

    # Grid search across lookback periods
    if grid_search:
        lookbacks = [20, 30, 45, 60, 90]
        z_entries = [2.0, 2.5]
        z_stops = [3.0, 3.5]
        time_stops = [20, 30]
    else:
        lookbacks = [30, 60]
        z_entries = [2.0]
        z_stops = [3.5]
        time_stops = [30]

    best = None
    best_score = 0

    for lb in lookbacks:
        for ze in z_entries:
            for zs in z_stops:
                for ts in time_stops:
                    result = quick_backtest(merged, lookback=lb, z_entry=ze,
                                           z_stop=zs, time_stop=ts)
                    if result and result['trades'] >= 3:
                        # Score = win_rate * sqrt(trades) * avg_return
                        score = result['win_rate'] * np.sqrt(result['trades']) * max(result['avg_return'], 0.1)
                        if score > best_score:
                            best_score = score
                            best = {
                                **result,
                                'lookback': lb,
                                'z_entry': ze,
                                'z_stop': zs,
                                'time_stop': ts,
                                'score': round(score, 1),
                            }

    if best is None or best['win_rate'] < 80:
        return None

    data_years = (merged.index.max() - merged.index.min()).days / 365

    return {
        'sym_a': sym_a,
        'sym_b': sym_b,
        'overall_corr': round(overall_corr, 3),
        'recent_corr': round(recent_corr, 3),
        'overlap_days': len(merged),
        'data_years': round(data_years, 1),
        'best_config': best,
    }


def discover_pairs(sector_filter=None, fast_mode=False):
    """
    Run pair discovery across the FNO universe.
    
    Args:
        sector_filter: Only scan specific sector
        fast_mode: Skip grid search, use default params
    """
    print("=" * 90)
    print(f"  PAIR DISCOVERY SCANNER — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Hunting for new proven pairs across {len(SECTOR_MAP)} sectors")
    print(f"  Mode: {'FAST (default params)' if fast_mode else 'FULL (grid search)'}")
    print("=" * 90)

    # Check which symbols have data files
    available_files = set()
    for f in os.listdir(DATA_DIR):
        if f.endswith('_5Y.csv'):
            available_files.add(f.replace('_5Y.csv', ''))

    # Also add any symbols NOT in our sector map but with data (uncategorized)
    classified = set()
    for syms in SECTOR_MAP.values():
        classified.update(syms)
    uncategorized = available_files - classified
    if uncategorized:
        print(f"\n  📋 {len(uncategorized)} symbols with data but not sector-classified")
        print(f"     (Consider adding to SECTOR_MAP: {sorted(list(uncategorized))[:15]}...)")

    all_candidates = []
    total_pairs_tested = 0
    total_pairs_tested_corr = 0

    sectors_to_scan = SECTOR_MAP.items()
    if sector_filter:
        sectors_to_scan = [(k, v) for k, v in SECTOR_MAP.items()
                           if sector_filter.lower() in k.lower()]
        if not sectors_to_scan:
            print(f"\n  ❌ No sector matching '{sector_filter}'")
            return []

    for sector, symbols in sectors_to_scan:
        # Filter to symbols that actually have data
        available = [s for s in symbols if s in available_files]

        if len(available) < 2:
            continue

        pairs = list(combinations(available, 2))
        print(f"\n  📊 {sector}: {len(available)} symbols, {len(pairs)} pairs to test")

        sector_candidates = []

        for sym_a, sym_b in pairs:
            total_pairs_tested += 1
            result = analyze_pair_candidate(sym_a, sym_b, grid_search=not fast_mode)

            if result:
                total_pairs_tested_corr += 1
                result['sector'] = sector
                b = result['best_config']

                if b['win_rate'] >= 90:
                    tier = 1
                    icon = "💎"
                elif b['win_rate'] >= 80:
                    tier = 2
                    icon = "🥈"
                else:
                    tier = 3
                    icon = "🔍"

                result['tier'] = tier
                sector_candidates.append(result)

                print(f"    {icon} {sym_a}/{sym_b}: WR={b['win_rate']}% "
                      f"| {b['trades']} trades | +{b['avg_return']}% avg "
                      f"| LB{b['lookback']} Z{b['z_entry']}/{b['z_stop']} "
                      f"| Corr={result['recent_corr']:.2f} "
                      f"| {result['data_years']}yr")

        all_candidates.extend(sector_candidates)

    # Sort by score
    all_candidates.sort(key=lambda x: x['best_config']['score'], reverse=True)

    # Summary
    t1 = [c for c in all_candidates if c['tier'] == 1]
    t2 = [c for c in all_candidates if c['tier'] == 2]
    t3 = [c for c in all_candidates if c['tier'] == 3]

    print(f"\n{'='*90}")
    print(f"  DISCOVERY RESULTS")
    print(f"{'='*90}")
    print(f"  Tested: {total_pairs_tested} pairs across all sectors")
    print(f"  Correlated (>0.3): {total_pairs_tested_corr}")
    print(f"  Qualified: {len(all_candidates)} (WR ≥ 80%)")
    print(f"  💎 Tier 1 (WR ≥ 90%): {len(t1)}")
    print(f"  🥈 Tier 2 (WR ≥ 80%): {len(t2)}")

    if t1:
        print(f"\n  {'='*80}")
        print(f"  💎 TIER 1 — Ready for Proven Pairs Scanner (WR ≥ 90%)")
        print(f"  {'='*80}")
        for c in t1:
            b = c['best_config']
            print(f"\n    {c['sym_a']}/{c['sym_b']} — {c['sector']}")
            print(f"      WR: {b['win_rate']}% | Trades: {b['trades']} "
                  f"| Avg Return: +{b['avg_return']}% | Avg Days: {b['avg_days']}")
            print(f"      Config: LB{b['lookback']} / Z_entry={b['z_entry']} "
                  f"/ Z_stop={b['z_stop']} / Time={b['time_stop']}d")
            print(f"      Corr: Overall={c['overall_corr']} Recent60D={c['recent_corr']}")
            print(f"      Data: {c['overlap_days']} days ({c['data_years']}yr)")
            print(f"      Max Loss: {b['max_drawdown']}% | Score: {b['score']}")

            # Output config snippet for scan_proven_pairs.py
            print(f'      # Config for scan_proven_pairs.py:')
            print(f'      "{c["sym_a"]}/{c["sym_b"]}": {{')
            print(f'          "sector": "{c["sector"]}", "tier": 1,')
            print(f'          "lookback": {b["lookback"]}, "z_entry": {b["z_entry"]}, '
                  f'"z_stop": {b["z_stop"]}, "time_stop": {b["time_stop"]},')
            print(f'          "hist_trades": {b["trades"]}, "hist_wr": {b["win_rate"]}, '
                  f'"hist_avg_return": {b["avg_return"]}, "hist_avg_days": {b["avg_days"]},')
            print(f'      }},')

    if t2:
        print(f"\n  {'='*80}")
        print(f"  🥈 TIER 2 — Monitor/Validate Further (WR 80-89%)")
        print(f"  {'='*80}")
        for c in t2[:10]:  # Top 10 only
            b = c['best_config']
            print(f"    {c['sym_a']}/{c['sym_b']} ({c['sector']}): "
                  f"WR={b['win_rate']}% | {b['trades']}T | +{b['avg_return']}% "
                  f"| LB{b['lookback']} | Corr={c['recent_corr']}")

    return all_candidates


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pair Discovery Scanner')
    parser.add_argument('--sector', type=str, help='Filter by sector name')
    parser.add_argument('--fast', action='store_true', help='Fast mode (no grid search)')
    args = parser.parse_args()

    candidates = discover_pairs(sector_filter=args.sector, fast_mode=args.fast)

    # Save results to CSV
    if candidates:
        rows = []
        for c in candidates:
            b = c['best_config']
            rows.append({
                'pair': f"{c['sym_a']}/{c['sym_b']}",
                'sector': c['sector'],
                'tier': c['tier'],
                'win_rate': b['win_rate'],
                'trades': b['trades'],
                'avg_return': b['avg_return'],
                'avg_days': b['avg_days'],
                'max_dd': b['max_drawdown'],
                'lookback': b['lookback'],
                'z_entry': b['z_entry'],
                'z_stop': b['z_stop'],
                'time_stop': b['time_stop'],
                'score': b['score'],
                'overall_corr': c['overall_corr'],
                'recent_corr': c['recent_corr'],
                'data_days': c['overlap_days'],
                'data_years': c['data_years'],
            })
        df = pd.DataFrame(rows)
        out_path = '.tmp/pair_discovery_results.csv'
        df.to_csv(out_path, index=False)
        print(f"\n  📁 Results saved to {out_path}")
