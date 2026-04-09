"""
Diamond Pair Scanner - Backtested & Validated

Scans curated pairs with:
1. Data Hygiene: Split/Bonus detection, regime filtering
2. Backtest Validation: Historical win rate calculation
3. Diamond Filter: Only shows trades with >80% historical win rate

Usage:
    python3 execution/scan_pair_universe.py [--threshold 2.0] [--min-winrate 80]
"""

import pandas as pd
import numpy as np
import os
import argparse
import logging
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from shoonya_client import ShoonyaClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = ".tmp/3y_data"

# =============================================================================
# CURATED PAIR UNIVERSE (Sector-Based)
# =============================================================================
SECTOR_PAIRS = {
    "Private Banks": [
        ("HDFCBANK", "ICICIBANK"),
        ("HDFCBANK", "KOTAKBANK"),
        ("ICICIBANK", "KOTAKBANK"),
        ("AXISBANK", "ICICIBANK"),
        ("AXISBANK", "HDFCBANK"),
    ],
    "PSU Banks": [
        ("SBIN", "BANKBARODA"),
        ("SBIN", "PNB"),
        ("BANKBARODA", "PNB"),
    ],
    "NBFCs": [
        ("BAJFINANCE", "BAJAJFINSV"),
        ("CHOLAFIN", "SHRIRAMFIN"),
        ("MUTHOOTFIN", "MANAPPURAM"),
    ],
    "Life Insurance": [
        ("HDFCLIFE", "SBILIFE"),
        ("HDFCLIFE", "ICICIPRULI"),
        ("SBILIFE", "ICICIPRULI"),
    ],
    "IT Services": [
        ("TCS", "INFY"),
        ("TCS", "HCLTECH"),
        ("INFY", "HCLTECH"),
        ("WIPRO", "TECHM"),
        ("TECHM", "HCLTECH"),
    ],
    "Pharma": [
        ("SUNPHARMA", "CIPLA"),
        ("SUNPHARMA", "DRREDDY"),
        ("CIPLA", "DRREDDY"),
        ("LUPIN", "CIPLA"),
    ],
    "Oil & Gas": [
        ("ONGC", "OIL"),
        ("BPCL", "IOC"),
        ("BPCL", "HINDPETRO"),
        ("IOC", "HINDPETRO"),
    ],
    "Power Finance": [
        ("PFC", "RECLTD"),
        ("IRFC", "PFC"),
        ("IRFC", "RECLTD"),
    ],
    "Metals - Steel": [
        ("TATASTEEL", "JSWSTEEL"),
        ("TATASTEEL", "JINDALSTEL"),
        ("JSWSTEEL", "JINDALSTEL"),
    ],
    "Metals - Non-Ferrous": [
        ("HINDALCO", "VEDL"),
        ("NMDC", "COALINDIA"),
    ],
    "Cement": [
        ("ULTRACEMCO", "SHREECEM"),
        ("ULTRACEMCO", "AMBUJACEM"),
    ],
    "Auto": [
        ("MARUTI", "M&M"),
        ("HEROMOTOCO", "BAJAJ-AUTO"),
        ("BAJAJ-AUTO", "TVSMOTOR"),
    ],
    "FMCG": [
        ("HINDUNILVR", "ITC"),
        ("DABUR", "MARICO"),
        ("BRITANNIA", "NESTLEIND"),
    ],
    "Capital Goods": [
        ("ABB", "SIEMENS"),
        ("HAVELLS", "POLYCAB"),
    ],
    "Defence": [
        ("BEL", "HAL"),
    ],
    "Exchanges": [
        ("CDSL", "CAMS"),
        ("KFINTECH", "CAMS"),
    ],
}


def load_historical_data(symbol, max_staleness_days=3):
    """Load 3Y data with freshness validation.
    
    FIX 1: Staleness reduced from 14 to 3 days.
    FIX 2: Split detection moved to cross-validate BOTH stocks in analyze_pair.
           Single-stock split detection caused false positives on market crashes
           (e.g., Jun 4 2024 election crash flagged BOB as 'split').
    """
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP'])
        df = df.sort_values('FH_TIMESTAMP')
        
        # --- DATA FRESHNESS CHECK (FIX 1: 14 -> 3 days) ---
        last_date = df['FH_TIMESTAMP'].max()
        staleness = (datetime.now() - last_date).days
        if staleness > max_staleness_days:
            logger.warning(f"Rejecting {symbol}: Data is {staleness} days stale (last: {last_date.date()})")
            return None
        
        # Continuous proxy: take nearest expiry for each date
        continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT'].idxmin()]
        result = continuous[['FH_TIMESTAMP', 'FH_CLOSING_PRICE']].set_index('FH_TIMESTAMP')
        
        # Store daily pct_change for cross-validated split detection later
        result['pct_change'] = result['FH_CLOSING_PRICE'].pct_change()
        
        return result
    except Exception as e:
        logger.error(f"Error loading {symbol}: {e}")
        return None


def get_live_futures_price(api, symbol):
    """Fetch current month futures price from Shoonya."""
    try:
        ret = api.searchscrip(exchange='NFO', searchtext=symbol)
        if not ret or 'values' not in ret:
            return None
        
        futures = [
            x for x in ret['values']
            if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX') and x['symname'] == symbol
        ]
        
        if not futures:
            return None
        
        def parse_expiry(x):
            try:
                return datetime.strptime(x['exd'], '%d-%b-%Y')
            except:
                return datetime.max
        
        futures.sort(key=parse_expiry)
        nearest = futures[0]
        
        quote = api.get_quotes(exchange='NFO', token=nearest['token'])
        if quote and 'lp' in quote:
            return float(quote['lp'])
    except Exception as e:
        logger.debug(f"Error getting live price for {symbol}: {e}")
    
    return None


def backtest_pair(hist_a, hist_b, z_entry=2.0, z_exit=0.5, lookback=60):
    """
    Backtest mean reversion strategy for a pair.
    
    Rules:
    - Entry: |Z| > z_entry
    - Exit: |Z| < z_exit OR opposite Z > z_entry (stop loss)
    - Returns: Win rate, avg return, number of trades
    
    FIX 2: Removed regime filtering. Split detection is now cross-validated
    in analyze_pair_with_backtest() to avoid false positives from market crashes.
    """
    # Merge data
    merged = hist_a[['FH_CLOSING_PRICE']].join(
        hist_b[['FH_CLOSING_PRICE']], 
        how='inner', 
        lsuffix='_A', 
        rsuffix='_B'
    )
    
    if len(merged) < lookback + 50:
        return None
    
    if len(merged) < lookback + 20:
        return None
    
    # Calculate ratio
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']
    
    # Rolling stats
    merged['MEAN'] = merged['RATIO'].rolling(window=lookback).mean()
    merged['STD'] = merged['RATIO'].rolling(window=lookback).std()
    merged['Z'] = (merged['RATIO'] - merged['MEAN']) / merged['STD'].replace(0, np.nan)
    
    merged = merged.dropna(subset=['Z'])
    
    if len(merged) < 20:
        return None
    
    # Simulate trades
    trades = []
    position = None  # None, 'LONG', 'SHORT'
    entry_z = None
    entry_ratio = None
    entry_idx = None
    
    for i in range(len(merged)):
        z = merged['Z'].iloc[i]
        ratio = merged['RATIO'].iloc[i]
        
        if position is None:
            # Entry conditions
            if z < -z_entry:
                position = 'LONG'
                entry_z = z
                entry_ratio = ratio
                entry_idx = i
            elif z > z_entry:
                position = 'SHORT'
                entry_z = z
                entry_ratio = ratio
                entry_idx = i
        else:
            # Exit conditions
            exit_trade = False
            
            if position == 'LONG':
                # Exit on mean reversion OR stop loss
                if z > -z_exit:
                    exit_trade = True
                elif z > z_entry:  # Stop loss - went wrong direction
                    exit_trade = True
                    
            elif position == 'SHORT':
                if z < z_exit:
                    exit_trade = True
                elif z < -z_entry:  # Stop loss
                    exit_trade = True
            
            if exit_trade:
                # Calculate PnL
                if position == 'LONG':
                    pnl_pct = (ratio - entry_ratio) / entry_ratio * 100
                else:
                    pnl_pct = (entry_ratio - ratio) / entry_ratio * 100
                
                trades.append({
                    'entry_z': entry_z,
                    'exit_z': z,
                    'pnl_pct': pnl_pct,
                    'duration': i - entry_idx
                })
                
                position = None
                entry_z = None
                entry_ratio = None
    
    if not trades:
        return None
    
    trades_df = pd.DataFrame(trades)
    
    wins = (trades_df['pnl_pct'] > 0).sum()
    total = len(trades_df)
    win_rate = wins / total * 100
    avg_return = trades_df['pnl_pct'].mean()
    avg_duration = trades_df['duration'].mean()
    
    return {
        'total_trades': total,
        'win_rate': round(win_rate, 1),
        'avg_return_pct': round(avg_return, 2),
        'avg_duration_days': round(avg_duration, 1),
        'max_win_pct': round(trades_df['pnl_pct'].max(), 2),
        'max_loss_pct': round(trades_df['pnl_pct'].min(), 2),
    }


def analyze_pair_with_backtest(api, symbol_a, symbol_b, z_threshold=2.0, min_winrate=80):
    """
    Full analysis with SAFETY GATES:
    1. Load historical data (freshness validated)
    2. Cross-validate splits (FIX 2: ignore market-wide crashes)
    3. Backtest the pair
    4. Get live prices
    5. GATE: 20D Correlation > 0.3 (FIX 3)
    6. GATE: Live ratio within historical range ± 5% (FIX 4)
    7. Calculate Z-score, reject if > 4.0
    """
    # 1. Load historical data (FIX 1: staleness <= 3 days enforced)
    hist_a = load_historical_data(symbol_a)
    hist_b = load_historical_data(symbol_b)
    
    if hist_a is None or hist_b is None:
        return None
    
    # --- FIX 2: Cross-validated split detection ---
    # Only flag as split if ONE stock moved >20% and the OTHER moved <5%
    # If both moved >10%, it's a market crash (e.g., Jun 4 2024 election)
    merged_raw = hist_a[['FH_CLOSING_PRICE', 'pct_change']].join(
        hist_b[['FH_CLOSING_PRICE', 'pct_change']],
        how='inner',
        lsuffix='_A',
        rsuffix='_B'
    )
    
    for idx in merged_raw.index:
        chg_a = abs(merged_raw.loc[idx, 'pct_change_A']) if not pd.isna(merged_raw.loc[idx, 'pct_change_A']) else 0
        chg_b = abs(merged_raw.loc[idx, 'pct_change_B']) if not pd.isna(merged_raw.loc[idx, 'pct_change_B']) else 0
        
        # TRUE split: one stock moves >20%, other moves <5%
        is_true_split = (chg_a > 0.20 and chg_b < 0.05) or (chg_b > 0.20 and chg_a < 0.05)
        
        if is_true_split:
            logger.warning(f"Detected TRUE split for {symbol_a}/{symbol_b} on {idx.date()} "
                          f"(A:{chg_a*100:+.1f}%, B:{chg_b*100:+.1f}%). Trimming data.")
            # Keep only data AFTER the split
            merged_raw = merged_raw.loc[idx:]
            break
    
    if len(merged_raw) < 120:
        return None
    
    # 2. Backtest
    backtest = backtest_pair(hist_a, hist_b, z_entry=z_threshold, z_exit=0.5, lookback=60)
    
    if backtest is None:
        return None
    
    # Filter by win rate
    if backtest['win_rate'] < min_winrate:
        return None
    
    # 3. Get live prices
    live_a = get_live_futures_price(api, symbol_a)
    live_b = get_live_futures_price(api, symbol_b)
    
    if live_a is None or live_b is None:
        return None
    
    if live_b == 0:
        return None
    
    live_ratio = live_a / live_b
    
    # 4. Compute merged ratio for stats
    merged = merged_raw[['FH_CLOSING_PRICE_A', 'FH_CLOSING_PRICE_B']].copy()
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']
    
    # --- FIX 4: Historical range check ---
    hist_min = merged['RATIO'].min()
    hist_max = merged['RATIO'].max()
    margin = (hist_max - hist_min) * 0.05  # 5% buffer
    
    if live_ratio > hist_max + margin or live_ratio < hist_min - margin:
        logger.warning(f"Rejecting {symbol_a}/{symbol_b}: Live ratio {live_ratio:.4f} "
                      f"outside historical range [{hist_min:.4f}, {hist_max:.4f}] + 5% margin")
        return None
    
    # Use last 60 days for current stats
    recent_60 = merged.tail(60)
    recent_20 = merged.tail(20)
    
    regime_mean = recent_60['RATIO'].mean()
    regime_std = recent_60['RATIO'].std()
    
    if regime_std == 0:
        return None
    
    z_score = (live_ratio - regime_mean) / regime_std
    
    # SANITY CHECK: Reject absurd Z-scores (tightened from 5.0 to 4.0)
    if abs(z_score) > 4.0:
        logger.warning(f"Rejecting {symbol_a}/{symbol_b}: Z={z_score:.1f} is extreme/data artifact")
        return None
    
    # --- FIX 3: Dual correlation gate ---
    corr_60 = recent_60['FH_CLOSING_PRICE_A'].corr(recent_60['FH_CLOSING_PRICE_B'])
    corr_20 = recent_20['FH_CLOSING_PRICE_A'].corr(recent_20['FH_CLOSING_PRICE_B'])
    
    if corr_60 < 0.5:
        logger.warning(f"Rejecting {symbol_a}/{symbol_b}: 60D correlation {corr_60:.2f} < 0.5")
        return None
    
    if corr_20 < 0.3:
        logger.warning(f"Rejecting {symbol_a}/{symbol_b}: 20D correlation {corr_20:.2f} < 0.3 (short-term decorrelation)")
        return None
    
    return {
        'Symbol_A': symbol_a,
        'Symbol_B': symbol_b,
        'Live_A': live_a,
        'Live_B': live_b,
        'Live_Ratio': round(live_ratio, 4),
        'Mean': round(regime_mean, 4),
        'Std': round(regime_std, 4),
        'Z_Score': round(z_score, 2),
        'Correlation_60D': round(corr_60, 2),
        'Correlation_20D': round(corr_20, 2),
        'Hist_Range': f"{hist_min:.4f}-{hist_max:.4f}",
        'Win_Rate': backtest['win_rate'],
        'Avg_Return': backtest['avg_return_pct'],
        'Total_Trades': backtest['total_trades'],
        'Avg_Duration': backtest['avg_duration_days'],
    }


def scan_diamond_trades(z_threshold=2.0, min_winrate=80):
    """Scan for DIAMOND trades only - validated by backtest."""
    client = ShoonyaClient()
    api = client.login()
    
    if not api:
        logger.error("Shoonya login failed.")
        return []
    
    all_pairs = []
    for sector, pairs in SECTOR_PAIRS.items():
        for pair in pairs:
            all_pairs.append((sector, pair[0], pair[1]))
    
    logger.info(f"Scanning {len(all_pairs)} pairs for DIAMOND trades (Win Rate >= {min_winrate}%)...")
    
    diamonds = []
    analyzed = 0
    
    for i, (sector, sym_a, sym_b) in enumerate(all_pairs):
        if (i + 1) % 10 == 0:
            logger.info(f"Progress: {i+1}/{len(all_pairs)}")
        
        result = analyze_pair_with_backtest(api, sym_a, sym_b, z_threshold, min_winrate)
        
        if result:
            analyzed += 1
            z = result['Z_Score']
            
            # Check for active signal
            if abs(z) >= z_threshold:
                result['Sector'] = sector
                if z > z_threshold:
                    result['Signal'] = f"SELL {sym_a} / BUY {sym_b}"
                    result['Direction'] = 'SHORT'
                else:
                    result['Signal'] = f"BUY {sym_a} / SELL {sym_b}"
                    result['Direction'] = 'LONG'
                diamonds.append(result)
    
    # Output
    if diamonds:
        print("\n" + "=" * 90)
        print("💎 DIAMOND PAIR TRADES (Backtest Validated)")
        print("=" * 90)
        
        diamonds_df = pd.DataFrame(diamonds)
        diamonds_df = diamonds_df.sort_values('Win_Rate', ascending=False)
        
        for _, row in diamonds_df.iterrows():
            print(f"\n📊 {row['Sector']}: {row['Symbol_A']} / {row['Symbol_B']}")
            print(f"   Live: {row['Live_A']:.2f} / {row['Live_B']:.2f}")
            print(f"   Z-Score: {row['Z_Score']:.2f} | Corr 60D: {row['Correlation_60D']:.2f} | Corr 20D: {row['Correlation_20D']:.2f}")
            print(f"   Hist Range: {row['Hist_Range']} | Live Ratio: {row['Live_Ratio']:.4f}")
            print(f"   Win Rate: {row['Win_Rate']:.0f}% ({row['Total_Trades']} trades)")
            print(f"   Avg Return: {row['Avg_Return']:.1f}% | Avg Duration: {row['Avg_Duration']:.0f} days")
            print(f"   ➡️  {row['Signal']}")
        
        print("\n" + "=" * 90)
        print(f"💎 Total Diamond Trades: {len(diamonds)}")
        print("=" * 90)
    else:
        print("\n" + "=" * 90)
        print("✅ NO DIAMOND TRADES AT THIS TIME")
        print(f"   Pairs with valid backtest: {analyzed}")
        print(f"   Criteria: |Z| >= {z_threshold}, Win Rate >= {min_winrate}%")
        print("=" * 90)
    
    # Save results
    if diamonds:
        df = pd.DataFrame(diamonds)
        df.to_csv(".tmp/diamond_trades.csv", index=False)
        logger.info("Results saved to .tmp/diamond_trades.csv")
    
    return diamonds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan for Diamond Pair Trades (Backtest Validated)')
    parser.add_argument('--threshold', type=float, default=2.0, help='Z-Score threshold (default: 2.0)')
    parser.add_argument('--min-winrate', type=float, default=80, help='Minimum win rate %% (default: 80)')
    args = parser.parse_args()
    
    scan_diamond_trades(z_threshold=args.threshold, min_winrate=args.min_winrate)
