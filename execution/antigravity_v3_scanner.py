"""
Antigravity v3 — Unified Daily Scanner.

Combines all strategy layers into a single daily scan:
  1. Pair Trading — Proven pairs with SSS > 4.0
  2. Calendar Spreads — Near/far month premium anomalies  
  3. Expiry Convergence — Premium/discount decay near expiry
  4. Near-Miss Pairs — Sector pairs that pass 3/4 Renaissance criteria

Outputs a unified signal report with Kelly-optimal sizing.

Usage:
    python3 execution/antigravity_v3_scanner.py
    python3 execution/antigravity_v3_scanner.py --capital 1000000
"""

import os
import sys
import json
import argparse
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.append(os.path.dirname(__file__))

from kelly_sizer import kelly_fraction, kelly_optimal_lots, kelly_for_pair

DATA_DIR = '.tmp/3y_data'

# ─── Near-miss pairs (passed 3/4 Renaissance criteria from universe scan) ────
NEAR_MISS_PAIRS = [
    ('ULTRACEMCO', 'GRASIM', 'CEMENT', {'adf': 0.0006, 'coint': 0.0208, 'hl': 12, 'corr': 0.636}),
    ('TATAPOWER', 'NHPC', 'POWER', {'adf': 0.0118, 'coint': 0.0504, 'hl': 10, 'corr': 0.707}),
    ('M&M', 'BHARATFORG', 'AUTO', {'adf': 0.0000, 'coint': 0.0001, 'hl': 5, 'corr': -0.195}),
    ('HUDCO', 'ADANIGREEN', 'POWER', {'adf': 0.0002, 'coint': 0.0031, 'hl': 6, 'corr': 0.564}),
    ('IRFC', 'ADANIENT', 'POWER', {'adf': 0.0116, 'coint': 0.0820, 'hl': 9, 'corr': 0.439}),
    ('LODHA', 'IRCTC', 'INFRA', {'adf': 0.0352, 'coint': 0.0440, 'hl': 12, 'corr': 0.475}),
    ('LT', 'GMRAIRPORT', 'INFRA', {'adf': 0.0450, 'coint': 0.0579, 'hl': 19, 'corr': 0.458}),
    ('ADANIGREEN', 'ADANIENSOL', 'POWER', {'adf': 0.0249, 'coint': 0.0016, 'hl': 25, 'corr': 0.831}),
    ('BIOCON', 'TORNTPHARM', 'PHARMA', {'adf': 0.0122, 'coint': 0.0441, 'hl': 14, 'corr': 0.293}),
    ('HINDUNILVR', 'DMART', 'FMCG', {'adf': 0.0401, 'coint': 0.0654, 'hl': 13, 'corr': 0.192}),
]


def load_prices(symbol):
    """Load continuous futures prices."""
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
        df = df.set_index('FH_TIMESTAMP')
        prices = df['FH_CLOSING_PRICE']
        lot_col = 'FH_MARKET_LOT'
        if lot_col in df.columns:
            lots = pd.to_numeric(df[lot_col], errors='coerce').replace(0, np.nan).ffill().bfill()
        else:
            lots = pd.Series(1, index=df.index, dtype=float)
        return prices, lots
    except:
        return None


def scan_pairs(capital):
    """Scan near-miss pairs for Z-score signals."""
    signals = []
    
    for sym_a, sym_b, sector, stats in NEAR_MISS_PAIRS:
        data_a = load_prices(sym_a)
        data_b = load_prices(sym_b)
        if data_a is None or data_b is None:
            continue
        
        prices_a, lots_a = data_a
        prices_b, lots_b = data_b

        merged = pd.DataFrame({
            'A': prices_a, 'LOT_A': lots_a,
            'B': prices_b, 'LOT_B': lots_b,
        }).dropna(subset=['A', 'B'])
        merged['LOT_A'] = merged['LOT_A'].ffill().bfill()
        merged['LOT_B'] = merged['LOT_B'].ffill().bfill()

        if len(merged) < 60:
            continue

        # Cash-neutral spread per row (directive: NEVER use price ratio for Z-score)
        merged['SPREAD'] = merged['A'] * merged['LOT_A'] - merged['B'] * merged['LOT_B']

        # Multi-timeframe Z-scores on cash-neutral spread
        for window in [20, 30, 60]:
            mean_w = merged['SPREAD'].rolling(window).mean()
            std_w = merged['SPREAD'].rolling(window).std()
            z = ((merged['SPREAD'] - mean_w) / std_w).iloc[-1]

            if abs(z) >= 2.0:
                # SSS
                corr = merged['A'].pct_change().tail(60).corr(merged['B'].pct_change().tail(60))
                sss = abs(z) * (1 + max(0, corr))

                direction = f"SELL {sym_a} / BUY {sym_b}" if z > 0 else f"BUY {sym_a} / SELL {sym_b}"

                live_a = float(merged['A'].iloc[-1])
                live_b = float(merged['B'].iloc[-1])
                live_lot_a = int(merged['LOT_A'].iloc[-1])
                live_lot_b = int(merged['LOT_B'].iloc[-1])

                # Multi-lot ratio solver (same as scan_proven_pairs.py)
                best_ratio_a, best_ratio_b, best_imbalance = 1, 1, float('inf')
                for n_a in range(1, 6):
                    for n_b in range(1, 6):
                        val_a = live_a * live_lot_a * n_a
                        val_b = live_b * live_lot_b * n_b
                        imb = abs(val_a - val_b) / max(val_a, val_b) * 100
                        if imb < best_imbalance:
                            best_imbalance, best_ratio_a, best_ratio_b = imb, n_a, n_b

                if best_imbalance > 50:
                    break  # No viable cash-neutral sizing for this pair

                # Kelly sizing with cash-neutral ratio
                wr_est = 0.60  # Conservative for near-miss pairs
                avg_win_est = 0.03
                avg_loss_est = 0.05

                kelly = kelly_for_pair(
                    wr=wr_est, avg_win=avg_win_est, avg_loss=avg_loss_est,
                    capital=capital,
                    price_a=live_a, lot_a=live_lot_a,
                    price_b=live_b, lot_b=live_lot_b,
                    ratio_a=best_ratio_a, ratio_b=best_ratio_b,
                )

                signals.append({
                    'strategy': 'PAIR',
                    'pair': f"{sym_a}/{sym_b}",
                    'sector': sector,
                    'window': window,
                    'z_score': round(z, 2),
                    'sss': round(sss, 2),
                    'corr': round(corr, 3),
                    'direction': direction,
                    'hl': stats['hl'],
                    'lot_ratio': f"{best_ratio_a}:{best_ratio_b}",
                    'kelly_lots': kelly.get('units', 0),
                    'kelly_pct': kelly.get('pct_of_capital', 0),
                    'conviction': 'NEAR-MISS' if sss < 5 else ('MODERATE' if sss < 7 else 'HIGH'),
                })
                break  # Only report once per pair (shortest window that triggers)
    
    return signals


def scan_expiry_convergence_live():
    """Scan for expiry convergence opportunities."""
    signals = []
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_5Y.csv')]
    
    for fname in sorted(files):
        symbol = fname.replace('_5Y.csv', '')
        path = os.path.join(DATA_DIR, fname)
        
        try:
            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
            df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
            if 'FH_UNDERLYING_VALUE' not in df.columns:
                continue
            df['FH_UNDERLYING_VALUE'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
            df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
            df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_UNDERLYING_VALUE', 'FH_EXPIRY_DT'])
            
            if df.empty:
                continue
            
            latest = df.sort_values('FH_TIMESTAMP').iloc[-1]
            
            fut = latest['FH_CLOSING_PRICE']
            spot = latest['FH_UNDERLYING_VALUE']
            expiry = latest['FH_EXPIRY_DT']
            
            if spot == 0 or np.isnan(spot) or np.isnan(fut):
                continue
            
            premium_pct = (fut - spot) / spot * 100
            days_to_expiry = (expiry - latest['FH_TIMESTAMP']).days
            
            lot_size = int(latest.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in latest.index else 1
            potential_gain = abs(premium_pct / 100) * spot * lot_size
            
            if abs(premium_pct) >= 0.3 and 0 < days_to_expiry <= 7:
                signals.append({
                    'strategy': 'EXPIRY_CONV',
                    'symbol': symbol,
                    'spot': round(spot, 2),
                    'futures': round(fut, 2),
                    'premium_pct': round(premium_pct, 3),
                    'days_to_expiry': days_to_expiry,
                    'lot_size': lot_size,
                    'potential_gain': round(potential_gain, 0),
                    'direction': 'SELL FUT' if premium_pct > 0 else 'BUY FUT',
                    'conviction': 'HIGH' if abs(premium_pct) > 0.8 else 'MODERATE',
                })
        except:
            continue
    
    return signals


def main():
    parser = argparse.ArgumentParser(description='Antigravity v3 Daily Scanner')
    parser.add_argument('--capital', type=float, default=1000000, help='Trading capital in ₹')
    args = parser.parse_args()
    
    now = datetime.now()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY v3 — UNIFIED DAILY SCANNER".ljust(79) + "║")
    print(f"║  Capital: ₹{args.capital:,.0f}".ljust(79) + "║")
    print(f"║  {now.strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    all_signals = []
    
    # ── Strategy 1: Pair Trading (Near-Miss Renaissance Pairs) ──────────────
    print(f"\n{'━'*80}")
    print(f"  LAYER 1: PAIR TRADING (Near-Miss Renaissance Pairs)")
    print(f"{'━'*80}")
    
    pair_signals = scan_pairs(args.capital)
    if pair_signals:
        print(f"\n  Found {len(pair_signals)} pair signals:")
        for s in pair_signals:
            print(f"    {'🔴' if abs(s['z_score']) > 2.5 else '🟡'} {s['pair']:20} Z={s['z_score']:+.2f} SSS={s['sss']:.1f} "
                  f"Corr={s['corr']:.2f} HL={s['hl']}d Ratio={s.get('lot_ratio','1:1')} → {s['direction']} [{s['conviction']}]")
        all_signals.extend(pair_signals)
    else:
        print(f"  No pair signals currently active.")
    
    # ── Strategy 3: Expiry Convergence ──────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  LAYER 3: EXPIRY CONVERGENCE")
    print(f"{'━'*80}")
    
    exp_signals = scan_expiry_convergence_live()
    if exp_signals:
        exp_sorted = sorted(exp_signals, key=lambda x: -abs(x['premium_pct']))
        print(f"\n  Found {len(exp_signals)} expiry convergence opportunities:")
        for s in exp_sorted[:10]:
            print(f"    {'🔴' if abs(s['premium_pct']) > 0.8 else '🟡'} {s['symbol']:15} "
                  f"Premium={s['premium_pct']:+.2f}% DTE={s['days_to_expiry']}d "
                  f"Gain=₹{s['potential_gain']:,.0f} → {s['direction']} [{s['conviction']}]")
        all_signals.extend(exp_signals)
    else:
        print(f"  No expiry convergence signals (check data freshness / DTE window).")
    
    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n{'═'*80}")
    print(f"  DAILY SUMMARY")
    print(f"{'═'*80}")
    
    high_conv = [s for s in all_signals if s.get('conviction') == 'HIGH']
    mod_conv = [s for s in all_signals if s.get('conviction') == 'MODERATE']
    
    print(f"  Total Signals:    {len(all_signals)}")
    print(f"  High Conviction:  {len(high_conv)}")
    print(f"  Moderate:         {len(mod_conv)}")
    
    if high_conv:
        print(f"\n  🔴 TOP SIGNALS (High Conviction):")
        for s in high_conv:
            if s['strategy'] == 'PAIR':
                print(f"    PAIR: {s['pair']} Z={s['z_score']:+.2f} → {s['direction']}")
            else:
                print(f"    EXPIRY: {s['symbol']} Premium={s['premium_pct']:+.2f}% → {s['direction']}")
    
    # Capital allocation
    if all_signals:
        total_deploy = sum(
            s.get('kelly_pct', 0) if s['strategy'] == 'PAIR' 
            else min(15, abs(s.get('premium_pct', 0)) * 10)  # Rough sizing for expiry
            for s in all_signals
        )
        print(f"\n  Estimated Capital Deployment: {min(total_deploy, 70):.1f}% of ₹{args.capital:,.0f}")
        print(f"  Max allowed: 70% (safety cap)")
    
    print(f"\n{'═'*80}")


if __name__ == "__main__":
    main()
