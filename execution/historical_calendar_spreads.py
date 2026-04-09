"""
Data-Backed Historical Calendar Spread Engine
Uses actual futures data from 3Y CSVs to reconstruct near/far month spreads,
compute Z-scores, probability distributions, mean reversion stats, and backtest.
"""
import pandas as pd
import numpy as np
import os
import sys
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(__file__))

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '.tmp', '3y_data')


# ---------------------------------------------------------------------------
# 1. DATA LOADING & SPREAD RECONSTRUCTION
# ---------------------------------------------------------------------------

def load_futures_data(symbol):
    """Load raw futures data with all expiry months."""
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='mixed', dayfirst=True)
    df['expiry'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='mixed', dayfirst=True)
    df = df.dropna(subset=['FH_CLOSING_PRICE', 'FH_UNDERLYING_VALUE'])
    df = df.sort_values(['date', 'expiry'])
    return df


def reconstruct_spread_series(df):
    """
    For each trading day, identify near-month and far-month futures.
    Near = closest expiry >= current date (or closest if all expired).
    Far = next expiry after near.
    Returns DataFrame with daily spread series.
    """
    records = []
    for date, group in df.groupby('date'):
        # Only consider expiries that haven't expired yet (expiry >= trade date)
        future_expiries = group[group['expiry'] >= date].sort_values('expiry')
        if len(future_expiries) < 2:
            continue

        near = future_expiries.iloc[0]
        far = future_expiries.iloc[1]

        near_price = near['FH_CLOSING_PRICE']
        far_price = far['FH_CLOSING_PRICE']
        spot = near['FH_UNDERLYING_VALUE']
        near_dte = (near['expiry'] - date).days
        far_dte = (far['expiry'] - date).days

        # Calendar spread = far - near
        spread = far_price - near_price
        # Basis: near premium over spot
        near_basis = near_price - spot
        far_basis = far_price - spot

        records.append({
            'date': date,
            'spot': spot,
            'near_price': near_price,
            'far_price': far_price,
            'near_expiry': near['expiry'],
            'far_expiry': far['expiry'],
            'near_dte': near_dte,
            'far_dte': far_dte,
            'near_oi': near['FH_OPEN_INT'],
            'far_oi': far['FH_OPEN_INT'],
            'spread': spread,          # far - near
            'near_basis': near_basis,   # near - spot
            'far_basis': far_basis,     # far - spot
            'near_basis_pct': near_basis / spot * 100,
            'far_basis_pct': far_basis / spot * 100,
            'spread_pct': spread / spot * 100,
        })

    return pd.DataFrame(records).set_index('date')


# ---------------------------------------------------------------------------
# 2. STATISTICAL ANALYSIS
# ---------------------------------------------------------------------------

def compute_statistics(spread_series):
    """Compute Z-score, Hurst, ADF, half-life on the spread series."""
    from statsmodels.tsa.stattools import adfuller

    stats = {}
    s = spread_series.dropna()
    if len(s) < 60:
        return None

    # Basic stats
    stats['mean'] = s.mean()
    stats['std'] = s.std()
    stats['median'] = s.median()
    stats['current'] = s.iloc[-1]
    stats['z_score'] = (s.iloc[-1] - s.mean()) / s.std() if s.std() > 0 else 0
    stats['min'] = s.min()
    stats['max'] = s.max()
    stats['pct_5'] = s.quantile(0.05)
    stats['pct_25'] = s.quantile(0.25)
    stats['pct_75'] = s.quantile(0.75)
    stats['pct_95'] = s.quantile(0.95)

    # Percentile of current value
    stats['current_percentile'] = (s < s.iloc[-1]).sum() / len(s) * 100

    # ADF test for stationarity
    try:
        adf_result = adfuller(s, maxlag=20)
        stats['adf_stat'] = adf_result[0]
        stats['adf_pvalue'] = adf_result[1]
        stats['is_stationary'] = adf_result[1] < 0.05
    except Exception:
        stats['adf_stat'] = np.nan
        stats['adf_pvalue'] = np.nan
        stats['is_stationary'] = False

    # Half-life (Ornstein-Uhlenbeck)
    try:
        lag = s.shift(1).dropna()
        delta = s.diff().dropna()
        aligned = pd.DataFrame({'lag': lag, 'delta': delta}).dropna()
        if len(aligned) > 10:
            from statsmodels.api import OLS, add_constant
            X = add_constant(aligned['lag'].values)
            model = OLS(aligned['delta'].values, X).fit()
            theta = model.params[1]
            if theta < 0:
                stats['half_life'] = -np.log(2) / theta
            else:
                stats['half_life'] = np.inf
        else:
            stats['half_life'] = np.nan
    except Exception:
        stats['half_life'] = np.nan

    # Hurst exponent
    try:
        stats['hurst'] = _hurst_exponent(s.values)
    except Exception:
        stats['hurst'] = np.nan

    # Probability of spread being negative (backwardation)
    stats['pct_negative'] = (s < 0).sum() / len(s) * 100

    # Mean reversion rate: % of times spread crosses mean within 20 days
    stats['mean_cross_rate'] = _mean_cross_rate(s, window=20)

    return stats


def _hurst_exponent(ts, max_lag=100):
    """Compute Hurst exponent via R/S analysis."""
    ts = np.array(ts, dtype=float)
    n = len(ts)
    max_lag = min(max_lag, n // 4)
    if max_lag < 4:
        return np.nan

    lags = range(2, max_lag)
    rs = []
    for lag in lags:
        chunks = [ts[i:i + lag] for i in range(0, n - lag + 1, lag)]
        rs_vals = []
        for chunk in chunks:
            if len(chunk) < 2:
                continue
            mean_c = np.mean(chunk)
            dev = np.cumsum(chunk - mean_c)
            r = np.max(dev) - np.min(dev)
            s = np.std(chunk, ddof=1)
            if s > 0:
                rs_vals.append(r / s)
        if rs_vals:
            rs.append(np.mean(rs_vals))
        else:
            rs.append(np.nan)

    valid = [(l, r) for l, r in zip(lags, rs) if not np.isnan(r) and r > 0]
    if len(valid) < 3:
        return np.nan
    log_lags = np.log([v[0] for v in valid])
    log_rs = np.log([v[1] for v in valid])
    slope, _ = np.polyfit(log_lags, log_rs, 1)
    return slope


def _mean_cross_rate(series, window=20):
    """What % of deviations from mean revert within `window` days."""
    mean = series.mean()
    above = series > mean
    crosses = 0
    total = 0
    for i in range(len(series) - window):
        if above.iloc[i]:
            # Check if it crosses below mean within window
            future = series.iloc[i + 1:i + 1 + window]
            if (future <= mean).any():
                crosses += 1
            total += 1
        elif not above.iloc[i]:
            future = series.iloc[i + 1:i + 1 + window]
            if (future >= mean).any():
                crosses += 1
            total += 1
    return crosses / total * 100 if total > 0 else 0


# ---------------------------------------------------------------------------
# 3. BACKTEST ENGINE
# ---------------------------------------------------------------------------

def backtest_calendar_spread(spread_df, entry_z=2.0, stop_z=3.5, time_stop=20,
                              lookback=60):
    """
    Backtest calendar spread trades on actual historical data.

    Strategy: When spread Z-score is extreme, enter expecting mean reversion.
    - Z < -entry_z: Spread is unusually negative (far much cheaper than near)
                     → BUY the spread (buy far, sell near)
    - Z > +entry_z: Spread is unusually positive
                     → SELL the spread (sell far, buy near)

    Exit: Z crosses 0, stop-loss at ±stop_z, or time stop.
    """
    s = spread_df['spread'].copy()
    roll_mean = s.rolling(lookback).mean()
    roll_std = s.rolling(lookback).std()
    z = (s - roll_mean) / roll_std

    trades = []
    pos = 0
    entry_date = entry_z_val = entry_spread = None

    valid_start = lookback
    for i in range(valid_start, len(s)):
        if np.isnan(z.iloc[i]):
            continue
        curr_z = z.iloc[i]
        curr_spread = s.iloc[i]
        curr_date = spread_df.index[i]

        if pos == 0:
            # Entry: spread is anomalously negative → buy spread
            if curr_z < -entry_z:
                pos = 1  # long spread (buy far, sell near)
                entry_date = curr_date
                entry_z_val = curr_z
                entry_spread = curr_spread
            elif curr_z > entry_z:
                pos = -1  # short spread (sell far, buy near)
                entry_date = curr_date
                entry_z_val = curr_z
                entry_spread = curr_spread
        else:
            days = (curr_date - entry_date).days
            exit_trade = False
            reason = ""

            if pos == 1:  # long spread
                if curr_z > 0:
                    exit_trade, reason = True, "Z crossed 0 (mean reversion)"
                elif curr_z < -stop_z:
                    exit_trade, reason = True, f"STOP-LOSS (Z={curr_z:.2f})"
                elif days >= time_stop:
                    exit_trade, reason = True, f"TIME STOP ({days}d)"
            else:  # short spread
                if curr_z < 0:
                    exit_trade, reason = True, "Z crossed 0 (mean reversion)"
                elif curr_z > stop_z:
                    exit_trade, reason = True, f"STOP-LOSS (Z={curr_z:.2f})"
                elif days >= time_stop:
                    exit_trade, reason = True, f"TIME STOP ({days}d)"

            if exit_trade:
                spread_pnl = (curr_spread - entry_spread) * pos
                trades.append({
                    'entry_date': entry_date,
                    'exit_date': curr_date,
                    'days': days,
                    'direction': 'BUY spread' if pos == 1 else 'SELL spread',
                    'entry_z': entry_z_val,
                    'exit_z': curr_z,
                    'entry_spread': entry_spread,
                    'exit_spread': curr_spread,
                    'pnl': spread_pnl,
                    'reason': reason,
                    'win': spread_pnl > 0,
                })
                pos = 0

    return trades


# ---------------------------------------------------------------------------
# 4. ANALYSIS FOR A SINGLE SYMBOL
# ---------------------------------------------------------------------------

def analyze_symbol(symbol, print_output=True):
    """Full analysis for one symbol: spread history + stats + backtest."""
    df = load_futures_data(symbol)
    if df is None:
        if print_output:
            print(f"  [MISSING] No data for {symbol}")
        return None

    spread_df = reconstruct_spread_series(df)
    if len(spread_df) < 60:
        if print_output:
            print(f"  [INSUFFICIENT] Only {len(spread_df)} days for {symbol}")
        return None

    stats = compute_statistics(spread_df['spread'])
    if stats is None:
        return None

    trades = backtest_calendar_spread(spread_df)

    result = {
        'symbol': symbol,
        'spread_df': spread_df,
        'stats': stats,
        'trades': trades,
        'days': len(spread_df),
        'date_range': f"{spread_df.index[0].strftime('%Y-%m-%d')} to {spread_df.index[-1].strftime('%Y-%m-%d')}",
    }

    # Current state
    latest = spread_df.iloc[-1]
    result['current'] = {
        'spot': latest['spot'],
        'near_price': latest['near_price'],
        'far_price': latest['far_price'],
        'spread': latest['spread'],
        'spread_pct': latest['spread_pct'],
        'near_basis': latest['near_basis'],
        'far_basis': latest['far_basis'],
        'near_dte': latest['near_dte'],
        'far_dte': latest['far_dte'],
        'z_score': stats['z_score'],
    }

    # Setup classification
    near_b = latest['near_basis']
    far_b = latest['far_basis']
    if near_b > 0 and far_b < 0:
        result['setup'] = 'CLASSIC'  # Best: near premium, far discount
    elif near_b < 0 and far_b < 0:
        result['setup'] = 'BOTH-DISCOUNT'
    elif near_b > 0 and far_b > 0:
        result['setup'] = 'BOTH-PREMIUM'
    else:
        result['setup'] = 'REVERSE'  # near discount, far premium

    if print_output:
        _print_analysis(result)

    return result


def _print_analysis(r):
    """Print full analysis for one symbol."""
    s = r['stats']
    c = r['current']
    sym = r['symbol']

    print(f"\n{'='*80}")
    print(f"  {sym} — HISTORICAL CALENDAR SPREAD ANALYSIS")
    print(f"{'='*80}")
    print(f"  Data: {r['date_range']} ({r['days']} trading days)")

    # Current State
    print(f"\n  ── CURRENT STATE ──")
    print(f"  Spot: ₹{c['spot']:.2f}")
    print(f"  Near Future: ₹{c['near_price']:.2f} (basis: {c['near_basis']:+.2f}, DTE: {c['near_dte']}d)")
    print(f"  Far Future:  ₹{c['far_price']:.2f} (basis: {c['far_basis']:+.2f}, DTE: {c['far_dte']}d)")
    print(f"  Spread (Far-Near): ₹{c['spread']:.2f} ({c['spread_pct']:+.3f}%)")
    print(f"  Setup: {r['setup']}")
    print(f"  Z-Score: {c['z_score']:+.2f}")

    # Historical Distribution
    print(f"\n  ── SPREAD DISTRIBUTION (₹) ──")
    print(f"  Mean: {s['mean']:+.2f} | Std: {s['std']:.2f} | Median: {s['median']:+.2f}")
    print(f"  Range: [{s['min']:+.2f}, {s['max']:+.2f}]")
    print(f"  5th pct: {s['pct_5']:+.2f} | 25th: {s['pct_25']:+.2f} | 75th: {s['pct_75']:+.2f} | 95th: {s['pct_95']:+.2f}")
    print(f"  Current at {s['current_percentile']:.1f}th percentile")
    print(f"  % of days spread < 0 (backwardation): {s['pct_negative']:.1f}%")

    # Statistical Tests
    print(f"\n  ── MEAN REVERSION STATISTICS ──")
    print(f"  Z-Score: {s['z_score']:+.2f}")
    print(f"  ADF Statistic: {s['adf_stat']:.3f} (p={s['adf_pvalue']:.4f}) → {'STATIONARY ✓' if s['is_stationary'] else 'NON-STATIONARY ✗'}")
    hl = s['half_life']
    if np.isinf(hl) or np.isnan(hl):
        print(f"  Half-Life: N/A (no mean reversion)")
    else:
        print(f"  Half-Life: {hl:.1f} days")
    print(f"  Hurst Exponent: {s['hurst']:.3f} → {'MEAN-REVERTING ✓' if s['hurst'] < 0.5 else 'TRENDING ✗'}")
    print(f"  Mean Cross Rate (20d): {s['mean_cross_rate']:.1f}%")

    # Backtest
    trades = r['trades']
    if trades:
        wins = [t for t in trades if t['win']]
        losses = [t for t in trades if not t['win']]
        total = len(trades)
        wr = len(wins) / total * 100
        avg_win = np.mean([t['pnl'] for t in wins]) if wins else 0
        avg_loss = np.mean([t['pnl'] for t in losses]) if losses else 0
        total_pnl = sum(t['pnl'] for t in trades)

        print(f"\n  ── BACKTEST (Z entry=±2.0, stop=±3.5, time=20d) ──")
        print(f"  Total Trades: {total} | Wins: {len(wins)} | Losses: {len(losses)} | WR: {wr:.1f}%")
        print(f"  Avg Win: ₹{avg_win:+.2f} | Avg Loss: ₹{avg_loss:+.2f}")
        print(f"  Total P&L: ₹{total_pnl:+.2f}")
        if wins and losses:
            print(f"  Profit Factor: {abs(sum(t['pnl'] for t in wins) / sum(t['pnl'] for t in losses)):.2f}")
        print(f"  Max Win: ₹{max(t['pnl'] for t in trades):+.2f} | Max Loss: ₹{min(t['pnl'] for t in trades):+.2f}")

        # Recent trades
        print(f"\n  Last 5 trades:")
        for t in trades[-5:]:
            marker = "✓" if t['win'] else "✗"
            print(f"    {marker} {t['entry_date'].strftime('%Y-%m-%d')} → {t['exit_date'].strftime('%Y-%m-%d')} "
                  f"({t['days']}d) | {t['direction']} | Z: {t['entry_z']:+.2f}→{t['exit_z']:+.2f} | "
                  f"₹{t['entry_spread']:+.1f}→₹{t['exit_spread']:+.1f} | P&L: ₹{t['pnl']:+.2f} | {t['reason']}")
    else:
        print(f"\n  ── BACKTEST ──")
        print(f"  No trades triggered (spread never hit Z=±2.0)")

    # Verdict
    print(f"\n  ── VERDICT ──")
    score = 0
    reasons = []
    if s['is_stationary']:
        score += 25
        reasons.append("Stationary spread (ADF p<0.05)")
    if s['hurst'] < 0.5:
        score += 25
        reasons.append(f"Mean-reverting (H={s['hurst']:.2f})")
    if not np.isinf(hl) and not np.isnan(hl) and hl < 15:
        score += 15
        reasons.append(f"Fast reversion (HL={hl:.0f}d)")
    if s['mean_cross_rate'] > 80:
        score += 15
        reasons.append(f"High cross rate ({s['mean_cross_rate']:.0f}%)")
    if trades:
        wr_val = len([t for t in trades if t['win']]) / len(trades) * 100
        if wr_val > 60:
            score += 20
            reasons.append(f"Profitable backtest (WR={wr_val:.0f}%)")

    print(f"  Reliability Score: {score}/100")
    for reason in reasons:
        print(f"    + {reason}")
    if abs(s['z_score']) > 2:
        print(f"  ⚡ ACTIVE SIGNAL: Z={s['z_score']:+.2f} — spread at extreme")
    elif abs(s['z_score']) > 1.5:
        print(f"  ⚠ WATCHLIST: Z={s['z_score']:+.2f} — approaching entry zone")


# ---------------------------------------------------------------------------
# 5. SCAN ALL SYMBOLS WITH DATA
# ---------------------------------------------------------------------------

def scan_all():
    """Scan all symbols that have 3Y data and rank by opportunity."""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_3Y.csv')]
    symbols = [f.replace('_3Y.csv', '') for f in files]
    print(f"\nFound {len(symbols)} symbols with historical data")
    print(f"Scanning for calendar spread opportunities...\n")

    results = []
    for i, sym in enumerate(sorted(symbols)):
        try:
            r = analyze_symbol(sym, print_output=False)
            if r and r['stats']:
                results.append(r)
        except Exception as e:
            pass
        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{len(symbols)}...")

    print(f"\nAnalyzed {len(results)} symbols successfully\n")

    # Sort by |Z-score| descending — most extreme spreads first
    results.sort(key=lambda x: abs(x['stats']['z_score']), reverse=True)

    # Print summary table
    print(f"{'='*130}")
    print(f"  {'Symbol':<12} {'Spread':>8} {'Z-Score':>8} {'Pctl':>6} {'Setup':<15} {'ADF-p':>7} {'Hurst':>6} {'HL(d)':>6} {'CrossR':>7} {'BT-WR':>6} {'BT-PnL':>9} {'Score':>6}")
    print(f"{'='*130}")

    active_signals = []
    for r in results:
        s = r['stats']
        c = r['current']
        trades = r['trades']
        wr = len([t for t in trades if t['win']]) / len(trades) * 100 if trades else 0
        total_pnl = sum(t['pnl'] for t in trades) if trades else 0
        hl_str = f"{s['half_life']:.0f}" if not (np.isinf(s['half_life']) or np.isnan(s['half_life'])) else "N/A"

        # Compute score
        score = 0
        if s['is_stationary']: score += 25
        if s['hurst'] < 0.5: score += 25
        if not (np.isinf(s['half_life']) or np.isnan(s['half_life'])) and s['half_life'] < 15: score += 15
        if s['mean_cross_rate'] > 80: score += 15
        if trades and wr > 60: score += 20

        z_marker = ""
        if abs(s['z_score']) > 2:
            z_marker = " ⚡"
            active_signals.append(r)
        elif abs(s['z_score']) > 1.5:
            z_marker = " ⚠"

        print(f"  {r['symbol']:<12} {c['spread']:>+8.2f} {s['z_score']:>+8.2f}{z_marker} {s['current_percentile']:>5.1f}% {r['setup']:<15} {s['adf_pvalue']:>7.4f} {s['hurst']:>6.3f} {hl_str:>6} {s['mean_cross_rate']:>6.1f}% {wr:>5.1f}% {total_pnl:>+9.2f} {score:>5}")

    # Active signals detail
    if active_signals:
        print(f"\n\n{'█'*80}")
        print(f"  ACTIVE SIGNALS (|Z| > 2.0)")
        print(f"{'█'*80}")
        for r in active_signals:
            _print_analysis(r)

    return results


# ---------------------------------------------------------------------------
# 6. MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', '-s', type=str, help='Analyze single symbol')
    parser.add_argument('--scan', action='store_true', help='Scan all symbols')
    parser.add_argument('--symbols', nargs='+', help='Analyze multiple symbols')
    args = parser.parse_args()

    if args.symbol:
        analyze_symbol(args.symbol.upper())
    elif args.symbols:
        for sym in args.symbols:
            analyze_symbol(sym.upper())
    elif args.scan:
        scan_all()
    else:
        # Default: analyze user's active symbols + scan
        print("█" * 80)
        print("  DATA-BACKED HISTORICAL CALENDAR SPREAD ENGINE")
        print("█" * 80)
        print("  Using actual futures data from 3Y CSVs")
        print("  Near/Far months reconstructed from FH_EXPIRY_DT")

        key_symbols = ['SBICARD', 'RVNL']
        for sym in key_symbols:
            analyze_symbol(sym)

        print("\n\n")
        scan_all()
