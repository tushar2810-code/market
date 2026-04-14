"""
Renaissance-Style Deep Dive — "Jim Simons" Quantitative Validation Engine

Implements the Level 4 Advanced Quantitative Validation from the pair trading directive:
  1. Kalman Filter — Estimate "True Price Ratio" and strip out noise
  2. Bayesian Sentinel (HMM) — Detect Hidden Market Regimes (Trending, Mean-Reverting, Volatile)
  3. Operator Activity Detection — Flag anomalous volume spikes (>3x average)
  4. Hurst Exponent — Is the spread actually mean-reverting? (H < 0.5 = yes)
  5. Augmented Dickey-Fuller — Stationarity test on the spread
  6. Half-Life of Mean Reversion — How many days for spread to revert 50%?
  7. Cointegration Test (Engle-Granger) — Are these prices truly cointegrated?
  8. Regime-Aware Backtest — Only trade in mean-reverting regimes

Usage:
    python3 execution/renaissance_deep_dive.py --symA SUNPHARMA --symB CIPLA
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
import warnings
warnings.filterwarnings('ignore')

from datetime import datetime
from scipy import stats as scipy_stats

# Quant libraries (manual Kalman — pykalman has scipy compat issues)
from hmmlearn.hmm import GaussianHMM
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant


def manual_kalman_1d(observations, Q=0.001, R=0.01):
    """
    Manual 1D Kalman Filter (avoids pykalman scipy issues).
    State: true price ratio (random walk).
    Q: process noise variance (how fast the true ratio can change)
    R: observation noise variance (how noisy the observed ratio is)
    """
    n = len(observations)
    x_est = np.zeros(n)  # state estimates
    P_est = np.zeros(n)  # error covariance estimates
    
    # Initialize
    x_est[0] = observations[0]
    P_est[0] = 1.0
    
    for t in range(1, n):
        # Predict
        x_pred = x_est[t-1]
        P_pred = P_est[t-1] + Q
        
        # Update
        K = P_pred / (P_pred + R)  # Kalman gain
        x_est[t] = x_pred + K * (observations[t] - x_pred)
        P_est[t] = (1 - K) * P_pred
    
    return x_est, P_est

sys.path.append(os.path.join(os.path.dirname(__file__)))

DATA_DIR = '.tmp/3y_data'


# =============================================================================
# DATA LOADING
# =============================================================================

def load_data(symbol):
    """Load historical futures data and build continuous series."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        print(f"  ❌ Data file not found: {path}")
        return None

    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

    # Build continuous series (nearest expiry per date)
    df['FH_EXPIRY_DT_parsed'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    if 'FH_INSTRUMENT' in df.columns:
        df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
    continuous = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT_parsed'].idxmin()].copy()
    continuous = continuous.sort_values('FH_TIMESTAMP')

    cols_to_keep = ['FH_TIMESTAMP', 'FH_CLOSING_PRICE']
    if 'FH_TOT_TRADED_QTY' in continuous.columns:
        cols_to_keep.append('FH_TOT_TRADED_QTY')
    if 'FH_OPEN_INT' in continuous.columns:
        cols_to_keep.append('FH_OPEN_INT')
    if 'FH_MARKET_LOT' in continuous.columns:
        cols_to_keep.append('FH_MARKET_LOT')

    result = continuous[cols_to_keep].set_index('FH_TIMESTAMP')
    # Ensure numeric and drop NaN prices
    result['FH_CLOSING_PRICE'] = pd.to_numeric(result['FH_CLOSING_PRICE'], errors='coerce')
    result = result.dropna(subset=['FH_CLOSING_PRICE'])
    return result


# =============================================================================
# MODULE 1: KALMAN FILTER — True Price Ratio Estimation
# =============================================================================

def kalman_filter_ratio(prices_a, prices_b):
    """
    Use a Kalman Filter to estimate the "true" hedge ratio between two assets.
    
    Instead of raw prices (which cause numerical overflow), we work on the
    price RATIO directly and use a 1D Kalman Filter to smooth it.
    Also produces a Kalman-filtered hedge ratio via normalized regression.
    
    Returns: filtered_state_means (alpha, beta over time), filtered spread, kalman Z-scores
    """
    print("\n" + "━" * 80)
    print("  MODULE 1: KALMAN FILTER — True Price Estimation")
    print("━" * 80)

    # --- Approach: Kalman-smooth the price ratio directly ---
    # Ensure clean numeric data
    valid_mask = prices_a.notna() & prices_b.notna() & (prices_b > 0)
    pa = prices_a[valid_mask].values.astype(float)
    pb = prices_b[valid_mask].values.astype(float)
    valid_index = prices_a[valid_mask].index
    
    ratio = pa / pb
    n = len(ratio)
    
    # 1D Kalman Filter on the ratio — state is the "true ratio"
    ratio_diff = np.diff(ratio)
    ratio_var = np.var(ratio_diff[np.isfinite(ratio_diff)])
    Q = ratio_var * 0.1   # Process noise: true ratio drifts slowly
    R = ratio_var          # Observation noise: observed ratio is noisy
    
    kalman_ratio, kalman_P = manual_kalman_1d(ratio, Q=Q, R=R)

    # Kalman-filtered spread = observed ratio - smoothed ratio
    kalman_spread = ratio - kalman_ratio

    # --- Also compute OLS hedge ratio for reference ---
    X = add_constant(pb)
    ols_result = OLS(pa, X).fit()
    alpha_original = ols_result.params[0]
    beta_original = ols_result.params[1]

    # Rolling hedge ratio (60d OLS)
    rolling_betas = []
    for i in range(60, n):
        chunk_a = pa[i-60:i]
        chunk_b = pb[i-60:i]
        X_c = add_constant(chunk_b)
        try:
            r = OLS(chunk_a, X_c).fit()
            rolling_betas.append(r.params[1])
        except:
            rolling_betas.append(np.nan)
    
    # Kalman Z-scores (rolling 30d on the Kalman spread)
    ks = pd.Series(kalman_spread, index=valid_index)
    ks_mean = ks.rolling(30).mean()
    ks_std = ks.rolling(30).std()
    kalman_z = (ks - ks_mean) / ks_std

    # Current state
    print(f"\n  Current Kalman State:")
    print(f"    Observed Ratio:     {ratio[-1]:.4f}")
    print(f"    Kalman True Ratio:  {kalman_ratio[-1]:.4f}")
    print(f"    Kalman Spread:      {kalman_spread[-1]:.4f}")
    print(f"    Kalman Z-Score:     {kalman_z.iloc[-1]:.2f}")
    
    print(f"\n  OLS Hedge Ratio (Full Period):")
    print(f"    Alpha (intercept): {alpha_original:.4f}")
    print(f"    Beta (hedge ratio): {beta_original:.4f}")
    print(f"    R²: {ols_result.rsquared:.4f}")

    # Hedge ratio stability (from rolling betas)
    if rolling_betas:
        rb = pd.Series(rolling_betas)
        beta_30d_std = rb.tail(30).std()
        beta_full_std = rb.std()
        print(f"\n  Rolling Hedge Ratio Stability (60d OLS):")
        print(f"    Current Beta:       {rb.iloc[-1]:.4f}")
        print(f"    Beta 30d StdDev:    {beta_30d_std:.4f}")
        print(f"    Beta full StdDev:   {beta_full_std:.4f}")
        
        stability = "✅ STABLE" if beta_30d_std < 0.05 else ("🟡 MODERATE" if beta_30d_std < 0.1 else "❌ UNSTABLE")
        print(f"    Assessment:         {stability}")

        # Beta trend (is it drifting?)
        beta_recent = rb.tail(60)
        beta_slope = np.polyfit(range(len(beta_recent)), beta_recent.values, 1)[0]
        drift_dir = "UPWARD" if beta_slope > 0.001 else ("DOWNWARD" if beta_slope < -0.001 else "FLAT")
        print(f"    Beta 60d trend:     {drift_dir} ({beta_slope:.6f}/day)")
    else:
        beta_30d_std = 0

    return np.full(n, alpha_original), np.full(n, beta_original), kalman_spread, kalman_z


# =============================================================================
# MODULE 2: HMM REGIME DETECTION — Bayesian Sentinel
# =============================================================================

def hmm_regime_detection(spread_series):
    """
    Use a Gaussian HMM to detect hidden market regimes in the spread.
    
    3 states:
      - Mean-Reverting (low vol, spread oscillates around mean)
      - Trending (spread drifts, higher vol)
      - Volatile (high vol, sharp moves — dangerous for pair trades)
    
    Returns: regime labels, current regime assessment
    """
    print("\n" + "━" * 80)
    print("  MODULE 2: BAYESIAN SENTINEL (HMM) — Regime Detection")
    print("━" * 80)

    # Features for HMM: spread returns + rolling volatility
    spread_returns = spread_series.pct_change().dropna().replace([np.inf, -np.inf], 0)
    
    # Use spread returns + absolute returns as features
    features = np.column_stack([
        spread_returns.values,
        np.abs(spread_returns.values)
    ])

    # Fit 3-state Gaussian HMM
    best_model = None
    best_score = -np.inf
    
    for seed in range(5):  # Multiple random starts for stability
        try:
            model = GaussianHMM(
                n_components=3,
                covariance_type='full',
                n_iter=200,
                random_state=seed,
                tol=0.01
            )
            model.fit(features)
            score = model.score(features)
            if score > best_score:
                best_score = score
                best_model = model
        except Exception:
            continue

    if best_model is None:
        print("  ❌ HMM fitting failed")
        return None, None

    # Predict regimes
    regimes = best_model.predict(features)
    
    # Label regimes by volatility (mean of absolute returns per state)
    state_vols = {}
    state_means = {}
    for s in range(3):
        mask = regimes == s
        if mask.sum() > 0:
            state_vols[s] = np.abs(spread_returns.values[mask]).mean()
            state_means[s] = spread_returns.values[mask].mean()

    # Sort states by volatility: lowest = Mean-Reverting, highest = Volatile
    sorted_states = sorted(state_vols.keys(), key=lambda x: state_vols[x])
    state_labels = {}
    label_names = ["MEAN-REVERTING", "TRENDING", "VOLATILE"]
    label_icons = ["🟢", "🟡", "🔴"]
    
    for i, s in enumerate(sorted_states):
        state_labels[s] = label_names[i]

    # Regime statistics
    print(f"\n  Regime Breakdown:")
    print(f"  {'State':<20} {'Days':>6} {'%':>6} {'Avg Ret':>10} {'Volatility':>12}")
    print(f"  {'─'*58}")
    
    total = len(regimes)
    for s in sorted_states:
        mask = regimes == s
        days = mask.sum()
        pct = days / total * 100
        avg_ret = state_means[s] * 100
        vol = state_vols[s] * 100
        i = sorted_states.index(s)
        print(f"  {label_icons[i]} {state_labels[s]:<17} {days:>6} {pct:>5.1f}% {avg_ret:>+9.4f}% {vol:>11.4f}%")

    # Current regime
    current_regime_id = regimes[-1]
    current_regime = state_labels[current_regime_id]
    current_icon = label_icons[sorted_states.index(current_regime_id)]
    
    # Recent regime history (last 20 days)
    recent_regimes = regimes[-20:]
    regime_counts = {}
    for r in recent_regimes:
        label = state_labels[r]
        regime_counts[label] = regime_counts.get(label, 0) + 1

    print(f"\n  Current Regime: {current_icon} {current_regime}")
    print(f"  Last 20 days:  ", end="")
    for label, count in sorted(regime_counts.items(), key=lambda x: -x[1]):
        print(f"{label}: {count}d  ", end="")
    print()

    # Regime transition probability
    print(f"\n  Transition Matrix (probability of switching):")
    trans_mat = best_model.transmat_
    print(f"  {'':>20} ", end="")
    for s in sorted_states:
        print(f"→{state_labels[s][:8]:>10} ", end="")
    print()
    for s_from in sorted_states:
        i_from = sorted_states.index(s_from)
        print(f"  {state_labels[s_from]:>20} ", end="")
        for s_to in sorted_states:
            print(f"{trans_mat[s_from, s_to]:>10.2%} ", end="")
        print()

    # Trading rule
    if current_regime == "VOLATILE":
        print(f"\n  ⛔ RULE: BLOCK TRADE — Volatile regime detected")
        print(f"     Mean reversion is unreliable. High risk of stop-outs.")
    elif current_regime == "TRENDING":
        print(f"\n  🟡 CAUTION: Trending regime — spread may not revert quickly")
        print(f"     Mean reversion trades have lower win rate in trending regimes.")
    else:
        print(f"\n  ✅ FAVORABLE: Mean-Reverting regime — pair trading conditions are ideal")

    return regimes, state_labels


# =============================================================================
# MODULE 3: OPERATOR ACTIVITY DETECTION
# =============================================================================

def operator_activity_check(df_a, df_b, sym_a, sym_b):
    """
    Detect anomalous volume/OI spikes that may indicate operator manipulation.
    Flag any day where volume > 3x 20-day average.
    """
    print("\n" + "━" * 80)
    print("  MODULE 3: OPERATOR ACTIVITY DETECTION")
    print("━" * 80)

    alerts = []
    
    for sym, df in [(sym_a, df_a), (sym_b, df_b)]:
        vol_col = 'FH_TOT_TRADED_QTY'
        oi_col = 'FH_OPEN_INT'
        
        if vol_col not in df.columns:
            print(f"  ⚠️  {sym}: No volume data available")
            continue
        
        vol = df[vol_col].astype(float)
        vol_20ma = vol.rolling(20).mean()
        vol_ratio = vol / vol_20ma
        
        # Last 30 days anomalies
        recent_spikes = vol_ratio.tail(30)
        spike_days = recent_spikes[recent_spikes > 3.0]
        
        print(f"\n  {sym}:")
        print(f"    Current Volume:    {vol.iloc[-1]:,.0f}")
        print(f"    20D Avg Volume:    {vol_20ma.iloc[-1]:,.0f}")
        print(f"    Volume Ratio:      {vol_ratio.iloc[-1]:.2f}x")
        
        if len(spike_days) > 0:
            print(f"    ⚠️  {len(spike_days)} volume spikes (>3x) in last 30 days:")
            for date, ratio in spike_days.items():
                print(f"       {date.date()}: {ratio:.1f}x average")
                alerts.append((sym, date.date(), ratio))
        else:
            print(f"    ✅ No anomalous volume spikes in last 30 days")
        
        # OI analysis
        if oi_col in df.columns:
            oi = df[oi_col].astype(float)
            oi_change = oi.pct_change(fill_method=None).tail(10)
            big_oi_moves = oi_change[abs(oi_change) > 0.15]
            
            if len(big_oi_moves) > 0:
                print(f"    ⚠️  Large OI changes (>15%) in last 10 days:")
                for date, chg in big_oi_moves.items():
                    print(f"       {date.date()}: {chg:+.1%}")
            else:
                print(f"    ✅ OI changes normal in last 10 days")

    if alerts:
        print(f"\n  ⛔ OPERATOR ALERT: {len(alerts)} anomalous volume spikes detected")
        print(f"     Possible institutional/operator activity. Exercise caution.")
    else:
        print(f"\n  ✅ No operator activity flags. Volume pattern is normal.")
    
    return alerts


# =============================================================================
# MODULE 4: HURST EXPONENT — Mean Reversion Test
# =============================================================================

def hurst_exponent(spread_series):
    """
    Calculate the Hurst Exponent using the Rescaled Range (R/S) method.
    H < 0.5 → Mean-Reverting (GOOD for pair trading)
    H = 0.5 → Random Walk (BAD — no edge)
    H > 0.5 → Trending (BAD — will diverge further)
    """
    print("\n" + "━" * 80)
    print("  MODULE 4: HURST EXPONENT — Mean Reversion Proof")
    print("━" * 80)

    ts = spread_series.dropna().values
    n = len(ts)
    
    lags = range(2, min(100, n // 4))
    tau = []
    rs_values = []
    
    for lag in lags:
        # Divide into chunks
        chunks = n // lag
        rs_list = []
        for i in range(chunks):
            chunk = ts[i * lag:(i + 1) * lag]
            mean_chunk = np.mean(chunk)
            deviations = chunk - mean_chunk
            cumulative = np.cumsum(deviations)
            R = max(cumulative) - min(cumulative)
            S = np.std(chunk, ddof=1)
            if S > 0:
                rs_list.append(R / S)
        if rs_list:
            tau.append(lag)
            rs_values.append(np.mean(rs_list))

    if len(tau) < 2:
        print("  ❌ Insufficient data for Hurst calculation")
        return None

    log_tau = np.log(tau)
    log_rs = np.log(rs_values)
    
    H, intercept = np.polyfit(log_tau, log_rs, 1)

    print(f"\n  Hurst Exponent: {H:.4f}")
    
    if H < 0.4:
        assessment = "✅ STRONGLY MEAN-REVERTING — Excellent for pair trading"
    elif H < 0.5:
        assessment = "✅ MEAN-REVERTING — Favorable conditions"
    elif H < 0.55:
        assessment = "🟡 BORDERLINE — Near random walk, weak edge"
    elif H < 0.65:
        assessment = "⚠️ TRENDING TENDENCY — Spread may continue diverging"
    else:
        assessment = "❌ STRONGLY TRENDING — DO NOT pair trade"
    
    print(f"  Assessment: {assessment}")
    print(f"\n  Interpretation:")
    print(f"    H < 0.5 → Mean-Reverting (spread tends to revert)")
    print(f"    H = 0.5 → Random Walk (no statistical edge)")
    print(f"    H > 0.5 → Trending (spread tends to continue diverging)")

    # Sub-period analysis
    periods = {
        'Full Period': ts,
        'Last 250d': ts[-250:] if len(ts) >= 250 else None,
        'Last 120d': ts[-120:] if len(ts) >= 120 else None,
        'Last 60d': ts[-60:] if len(ts) >= 60 else None,
    }
    
    print(f"\n  Sub-Period Hurst Analysis:")
    for name, data in periods.items():
        if data is None or len(data) < 30:
            continue
        sub_lags = range(2, min(30, len(data) // 4))
        sub_tau = []
        sub_rs = []
        for lag in sub_lags:
            chunks = len(data) // lag
            rs_list = []
            for i in range(chunks):
                chunk = data[i * lag:(i + 1) * lag]
                mean_c = np.mean(chunk)
                dev = chunk - mean_c
                cum = np.cumsum(dev)
                R = max(cum) - min(cum)
                S = np.std(chunk, ddof=1)
                if S > 0:
                    rs_list.append(R / S)
            if rs_list:
                sub_tau.append(lag)
                sub_rs.append(np.mean(rs_list))
        if len(sub_tau) >= 2:
            h_sub, _ = np.polyfit(np.log(sub_tau), np.log(sub_rs), 1)
            icon = "✅" if h_sub < 0.5 else ("🟡" if h_sub < 0.55 else "❌")
            print(f"    {icon} {name:>15}: H = {h_sub:.4f}")
    
    return H


# =============================================================================
# MODULE 5: STATIONARITY & COINTEGRATION TESTS
# =============================================================================

def stationarity_tests(prices_a, prices_b, spread_series):
    """
    Run ADF (stationarity) and Engle-Granger cointegration tests.
    """
    print("\n" + "━" * 80)
    print("  MODULE 5: STATIONARITY & COINTEGRATION")
    print("━" * 80)

    # ADF on the spread
    print(f"\n  Augmented Dickey-Fuller Test (on ratio spread):")
    adf_result = adfuller(spread_series.dropna(), maxlag=20)
    adf_stat = adf_result[0]
    adf_p = adf_result[1]
    
    print(f"    ADF Statistic: {adf_stat:.4f}")
    print(f"    p-value:       {adf_p:.6f}")
    print(f"    Critical Values:")
    for key, val in adf_result[4].items():
        marker = "←" if adf_stat < val else ""
        print(f"      {key}: {val:.4f} {marker}")
    
    if adf_p < 0.01:
        print(f"    ✅ STATIONARY at 1% level — Strong mean reversion")
    elif adf_p < 0.05:
        print(f"    ✅ STATIONARY at 5% level — Moderate mean reversion")
    elif adf_p < 0.10:
        print(f"    🟡 STATIONARY at 10% level — Weak evidence")
    else:
        print(f"    ❌ NOT STATIONARY — Spread is NOT mean-reverting (p={adf_p:.4f})")

    # Engle-Granger Cointegration
    print(f"\n  Engle-Granger Cointegration Test:")
    coint_stat, coint_p, coint_crit = coint(prices_a.dropna(), prices_b.dropna())
    
    print(f"    Test Statistic: {coint_stat:.4f}")
    print(f"    p-value:        {coint_p:.6f}")
    print(f"    Critical Values: 1%={coint_crit[0]:.4f}, 5%={coint_crit[1]:.4f}, 10%={coint_crit[2]:.4f}")
    
    if coint_p < 0.05:
        print(f"    ✅ COINTEGRATED — These assets share a long-run equilibrium")
    elif coint_p < 0.10:
        print(f"    🟡 WEAKLY COINTEGRATED — Marginal evidence")
    else:
        print(f"    ❌ NOT COINTEGRATED — No long-run equilibrium relationship (p={coint_p:.4f})")

    return adf_p, coint_p


# =============================================================================
# MODULE 6: HALF-LIFE OF MEAN REVERSION
# =============================================================================

def half_life(spread_series):
    """
    Calculate the half-life of mean reversion using an Ornstein-Uhlenbeck model.
    Spread[t] - Spread[t-1] = theta * (mu - Spread[t-1]) + noise
    Half-life = -ln(2) / ln(1 + theta)
    """
    print("\n" + "━" * 80)
    print("  MODULE 6: HALF-LIFE OF MEAN REVERSION")
    print("━" * 80)

    spread = spread_series.dropna()
    spread_lag = spread.shift(1)
    delta_spread = spread - spread_lag
    
    # Drop NaN
    valid = ~(spread_lag.isna() | delta_spread.isna())
    y = delta_spread[valid].values
    x = spread_lag[valid].values
    
    x_const = add_constant(x)
    model = OLS(y, x_const)
    results = model.fit()
    
    theta = results.params[1]
    
    if theta >= 0:
        print(f"\n  Theta: {theta:.6f}")
        print(f"  ❌ Theta >= 0 — Spread is NOT mean-reverting (diverging)")
        print(f"     This is a CRITICAL red flag. The spread has no tendency to revert.")
        return None
    
    hl = -np.log(2) / np.log(1 + theta)
    
    print(f"\n  Theta (speed of reversion): {theta:.6f}")
    print(f"  Half-Life: {hl:.1f} trading days")
    
    if hl < 10:
        assessment = "✅ FAST — Excellent for short-term pair trades"
    elif hl < 20:
        assessment = "✅ MODERATE — Good for pair trades (2-4 week horizon)"
    elif hl < 40:
        assessment = "🟡 SLOW — Requires patience, higher capital at risk"
    elif hl < 60:
        assessment = "⚠️ VERY SLOW — Position may take 2+ months to converge"
    else:
        assessment = "❌ TOO SLOW — Not practical for pair trading"
    
    print(f"  Assessment: {assessment}")
    
    # Sub-period half-lives
    print(f"\n  Sub-Period Half-Life:")
    for name, tail_n in [('Last 250d', 250), ('Last 120d', 120), ('Last 60d', 60)]:
        sub_spread = spread.tail(tail_n)
        sub_lag = sub_spread.shift(1)
        sub_delta = sub_spread - sub_lag
        valid = ~(sub_lag.isna() | sub_delta.isna())
        if valid.sum() < 20:
            continue
        sub_y = sub_delta[valid].values
        sub_x = sub_lag[valid].values
        sub_x_c = add_constant(sub_x)
        try:
            sub_res = OLS(sub_y, sub_x_c).fit()
            sub_theta = sub_res.params[1]
            if sub_theta < 0:
                sub_hl = -np.log(2) / np.log(1 + sub_theta)
                icon = "✅" if sub_hl < 30 else ("🟡" if sub_hl < 60 else "❌")
                print(f"    {icon} {name:>10}: {sub_hl:.1f} days (θ={sub_theta:.6f})")
            else:
                print(f"    ❌ {name:>10}: Diverging (θ={sub_theta:.6f})")
        except Exception:
            continue
    
    return hl


# =============================================================================
# MODULE 7: REGIME-AWARE BACKTEST
# =============================================================================

def regime_aware_backtest(merged, regimes, state_labels, window=30, entry_z=2.0, stop_z=3.5, time_stop=30):
    """
    Run a backtest that ONLY trades during Mean-Reverting regimes.
    Compare with unrestricted backtest to show edge.
    """
    print("\n" + "━" * 80)
    print("  MODULE 7: REGIME-AWARE BACKTEST")
    print("━" * 80)

    # Align regimes with merged data (regimes are 1 shorter due to pct_change)
    merged_bt = merged.copy()
    merged_bt['Mean'] = merged_bt['RATIO'].rolling(window=window).mean()
    merged_bt['Std'] = merged_bt['RATIO'].rolling(window=window).std()
    merged_bt['Z'] = (merged_bt['RATIO'] - merged_bt['Mean']) / merged_bt['Std']
    merged_bt = merged_bt.dropna()

    # Map regimes: pad to match length
    # regimes are from spread_returns (1 shorter than spread), align from end
    regime_series = pd.Series(index=merged_bt.index, dtype=int)
    regime_len = len(regimes)
    bt_len = len(merged_bt)
    
    if regime_len < bt_len:
        # Pad beginning with -1 (unknown)
        aligned_regimes = np.concatenate([np.full(bt_len - regime_len, -1), regimes[-(bt_len):]])
    else:
        aligned_regimes = regimes[-bt_len:]
    
    merged_bt['regime'] = aligned_regimes

    # Identify mean-reverting state
    mr_state = None
    for s, label in state_labels.items():
        if label == "MEAN-REVERTING":
            mr_state = s
            break

    def run_bt(data, filter_regime=False, regime_id=None):
        trades = []
        position = 0
        entry_ratio = 0
        entry_date = None

        for i in range(len(data)):
            row = data.iloc[i]
            z = row['Z']
            ratio = row['RATIO']
            current_date = data.index[i]
            current_regime = row.get('regime', -1)

            if position == 0:
                # Only enter if in correct regime (or unrestricted)
                if filter_regime and current_regime != regime_id:
                    continue

                if z < -entry_z:
                    position = 1; entry_ratio = ratio; entry_date = current_date
                elif z > entry_z:
                    position = -1; entry_ratio = ratio; entry_date = current_date

            elif position != 0:
                days_held = (current_date - entry_date).days
                exit_signal = False

                if position == 1:
                    if z > 0: exit_signal = True
                    elif z < -stop_z: exit_signal = True
                    elif days_held >= time_stop: exit_signal = True
                    if exit_signal:
                        pnl = (ratio - entry_ratio) / entry_ratio
                elif position == -1:
                    if z < 0: exit_signal = True
                    elif z > stop_z: exit_signal = True
                    elif days_held >= time_stop: exit_signal = True
                    if exit_signal:
                        pnl = (entry_ratio - ratio) / entry_ratio

                if exit_signal:
                    trades.append({
                        'entry': entry_date, 'exit': current_date,
                        'return': pnl, 'duration': days_held,
                        'type': 'Long' if position == 1 else 'Short'
                    })
                    position = 0

        return trades

    # Run both backtests
    all_trades = run_bt(merged_bt, filter_regime=False)
    mr_trades = run_bt(merged_bt, filter_regime=True, regime_id=mr_state) if mr_state is not None else []

    def summarize(trades, label):
        if not trades:
            print(f"\n  {label}: No trades generated")
            return
        
        returns = [t['return'] for t in trades]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]
        wr = len(wins) / len(returns) * 100
        avg = np.mean(returns) * 100
        total = sum(returns) * 100
        max_dd = min(returns) * 100
        avg_win = np.mean(wins) * 100 if wins else 0
        avg_loss = np.mean(losses) * 100 if losses else 0
        avg_days = np.mean([t['duration'] for t in trades])
        
        print(f"\n  {label}:")
        print(f"    Trades:      {len(trades)}")
        print(f"    Win Rate:    {wr:.1f}%")
        print(f"    Avg Return:  {avg:+.2f}%")
        print(f"    Avg Win:     {avg_win:+.2f}%")
        print(f"    Avg Loss:    {avg_loss:+.2f}%")
        print(f"    Max DD:      {max_dd:+.2f}%")
        print(f"    Total:       {total:+.2f}%")
        print(f"    Avg Days:    {avg_days:.1f}")
        
        return wr, avg, total

    print(f"\n  Config: Window={window} | Entry Z={entry_z} | Stop Z={stop_z} | Time={time_stop}d")
    
    result_all = summarize(all_trades, "📊 UNRESTRICTED Backtest (All Regimes)")
    result_mr = summarize(mr_trades, "🎯 REGIME-FILTERED Backtest (Mean-Reverting Only)")

    if result_all and result_mr:
        wr_diff = result_mr[0] - result_all[0]
        print(f"\n  📈 Regime Filter Impact:")
        print(f"     Win Rate Improvement: {wr_diff:+.1f}%")
        print(f"     {'✅ Regime filter IMPROVES results' if wr_diff > 0 else '⚠️ Regime filter did NOT improve results'}")

    return all_trades, mr_trades


# =============================================================================
# FINAL VERDICT
# =============================================================================

def final_verdict(kalman_z, hurst_h, adf_p, coint_p, hl, current_regime, operator_alerts):
    """
    Aggregate all quantitative signals into a final go/no-go decision.
    """
    print("\n" + "═" * 80)
    print("  RENAISSANCE VERDICT — FINAL ASSESSMENT")
    print("═" * 80)
    
    scores = {}
    
    # 1. Kalman Z-Score
    kz = abs(kalman_z.iloc[-1]) if not pd.isna(kalman_z.iloc[-1]) else 0
    if kz > 2.0:
        scores['Kalman Signal'] = ('✅', 'Active signal', 1)
    elif kz > 1.5:
        scores['Kalman Signal'] = ('🟡', 'Approaching', 0.5)
    else:
        scores['Kalman Signal'] = ('❌', 'No signal', 0)

    # 2. Hurst
    if hurst_h is not None:
        if hurst_h < 0.45:
            scores['Hurst (Mean Reversion)'] = ('✅', f'H={hurst_h:.3f} — Strong MR', 1)
        elif hurst_h < 0.5:
            scores['Hurst (Mean Reversion)'] = ('✅', f'H={hurst_h:.3f} — MR confirmed', 0.75)
        elif hurst_h < 0.55:
            scores['Hurst (Mean Reversion)'] = ('🟡', f'H={hurst_h:.3f} — Borderline', 0.25)
        else:
            scores['Hurst (Mean Reversion)'] = ('❌', f'H={hurst_h:.3f} — Trending', 0)
    
    # 3. Stationarity (ADF)
    if adf_p < 0.01:
        scores['Stationarity (ADF)'] = ('✅', f'p={adf_p:.4f} — Strongly stationary', 1)
    elif adf_p < 0.05:
        scores['Stationarity (ADF)'] = ('✅', f'p={adf_p:.4f} — Stationary', 0.75)
    elif adf_p < 0.10:
        scores['Stationarity (ADF)'] = ('🟡', f'p={adf_p:.4f} — Marginal', 0.25)
    else:
        scores['Stationarity (ADF)'] = ('❌', f'p={adf_p:.4f} — NOT stationary', 0)
    
    # 4. Cointegration
    if coint_p < 0.05:
        scores['Cointegration'] = ('✅', f'p={coint_p:.4f} — Cointegrated', 1)
    elif coint_p < 0.10:
        scores['Cointegration'] = ('🟡', f'p={coint_p:.4f} — Marginal', 0.5)
    else:
        scores['Cointegration'] = ('❌', f'p={coint_p:.4f} — NOT cointegrated', 0)
    
    # 5. Half-Life
    if hl is not None:
        if hl < 15:
            scores['Half-Life'] = ('✅', f'{hl:.0f} days — Fast reversion', 1)
        elif hl < 30:
            scores['Half-Life'] = ('✅', f'{hl:.0f} days — Reasonable', 0.75)
        elif hl < 50:
            scores['Half-Life'] = ('🟡', f'{hl:.0f} days — Slow', 0.5)
        else:
            scores['Half-Life'] = ('❌', f'{hl:.0f} days — Too slow', 0)
    else:
        scores['Half-Life'] = ('❌', 'Diverging — no mean reversion', 0)
    
    # 6. Regime
    if current_regime == "MEAN-REVERTING":
        scores['Market Regime'] = ('✅', 'Mean-Reverting', 1)
    elif current_regime == "TRENDING":
        scores['Market Regime'] = ('🟡', 'Trending — lower confidence', 0.25)
    else:
        scores['Market Regime'] = ('❌', 'Volatile — BLOCK TRADE', 0)
    
    # 7. Operator Activity
    if len(operator_alerts) == 0:
        scores['Operator Activity'] = ('✅', 'No manipulation flags', 1)
    else:
        scores['Operator Activity'] = ('⚠️', f'{len(operator_alerts)} anomalous spikes', 0.5)

    # Print scorecard
    print(f"\n  {'Check':<25} {'Score':>5} {'Detail':<45}")
    print(f"  {'─'*78}")
    
    total_score = 0
    max_score = 0
    
    for check, (icon, detail, score) in scores.items():
        print(f"  {icon} {check:<23} {score:>4.2f}  {detail:<45}")
        total_score += score
        max_score += 1
    
    pct = total_score / max_score * 100 if max_score > 0 else 0
    
    print(f"\n  {'─'*78}")
    print(f"  COMPOSITE SCORE: {total_score:.2f} / {max_score:.0f} ({pct:.0f}%)")
    
    if pct >= 75:
        print(f"\n  ✅ VERDICT: HIGH CONVICTION — Trade is statistically supported")
        print(f"     Renaissance-grade validation passed. Proceed with standard risk management.")
    elif pct >= 50:
        print(f"\n  🟡 VERDICT: MODERATE CONVICTION — Trade has mixed signals")
        print(f"     Some quantitative tests pass, others fail. Reduce position size.")
    elif pct >= 30:
        print(f"\n  ⚠️ VERDICT: LOW CONVICTION — More signals against than for")
        print(f"     Quantitative edge is weak. Consider avoiding this trade.")
    else:
        print(f"\n  ❌ VERDICT: NO TRADE — Quantitative evidence does not support this trade")
        print(f"     The spread lacks mean-reverting properties. Walking away is the edge.")
    
    print(f"\n{'═'*80}")
    
    return pct


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Renaissance Deep Dive')
    parser.add_argument('--symA', required=True, help='First symbol')
    parser.add_argument('--symB', required=True, help='Second symbol')
    parser.add_argument('--window', type=int, default=30, help='Z-score window')
    args = parser.parse_args()

    SYM_A = args.symA
    SYM_B = args.symB

    print("╔" + "═" * 78 + "╗")
    print(f"║  RENAISSANCE DEEP DIVE — {SYM_A} / {SYM_B}".ljust(79) + "║")
    print(f"║  Jim Simons Quantitative Validation Engine".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    # Load Data
    print(f"\n  Loading data...")
    df_a = load_data(SYM_A)
    df_b = load_data(SYM_B)

    if df_a is None or df_b is None:
        print("  ❌ Data load failed.")
        return

    # Merge
    merged = df_a[['FH_CLOSING_PRICE']].join(
        df_b[['FH_CLOSING_PRICE']],
        how='inner', lsuffix='_A', rsuffix='_B'
    )
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']

    print(f"  {SYM_A}: {len(df_a)} days ({df_a.index.min().date()} → {df_a.index.max().date()})")
    print(f"  {SYM_B}: {len(df_b)} days ({df_b.index.min().date()} → {df_b.index.max().date()})")
    print(f"  Merged: {len(merged)} common trading days")

    prices_a = merged['FH_CLOSING_PRICE_A']
    prices_b = merged['FH_CLOSING_PRICE_B']
    ratio = merged['RATIO']

    # ── MODULE 1: Kalman Filter ──
    alpha, beta, kalman_spread, kalman_z = kalman_filter_ratio(prices_a, prices_b)

    # ── MODULE 2: HMM Regime Detection ──
    regimes, state_labels = hmm_regime_detection(ratio)
    current_regime = "UNKNOWN"
    if regimes is not None and state_labels is not None:
        current_regime = state_labels.get(regimes[-1], "UNKNOWN")

    # ── MODULE 3: Operator Activity ──
    operator_alerts = operator_activity_check(df_a, df_b, SYM_A, SYM_B)

    # ── MODULE 4: Hurst Exponent ──
    hurst_h = hurst_exponent(ratio)

    # ── MODULE 5: Stationarity & Cointegration ──
    adf_p, coint_p = stationarity_tests(prices_a, prices_b, ratio)

    # ── MODULE 6: Half-Life ──
    hl = half_life(ratio)

    # ── MODULE 7: Regime-Aware Backtest ──
    if regimes is not None:
        all_trades, mr_trades = regime_aware_backtest(
            merged, regimes, state_labels,
            window=args.window, entry_z=2.0, stop_z=3.5, time_stop=30
        )

    # ── FINAL VERDICT ──
    final_verdict(kalman_z, hurst_h, adf_p, coint_p, hl, current_regime, operator_alerts)


if __name__ == "__main__":
    main()
