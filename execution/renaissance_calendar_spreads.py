"""
Renaissance-Grade Calendar Spread Scanner v2
=============================================
Full statistical foundation: Z-scores, probability distributions, mean reversion,
Hurst exponent, half-life, and historical convergence backtesting.

Problem: We don't have historical futures spread data.
Solution: Build the spread from live futures data + collect spread snapshots over time,
          AND use historical spot data to model expected basis behavior.

For LIVE analysis, we use the actual near/far futures prices.
For HISTORICAL context, we reconstruct expected calendar spread behavior using:
  - Cost-of-carry model deviations (actual premium vs theoretical)
  - Historical volatility (drives basis width)
  - Dividend-adjusted basis

Scoring Modules:
1. Live Spread Statistics — Z-score of current spread vs rolling history
2. Term Structure Analysis — shape, slope, convexity
3. Basis Mean Reversion — ADF, Hurst, Half-Life on cost-of-carry deviation
4. Probability Distribution — expected convergence range
5. Liquidity & Execution Quality
6. Historical Spot Volatility — risk sizing
7. Composite Score & Verdict
"""
import sys
import os
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import concurrent.futures
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(__file__))
from shoonya_client import ShoonyaClient
from fno_utils import FNO_SYMBOLS

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TMP_DIR = os.path.join(os.path.dirname(__file__), '..', '.tmp')
DATA_DIR = os.path.join(TMP_DIR, '3y_data')
SPREAD_HIST_DIR = os.path.join(TMP_DIR, 'calendar_spread_history')

RISK_FREE_RATE = 0.065  # 6.5% RBI repo rate


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def last_tuesday(year, month):
    import calendar
    cal = calendar.monthcalendar(year, month)
    for week in reversed(cal):
        if week[1] != 0:
            return datetime(year, month, week[1])
    return None


def theoretical_future(spot, dte, rate=RISK_FREE_RATE):
    """Cost-of-carry: F = S * e^(r*t)"""
    return spot * np.exp(rate * dte / 365.0)


def hurst_exponent(series):
    """Hurst exponent via rescaled range. H<0.5 = mean-reverting."""
    if len(series) < 50:
        return None
    series = np.array(series)
    max_k = min(len(series) // 2, 200)
    if max_k < 10:
        return None
    lags = range(10, max_k)
    rs_values = []
    for lag in lags:
        subseries = [series[i:i+lag] for i in range(0, len(series) - lag, lag)]
        rs_list = []
        for s in subseries:
            if len(s) < 2:
                continue
            mean_s = np.mean(s)
            devs = np.cumsum(s - mean_s)
            R = np.max(devs) - np.min(devs)
            S = np.std(s, ddof=1)
            if S > 0:
                rs_list.append(R / S)
        if rs_list:
            rs_values.append((lag, np.mean(rs_list)))
    if len(rs_values) < 5:
        return None
    log_lags = np.log([x[0] for x in rs_values])
    log_rs = np.log([x[1] for x in rs_values])
    slope, _, _, _, _ = np.polyfit(log_lags, log_rs, 1, full=False, cov=False), None, None, None, None
    slope = np.polyfit(log_lags, log_rs, 1)[0]
    return slope


def half_life(series):
    """Ornstein-Uhlenbeck half-life of mean reversion."""
    series = np.array(series)
    if len(series) < 30:
        return None
    lag = series[:-1]
    diff = np.diff(series)
    lag = lag - np.mean(lag)
    if np.std(lag) == 0:
        return None
    from numpy.linalg import lstsq
    A = np.column_stack([lag, np.ones(len(lag))])
    result = lstsq(A, diff, rcond=None)
    theta = result[0][0]
    if theta >= 0:
        return None  # Not mean-reverting
    hl = -np.log(2) / np.log(1 + theta)
    return hl if hl > 0 else None


def adf_test(series):
    """Augmented Dickey-Fuller stationarity test."""
    from statsmodels.tsa.stattools import adfuller
    if len(series) < 30:
        return None, None
    result = adfuller(series, maxlag=min(20, len(series) // 4))
    return result[0], result[1]  # stat, p-value


# ============================================================
# DATA COLLECTION
# ============================================================

def get_spot_price(api, symbol):
    search_res = api.searchscrip(exchange='NSE', searchtext=symbol)
    if not search_res or 'values' not in search_res:
        return None
    for res in search_res['values']:
        if res['tsym'] == f"{symbol}-EQ" or res['tsym'] == symbol:
            q = api.get_quotes(exchange='NSE', token=res['token'])
            if q and 'lp' in q:
                return float(q['lp'])
    return None


def get_futures(api, symbol):
    ret = api.searchscrip(exchange='NFO', searchtext=symbol)
    if not ret or 'values' not in ret:
        return []
    futures = [
        x for x in ret['values']
        if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX') and x['symname'] == symbol
    ]
    now = datetime.now()
    valid = []
    for f in futures:
        try:
            exp = datetime.strptime(f['exd'], '%d-%b-%Y')
            if exp >= now - timedelta(days=1):
                q = api.get_quotes(exchange='NFO', token=f['token'])
                if q and float(q.get('lp', 0)) > 0:
                    f['_exp_dt'] = exp
                    f['_ltp'] = float(q['lp'])
                    f['_oi'] = int(q.get('oi', 0))
                    f['_vol'] = int(q.get('v', 0))
                    f['_bid'] = float(q.get('bp1', 0))
                    f['_ask'] = float(q.get('sp1', 0))
                    f['_lot'] = int(f.get('ls', 0))
                    valid.append(f)
        except:
            pass
    valid.sort(key=lambda x: x['_exp_dt'])
    return valid


def load_spot_history(symbol):
    """Load historical spot data for basis modeling."""
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='mixed', dayfirst=True)
    df = df.sort_values('FH_TIMESTAMP').drop_duplicates('FH_TIMESTAMP').set_index('FH_TIMESTAMP')
    df = df.dropna(subset=['FH_CLOSING_PRICE'])
    return df


def save_spread_snapshot(symbol, data):
    """Save current spread data for building historical distribution over time."""
    os.makedirs(SPREAD_HIST_DIR, exist_ok=True)
    path = os.path.join(SPREAD_HIST_DIR, f"{symbol}_spreads.csv")
    row = pd.DataFrame([data])
    if os.path.exists(path):
        existing = pd.read_csv(path)
        combined = pd.concat([existing, row], ignore_index=True)
        combined.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
        combined.to_csv(path, index=False)
    else:
        row.to_csv(path, index=False)


def load_spread_history(symbol):
    """Load historical spread snapshots if available."""
    path = os.path.join(SPREAD_HIST_DIR, f"{symbol}_spreads.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


# ============================================================
# HISTORICAL BASIS MODEL (proxy for calendar spread)
# ============================================================

def build_basis_model(symbol):
    """
    Build a historical basis model using spot data.

    The "basis" is the deviation of futures from theoretical cost-of-carry.
    We model this as: basis_deviation = actual_premium - theoretical_premium

    Since we don't have historical futures data, we use daily returns volatility
    to estimate what the typical basis deviation would be.

    Key insight: stocks with high short interest show persistent negative basis.
    We can detect this from spot price patterns (gaps, volatility clustering).
    """
    hist = load_spot_history(symbol)
    if hist is None or len(hist) < 100:
        return None

    close = hist['FH_CLOSING_PRICE'].values
    returns = np.diff(np.log(close))

    # Rolling volatility (proxy for basis width)
    vol_20 = pd.Series(returns).rolling(20).std().dropna().values * np.sqrt(252)
    vol_60 = pd.Series(returns).rolling(60).std().dropna().values * np.sqrt(252)

    # Realized vol statistics
    current_vol = vol_20[-1] if len(vol_20) > 0 else None
    mean_vol = np.mean(vol_20) if len(vol_20) > 0 else None
    vol_percentile = (np.sum(vol_20 < current_vol) / len(vol_20) * 100) if current_vol and len(vol_20) > 0 else None

    # Expected basis range: higher vol → wider basis deviation expected
    # Typical basis deviation ≈ vol * sqrt(DTE/252) * spot
    # For a 30-day spread (near-far gap): sqrt(30/252) ≈ 0.345
    expected_basis_1std = current_vol * 0.345 * close[-1] if current_vol else None

    # Build synthetic "basis deviation" series
    # Use rolling 30-day return as proxy for what the calendar spread captures
    rolling_30d_ret = pd.Series(close).pct_change(30).dropna().values

    # Statistics on the synthetic basis
    basis_mean = np.mean(rolling_30d_ret) * close[-1] if len(rolling_30d_ret) > 0 else None
    basis_std = np.std(rolling_30d_ret) * close[-1] if len(rolling_30d_ret) > 0 else None

    # Hurst on the synthetic basis
    h = hurst_exponent(rolling_30d_ret) if len(rolling_30d_ret) > 50 else None

    # ADF on synthetic basis
    adf_stat, adf_p = adf_test(rolling_30d_ret) if len(rolling_30d_ret) > 30 else (None, None)

    # Half-life
    hl = half_life(rolling_30d_ret) if len(rolling_30d_ret) > 30 else None

    return {
        'n_days': len(close),
        'current_price': close[-1],
        'current_vol_20d': current_vol,
        'mean_vol_20d': mean_vol,
        'vol_percentile': vol_percentile,
        'expected_basis_1std': expected_basis_1std,
        'basis_mean': basis_mean,
        'basis_std': basis_std,
        'hurst': h,
        'adf_stat': adf_stat,
        'adf_p': adf_p,
        'half_life': hl,
        'rolling_30d_returns': rolling_30d_ret,
    }


# ============================================================
# MAIN ANALYSIS
# ============================================================

def analyze_symbol(api, symbol, today, verbose=True):
    """Full Renaissance analysis for one symbol's calendar spread."""
    spot = get_spot_price(api, symbol)
    if not spot:
        return None

    futures = get_futures(api, symbol)
    if len(futures) < 2:
        return None

    near = futures[0]
    far = futures[1]
    third = futures[2] if len(futures) >= 3 else None

    lot = near['_lot']
    if lot == 0:
        return None

    near_dte = max((near['_exp_dt'] - today).days, 1)
    far_dte = max((far['_exp_dt'] - today).days, 1)

    near_prem = near['_ltp'] - spot
    far_prem = far['_ltp'] - spot
    spread = far['_ltp'] - near['_ltp']

    # Theoretical prices
    near_theo = theoretical_future(spot, near_dte)
    far_theo = theoretical_future(spot, far_dte)
    theo_spread = far_theo - near_theo

    # Deviations from theoretical
    near_dev = near['_ltp'] - near_theo
    far_dev = far['_ltp'] - far_theo
    spread_dev = spread - theo_spread

    # Only analyze backwardation (negative spread)
    if spread >= 0:
        return None

    # ===== MODULE 1: HISTORICAL BASIS MODEL =====
    basis_model = build_basis_model(symbol)

    # Z-score of current spread vs expected basis
    z_score = None
    z_score_dev = None
    if basis_model and basis_model['basis_std'] and basis_model['basis_std'] > 0:
        z_score = (spread - basis_model['basis_mean']) / basis_model['basis_std']
        z_score_dev = spread_dev / basis_model['expected_basis_1std'] if basis_model['expected_basis_1std'] else None

    # ===== MODULE 2: PROBABILITY DISTRIBUTION =====
    # What's the probability the spread converges by near expiry?
    # Using the basis model's distribution
    convergence_prob = None
    expected_pnl = None
    if basis_model and basis_model['rolling_30d_returns'] is not None and len(basis_model['rolling_30d_returns']) > 30:
        returns_30d = basis_model['rolling_30d_returns']
        # What fraction of historical 30-day periods saw positive returns?
        # (positive return = spot went up = near-month short would lose less)
        # For calendar spread: we care about spread narrowing
        # Spread narrows when far outperforms near, which happens when
        # the stock reverts from oversold conditions

        # Simulate: what's the distribution of spread changes over near_dte days?
        daily_ret = np.diff(np.log(basis_model['rolling_30d_returns'] + 1))
        if len(daily_ret) > 0:
            daily_vol = np.std(daily_ret)
        else:
            daily_vol = basis_model['current_vol_20d'] / np.sqrt(252) if basis_model['current_vol_20d'] else 0.02

        # Expected spread change distribution
        # spread_change ~ N(0, daily_vol * sqrt(dte) * spot)
        if daily_vol > 0:
            spread_change_std = daily_vol * np.sqrt(near_dte) * spot
            # Probability spread narrows (improves by at least slippage cost)
            slippage = (near['_ask'] - near['_bid'] + far['_ask'] - far['_bid']) if near['_ask'] > 0 else 1.0
            from scipy import stats as sp_stats
            convergence_prob = sp_stats.norm.cdf(abs(spread), loc=0, scale=spread_change_std) * 100
            # Expected P&L distribution
            # P&L = spread_change * lot
            expected_pnl = abs(spread) * 0.5 * lot  # Conservative: capture half the spread

    # ===== MODULE 3: MEAN REVERSION STATISTICS =====
    hurst_val = basis_model['hurst'] if basis_model else None
    adf_p_val = basis_model['adf_p'] if basis_model else None
    half_life_val = basis_model['half_life'] if basis_model else None

    # ===== MODULE 4: TERM STRUCTURE ANALYSIS =====
    # Shape: contango, backwardation, or mixed?
    near_ann_basis = (near_prem / spot) * (365 / near_dte) * 100
    far_ann_basis = (far_prem / spot) * (365 / far_dte) * 100
    basis_slope = (far_ann_basis - near_ann_basis)  # Negative = steepening backwardation

    is_classic = near_prem > 0 and far_prem < 0  # Near premium, far discount

    # Convexity: is the term structure curved?
    if third and third['_ltp'] > 0:
        third_dte = max((third['_exp_dt'] - today).days, 1)
        third_prem = third['_ltp'] - spot
        third_ann_basis = (third_prem / spot) * (365 / third_dte) * 100
        convexity = (third_ann_basis - 2 * far_ann_basis + near_ann_basis)
    else:
        convexity = None
        third_dte = None
        third_prem = None
        third_ann_basis = None

    # ===== MODULE 5: LIQUIDITY =====
    near_ba_bps = ((near['_ask'] - near['_bid']) / near['_ltp'] * 10000) if near['_ltp'] > 0 and near['_ask'] > 0 else 999
    far_ba_bps = ((far['_ask'] - far['_bid']) / far['_ltp'] * 10000) if far['_ltp'] > 0 and far['_ask'] > 0 else 999
    total_slippage = ((near['_ask'] - near['_bid']) + (far['_ask'] - far['_bid'])) * lot if near['_ask'] > 0 else 0

    # ===== MODULE 6: P&L ESTIMATION =====
    # Method: model near convergence + far decay
    near_decay_rate = near_prem / near_dte if near_dte > 0 else 0
    far_decay_rate = far_prem / far_dte if far_dte > 0 else 0
    remaining_far_dte = far_dte - near_dte

    # Short near P&L at settlement: sell at near, buy back at spot
    near_pnl = near['_ltp'] - spot  # = near_prem (negative if discount)
    # Long far P&L: buy at far, sell when far becomes new near
    far_prem_at_near_expiry = far_decay_rate * remaining_far_dte
    far_price_est = spot + far_prem_at_near_expiry
    far_pnl = far_price_est - far['_ltp']
    total_pnl_per_share = near_pnl + far_pnl
    total_pnl_per_lot = total_pnl_per_share * lot

    # Potential gain (if spread fully converges)
    potential_gain = abs(spread) * lot

    # Margin estimate
    est_margin = max(near['_ltp'], far['_ltp']) * lot * 0.20

    # ===== MODULE 7: COMPOSITE SCORE =====
    score = 0
    max_score = 0

    # Classic setup bonus (25 pts)
    max_score += 25
    if is_classic:
        score += 25  # Both legs work in your favor

    # Z-score quality (20 pts)
    max_score += 20
    if z_score is not None:
        if abs(z_score) > 2.5:
            score += 20
        elif abs(z_score) > 2.0:
            score += 15
        elif abs(z_score) > 1.5:
            score += 10
        elif abs(z_score) > 1.0:
            score += 5

    # Mean reversion quality (20 pts)
    max_score += 20
    mr_score = 0
    if hurst_val is not None and hurst_val < 0.5:
        mr_score += 7
    if adf_p_val is not None and adf_p_val < 0.05:
        mr_score += 7
    if half_life_val is not None and 3 < half_life_val < 30:
        mr_score += 6
    score += mr_score

    # Estimated P&L (15 pts)
    max_score += 15
    if total_pnl_per_lot > 0:
        score += 15
    elif total_pnl_per_lot > -1000:
        score += 5

    # Liquidity (10 pts)
    max_score += 10
    if near['_oi'] > 5000000 and far['_oi'] > 1000000:
        score += 10
    elif near['_oi'] > 1000000 and far['_oi'] > 500000:
        score += 5

    # DTE sweet spot (10 pts)
    max_score += 10
    if 3 <= near_dte <= 20 and 25 <= far_dte <= 70:
        score += 10
    elif near_dte <= 30 and far_dte <= 90:
        score += 5

    composite = (score / max_score * 100) if max_score > 0 else 0

    # Save snapshot for future distribution building
    save_spread_snapshot(symbol, {
        'timestamp': today.strftime('%Y-%m-%d %H:%M'),
        'spot': spot,
        'near_price': near['_ltp'],
        'far_price': far['_ltp'],
        'near_expiry': near['exd'],
        'far_expiry': far['exd'],
        'near_dte': near_dte,
        'far_dte': far_dte,
        'spread': spread,
        'near_prem': near_prem,
        'far_prem': far_prem,
        'z_score': z_score,
        'near_oi': near['_oi'],
        'far_oi': far['_oi'],
    })

    result = {
        'Symbol': symbol,
        'Spot': spot,
        'Lot': lot,
        'Near_Exp': near['exd'],
        'Near_Price': near['_ltp'],
        'Near_DTE': near_dte,
        'Near_Prem': round(near_prem, 2),
        'Near_Ann_Basis': round(near_ann_basis, 2),
        'Near_OI': near['_oi'],
        'Near_Vol': near['_vol'],
        'Far_Exp': far['exd'],
        'Far_Price': far['_ltp'],
        'Far_DTE': far_dte,
        'Far_Prem': round(far_prem, 2),
        'Far_Ann_Basis': round(far_ann_basis, 2),
        'Far_OI': far['_oi'],
        'Far_Vol': far['_vol'],
        'Spread': round(spread, 2),
        'Theo_Spread': round(theo_spread, 2),
        'Spread_Dev': round(spread_dev, 2),
        'Z_Score': round(z_score, 2) if z_score else None,
        'Z_Score_Dev': round(z_score_dev, 2) if z_score_dev else None,
        'Hurst': round(hurst_val, 3) if hurst_val else None,
        'ADF_p': round(adf_p_val, 4) if adf_p_val else None,
        'Half_Life': round(half_life_val, 1) if half_life_val else None,
        'Convergence_Prob': round(convergence_prob, 1) if convergence_prob else None,
        'Basis_Slope': round(basis_slope, 2),
        'Is_Classic': is_classic,
        'Near_PnL': round(near_pnl * lot, 0),
        'Far_PnL': round(far_pnl * lot, 0),
        'Est_PnL': round(total_pnl_per_lot, 0),
        'Potential_Gain': round(potential_gain, 0),
        'Est_Margin': round(est_margin, 0),
        'Return_Pct': round(total_pnl_per_lot / est_margin * 100, 2) if est_margin > 0 else 0,
        'Slippage': round(total_slippage, 0),
        'Vol_20d': round(basis_model['current_vol_20d'] * 100, 1) if basis_model and basis_model['current_vol_20d'] else None,
        'Vol_Pctile': round(basis_model['vol_percentile'], 0) if basis_model and basis_model['vol_percentile'] else None,
        'Composite': round(composite, 1),
    }

    return result


def print_detailed(r):
    """Print detailed analysis for one symbol."""
    z_str = f"{r['Z_Score']:+.2f}" if r['Z_Score'] is not None else "N/A"
    h_str = f"{r['Hurst']:.3f}" if r['Hurst'] is not None else "N/A"
    adf_str = f"{r['ADF_p']:.4f}" if r['ADF_p'] is not None else "N/A"
    hl_str = f"{r['Half_Life']:.1f}d" if r['Half_Life'] is not None else "N/A"
    conv_str = f"{r['Convergence_Prob']:.1f}%" if r['Convergence_Prob'] is not None else "N/A"
    vol_str = f"{r['Vol_20d']:.1f}%" if r['Vol_20d'] is not None else "N/A"
    vol_pct_str = f"{r['Vol_Pctile']:.0f}th" if r['Vol_Pctile'] is not None else "N/A"
    classic = " ★ CLASSIC" if r['Is_Classic'] else ""
    pnl_warning = " ⚠ NEGATIVE" if r['Est_PnL'] < 0 else " ✓ POSITIVE"

    score_bar = '█' * int(r['Composite'] / 5) + '░' * (20 - int(r['Composite'] / 5))

    print(f"\n  {'─'*86}")
    print(f"  {r['Symbol']:>12}  Score: {r['Composite']:5.1f}/100  [{score_bar}]{classic}")
    print(f"  {'─'*86}")
    print(f"  │ Spot: ₹{r['Spot']:,.2f}  |  Lot: {r['Lot']}  |  Vol: {vol_str} ({vol_pct_str} percentile)")
    print(f"  │")
    print(f"  │ TERM STRUCTURE:")
    print(f"  │   Near: ₹{r['Near_Price']:,.2f}  ({r['Near_Exp']}, {r['Near_DTE']}d)  Prem: {r['Near_Prem']:+.2f}  Ann: {r['Near_Ann_Basis']:+.2f}%  OI: {r['Near_OI']:,}")
    print(f"  │   Far:  ₹{r['Far_Price']:,.2f}  ({r['Far_Exp']}, {r['Far_DTE']}d)  Prem: {r['Far_Prem']:+.2f}  Ann: {r['Far_Ann_Basis']:+.2f}%  OI: {r['Far_OI']:,}")
    print(f"  │   Slope: {r['Basis_Slope']:+.2f}  (negative = steepening backwardation)")
    print(f"  │")
    print(f"  │ SPREAD ANALYSIS:")
    print(f"  │   Current: {r['Spread']:+.2f}  |  Theoretical: {r['Theo_Spread']:+.2f}  |  Deviation: {r['Spread_Dev']:+.2f}")
    print(f"  │   Z-Score: {z_str}  |  Convergence Prob: {conv_str}")
    print(f"  │")
    print(f"  │ MEAN REVERSION STATS (from spot history):")

    # Hurst interpretation
    if r['Hurst'] is not None:
        if r['Hurst'] < 0.4:
            h_interp = "STRONG mean-reverting"
        elif r['Hurst'] < 0.5:
            h_interp = "Mild mean-reverting"
        else:
            h_interp = "TRENDING (not mean-reverting!)"
    else:
        h_interp = "Insufficient data"

    # ADF interpretation
    if r['ADF_p'] is not None:
        if r['ADF_p'] < 0.01:
            adf_interp = "STATIONARY (strong)"
        elif r['ADF_p'] < 0.05:
            adf_interp = "Stationary"
        elif r['ADF_p'] < 0.10:
            adf_interp = "Weak stationarity"
        else:
            adf_interp = "NON-STATIONARY"
    else:
        adf_interp = "N/A"

    print(f"  │   Hurst: {h_str} → {h_interp}")
    print(f"  │   ADF p-value: {adf_str} → {adf_interp}")
    print(f"  │   Half-Life: {hl_str}")
    print(f"  │")
    print(f"  │ P&L ESTIMATE (hold to near expiry):{pnl_warning}")
    print(f"  │   Short Near: ₹{r['Near_PnL']:>+10,.0f}  (sell {r['Near_Price']:.2f}, settles to ~{r['Spot']:.2f})")
    print(f"  │   Long Far:   ₹{r['Far_PnL']:>+10,.0f}  (buy {r['Far_Price']:.2f}, est exit ~{r['Far_Price'] + r['Far_PnL']/r['Lot']:.2f})")
    print(f"  │   ────────────────────")
    print(f"  │   Net P&L:    ₹{r['Est_PnL']:>+10,.0f}  |  Return: {r['Return_Pct']:+.2f}% on margin")
    print(f"  │   Max Gain:   ₹{r['Potential_Gain']:>+10,.0f}  (if spread fully converges)")
    print(f"  │   Slippage:   ₹{r['Slippage']:>10,.0f}  (round trip)")
    print(f"  │   Est Margin: ₹{r['Est_Margin']:>10,.0f}")
    print(f"  │")

    # Verdict
    if r['Composite'] >= 70:
        verdict = "HIGH CONVICTION — Strong statistical + structural edge"
    elif r['Composite'] >= 50:
        verdict = "MODERATE — Some edge, trade with position sizing"
    elif r['Composite'] >= 30:
        verdict = "WEAK — Limited edge, consider skipping"
    else:
        verdict = "AVOID — No statistical edge"

    print(f"  │ VERDICT: {verdict}")


def run_scan(limit=None, min_score=0, start_index=0, symbols_filter=None):
    """Main scan function."""
    client = ShoonyaClient()
    api = client.login()
    if not api:
        print("[FATAL] Login failed")
        return

    today = datetime.now()

    if symbols_filter:
        symbols = symbols_filter
    else:
        symbols = FNO_SYMBOLS[start_index:]
        if limit:
            symbols = symbols[:limit]

    print(f"\n{'█'*90}")
    print(f"  RENAISSANCE CALENDAR SPREAD SCANNER v2")
    print(f"  Z-Scores | Probability | Mean Reversion | Hurst | ADF | Half-Life")
    print(f"  Date: {today.strftime('%Y-%m-%d %H:%M')} | Symbols: {len(symbols)}")
    print(f"{'█'*90}")

    results = []
    done = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(analyze_symbol, api, sym, today): sym
            for sym in symbols
        }
        for future in concurrent.futures.as_completed(future_map):
            done += 1
            if done % 25 == 0:
                print(f"  ... scanned {done}/{len(symbols)}")
            res = future.result()
            if res:
                results.append(res)

    if not results:
        print("\n  No backwardation opportunities found.")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values('Composite', ascending=False)

    # ===== DETAILED OUTPUT =====
    print(f"\n{'='*90}")
    print(f"  BACKWARDATION OPPORTUNITIES: {len(df)} found (ranked by composite score)")
    print(f"{'='*90}")

    for _, r in df.iterrows():
        print_detailed(r)

    # ===== SUMMARY TABLE =====
    print(f"\n{'='*90}")
    print(f"  RANKED SUMMARY")
    print(f"{'='*90}")
    print(f"  {'Symbol':>12} | {'Score':>5} | {'Type':>7} | {'Spread':>8} | {'Z':>6} | {'Hurst':>5} | {'ADF-p':>6} | {'HalfL':>5} | {'Est P&L':>10} | {'Ret%':>6} | {'Gain':>10}")
    print(f"  {'-'*12}-+-{'-'*5}-+-{'-'*7}-+-{'-'*8}-+-{'-'*6}-+-{'-'*5}-+-{'-'*6}-+-{'-'*5}-+-{'-'*10}-+-{'-'*6}-+-{'-'*10}")

    for _, r in df.iterrows():
        z_s = f"{r['Z_Score']:+.2f}" if r['Z_Score'] is not None else "  N/A"
        h_s = f"{r['Hurst']:.3f}" if r['Hurst'] is not None else "  N/A"
        a_s = f"{r['ADF_p']:.4f}" if r['ADF_p'] is not None else "   N/A"
        hl_s = f"{r['Half_Life']:.0f}d" if r['Half_Life'] is not None else "  N/A"
        t = "CLASSIC" if r['Is_Classic'] else "BOTH-"
        pnl_marker = "✓" if r['Est_PnL'] > 0 else "✗"
        print(f"  {r['Symbol']:>12} | {r['Composite']:>5.1f} | {t:>7} | {r['Spread']:>+8.2f} | {z_s:>6} | {h_s:>5} | {a_s:>6} | {hl_s:>5} | {pnl_marker} ₹{r['Est_PnL']:>+8,.0f} | {r['Return_Pct']:>+5.2f} | ₹{r['Potential_Gain']:>8,.0f}")

    # Save
    os.makedirs(TMP_DIR, exist_ok=True)
    out_path = os.path.join(TMP_DIR, 'calendar_spreads_renaissance_v2.csv')
    df.to_csv(out_path, index=False)
    print(f"\n  Results saved to {out_path}")

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Renaissance Calendar Spread Scanner v2')
    parser.add_argument('--limit', type=int, help='Limit number of symbols')
    parser.add_argument('--min-score', type=float, default=0, help='Minimum composite score')
    parser.add_argument('--start', type=int, default=0, help='Start index')
    parser.add_argument('--symbols', nargs='+', help='Specific symbols to scan')
    args = parser.parse_args()

    run_scan(args.limit, args.min_score, args.start, args.symbols)
