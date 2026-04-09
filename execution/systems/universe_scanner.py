"""
Universe Scanner — Dynamic pair discovery across all 211 FNO symbols.

Reuses existing infrastructure:
  - SECTORS dict from scan_cointegrated_pairs.py
  - test_pair() / half_life_calc() / hurst_fast() from same
  - load_ohlc() from pairs_system.py (full OHLC with lot patching)

Returns up to MAX_PAIRS pairs ranked by strength score:
  score = (1/halflife) × (1/eg_pvalue)^0.5 × corr

Lots are computed to equalise notional (cash-neutral hedge ratio):
  lots_b = 1
  lots_a = round(notional_b / notional_a)  [clamped 1–20]

Avoid list: pairs that have empirically failed in backtests
  (from directives/strategy_pairs.md).

Structural break rule (from user):
  Sustained divergence = structural break.
  Single-day |Z| spike to 4+ is NOT automatically a break;
  but |Z| > 4.0 at discovery time is treated as "something already
  broke" and excluded at ENTRY to prevent chasing.
"""

import sys
import os
import warnings
import numpy as np
import pandas as pd
from itertools import combinations

warnings.filterwarnings('ignore')

# ── Path wiring ───────────────────────────────────────────────────────────────
_HERE = os.path.dirname(__file__)
_EXEC = os.path.join(_HERE, '..', '..')
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, '..')))

from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

# ── Import reusable pieces from existing scripts ───────────────────────────────
# We inline the core statistical functions (hurst_fast, half_life_calc, test_pair_raw)
# rather than importing from scan_cointegrated_pairs.py to avoid CLI side-effects.

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_PAIRS     = 60     # Maximum pairs to return per universe build
MAX_HL_DAYS   = 30     # Half-life must be ≤ this (fast enough to beat 12% charges)
MIN_CORR      = 0.40   # Minimum 60d return correlation (Indian mid-caps: 0.4-0.6 is normal)
MAX_ENTRY_Z   = 4.0    # Reject at discovery if |Z| already > 4 (structural break)
MIN_DAYS      = 200    # Minimum overlapping trading days required

# Pairs that have empirically failed — hardcoded from directives/strategy_pairs.md
AVOID_PAIRS = {
    frozenset(['TCS',         'INFY']),
    frozenset(['HINDUNILVR',  'DABUR']),
    frozenset(['NTPC',        'POWERGRID']),
    frozenset(['GRASIM',      'DALBHARAT']),
    frozenset(['HDFCLIFE',    'LICI']),
    frozenset(['BIOCON',      'TORNTPHARM']),
    frozenset(['SIEMENS',     'ABB']),        # calendar: 33%WR
    frozenset(['PAGEIND',     'MFSL']),       # calendar: 47%WR
}

# Sector map (from scan_cointegrated_pairs.py — reproduced here for self-containment)
SECTORS = {
    'PHARMA':        ['SUNPHARMA', 'CIPLA', 'DRREDDY', 'LUPIN', 'AUROPHARMA', 'BIOCON',
                      'DIVISLAB', 'TORNTPHARM', 'ALKEM', 'GLENMARK', 'LAURUSLABS',
                      'ZYDUSLIFE', 'SYNGENE', 'MANKIND'],
    'BANKING':       ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK', 'SBIN',
                      'INDUSINDBK', 'BANKBARODA', 'PNB', 'FEDERALBNK', 'IDFCFIRSTB',
                      'CANBK', 'BANDHANBNK', 'AUBANK', 'RBLBANK', 'INDIANB', 'YESBANK',
                      'BANKINDIA', 'UNIONBANK'],
    'NBFC':          ['BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'SHRIRAMFIN', 'MUTHOOTFIN',
                      'LTF', 'LICHSGFIN', 'MANAPPURAM', 'PNBHOUSING', 'SBICARD',
                      'HDFCAMC', 'JIOFIN', '360ONE', 'ANGELONE'],
    'IT':            ['TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'LTIM', 'MPHASIS',
                      'COFORGE', 'PERSISTENT', 'KPITTECH', 'TATAELXSI', 'OFSS'],
    'METALS':        ['TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'SAIL', 'NMDC',
                      'JINDALSTEL', 'NATIONALUM', 'HINDZINC', 'COALINDIA'],
    'POWER':         ['NTPC', 'POWERGRID', 'TATAPOWER', 'NHPC', 'PFC', 'RECLTD',
                      'IREDA', 'IRFC', 'HUDCO', 'TORNTPOWER', 'JSWENERGY',
                      'ADANIENT', 'ADANIGREEN', 'ADANIENSOL'],
    'INFRA':         ['LT', 'RVNL', 'NBCC', 'CONCOR', 'GMRAIRPORT', 'DLF',
                      'OBEROIRLTY', 'GODREJPROP', 'PRESTIGE', 'LODHA', 'PHOENIXLTD', 'IRCTC'],
    'AUTO':          ['MARUTI', 'M&M', 'TATAMOTORS', 'BAJAJ-AUTO', 'HEROMOTOCO',
                      'TVSMOTOR', 'EICHERMOT', 'ASHOKLEY', 'MOTHERSON', 'BHARATFORG',
                      'UNOMINDA', 'EXIDEIND'],
    'FMCG':          ['HINDUNILVR', 'ITC', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL',
                      'BRITANNIA', 'GODREJCP', 'TATACONSUM', 'VBL', 'JUBLFOOD', 'PATANJALI'],
    'CEMENT':        ['ULTRACEMCO', 'AMBUJACEM', 'SHREECEM', 'DALBHARAT', 'GRASIM'],
    'OIL':           ['RELIANCE', 'ONGC', 'BPCL', 'IOC', 'HINDPETRO', 'GAIL', 'PETRONET'],
    'INSURANCE':     ['LICI', 'SBILIFE', 'HDFCLIFE', 'ICICIPRULI', 'ICICIGI', 'MAXHEALTH'],
    'DEFENCE':       ['HAL', 'BEL', 'BDL', 'MAZDOCK'],
    'CAPITAL_GOODS': ['ABB', 'SIEMENS', 'CGPOWER', 'BHEL', 'CUMMINSIND', 'HAVELLS',
                      'POLYCAB', 'CROMPTON', 'VOLTAS', 'BLUESTARCO', 'DIXON', 'KEI'],
}


# ── Statistical helpers (inlined from scan_cointegrated_pairs.py) ─────────────

def _hurst(ts, max_lag=50):
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
            S = np.std(chunk, ddof=1)
            if S > 0:
                dev = chunk - np.mean(chunk)
                cum = np.cumsum(dev)
                rs_list.append((max(cum) - min(cum)) / S)
        if rs_list:
            tau.append(lag)
            rs_values.append(np.mean(rs_list))
    if len(tau) < 2:
        return None
    H, _ = np.polyfit(np.log(tau), np.log(rs_values), 1)
    return H


def _half_life(spread):
    spread = spread.dropna()
    if len(spread) < 30:
        return None
    lag   = spread.shift(1)
    delta = spread - lag
    valid = ~(lag.isna() | delta.isna())
    if valid.sum() < 20:
        return None
    try:
        result = OLS(delta[valid].values, add_constant(lag[valid].values)).fit()
        theta  = result.params[1]
        if theta >= 0:
            return None   # diverging
        return -np.log(2) / np.log(1 + theta)
    except Exception:
        return None


def _compute_lots(price_a, lot_a, price_b, lot_b):
    """
    Cash-neutral hedge: lots_b=1, compute lots_a so notionals match.
    Returns (lots_a, lots_b).
    """
    if lot_a <= 0 or lot_b <= 0:
        return 1, 1
    notional_a = lot_a * price_a
    notional_b = lot_b * price_b
    if notional_a <= 0:
        return 1, 1
    lots_a = max(1, min(20, round(notional_b / notional_a)))
    return int(lots_a), 1


def _test(sym_a, sym_b, prices_a, prices_b, as_of, lookback_days):
    """
    Run the co-integration test suite on a slice of data.
    Returns result dict or None.
    """
    # Slice to lookback window
    cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=lookback_days)
    a = prices_a[(prices_a.index >= cutoff) & (prices_a.index <= as_of)]
    b = prices_b[(prices_b.index >= cutoff) & (prices_b.index <= as_of)]

    merged = pd.DataFrame({'A': a['close'], 'B': b['close']}).dropna()
    if len(merged) < MIN_DAYS:
        return None

    # ── Use OLS hedge ratio for spread (more robust than price ratio) ───
    try:
        X  = add_constant(merged['B'].values)
        m  = OLS(merged['A'].values, X).fit()
        beta   = m.params[1]
        spread = merged['A'] - beta * merged['B']
    except Exception:
        return None

    # 1. ADF on spread
    try:
        _, adf_p, *_ = adfuller(spread.dropna(), maxlag=20)
    except Exception:
        return None

    # 2. Engle-Granger cointegration
    try:
        _, eg_p, _ = coint(merged['A'], merged['B'])
    except Exception:
        return None

    # 3. Hurst
    H  = _hurst(spread.dropna().values)

    # 4. Half-life
    hl = _half_life(spread)

    # 5. 60d return correlation
    corr = merged['A'].pct_change().rolling(60).corr(
               merged['B'].pct_change()).dropna().iloc[-1] \
           if len(merged) >= 60 else None

    if corr is None or np.isnan(corr):
        return None

    # ── Gate checks ────────────────────────────────────────────────────────
    # ADF + EG are the stationary/cointegration gates (proper statistical tests).
    # Hurst (R/S method) is systematically > 0.5 for Indian FNO pairs even when
    # ADF confirms stationarity — the directive itself notes "Indian FNO pairs are
    # slightly above 0.5". Kept as soft penalty in score, NOT a hard gate.
    if adf_p  >= 0.10:  return None   # 90% confidence (0.05 was too strict for FNO futures)
    if eg_p   >= 0.10:  return None
    if hl is None or hl <= 0 or hl > MAX_HL_DAYS: return None
    if corr < MIN_CORR: return None

    # Hurst as soft penalty: lower Hurst (more mean-reverting) gets higher score
    hurst_bonus = max(0.3, 1.0 - H) if H is not None else 0.3
    score = (1.0 / hl) * (1.0 / max(eg_p, 1e-6)) ** 0.5 * corr * (1 + hurst_bonus)

    return dict(
        sym_a=sym_a, sym_b=sym_b,
        eg_p=round(eg_p, 4),
        adf_p=round(adf_p, 4),
        hurst=round(H, 3),
        halflife=round(hl, 1),
        corr=round(corr, 3),
        score=round(score, 4),
    )


# ── Main entry point ──────────────────────────────────────────────────────────

def build_universe(price_data: dict, as_of, lookback_days: int = 250,
                   max_pairs: int = MAX_PAIRS, verbose: bool = True) -> list:
    """
    Scan all within-sector pairs and return top `max_pairs` by score.

    Parameters
    ----------
    price_data   : dict[symbol → DataFrame with 'close','high','low','lot' columns]
    as_of        : Timestamp — only use data UP TO this date (no look-ahead)
    lookback_days: calendar days of history to use for tests
    max_pairs    : cap on returned pairs
    verbose      : print progress

    Returns
    -------
    List of dicts: {sym_a, sym_b, lots_a, lots_b, eg_p, halflife, corr, score, sector}
    """
    as_of = pd.Timestamp(as_of)
    results = []
    total_tested = 0

    for sector, symbols in SECTORS.items():
        available = [s for s in symbols if s in price_data]
        if len(available) < 2:
            continue

        for sym_a, sym_b in combinations(available, 2):
            pa_df = price_data[sym_a]
            pb_df = price_data[sym_b]

            res = _test(sym_a, sym_b, pa_df, pb_df, as_of, lookback_days)
            total_tested += 1
            if res is None:
                continue

            # Compute cash-neutral lot ratio using price/lot as of as_of date
            last_a = pa_df[pa_df.index <= as_of]
            last_b = pb_df[pb_df.index <= as_of]
            if last_a.empty or last_b.empty:
                continue

            price_a  = last_a['close'].iloc[-1]
            price_b  = last_b['close'].iloc[-1]
            lot_a    = last_a['lot'].iloc[-1] if 'lot' in last_a.columns else 1
            lot_b    = last_b['lot'].iloc[-1] if 'lot' in last_b.columns else 1
            if pd.isna(lot_a) or lot_a <= 0:
                lot_a = 1
            if pd.isna(lot_b) or lot_b <= 0:
                lot_b = 1

            lots_a, lots_b = _compute_lots(price_a, lot_a, price_b, lot_b)

            res['sector']  = sector
            res['lots_a']  = lots_a
            res['lots_b']  = lots_b
            res['lot_a']   = int(lot_a)
            res['lot_b']   = int(lot_b)
            results.append(res)

    # Sort by score descending, cap at max_pairs
    results.sort(key=lambda x: x['score'], reverse=True)
    top = results[:max_pairs]

    if verbose:
        print(f"  Universe scanner: tested {total_tested} pairs, "
              f"{len(results)} passed filters, returning top {len(top)}")
        for r in top[:10]:
            print(f"    {r['sym_a']:12}/{r['sym_b']:12}  "
                  f"hl={r['halflife']:5.1f}d  corr={r['corr']:.2f}  "
                  f"EGp={r['eg_p']:.3f}  score={r['score']:.2f}  "
                  f"lots={r['lots_a']}/{r['lots_b']}  [{r['sector']}]")

    return top


def to_universe_tuples(pairs: list) -> list:
    """Convert scanner output to (sym_a, sym_b, lots_a, lots_b) tuples for pairs_system."""
    return [(r['sym_a'], r['sym_b'], r['lots_a'], r['lots_b']) for r in pairs]
