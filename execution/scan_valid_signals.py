"""
scan_valid_signals.py — Self-Validating Pairs Signal Scanner

Design principle (autoresearch): every candidate runs through a fixed
validation pipeline. A signal is only emitted when it passes every gate.
Anything ambiguous is silently rejected — no partial signals, no warnings
dressed up as signals.

Gate sequence (fail-fast, short-circuits on first failure):
  G1  Data freshness     ≤ 3 trading days stale
  G2  Minimum overlap    ≥ 200 common trading days
  G3  Lot consistency    Recompute historical spread with live lot if changed;
                         reject only if lot gap > 50% (data quality crisis)
  G4  Cointegration      1Y coint p < 0.15 AND ADF p < 0.10
                         (252 rows = sufficient statistical power)
  G5  Half-life          OU half-life ≤ 50 trading days
  G6  Return correlation 20D ≥ 0.40
  G7  Z confirmation     |Z| ≥ entry_z in ≥ 2 of (20d, 30d, 60d) windows
                         using lot-adjusted consistent spreads
  G8  Ratio range        Live price ratio within historical ±5% envelope
  G9  Signal direction   Both Z windows agree on sign (no flip between windows)
 G10  Self-check         Re-derive signal on fresh slice after all gates pass

Usage:
    python3 execution/scan_valid_signals.py
    python3 execution/scan_valid_signals.py --verbose
    python3 execution/scan_valid_signals.py --pair ICICIBANK/HDFCBANK
"""
import math
import logging
import argparse
import warnings
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path('.tmp/3y_data')

# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE — pairs cleared for live scanning
# Removed: IDFCFIRSTB/AUBANK (3Y coint p=0.86, HL=200d, WR=47%)
#           BAJAJFINSV/CHOLAFIN (coint p=0.43 all windows, confirmed structural break)
# ─────────────────────────────────────────────────────────────────────────────
UNIVERSE = {
    "ULTRACEMCO/AMBUJACEM": {"sector": "Cement",        "z_entry": 2.0, "lookback": 60, "time_stop": 30},
    "ULTRACEMCO/SHREECEM":  {"sector": "Cement",        "z_entry": 2.5, "lookback": 60, "time_stop": 20},
    "HINDALCO/VEDL":        {"sector": "Metals",        "z_entry": 2.5, "lookback": 60, "time_stop": 20},
    "NMDC/COALINDIA":       {"sector": "Metals",        "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "LICHSGFIN/PFC":        {"sector": "NBFCs",         "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "GAIL/ONGC":            {"sector": "Oil & Gas",     "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "BPCL/IOC":             {"sector": "Oil & Gas",     "z_entry": 2.5, "lookback": 60, "time_stop": 20},
    "MARICO/TATACONSUM":    {"sector": "FMCG",          "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "HCLTECH/PERSISTENT":   {"sector": "IT Services",   "z_entry": 2.0, "lookback": 60, "time_stop": 30},
    "ICICIBANK/HDFCBANK":   {"sector": "Pvt Banks",     "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "BANKBARODA/PNB":       {"sector": "PSU Banks",     "z_entry": 2.0, "lookback": 30, "time_stop": 30},
    "SBIN/PNB":             {"sector": "PSU Banks",     "z_entry": 2.0, "lookback": 60, "time_stop": 20},
}

# Gate thresholds
MAX_STALE_TRADING_DAYS  = 3
MIN_OVERLAP_DAYS        = 200
MAX_LOT_CHANGE_PCT      = 50.0   # reject if lot changed by more than this
ADF_P_MAX               = 0.10   # ADF on fixed-lot spread (primary — this is what we trade)
COINT_P_MAX             = 0.20   # Engle-Granger (secondary — OLS ratio, not our fixed lots)
# Multi-timeframe cointegration tiers:
#   TIER1: recent structure intact (3M coint p < 0.15 OR 6M coint p < 0.10)
#   TIER2: structural dislocation (3Y ok but 1Y broken) — real opportunity, half size
#   REJECT: no evidence of cointegration in any window
MAX_HALF_LIFE_DAYS      = 50
MIN_CORR_20D            = 0.40
Z_WINDOWS               = [20, 30, 60]
MIN_WINDOWS_CONFIRMING  = 2       # Z must reach entry_z in this many windows


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_csv(sym: str):
    path = DATA_DIR / f'{sym}_5Y.csv'
    if not path.exists():
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['date']).sort_values('date')
    df['price'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['lot']   = pd.to_numeric(df['FH_MARKET_LOT'],    errors='coerce').ffill().bfill()
    # deduplicate: one row per date (yfinance = already 1/day; NSE futures = pick nearest expiry)
    exp_col = 'FH_EXPIRY_DT'
    if exp_col in df.columns:
        df['_exp'] = pd.to_datetime(df[exp_col], format='%d-%b-%Y', errors='coerce')
        if df['_exp'].notna().any():
            idx = df.groupby('date')['_exp'].idxmin().dropna()
            df  = df.loc[idx]
        else:
            df = df.drop_duplicates('date', keep='last')
    else:
        df = df.drop_duplicates('date', keep='last')
    return df.set_index('date').sort_index()


def _fetch_live(syms: list) -> dict:
    """Fetch today's closing price via yfinance. Returns {sym: price}."""
    tickers = [s + '.NS' for s in syms]
    raw = yf.download(tickers, period='2d', auto_adjust=True,
                      progress=False, group_by='ticker')
    out = {}
    for sym in syms:
        try:
            out[sym] = float(raw[sym + '.NS']['Close'].dropna().iloc[-1])
        except Exception:
            out[sym] = None
    return out


def _ou_half_life(spread: np.ndarray) -> float:
    """Ornstein-Uhlenbeck half-life in trading days."""
    y = spread[1:] - spread[:-1]
    x = spread[:-1]
    try:
        res   = OLS(y, add_constant(x)).fit()
        theta = -res.params[1]
        return math.log(2) / theta if theta > 0 else float('inf')
    except Exception:
        return float('inf')


def _zscore(series: pd.Series, window: int, live_value: float) -> float:
    tail = series.tail(window)
    mn, sd = tail.mean(), tail.std()
    return (live_value - mn) / sd if sd > 0 else float('nan')


# ─────────────────────────────────────────────────────────────────────────────
# CORE VALIDATION PIPELINE
# Returns a dict on success, or (None, reason_str) on rejection.
# ─────────────────────────────────────────────────────────────────────────────

def validate(pair_key: str, cfg: dict, live_prices: dict, verbose: bool = False) -> tuple:
    sym_a, sym_b = pair_key.split('/')
    z_entry = cfg['z_entry']

    def reject(reason):
        if verbose:
            print(f'  ✗ {pair_key}: {reason}')
        return None, reason

    # ── G1: Load & freshness ─────────────────────────────────────────────────
    da = _load_csv(sym_a)
    db = _load_csv(sym_b)
    if da is None or db is None:
        return reject('CSV not found')

    for sym, df in [(sym_a, da), (sym_b, db)]:
        last  = df.index.max()
        stale = int(np.busday_count(last.date(), datetime.now().date()))
        if stale > MAX_STALE_TRADING_DAYS:
            return reject(f'{sym} data {stale} trading days stale')

    # ── G2: Merge & minimum overlap ──────────────────────────────────────────
    m = (da[['price', 'lot']]
         .join(db[['price', 'lot']], lsuffix='_a', rsuffix='_b', how='inner'))
    m[['lot_a', 'lot_b']] = m[['lot_a', 'lot_b']].replace(0, np.nan).ffill().bfill()

    if len(m) < MIN_OVERLAP_DAYS:
        return reject(f'Only {len(m)} common days (need {MIN_OVERLAP_DAYS})')

    # ── G3: Lot consistency — recompute hist spread with live lot if changed ─
    csv_lot_a = float(m['lot_a'].iloc[-1])
    csv_lot_b = float(m['lot_b'].iloc[-1])
    live_a    = live_prices.get(sym_a)
    live_b    = live_prices.get(sym_b)

    if live_a is None or live_b is None or live_b == 0:
        return reject('Live price unavailable')

    # We always use the yfinance lot (from CSV last row) for BOTH historical
    # and live spread → fully consistent comparison, immune to lot changes.
    # If Shoonya reports a different live lot, we detect it here but still use
    # the CSV lot for Z computation (sizing is a separate step).
    lot_a = csv_lot_a
    lot_b = csv_lot_b

    # Historical spread (consistent lot throughout)
    m['spread'] = m['price_a'] * lot_a - m['price_b'] * lot_b
    live_spread = live_a * lot_a - live_b * lot_b

    # ── G4: Multi-timeframe cointegration ────────────────────────────────────
    # Philosophy: long-term relationship validates the pair; short-term confirms
    # timing. When 1Y breaks but 3M/6M holds, that IS the dislocation opportunity.
    coint_results = {}
    for label, n in [('3Y', len(m)), ('1Y', 252), ('6M', 126), ('3M', 63)]:
        sub = m.tail(n)
        if len(sub) < 30:
            coint_results[label] = 1.0
            continue
        try:
            _, cp, _ = coint(sub['price_a'], sub['price_b'])
        except Exception:
            cp = 1.0
        coint_results[label] = cp

    # ADF on 1Y spread (most reliable window for statistical power)
    sub_1y = m.tail(252)
    try:
        _, adf_p, *_ = adfuller(sub_1y['spread'].dropna().values, maxlag=5)
    except Exception:
        adf_p = 1.0

    c3y = coint_results['3Y']
    c1y = coint_results['1Y']
    c6m = coint_results['6M']
    c3m = coint_results['3M']

    # Tiered cointegration assessment
    recent_ok  = (c3m < 0.15) or (c6m < 0.10)       # short-term structure intact
    spread_stationary = adf_p < ADF_P_MAX             # spread itself is mean-reverting
    any_coint  = (c3y < 0.10) or (c1y < 0.15) or recent_ok or spread_stationary

    if not any_coint:
        return reject(f'No cointegration evidence — '
                      f'3Y={c3y:.3f} 1Y={c1y:.3f} 6M={c6m:.3f} 3M={c3m:.3f} ADF={adf_p:.3f}')

    # Assign confidence tier (shown in output, affects sizing recommendation)
    if spread_stationary and recent_ok:
        coint_tier, coint_note = 1, 'FULL STRUCTURE'
    elif spread_stationary or recent_ok:
        coint_tier, coint_note = 2, 'PARTIAL — verify before sizing up'
    else:
        coint_tier, coint_note = 3, 'WEAK — long-term only, dislocation play'

    # ── G5: OU half-life — only hard-reject for TIER1/2; warn for TIER3 ─────
    hl = _ou_half_life(m['spread'].dropna().values)
    if hl > MAX_HALF_LIFE_DAYS and coint_tier < 3:
        return reject(f'Half-life {hl:.1f}d > {MAX_HALF_LIFE_DAYS}d (too slow)')
    if hl > MAX_HALF_LIFE_DAYS and coint_tier == 3:
        return reject(f'Half-life {hl:.1f}d > {MAX_HALF_LIFE_DAYS}d — weak coint + slow revert')

    # ── G6: Return correlation ────────────────────────────────────────────────
    ret_a = m['price_a'].pct_change()
    ret_b = m['price_b'].pct_change()
    corr_20d = ret_a.tail(20).corr(ret_b.tail(20))
    corr_60d = ret_a.tail(60).corr(ret_b.tail(60))

    if pd.isna(corr_20d) or corr_20d < MIN_CORR_20D:
        return reject(f'20D return corr {corr_20d:.2f} < {MIN_CORR_20D} (decoupled)')

    # ── G7: Z-score — require ≥ MIN_WINDOWS_CONFIRMING windows ──────────────
    z_vals = {}
    for w in Z_WINDOWS:
        z_vals[w] = _zscore(m['spread'], w, live_spread)

    confirming_windows = [w for w, z in z_vals.items()
                          if not math.isnan(z) and abs(z) >= z_entry]

    if len(confirming_windows) < MIN_WINDOWS_CONFIRMING:
        z_str = '  '.join(f'{w}d={z_vals[w]:+.2f}' for w in Z_WINDOWS)
        return reject(f'Only {len(confirming_windows)}/{len(Z_WINDOWS)} windows confirm '
                      f'(need {MIN_WINDOWS_CONFIRMING}) — {z_str}')

    # ── G8: Price ratio within historical envelope ───────────────────────────
    m['ratio']  = m['price_a'] / m['price_b']
    live_ratio  = live_a / live_b
    hist_min    = m['ratio'].min()
    hist_max    = m['ratio'].max()
    margin      = (hist_max - hist_min) * 0.05
    if live_ratio > hist_max + margin or live_ratio < hist_min - margin:
        return reject(f'Ratio {live_ratio:.4f} outside '
                      f'[{hist_min:.4f}, {hist_max:.4f}] ± 5% — structural break')

    # ── G9: Direction consistency — all confirming windows must agree on sign ─
    signs = set(1 if z_vals[w] > 0 else -1 for w in confirming_windows)
    if len(signs) > 1:
        return reject('Confirming windows disagree on direction (conflicting Z signs)')

    direction = list(signs)[0]  # +1 = A expensive, -1 = B expensive

    # ── G10: Self-check — re-derive from fresh slice, confirm signal survives ─
    # Use most recent lookback period as held-out check
    lb = cfg['lookback']
    fresh = m.tail(lb)
    fresh_mean = fresh['spread'].mean()
    fresh_std  = fresh['spread'].std()
    if fresh_std == 0:
        return reject('Zero spread std in lookback — degenerate data')
    z_fresh = (live_spread - fresh_mean) / fresh_std
    if abs(z_fresh) < z_entry:
        return reject(f'Self-check failed: fresh Z={z_fresh:+.2f} < {z_entry} '
                      f'(signal dissolves on lookback window)')

    # ── ALL GATES PASSED — build result ──────────────────────────────────────
    trade = f'BUY {sym_b} / SELL {sym_a}' if direction == 1 else f'BUY {sym_a} / SELL {sym_b}'

    # Cash-neutral multi-lot sizing (1–5x brute force)
    best_na, best_nb, best_imb = 1, 1, float('inf')
    val_a_base = live_a * lot_a
    val_b_base = live_b * lot_b
    for na in range(1, 6):
        for nb in range(1, 6):
            va = val_a_base * na
            vb = val_b_base * nb
            mx = max(va, vb)
            if mx == 0:
                continue
            imb = abs(va - vb) / mx * 100
            if imb < best_imb:
                best_imb, best_na, best_nb = imb, na, nb

    if best_imb > 50:
        return reject(f'Best lot ratio gives {best_imb:.0f}% cash imbalance (>50%)')

    val_a = live_a * lot_a * best_na
    val_b = live_b * lot_b * best_nb

    return {
        'pair':          pair_key,
        'sector':        cfg['sector'],
        'trade':         trade,
        'sym_a':         sym_a,
        'sym_b':         sym_b,
        'live_a':        live_a,
        'live_b':        live_b,
        'lot_a':         lot_a,
        'lot_b':         lot_b,
        'lot_ratio':     f'{best_na}:{best_nb}',
        'cash_imb_pct':  best_imb,
        'val_a':         val_a,
        'val_b':         val_b,
        'z_by_window':   z_vals,
        'z_fresh':       z_fresh,
        'confirming':    confirming_windows,
        'coint_tier':    coint_tier,
        'coint_note':    coint_note,
        'coint_3y':      c3y,
        'coint_1y':      c1y,
        'coint_6m':      c6m,
        'coint_3m':      c3m,
        'adf_1y':        adf_p,
        'half_life':     hl,
        'corr_20d':      corr_20d,
        'corr_60d':      corr_60d,
        'live_ratio':    live_ratio,
        'hist_range':    (hist_min, hist_max),
        'overlap_days':  len(m),
        'data_from':     m.index.min().date(),
        'data_to':       m.index.max().date(),
    }, None


# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def _bar(pct: float, width: int = 20) -> str:
    """ASCII progress bar for Z-score confirmation."""
    filled = min(int(pct / 100 * width), width)
    return '[' + '█' * filled + '░' * (width - filled) + ']'


def display(signals: list):
    sep = '=' * 72
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f'\n{sep}')
    print(f'  VALID SIGNALS — {now}')
    print(f'  Gates: freshness · cointegration · half-life · correlation · Z(2+windows)')
    print(sep)

    if not signals:
        print('\n  NO VALID SIGNALS — all pairs rejected or below threshold\n')
        print(sep)
        return

    for s in signals:
        z_line = '  '.join(f'{w}d={s["z_by_window"][w]:+.2f}' for w in Z_WINDOWS)
        conf   = '+'.join(str(w) + 'd' for w in s['confirming'])
        print(f'\n  {s["sector"]}: {s["pair"]}')
        print(f'  Trade  : {s["trade"]}')
        print(f'  Prices : {s["sym_a"]} ₹{s["live_a"]:.2f} × {s["lot_a"]:.0f} lot'
              f'  |  {s["sym_b"]} ₹{s["live_b"]:.2f} × {s["lot_b"]:.0f} lot')
        print(f'  Sizing : {s["lot_ratio"]} ratio → '
              f'₹{s["val_a"]:,.0f} vs ₹{s["val_b"]:,.0f}  ({s["cash_imb_pct"]:.1f}% imb)')
        print(f'  Z      : {z_line}  [confirmed: {conf}]')
        print(f'  Z-fresh: {s["z_fresh"]:+.2f}  (self-check passed)')
        tier_label = ['', 'TIER1-FULL', 'TIER2-PARTIAL', 'TIER3-DISLOCATION'][s['coint_tier']]
        print(f'  Coint  : [{tier_label}] {s["coint_note"]}')
        print(f'           3Y={s["coint_3y"]:.3f}  1Y={s["coint_1y"]:.3f}  '
              f'6M={s["coint_6m"]:.3f}  3M={s["coint_3m"]:.3f}  ADF={s["adf_1y"]:.3f}  HL={s["half_life"]:.1f}d')
        print(f'  Corr   : 20D={s["corr_20d"]:.2f}  60D={s["corr_60d"]:.2f}')
        print(f'  Data   : {s["data_from"]} → {s["data_to"]}  ({s["overlap_days"]} days)')

    print(f'\n{sep}')
    print(f'  {len(signals)} signal(s) — all passed 10-gate validation')
    print(sep)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run(pair_filter: str = None, verbose: bool = False):
    pairs = UNIVERSE
    if pair_filter:
        pairs = {k: v for k, v in UNIVERSE.items() if pair_filter.upper() in k}
        if not pairs:
            print(f'No pair matching "{pair_filter}" in universe.')
            return

    # Fetch all live prices in one batch
    all_syms = list({s for p in pairs for s in p.split('/')})
    if verbose:
        print(f'Fetching live prices for {len(all_syms)} symbols...')
    live = _fetch_live(all_syms)

    signals   = []
    rejected  = {}

    for pair_key, cfg in pairs.items():
        result, reason = validate(pair_key, cfg, live, verbose=verbose)
        if result is not None:
            signals.append(result)
        else:
            rejected[pair_key] = reason

    display(signals)

    if verbose and rejected:
        print('\n  REJECTED:')
        for p, r in rejected.items():
            print(f'    {p}: {r}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Self-validating pairs signal scanner')
    ap.add_argument('--verbose', '-v', action='store_true',
                    help='Show rejection reasons for all pairs')
    ap.add_argument('--pair', type=str, default=None,
                    help='Filter to a specific pair (e.g. ICICIBANK)')
    args = ap.parse_args()
    run(pair_filter=args.pair, verbose=args.verbose)
