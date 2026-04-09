"""
System 1: Dynamic Pairs Trading Engine (v5)
===========================================
Accepts a dynamic universe from universe_scanner — NOT a fixed pair list.

Key upgrades vs v4:
  - Ranking by Signal Strength Score (SSS = |Z| × (1 + corr_60d))
    rather than raw |Z|. Rewards pairs that are BOTH extreme AND correlated.
  - Capital-aware entry gate via CapitalTracker (never over-commit margin)
  - Self-annealing pair modifiers (suppress/boost after ≥10 trades)
  - Structural break detection: Z GROWING FURTHER from 0 while in trade
    (sustained divergence) triggers early close, not just magnitude on day 1

Exit logic (intraday-aware):
  - BUY_A (spread was too low): best_z = (A_hi×ma - B_lo×mb - mean) / std
      if best_z >= -z_exit → intraday exit at midpoint(H/L, EOD)
  - SELL_A (spread was too high): best_z = (A_lo×ma - B_hi×mb - mean) / std
      if best_z <= +z_exit → intraday exit
  - Time stop: 30 days
  - Structural break: |Z| > |entry_Z| for 5 consecutive days (widening, not reverting)

Self-annealing rules (pair_modifiers dict):
  - < 10 trades:               modifier = 1.0  (no penalty for early bad luck)
  - ≥ 10 trades, WR < 50%:    modifier = 0.7  (deprioritise, don't eliminate)
  - ≥ 10 trades, WR ≥ 70%:    modifier = 1.2  (boost reliable pairs)
  - 3 consecutive losses:      modifier = 0.0  (blacklist for this window)
"""

import pandas as pd
import numpy as np
import os

DATA_DIR        = '.tmp/3y_data'
Z_ENTRY         = 2.5      # default; overridden by run() / sss_threshold replaces this
Z_EXIT          = 0.3      # overridden by run() parameter
TIME_STOP_DAYS  = 30
MAX_POSITIONS   = 5        # raised from 3 — capital tracker is the real constraint
WINDOW          = 60
SSS_THRESHOLD   = 4.0      # minimum Signal Strength Score to enter

# ── Historical lot corrections (yfinance/stale data patches) ──────────────────
LOT_SCHEDULE = {
    'NMDC':      [(pd.Timestamp('1900-01-01'), 4500),
                  (pd.Timestamp('2024-12-27'), 13500),
                  (pd.Timestamp('2025-04-25'), 6750)],
    'COALINDIA': [(pd.Timestamp('1900-01-01'), 1050),
                  (pd.Timestamp('2025-10-28'), 1350)],
}

# Fallback universe (used if no dynamic universe provided)
UNIVERSE = [
    ('ULTRACEMCO', 'GRASIM',      5,   5),
    ('ICICIBANK',  'HDFCBANK',    2,   5),
    ('LICHSGFIN',  'PFC',         3,   3),
    ('NMDC',       'COALINDIA',   10,  12),
    ('LT',         'GMRAIRPORT',  2,   5),
    ('TATAPOWER',  'NHPC',        5,   8),
]


def get_lot(symbol, date):
    if symbol not in LOT_SCHEDULE:
        return None
    lot = LOT_SCHEDULE[symbol][0][1]
    for eff, size in LOT_SCHEDULE[symbol]:
        if date >= eff:
            lot = size
    return lot


def load_ohlc(symbol):
    """Load daily OHLC + lot series with historical lot patching."""
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date']  = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['close'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['high']  = pd.to_numeric(df['FH_TRADE_HIGH_PRICE'], errors='coerce')
    df['low']   = pd.to_numeric(df['FH_TRADE_LOW_PRICE'], errors='coerce')
    df['open']  = pd.to_numeric(df['FH_OPENING_PRICE'], errors='coerce')
    df['lot']   = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce')

    has_expiry = False
    if 'FH_EXPIRY_DT' in df.columns:
        df['expiry'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        if df['expiry'].notna().sum() > 10:
            has_expiry = True

    if has_expiry:
        if 'FH_INSTRUMENT' in df.columns:
            df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
        df = df.dropna(subset=['date', 'close', 'expiry'])
        df = df.sort_values(['date', 'expiry'])
        rows = []
        for dt, grp in df.groupby('date'):
            near = grp[grp['expiry'] > dt].nsmallest(1, 'expiry')
            if not near.empty:
                r = near.iloc[0]
                rows.append({'date': dt, 'close': r['close'],
                             'high':  r['FH_TRADE_HIGH_PRICE'],
                             'low':   r['FH_TRADE_LOW_PRICE'],
                             'open':  r['FH_OPENING_PRICE'],
                             'lot':   r['lot']})
        out = pd.DataFrame(rows).set_index('date')
    else:
        df = df.dropna(subset=['date', 'close'])
        df = df.sort_values('date').drop_duplicates('date')
        out = df[['date', 'close', 'high', 'low', 'open', 'lot']].set_index('date')

    # Patch with real historical lots
    if symbol in LOT_SCHEDULE:
        out['lot'] = out.index.map(lambda d: get_lot(symbol, d))

    return out


# ── Self-annealing helpers ────────────────────────────────────────────────────

def _update_modifier(pair_modifiers: dict, key: tuple, win: bool):
    """
    Update the pair modifier after each trade close.

    Self-annealing: cut losers, ride winners. Never blacklist.
    Rules:
      < 10 trades:             modifier = 1.0  (no judgement yet)
      ≥10 trades, WR < 40%:   modifier = 0.5  (heavy deprioritise)
      ≥10 trades, WR < 55%:   modifier = 0.7  (mild deprioritise)
      ≥10 trades, WR ≥ 70%:   modifier = 1.3  (ride winners hard)
      ≥10 trades, WR ≥ 60%:   modifier = 1.1  (mild boost)
      otherwise:               modifier = 1.0
    """
    if key not in pair_modifiers:
        pair_modifiers[key] = {'results': [], 'modifier': 1.0}

    state = pair_modifiers[key]
    state['results'].append(win)

    results = state['results']
    n = len(results)

    # Only evaluate with statistical significance
    if n >= 10:
        wr = sum(results) / n
        if wr < 0.40:
            state['modifier'] = 0.50
        elif wr < 0.55:
            state['modifier'] = 0.70
        elif wr >= 0.70:
            state['modifier'] = 1.30
        elif wr >= 0.60:
            state['modifier'] = 1.10
        else:
            state['modifier'] = 1.00


def get_modifier(pair_modifiers: dict, key: tuple) -> float:
    if key not in pair_modifiers:
        return 1.0
    return pair_modifiers[key]['modifier']


# ── Signal precomputation ─────────────────────────────────────────────────────

def precompute_signals(price_data: dict, universe: list) -> dict:
    """
    Precompute for every pair: daily Z-score, SSS inputs, blackout flags.

    universe: list of (sym_a, sym_b, lots_a, lots_b)
    Returns dict keyed by (sym_a, sym_b).
    """
    signals = {}
    for sym_a, sym_b, lots_a, lots_b in universe:
        if sym_a not in price_data or sym_b not in price_data:
            continue
        pa_df = price_data[sym_a]
        pb_df = price_data[sym_b]
        common = pa_df.index.intersection(pb_df.index)
        if len(common) < WINDOW + 10:
            continue

        pa = pa_df.loc[common, 'close']
        pb = pb_df.loc[common, 'close']
        la = pa_df.loc[common, 'lot']
        lb = pb_df.loc[common, 'lot']

        mult_a = lots_a * la
        mult_b = lots_b * lb
        spread = mult_a * pa - mult_b * pb
        mean   = spread.rolling(WINDOW).mean()
        std    = spread.rolling(WINDOW).std().replace(0, np.nan)
        z      = (spread - mean) / std

        # 60d return correlation for SSS = |Z| × (1 + corr)
        ret_a  = pa.pct_change()
        ret_b  = pb.pct_change()
        corr   = ret_a.rolling(WINDOW).corr(ret_b).clip(0, 1)

        # Blackout: WINDOW days after any lot change
        lc       = ((la != la.shift(1)) & la.notna() & la.shift(1).notna()) | \
                   ((lb != lb.shift(1)) & lb.notna() & lb.shift(1).notna())
        blackout = pd.Series(False, index=common)
        for chg in common[lc]:
            idx = common.get_loc(chg)
            blackout.iloc[idx:min(idx + WINDOW, len(common))] = True

        signals[(sym_a, sym_b)] = {
            'z': z, 'mean': mean, 'std': std,
            'spread': spread, 'blackout': blackout,
            'corr': corr,
            'mult_a': mult_a, 'mult_b': mult_b,
            'lots_a': lots_a, 'lots_b': lots_b,
            'pa': pa, 'pb': pb,
            'pa_hi': pa_df.loc[common, 'high'],
            'pa_lo': pa_df.loc[common, 'low'],
            'pb_hi': pb_df.loc[common, 'high'],
            'pb_lo': pb_df.loc[common, 'low'],
        }

    return signals


# ── Main run loop ─────────────────────────────────────────────────────────────

def run(price_data: dict, start, end,
        z_exit        = Z_EXIT,
        sss_threshold = SSS_THRESHOLD,
        universe      = None,
        pair_modifiers: dict = None,
        capital_tracker      = None) -> pd.DataFrame:
    """
    Run the dynamic pairs system.

    Parameters
    ----------
    price_data      : {symbol → OHLC DataFrame from load_ohlc()}
    start / end     : backtest date range
    z_exit          : Z at which we take profit (exit threshold)
    sss_threshold   : minimum SSS = |Z| × (1 + corr) to open a position
    universe        : list of (sym_a, sym_b, lots_a, lots_b)
                      If None, falls back to the hardcoded UNIVERSE constant.
    pair_modifiers  : shared dict for self-annealing state (mutated in place)
    capital_tracker : CapitalTracker instance (None = unlimited, for grid search compat)
    """
    active_universe = universe if universe is not None else UNIVERSE
    signals  = precompute_signals(price_data, active_universe)
    dates    = pd.bdate_range(start, end)
    positions = {}    # key: (sym_a, sym_b) → position dict
    trades    = []

    for dt in dates:
        dt = pd.Timestamp(dt)

        # ── 1. Manage existing positions ──────────────────────────────────────
        to_close = []
        for key, pos in positions.items():
            sym_a, sym_b = key
            sig = signals.get(key)
            if sig is None or dt not in sig['z'].index:
                continue

            cur_z    = sig['z'].get(dt)
            cur_mean = sig['mean'].get(dt)
            cur_std  = sig['std'].get(dt)
            cur_pa   = sig['pa'].get(dt)
            cur_pb   = sig['pb'].get(dt)
            cur_ma   = sig['mult_a'].get(dt)
            cur_mb   = sig['mult_b'].get(dt)
            pa_hi    = sig['pa_hi'].get(dt)
            pa_lo    = sig['pa_lo'].get(dt)
            pb_hi    = sig['pb_hi'].get(dt)
            pb_lo    = sig['pb_lo'].get(dt)

            if pd.isna(cur_z) or pd.isna(cur_std) or cur_std == 0:
                continue

            days      = (dt - pos['entry_date']).days
            direction = pos['direction']
            entry_ma  = pos['mult_a']
            entry_mb  = pos['mult_b']
            entry_z   = pos['entry_z']

            # ── Lot change: force exit ────────────────────────────────────────
            lot_changed = sig['blackout'].get(dt, False) and \
                          (cur_ma != entry_ma or cur_mb != entry_mb)

            # ── Intraday best-case spread ─────────────────────────────────────
            if direction == +1:   # BUY_A: profit when spread rises
                best_sp = pa_hi * entry_ma - pb_lo * entry_mb
                intr_exit_pa, intr_exit_pb = pa_hi, pb_lo
            else:                 # SELL_A: profit when spread falls
                best_sp = pa_lo * entry_ma - pb_hi * entry_mb
                intr_exit_pa, intr_exit_pb = pa_lo, pb_hi
            best_z = (best_sp - cur_mean) / cur_std if cur_std else cur_z

            intraday_hit = (direction == +1 and best_z >= -z_exit) or \
                           (direction == -1 and best_z <=  z_exit)
            eod_hit      = (direction == +1 and cur_z  >= -z_exit) or \
                           (direction == -1 and cur_z  <=  z_exit)

            # ── Structural break: Z widening further from 0 for 5 days ───────
            # User insight: sustained divergence (not a single-day spike) = break
            struct_break = False
            if days >= 5:
                # Check last 5 days of Z: all going AWAY from 0 vs entry direction
                z_hist = sig['z']
                z_window = z_hist[(z_hist.index <= dt) &
                                  (z_hist.index >= pos['entry_date'])]
                if len(z_window) >= 5:
                    last5 = z_window.iloc[-5:]
                    # If spread is consistently more extreme than at entry
                    if direction == +1:   # BUY_A: entered at Z < -entry_thresh
                        struct_break = bool((last5 < entry_z).all())  # Z still < entry_Z = widening
                    else:                 # SELL_A: entered at Z > entry_thresh
                        struct_break = bool((last5 > entry_z).all())  # Z still > entry_Z = widening

            reason = None
            if lot_changed:              reason = 'LOT_CHANGE'
            elif struct_break:           reason = 'STRUCT_BREAK'
            elif intraday_hit:
                reason = 'INTRADAY' if not eod_hit else 'PROFIT'
            elif days >= TIME_STOP_DAYS: reason = 'TIME_STOP'

            if reason:
                if reason == 'INTRADAY':
                    # Mid-point of best H/L and EOD close — realistic, avoids
                    # "hit A's high AND B's low simultaneously" fantasy
                    exit_pa = (intr_exit_pa + cur_pa) / 2
                    exit_pb = (intr_exit_pb + cur_pb) / 2
                else:
                    exit_pa, exit_pb = cur_pa, cur_pb

                gross = direction * (exit_pa * entry_ma - exit_pb * entry_mb
                                     - pos['entry_pa'] * entry_ma
                                     + pos['entry_pb'] * entry_mb)
                chg   = abs(gross) * 0.12
                net   = gross - chg

                trades.append(dict(
                    pair=f"{sym_a}/{sym_b}", strategy='PAIRS',
                    entry_date=pos['entry_date'], exit_date=dt,
                    days=days,
                    direction='BUY_A' if direction == +1 else 'SELL_A',
                    entry_z=round(entry_z, 3),
                    exit_z=round(cur_z, 3),
                    gross=round(gross, 0),
                    charges=round(chg, 0),
                    net=round(net, 0),
                    reason=reason,
                    win=net > 0,
                    shares_a=int(entry_ma),
                    shares_b=int(entry_mb),
                ))
                to_close.append(key)

                # Self-annealing: update pair modifier
                if pair_modifiers is not None:
                    _update_modifier(pair_modifiers, key, net > 0)

                # Capital: release margin, credit P&L
                if capital_tracker is not None:
                    capital_tracker.release(key, net)

        for key in to_close:
            del positions[key]

        # ── 2. Scan for new signals ───────────────────────────────────────────
        if len(positions) >= MAX_POSITIONS:
            continue

        candidates = []
        active_syms = {s for pair in positions for s in pair}

        for key, sig in signals.items():
            sym_a, sym_b = key
            if sym_a in active_syms or sym_b in active_syms:
                continue
            if dt not in sig['z'].index:
                continue
            if sig['blackout'].get(dt, False):
                continue

            cur_z    = sig['z'].get(dt)
            corr_60d = sig['corr'].get(dt, 0)
            if pd.isna(cur_z) or pd.isna(corr_60d):
                continue
            corr_60d = max(0.0, float(corr_60d))

            # Reject Z > 4.0 at discovery — sustained move before we looked
            # means the relationship may have already broken (NOT a new signal)
            if abs(cur_z) > 4.0:
                continue

            sss = abs(cur_z) * (1.0 + corr_60d)
            modifier = get_modifier(pair_modifiers, key) if pair_modifiers else 1.0
            if modifier == 0.0:
                continue   # blacklisted pair

            eff_sss = sss * modifier
            if eff_sss < sss_threshold:
                continue

            candidates.append((eff_sss, key, cur_z, corr_60d))

        # Sort strongest SSS first
        candidates.sort(reverse=True)

        for eff_sss, key, cur_z, corr_60d in candidates:
            if len(positions) >= MAX_POSITIONS:
                break
            sym_a, sym_b = key
            active_syms  = {s for pair in positions for s in pair}
            if sym_a in active_syms or sym_b in active_syms:
                continue

            sig    = signals[key]
            cur_pa = sig['pa'].get(dt)
            cur_pb = sig['pb'].get(dt)
            cur_ma = sig['mult_a'].get(dt)
            cur_mb = sig['mult_b'].get(dt)
            if pd.isna(cur_pa) or pd.isna(cur_pb):
                continue

            # Capital gate: check margin before committing
            if capital_tracker is not None:
                margin = capital_tracker.estimate_margin(cur_pa, cur_pb, cur_ma, cur_mb)
                if not capital_tracker.can_open(margin):
                    continue   # not enough free capital
                capital_tracker.commit(key, margin)

            positions[key] = dict(
                entry_date=dt,
                entry_z=cur_z,
                entry_pa=cur_pa,
                entry_pb=cur_pb,
                mult_a=cur_ma,
                mult_b=cur_mb,
                direction=-1 if cur_z > 0 else +1,
            )

    return pd.DataFrame(trades) if trades else pd.DataFrame()
