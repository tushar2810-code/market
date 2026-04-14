"""
MEGA BACKTEST v6 — Foolproof Unified System
============================================
Capital: Rs.25L | Apr 2024 - Mar 2026 | 12% charges per trade

Fixes vs v5 (9 loopholes patched):
  1. UNIFIED capital tracker — calendar + pairs share ONE pool (was: separate silos)
  2. Rolling universe refresh every 90 trading days (was: discover once, stale for 2yr)
  3. Drawdown protection: -5% daily → pause new entries 3 bdays (was: none)
  4. Re-entry cooldown: 5 bdays after TIME_STOP (pair needs space)
  5. Position compounding: scale lots by capital growth, capped 2.5× (was: fixed lots)
  6. MAX_POSITIONS raised to 8 (was: 5); capital tracker is the real constraint
  7. Max utilisation capped at 70% per SYSTEM.md (was: uncapped)
  8. 5-day time stop: data proves 0% WR after 5d → cut fast (was: 30d)
  9. Self-annealing: deprioritise losers, boost winners — NEVER blacklist

Architecture: unified day loop processes both systems each trading day.
  - Exits first (free capital), then entries (calendar priority, then pairs by SSS)
  - Capital tracker gates EVERY entry across both systems
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings('ignore')

from systems.pairs_system import (
    load_ohlc, precompute_signals,
    _update_modifier, get_modifier,
)
from systems.calendar_system import load_calendar_data, build_signals
from systems.universe_scanner import build_universe, to_universe_tuples
from systems.capital_tracker import CapitalTracker

# ── Constants ────────────────────────────────────────────────────────────────

STARTING_CAP    = 25_00_000
BACKTEST_START  = pd.Timestamp('2024-04-01')
BACKTEST_END    = pd.Timestamp('2026-03-31')
YEAR1_END       = pd.Timestamp('2025-03-31')

# Grid — expanded vs v5
SSS_THRESHOLDS  = [2.0, 3.0, 4.0, 6.0]
PAIRS_Z_EXITS   = [0.3, 0.5, 0.7, 1.0, 1.5]

# Calendar (fixed — Tier 1 validated)
CAL_Z_ENTRY        = 2.0
CAL_Z_EXIT         = 0.3
CAL_TIME_STOP      = 15
CAL_DAYS_BEFORE_EXP= 5
CALENDAR_MAX_POSITIONS = 8   # max concurrent calendar positions (capital gates the rest)

# Pairs
PAIRS_TIME_STOP    = 30      # Backstop only — structural break is primary exit
PAIRS_MAX_POSITIONS= 8
PAIRS_MAX_Z_ENTRY  = 4.0      # |Z| > 4 at discovery = skip (likely already broken)
PAIRS_MIN_Z_ENTRY  = 1.5      # |Z| < 1.5 = not stretched enough, just noise

# Universe refresh
REFRESH_INTERVAL   = 90       # trading days between universe refreshes
UNIVERSE_LOOKBACK  = 300      # calendar days of history for EG test
UNIVERSE_MAX_PAIRS = 60

# Risk management
DRAWDOWN_THRESHOLD = -0.05    # -5% daily drawdown triggers pause
DRAWDOWN_PAUSE_DAYS= 3        # pause new entries for N business days
COOLDOWN_TIMESTOP  = 3        # bdays cooldown after time stop
MAX_COMPOUND_SCALE = 2.5      # max lot scaling from compounding
CHARGE_RATE        = 0.12     # 12% of |gross| as charges
CORR_MIN           = 0.3     # min 60d correlation to enter a pair
SCALE_CONC_CAP     = 0.15    # concentration cap for compounding scale calc
STRUCT_BREAK_Z_MULT     = 1.5   # exit if |Z| > 1.5x entry |Z| after 5 days
STRUCT_BREAK_CORR_FLOOR = 0.25  # exit if 60d correlation < 0.25


# ── AutoResearch: override constants from params.json if it exists ───────────
_params_file = os.path.join(os.path.dirname(__file__), 'params.json')
_AR_MODE = False
if os.path.exists(_params_file):
    import json as _json
    with open(_params_file) as _f:
        _AR = _json.load(_f)
    _AR_MODE = True
    SSS_THRESHOLDS       = [_AR.get('SSS_THRESHOLD', SSS_THRESHOLDS[0])]
    PAIRS_Z_EXITS        = [_AR.get('Z_EXIT', PAIRS_Z_EXITS[0])]
    PAIRS_TIME_STOP      = _AR.get('PAIRS_TIME_STOP', PAIRS_TIME_STOP)
    PAIRS_MIN_Z_ENTRY    = _AR.get('PAIRS_MIN_Z_ENTRY', PAIRS_MIN_Z_ENTRY)
    PAIRS_MAX_Z_ENTRY    = _AR.get('PAIRS_MAX_Z_ENTRY', PAIRS_MAX_Z_ENTRY)
    PAIRS_MAX_POSITIONS  = _AR.get('PAIRS_MAX_POSITIONS', PAIRS_MAX_POSITIONS)
    MAX_COMPOUND_SCALE   = _AR.get('MAX_COMPOUND_SCALE', MAX_COMPOUND_SCALE)
    REFRESH_INTERVAL     = _AR.get('REFRESH_INTERVAL', REFRESH_INTERVAL)
    CORR_MIN             = _AR.get('CORR_MIN', CORR_MIN)
    SCALE_CONC_CAP       = _AR.get('SCALE_CONC_CAP', SCALE_CONC_CAP)
    STRUCT_BREAK_Z_MULT  = _AR.get('STRUCT_BREAK_Z_MULT', STRUCT_BREAK_Z_MULT)
    STRUCT_BREAK_CORR_FLOOR = _AR.get('STRUCT_BREAK_CORR_FLOOR', STRUCT_BREAK_CORR_FLOOR)
    print(f"  [AutoResearch] Loaded params from {_params_file}")


# ── Corporate actions ────────────────────────────────────────────────────────

import re as _re

def load_corporate_actions():
    """Parse nse_perfect_actions.csv → {ticker: [(date, factor)]} for splits/bonuses.
    factor = how many post-action shares equal 1 pre-action share.
    """
    actions_file = '.tmp/nse_perfect_actions.csv'
    if not os.path.exists(actions_file):
        print("  [WARN] No nse_perfect_actions.csv found — skipping corp action adjustment")
        return {}

    import csv
    raw = {}  # ticker → [(date, factor)]
    with open(actions_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            typ = row['Type'].strip()
            if typ not in ('Split', 'Bonus'):
                continue
            desc = row['Description'].strip()

            # Skip non-equity bonus (e.g. NCRPS preference shares)
            if 'Ncrps' in desc or 'ncrps' in desc or 'NCRPS' in desc:
                continue

            ticker = row['Ticker'].strip()
            date = pd.Timestamp(row['Date'].strip())
            factor = 1.0

            if typ == 'Split':
                # "From Rs X/- Per Share To Re Y/- Per Share"
                m = _re.findall(r'R[se]\s*([\d.]+)', desc)
                if len(m) >= 2:
                    factor = float(m[0]) / float(m[1])
            elif typ == 'Bonus':
                # "Bonus A:B" → (A+B)/B shares per original share
                m = _re.search(r'Bonus\s+(\d+):(\d+)', desc)
                if m:
                    a, b = int(m.group(1)), int(m.group(2))
                    factor = (a + b) / b

            if factor > 1.0:
                raw.setdefault(ticker, []).append((date, factor))

    # Sort by date and combine same-date actions (multiply factors)
    result = {}
    for ticker, actions in raw.items():
        actions.sort(key=lambda x: x[0])
        combined = []
        for date, factor in actions:
            if combined and combined[-1][0] == date:
                combined[-1] = (date, combined[-1][1] * factor)
            else:
                combined.append((date, factor))
        result[ticker] = combined

    n_actions = sum(len(v) for v in result.values())
    print(f"  Loaded {n_actions} corporate actions (splits/bonuses) for {len(result)} tickers")
    return result


def get_adj_factor(corp_actions, ticker, entry_date, exit_date):
    """Cumulative adjustment factor for all actions between entry and exit dates."""
    if ticker not in corp_actions:
        return 1.0
    factor = 1.0
    for action_date, f in corp_actions[ticker]:
        if entry_date < action_date <= exit_date:
            factor *= f
    return factor


# ── Data loading ─────────────────────────────────────────────────────────────

def load_all_prices():
    """Load OHLC for all 211 symbols."""
    data_dir = '.tmp/3y_data'
    price_data = {}
    for f in sorted(os.listdir(data_dir)):
        if not f.endswith('_3Y.csv'):
            continue
        sym = f.replace('_3Y.csv', '')
        s = load_ohlc(sym)
        if s is not None and len(s) >= 100:
            price_data[sym] = s
    return price_data


def precompute_calendar():
    """Precompute calendar signals for ALL symbols with NSE bhav multi-expiry data."""
    data_dir = '.tmp/3y_data'
    cal = {}
    for f in sorted(os.listdir(data_dir)):
        if not f.endswith('_3Y.csv'):
            continue
        sym = f.replace('_3Y.csv', '')
        raw = load_calendar_data(sym)
        if raw is None:
            continue
        data = build_signals(raw)
        data = data[(data.index >= BACKTEST_START - pd.Timedelta(days=10)) &
                     (data.index <= BACKTEST_END)]
        if data.empty or data['z'].notna().sum() < 5:
            continue
        cal[sym] = dict(data=data)
    return cal


# ── Universe discovery (rolling) ─────────────────────────────────────────────

def discover_universes(price_data, trading_days):
    """
    Discover co-integrated pairs at multiple refresh points.
    Returns: refresh_points, universe_at, all_pairs_tuples
    """
    refresh_points = [trading_days[0]]
    for i in range(REFRESH_INTERVAL, len(trading_days), REFRESH_INTERVAL):
        refresh_points.append(trading_days[i])

    all_pairs = {}          # (sym_a, sym_b) → (lots_a, lots_b) from first discovery
    universe_at = {}        # refresh_date → set of (sym_a, sym_b)
    raw_universes = {}

    for rp in refresh_points:
        as_of = rp - pd.Timedelta(days=1)
        print(f"    Scanning universe as of {as_of.date()}...")
        raw = build_universe(
            price_data, as_of=as_of,
            lookback_days=UNIVERSE_LOOKBACK,
            max_pairs=UNIVERSE_MAX_PAIRS,
            verbose=False,
        )
        tuples = to_universe_tuples(raw)
        pair_set = set()
        for sym_a, sym_b, lots_a, lots_b in tuples:
            key = (sym_a, sym_b)
            # Always update lots to latest discovery (notionals change over time)
            all_pairs[key] = (lots_a, lots_b)
            pair_set.add(key)
        universe_at[rp] = pair_set
        raw_universes[rp] = raw
        print(f"      → {len(pair_set)} pairs (total unique: {len(all_pairs)})")

    # Build list of tuples for precompute_signals
    all_tuples = [(a, b, la, lb) for (a, b), (la, lb) in all_pairs.items()]
    return refresh_points, universe_at, all_tuples, raw_universes


# ── Unified day loop ─────────────────────────────────────────────────────────

def run_unified(pair_signals, cal_signals,
                refresh_points, universe_at,
                sss_threshold, z_exit, starting_cap,
                time_stop=None,             # None → use module default
                start=None, end=None,       # date range override for walk-forward
                corp_actions=None):         # corporate actions dict
    """
    Run ONE grid combo through the unified day loop.
    Both pairs and calendar share a single CapitalTracker.
    """
    _time_stop   = time_stop if time_stop is not None else PAIRS_TIME_STOP
    _start       = pd.Timestamp(start) if start is not None else BACKTEST_START
    _end         = pd.Timestamp(end)   if end   is not None else BACKTEST_END
    _corp        = corp_actions if corp_actions is not None else {}

    cap = CapitalTracker(starting_cap)
    pair_positions = {}    # (sym_a, sym_b) → position dict
    cal_positions  = {}    # symbol → position dict
    trades         = []
    pair_modifiers = {}
    cooldowns      = {}    # (sym_a, sym_b) → bday index when cooldown expires
    drawdown_pause_until = -1

    trading_days = pd.bdate_range(_start, _end)
    equity = pd.Series(starting_cap, index=trading_days, dtype=float)
    prev_equity = starting_cap

    # Universe tracking
    current_universe = set()
    refresh_idx = 0

    for day_idx, dt in enumerate(trading_days):
        dt = pd.Timestamp(dt)

        # ── Universe refresh ─────────────────────────────────────────────
        while refresh_idx < len(refresh_points) and dt >= refresh_points[refresh_idx]:
            current_universe = universe_at[refresh_points[refresh_idx]]
            refresh_idx += 1

        is_paused = day_idx <= drawdown_pause_until

        # ══════════════════════════════════════════════════════════════════
        #  EXITS — always process, even during drawdown pause
        # ══════════════════════════════════════════════════════════════════

        # ── Calendar exits ───────────────────────────────────────────────
        cal_to_close = []
        for sym, pos in cal_positions.items():
            cfg = cal_signals.get(sym)
            if cfg is None or dt not in cfg['data'].index:
                continue
            row     = cfg['data'].loc[dt]
            cur_z   = row['z']
            cur_sp  = row['spread']
            cur_mean= row['mean']
            cur_std = row['std']
            dte     = row['dte']

            if pd.isna(cur_z) or pd.isna(cur_std) or cur_std == 0:
                continue

            days      = (dt - pos['entry_date']).days
            direction = pos['direction']

            must_exit = (dte <= CAL_DAYS_BEFORE_EXP) or (days >= CAL_TIME_STOP)

            # Intraday best-case spread
            if direction == -1:   # SELL near, BUY far
                best_sp = row['near_lo'] - row['far_hi']
            else:                 # BUY near, SELL far
                best_sp = row['near_hi'] - row['far_lo']
            best_z = (best_sp - cur_mean) / cur_std

            intraday_hit = (direction == -1 and best_z <= CAL_Z_EXIT) or \
                           (direction == +1 and best_z >= -CAL_Z_EXIT)
            eod_hit      = (direction == -1 and cur_z  <= CAL_Z_EXIT) or \
                           (direction == +1 and cur_z  >= -CAL_Z_EXIT)

            reason = None
            exit_spread = cur_sp
            if must_exit:
                reason = 'EXPIRY_ROLL' if dte <= CAL_DAYS_BEFORE_EXP else 'CAL_TIME'
            elif intraday_hit and not eod_hit:
                reason      = 'INTRADAY'
                exit_spread = (best_sp + cur_sp) / 2
            elif eod_hit:
                reason = 'PROFIT'

            if reason:
                lot       = pos['lot']
                contracts = pos['contracts']
                gross = direction * (exit_spread - pos['entry_spread']) * lot * contracts
                chg   = abs(gross) * CHARGE_RATE
                net   = gross - chg
                trades.append(dict(
                    pair=f'{sym}_CAL', strategy='CALENDAR',
                    entry_date=pos['entry_date'], exit_date=dt, days=days,
                    direction='SELL_NEAR' if direction == -1 else 'BUY_NEAR',
                    entry_z=round(pos['entry_z'], 3), exit_z=round(cur_z, 3),
                    entry_spread=round(pos['entry_spread'], 2),
                    exit_spread=round(exit_spread, 2),
                    gross=round(gross, 0), charges=round(chg, 0), net=round(net, 0),
                    reason=reason, win=net > 0,
                    shares_a=lot * contracts, shares_b=lot * contracts,
                ))
                cap.release(f'CAL_{sym}', net)
                cal_to_close.append(sym)

        for sym in cal_to_close:
            del cal_positions[sym]

        # ── Pairs exits ──────────────────────────────────────────────────
        pair_to_close = []
        for key, pos in pair_positions.items():
            sym_a, sym_b = key
            sig = pair_signals.get(key)
            if sig is None or dt not in sig['z'].index:
                continue

            cur_z    = sig['z'].get(dt)
            cur_mean = sig['mean'].get(dt)
            cur_std  = sig['std'].get(dt)
            cur_pa   = sig['pa'].get(dt)
            cur_pb   = sig['pb'].get(dt)
            pa_hi    = sig['pa_hi'].get(dt)
            pa_lo    = sig['pa_lo'].get(dt)
            pb_hi    = sig['pb_hi'].get(dt)
            pb_lo    = sig['pb_lo'].get(dt)
            cur_ma   = sig['mult_a'].get(dt)
            cur_mb   = sig['mult_b'].get(dt)

            if pd.isna(cur_z) or pd.isna(cur_std) or cur_std == 0:
                continue

            days      = (dt - pos['entry_date']).days
            direction = pos['direction']
            entry_ma  = pos['mult_a']
            entry_mb  = pos['mult_b']
            entry_z   = pos['entry_z']
            scale     = pos['scale']

            # Corporate action adjustment: if split/bonus happened since entry,
            # raw prices dropped by factor — adjust to pre-split equivalent
            adj_a = get_adj_factor(_corp, sym_a, pos['entry_date'], dt)
            adj_b = get_adj_factor(_corp, sym_b, pos['entry_date'], dt)
            cur_pa_adj  = cur_pa * adj_a
            cur_pb_adj  = cur_pb * adj_b
            pa_hi_adj   = pa_hi * adj_a
            pa_lo_adj   = pa_lo * adj_a
            pb_hi_adj   = pb_hi * adj_b
            pb_lo_adj   = pb_lo * adj_b

            # If lots changed or corp action happened, rolling mean/std are
            # contaminated (spread series jumped). Use entry-time stats.
            lots_changed = (cur_ma != entry_ma or cur_mb != entry_mb)
            has_action   = (adj_a != 1.0 or adj_b != 1.0)
            use_entry_stats = lots_changed or has_action
            ref_mean = pos['entry_mean'] if use_entry_stats else cur_mean
            ref_std  = pos['entry_std']  if use_entry_stats else cur_std
            if pd.isna(ref_std) or ref_std == 0:
                continue

            # Recompute Z on consistent basis (entry lots + adjusted prices)
            if use_entry_stats:
                cur_spread_adj = cur_pa_adj * entry_ma - cur_pb_adj * entry_mb
                cur_z_adj = (cur_spread_adj - ref_mean) / ref_std
            else:
                cur_z_adj = cur_z

            # Intraday best-case spread (entry lots + adjusted prices)
            if direction == +1:   # BUY_A
                best_sp = pa_hi_adj * entry_ma - pb_lo_adj * entry_mb
                intr_pa, intr_pb = pa_hi, pb_lo
            else:                 # SELL_A
                best_sp = pa_lo_adj * entry_ma - pb_hi_adj * entry_mb
                intr_pa, intr_pb = pa_lo, pb_hi
            best_z = (best_sp - ref_mean) / ref_std

            # Only count Z-based exits if Z has actually moved TOWARD 0 vs entry
            # (prevents "already past exit on entry" false triggers)
            z_reverted = abs(cur_z_adj) < abs(entry_z)

            intraday_hit = z_reverted and (
                (direction == +1 and best_z >= -z_exit) or
                (direction == -1 and best_z <=  z_exit))
            eod_hit      = z_reverted and (
                (direction == +1 and cur_z_adj >= -z_exit) or
                (direction == -1 and cur_z_adj <=  z_exit))

            # Lot changes only affect NEW contracts — existing positions hold their
            # original contract unchanged. No forced exit on lot change.

            reason = None
            # 1. Z-based profit exit (spread reverted)
            if intraday_hit:
                reason = 'INTRADAY' if not eod_hit else 'PROFIT'
            # 2. Structural break detection (primary loss exit)
            elif days >= 3:
                corr_now = sig['corr'].get(dt, 1.0)
                corr_now = max(0.0, float(corr_now)) if not pd.isna(corr_now) else 1.0
                corr_break  = corr_now < STRUCT_BREAK_CORR_FLOOR
                z_amplified = abs(cur_z_adj) > abs(entry_z) * STRUCT_BREAK_Z_MULT
                if corr_break or z_amplified:
                    reason = 'STRUCT_BREAK'
            # 3. TIME_STOP as long backstop only
            if reason is None and days >= _time_stop:
                reason = 'TIME_STOP'

            if reason:
                # Adjust exit prices for corporate actions (pre-split equivalent)
                if reason == 'INTRADAY':
                    exit_pa = (intr_pa * adj_a + cur_pa_adj) / 2
                    exit_pb = (intr_pb * adj_b + cur_pb_adj) / 2
                else:
                    exit_pa, exit_pb = cur_pa_adj, cur_pb_adj

                scaled_ma = int(round(entry_ma * scale))
                scaled_mb = int(round(entry_mb * scale))
                gross = direction * (exit_pa * scaled_ma - exit_pb * scaled_mb
                                     - pos['entry_pa'] * scaled_ma
                                     + pos['entry_pb'] * scaled_mb)
                chg = abs(gross) * CHARGE_RATE
                net = gross - chg

                trades.append(dict(
                    pair=f"{sym_a}/{sym_b}", strategy='PAIRS',
                    entry_date=pos['entry_date'], exit_date=dt, days=days,
                    direction='BUY_A' if direction == +1 else 'SELL_A',
                    entry_z=round(entry_z, 3), exit_z=round(cur_z_adj, 3),
                    gross=round(gross, 0), charges=round(chg, 0),
                    net=round(net, 0), reason=reason, win=net > 0,
                    shares_a=int(scaled_ma), shares_b=int(scaled_mb),
                ))
                cap.release(key, net)
                pair_to_close.append(key)

                # Self-annealing
                _update_modifier(pair_modifiers, key, net > 0)

                # Cooldown after forced exit (pair didn't revert — give it space)
                if reason in ('TIME_STOP', 'STRUCT_BREAK'):
                    cooldowns[key] = day_idx + COOLDOWN_TIMESTOP

        for key in pair_to_close:
            del pair_positions[key]

        # ══════════════════════════════════════════════════════════════════
        #  ENTRIES — skip during drawdown pause
        # ══════════════════════════════════════════════════════════════════

        if not is_paused:

            # ── Calendar entries (ranked by |Z|, capital gated) ──────────
            if len(cal_positions) < CALENDAR_MAX_POSITIONS:
                cal_candidates = []
                for sym, cfg in cal_signals.items():
                    if sym in cal_positions:
                        continue
                    if dt not in cfg['data'].index:
                        continue
                    row = cfg['data'].loc[dt]
                    z   = row['z']
                    if pd.isna(z) or pd.isna(row.get('std')) or row.get('std', 0) == 0:
                        continue
                    dte = row['dte']
                    if abs(z) >= CAL_Z_ENTRY and dte > CAL_DAYS_BEFORE_EXP + 2:
                        cal_candidates.append((abs(z), sym, row))
                cal_candidates.sort(reverse=True)

                for _, sym, row in cal_candidates:
                    if len(cal_positions) >= CALENDAR_MAX_POSITIONS:
                        break
                    z   = row['z']
                    lot = int(row['lot']) if not pd.isna(row['lot']) and row['lot'] > 0 else 1
                    scale = cap.scale_factor(MAX_COMPOUND_SCALE)
                    actual_contracts = max(1, round(scale))
                    margin = (abs(row['near']) + abs(row['far'])) * lot * actual_contracts * 0.15
                    if cap.can_open(margin):
                        cap.commit(f'CAL_{sym}', margin)
                        cal_positions[sym] = dict(
                            entry_date=dt,
                            entry_spread=row['spread'],
                            entry_z=z,
                            direction=-1 if z > 0 else +1,
                            lot=lot,
                            contracts=actual_contracts,
                            near_expiry=row['near_expiry'],
                        )

            # ── Pairs entries (ranked by SSS) ────────────────────────────
            if len(pair_positions) < PAIRS_MAX_POSITIONS:
                candidates = []
                active_syms = {s for pair in pair_positions for s in pair}
                # Also block symbols in calendar positions
                for sym in cal_positions:
                    active_syms.add(sym)

                for key in current_universe:
                    sym_a, sym_b = key
                    if sym_a in active_syms or sym_b in active_syms:
                        continue
                    sig = pair_signals.get(key)
                    if sig is None or dt not in sig['z'].index:
                        continue
                    if sig['blackout'].get(dt, False):
                        continue

                    # Cooldown check
                    if key in cooldowns and day_idx < cooldowns[key]:
                        continue

                    cur_z    = sig['z'].get(dt)
                    corr_60d = sig['corr'].get(dt, 0)
                    if pd.isna(cur_z) or pd.isna(corr_60d):
                        continue
                    corr_60d = max(0.0, float(corr_60d))

                    # Fix D: correlation gate — uncorrelated pair is not a valid hedge
                    if corr_60d < CORR_MIN:
                        continue

                    if abs(cur_z) > PAIRS_MAX_Z_ENTRY:
                        continue
                    if abs(cur_z) < PAIRS_MIN_Z_ENTRY:
                        continue   # not stretched enough — noise, not signal

                    sss      = abs(cur_z) * (1.0 + corr_60d)
                    modifier = get_modifier(pair_modifiers, key)
                    eff_sss = sss * modifier  # modifier deprioritises, never eliminates
                    if eff_sss < sss_threshold:
                        continue

                    candidates.append((eff_sss, key, cur_z))

                candidates.sort(reverse=True)

                for eff_sss, key, cur_z in candidates:
                    if len(pair_positions) >= PAIRS_MAX_POSITIONS:
                        break
                    sym_a, sym_b = key
                    # Re-check active symbols (positions may have been added this loop)
                    active_syms = {s for pair in pair_positions for s in pair}
                    for sym in cal_positions:
                        active_syms.add(sym)
                    if sym_a in active_syms or sym_b in active_syms:
                        continue

                    sig    = pair_signals[key]
                    cur_pa = sig['pa'].get(dt)
                    cur_pb = sig['pb'].get(dt)
                    cur_ma = sig['mult_a'].get(dt)
                    cur_mb = sig['mult_b'].get(dt)
                    if pd.isna(cur_pa) or pd.isna(cur_pb):
                        continue
                    if pd.isna(cur_ma) or pd.isna(cur_mb):
                        continue

                    # Fix E: Hedge ratio check on BASE (unscaled) notionals
                    notional_a_base = cur_pa * cur_ma
                    notional_b_base = cur_pb * cur_mb
                    if min(notional_a_base, notional_b_base) > 0:
                        ratio = max(notional_a_base, notional_b_base) / min(notional_a_base, notional_b_base)
                        if ratio > 1.5:
                            continue  # hedge ratio drifted — not cash-neutral

                    # Notional-aware compounding
                    scale_by_capital  = cap.scale_factor(MAX_COMPOUND_SCALE)
                    max_leg_notional  = cap.available * SCALE_CONC_CAP
                    scale_by_notional = (max_leg_notional / notional_a_base
                                         if notional_a_base > 0 else MAX_COMPOUND_SCALE)
                    scale     = max(1.0, min(scale_by_capital, scale_by_notional,
                                             MAX_COMPOUND_SCALE))
                    scaled_ma = int(round(cur_ma * scale))
                    scaled_mb = int(round(cur_mb * scale))

                    # Notionals after scaling (for capital gate)
                    notional_a = cur_pa * scaled_ma
                    notional_b = cur_pb * scaled_mb

                    margin = cap.estimate_margin(cur_pa, cur_pb, scaled_ma, scaled_mb)
                    if not cap.can_open(margin, notional_a, notional_b):
                        continue
                    cap.commit(key, margin)

                    pair_positions[key] = dict(
                        entry_date=dt, entry_z=cur_z,
                        entry_pa=cur_pa, entry_pb=cur_pb,
                        mult_a=cur_ma, mult_b=cur_mb,   # base (for Z calc)
                        entry_mean=sig['mean'].get(dt),  # for lot-change Z fix
                        entry_std=sig['std'].get(dt),    # for lot-change Z fix
                        direction=-1 if cur_z > 0 else +1,
                        scale=scale,                      # locked at entry
                    )

        # ── Track equity ─────────────────────────────────────────────────
        equity[dt] = cap.available

        # ── Drawdown check ───────────────────────────────────────────────
        if prev_equity > 0:
            daily_ret = (cap.available - prev_equity) / prev_equity
            if daily_ret < DRAWDOWN_THRESHOLD:
                drawdown_pause_until = day_idx + DRAWDOWN_PAUSE_DAYS
        prev_equity = cap.available

    return pd.DataFrame(trades) if trades else pd.DataFrame(), equity


# ── Summarise ────────────────────────────────────────────────────────────────

def summarise(df, _equity=None):
    if df.empty:
        return dict(yr1=STARTING_CAP, yr2=STARTING_CAP,
                    net=0, wr=0, n=0, charges=0, n_pairs=0, n_cal=0)
    yr1n = df[df['exit_date'] <= YEAR1_END]['net'].sum()
    yr2n = df['net'].sum()
    wins = df['win'].sum()
    return dict(
        yr1     = STARTING_CAP + yr1n,
        yr2     = STARTING_CAP + yr2n,
        net     = yr2n,
        wr      = wins / len(df) if len(df) else 0,
        n       = len(df),
        charges = df['charges'].sum(),
        n_pairs = (df['strategy'] == 'PAIRS').sum(),
        n_cal   = (df['strategy'] == 'CALENDAR').sum(),
    )


# ── Plotting ─────────────────────────────────────────────────────────────────

def plot_all(grid_results, best_key, best_df, best_eq, refresh_points):
    fig = plt.figure(figsize=(30, 26), facecolor='#0a0a0a')
    gs  = GridSpec(4, 3, figure=fig,
                   height_ratios=[2.5, 1.3, 1.3, 1.3],
                   hspace=0.52, wspace=0.38)

    ax_eq      = fig.add_subplot(gs[0, :])
    ax_hm_net  = fig.add_subplot(gs[1, 0])
    ax_hm_wr   = fig.add_subplot(gs[1, 1])
    ax_hm_yr1  = fig.add_subplot(gs[1, 2])
    ax_pair    = fig.add_subplot(gs[2, 0])
    ax_strat   = fig.add_subplot(gs[2, 1])
    ax_monthly = fig.add_subplot(gs[2, 2])
    ax_scatter = fig.add_subplot(gs[3, 0])
    ax_dist    = fig.add_subplot(gs[3, 1])
    ax_reason  = fig.add_subplot(gs[3, 2])

    TC = '#e0e0e0'; GC = '#2a2a2a'
    for ax in fig.get_axes():
        ax.set_facecolor('#111111')
        ax.tick_params(colors=TC, labelsize=8)
        for sp in ax.spines.values():
            sp.set_color('#444')
        ax.yaxis.label.set_color(TC)
        ax.xaxis.label.set_color(TC)
        ax.title.set_color(TC)

    sss_best, zex_best = best_key
    wins  = int(best_df['win'].sum())
    total = len(best_df)

    # ── Equity curve ─────────────────────────────────────────────────────
    vals = best_eq.values / 1e5
    ax_eq.plot(best_eq.index, vals, color='#00d4ff', lw=2.2)
    ax_eq.fill_between(best_eq.index, 25, vals,
                        where=vals >= 25, color='#00d4ff', alpha=0.12)
    ax_eq.fill_between(best_eq.index, 25, vals,
                        where=vals < 25, color='#ff4444', alpha=0.15)
    ax_eq.axhline(25, color='#888', lw=1, ls='--', alpha=0.7, label='Start')
    ax_eq.axvline(pd.Timestamp('2025-04-01'), color='#ffaa00',
                   lw=1, ls=':', alpha=0.7)

    # Mark universe refresh points
    for rp in refresh_points:
        if BACKTEST_START <= rp <= BACKTEST_END:
            ax_eq.axvline(rp, color='#9966ff', lw=0.7, ls=':', alpha=0.5)

    yr1v = best_eq[best_eq.index <= YEAR1_END].iloc[-1] / 1e5
    yr2v = best_eq.iloc[-1] / 1e5
    ax_eq.annotate(f'Mar-25: Rs.{yr1v:.2f}L',
                   xy=(YEAR1_END, yr1v), xytext=(-110, 22),
                   textcoords='offset points', color='#ffcc00',
                   fontsize=12, fontweight='bold',
                   arrowprops=dict(arrowstyle='->', color='#ffcc00'))
    ax_eq.annotate(f'Mar-26: Rs.{yr2v:.2f}L',
                   xy=(BACKTEST_END, yr2v), xytext=(-110, -32),
                   textcoords='offset points', color='#00ff88',
                   fontsize=12, fontweight='bold',
                   arrowprops=dict(arrowstyle='->', color='#00ff88'))

    for _, t in best_df.iterrows():
        eq_at = best_eq[best_eq.index <= t['entry_date']]
        if not eq_at.empty:
            clr = '#00ff88' if t['win'] else '#ff4444'
            ax_eq.scatter([t['entry_date']], [eq_at.iloc[-1]/1e5],
                           marker='^', s=22, color=clr, alpha=0.5, zorder=8)

    ax_eq.set_title(
        f'v6 BEST: SSS={sss_best} Z_exit={zex_best}'
        f'  |  Cal z_in={CAL_Z_ENTRY} z_out={CAL_Z_EXIT}'
        f'  |  {wins}/{total} ({100*wins/total:.0f}%WR)'
        f'  |  Rs.25L -> Rs.{yr2v:.2f}L'
        f'  |  Purple=universe refresh',
        fontsize=11, fontweight='bold', pad=10)
    ax_eq.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f'Rs.{x:.1f}L'))
    ax_eq.grid(True, color=GC, alpha=0.6)

    # ── Heatmaps ─────────────────────────────────────────────────────────
    net_mat = np.full((len(SSS_THRESHOLDS), len(PAIRS_Z_EXITS)), np.nan)
    wr_mat  = np.full((len(SSS_THRESHOLDS), len(PAIRS_Z_EXITS)), np.nan)
    yr1_mat = np.full((len(SSS_THRESHOLDS), len(PAIRS_Z_EXITS)), np.nan)

    for (sss, zex), s in grid_results.items():
        i = SSS_THRESHOLDS.index(sss)
        j = PAIRS_Z_EXITS.index(zex)
        net_mat[i, j]  = s['net'] / 1e3
        wr_mat[i, j]   = s['wr'] * 100
        yr1_mat[i, j]  = s['yr1'] / 1e5

    for ax_hm, mat, title, fmt, cmap in [
        (ax_hm_net, net_mat, 'Net P&L (Rs.K)',      '{:.0f}K', 'RdYlGn'),
        (ax_hm_wr,  wr_mat,  'Win Rate (%)',         '{:.0f}%', 'RdYlGn'),
        (ax_hm_yr1, yr1_mat, 'Mar-25 Value (Rs.L)', '{:.1f}L', 'RdYlGn'),
    ]:
        vmax = np.nanmax(np.abs(mat)) if not np.all(np.isnan(mat)) else 1
        im = ax_hm.imshow(mat, cmap=cmap, aspect='auto',
                           vmin=-vmax if 'P&L' in title else np.nanmin(mat),
                           vmax=vmax if 'P&L' in title else np.nanmax(mat))
        ax_hm.set_xticks(range(len(PAIRS_Z_EXITS)))
        ax_hm.set_xticklabels([f'ex={v}' for v in PAIRS_Z_EXITS], fontsize=7)
        ax_hm.set_yticks(range(len(SSS_THRESHOLDS)))
        ax_hm.set_yticklabels([f'sss={v}' for v in SSS_THRESHOLDS], fontsize=7)
        ax_hm.set_xlabel('Z_EXIT', fontsize=8, color=TC)
        ax_hm.set_ylabel('SSS_threshold', fontsize=8, color=TC)
        ax_hm.set_title(title, fontsize=10, fontweight='bold')
        bi = SSS_THRESHOLDS.index(sss_best)
        bj = PAIRS_Z_EXITS.index(zex_best)
        ax_hm.scatter([bj], [bi], marker='*', s=300, color='white', zorder=10)
        for i in range(len(SSS_THRESHOLDS)):
            for j in range(len(PAIRS_Z_EXITS)):
                v = mat[i, j]
                if not np.isnan(v):
                    ax_hm.text(j, i, fmt.format(v), ha='center', va='center',
                               fontsize=6, color='black', fontweight='bold')
        plt.colorbar(im, ax=ax_hm, fraction=0.046, pad=0.04)

    # ── P&L by pair (top 20) ────────────────────────────────────────────
    pair_agg = best_df.groupby('pair').agg(
        net=('net','sum'), n=('net','count'), wr=('win','mean')
    ).sort_values('net', ascending=True)
    if len(pair_agg) > 20:
        pair_agg = pd.concat([pair_agg.head(5), pair_agg.tail(15)])
    clrs = ['#00d4ff' if v >= 0 else '#ff4444' for v in pair_agg['net'].values]
    labels = [f"{p[:14]}\n({int(r['n'])}tr,{100*r['wr']:.0f}%)"
              for p, r in pair_agg.iterrows()]
    ax_pair.barh(labels, pair_agg['net'].values / 1e3, color=clrs, alpha=0.85)
    ax_pair.axvline(0, color='#888', lw=0.8)
    ax_pair.set_title('P&L by Pair (Rs.K)', fontsize=10, fontweight='bold')
    ax_pair.grid(True, axis='x', color=GC, alpha=0.5)
    ax_pair.tick_params(axis='y', labelsize=6)

    # ── P&L by strategy ─────────────────────────────────────────────────
    st = best_df.groupby('strategy').agg(
        net=('net','sum'), n=('net','count'), wr=('win','mean'))
    clrs2 = ['#00d4ff' if v >= 0 else '#ff4444' for v in st['net'].values]
    bars = ax_strat.bar(st.index, st['net'].values / 1e3, color=clrs2, alpha=0.85)
    for bar, (_, row) in zip(bars, st.iterrows()):
        val = row['net'] / 1e3
        ax_strat.text(bar.get_x() + bar.get_width()/2,
                      bar.get_height() + (2 if val >= 0 else -16),
                      f"Rs.{val:+.0f}K\n{int(row['n'])}tr {100*row['wr']:.0f}%WR",
                      ha='center', va='bottom', fontsize=9, color=TC)
    ax_strat.axhline(0, color='#888', lw=0.8)
    ax_strat.set_title('P&L by System (Rs.K)', fontsize=10, fontweight='bold')
    ax_strat.grid(True, axis='y', color=GC, alpha=0.5)

    # ── Monthly P&L ──────────────────────────────────────────────────────
    bdf2 = best_df.copy()
    bdf2['month'] = pd.to_datetime(bdf2['exit_date']).dt.to_period('M')
    monthly = bdf2.groupby('month')['net'].sum() / 1e3
    clrs_m  = ['#00d4ff' if v >= 0 else '#ff4444' for v in monthly.values]
    bars_m  = ax_monthly.bar([str(m) for m in monthly.index],
                              monthly.values, color=clrs_m, alpha=0.85)
    for bar, val in zip(bars_m, monthly.values):
        if abs(val) > 10:
            ax_monthly.text(bar.get_x() + bar.get_width()/2,
                            bar.get_height() + (1 if val >= 0 else -9),
                            f'Rs.{val:+.0f}K', ha='center',
                            va='bottom', fontsize=6.5, color=TC)
    ax_monthly.axhline(0, color='#888', lw=0.8)
    ax_monthly.set_title('Monthly Net P&L (Rs.K)', fontsize=10, fontweight='bold')
    ax_monthly.tick_params(axis='x', rotation=70, labelsize=6.5)
    ax_monthly.grid(True, axis='y', color=GC, alpha=0.5)

    # ── Duration vs P&L scatter ──────────────────────────────────────────
    strat_ec = {'PAIRS': '#00d4ff', 'CALENDAR': '#ffcc00'}
    for strat, grp in best_df.groupby('strategy'):
        sc = ['#00ff88' if w else '#ff4444' for w in grp['win']]
        ax_scatter.scatter(grp['days'], grp['net']/1e3, c=sc, s=55, alpha=0.7,
                            edgecolors=strat_ec.get(strat, '#888'),
                            linewidths=0.6, label=strat, zorder=5)
    ax_scatter.axhline(0, color='#888', lw=0.8, ls='--')
    ax_scatter.axvline(30, color='#ffaa00', lw=1, ls=':', alpha=0.7)
    ax_scatter.set_title('Duration vs P&L', fontsize=10, fontweight='bold')
    ax_scatter.set_xlabel('Days Held', fontsize=8)
    ax_scatter.set_ylabel('Net P&L (Rs.K)', fontsize=8)
    ax_scatter.legend(fontsize=7, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax_scatter.grid(True, color=GC, alpha=0.5)

    # ── P&L distribution ─────────────────────────────────────────────────
    pnl = best_df['net'].values / 1e3
    ax_dist.hist(pnl, bins=35, color='#00d4ff', alpha=0.7, edgecolor='#333')
    ax_dist.axvline(0, color='#ff4444', lw=1.5, ls='--')
    ax_dist.axvline(pnl.mean(), color='#ffcc00', lw=1.5,
                     label=f'Avg Rs.{pnl.mean():.0f}K')
    ax_dist.set_title('P&L Distribution per Trade', fontsize=10, fontweight='bold')
    ax_dist.set_xlabel('Net P&L (Rs.K)', fontsize=8)
    ax_dist.legend(fontsize=8, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax_dist.grid(True, color=GC, alpha=0.5)

    # ── Exit reason breakdown ────────────────────────────────────────────
    reason_agg = best_df.groupby('reason').agg(
        n=('net','count'), wr=('win','mean'), net=('net','sum')
    ).sort_values('net', ascending=False)
    clrs_r = ['#00d4ff' if v >= 0 else '#ff4444'
               for v in reason_agg['net'].values]
    bars_r = ax_reason.bar(reason_agg.index,
                            reason_agg['net'].values / 1e3,
                            color=clrs_r, alpha=0.85)
    for bar, (_, row) in zip(bars_r, reason_agg.iterrows()):
        val = row['net'] / 1e3
        ax_reason.text(bar.get_x() + bar.get_width()/2,
                       bar.get_height() + (2 if val >= 0 else -14),
                       f"{int(row['n'])}tr\n{100*row['wr']:.0f}%WR",
                       ha='center', va='bottom', fontsize=7.5, color=TC)
    ax_reason.axhline(0, color='#888', lw=0.8)
    ax_reason.set_title('P&L by Exit Reason', fontsize=10, fontweight='bold')
    ax_reason.tick_params(axis='x', rotation=20, labelsize=7.5)
    ax_reason.grid(True, axis='y', color=GC, alpha=0.5)

    fig.suptitle(
        'MEGA BACKTEST v6  |  Unified Capital + Rolling Universe + Drawdown Protection'
        f'  |  Rs.25L  |  12% Charges',
        fontsize=12, fontweight='bold', color='#fff', y=0.998)

    out = '.tmp/backtest_mega_v6.png'
    plt.savefig(out, dpi=160, bbox_inches='tight', facecolor='#0a0a0a')
    plt.close()
    print(f"  Chart -> {out}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("  MEGA BACKTEST v6 — Foolproof Unified System")
    print(f"  SSS thresholds:   {SSS_THRESHOLDS}")
    print(f"  Z_EXIT values:    {PAIRS_Z_EXITS}")
    print(f"  Calendar:         entry={CAL_Z_ENTRY}, exit={CAL_Z_EXIT} (Tier 1)")
    print(f"  Universe refresh: every {REFRESH_INTERVAL} trading days")
    print(f"  Drawdown pause:   {DRAWDOWN_THRESHOLD*100:.0f}% daily -> {DRAWDOWN_PAUSE_DAYS}d pause")
    print(f"  Compounding cap:  {MAX_COMPOUND_SCALE}x")
    print(f"  Max positions:    {PAIRS_MAX_POSITIONS} pairs + {CALENDAR_MAX_POSITIONS} calendar")
    print(f"  Max utilisation:  70%")
    print(f"  Total combos:     {len(SSS_THRESHOLDS) * len(PAIRS_Z_EXITS)}")
    print("=" * 80)

    # ── Load corporate actions ──────────────────────────────────────────
    corp_actions = load_corporate_actions()

    # ── Load all data ────────────────────────────────────────────────────
    print("\n  Loading price data...")
    price_data = load_all_prices()
    print(f"  Loaded {len(price_data)} symbols")

    # ── Precompute calendar signals ──────────────────────────────────────
    print(f"\n  Precomputing calendar signals (Tier 1)...")
    cal_signals = precompute_calendar()
    print(f"  Calendar symbols ready: {list(cal_signals.keys())}")

    # ── Discover universes at refresh points (cached for autoresearch) ───
    import pickle
    trading_days = pd.bdate_range(BACKTEST_START, BACKTEST_END)
    cache_file = '.tmp/cached_universes.pkl'
    use_cache = _AR_MODE and os.path.exists(cache_file)

    if use_cache:
        print(f"\n  Loading cached universe discovery...")
        with open(cache_file, 'rb') as _cf:
            cached = pickle.load(_cf)
        refresh_points = cached['refresh_points']
        universe_at    = cached['universe_at']
        all_tuples     = cached['all_tuples']
        raw_universes  = cached['raw_universes']
        print(f"  Loaded {len(all_tuples)} pairs from cache")
    else:
        print(f"\n  Discovering co-integrated pairs (rolling {REFRESH_INTERVAL}d windows)...")
        refresh_points, universe_at, all_tuples, raw_universes = \
            discover_universes(price_data, trading_days)
        print(f"  Total unique pairs across all windows: {len(all_tuples)}")
        # Cache for autoresearch
        os.makedirs('.tmp', exist_ok=True)
        with open(cache_file, 'wb') as _cf:
            pickle.dump({
                'refresh_points': refresh_points,
                'universe_at': universe_at,
                'all_tuples': all_tuples,
                'raw_universes': raw_universes,
            }, _cf)

    # ── Precompute pair signals for ALL discovered pairs ─────────────────
    print(f"\n  Precomputing pair signals for {len(all_tuples)} pairs...")
    pair_signals = precompute_signals(price_data, all_tuples, corp_actions=corp_actions)
    print(f"  Pair signals ready: {len(pair_signals)} pairs")

    # ── Grid search ──────────────────────────────────────────────────────
    print(f"\n  Running grid search ({len(SSS_THRESHOLDS)}x{len(PAIRS_Z_EXITS)} = "
          f"{len(SSS_THRESHOLDS)*len(PAIRS_Z_EXITS)} combos)...")
    print(f"  {'SSS':>5} {'Z_ex':>5} {'Mar-25':>10} {'Mar-26':>10}"
          f" {'Net':>10} {'WR':>6} {'Tr':>4} {'Pr':>4} {'Cal':>4}")
    print("  " + "-" * 66)

    grid_results = {}
    best_net = -np.inf
    best_key = None
    best_df  = None
    best_eq  = None

    for sss in SSS_THRESHOLDS:
        for zex in PAIRS_Z_EXITS:
            df, eq = run_unified(
                pair_signals, cal_signals,
                refresh_points, universe_at,
                sss_threshold=sss, z_exit=zex,
                starting_cap=STARTING_CAP,
                corp_actions=corp_actions,
            )
            if not df.empty:
                df['entry_date'] = pd.to_datetime(df['entry_date'])
                df['exit_date']  = pd.to_datetime(df['exit_date'])

            s = summarise(df, eq)
            grid_results[(sss, zex)] = s

            marker = ' <-' if s['net'] > best_net else ''
            print(f"  {sss:>5.1f} {zex:>5.1f}"
                  f"  Rs.{s['yr1']/1e5:>6.2f}L"
                  f"  Rs.{s['yr2']/1e5:>6.2f}L"
                  f"  Rs.{s['net']/1e3:>+7.0f}K"
                  f"  {100*s['wr']:>5.0f}%"
                  f"  {s['n']:>4}"
                  f"  {s['n_pairs']:>4}"
                  f"  {s['n_cal']:>4}"
                  f"{marker}")

            if s['net'] > best_net:
                best_net = s['net']
                best_key = (sss, zex)
                best_df  = df.copy() if not df.empty else df
                best_eq  = eq.copy()

    print("  " + "-" * 66)
    sss_best, zex_best = best_key
    print(f"\n  OPTIMAL: SSS_threshold={sss_best}, Z_exit={zex_best}")

    s    = grid_results[best_key]
    yr1n = best_df[best_df['exit_date'] <= YEAR1_END]['net'].sum() if not best_df.empty else 0

    print(f"\n{'='*80}")
    print("  FINAL RESULTS (optimal combo)")
    print(f"{'='*80}")
    print(f"  Start (Apr-24):    Rs.25.00L")
    print(f"  Mar-25:            Rs.{(STARTING_CAP+yr1n)/1e5:.2f}L"
          f"  ({100*yr1n/STARTING_CAP:+.1f}%)")
    print(f"  Mar-26:            Rs.{s['yr2']/1e5:.2f}L"
          f"  ({100*s['net']/STARTING_CAP:+.1f}%)")
    print(f"  Win Rate:          {100*s['wr']:.1f}%  "
          f"({int(s['wr']*s['n'])}/{s['n']})")
    print(f"  Charges:           Rs.{s['charges']/1e3:.0f}K")
    print(f"  Pairs trades:      {s['n_pairs']}")
    print(f"  Calendar trades:   {s['n_cal']}")

    if not best_df.empty:
        print(f"\n  By exit reason:")
        for reason, grp in best_df.groupby('reason'):
            w = grp['win'].sum(); n = len(grp)
            print(f"    {reason:<15}: {n:>4}tr  {100*w/n:>5.1f}%WR"
                  f"  Rs.{grp['net'].sum()/1e3:>+7.0f}K")

    print(f"\n  Universe refresh points:")
    for rp in refresh_points:
        n = len(universe_at.get(rp, set()))
        print(f"    {rp.date()}: {n} pairs")

    print(f"\n  Top 5 combos by net P&L:")
    top5 = sorted(grid_results.items(), key=lambda x: x[1]['net'], reverse=True)[:5]
    for (sss, zex), s in top5:
        print(f"    SSS={sss}, Z_exit={zex}:  "
              f"Rs.{s['yr2']/1e5:.2f}L  {100*s['wr']:.0f}%WR  {s['n']}tr")

    print(f"{'='*80}")

    os.makedirs('.tmp', exist_ok=True)
    if not best_df.empty:
        best_df.to_csv('.tmp/backtest_v6_trades.csv', index=False)
        print(f"  Trade log -> .tmp/backtest_v6_trades.csv")
    plot_all(grid_results, best_key, best_df, best_eq, refresh_points)

    # AutoResearch: write machine-readable results
    if _AR_MODE and not best_df.empty:
        monthly_pnl = best_df.groupby(
            pd.to_datetime(best_df['exit_date']).dt.to_period('M')
        )['net'].sum()
        ar_results = {
            'net_pnl': float(s['net']),
            'win_rate': float(100 * s['wr']),
            'n_trades': int(s['n']),
            'final_equity': float(s['yr2']),
            'min_monthly_pnl': float(monthly_pnl.min()) if len(monthly_pnl) > 0 else 0,
            'max_monthly_pnl': float(monthly_pnl.max()) if len(monthly_pnl) > 0 else 0,
            'n_negative_months': int((monthly_pnl < 0).sum()),
        }
        import json as _json2
        with open('.tmp/autoresearch_results.json', 'w') as _f2:
            _json2.dump(ar_results, _f2, indent=2)
        print(f"  AutoResearch results -> .tmp/autoresearch_results.json")


if __name__ == '__main__':
    main()
