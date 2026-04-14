"""
System 2: Calendar Spread Engine (Backwardation / Contango extremes)
=====================================================================
Scans all symbols with multi-expiry NSE bhav data.

The spread we trade: near_price - far_price
  Normal (contango): far > near → spread < 0
  Backwardation:     near > far → spread > 0

The spread IS mean-reverting (it oscillates around a stable mean,
confirmed empirically). It does NOT converge to zero — we are NOT
betting on settlement convergence.

What we're betting on: the spread returns to its rolling mean.

Entry:
  Compute rolling 40d mean and std of (near - far).
  |Z| >= 2.0 → enter
    Z > +2 (backwardation extreme) → SELL near, BUY far
    Z < -2 (contango too wide)     → BUY near, SELL far

Exit (whichever comes first):
  1. Z returns to mean (|Z| <= 0.3) — detected intraday via H/L
  2. 5 days before near expiry (mandatory — avoids delivery/liquidity risk)
  3. Time stop: 15 days (calendar spreads are short-hold by nature)

Intraday exit:
  For SELL_near / BUY_far (Z was positive):
    Best intraday spread = near_low - far_high (most compressed version of spread)
    If best_z <= +0.3 → exit intraday
  For BUY_near / SELL_far (Z was negative):
    Best intraday spread = near_high - far_low (most expanded)
    If best_z >= -0.3 → exit intraday

P&L per trade:
  gross = direction * (exit_spread - entry_spread) * lot * contract_lots
  charges = 12% of |gross|
"""

import pandas as pd
import numpy as np
import os

DATA_DIR        = '.tmp/5y_data'
WINDOW          = 40
Z_ENTRY         = 2.0     # Lower than pairs — calendar signals are cleaner
Z_EXIT          = 0.3
DAYS_BEFORE_EXP = 5       # Exit this many days before near expiry
TIME_STOP_DAYS  = 15      # Calendar trades should not need more than 15 days

SYMBOLS = [
    # Tier 1 validated symbols (directives/SYSTEM.md — WR ≥ 75%, n ≥ 8)
    # (symbol, lot_size, contracts_to_trade)
    # Lot sizes read dynamically from FH_MARKET_LOT in the data;
    # these are fallbacks used only when the data has 0/NaN lots.
    ('SAIL',       4700, 1),   # 83% WR — metals, stable carry structure
    ('HINDUNILVR',  300, 1),   # 82% WR — FMCG, tight near-far spread
    ('COLPAL',      225, 1),   # 78% WR — FMCG
    ('ITC',        1600, 1),   # 77% WR — FMCG
    ('OBEROIRLTY',  350, 1),   # 75% WR — infra/realty
]


def load_calendar_data(symbol):
    """
    Load multi-expiry data and build a daily near/far time series.
    Returns DataFrame with columns: near, far, near_hi, near_lo, far_hi, far_lo,
    near_expiry, dte, spread, lot.
    Only works for NSE bhav data (has multiple rows per date for different expiries).
    """
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    # Reject yfinance files — they have one row per date, no multi-expiry
    if 'SOURCE' in df.columns and 'YFINANCE' in df['SOURCE'].dropna().values:
        return None
    if 'FH_EXPIRY_DT' not in df.columns:
        return None

    df['date']    = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['expiry']  = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    df['close']   = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['high']    = pd.to_numeric(df['FH_TRADE_HIGH_PRICE'], errors='coerce')
    df['low']     = pd.to_numeric(df['FH_TRADE_LOW_PRICE'], errors='coerce')
    df['lot']     = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce')

    if 'FH_INSTRUMENT' in df.columns:
        df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
    df = df.dropna(subset=['date', 'expiry', 'close'])
    df = df.sort_values(['date', 'expiry'])

    rows = []
    for dt, grp in df.groupby('date'):
        future_expiries = grp[grp['expiry'] > dt].sort_values('expiry')
        if len(future_expiries) < 2:
            continue
        near_row = future_expiries.iloc[0]
        far_row  = future_expiries.iloc[1]
        dte = (near_row['expiry'] - dt).days

        rows.append({
            'date':       dt,
            'near':       near_row['close'],
            'far':        far_row['close'],
            'near_hi':    near_row['high'],
            'near_lo':    near_row['low'],
            'far_hi':     far_row['high'],
            'far_lo':     far_row['low'],
            'near_expiry':near_row['expiry'],
            'dte':        dte,
            'lot':        int(near_row['lot'] or 1),
        })

    if not rows:
        return None
    out = pd.DataFrame(rows).set_index('date')
    out['spread'] = out['near'] - out['far']   # positive = backwardation
    return out


def build_signals(cal_df):
    """Add rolling Z-score to calendar data."""
    cal_df = cal_df.copy()
    cal_df['mean'] = cal_df['spread'].rolling(WINDOW).mean()
    cal_df['std']  = cal_df['spread'].rolling(WINDOW).std().replace(0, np.nan)
    cal_df['z']    = (cal_df['spread'] - cal_df['mean']) / cal_df['std']
    return cal_df


def run(start, end, z_exit=Z_EXIT, z_entry=Z_ENTRY):
    """
    Run the calendar system across all symbols.
    z_entry: minimum |Z| to open a position
    z_exit:  |Z| threshold at which we exit (profit target)
    Returns consolidated trade DataFrame.
    """
    all_trades = []

    for symbol, lot_size, contracts in SYMBOLS:
        raw = load_calendar_data(symbol)
        if raw is None:
            continue

        data = build_signals(raw)
        data = data[(data.index >= start) & (data.index <= end)]

        if data.empty or data['z'].notna().sum() < 5:
            continue

        # Simulate trades
        in_trade     = False
        entry_date   = entry_spread = entry_z = direction = None
        entry_near_exp = None

        for dt, row in data.iterrows():
            cur_z   = row['z']
            cur_sp  = row['spread']
            cur_mean= row['mean']
            cur_std = row['std']
            lot     = row['lot']
            dte     = row['dte']

            if pd.isna(cur_z) or pd.isna(cur_std):
                continue

            if in_trade:
                days = (dt - entry_date).days

                # Force exit before near expiry
                must_exit = (dte <= DAYS_BEFORE_EXP) or (days >= TIME_STOP_DAYS)

                # Intraday: best-case Z in direction of our profit
                # SELL_NEAR (direction=-1, spread was too high, we profit as it falls):
                #   best case = near_lo - far_hi (most compressed spread intraday)
                # BUY_NEAR (direction=+1, spread was too low/contango wide):
                #   best case = near_hi - far_lo (most expanded spread intraday)
                if direction == -1:  # SELL near, BUY far
                    best_sp = row['near_lo'] - row['far_hi']
                else:                # BUY near, SELL far
                    best_sp = row['near_hi'] - row['far_lo']

                best_z = (best_sp - cur_mean) / cur_std if cur_std else cur_z

                intraday_hit = (direction == -1 and best_z <= z_exit) or \
                               (direction == +1 and best_z >= -z_exit)
                eod_hit      = (direction == -1 and cur_z  <= z_exit) or \
                               (direction == +1 and cur_z  >= -z_exit)

                reason = None
                if must_exit:
                    reason     = 'EXPIRY_ROLL' if dte <= DAYS_BEFORE_EXP else 'TIME_STOP'
                    exit_spread = cur_sp
                elif intraday_hit and not eod_hit:
                    reason     = 'INTRADAY'
                    # Mid-point between best H/L spread (trigger) and EOD close
                    # (realistic exit price — not the fantasy of hitting the H/L exactly)
                    exit_spread = (best_sp + cur_sp) / 2
                elif eod_hit:
                    reason     = 'PROFIT'
                    exit_spread = cur_sp

                if reason:
                    # SELL_NEAR (dir=-1): profit = entry_spread - exit_spread
                    #   = -1 * (exit_spread - entry_spread) = direction * (exit_spread - entry_spread)
                    # BUY_NEAR  (dir=+1): profit = exit_spread - entry_spread
                    #   = +1 * (exit_spread - entry_spread) = direction * (exit_spread - entry_spread)
                    gross = direction * (exit_spread - entry_spread) * lot * contracts
                    chg   = abs(gross) * 0.12
                    net   = gross - chg
                    all_trades.append(dict(
                        pair=f'{symbol}_CAL',
                        strategy='CALENDAR',
                        entry_date=entry_date,
                        exit_date=dt,
                        days=days,
                        direction='SELL_NEAR' if direction == -1 else 'BUY_NEAR',
                        entry_z=round(entry_z, 3),
                        exit_z=round(cur_z, 3),
                        entry_spread=round(entry_spread, 2),
                        exit_spread=round(exit_spread, 2),
                        gross=round(gross, 0),
                        charges=round(chg, 0),
                        net=round(net, 0),
                        reason=reason,
                        win=net > 0,
                        shares_a=lot * contracts,
                        shares_b=lot * contracts,
                    ))
                    in_trade = False

            else:
                # Entry: |Z| >= Z_ENTRY, not too close to near expiry
                if abs(cur_z) >= z_entry and dte > DAYS_BEFORE_EXP + 2:
                    in_trade       = True
                    entry_date     = dt
                    entry_z        = cur_z
                    entry_spread   = cur_sp
                    entry_near_exp = row['near_expiry']
                    # Z > 0 → spread too wide (backwardation) → sell near, buy far
                    direction = -1 if cur_z > 0 else +1

    return pd.DataFrame(all_trades) if all_trades else pd.DataFrame()
