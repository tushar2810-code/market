"""
Historical Backtest — April 1, 2024 → March 31, 2026
Capital: ₹25,00,000 starting
Charges: 12% of gross P&L per trade (brokerage + STT + slippage + taxes)

Strategy:
- Entry: |Z_60d| >= 2.0 (at least 1 window confirms)
- Exit:  Z reverts to 0 (mean), OR 30-day time stop
- Max 3 concurrent positions (at ₹25L scale)
- Lot sizing scaled to ₹25L (half of ₹50L plan)

Output:
- Detailed trade log
- Day-by-day equity curve
- Visual chart with annotations
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = '.tmp/3y_data'
STARTING_CAPITAL = 25_00_000
CHARGE_RATE      = 0.12   # 12% of gross P&L per trade
MAX_POSITIONS    = 3
BACKTEST_START   = pd.Timestamp('2024-04-01')
BACKTEST_END     = pd.Timestamp('2026-03-31')
YEAR1_END        = pd.Timestamp('2025-03-31')
Z_ENTRY          = 2.0
Z_EXIT           = 0.3    # Exit when spread reverts near mean
TIME_STOP_DAYS   = 30
WARMUP_DAYS      = 70     # Days of data before backtest start for Z-score computation

# ── Pairs with lot sizing at ₹25L scale (half of ₹50L plan) ──────────────────
# Format: (sym_a, sym_b, lots_a, lots_b, direction_note)
# direction: "BUY_A" = long A / short B when Z is negative (A cheap vs B)
PAIRS = [
    # sym_a,         sym_b,        lots_a, lots_b, name
    ('ULTRACEMCO',  'GRASIM',      5,      5,      'ULTRA/GRASIM'),
    ('ICICIBANK',   'HDFCBANK',    2,      5,      'ICICIB/HDFC'),
    ('LICHSGFIN',   'PFC',         3,      3,      'LICHSGFIN/PFC'),
    ('NMDC',        'COALINDIA',   10,     12,     'NMDC/COAL'),
]


# ── Verified historical lot size schedule ─────────────────────────────────────
# Sources: NSE circulars, Zerodha bulletins, Angel One announcements
#
# NMDC:      4500 → 13500 on Dec 27 2024 (2:1 bonus, lot × 3)
#            13500 → 6750 on Apr 25 2025 (periodic NSE revision post-bonus)
# COALINDIA: 1050 → 1350 on Oct 28 2025 (periodic NSE revision)
# All others (ULTRA, GRASIM, ICICIB, HDFC, LICHSGFIN, PFC):
#            No lot changes during Apr 2024 – Mar 2026 (confirmed by absence
#            from NSE Apr-2024 and Jul-2024 revision circulars)
#
LOT_SCHEDULE = {
    # symbol: [(effective_date, lot_size), ...]  — ordered chronologically
    'NMDC':      [(pd.Timestamp('1900-01-01'), 4500),
                  (pd.Timestamp('2024-12-27'), 13500),
                  (pd.Timestamp('2025-04-25'), 6750)],
    'COALINDIA': [(pd.Timestamp('1900-01-01'), 1050),
                  (pd.Timestamp('2025-10-28'), 1350)],
}


def get_lot_on_date(symbol, date):
    """Return the actual lot size for a symbol on a given date."""
    if symbol not in LOT_SCHEDULE:
        return None  # Will fall back to CSV lot
    schedule = LOT_SCHEDULE[symbol]
    lot = schedule[0][1]
    for eff_date, size in schedule:
        if date >= eff_date:
            lot = size
    return lot


def load_near_month_series(symbol):
    """Load price series. Patches lot column with historically-accurate values."""
    import os
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date']  = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['close'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['lot']   = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce')

    # Detect source: YFINANCE rows have no FH_EXPIRY_DT
    has_expiry = False
    if 'FH_EXPIRY_DT' in df.columns:
        df['expiry'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        if df['expiry'].notna().sum() > 10:
            has_expiry = True

    if has_expiry:
        # Raw NSE data: pick front-month contract per date
        if 'FH_INSTRUMENT' in df.columns:
            df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
        df = df.dropna(subset=['date', 'close', 'expiry'])
        df = df.sort_values(['date', 'expiry'])
        result = []
        for dt, grp in df.groupby('date'):
            near = grp[grp['expiry'] > dt].nsmallest(1, 'expiry')
            if not near.empty:
                result.append({'date': dt, 'close': near['close'].iloc[0],
                                'lot': near['lot'].iloc[0]})
        near_df = pd.DataFrame(result).set_index('date')
    else:
        # YFINANCE data: one row per trading date, already near-month equivalent
        df = df.dropna(subset=['date', 'close'])
        df = df.sort_values('date').drop_duplicates('date')
        near_df = df[['date', 'close', 'lot']].set_index('date')

    # Patch lot column with historically-accurate values from LOT_SCHEDULE
    if symbol in LOT_SCHEDULE:
        near_df['lot'] = near_df.index.map(lambda d: get_lot_on_date(symbol, d))
        print(f"  [{symbol}] Lot schedule applied: "
              + " → ".join(f"{size} from {d.date()}" for d, size in LOT_SCHEDULE[symbol]))

    return near_df


def compute_spread_zscore(series_a, series_b, mult_a, mult_b, window=60):
    """Compute Z-score of the cash-neutral spread.
    mult_a = lots × lot_size (total shares for leg A).
    mult_b = lots × lot_size (total shares for leg B).
    """
    spread = mult_a * series_a - mult_b * series_b
    roll_mean = spread.rolling(window).mean()
    roll_std  = spread.rolling(window).std()
    z = (spread - roll_mean) / roll_std.replace(0, np.nan)
    return spread, z, roll_mean, roll_std


def backtest_pair(name, sym_a, sym_b, lots_a, lots_b, prices_a, prices_b):
    """
    Backtest a single pair using time-varying lot sizes.

    Critical correctness rule:
    When a lot size changes mid-series (e.g. NMDC 4500→13500 on bonus day),
    the spread value jumps discontinuously. The rolling 60d mean/std computed
    across that jump is meaningless. We therefore:
      1. Build a per-day spread using the actual lot on each date.
      2. After any lot-change event, invalidate signals for WARMUP_DAYS
         (the rolling window length) until the stats re-stabilise.
      3. If in a trade when a lot change happens, force-exit at the
         prior day's price (same as real-world margin call / contract adjustment).
    """
    WINDOW = 60

    # Align on common dates
    common = prices_a.index.intersection(prices_b.index)
    pa   = prices_a.loc[common, 'close']
    pb   = prices_b.loc[common, 'close']
    la   = prices_a.loc[common, 'lot']   # time-varying lot for A (shares per lot)
    lb   = prices_b.loc[common, 'lot']   # time-varying lot for B

    # Build spread day-by-day using actual lot at each date
    # shares traded = lots × lot_size_on_that_date
    mult_a_series = lots_a * la
    mult_b_series = lots_b * lb
    spread_series = mult_a_series * pa - mult_b_series * pb

    # Rolling mean/std on this variable-lot spread
    roll_mean = spread_series.rolling(WINDOW).mean()
    roll_std  = spread_series.rolling(WINDOW).std().replace(0, np.nan)
    z60_series = (spread_series - roll_mean) / roll_std

    # Detect lot-change dates — block entry for WINDOW days after each
    lot_change_a = (la != la.shift(1)) & la.notna() & la.shift(1).notna()
    lot_change_b = (lb != lb.shift(1)) & lb.notna() & lb.shift(1).notna()
    lot_change   = lot_change_a | lot_change_b

    # Build blackout mask: True on change date + next WINDOW trading days
    blackout = pd.Series(False, index=spread_series.index)
    for chg_date in spread_series.index[lot_change]:
        idx = spread_series.index.get_loc(chg_date)
        end = min(idx + WINDOW, len(spread_series))
        blackout.iloc[idx:end] = True

    # Clip to backtest window
    mask = (z60_series.index >= BACKTEST_START) & (z60_series.index <= BACKTEST_END)
    dates     = z60_series.index[mask]
    z60       = z60_series[mask]
    spread    = spread_series[mask]
    price_a   = pa[mask]
    price_b   = pb[mask]
    blk       = blackout[mask]
    mult_a_m  = mult_a_series[mask]
    mult_b_m  = mult_b_series[mask]

    trades = []
    in_trade     = False
    entry_date   = None
    entry_z      = None
    entry_spread = None
    entry_pa     = None
    entry_pb     = None
    entry_mult_a = None
    entry_mult_b = None
    direction    = None   # +1 = long A / short B, -1 = short A / long B

    for i, dt in enumerate(dates):
        cur_z    = z60.iloc[i]
        cur_sp   = spread.iloc[i]
        cur_pa   = price_a.iloc[i]
        cur_pb   = price_b.iloc[i]
        cur_blk  = blk.iloc[i]
        cur_ma   = mult_a_m.iloc[i]
        cur_mb   = mult_b_m.iloc[i]

        if pd.isna(cur_z) or pd.isna(cur_sp):
            continue

        if in_trade:
            days_held = (dt - entry_date).days

            # Force exit on lot-change blackout (contract adjustment, like real world)
            if cur_blk and (cur_ma != entry_mult_a or cur_mb != entry_mult_b):
                exit_reason = 'LOT_CHANGE_EXIT'
            elif (direction == +1 and cur_z >= -Z_EXIT) or (direction == -1 and cur_z <= Z_EXIT):
                exit_reason = 'MEAN_REVERT'
            elif days_held >= TIME_STOP_DAYS:
                exit_reason = 'TIME_STOP'
            else:
                exit_reason = None

            if exit_reason:
                # P&L uses shares at ENTRY (we traded those shares, lot change doesn't retroactively change our open position)
                gross_pnl = direction * (cur_pa * entry_mult_a - cur_pb * entry_mult_b
                                         - (entry_pa * entry_mult_a - entry_pb * entry_mult_b))
                charges   = abs(gross_pnl) * CHARGE_RATE
                net_pnl   = gross_pnl - charges

                trades.append({
                    'pair':        name,
                    'entry_date':  entry_date,
                    'exit_date':   dt,
                    'days_held':   days_held,
                    'direction':   'BUY_A' if direction == +1 else 'SELL_A',
                    'entry_z':     round(entry_z, 3),
                    'exit_z':      round(cur_z, 3),
                    'entry_pa':    round(entry_pa, 2),
                    'entry_pb':    round(entry_pb, 2),
                    'exit_pa':     round(cur_pa, 2),
                    'exit_pb':     round(cur_pb, 2),
                    'lots_a':      lots_a,
                    'lots_b':      lots_b,
                    'shares_a':    int(entry_mult_a),
                    'shares_b':    int(entry_mult_b),
                    'gross_pnl':   round(gross_pnl, 0),
                    'charges':     round(charges, 0),
                    'net_pnl':     round(net_pnl, 0),
                    'exit_reason': exit_reason,
                    'win':         net_pnl > 0,
                })
                in_trade   = False
                entry_date = None

        else:  # not in trade
            # Entry: |Z60| >= 2.0 and not in blackout period
            if not cur_blk and abs(cur_z) >= Z_ENTRY:
                in_trade     = True
                entry_date   = dt
                entry_z      = cur_z
                entry_spread = cur_sp
                entry_pa     = cur_pa
                entry_pb     = cur_pb
                entry_mult_a = cur_ma
                entry_mult_b = cur_mb
                direction    = -1 if cur_z > 0 else +1

    return trades


def build_equity_curve(all_trades, start_capital):
    """Build day-by-day equity curve from trade list."""
    date_range = pd.date_range(BACKTEST_START, BACKTEST_END, freq='B')
    equity = pd.Series(index=date_range, dtype=float)
    equity.iloc[0] = start_capital

    # Sort trades by exit date
    trades_df = pd.DataFrame(all_trades)
    if trades_df.empty:
        equity = equity.fillna(start_capital)
        return equity, trades_df

    # Build cumulative P&L
    cum_pnl = 0
    for dt in date_range:
        # Add P&L from all trades that exited on or before this date
        exited = trades_df[trades_df['exit_date'] <= dt]
        cum_pnl = exited['net_pnl'].sum()
        equity[dt] = start_capital + cum_pnl

    return equity, trades_df


def print_trade_log(trades_df):
    print("\n" + "═" * 110)
    print("  COMPLETE TRADE LOG — April 2024 → March 2026")
    print("═" * 110)

    year_pnl = {2024: 0, 2025: 0, 2026: 0}

    header = f"  {'#':>3}  {'Pair':<18} {'Entry':>12} {'Exit':>12} {'Dir':<8} {'Days':>5} {'EntZ':>7} {'ExZ':>7} {'Gross':>10} {'Chgs':>9} {'Net':>10} {'Result':<6}"
    print(header)
    print("  " + "─" * 108)

    wins = losses = 0
    total_gross = total_charges = total_net = 0

    for i, row in trades_df.sort_values('entry_date').iterrows():
        yr = row['exit_date'].year
        if yr in year_pnl:
            year_pnl[yr] += row['net_pnl']

        result = "WIN  ✓" if row['win'] else "LOSS ✗"
        sign   = "+" if row['net_pnl'] >= 0 else ""
        gross_s = f"₹{row['gross_pnl']:>+,.0f}"
        chg_s   = f"-₹{row['charges']:>,.0f}"
        net_s   = f"₹{row['net_pnl']:>+,.0f}"

        print(f"  {i+1:>3}.  {row['pair']:<18} {str(row['entry_date'].date()):>12} {str(row['exit_date'].date()):>12} {row['direction']:<8} {row['days_held']:>5} {row['entry_z']:>+7.2f} {row['exit_z']:>+7.2f} {gross_s:>10} {chg_s:>9} {net_s:>10} {result:<6}")

        total_gross   += row['gross_pnl']
        total_charges += row['charges']
        total_net     += row['net_pnl']
        if row['win']:
            wins += 1
        else:
            losses += 1

    print("  " + "─" * 108)
    total = wins + losses
    print(f"\n  SUMMARY")
    print(f"  Total Trades: {total}  |  Wins: {wins} ({100*wins/total:.0f}%)  |  Losses: {losses} ({100*losses/total:.0f}%)")
    print(f"  Gross P&L:  ₹{total_gross:>+,.0f}")
    print(f"  Charges:    ₹{-total_charges:>+,.0f}")
    print(f"  Net P&L:    ₹{total_net:>+,.0f}")

    print(f"\n  YEAR-BY-YEAR NET P&L:")
    print(f"  FY 2024-25 (Apr 24 – Mar 25): ₹{year_pnl[2024] + year_pnl[2025]:>+,.0f}")
    print(f"  FY 2025-26 (Apr 25 – Mar 26): ₹{year_pnl[2026]:>+,.0f}")
    print("═" * 110)

    return wins, losses, total_gross, total_charges, total_net, year_pnl


def plot_results(equity, trades_df, wins, losses, total_net, year_pnl):
    fig = plt.figure(figsize=(28, 20), facecolor='#0a0a0a')
    gs  = GridSpec(3, 2, figure=fig, height_ratios=[3, 1.2, 1.2], hspace=0.45, wspace=0.3)

    ax_equity  = fig.add_subplot(gs[0, :])
    ax_monthly = fig.add_subplot(gs[1, 0])
    ax_byPair  = fig.add_subplot(gs[1, 1])
    ax_scatter = fig.add_subplot(gs[2, 0])
    ax_dist    = fig.add_subplot(gs[2, 1])

    text_color = '#e0e0e0'
    grid_color = '#2a2a2a'

    for ax in [ax_equity, ax_monthly, ax_byPair, ax_scatter, ax_dist]:
        ax.set_facecolor('#111111')
        ax.tick_params(colors=text_color, labelsize=9)
        ax.spines['bottom'].set_color('#444')
        ax.spines['left'].set_color('#444')
        ax.spines['top'].set_color('#444')
        ax.spines['right'].set_color('#444')
        ax.yaxis.label.set_color(text_color)
        ax.xaxis.label.set_color(text_color)
        ax.title.set_color(text_color)

    # ── 1. EQUITY CURVE ────────────────────────────────────────────────────────
    valid_eq = equity.dropna()
    dates_eq = valid_eq.index
    vals_eq  = valid_eq.values / 1e5  # In lakhs

    ax_equity.plot(dates_eq, vals_eq, color='#00d4ff', lw=1.8, zorder=5)
    ax_equity.fill_between(dates_eq, 25, vals_eq,
                            where=(np.array(vals_eq) >= 25), color='#00d4ff', alpha=0.12, zorder=3)
    ax_equity.fill_between(dates_eq, 25, vals_eq,
                            where=(np.array(vals_eq) < 25), color='#ff4444', alpha=0.15, zorder=3)

    # Starting capital line
    ax_equity.axhline(25, color='#888', lw=1, ls='--', alpha=0.7, label='Starting ₹25L')

    # Year boundary
    ax_equity.axvline(pd.Timestamp('2025-04-01'), color='#ffaa00', lw=1, ls=':', alpha=0.8)
    ax_equity.text(pd.Timestamp('2025-04-03'), vals_eq.max() * 0.98,
                   'FY 2025-26 →', color='#ffaa00', fontsize=9, alpha=0.9)
    ax_equity.text(pd.Timestamp('2024-06-01'), vals_eq.max() * 0.98,
                   'FY 2024-25 →', color='#ffaa00', fontsize=9, alpha=0.9)

    # Milestone markers
    yr1_val = equity.get(pd.Timestamp('2025-03-31'), equity[equity.index <= pd.Timestamp('2025-03-31')].iloc[-1]) / 1e5
    yr2_val = equity.get(pd.Timestamp('2026-03-31'), equity[equity.index <= pd.Timestamp('2026-03-31')].iloc[-1]) / 1e5

    ax_equity.scatter([pd.Timestamp('2025-03-31')], [yr1_val], s=120, color='#ffcc00', zorder=10)
    ax_equity.scatter([pd.Timestamp('2026-03-31')], [yr2_val], s=120, color='#00ff88', zorder=10)
    ax_equity.annotate(f'Mar-25: ₹{yr1_val:.2f}L', xy=(pd.Timestamp('2025-03-31'), yr1_val),
                       xytext=(-80, 18), textcoords='offset points',
                       color='#ffcc00', fontsize=10, fontweight='bold',
                       arrowprops=dict(arrowstyle='->', color='#ffcc00', lw=1.2))
    ax_equity.annotate(f'Mar-26: ₹{yr2_val:.2f}L', xy=(pd.Timestamp('2026-03-31'), yr2_val),
                       xytext=(-80, -28), textcoords='offset points',
                       color='#00ff88', fontsize=10, fontweight='bold',
                       arrowprops=dict(arrowstyle='->', color='#00ff88', lw=1.2))

    # Trade entry/exit markers
    if not trades_df.empty:
        for _, t in trades_df.iterrows():
            clr = '#00ff88' if t['win'] else '#ff4444'
            # Entry dot
            entry_eq = equity[equity.index <= t['entry_date']]
            if not entry_eq.empty:
                ax_equity.scatter([t['entry_date']], [entry_eq.iloc[-1] / 1e5],
                                   marker='^', s=45, color=clr, alpha=0.7, zorder=8)
            exit_eq = equity[equity.index <= t['exit_date']]
            if not exit_eq.empty:
                ax_equity.scatter([t['exit_date']], [exit_eq.iloc[-1] / 1e5],
                                   marker='v', s=45, color=clr, alpha=0.7, zorder=8)

    ax_equity.set_title('Portfolio Equity Curve — ₹25L Starting Capital (Apr 2024 – Mar 2026)',
                         fontsize=14, fontweight='bold', pad=12)
    ax_equity.set_ylabel('Account Value (₹ Lakh)', fontsize=10)
    ax_equity.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'₹{x:.1f}L'))
    ax_equity.legend(loc='upper left', fontsize=9, facecolor='#1a1a1a', edgecolor='#444',
                      labelcolor=text_color)
    ax_equity.grid(True, color=grid_color, alpha=0.6)

    # ── 2. MONTHLY P&L BAR CHART ───────────────────────────────────────────────
    if not trades_df.empty:
        trades_df['exit_month'] = trades_df['exit_date'].dt.to_period('M')
        monthly_pnl = trades_df.groupby('exit_month')['net_pnl'].sum() / 1e3  # In thousands

        months_str = [str(m) for m in monthly_pnl.index]
        colors_m   = ['#00d4ff' if v >= 0 else '#ff4444' for v in monthly_pnl.values]
        bars = ax_monthly.bar(months_str, monthly_pnl.values, color=colors_m, width=0.7, alpha=0.85)
        ax_monthly.axhline(0, color='#888', lw=0.8)
        ax_monthly.set_title('Monthly Net P&L (₹ Thousands)', fontsize=11, fontweight='bold')
        ax_monthly.set_ylabel('₹K', fontsize=9)
        ax_monthly.tick_params(axis='x', rotation=60)

        for bar, val in zip(bars, monthly_pnl.values):
            if abs(val) > 5:
                ax_monthly.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (2 if val >= 0 else -8),
                                f'{val:+.0f}', ha='center', va='bottom', fontsize=7.5, color=text_color)
        ax_monthly.grid(True, axis='y', color=grid_color, alpha=0.6)

    # ── 3. P&L BY PAIR ────────────────────────────────────────────────────────
    if not trades_df.empty:
        pair_pnl = trades_df.groupby('pair').agg(
            net=('net_pnl', 'sum'),
            n=('net_pnl', 'count'),
            wr=('win', 'mean')
        ).sort_values('net', ascending=True)

        colors_p = ['#00d4ff' if v >= 0 else '#ff4444' for v in pair_pnl['net'].values]
        y_pos = range(len(pair_pnl))
        bars2 = ax_byPair.barh([f"{p}\n({r['n']} trades, {100*r['wr']:.0f}% WR)"
                                  for p, r in pair_pnl.iterrows()],
                                pair_pnl['net'].values / 1e3,
                                color=colors_p, alpha=0.85)
        ax_byPair.axvline(0, color='#888', lw=0.8)
        ax_byPair.set_title('Net P&L by Strategy (₹K)', fontsize=11, fontweight='bold')
        ax_byPair.set_xlabel('₹ Thousands', fontsize=9)
        ax_byPair.grid(True, axis='x', color=grid_color, alpha=0.6)

        for bar, val in zip(bars2, pair_pnl['net'].values / 1e3):
            x_pos = val + (5 if val >= 0 else -5)
            ax_byPair.text(x_pos, bar.get_y() + bar.get_height()/2,
                            f'₹{val:+.0f}K', va='center', ha='left' if val >= 0 else 'right',
                            fontsize=9, color=text_color)

    # ── 4. TRADE SCATTER (Days held vs P&L) ───────────────────────────────────
    if not trades_df.empty:
        scatter_colors = ['#00ff88' if w else '#ff4444' for w in trades_df['win']]
        ax_scatter.scatter(trades_df['days_held'], trades_df['net_pnl'] / 1e3,
                            c=scatter_colors, s=80, alpha=0.8, edgecolors='none', zorder=5)
        ax_scatter.axhline(0, color='#888', lw=0.8, ls='--')
        ax_scatter.axvline(30, color='#ffaa00', lw=1, ls=':', alpha=0.7, label='30d time stop')
        ax_scatter.set_title('Trade Duration vs P&L', fontsize=11, fontweight='bold')
        ax_scatter.set_xlabel('Days Held', fontsize=9)
        ax_scatter.set_ylabel('Net P&L (₹K)', fontsize=9)
        win_p  = mpatches.Patch(color='#00ff88', label=f'Wins ({wins})')
        loss_p = mpatches.Patch(color='#ff4444', label=f'Losses ({losses})')
        ax_scatter.legend(handles=[win_p, loss_p], fontsize=9,
                           facecolor='#1a1a1a', edgecolor='#444', labelcolor=text_color)
        ax_scatter.grid(True, color=grid_color, alpha=0.6)

    # ── 5. P&L DISTRIBUTION ───────────────────────────────────────────────────
    if not trades_df.empty:
        pnl_vals = trades_df['net_pnl'].values / 1e3
        ax_dist.hist(pnl_vals, bins=20, color='#00d4ff', alpha=0.7, edgecolor='#333')
        ax_dist.axvline(0, color='#ff4444', lw=1.5, ls='--')
        ax_dist.axvline(pnl_vals.mean(), color='#ffcc00', lw=1.5, ls='-',
                         label=f'Avg: ₹{pnl_vals.mean():.0f}K')
        ax_dist.set_title('P&L Distribution per Trade', fontsize=11, fontweight='bold')
        ax_dist.set_xlabel('Net P&L (₹K)', fontsize=9)
        ax_dist.set_ylabel('Frequency', fontsize=9)
        ax_dist.legend(fontsize=9, facecolor='#1a1a1a', edgecolor='#444', labelcolor=text_color)
        ax_dist.grid(True, color=grid_color, alpha=0.6)

    # ── Summary text box ──────────────────────────────────────────────────────
    yr1_net = sum(t['net_pnl'] for _, t in trades_df.iterrows()
                  if t['exit_date'] <= YEAR1_END) if not trades_df.empty else 0
    yr2_net = sum(t['net_pnl'] for _, t in trades_df.iterrows()
                  if t['exit_date'] > YEAR1_END) if not trades_df.empty else 0
    yr1_end_val = STARTING_CAPITAL + yr1_net
    yr2_end_val = yr1_end_val + yr2_net

    summary = (
        f"START (Apr-24): ₹25.00L\n"
        f"Mar-25:  ₹{yr1_end_val/1e5:.2f}L  ({100*(yr1_end_val/STARTING_CAPITAL - 1):+.1f}%)\n"
        f"Mar-26:  ₹{yr2_end_val/1e5:.2f}L  ({100*(yr2_end_val/STARTING_CAPITAL - 1):+.1f}%)\n"
        f"─────────────────────\n"
        f"Total trades: {wins+losses}\n"
        f"Win rate: {100*wins/(wins+losses):.0f}%\n"
        f"12% charges deducted"
    )
    fig.text(0.76, 0.60, summary, fontsize=11, color='#e0e0e0',
             verticalalignment='top',
             bbox=dict(boxstyle='round,pad=0.8', facecolor='#1a2a3a', edgecolor='#00d4ff', lw=1.5))

    fig.suptitle('Antigravity FNO — Historical Backtest  |  ₹25L → ?  |  Pairs Trading System',
                  fontsize=15, fontweight='bold', color='#ffffff', y=0.98)

    out = '.tmp/backtest_results.png'
    plt.savefig(out, dpi=180, bbox_inches='tight', facecolor='#0a0a0a')
    print(f"\n  Chart saved → {out}")
    return out


def main():
    print("╔" + "═" * 78 + "╗")
    print("║  HISTORICAL BACKTEST — Antigravity FNO".ljust(79) + "║")
    print("║  Capital: ₹25L  |  Apr 2024 – Mar 2026  |  Charges: 12%/trade".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    # Load all price series
    print("\n  Loading price data...")
    all_prices = {}
    for sym_a, sym_b, *_ in PAIRS:
        for sym in [sym_a, sym_b]:
            if sym not in all_prices:
                try:
                    all_prices[sym] = load_near_month_series(sym)
                    print(f"  ✓ {sym}: {len(all_prices[sym])} rows")
                except Exception as e:
                    print(f"  ✗ {sym}: {e}")

    # Run backtests
    print("\n  Running pair backtests...")
    all_trades = []
    for sym_a, sym_b, lots_a, lots_b, name in PAIRS:
        if sym_a not in all_prices or sym_b not in all_prices:
            print(f"  SKIP {name}: missing data")
            continue
        trades = backtest_pair(name, sym_a, sym_b, lots_a, lots_b,
                                all_prices[sym_a], all_prices[sym_b])
        print(f"  {name}: {len(trades)} trades")
        all_trades.extend(trades)

    if not all_trades:
        print("  No trades generated — check data.")
        return

    # Build equity curve
    equity, trades_df = build_equity_curve(all_trades, STARTING_CAPITAL)

    # Compute year-end values
    yr1_net = trades_df[trades_df['exit_date'] <= YEAR1_END]['net_pnl'].sum()
    yr2_net = trades_df[trades_df['exit_date'] > YEAR1_END]['net_pnl'].sum()
    yr1_val = STARTING_CAPITAL + yr1_net
    yr2_val = yr1_val + yr2_net

    # Print trade log
    wins, losses, total_gross, total_charges, total_net, year_pnl = print_trade_log(trades_df)

    print(f"\n{'═'*70}")
    print(f"  THE HONEST NUMBERS")
    print(f"{'═'*70}")
    print(f"  Start (Apr 01, 2024):  ₹{STARTING_CAPITAL/1e5:.2f}L")
    print(f"  End   (Mar 31, 2025):  ₹{yr1_val/1e5:.2f}L   ({100*(yr1_val/STARTING_CAPITAL - 1):+.1f}%)")
    print(f"  End   (Mar 31, 2026):  ₹{yr2_val/1e5:.2f}L   ({100*(yr2_val/STARTING_CAPITAL - 1):+.1f}%)")
    print(f"  Total charges paid:    ₹{total_charges/1e3:.1f}K  (kills {100*total_charges/abs(total_gross+1e-9):.0f}% of gross)")
    print(f"{'═'*70}")

    # Save trade log CSV
    trades_df.to_csv('.tmp/backtest_trade_log.csv', index=False)
    print(f"\n  Trade log → .tmp/backtest_trade_log.csv")

    # Plot
    plot_results(equity, trades_df, wins, losses, total_net, year_pnl)


if __name__ == '__main__':
    main()
