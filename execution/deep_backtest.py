"""
DEEP BACKTEST — Variable Time Stops + Walk-Forward Validation
=============================================================
Fixed: SSS=3.0, Z_EXIT=1.5 (optimal from v6 grid)
Varies: TIME_STOP in [5, 7, 10, 14, 20, 30, 60, ∞]

Walk-forward windows (expanding, 3-month test slices):
  Train Apr-24→Jun-24  / Test Jul-24→Sep-24
  Train Apr-24→Sep-24  / Test Oct-24→Dec-24
  Train Apr-24→Dec-24  / Test Jan-25→Mar-25
  Train Apr-24→Mar-25  / Test Apr-25→Jun-25
  Train Apr-24→Jun-25  / Test Jul-25→Sep-25
  Train Apr-24→Sep-25  / Test Oct-25→Dec-25

Outputs:
  .tmp/deep_backtest_timestop.png   — time stop P&L curve
  .tmp/deep_backtest_walkforward.png — OOS vs IS results
  .tmp/deep_backtest_survival.png   — survival curve (no-stop mode)
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

from backtest_mega import (
    load_all_prices, precompute_calendar,
    discover_universes, summarise, run_unified,
    STARTING_CAP,
)
from systems.pairs_system import precompute_signals

# ── Fixed params (optimal from v6) ───────────────────────────────────────────
SSS      = 3.0
Z_EXIT   = 1.5

# ── Time stop grid ────────────────────────────────────────────────────────────
TIME_STOPS = [5, 7, 10, 14, 20, 30, 60, 999]   # 999 = no stop
TS_LABELS  = {999: '∞'}

# ── Walk-forward windows ──────────────────────────────────────────────────────
FULL_START = pd.Timestamp('2024-04-01')
FULL_END   = pd.Timestamp('2026-03-31')

WF_WINDOWS = []
train_end_dates = pd.date_range('2024-06-30', '2025-09-30', freq='3ME')
for te in train_end_dates:
    test_start = te + pd.Timedelta(days=1)
    test_end   = te + pd.DateOffset(months=3)
    if test_end > FULL_END:
        test_end = FULL_END
    if test_start >= FULL_END:
        break
    WF_WINDOWS.append((FULL_START, te, test_start, test_end))


# ── Helpers ───────────────────────────────────────────────────────────────────

def ts_label(ts):
    return TS_LABELS.get(ts, str(ts))


def run_ts(pair_signals, cal_signals, refresh_points, universe_at,
           ts, start=None, end=None):
    """Run one time-stop value, return trades df and equity series."""
    df, eq = run_unified(
        pair_signals, cal_signals,
        refresh_points, universe_at,
        sss_threshold=SSS, z_exit=Z_EXIT,
        starting_cap=STARTING_CAP,
        time_stop=ts, start=start, end=end,
    )
    if not df.empty:
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date']  = pd.to_datetime(df['exit_date'])
    return df, eq


def net_pnl(df):
    return df['net'].sum() if not df.empty else 0


# ── Survival curve (no-stop mode) ─────────────────────────────────────────────

def compute_survival(df_no_stop):
    """
    Survival curve: at each holding day N, what % of trades had NOT yet
    reverted (i.e. exited on TIME_STOP at exactly day N)?
    This tells us: if we cut at day N, how much profit do we abandon?
    """
    if df_no_stop.empty:
        return pd.Series(dtype=float)

    # Filter pairs trades only
    pairs = df_no_stop[df_no_stop['strategy'] == 'PAIRS'].copy()
    if pairs.empty:
        return pd.Series(dtype=float)

    max_days = int(pairs['days'].max()) + 1
    days = np.arange(1, min(max_days, 120))

    # For each cutoff day: what % of trades would be force-exited vs profit
    surviving_net = []
    for d in days:
        # Trades that reverted BEFORE day d: keep their actual P&L
        early = pairs[pairs['days'] < d]
        # Trades that were still open at day d: assume exited at day d → 0 P&L
        # (conservative: they would exit at breakeven-ish; real value unknown)
        surviving_net.append(early['net'].sum())

    return pd.Series(surviving_net, index=days)


# ── Plotting ──────────────────────────────────────────────────────────────────

TC = '#e0e0e0'; GC = '#2a2a2a'

def style_ax(ax):
    ax.set_facecolor('#111111')
    ax.tick_params(colors=TC, labelsize=8)
    for sp in ax.spines.values():
        sp.set_color('#444')
    ax.yaxis.label.set_color(TC)
    ax.xaxis.label.set_color(TC)
    ax.title.set_color(TC)


def plot_timestop_curve(ts_results):
    fig, axes = plt.subplots(1, 3, figsize=(20, 6), facecolor='#0a0a0a')
    fig.suptitle('Deep Backtest — Variable Time Stop Analysis',
                 color='#fff', fontweight='bold', fontsize=13)

    labels  = [ts_label(ts) for ts in TIME_STOPS]
    nets    = [ts_results[ts]['net'] / 1e3 for ts in TIME_STOPS]
    wrs     = [ts_results[ts]['wr'] * 100  for ts in TIME_STOPS]
    counts  = [ts_results[ts]['n']          for ts in TIME_STOPS]
    ts_exits= [ts_results[ts].get('ts_pnl', 0) / 1e3 for ts in TIME_STOPS]

    # Net P&L vs time stop
    ax = axes[0]; style_ax(ax)
    clrs = ['#00ff88' if v >= 0 else '#ff4444' for v in nets]
    bars = ax.bar(labels, nets, color=clrs, alpha=0.85)
    best_ts = TIME_STOPS[int(np.argmax(nets))]
    ax.bar([ts_label(best_ts)], [ts_results[best_ts]['net'] / 1e3],
           color='#ffcc00', alpha=1.0, label=f'Best: {ts_label(best_ts)}d')
    ax.axhline(0, color='#888', lw=0.8)
    for bar, val in zip(bars, nets):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + (30 if val >= 0 else -80),
                f'Rs.{val:+.0f}K', ha='center', fontsize=7.5, color=TC)
    ax.set_title('Net P&L by Time Stop', fontweight='bold')
    ax.set_xlabel('Time Stop (days)')
    ax.set_ylabel('Net P&L (Rs.K)')
    ax.legend(fontsize=8, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax.grid(True, axis='y', color=GC, alpha=0.5)

    # Win rate vs time stop
    ax = axes[1]; style_ax(ax)
    ax.plot(labels, wrs, marker='o', color='#00d4ff', lw=2)
    ax.fill_between(range(len(labels)), wrs, alpha=0.15, color='#00d4ff')
    ax.axhline(56, color='#ffaa00', lw=1, ls='--', label='Break-even ~56%')
    ax.set_title('Win Rate by Time Stop', fontweight='bold')
    ax.set_xlabel('Time Stop (days)')
    ax.set_ylabel('Win Rate (%)')
    ax.legend(fontsize=8, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax.grid(True, color=GC, alpha=0.5)

    # TIME_STOP exit P&L drag
    ax = axes[2]; style_ax(ax)
    clrs2 = ['#ff4444' if v < 0 else '#00d4ff' for v in ts_exits]
    ax.bar(labels, ts_exits, color=clrs2, alpha=0.85)
    ax.axhline(0, color='#888', lw=0.8)
    ax.set_title('P&L Drag from TIME_STOP Exits', fontweight='bold')
    ax.set_xlabel('Time Stop (days)')
    ax.set_ylabel('TIME_STOP P&L (Rs.K)')
    ax.grid(True, axis='y', color=GC, alpha=0.5)
    for i, (label, val) in enumerate(zip(labels, ts_exits)):
        ax.text(i, val + (15 if val >= 0 else -60),
                f'Rs.{val:+.0f}K', ha='center', fontsize=7, color=TC)

    plt.tight_layout()
    out = '.tmp/deep_backtest_timestop.png'
    plt.savefig(out, dpi=140, bbox_inches='tight', facecolor='#0a0a0a')
    plt.close()
    print(f"  Chart -> {out}")


def plot_walkforward(wf_results):
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor='#0a0a0a')
    fig.suptitle('Walk-Forward Validation — IS vs OOS',
                 color='#fff', fontweight='bold', fontsize=13)

    window_labels = [f"W{i+1}\n{r['test_start'].strftime('%b%y')}" for i, r in enumerate(wf_results)]
    is_nets  = [r['best_is_net'] / 1e3 for r in wf_results]
    oos_nets = [r['oos_net']     / 1e3 for r in wf_results]
    best_tss = [r['best_ts']           for r in wf_results]

    ax = axes[0]; style_ax(ax)
    x = np.arange(len(wf_results))
    w = 0.35
    bars_is  = ax.bar(x - w/2, is_nets,  w, label='In-Sample',     color='#00d4ff', alpha=0.8)
    bars_oos = ax.bar(x + w/2, oos_nets, w, label='Out-of-Sample', color='#00ff88', alpha=0.8)
    for bar, val in zip(list(bars_is) + list(bars_oos), is_nets + oos_nets):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + (10 if val >= 0 else -40),
                f'Rs.{val:+.0f}K', ha='center', fontsize=7, color=TC)
    ax.axhline(0, color='#888', lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(window_labels)
    ax.legend(fontsize=9, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax.set_title('IS vs OOS Net P&L per Window', fontweight='bold')
    ax.set_ylabel('Net P&L (Rs.K)')
    ax.grid(True, axis='y', color=GC, alpha=0.5)

    # Best time stop selected per window
    ax = axes[1]; style_ax(ax)
    ax.bar(window_labels, [ts_label(t) for t in best_tss],
           color='#9966ff', alpha=0.85)
    # Plot as numeric
    ax.bar(range(len(best_tss)),
           [t if t != 999 else 90 for t in best_tss],
           color='#9966ff', alpha=0.85)
    ax.set_xticks(range(len(wf_results)))
    ax.set_xticklabels(window_labels)
    for i, ts in enumerate(best_tss):
        ax.text(i, (ts if ts != 999 else 90) + 1,
                ts_label(ts), ha='center', fontsize=9,
                color='#ffcc00', fontweight='bold')
    ax.set_title('Best TIME_STOP Selected (IS)', fontweight='bold')
    ax.set_ylabel('Time Stop (days)')
    ax.grid(True, axis='y', color=GC, alpha=0.5)

    plt.tight_layout()
    out = '.tmp/deep_backtest_walkforward.png'
    plt.savefig(out, dpi=140, bbox_inches='tight', facecolor='#0a0a0a')
    plt.close()
    print(f"  Chart -> {out}")


def plot_survival(df_no_stop):
    surv = compute_survival(df_no_stop)
    if surv.empty:
        return

    pairs_no_stop = df_no_stop[df_no_stop['strategy'] == 'PAIRS']
    total_pnl = pairs_no_stop['net'].sum()

    fig, axes = plt.subplots(1, 2, figsize=(16, 6), facecolor='#0a0a0a')
    fig.suptitle('Survival Curve — No Time Stop (∞) Analysis',
                 color='#fff', fontweight='bold', fontsize=13)

    # Cumulative P&L captured vs cutoff day
    ax = axes[0]; style_ax(ax)
    pct = (surv / total_pnl * 100) if total_pnl != 0 else surv * 0
    ax.plot(surv.index, pct, color='#00d4ff', lw=2)
    ax.fill_between(surv.index, pct, alpha=0.15, color='#00d4ff')
    ax.axhline(90, color='#ffaa00', lw=1, ls='--', label='90% captured')
    # Find 90% point
    idx_90 = pct[pct >= 90].index.min() if (pct >= 90).any() else None
    if idx_90:
        ax.axvline(idx_90, color='#ffaa00', lw=1, ls=':')
        ax.text(idx_90 + 1, 45, f'{idx_90}d\ncaptures 90%',
                color='#ffaa00', fontsize=9)
    ax.set_title('% of Total P&L Captured by Day N', fontweight='bold')
    ax.set_xlabel('Days Since Entry (cutoff)')
    ax.set_ylabel('% of No-Stop P&L Captured')
    ax.legend(fontsize=8, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax.grid(True, color=GC, alpha=0.5)

    # Duration distribution (no-stop mode)
    ax = axes[1]; style_ax(ax)
    days_dist = pairs_no_stop['days'].clip(upper=90)
    win_days  = pairs_no_stop[pairs_no_stop['win']]['days'].clip(upper=90)
    loss_days = pairs_no_stop[~pairs_no_stop['win']]['days'].clip(upper=90)
    ax.hist(win_days,  bins=30, color='#00ff88', alpha=0.6, label='Wins')
    ax.hist(loss_days, bins=30, color='#ff4444', alpha=0.6, label='Losses')
    ax.axvline(win_days.median(),  color='#00ff88', lw=2, ls='--',
               label=f'Win median: {win_days.median():.0f}d')
    ax.axvline(loss_days.median(), color='#ff4444', lw=2, ls='--',
               label=f'Loss median: {loss_days.median():.0f}d')
    ax.set_title('Win vs Loss Duration (No-Stop)', fontweight='bold')
    ax.set_xlabel('Days Held')
    ax.set_ylabel('Trade Count')
    ax.legend(fontsize=8, facecolor='#1a1a1a', edgecolor='#444', labelcolor=TC)
    ax.grid(True, color=GC, alpha=0.5)

    plt.tight_layout()
    out = '.tmp/deep_backtest_survival.png'
    plt.savefig(out, dpi=140, bbox_inches='tight', facecolor='#0a0a0a')
    plt.close()
    print(f"  Chart -> {out}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("  DEEP BACKTEST — Variable Time Stops + Walk-Forward")
    print(f"  Fixed: SSS={SSS}, Z_EXIT={Z_EXIT}")
    print(f"  TIME_STOP grid: {TIME_STOPS}")
    print(f"  Walk-forward windows: {len(WF_WINDOWS)}")
    print("=" * 80)

    # ── Load + precompute once ────────────────────────────────────────────
    print("\n  Loading data...")
    price_data  = load_all_prices()
    cal_signals = precompute_calendar()

    trading_days = pd.bdate_range(FULL_START, FULL_END)
    print("  Discovering universes (rolling)...")
    refresh_points, universe_at, all_tuples, _ = \
        discover_universes(price_data, trading_days)

    print(f"  Precomputing signals for {len(all_tuples)} pairs...")
    pair_signals = precompute_signals(price_data, all_tuples)
    print(f"  Signals ready: {len(pair_signals)} pairs")

    # ── Phase 1: Full-period time stop grid ──────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 1: Time Stop Grid (full 2-year period)")
    print(f"  {'TS':>5} {'Net P&L':>10} {'WR':>6} {'Tr':>5} "
          f"{'TS exits':>9} {'TS P&L':>10}")
    print("  " + "-" * 55)

    ts_results = {}
    for ts in TIME_STOPS:
        df, eq = run_ts(pair_signals, cal_signals, refresh_points, universe_at, ts)
        s = summarise(df)

        # P&L attributable to TIME_STOP exits
        ts_pnl = df[df['reason'] == 'TIME_STOP']['net'].sum() if not df.empty else 0
        ts_n   = (df['reason'] == 'TIME_STOP').sum()          if not df.empty else 0

        ts_results[ts] = {**s, 'ts_pnl': ts_pnl, 'ts_n': ts_n, 'eq': eq}

        lbl = f"{ts_label(ts):>5}"
        print(f"  {lbl} "
              f"  Rs.{s['net']/1e3:>+7.0f}K"
              f"  {100*s['wr']:>5.0f}%"
              f"  {s['n']:>5}"
              f"  {ts_n:>6} exits"
              f"  Rs.{ts_pnl/1e3:>+7.0f}K")

    best_ts    = max(ts_results, key=lambda t: ts_results[t]['net'])
    no_stop_df, _ = run_ts(pair_signals, cal_signals, refresh_points,
                           universe_at, 999)

    print("  " + "-" * 55)
    print(f"  BEST: TIME_STOP={ts_label(best_ts)}d  "
          f"Net=Rs.{ts_results[best_ts]['net']/1e5:.2f}L  "
          f"WR={100*ts_results[best_ts]['wr']:.1f}%")

    # ── Phase 2: Walk-forward validation ─────────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 2: Walk-Forward Validation")
    print(f"  {'Window':>8} {'Train':>20} {'Test':>20} "
          f"{'Best TS':>8} {'IS Net':>10} {'OOS Net':>10}")
    print("  " + "-" * 80)

    wf_results = []
    for i, (tr_start, tr_end, te_start, te_end) in enumerate(WF_WINDOWS):
        # In-sample: find best time stop
        is_nets = {}
        for ts in TIME_STOPS:
            df_is, _ = run_ts(pair_signals, cal_signals, refresh_points,
                              universe_at, ts, start=tr_start, end=tr_end)
            is_nets[ts] = net_pnl(df_is)

        best_is_ts  = max(is_nets, key=lambda t: is_nets[t])

        # Out-of-sample: run with best IS time stop
        df_oos, _ = run_ts(pair_signals, cal_signals, refresh_points,
                           universe_at, best_is_ts,
                           start=te_start, end=te_end)
        oos_net = net_pnl(df_oos)

        wf_results.append(dict(
            test_start   = te_start,
            best_ts      = best_is_ts,
            best_is_net  = is_nets[best_is_ts],
            oos_net      = oos_net,
        ))

        print(f"  W{i+1:>2}    "
              f"  {tr_start.date()} → {tr_end.date()}"
              f"  {te_start.date()} → {te_end.date()}"
              f"  {ts_label(best_is_ts):>7}d"
              f"  Rs.{is_nets[best_is_ts]/1e3:>+7.0f}K"
              f"  Rs.{oos_net/1e3:>+7.0f}K")

    total_oos = sum(r['oos_net'] for r in wf_results)
    print("  " + "-" * 80)
    print(f"  Total OOS P&L: Rs.{total_oos/1e3:+.0f}K")

    # ── Phase 3: Survival analysis (no-stop) ─────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 3: Survival Analysis (no time stop)")
    if not no_stop_df.empty:
        pairs_ns = no_stop_df[no_stop_df['strategy'] == 'PAIRS']
        if not pairs_ns.empty:
            wins   = pairs_ns[pairs_ns['win']]
            losses = pairs_ns[~pairs_ns['win']]
            print(f"  No-stop pairs trades: {len(pairs_ns)}")
            print(f"  Win median hold:  {wins['days'].median():.0f}d")
            print(f"  Loss median hold: {losses['days'].median():.0f}d")
            surv = compute_survival(no_stop_df)
            total = pairs_ns['net'].sum()
            # 90% capture day
            pct = surv / total * 100 if total != 0 else surv * 0
            idx_90 = pct[pct >= 90].index.min() if (pct >= 90).any() else None
            if idx_90:
                print(f"  90% of P&L captured by day: {idx_90}")
            print(f"  Recommendation: TIME_STOP = {idx_90 or best_ts}d  "
                  f"(empirically derived from reversion data)")

    print(f"{'='*80}")

    # ── Plots ─────────────────────────────────────────────────────────────
    os.makedirs('.tmp', exist_ok=True)
    plot_timestop_curve(ts_results)
    plot_walkforward(wf_results)
    plot_survival(no_stop_df)


if __name__ == '__main__':
    main()
