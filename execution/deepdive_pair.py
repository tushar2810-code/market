"""
Deep dive analysis for a single pair: cointegration, Hurst, OU half-life,
rolling structural integrity, Z-score history, recent price action, backtest.

Usage:
    python3 execution/deepdive_pair.py IDFCFIRSTB AUBANK
"""
import sys
import warnings
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

warnings.filterwarnings('ignore')

DATA_DIR = Path('.tmp/3y_data')


def load(sym):
    df = pd.read_csv(DATA_DIR / f'{sym}_5Y.csv')
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')
    df = df.drop_duplicates('FH_TIMESTAMP', keep='last').set_index('FH_TIMESTAMP')
    df['lot']   = pd.to_numeric(df['FH_MARKET_LOT'],    errors='coerce').ffill().bfill()
    df['price'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    return df


def hurst_rs(series):
    """R/S Hurst exponent."""
    vals = np.array(series)
    lags = range(2, min(100, len(vals) // 4))
    rs_list = []
    for lag in lags:
        chunks = [vals[i:i+lag] for i in range(0, len(vals)-lag, lag)]
        rs_vals = []
        for chunk in chunks:
            mean = np.mean(chunk)
            dev  = np.cumsum(chunk - mean)
            R    = dev.max() - dev.min()
            S    = np.std(chunk, ddof=1)
            if S > 0:
                rs_vals.append(R / S)
        if rs_vals:
            rs_list.append(np.mean(rs_vals))
    lags_arr = np.array(list(lags[:len(rs_list)]))
    rs_arr   = np.array(rs_list)
    valid    = rs_arr > 0
    if valid.sum() < 2:
        return float('nan')
    return np.polyfit(np.log(lags_arr[valid]), np.log(rs_arr[valid]), 1)[0]


def run(sym_a, sym_b):
    ha = load(sym_a)
    hb = load(sym_b)

    m = ha[['price', 'lot']].join(hb[['price', 'lot']], lsuffix='_a', rsuffix='_b', how='inner')
    m['spread'] = m['price_a'] * m['lot_a'] - m['price_b'] * m['lot_b']

    # Live prices (today)
    raw = yf.download(
        [sym_a + '.NS', sym_b + '.NS'],
        period='5d', auto_adjust=True, progress=False, group_by='ticker'
    )
    live_a = float(raw[sym_a + '.NS']['Close'].dropna().iloc[-1])
    live_b = float(raw[sym_b + '.NS']['Close'].dropna().iloc[-1])
    live_date = raw[sym_a + '.NS']['Close'].dropna().index[-1].strftime('%Y-%m-%d')
    lot_a  = float(m['lot_a'].iloc[-1])
    lot_b  = float(m['lot_b'].iloc[-1])
    live_spread = live_a * lot_a - live_b * lot_b

    sep = '=' * 70
    print(sep)
    print(f'  {sym_a} / {sym_b} — DEEP DIVE  [{live_date}]')
    print(sep)

    # ── 1. DATA OVERVIEW ──────────────────────────────────────────────────────
    print('\n[1] DATA OVERVIEW')
    print(f'    History   : {m.index[0].date()} → {m.index[-1].date()}  ({len(m)} days)')
    print(f'    {sym_a:12}: CSV ₹{m["price_a"].iloc[-1]:.2f}  |  live ₹{live_a:.2f}  |  lot {int(lot_a)}')
    print(f'    {sym_b:12}: CSV ₹{m["price_b"].iloc[-1]:.2f}  |  live ₹{live_b:.2f}  |  lot {int(lot_b)}')
    print(f'    Live spread: ₹{live_spread:,.0f}')

    # ── 2. COINTEGRATION TESTS (full 3Y) ──────────────────────────────────────
    print('\n[2] COINTEGRATION TESTS (full 3Y)')
    spread_3y = m['spread'].dropna()

    adf_stat, adf_p, adf_lags, _, adf_crit, _ = adfuller(spread_3y, maxlag=10, autolag='AIC')
    adf_ok = adf_p < 0.05
    print(f'    ADF        : stat={adf_stat:.3f}  p={adf_p:.4f}  lags={adf_lags}  '
          f'crit5%={adf_crit["5%"]:.3f}  → {"STATIONARY ✅" if adf_ok else "NON-STATIONARY ❌"}')

    _, coint_p, _ = coint(m['price_a'], m['price_b'])
    coint_ok = coint_p < 0.05
    print(f'    Engle-Gran : p={coint_p:.4f}  '
          f'→ {"COINTEGRATED ✅" if coint_ok else "NOT COINTEGRATED ❌"}')

    # ── 3. ROLLING STRUCTURAL INTEGRITY ───────────────────────────────────────
    print('\n[3] ROLLING STRUCTURAL INTEGRITY')
    for label, w in [('3M', 63), ('6M', 126), ('1Y', 252)]:
        sub = m.tail(w)
        if len(sub) < 30:
            print(f'    {label}: insufficient data')
            continue
        _, cp, _ = coint(sub['price_a'], sub['price_b'])
        sp_sub = (sub['price_a'] * sub['lot_a'] - sub['price_b'] * sub['lot_b']).dropna()
        _, ap, *_ = adfuller(sp_sub, maxlag=5)
        broken = cp > 0.20 and ap > 0.10
        flag = '❌ STRUCTURAL BREAK' if broken else '✅ INTACT'
        print(f'    {label} ({len(sub)}d): coint p={cp:.3f}  ADF p={ap:.3f}  → {flag}')

    # ── 4. HURST EXPONENT ─────────────────────────────────────────────────────
    print('\n[4] HURST EXPONENT (spread)')
    try:
        from hurst import compute_Hc
        H, _, _ = compute_Hc(spread_3y.values, kind='change', simplified=True)
    except ImportError:
        H = hurst_rs(spread_3y.values)
    print(f'    H = {H:.4f}  → {"MEAN-REVERTING ✅ (H<0.5)" if H < 0.5 else "TRENDING/RANDOM ❌ (H>=0.5)"}')

    # ── 5. OU HALF-LIFE ───────────────────────────────────────────────────────
    print('\n[5] OU HALF-LIFE')
    sv = spread_3y.values
    res = OLS(sv[1:] - sv[:-1], add_constant(sv[:-1])).fit()
    theta = -res.params[1]
    hl = np.log(2) / theta if theta > 0 else float('inf')
    print(f'    theta={theta:.6f}  half-life={hl:.1f} days  '
          f'→ {"✅ Tradeable (<40d)" if hl < 40 else "❌ Too slow (>40d)"}')

    # ── 6. Z-SCORE (live) ─────────────────────────────────────────────────────
    print('\n[6] Z-SCORE (live prices vs historical)')
    for label, w in [('20d', 20), ('30d', 30), ('60d', 60)]:
        mn = m['spread'].tail(w).mean()
        sd = m['spread'].tail(w).std()
        z  = (live_spread - mn) / sd if sd else float('nan')
        roll_z = (m['spread'] - m['spread'].rolling(w).mean()) / m['spread'].rolling(w).std()
        pct_more_extreme = (roll_z.abs() > abs(z)).sum() / roll_z.dropna().__len__() * 100
        print(f'    {label}: Z={z:+.2f}  ({pct_more_extreme:.1f}% of history was more extreme)')

    # ── 7. RECENT PRICE ACTION ────────────────────────────────────────────────
    print('\n[7] RECENT PRICE ACTION (last 15 days)')
    last15 = m.tail(15)
    roll_mean = m['spread'].rolling(60).mean()
    roll_std  = m['spread'].rolling(60).std()
    roll_z60  = (m['spread'] - roll_mean) / roll_std
    print(f'    {"Date":12} {sym_a:>12} {sym_b:>10} {"Spread":>14} {"Z(60d)":>8}')
    print(f'    {"-"*60}')
    for dt, row in last15.iterrows():
        z = roll_z60.loc[dt]
        print(f'    {str(dt.date()):12} {row["price_a"]:>12.2f} {row["price_b"]:>10.2f} '
              f'{row["spread"]:>14,.0f} {z:>8.2f}')
    print(f'    {"TODAY(live)":12} {live_a:>12.2f} {live_b:>10.2f} {live_spread:>14,.0f}  (above)')

    # ── 8. RETURN CORRELATION ─────────────────────────────────────────────────
    print('\n[8] RETURN CORRELATION')
    ret_a = m['price_a'].pct_change()
    ret_b = m['price_b'].pct_change()
    for label, w in [('20d', 20), ('60d', 60), ('252d', 252)]:
        c_ret   = ret_a.tail(w).corr(ret_b.tail(w))
        c_price = m['price_a'].tail(w).corr(m['price_b'].tail(w))
        print(f'    {label}: return_corr={c_ret:.3f}  price_corr={c_price:.3f}')

    # ── 9. BACKTEST (60d Z, structural stop only) ─────────────────────────────
    print('\n[9] BACKTEST (60d Z, entry |Z|>=2, exit Z->0 or 30d time stop)')
    m2 = m.copy()
    m2['z60'] = (m2['spread'] - m2['spread'].rolling(60).mean()) / m2['spread'].rolling(60).std()

    trades = []
    in_trade = False
    entry_z = entry_i = entry_spread = 0

    for i, (dt, row) in enumerate(m2.iterrows()):
        z = row['z60']
        if pd.isna(z):
            continue
        if not in_trade and abs(z) >= 2.0:
            in_trade, entry_z, entry_i, entry_spread = True, z, i, row['spread']
        elif in_trade:
            days_held = i - entry_i
            win = (entry_z > 0 and z <= 0) or (entry_z < 0 and z >= 0)
            if win or days_held >= 30:
                pnl = (entry_spread - row['spread']) if entry_z > 0 else (row['spread'] - entry_spread)
                ret_pct = pnl / abs(entry_spread) * 100
                trades.append({
                    'date': dt, 'entry_z': round(entry_z, 2), 'exit_z': round(z, 2),
                    'days': days_held, 'win': win, 'ret_pct': round(ret_pct, 2)
                })
                in_trade = False

    if trades:
        df_t = pd.DataFrame(trades)
        n    = len(df_t)
        wins = df_t['win'].sum()
        avg_w = df_t[df_t['win']]['ret_pct'].mean() if wins else 0
        avg_l = df_t[~df_t['win']]['ret_pct'].mean() if (n - wins) else 0
        print(f'    Trades={n}  WR={wins/n*100:.1f}%  Avg win={avg_w:.2f}%  Avg loss={avg_l:.2f}%')
        print(f'    All trades:')
        for _, t in df_t.iterrows():
            tag = 'WIN ' if t['win'] else 'LOSS'
            print(f'      {str(t["date"].date()):12}  entry Z={t["entry_z"]:+.2f}  '
                  f'exit Z={t["exit_z"]:+.2f}  {t["days"]:2}d  {tag}  {t["ret_pct"]:+.2f}%')
    else:
        print('    No completed trades in history')

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    print('\n' + sep)
    print('  VERDICT')
    print(sep)
    checks = {
        'ADF stationary (3Y)': adf_ok,
        'Cointegrated (3Y)':   coint_ok,
        'Hurst < 0.5':         H < 0.5,
        'Half-life < 40d':     hl < 40,
    }
    all_pass = all(checks.values())
    for name, ok in checks.items():
        print(f'    {"✅" if ok else "❌"} {name}')
    print()
    if all_pass:
        print('  ALL GATES PASS — Statistically valid pair')
    else:
        failed = [k for k, v in checks.items() if not v]
        print(f'  FAILED: {", ".join(failed)}')

    # Z-score verdict
    mn60 = m['spread'].tail(60).mean()
    sd60 = m['spread'].tail(60).std()
    z60_live = (live_spread - mn60) / sd60 if sd60 else float('nan')
    mn30 = m['spread'].tail(30).mean()
    sd30 = m['spread'].tail(30).std()
    z30_live = (live_spread - mn30) / sd30 if sd30 else float('nan')

    windows_signaling = sum([
        abs(z60_live) >= 2.0,
        abs(z30_live) >= 2.0,
    ])
    print()
    if windows_signaling >= 2 and all_pass:
        direction = f'BUY {sym_a} / SELL {sym_b}' if z60_live < 0 else f'SELL {sym_a} / BUY {sym_b}'
        print(f'  SIGNAL: {windows_signaling}/2 windows confirm  →  {direction}')
    elif windows_signaling == 1:
        print(f'  WATCH: 1/2 windows at threshold — not high-conviction yet')
    else:
        print(f'  NO SIGNAL: Z below entry threshold in both 30d and 60d windows')
    print(sep)


if __name__ == '__main__':
    sym_a = sys.argv[1].upper() if len(sys.argv) > 1 else 'IDFCFIRSTB'
    sym_b = sys.argv[2].upper() if len(sys.argv) > 2 else 'AUBANK'
    run(sym_a, sym_b)
