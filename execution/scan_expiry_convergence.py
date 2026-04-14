"""
Expiry Convergence Scanner — Near-month futures vs spot premium/discount.

Near expiry, strong convergence pressure exists due to physical settlement obligations.
Backtest: 64% overall WR across 153 symbols. Top symbols (SAIL, ITC, HINDUNILVR) reach 77-83%.
NOT a guaranteed arbitrage — corporate actions, circuits, and FII activity can prevent convergence.
Exit early when in profit (50%+ premium closure). Do NOT hold to expiry by default.

Math:
  Premium = (Futures - Spot) / Spot * 100
  If Premium > +0.5% → Sell Futures (premium tends to decay toward 0)
  If Premium < -0.5% → Buy Futures (discount tends to close toward 0)
  Premiums up to ±15% are observed in real data and DO converge.

Usage:
    python3 execution/scan_expiry_convergence.py
    python3 execution/scan_expiry_convergence.py --threshold 0.3 --backtest
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(__file__))

DATA_DIR = '.tmp/3y_data'


def get_last_tuesday(year, month):
    """Get the last Tuesday of a given month (FNO expiry rule)."""
    import calendar
    cal = calendar.monthcalendar(year, month)
    # calendar.TUESDAY = 1
    last_tuesday = None
    for week in cal:
        if week[calendar.TUESDAY] != 0:
            last_tuesday = week[calendar.TUESDAY]
    return datetime(year, month, last_tuesday)


def load_futures_data(symbol):
    """Load raw futures data with expiry info."""
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        return None
    
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
    df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    
    if 'FH_UNDERLYING_VALUE' in df.columns:
        df['FH_UNDERLYING_VALUE'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
    
    df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_EXPIRY_DT'])
    
    if 'FH_INSTRUMENT' in df.columns:
        df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
    
    return df.sort_values('FH_TIMESTAMP')


def backtest_expiry_convergence(symbol, df, threshold_pct=0.5, entry_days_before=5):
    """
    Backtest the expiry convergence strategy on historical data.
    
    For each expiry:
    1. Look at futures price N days before expiry
    2. Calculate premium/discount vs underlying
    3. If |premium| > threshold, enter trade
    4. Exit at expiry (premium → 0)
    """
    if 'FH_UNDERLYING_VALUE' not in df.columns:
        return []
    
    # Get unique expiry dates
    expiries = df['FH_EXPIRY_DT'].dropna().unique()
    expiries = sorted(expiries)
    
    trades = []
    
    for expiry in expiries:
        expiry_dt = pd.Timestamp(expiry)
        
        # Get nearest-expiry contracts for this expiry date
        month_data = df[df['FH_EXPIRY_DT'] == expiry].copy()
        if month_data.empty:
            continue
        
        # Find the entry window: N trading days before expiry
        entry_window_start = expiry_dt - timedelta(days=entry_days_before + 3)  # Buffer for weekends
        
        # Get data in the entry window
        window_data = month_data[
            (month_data['FH_TIMESTAMP'] >= entry_window_start) & 
            (month_data['FH_TIMESTAMP'] <= expiry_dt)
        ].sort_values('FH_TIMESTAMP')
        
        if len(window_data) < 2:
            continue
        
        # Entry: N days before expiry
        entry_candidates = window_data[
            window_data['FH_TIMESTAMP'] <= expiry_dt - timedelta(days=entry_days_before - 2)
        ]
        
        if entry_candidates.empty:
            entry_candidates = window_data.head(1)
        
        entry_row = entry_candidates.iloc[-1]  # Latest row in entry window
        
        futures_price = entry_row['FH_CLOSING_PRICE']
        spot_price = entry_row.get('FH_UNDERLYING_VALUE', None)
        
        if spot_price is None or spot_price == 0 or np.isnan(spot_price):
            continue
        
        premium_pct = (futures_price - spot_price) / spot_price * 100
        
        # Check threshold
        if abs(premium_pct) < threshold_pct:
            continue
        
        # Exit: last trading day at/before expiry
        exit_data = window_data[window_data['FH_TIMESTAMP'] <= expiry_dt]
        if exit_data.empty:
            continue
        
        exit_row = exit_data.iloc[-1]
        exit_futures = exit_row['FH_CLOSING_PRICE']
        exit_spot = exit_row.get('FH_UNDERLYING_VALUE', spot_price)
        exit_premium_pct = (exit_futures - exit_spot) / exit_spot * 100 if exit_spot > 0 else 0
        
        # P&L
        if premium_pct > 0:
            # Premium → Sell futures, premium should decay to 0
            pnl_pct = premium_pct - exit_premium_pct  # We captured the premium decay
            direction = "SELL"
        else:
            # Discount → Buy futures, discount should converge to 0
            pnl_pct = exit_premium_pct - premium_pct  # Discount tightened
            direction = "BUY"
        
        # Per-lot P&L estimate
        lot_size = int(entry_row.get('FH_MARKET_LOT', 1)) if 'FH_MARKET_LOT' in entry_row.index else 1
        pnl_rupees = (pnl_pct / 100) * spot_price * lot_size
        
        days_held = (exit_row['FH_TIMESTAMP'] - entry_row['FH_TIMESTAMP']).days
        
        trades.append({
            'symbol': symbol,
            'expiry': expiry_dt.date(),
            'entry_date': entry_row['FH_TIMESTAMP'].date(),
            'exit_date': exit_row['FH_TIMESTAMP'].date(),
            'direction': direction,
            'entry_premium_pct': round(premium_pct, 3),
            'exit_premium_pct': round(exit_premium_pct, 3),
            'pnl_pct': round(pnl_pct, 3),
            'pnl_rupees': round(pnl_rupees, 0),
            'spot': round(spot_price, 2),
            'lot_size': lot_size,
            'days_held': days_held
        })
    
    return trades


def scan_live_convergence(threshold_pct=0.5):
    """
    Scan current FNO data for expiry convergence opportunities.
    Look at the most recent data point and check premium/discount.
    """
    print("\n" + "━" * 80)
    print("  LIVE EXPIRY CONVERGENCE SCAN")
    print("━" * 80)
    
    results = []
    data_dir = DATA_DIR
    files = [f for f in os.listdir(data_dir) if f.endswith('_5Y.csv')]
    
    for fname in sorted(files):
        symbol = fname.replace('_5Y.csv', '')
        df = load_futures_data(symbol)
        if df is None or 'FH_UNDERLYING_VALUE' not in df.columns:
            continue
        
        # Gate 1: Data freshness — reject if > 3 trading days stale
        last_date = df['FH_TIMESTAMP'].max()
        if pd.isna(last_date):
            continue
        try:
            trading_days_stale = int(np.busday_count(last_date.date(), datetime.now().date()))
        except Exception:
            continue
        if trading_days_stale > 3:
            continue

        # Select nearest active contract (min future expiry date > today)
        today = datetime.now().date()
        active = df[df['FH_EXPIRY_DT'].dt.date > today]
        if active.empty:
            continue
        nearest_expiry = active['FH_EXPIRY_DT'].min()
        near_df = active[active['FH_EXPIRY_DT'] == nearest_expiry]
        latest = near_df.sort_values('FH_TIMESTAMP').iloc[-1]

        futures_price = latest['FH_CLOSING_PRICE']
        spot_price = latest.get('FH_UNDERLYING_VALUE', None)
        expiry = latest['FH_EXPIRY_DT']

        if spot_price is None or spot_price == 0 or np.isnan(spot_price) or np.isnan(futures_price):
            continue

        premium_pct = (futures_price - spot_price) / spot_price * 100

        # Days to expiry from today (not from stale data date)
        days_to_expiry = (expiry.date() - today).days

        lot_size = int(latest.get('FH_MARKET_LOT', 1) or 1) if 'FH_MARKET_LOT' in latest.index else 1
        potential_gain = abs(premium_pct / 100) * spot_price * lot_size
        
        if abs(premium_pct) >= threshold_pct and 0 < days_to_expiry <= 10:
            results.append({
                'symbol': symbol,
                'spot': round(spot_price, 2),
                'futures': round(futures_price, 2),
                'premium_pct': round(premium_pct, 3),
                'expiry': expiry.date(),
                'days_to_expiry': days_to_expiry,
                'lot_size': lot_size,
                'potential_gain': round(potential_gain, 0),
                'direction': 'SELL FUT' if premium_pct > 0 else 'BUY FUT'
            })
    
    if results:
        df_res = pd.DataFrame(results).sort_values('potential_gain', ascending=False)
        print(f"\n  Found {len(df_res)} opportunities (|premium| > {threshold_pct}%, ≤10 days to expiry):")
        print(f"  {'Symbol':<15} {'Spot':>8} {'Futures':>8} {'Prem%':>7} {'Expiry':>12} {'DTE':>4} {'Gain':>8} {'Action':<10}")
        print(f"  {'─'*80}")
        for _, r in df_res.head(20).iterrows():
            print(f"  {r['symbol']:<15} {r['spot']:>8.1f} {r['futures']:>8.1f} {r['premium_pct']:>+6.2f}% {r['expiry']} {r['days_to_expiry']:>4} ₹{r['potential_gain']:>6.0f} {r['direction']:<10}")
        return df_res
    else:
        print(f"  No opportunities found (threshold: {threshold_pct}%)")
        return pd.DataFrame()


def run_backtest(threshold_pct=0.5, entry_days=5):
    """Run backtest across all symbols and aggregate results."""
    print("\n" + "═" * 80)
    print("  EXPIRY CONVERGENCE — BACKTEST")
    print(f"  Threshold: {threshold_pct}% | Entry: {entry_days} days before expiry")
    print("═" * 80)
    
    all_trades = []
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_5Y.csv')]
    
    symbols_tested = 0
    for fname in sorted(files):
        symbol = fname.replace('_5Y.csv', '')
        df = load_futures_data(symbol)
        if df is None:
            continue
        
        trades = backtest_expiry_convergence(symbol, df, threshold_pct, entry_days)
        all_trades.extend(trades)
        symbols_tested += 1
    
    if not all_trades:
        print("  No trades generated.")
        return
    
    df_trades = pd.DataFrame(all_trades)
    
    # Aggregate stats
    total = len(df_trades)
    wins = df_trades[df_trades['pnl_pct'] > 0]
    losses = df_trades[df_trades['pnl_pct'] <= 0]
    
    wr = len(wins) / total * 100
    avg_return = df_trades['pnl_pct'].mean()
    avg_win = wins['pnl_pct'].mean() if len(wins) > 0 else 0
    avg_loss = losses['pnl_pct'].mean() if len(losses) > 0 else 0
    worst_trade = df_trades['pnl_pct'].min()
    total_pnl = df_trades['pnl_rupees'].sum()
    
    print(f"\n  Symbols tested: {symbols_tested}")
    print(f"  Total Trades:   {total}")
    print(f"  Win Rate:       {wr:.1f}%")
    print(f"  Avg Return:     {avg_return:+.3f}%")
    print(f"  Avg Win:        {avg_win:+.3f}%")
    print(f"  Avg Loss:       {avg_loss:+.3f}%")
    print(f"  Worst Trade:    {worst_trade:+.3f}%")
    print(f"  Total P&L:      ₹{total_pnl:,.0f}")
    
    # Per-symbol breakdown (top performers)
    by_symbol = df_trades.groupby('symbol').agg(
        trades=('pnl_pct', 'count'),
        wr=('pnl_pct', lambda x: (x > 0).mean() * 100),
        avg_ret=('pnl_pct', 'mean'),
        total_pnl=('pnl_rupees', 'sum')
    ).sort_values('total_pnl', ascending=False)
    
    print(f"\n  Top 10 Symbols by Total P&L:")
    print(f"  {'Symbol':<15} {'Trades':>6} {'WR':>6} {'Avg%':>7} {'Total P&L':>12}")
    print(f"  {'─'*50}")
    for sym, row in by_symbol.head(10).iterrows():
        print(f"  {sym:<15} {row['trades']:>6.0f} {row['wr']:>5.1f}% {row['avg_ret']:>+6.3f}% ₹{row['total_pnl']:>10,.0f}")
    
    # Monthly distribution
    df_trades['month'] = pd.to_datetime(df_trades['expiry']).dt.to_period('M')
    monthly = df_trades.groupby('month').agg(
        trades=('pnl_pct', 'count'),
        pnl=('pnl_rupees', 'sum')
    )
    
    print(f"\n  Monthly P&L (last 12 months):")
    for month, row in monthly.tail(12).iterrows():
        bar = "█" * max(1, int(abs(row['pnl']) / 5000))
        print(f"    {month}: {row['trades']:>3} trades  ₹{row['pnl']:>+10,.0f} {bar}")
    
    # Save
    df_trades.to_csv('.tmp/expiry_convergence_backtest.csv', index=False)
    print(f"\n  Full results saved to .tmp/expiry_convergence_backtest.csv")
    
    # Kelly sizing recommendation
    if wr > 50 and avg_win > 0 and avg_loss < 0:
        from kelly_sizer import kelly_fraction
        kf, kh, edge = kelly_fraction(wr / 100, avg_win / 100, abs(avg_loss) / 100)
        print(f"\n  Kelly Sizing:")
        print(f"    Edge per trade: {edge * 100:.3f}%")
        print(f"    Full Kelly: {kf:.1%}")
        print(f"    Half Kelly: {kh:.1%}")
    
    print(f"\n{'═'*80}")


def main():
    parser = argparse.ArgumentParser(description='Expiry Convergence Scanner')
    parser.add_argument('--threshold', type=float, default=0.5, help='Min premium/discount %% to trade')
    parser.add_argument('--backtest', action='store_true', help='Run historical backtest')
    parser.add_argument('--entry-days', type=int, default=5, help='Days before expiry to enter')
    args = parser.parse_args()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  EXPIRY CONVERGENCE — Strategy Layer 3".ljust(79) + "║")
    print(f"║  Antigravity v3 — High-Probability Convergence (~64% WR)".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    if args.backtest:
        run_backtest(args.threshold, args.entry_days)
    else:
        scan_live_convergence(args.threshold)


if __name__ == "__main__":
    main()
