"""
Antigravity v3 — Combined Portfolio Backtester.

Merges all strategy layers with Kelly sizing and compounding to 
calculate the ACTUAL portfolio CAGR.

Strategies:
  1. Pair Trading (near-miss Renaissance pairs)
  2. Expiry Convergence (premium/discount decay)
  3. Momentum RSI (sector-filtered oversold/overbought)

Uses actual trade logs from individual backtests.

Usage:
    python3 execution/portfolio_backtester.py --capital 1000000
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime

sys.path.append(os.path.dirname(__file__))
from kelly_sizer import kelly_fraction


def load_trade_logs():
    """Load all individual strategy backtest results."""
    strategies = {}
    
    # Expiry convergence
    ec_path = '.tmp/expiry_convergence_backtest.csv'
    if os.path.exists(ec_path):
        df = pd.read_csv(ec_path)
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date'] = pd.to_datetime(df['exit_date'])
        df['strategy'] = 'EXPIRY_CONV'
        strategies['EXPIRY_CONV'] = df
    
    # Momentum RSI
    mom_path = '.tmp/momentum_rsi_backtest.csv'
    if os.path.exists(mom_path):
        df = pd.read_csv(mom_path)
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        df['exit_date'] = pd.to_datetime(df['exit_date'])
        df['strategy'] = 'MOMENTUM_RSI'
        strategies['MOMENTUM_RSI'] = df
    
    return strategies


def compute_strategy_stats(trades_df, name):
    """Compute comprehensive stats for a strategy."""
    n = len(trades_df)
    if n == 0:
        return None
    
    wins = trades_df[trades_df['pnl_pct'] > 0]
    losses = trades_df[trades_df['pnl_pct'] <= 0]
    
    wr = len(wins) / n
    avg_win = wins['pnl_pct'].mean() if len(wins) > 0 else 0
    avg_loss = abs(losses['pnl_pct'].mean()) if len(losses) > 0 else 0
    avg_ret = trades_df['pnl_pct'].mean()
    
    # Kelly
    kf, kh, edge = kelly_fraction(wr, avg_win / 100, avg_loss / 100) if avg_loss > 0 else (0, 0, avg_ret / 100)
    
    # Duration
    date_range = (trades_df['exit_date'].max() - trades_df['entry_date'].min()).days
    years = max(date_range / 365.25, 0.5)
    trades_per_year = n / years
    
    return {
        'name': name,
        'trades': n,
        'wr': wr,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'avg_ret': avg_ret,
        'kelly_full': kf,
        'kelly_half': kh,
        'edge': edge,
        'trades_per_year': trades_per_year,
        'years': years,
        'total_pnl': trades_df['pnl_rupees'].sum() if 'pnl_rupees' in trades_df.columns else 0,
    }


def simulate_portfolio(strategies, initial_capital=1000000, kelly_mode='half',
                       max_per_strategy=0.40, max_total=0.70):
    """
    Simulate portfolio with Kelly sizing and compounding across all strategies.
    
    Key: capital grows as trades profit, allowing compounding.
    """
    print(f"\n{'═'*80}")
    print(f"  PORTFOLIO SIMULATION — Kelly Compounding Engine")
    print(f"  Initial Capital: ₹{initial_capital:,.0f}")
    print(f"  Kelly Mode: {kelly_mode} | Max/Strategy: {max_per_strategy:.0%} | Max Total: {max_total:.0%}")
    print(f"{'═'*80}")
    
    # Combine all trades, sort by exit date
    all_trades = []
    strategy_stats = {}
    
    for name, df in strategies.items():
        stats = compute_strategy_stats(df, name)
        if stats:
            strategy_stats[name] = stats
            
            print(f"\n  Strategy: {name}")
            print(f"    Trades: {stats['trades']} | WR: {stats['wr']:.1%} | Avg: {stats['avg_ret']:+.3f}%")
            print(f"    Kelly: {stats['kelly_half']:.1%} (half) | Edge: {stats['edge']*100:.3f}%/trade")
            print(f"    Trades/Year: {stats['trades_per_year']:.1f}")
        
        for _, row in df.iterrows():
            all_trades.append({
                'strategy': name,
                'entry_date': row['entry_date'],
                'exit_date': row['exit_date'],
                'pnl_pct': row['pnl_pct'],
                'pnl_rupees': row.get('pnl_rupees', 0),
                'symbol': row.get('symbol', ''),
            })
    
    if not all_trades:
        print("  No trades to simulate.")
        return
    
    trades_df = pd.DataFrame(all_trades).sort_values('exit_date').reset_index(drop=True)
    
    # Simulate with compounding
    capital = initial_capital
    peak_capital = initial_capital
    max_drawdown = 0
    
    equity_curve = [{'date': trades_df['entry_date'].min(), 'capital': capital}]
    monthly_pnl = {}
    strategy_pnl = {name: 0 for name in strategies.keys()}
    
    for _, trade in trades_df.iterrows():
        strat = trade['strategy']
        stats = strategy_stats.get(strat)
        if not stats:
            continue
        
        # Kelly-sized position
        kelly_pct = stats['kelly_half'] if kelly_mode == 'half' else stats['kelly_full']
        kelly_pct = min(kelly_pct, max_per_strategy)  # Cap per strategy
        
        # Position size
        position_size = capital * kelly_pct
        
        # P&L
        pnl = position_size * (trade['pnl_pct'] / 100)
        capital += pnl
        
        # Track
        peak_capital = max(peak_capital, capital)
        drawdown = (peak_capital - capital) / peak_capital
        max_drawdown = max(max_drawdown, drawdown)
        
        strategy_pnl[strat] = strategy_pnl.get(strat, 0) + pnl
        
        month_key = trade['exit_date'].strftime('%Y-%m')
        if month_key not in monthly_pnl:
            monthly_pnl[month_key] = 0
        monthly_pnl[month_key] += pnl
        
        equity_curve.append({
            'date': trade['exit_date'],
            'capital': capital
        })
    
    # Final stats
    total_return = (capital - initial_capital) / initial_capital
    date_range = (trades_df['exit_date'].max() - trades_df['entry_date'].min()).days
    years = date_range / 365.25
    
    if years > 0 and capital > 0:
        cagr = (capital / initial_capital) ** (1 / years) - 1
    else:
        cagr = 0
    
    # Sharpe approximation
    monthly_returns = pd.Series(monthly_pnl)
    if len(monthly_returns) > 1:
        monthly_std = monthly_returns.std()
        monthly_mean = monthly_returns.mean()
        sharpe = (monthly_mean / monthly_std) * np.sqrt(12) if monthly_std > 0 else 0
    else:
        sharpe = 0
    
    print(f"\n{'─'*80}")
    print(f"  PORTFOLIO RESULTS")
    print(f"{'─'*80}")
    print(f"  Initial Capital:  ₹{initial_capital:,.0f}")
    print(f"  Final Capital:    ₹{capital:,.0f}")
    print(f"  Total Return:     {total_return:.1%}")
    print(f"  Period:           {years:.1f} years ({date_range} days)")
    print(f"  CAGR:             {cagr:.1%}")
    print(f"  Max Drawdown:     {max_drawdown:.1%}")
    print(f"  Sharpe Ratio:     {sharpe:.2f}")
    print(f"  Total Trades:     {len(trades_df)}")
    print(f"  Trades/Year:      {len(trades_df) / max(years, 0.5):.1f}")
    
    print(f"\n  P&L by Strategy:")
    for strat, pnl in sorted(strategy_pnl.items(), key=lambda x: -x[1]):
        pct = pnl / initial_capital * 100
        print(f"    {strat:<20} ₹{pnl:>+12,.0f}  ({pct:+.1f}%)")
    
    print(f"\n  Monthly Equity (last 12):")
    for month in sorted(monthly_pnl.keys())[-12:]:
        pnl = monthly_pnl[month]
        bar = "█" * max(1, int(abs(pnl) / 5000))
        color = "+" if pnl > 0 else ""
        print(f"    {month}: ₹{color}{pnl:>+10,.0f} {bar}")
    
    # ── What-if: Optimal parameters ────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  WHAT-IF ANALYSIS — Scaling to 66% CAGR")
    print(f"{'─'*80}")
    
    current_tpy = len(trades_df) / max(years, 0.5)
    current_avg = trades_df['pnl_pct'].mean()
    
    # What trade frequency + avg return combos give 66%?
    print(f"\n  Current: {current_tpy:.0f} trades/yr × {current_avg:+.3f}% → {cagr:.1%} CAGR")
    print(f"\n  To reach 66% CAGR with Kelly compounding:")
    
    for tpy in [50, 75, 100, 150, 200]:
        needed_avg = (1.66 ** (1/tpy) - 1) * 100  # Solve: (1+r)^n = 1.66
        print(f"    {tpy:>4} trades/yr × {needed_avg:+.4f}% avg return per trade")
    
    # Strategy-specific improvements needed
    print(f"\n  Lever 1 — More Trades:")
    print(f"    Current: {current_tpy:.0f}/yr")
    print(f"    Target:  100+/yr")
    print(f"    Gap:     Add ~{max(0, 100 - current_tpy):.0f} more trades/yr")
    print(f"    How:     More symbols, tighter RSI thresholds (30/70), shorter time stops")
    
    print(f"\n  Lever 2 — Bigger Edge:")
    print(f"    Current avg: {current_avg:+.3f}%")
    print(f"    Needed at 100 tpy: +0.50%")
    print(f"    How:     Filter for only the highest-edge setups, increase Kelly fraction")
    
    print(f"\n  Lever 3 — Full Kelly (vs Half):")
    print(f"    Half-Kelly CAGR:  {cagr:.1%}")
    
    # Resimulate with full Kelly
    capital_full = initial_capital
    for _, trade in trades_df.iterrows():
        strat = trade['strategy']
        stats = strategy_stats.get(strat)
        if not stats:
            continue
        kelly_pct = min(stats['kelly_full'], max_per_strategy)
        position_size = capital_full * kelly_pct
        pnl = position_size * (trade['pnl_pct'] / 100)
        capital_full += pnl
    
    cagr_full = (capital_full / initial_capital) ** (1 / max(years, 0.5)) - 1 if capital_full > 0 else 0
    print(f"    Full-Kelly CAGR:  {cagr_full:.1%}")
    
    print(f"\n{'═'*80}")
    
    # Save equity curve
    eq = pd.DataFrame(equity_curve)
    eq.to_csv('.tmp/portfolio_equity_curve.csv', index=False)
    print(f"  Equity curve saved to .tmp/portfolio_equity_curve.csv")
    
    return {
        'cagr': cagr,
        'cagr_full_kelly': cagr_full,
        'total_return': total_return,
        'max_drawdown': max_drawdown,
        'sharpe': sharpe,
        'final_capital': capital,
        'trades': len(trades_df),
    }


def main():
    parser = argparse.ArgumentParser(description='Portfolio Backtester')
    parser.add_argument('--capital', type=float, default=1000000, help='Initial capital ₹')
    parser.add_argument('--kelly', choices=['full', 'half'], default='half')
    args = parser.parse_args()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY v3 — PORTFOLIO BACKTESTER".ljust(79) + "║")
    print(f"║  Combined Multi-Strategy Kelly Compounding".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    strategies = load_trade_logs()
    
    if not strategies:
        print("\n  No strategy backtests found. Run individual backtests first:")
        print("    python3 execution/scan_expiry_convergence.py --backtest")
        print("    python3 execution/momentum_rsi_strategy.py --backtest")
        return
    
    result = simulate_portfolio(strategies, args.capital, args.kelly)


if __name__ == "__main__":
    main()
