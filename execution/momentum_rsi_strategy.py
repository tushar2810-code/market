"""
Momentum + RSI Mean Reversion — Strategy Layer 5.

Key Insight: Indian FNO stocks have Hurst > 0.5 (trending). Instead of 
fighting trends (pair trading), we RIDE them.

Strategy:
  1. RSI(14) < 25 on a stock → BUY futures (oversold bounce)
  2. RSI(14) > 75 on a stock → SELL futures (overbought fade)
  3. Sector momentum filter: only BUY from top-3 momentum sectors, 
     only SELL from bottom-3 sectors
  4. Exit: RSI crosses back to 50 OR time-stop (10 days)

Usage:
    python3 execution/momentum_rsi_strategy.py --backtest
    python3 execution/momentum_rsi_strategy.py --scan
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
from datetime import datetime
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))

DATA_DIR = '.tmp/3y_data'

SECTORS = {
    'PHARMA': ['SUNPHARMA', 'CIPLA', 'DRREDDY', 'LUPIN', 'AUROPHARMA', 'DIVISLAB', 'TORNTPHARM', 'ALKEM', 'ZYDUSLIFE'],
    'BANKING': ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK', 'SBIN', 'INDUSINDBK', 'BANKBARODA', 'PNB', 'FEDERALBNK'],
    'IT': ['TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'LTIM', 'MPHASIS', 'COFORGE', 'PERSISTENT'],
    'METALS': ['TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'SAIL', 'NMDC', 'JINDALSTEL', 'COALINDIA'],
    'POWER': ['NTPC', 'POWERGRID', 'TATAPOWER', 'NHPC', 'PFC', 'RECLTD', 'IREDA', 'IRFC'],
    'AUTO': ['MARUTI', 'M&M', 'BAJAJ-AUTO', 'HEROMOTOCO', 'TVSMOTOR', 'EICHERMOT', 'ASHOKLEY'],
    'FMCG': ['HINDUNILVR', 'ITC', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL', 'BRITANNIA', 'TATACONSUM'],
    'CEMENT': ['ULTRACEMCO', 'AMBUJACEM', 'SHREECEM', 'DALBHARAT', 'GRASIM'],
    'OIL': ['RELIANCE', 'ONGC', 'BPCL', 'IOC', 'HINDPETRO', 'GAIL'],
    'INFRA': ['LT', 'DLF', 'OBEROIRLTY', 'GODREJPROP', 'PRESTIGE', 'LODHA'],
}


def load_prices(symbol):
    """Load continuous futures prices."""
    path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        df['FH_CLOSING_PRICE'] = pd.to_numeric(df['FH_CLOSING_PRICE'], errors='coerce')
        df = df.dropna(subset=['FH_TIMESTAMP', 'FH_CLOSING_PRICE']).sort_values('FH_TIMESTAMP')
        
        if 'FH_EXPIRY_DT' in df.columns:
            df['exp'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
            if 'FH_INSTRUMENT' in df.columns:
                df = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])]
            df = df.loc[df.groupby('FH_TIMESTAMP')['exp'].idxmin()]
        
        lot_size = int(df['FH_MARKET_LOT'].iloc[-1]) if 'FH_MARKET_LOT' in df.columns else 1
        return df.set_index('FH_TIMESTAMP')['FH_CLOSING_PRICE'], lot_size
    except:
        return None


def compute_rsi(series, period=14):
    """Calculate RSI."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    
    # Use EMA after initial SMA
    for i in range(period, len(avg_gain)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def sector_momentum(all_prices, date, lookback=20):
    """Calculate sector momentum ranking at a given date."""
    sector_returns = {}
    
    for sector_name, symbols in SECTORS.items():
        returns = []
        for sym in symbols:
            if sym in all_prices:
                prices = all_prices[sym]
                idx = prices.index.get_indexer([date], method='ffill')
                if idx[0] < lookback or idx[0] < 0:
                    continue
                current = prices.iloc[idx[0]]
                past = prices.iloc[idx[0] - lookback]
                if past > 0:
                    returns.append((current - past) / past)
        
        if returns:
            sector_returns[sector_name] = np.mean(returns)
    
    # Rank sectors
    sorted_sectors = sorted(sector_returns.items(), key=lambda x: -x[1])
    return sorted_sectors


def backtest_momentum_rsi(rsi_buy=25, rsi_sell=75, rsi_exit=50, time_stop=10, 
                          use_sector_filter=True, max_concurrent=5):
    """
    Backtest the momentum RSI strategy across all FNO stocks.
    """
    print(f"\n{'═'*80}")
    print(f"  MOMENTUM + RSI STRATEGY — BACKTEST")
    print(f"  RSI Buy < {rsi_buy} | RSI Sell > {rsi_sell} | Exit @ {rsi_exit} | Time Stop {time_stop}d")
    print(f"  Sector filter: {'ON' if use_sector_filter else 'OFF'} | Max concurrent: {max_concurrent}")
    print(f"{'═'*80}")
    
    # Load all data
    all_prices = {}
    all_lots = {}
    all_rsi = {}
    
    sym_to_sector = {}
    for sector, syms in SECTORS.items():
        for s in syms:
            sym_to_sector[s] = sector
    
    all_symbols = list(sym_to_sector.keys())
    
    for sym in all_symbols:
        result = load_prices(sym)
        if result is not None:
            prices, lot = result
            if len(prices) > 30:
                all_prices[sym] = prices
                all_lots[sym] = lot
                all_rsi[sym] = compute_rsi(prices, 14)
    
    print(f"  Loaded {len(all_prices)} symbols")
    
    # Get common date range
    all_dates = set()
    for p in all_prices.values():
        all_dates.update(p.index)
    all_dates = sorted(all_dates)
    
    if len(all_dates) < 100:
        print("  Insufficient data")
        return
    
    # Simulate
    trades = []
    open_positions = []  # list of (sym, direction, entry_date, entry_price, lot_size)
    
    for i, date in enumerate(all_dates[30:], 30):  # Skip initial period for RSI warmup
        # Check exits first
        positions_to_close = []
        for pos_idx, (sym, direction, entry_date, entry_price, lot_size) in enumerate(open_positions):
            if sym not in all_rsi or date not in all_rsi[sym].index:
                continue
            
            current_rsi = all_rsi[sym].get(date, 50)
            current_price = all_prices[sym].get(date, entry_price)
            days_held = (date - entry_date).days
            
            should_exit = False
            reason = ""
            
            if direction == 'LONG':
                if current_rsi >= rsi_exit:
                    should_exit = True
                    reason = "RSI Target"
                elif days_held >= time_stop:
                    should_exit = True
                    reason = "Time Stop"
            elif direction == 'SHORT':
                if current_rsi <= rsi_exit:
                    should_exit = True
                    reason = "RSI Target"
                elif days_held >= time_stop:
                    should_exit = True
                    reason = "Time Stop"
            
            if should_exit:
                if direction == 'LONG':
                    pnl_pct = (current_price - entry_price) / entry_price * 100
                else:
                    pnl_pct = (entry_price - current_price) / entry_price * 100
                
                pnl_rupees = (pnl_pct / 100) * entry_price * lot_size
                
                trades.append({
                    'symbol': sym,
                    'sector': sym_to_sector.get(sym, 'OTHER'),
                    'direction': direction,
                    'entry_date': entry_date.date(),
                    'exit_date': date.date(),
                    'entry_price': round(entry_price, 2),
                    'exit_price': round(current_price, 2),
                    'pnl_pct': round(pnl_pct, 3),
                    'pnl_rupees': round(pnl_rupees, 0),
                    'days_held': days_held,
                    'reason': reason,
                    'lot_size': lot_size
                })
                positions_to_close.append(pos_idx)
        
        # Remove closed positions
        for idx in sorted(positions_to_close, reverse=True):
            open_positions.pop(idx)
        
        # Check entries (skip if max concurrent reached)
        if len(open_positions) >= max_concurrent:
            continue
        
        # Sector momentum (recalculate weekly)
        if use_sector_filter and i % 5 == 0:
            sector_rank = sector_momentum(all_prices, date, 20)
            if sector_rank:
                top_sectors = set(s for s, _ in sector_rank[:3])
                bottom_sectors = set(s for s, _ in sector_rank[-3:])
            else:
                top_sectors = set(SECTORS.keys())
                bottom_sectors = set(SECTORS.keys())
        elif not use_sector_filter:
            top_sectors = set(SECTORS.keys())
            bottom_sectors = set(SECTORS.keys())
        
        # Scan for RSI extremes
        open_syms = set(s for s, _, _, _, _ in open_positions)
        
        for sym in all_symbols:
            if sym in open_syms or sym not in all_rsi:
                continue
            if date not in all_rsi[sym].index:
                continue
            if len(open_positions) >= max_concurrent:
                break
            
            rsi_val = all_rsi[sym].get(date, 50)
            sector = sym_to_sector.get(sym, 'OTHER')
            
            if np.isnan(rsi_val):
                continue
            
            # BUY signal: RSI < threshold + top sector
            if rsi_val < rsi_buy and sector in top_sectors:
                entry_price = all_prices[sym].get(date, None)
                if entry_price and entry_price > 0:
                    lot_size = all_lots.get(sym, 1)
                    open_positions.append((sym, 'LONG', date, entry_price, lot_size))
            
            # SELL signal: RSI > threshold + bottom sector  
            elif rsi_val > rsi_sell and sector in bottom_sectors:
                entry_price = all_prices[sym].get(date, None)
                if entry_price and entry_price > 0:
                    lot_size = all_lots.get(sym, 1)
                    open_positions.append((sym, 'SHORT', date, entry_price, lot_size))
    
    if not trades:
        print("  No trades generated")
        return
    
    # Results
    df_trades = pd.DataFrame(trades)
    wins = df_trades[df_trades['pnl_pct'] > 0]
    losses = df_trades[df_trades['pnl_pct'] <= 0]
    
    wr = len(wins) / len(df_trades) * 100
    avg_ret = df_trades['pnl_pct'].mean()
    avg_win = wins['pnl_pct'].mean() if len(wins) > 0 else 0
    avg_loss = losses['pnl_pct'].mean() if len(losses) > 0 else 0
    total_pnl = df_trades['pnl_rupees'].sum()
    
    print(f"\n  Total Trades:   {len(df_trades)}")
    print(f"  Win Rate:       {wr:.1f}%")
    print(f"  Avg Return:     {avg_ret:+.3f}%")
    print(f"  Avg Win:        {avg_win:+.3f}%")
    print(f"  Avg Loss:       {avg_loss:+.3f}%")
    print(f"  Max DD:         {df_trades['pnl_pct'].min():+.3f}%")
    print(f"  Total P&L:      ₹{total_pnl:,.0f}")
    print(f"  Avg Days Held:  {df_trades['days_held'].mean():.1f}")
    
    # By direction
    for direction in ['LONG', 'SHORT']:
        sub = df_trades[df_trades['direction'] == direction]
        if len(sub) > 0:
            sub_wr = (sub['pnl_pct'] > 0).mean() * 100
            print(f"\n  {direction}: {len(sub)} trades, {sub_wr:.1f}% WR, {sub['pnl_pct'].mean():+.3f}% avg")
    
    # Monthly
    df_trades['month'] = pd.to_datetime(df_trades['exit_date']).dt.to_period('M')
    monthly = df_trades.groupby('month').agg(
        trades=('pnl_pct', 'count'),
        pnl=('pnl_rupees', 'sum'),
        avg_ret=('pnl_pct', 'mean')
    )
    
    print(f"\n  Monthly Performance (last 12):")
    for month, row in monthly.tail(12).iterrows():
        bar = "█" * max(1, int(abs(row['pnl']) / 10000))
        sign = "+" if row['pnl'] > 0 else ""
        print(f"    {month}: {row['trades']:>3} trades  ₹{sign}{row['pnl']:>10,.0f}  avg: {row['avg_ret']:+.2f}%")
    
    # Kelly
    if wr > 50 and avg_win > 0 and avg_loss < 0:
        from kelly_sizer import kelly_fraction
        kf, kh, edge = kelly_fraction(wr / 100, avg_win / 100, abs(avg_loss) / 100)
        print(f"\n  Kelly Sizing:")
        print(f"    Edge/trade:  {edge * 100:.3f}%")
        print(f"    Full Kelly:  {kf:.1%}")
        print(f"    Half Kelly:  {kh:.1%}")
    
    # CAGR estimate
    date_range = (pd.to_datetime(df_trades['exit_date'].max()) - pd.to_datetime(df_trades['entry_date'].min())).days
    years = date_range / 365.25
    if years > 0:
        # Simple return sum → annualized
        trades_per_year = len(df_trades) / years
        compound = (1 + avg_ret / 100) ** trades_per_year - 1
        print(f"\n  CAGR Estimate:")
        print(f"    Trades/Year:     {trades_per_year:.1f}")
        print(f"    Avg Ret/Trade:   {avg_ret:+.3f}%")
        print(f"    Compound CAGR:   {compound * 100:.1f}%")
    
    # Save
    df_trades.to_csv('.tmp/momentum_rsi_backtest.csv', index=False)
    print(f"\n  Results saved to .tmp/momentum_rsi_backtest.csv")
    
    print(f"\n{'═'*80}")
    return df_trades


def scan_live():
    """Scan for current RSI extremes."""
    print(f"\n{'═'*80}")
    print(f"  LIVE RSI SCAN")
    print(f"{'═'*80}")
    
    signals = []
    for sector, symbols in SECTORS.items():
        for sym in symbols:
            result = load_prices(sym)
            if result is None:
                continue
            prices, lot = result
            rsi = compute_rsi(prices, 14)
            if len(rsi) < 2:
                continue
            
            current_rsi = rsi.iloc[-1]
            current_price = prices.iloc[-1]
            
            if np.isnan(current_rsi):
                continue
            
            if current_rsi < 25:
                signals.append({
                    'symbol': sym, 'sector': sector, 'rsi': round(current_rsi, 1),
                    'price': round(current_price, 2), 'lot': lot,
                    'direction': 'BUY', 'notional': round(current_price * lot, 0)
                })
            elif current_rsi > 75:
                signals.append({
                    'symbol': sym, 'sector': sector, 'rsi': round(current_rsi, 1),
                    'price': round(current_price, 2), 'lot': lot,
                    'direction': 'SELL', 'notional': round(current_price * lot, 0)
                })
    
    if signals:
        df = pd.DataFrame(signals).sort_values('rsi')
        print(f"\n  {'Symbol':<15} {'Sector':<10} {'RSI':>5} {'Price':>10} {'Lot':>5} {'Action':<6} {'Notional':>10}")
        print(f"  {'─'*70}")
        for _, r in df.iterrows():
            icon = "🟢" if r['direction'] == 'BUY' else "🔴"
            print(f"  {icon} {r['symbol']:<13} {r['sector']:<10} {r['rsi']:>5.1f} {r['price']:>10.1f} {r['lot']:>5} {r['direction']:<6} ₹{r['notional']:>9,.0f}")
    else:
        print("  No RSI extremes found")
    
    print(f"\n{'═'*80}")
    return signals


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Momentum RSI Strategy')
    parser.add_argument('--backtest', action='store_true', help='Run backtest')
    parser.add_argument('--scan', action='store_true', help='Live RSI scan')
    parser.add_argument('--rsi-buy', type=int, default=25, help='RSI buy threshold')
    parser.add_argument('--rsi-sell', type=int, default=75, help='RSI sell threshold')
    parser.add_argument('--time-stop', type=int, default=10, help='Time stop in days')
    parser.add_argument('--max-concurrent', type=int, default=5, help='Max concurrent positions')
    parser.add_argument('--no-sector-filter', action='store_true', help='Disable sector filter')
    args = parser.parse_args()
    
    print("╔" + "═" * 78 + "╗")
    print(f"║  MOMENTUM + RSI STRATEGY — Layer 5".ljust(79) + "║")
    print(f"║  Antigravity v3".ljust(79) + "║")
    print(f"║  {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")
    
    if args.backtest:
        backtest_momentum_rsi(
            rsi_buy=args.rsi_buy, rsi_sell=args.rsi_sell,
            time_stop=args.time_stop, max_concurrent=args.max_concurrent,
            use_sector_filter=not args.no_sector_filter
        )
    elif args.scan:
        scan_live()
    else:
        scan_live()
