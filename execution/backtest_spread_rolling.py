import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from shoonya_client import ShoonyaClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_expiry(x):
    try:
         return datetime.strptime(x['exd'], '%d-%b-%Y')
    except:
         return datetime.max

def get_futures(api, exchange, symbol):
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)
    if not ret or 'values' not in ret:
        return []
    
    futures = [x for x in ret['values'] if x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX']
    futures.sort(key=parse_expiry)
    return futures

def fetch_history(api, exchange, token, days, interval):
    end_time = datetime.now()
    start_time = end_time - pd.Timedelta(days=days)
    
    start_time_epoch = start_time.timestamp()
    end_time_epoch = end_time.timestamp()
    
    data = api.get_time_price_series(exchange=exchange, token=token, starttime=start_time_epoch, endtime=end_time_epoch, interval=interval)
    
    if not data:
        return pd.DataFrame()
        
    df = pd.DataFrame(data)
    
    # Process Columns
    # Prioritize 'ssboe' (Epoch)
    if 'ssboe' in df.columns:
        df.rename(columns={'ssboe': 'Time'}, inplace=True)
    elif 'time' in df.columns:
        df.rename(columns={'time': 'Time'}, inplace=True)

    rename_map = {
        'into': 'Open', 'inth': 'High', 'intl': 'Low', 'intc': 'Close', 'intv': 'Volume'
    }
    df.rename(columns=rename_map, inplace=True)
    
    numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
            
    if 'Time' in df.columns:
         try:
             df['Time'] = pd.to_numeric(df['Time'], errors='raise')
             df['Time'] = pd.to_datetime(df['Time'], unit='s')
         except:
             df['Time'] = pd.to_datetime(df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
    
    return df[['Time', 'Close']]

def backtest(symbol, exchange, days, interval):
    client = ShoonyaClient()
    api = client.login()
    if not api:
        return

    # 1. Get Futures
    futures = get_futures(api, exchange, symbol)
    if len(futures) < 2:
        logger.error("Need at least 2 active futures contracts.")
        return
        
    near_fut = futures[0]
    far_fut = futures[1]
    
    logger.info(f"Backtesting Spread: BUY {far_fut['tsym']} / SELL {near_fut['tsym']}")
    logger.info(f"Near Expiry: {near_fut['exd']} | Far Expiry: {far_fut['exd']}")

    # 2. Fetch Data
    logger.info(f"Fetching {days} days of history...")
    df_near = fetch_history(api, exchange, near_fut['token'], days, interval)
    df_far = fetch_history(api, exchange, far_fut['token'], days, interval)

    if df_near.empty or df_far.empty:
        logger.error("Insufficient data.")
        return

    # 3. Merge Data
    df = pd.merge_asof(
        df_near.sort_values('Time'), 
        df_far.sort_values('Time'), 
        on='Time', 
        suffixes=('_Near', '_Far'),
        direction='nearest',
        tolerance=pd.Timedelta('10min') # Allow slight time mismatch
    )
    
    df.dropna(inplace=True)
    
    # 4. Calculate Spread
    # Spread = Far - Near
    df['Spread'] = df['Close_Far'] - df['Close_Near']
    
    # 5. Stats
    max_negative_spread = df['Spread'].min()
    mean_spread = df['Spread'].mean()
    std_spread = df['Spread'].std()
    
    print(f"\n--- Backtest Results ({symbol}) ---")
    print(f"Data Points: {len(df)}")
    print(f"Max Negative Difference (Min Spread): {max_negative_spread:.2f}")
    print(f"Mean Spread: {mean_spread:.2f}")
    print(f"Std Dev: {std_spread:.2f}")
    
    # 6. Simulate Reversion
    # Strategy: Buy Spread (Enter) when Spread <= Threshold (e.g. Mean - 2*StdDev)
    # Exit when Spread >= Mean
    
    # We want to test "Reversion Win Rate" from deep discounts.
    # Let's define "Deep Discount" as Min Spread (Max Negative).
    # Does it revert?
    
    print("\n--- Reversion Analysis ---")
    # Define Entry Thresholds
    thresholds = [mean_spread - std_spread, mean_spread - 2*std_spread, max_negative_spread * 0.9] # various levels
    
    for thresh in thresholds:
        entries = df[df['Spread'] <= thresh]
        if entries.empty:
            continue
            
        wins = 0
        total_trades = 0
        
        # Simple Loop for Trade Simulation
        in_trade = False
        entry_price = 0
        
        for index, row in df.iterrows():
            if not in_trade:
                if row['Spread'] <= thresh:
                    in_trade = True
                    entry_price = row['Spread']
                    # print(f"Entry at {entry_price:.2f} on {row['Time']}")
            else:
                # Exit condition: Revert to Mean OR Expiry (End of Data)
                if row['Spread'] >= mean_spread:
                    in_trade = False
                    exit_price = row['Spread']
                    profit = exit_price - entry_price
                    if profit > 0:
                        wins += 1
                    total_trades += 1
                    # print(f"Exit at {exit_price:.2f} on {row['Time']} | PnL: {profit:.2f}")
        
        if in_trade: # Close open trade at last price
             exit_price = df.iloc[-1]['Spread']
             if exit_price > entry_price:
                 wins += 1
             total_trades += 1
        
        if total_trades > 0:
            win_rate = (wins / total_trades) * 100
            print(f"Entry Threshold: {thresh:.2f} | Trades: {total_trades} | Win Rate: {win_rate:.1f}%")

    # Save CSV
    output_file = f".tmp/backtest_{symbol}.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Detailed data saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backtest Calendar Spread')
    parser.add_argument('--symbol', required=True, help='Trading Symbol (e.g., KFINTECH)')
    parser.add_argument('--exchange', default='NFO', help='Exchange (NFO)')
    parser.add_argument('--days', type=int, default=60, help='Days of history')
    parser.add_argument('--interval', default='60', help='Interval (60 or DAY)') # Using 60min as default
    
    args = parser.parse_args()
    
    backtest(args.symbol, args.exchange, args.days, args.interval)
