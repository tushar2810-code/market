import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from shoonya_client import ShoonyaClient
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_data(symbol, exchange, days, interval):
    client = ShoonyaClient()
    api = client.login()
    
    if not api:
        logger.error("Authentication failed. Cannot fetch data.")
        return

    # Calculate start and end time
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    # Format times as required by Shoonya API (epoch or specific format, usually 'dd-mm-yyyy HH:MM:SS')
    # Use standard format for get_time_price_series if required, typically epoch for some calls or string for others.
    # NorenApi Python wrapper usually expects time in seconds (epoch) or string depending on version.
    # Checking common usage: starttime and endtime are epoch timestamps.
    
    start_time_epoch = start_time.timestamp()
    end_time_epoch = end_time.timestamp()

    # Need to find the token for the symbol
    # This is a bit tricky without a master contract. 
    # For now, we search for the script.
    
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)
    if not ret or 'values' not in ret:
        logger.error(f"Symbol {symbol} not found in {exchange}")
        return

    # Pick the first match or handle ambiguity
    # Usually we want the equity symbol or specific contract
    script_info = ret['values'][0]
    token = script_info['token']
    tsym = script_info['tsym']
    logger.info(f"Found {tsym} with token {token}")

    # Fetch data
    # interval: '1', '5', '15', '30', '60'
    
    logger.info(f"Fetching data for {tsym} from {start_time} to {end_time}...")
    
    # Note: get_time_price_series arguments: exchange, token, starttime, endtime, interval
    # Time must be in epoch strictly for some versions, or string 'dd-mm-yyyy HH:MM:SS' for others.
    # We will try the epoch approach first as per recent NorenApi standards.
    
    # Shoonya API: 'd' interval is unreliable. Fetch 1-minute data and aggregate.
    
    start_time_epoch = int(start_time.timestamp())
    end_time_epoch = int(end_time.timestamp())
    
    # Fetch 1-minute data
    ret = api.get_time_price_series(exchange=exchange, token=token, starttime=start_time_epoch, endtime=end_time_epoch, interval='1')
    
    if not ret:
        logger.error("No data returned from API.")
        return

    # Process and Aggregate
    df_intra = pd.DataFrame(ret)
    
    # Map columns
    df_intra.rename(columns={
        'ssboe': 'Time', 'into': 'Open', 'inth': 'High', 'intl': 'Low', 'intc': 'Close', 'intv': 'Volume', 'oi': 'OI'
    }, inplace=True)
    
    # Convert types
    cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'OI']
    for c in cols:
        if c in df_intra.columns:
            df_intra[c] = pd.to_numeric(df_intra[c])
    
    df_intra['Time'] = pd.to_numeric(df_intra['Time'])
    df_intra['Datetime'] = pd.to_datetime(df_intra['Time'], unit='s')
    df_intra.set_index('Datetime', inplace=True)
    
    # Resample to Daily
    agg_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'last'
    }
    
    # Convert interval to pandas frequency
    # Assuming user wants Daily if they run this script essentially?
    # Original script took 'interval' arg. If '1', return raw. If 'd', return agg.
    # If interval is '1' or 'minute', return raw.
    pd_interval = 'D'
    if interval not in ['1', '5', '15', '30', '60']:
        pd_interval = 'D'
        data = None # trigger agg
    else:
        # Just return raw
        data = ret # Use original list
    
    if pd_interval == 'D':
         df = df_intra.resample('D').agg(agg_dict).dropna()
         df.reset_index(inplace=True)
         # Rename for consistency
         df.rename(columns={'Datetime': 'Time'}, inplace=True)
    else:
         df = df_intra.reset_index()
         df.rename(columns={'Datetime': 'Time'}, inplace=True)


    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Organize columns (typical response has 'ssOE', 'into', 'intc', etc. mapping to OHLC)
    # Mapping based on standard NorenApi response:
    # time -> ssOE (Epoch), into -> Open, inth -> High, intl -> Low, intc -> Close, intv -> Volume
    # NOTE: Check actual keys returned. Usually: 'ssboe', 'into', 'inth', 'intl', 'intc', 'intv'
    
    # Rename columns if they match expectations
    # Prioritize 'ssboe' (Epoch)
    if 'ssboe' in df.columns:
        df.rename(columns={'ssboe': 'Time'}, inplace=True)
    elif 'time' in df.columns:
        df.rename(columns={'time': 'Time'}, inplace=True)

    rename_map = {
        'into': 'Open', 'inth': 'High', 'intl': 'Low', 'intc': 'Close', 'intv': 'Volume'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # Convert types
    numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
            
    if 'Time' in df.columns:
         # Convert to datetime, handling both epoch and string formats safely
         # If it looks like numeric (epoch), convert unit='s'
         # If string, parse format.
         
         # Check type of first element to decide strategy or use safe approach
         try:
             df['Time'] = pd.to_numeric(df['Time'], errors='raise')
             df['Time'] = pd.to_datetime(df['Time'], unit='s')
         except:
             df['Time'] = pd.to_datetime(df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

    # Save to .tmp
    output_dir = ".tmp"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    filename = f"{output_dir}/shoonya_{tsym}_{interval}min.csv"
    df.to_csv(filename, index=False)
    logger.info(f"Data saved to {filename}")
    print(df.head())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch historical data from Shoonya.')
    parser.add_argument('--symbol', required=True, help='Trading Symbol (e.g., NIFTY, RELIANCE)')
    parser.add_argument('--exchange', default='NSE', help='Exchange (NSE, NFO, CDS)')
    parser.add_argument('--days', type=int, default=30, help='Number of days of history')
    parser.add_argument('--interval', default='1', help='Candle interval in minutes (1, 5, 10, 15, 30, 60, DAY)')
    
    args = parser.parse_args()
    
    fetch_data(args.symbol, args.exchange, args.days, args.interval)
