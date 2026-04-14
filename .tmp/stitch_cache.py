import pandas as pd
import os
import glob
from sys import stdout

OUT_DIR = '.tmp/5y_data'
CACHE_DIR = '.tmp/bhav_cache'

cache_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))
print(f"Loading {len(cache_files)} daily files from cache...")

all_recent_data = []

# 1. First, consolidate all daily files into one big dataframe of recent data
for cf in cache_files:
    try:
        df = pd.read_csv(cf)
        
        # Format nsefin columns into our legacy structure
        df['FH_INSTRUMENT'] = df['category'].map({
            'STO': 'OPTSTK', 
            'IDO': 'OPTIDX', 
            'STF': 'FUTSTK', 
            'IDF': 'FUTIDX'
        })
        
        rename_map = {
            'symbol': 'FH_SYMBOL',
            'expiry': 'FH_EXPIRY_DT',
            'strike': 'FH_STRIKE_PRICE',
            'right': 'FH_OPTION_TYPE',
            'open': 'FH_OPENING_PRICE',
            'high': 'FH_TRADE_HIGH_PRICE',
            'low': 'FH_TRADE_LOW_PRICE',
            'close': 'FH_CLOSING_PRICE',
            'last': 'FH_LAST_TRADED_PRICE', 
            'prv_close': 'FH_PREV_CLS',
            'volume': 'FH_TOT_TRADED_QTY',
            'trade_value': 'FH_TOT_TRADED_VAL',
            'oi': 'FH_OPEN_INT',
            'coi': 'FH_CHANGE_IN_OI',
            'date': 'FH_TIMESTAMP',
            'lot_size': 'FH_MARKET_LOT'
        }
        df = df.rename(columns=rename_map)
        df['FH_SETTLE_PRICE'] = df['FH_CLOSING_PRICE']
        
        # Only keep Futures!
        futures = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])].copy()
        
        if futures.empty: continue
            
        futures['FH_OPTION_TYPE'] = futures['FH_OPTION_TYPE'].fillna('XX')
        if 'spot' in futures.columns:
            futures['FH_UNDERLYING_VALUE'] = futures['spot']
            
        futures['FH_TIMESTAMP'] = pd.to_datetime(futures['FH_TIMESTAMP']).dt.strftime('%d-%b-%Y')
        futures['FH_EXPIRY_DT'] = pd.to_datetime(futures['FH_EXPIRY_DT']).dt.strftime('%d-%b-%Y')
        
        all_recent_data.append(futures)
    except Exception as e:
        print(f"Error parsing cache file {cf}: {e}")

if not all_recent_data:
    print("No recent futures data extracted!")
    exit()

recent_combined = pd.concat(all_recent_data, ignore_index=True)
recent_combined['FH_MARKET_TYPE'] = 'N'

print(f"Extracted {len(recent_combined)} futures rows from cache.")

# 2. Iterate through 5y_data files and append recent rows for each ticker
fno_files = glob.glob(os.path.join(OUT_DIR, "*_5Y.csv"))
print(f"Stitching into {len(fno_files)} tickers in 5y_data...")

processed_tickers = 0
for f in fno_files:
    ticker = os.path.basename(f).replace('_5Y.csv', '')
    
    # Map old symbols to the current ticker
    search_symbols = [ticker]
    if ticker == 'SAMMAANCAP':
        search_symbols.append('IBULHSGFIN')
    elif ticker == 'LTIM':
        search_symbols.extend(['LTI', 'LTIMINDTREE'])
    
    ticker_recent = recent_combined[recent_combined['FH_SYMBOL'].isin(search_symbols)].copy()
    if ticker_recent.empty:
        continue
    
    ticker_recent['FH_SYMBOL'] = ticker
        
    df_5y = pd.read_csv(f)
    
    # combine
    stitched = pd.concat([df_5y, ticker_recent], ignore_index=True)
    
    # standard dedup based on Date & Expiry
    stitched['date_obj'] = pd.to_datetime(stitched['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    stitched = stitched.dropna(subset=['date_obj'])
    
    if 'FH_EXPIRY_DT' in stitched.columns:
        stitched['exp_obj'] = pd.to_datetime(stitched['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        stitched = stitched.sort_values(by=['date_obj', 'exp_obj']).drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT'], keep='last')
        stitched['FH_EXPIRY_DT'] = stitched['exp_obj'].dt.strftime('%d-%b-%Y')
        stitched = stitched.drop(columns=['exp_obj'])
    else:
        stitched = stitched.sort_values(by=['date_obj']).drop_duplicates(subset=['FH_TIMESTAMP'], keep='last')

    stitched['FH_TIMESTAMP'] = stitched['date_obj'].dt.strftime('%d-%b-%Y')
    stitched = stitched.drop(columns=['date_obj'])
    
    # drop leftover nsefin noise
    for col in ['category', 'ticker', 'spot', 'trade_contract']:
        if col in stitched.columns:
            stitched = stitched.drop(columns=[col])

    stitched.to_csv(f, index=False)
    processed_tickers += 1

print(f"Successfully stitched recent data into {processed_tickers} ticker files.")
