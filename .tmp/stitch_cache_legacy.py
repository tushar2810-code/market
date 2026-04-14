import pandas as pd
import os
import glob

OUT_DIR = '.tmp/5y_data'
CACHE_DIR = '.tmp/bhav_cache'

cache_files = glob.glob(os.path.join(CACHE_DIR, "*.csv"))

all_legacy_data = []

# Legacy Format:
# INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP

rename_map_legacy = {
    'INSTRUMENT': 'FH_INSTRUMENT',
    'SYMBOL': 'FH_SYMBOL',
    'EXPIRY_DT': 'FH_EXPIRY_DT',
    'STRIKE_PR': 'FH_STRIKE_PRICE',
    'OPTION_TYP': 'FH_OPTION_TYPE',
    'OPEN': 'FH_OPENING_PRICE',
    'HIGH': 'FH_TRADE_HIGH_PRICE',
    'LOW': 'FH_TRADE_LOW_PRICE',
    'CLOSE': 'FH_CLOSING_PRICE',
    'SETTLE_PR': 'FH_SETTLE_PRICE',
    'CONTRACTS': 'FH_TOT_TRADED_QTY', # Usually contracts mapped to qty
    'VAL_INLAKH': 'FH_TOT_TRADED_VAL',
    'OPEN_INT': 'FH_OPEN_INT',
    'CHG_IN_OI': 'FH_CHANGE_IN_OI',
    'TIMESTAMP': 'FH_TIMESTAMP',
}

for cf in cache_files:
    try:
        df = pd.read_csv(cf)
        if 'INSTRUMENT' not in df.columns:
            continue # not legacy format
            
        df = df.rename(columns=rename_map_legacy)
        
        # Keep only futures!
        futures = df[df['FH_INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])].copy()
        if futures.empty: continue
            
        futures['FH_OPTION_TYPE'] = futures['FH_OPTION_TYPE'].fillna('XX')
        
        # Adjust timestamp format if needed (e.g. 05-MAY-2023 -> 05-May-2023)
        futures['FH_TIMESTAMP'] = pd.to_datetime(futures['FH_TIMESTAMP'], format='mixed', dayfirst=True).dt.strftime('%d-%b-%Y')
        futures['FH_EXPIRY_DT'] = pd.to_datetime(futures['FH_EXPIRY_DT'], format='mixed', dayfirst=True).dt.strftime('%d-%b-%Y')
        
        all_legacy_data.append(futures)
    except Exception as e:
        print(f"Error parsing legacy cache file {cf}: {e}")

if not all_legacy_data:
    print("No legacy futures data extracted!")
    exit()

legacy_combined = pd.concat(all_legacy_data, ignore_index=True)
legacy_combined['FH_MARKET_TYPE'] = 'N'

print(f"Extracted {len(legacy_combined)} futures rows from legacy cache.")

# Stitch back to 5y_data
fno_files = glob.glob(os.path.join(OUT_DIR, "*_5Y.csv"))
processed_tickers = 0
for f in fno_files:
    ticker = os.path.basename(f).replace('_5Y.csv', '')
    
    # Map old symbols to the current ticker
    search_symbols = [ticker]
    if ticker == 'SAMMAANCAP':
        search_symbols.append('IBULHSGFIN')
    elif ticker == 'LTIM':
        search_symbols.extend(['LTI', 'LTIMINDTREE'])
    
    ticker_recent = legacy_combined[legacy_combined['FH_SYMBOL'].isin(search_symbols)].copy()
    if ticker_recent.empty:
        continue
    
    ticker_recent['FH_SYMBOL'] = ticker
        
    df_5y = pd.read_csv(f)
    stitched = pd.concat([df_5y, ticker_recent], ignore_index=True)
    
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

    stitched.to_csv(f, index=False)
    processed_tickers += 1

print(f"Successfully stitched legacy historical data into {processed_tickers} ticker files.")
