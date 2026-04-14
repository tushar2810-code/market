from jugaad_data.nse import bhavcopy_fo_save
from datetime import date, timedelta
import pandas as pd
import time
import os
import sys

OUTPUT_DIR = '.tmp/recent_futures_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)
BHAV_DIR = '.tmp/bhav_cache'
os.makedirs(BHAV_DIR, exist_ok=True)

start_date = date(2025, 2, 6)
end_date = date(2026, 4, 7)  # Explicitly stop at April 7

current = start_date
dates_to_fetch = []
while current <= end_date:
    if current.weekday() < 5: 
        dates_to_fetch.append(current)
    current += timedelta(days=1)

print(f"Total potential trading days to process: {len(dates_to_fetch)}")

fno_dir = '.tmp/3y_data'
fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(fno_dir) if f.endswith('_3Y.csv')]

def process_bhavcopy(path):
    try:
        df = pd.read_csv(path)
        futures = df[df['INSTRUMENT'].isin(['FUTSTK', 'FUTIDX'])].copy()
        
        rename_map = {
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
            'CONTRACTS': 'FH_TOT_TRADED_QTY', 
            'VAL_INLAKH': 'FH_TOT_TRADED_VAL',
            'OPEN_INT': 'FH_OPEN_INT',
            'CHG_IN_OI': 'FH_CHANGE_IN_OI',
            'TIMESTAMP': 'FH_TIMESTAMP'
        }
        futures.rename(columns=rename_map, inplace=True)
        return futures[futures['FH_SYMBOL'].isin(fno_tickers)]
    except Exception as e:
        return pd.DataFrame()

ticker_data = {}
count = 0

for dt in dates_to_fetch:
    count += 1
    if count % 20 == 0:
        print(f"Processing day {count}/{len(dates_to_fetch)}: {dt}")
        sys.stdout.flush()
        
    date_str = dt.strftime("%d%b%Y").upper()
    expected_path = os.path.join(BHAV_DIR, f"fo{date_str}bhav.csv")
    
    if not os.path.exists(expected_path):
        try:
            time.sleep(0.5)
            expected_path = bhavcopy_fo_save(dt, BHAV_DIR)
        except Exception:
            continue

    df = process_bhavcopy(expected_path)
    if not df.empty:
        for symbol, group in df.groupby('FH_SYMBOL'):
            if symbol not in ticker_data:
                ticker_data[symbol] = []
            ticker_data[symbol].append(group)

print(f"Saving aggregated data to {OUTPUT_DIR}...")
for symbol, chunks in ticker_data.items():
    if not chunks:
        continue
    combined = pd.concat(chunks, ignore_index=True)
    if 'FH_MARKET_LOT' not in combined.columns:
        combined['FH_MARKET_LOT'] = None
    if 'FH_MARKET_TYPE' not in combined.columns:
        combined['FH_MARKET_TYPE'] = 'N'
        
    out_path = os.path.join(OUTPUT_DIR, f"{symbol}_recent_futures.csv")
    combined.to_csv(out_path, index=False)

print("Done successfully.")
