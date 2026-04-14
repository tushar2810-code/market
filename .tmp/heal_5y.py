import pandas as pd
import os
import glob

Y3_DIR = '.tmp/3y_data'
OUT_DIR = '.tmp/5y_data'

fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(Y3_DIR) if f.endswith('_3Y.csv')]

count = 0
for ticker in fno_tickers:
    dfs = []
    
    # 1. We load the existing 5Y data (which contains 2021 + nsefin recent).
    p1 = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    if os.path.exists(p1):
        dfs.append(pd.read_csv(p1))
        
    # 2. We load the 3Y data again! Because clearly some 3Y data got left out 
    # either by the stitcher or by a bug.
    p2 = os.path.join(Y3_DIR, f"{ticker}_3Y.csv")
    if os.path.exists(p2):
        dfs.append(pd.read_csv(p2))
        
    if not dfs:
        continue
        
    combined = pd.concat(dfs, ignore_index=True)
    combined.columns = [c.strip() for c in combined.columns]
    
    # Make sure we don't have PE/CE options sneaking back in!
    if 'FH_OPTION_TYPE' in combined.columns:
        combined = combined[~combined['FH_OPTION_TYPE'].isin(['CE', 'PE'])]
    
    combined['date_obj'] = pd.to_datetime(combined['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    combined['date_obj'] = combined['date_obj'].fillna(pd.to_datetime(combined['FH_TIMESTAMP'], format='%d-%b-%y', errors='coerce'))
    combined['date_obj'] = combined['date_obj'].fillna(pd.to_datetime(combined['FH_TIMESTAMP'], errors='coerce'))
    
    combined = combined.dropna(subset=['date_obj'])
    
    if 'FH_EXPIRY_DT' in combined.columns:
        combined['exp_obj'] = pd.to_datetime(combined['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
        combined['exp_obj'] = combined['exp_obj'].fillna(pd.to_datetime(combined['FH_EXPIRY_DT'], format='%d-%b-%y', errors='coerce'))
        combined['exp_obj'] = combined['exp_obj'].fillna(pd.to_datetime(combined['FH_EXPIRY_DT'], errors='coerce'))
        
        combined = combined.sort_values(by=['date_obj', 'exp_obj']).drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT'], keep='last')
        combined['FH_EXPIRY_DT'] = combined['exp_obj'].dt.strftime('%d-%b-%Y')
        combined = combined.drop(columns=['exp_obj'])
    else:
        combined = combined.sort_values(by=['date_obj']).drop_duplicates(subset=['FH_TIMESTAMP'], keep='last')

    combined['FH_TIMESTAMP'] = combined['date_obj'].dt.strftime('%d-%b-%Y')
    combined = combined.drop(columns=['date_obj'])
    
    # clean extra columns
    for col in ['category', 'ticker', 'spot', 'trade_contract']:
        if col in combined.columns:
            combined = combined.drop(columns=[col])

    out_path = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    combined.to_csv(out_path, index=False)
    count += 1

print(f"Restitched and healed {count} ticker timelines to ensure zero data loss from 3y_data.")
