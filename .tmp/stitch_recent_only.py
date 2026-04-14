import pandas as pd
import os

Y3_DIR = '.tmp/3y_data'
DIR_2021 = '.tmp/2021_futures_data'
RECENT_DIR = '.tmp/recent_futures_data'
OUT_DIR = '.tmp/5y_data'

os.makedirs(OUT_DIR, exist_ok=True)

fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(Y3_DIR) if f.endswith('_3Y.csv')]

count = 0
for ticker in fno_tickers:
    dfs = []
    
    # We DO NOT have the 2021 temporary folder anymore (it was deleted).
    # But wait! We DO have the stitched `.tmp/5y_data/{ticker}_5Y.csv` which ALREADY HAS the 2021 data and 3Y data perfectly stitched!
    # Let me just load `_5Y.csv` from the 5y_data folder, and append `recent_futures_data` to it!
    
    p_current_5y = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    if os.path.exists(p_current_5y):
        dfs.append(pd.read_csv(p_current_5y))
    else:
        # If it doesn't exist for some reason, at least use 3Y
        p2 = os.path.join(Y3_DIR, f"{ticker}_3Y.csv")
        if os.path.exists(p2):
            dfs.append(pd.read_csv(p2))
            
    # Recent Data
    p3 = os.path.join(RECENT_DIR, f"{ticker}_recent_futures.csv")
    if os.path.exists(p3):
        dfs.append(pd.read_csv(p3))
        
    if not dfs:
        continue
        
    combined = pd.concat(dfs, ignore_index=True)
    combined.columns = [c.strip() for c in combined.columns]
    
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
    
    out_path = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    combined.to_csv(out_path, index=False)
    count += 1

print(f"Appended recent data exactly to 5y_data for {count} files.")
