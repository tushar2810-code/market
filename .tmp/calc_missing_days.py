import pandas as pd
import glob
import os
import json

OUT_DIR = '.tmp/5y_data'

fno_tickers = [os.path.basename(f).replace('_5Y.csv', '') for f in glob.glob(os.path.join(OUT_DIR, "*_5Y.csv"))]

all_trading_days = pd.read_csv(os.path.join(OUT_DIR, 'RELIANCE_5Y.csv'))
all_trading_days['date'] = pd.to_datetime(all_trading_days['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
all_trading_dates = set(all_trading_days.dropna(subset=['date'])['date'].dt.strftime('%Y-%m-%d'))

missing_dates_union = set()
ticker_gaps = {}

for ticker in fno_tickers:
    p = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    ticker_dates = set(df.dropna(subset=['date'])['date'].dt.strftime('%Y-%m-%d'))
    
    if len(ticker_dates) < 100: continue # Skip if very newly listed
    
    start_dt = sorted(list(ticker_dates))[0]
    expected_dates_for_ticker = {d for d in all_trading_dates if d >= start_dt}
    
    missing = expected_dates_for_ticker - ticker_dates
    
    if len(missing) > 10:
        missing_dates_union.update(missing)
        ticker_gaps[ticker] = len(missing)

print(f"Total unique missing trading days across all tickers: {len(missing_dates_union)}")
print(f"Total tickers with >10 missing days: {len(ticker_gaps)}")

if len(missing_dates_union) < 1500:
    with open('.tmp/missing_dates_union.json', 'w') as f:
        json.dump(sorted(list(missing_dates_union)), f)
