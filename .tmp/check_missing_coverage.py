import pandas as pd
import glob
import os

OUT_DIR = '.tmp/5y_data'

# Let's pick a few old symbols
symbols_to_test = ['RELIANCE', 'INFY', 'TCS', 'HDFCBANK', 'AXISBANK']

for ticker in symbols_to_test:
    p = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    if not os.path.exists(p): continue
    
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['date'])
    
    # Extract unique Year-Month combinations
    df['YM'] = df['date'].dt.to_period('M')
    unique_ym = sorted(df['YM'].unique())
    
    expected_start = pd.Period('2021-01', freq='M')
    expected_end = pd.Period('2026-04', freq='M')
    
    all_expected = pd.period_range(start=expected_start, end=expected_end, freq='M')
    
    missing = [ym for ym in all_expected if ym not in unique_ym]
    
    if len(missing) == 0:
        print(f"[{ticker}] Complete coverage from {expected_start} to {expected_end} ({len(unique_ym)} months)")
    else:
        print(f"[{ticker}] MISSING: {missing}")

