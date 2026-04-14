import pandas as pd
import glob
import os

OUT_DIR = '.tmp/5y_data'

fno_tickers = [os.path.basename(f).replace('_5Y.csv', '') for f in glob.glob(os.path.join(OUT_DIR, "*_5Y.csv"))]

missing_tickers = {}
expected_start = pd.Period('2021-01', freq='M')
expected_end = pd.Period('2026-04', freq='M')
all_expected = pd.period_range(start=expected_start, end=expected_end, freq='M')

for ticker in fno_tickers:
    p = os.path.join(OUT_DIR, f"{ticker}_5Y.csv")
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['date'])
    
    # Extract unique Year-Month combinations
    df['YM'] = df['date'].dt.to_period('M')
    unique_ym = sorted(df['YM'].unique())
    if len(unique_ym) == 0:
        continue
    
    start = max(unique_ym[0], expected_start)  # To account for newly listed stocks
    actual_expected = pd.period_range(start=start, end=expected_end, freq='M')
    
    missing = [ym for ym in actual_expected if ym not in unique_ym]
    if missing:
        missing_tickers[ticker] = missing

if missing_tickers:
    print(f"Missing months for {len(missing_tickers)} tickers.")
    for t, m in list(missing_tickers.items()):
        print(f"[{t}] MISSING: {m}")
else:
    print("NO missing months!")
