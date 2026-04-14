import os
import yfinance as yf
import pandas as pd

data_dir = '.tmp/2021_data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Get symbols from existing directory
fno_dir = '.tmp/3y_data'
files = [f for f in os.listdir(fno_dir) if f.endswith('_3Y.csv')]
fno_tickers = [f.replace('_3Y.csv', '') for f in files]

print(f"Fetching 2021-2022 price data for {len(fno_tickers)} symbols...")

for symbol in fno_tickers:
    try:
        ticker = f"{symbol}.NS"
        data = yf.download(ticker, start="2021-01-01", end="2022-04-01", progress=False)
        if not data.empty:
            # Flatten multi-index columns if present
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [c[0].lower() for c in data.columns]
            else:
                data.columns = [c.lower() for c in data.columns]
                
            # Keep required columns only
            req_cols = ['open', 'high', 'low', 'close', 'volume']
            df = data[[c for c in req_cols if c in data.columns]]
            df.index.name = 'Date'
            
            # Save
            df.to_csv(f"{data_dir}/{symbol}_2021.csv")
    except Exception as e:
        pass

print(f"Price data saved to {data_dir}/")
