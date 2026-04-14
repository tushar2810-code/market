import os
import yfinance as yf
import pandas as pd

data_dir = '.tmp/3y_data'
files = [f for f in os.listdir(data_dir) if f.endswith('_3Y.csv')]
tickers = [f.replace('_3Y.csv', '.NS') for f in files]

all_actions = []
for ticker in tickers:
    try:
        t = yf.Ticker(ticker)
        actions = t.actions
        if actions is not None and not actions.empty:
            # Filter for last 3 years
            start_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=3*365)
            actions.index = pd.to_datetime(actions.index, utc=True)
            actions = actions[actions.index >= start_date]
            
            for index, row in actions.iterrows():
                if row.get('Dividends', 0) > 0 or row.get('Stock Splits', 0) > 0:
                    all_actions.append({
                        'Ticker': ticker.replace('.NS', ''),
                        'Date': index.strftime('%Y-%m-%d'),
                        'Dividends': row.get('Dividends', 0),
                        'Stock Splits': row.get('Stock Splits', 0)
                    })
    except Exception as e:
        pass

df = pd.DataFrame(all_actions)
if not df.empty:
    df.to_csv('.tmp/fno_actions_3y.csv', index=False)
    print(f"Found {len(df)} corporate actions. Saved to .tmp/fno_actions_3y.csv")
    splits = df[df['Stock Splits'] > 0]
    print(f"Total Splits/Bonuses: {len(splits)}")
else:
    print("No actions found.")
