import requests
import json
from datetime import datetime
import pandas as pd

def fetch_nse_historical_spot(symbol, from_date, to_date):
    url = f"https://www.nseindia.com/api/historical/cm/equity?symbol={symbol}&series=[%22EQ%22]&from={from_date}&to={to_date}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': '*/* ',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    session = requests.Session()
    session.get('https://www.nseindia.com', headers=headers)
    
    try:
        response = session.get(url, headers=headers)
        data = response.json()
        if 'data' in data:
            return pd.DataFrame(data['data'])
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching {symbol} from NSE: {e}")
        return pd.DataFrame()

# Known expiry dates from our previous run
expiries = [
    '26-06-2025',
    '31-07-2025',
    '28-08-2025',
    '30-09-2025',
    '28-10-2025',
    '25-11-2025',
    '30-12-2025'
]

print("Fetching historical spot data for RVNL...")
# Let's just fetch all of 2025 
df_spot = fetch_nse_historical_spot('RVNL', '01-05-2025', '31-12-2025')

if not df_spot.empty:
    df_spot['CH_TIMESTAMP'] = pd.to_datetime(df_spot['CH_TIMESTAMP'])
    df_spot.set_index('CH_TIMESTAMP', inplace=True)
    
    # Load Futures data
    df_fut = pd.read_csv('RVNL.csv')
    df_fut['Date'] = pd.to_datetime(df_fut['Date'])
    df_fut['Expiry Date'] = pd.to_datetime(df_fut['Expiry Date'])
    df_fut['Close Price'] = df_fut['Close Price'].astype(str).str.replace(',', '').astype(float)
    
    print("\n--- EXACT HISTORICAL END-OF-DAY GAP ON EXPIRY DAY ---")
    print("DATE         | SPOT CLOSE | FUTURE CLOSE | EXACT GAP (SPOT - FUTURE)")
    print("-" * 65)
    
    for exp_str in expiries:
        exp_dt = pd.to_datetime(exp_str, format='%d-%m-%Y')
        
        # Get Spot Close
        spot_close = None
        if exp_dt in df_spot.index:
            spot_close = df_spot.loc[exp_dt, 'CH_CLOSING_PRICE']
            
        # Get Near Future Close
        fut_data = df_fut[(df_fut['Date'] == exp_dt) & (df_fut['Expiry Date'] == exp_dt)]
        fut_close = None
        if not fut_data.empty:
            fut_close = fut_data.iloc[0]['Close Price']
            
        if spot_close is not None and fut_close is not None:
            gap = float(spot_close) - float(fut_close)
            print(f"{exp_dt.date()}   | {spot_close:>10.2f} | {fut_close:>12.2f} | {gap:>18.2f}")
        else:
            print(f"{exp_dt.date()}   | Data Missing")
else:
    print("Failed to fetch spot data.")
