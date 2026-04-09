import pandas as pd
from datetime import datetime
import numpy as np

try:
    # Use the reliable recent 6-month CSV we've been using which has Spot/Close data
    df = pd.read_csv('RVNL.csv')

    def parse_date(d):
        try: return datetime.strptime(str(d).strip(), '%d-%b-%Y')
        except: return None

    df['Date'] = df['Date'].apply(parse_date)
    df['Expiry Date'] = df['Expiry Date'].apply(parse_date)

    def clean_price(p):
        if isinstance(p, str):
            p = p.replace(',', '').strip()
            if p in ('-', ''): return np.nan
            try: return float(p)
            except: return np.nan
        return float(p)

    df['Close Price'] = df['Close Price'].apply(clean_price)
    
    # In Shoonya/NSE Historical CSVs downloaded from the terminal, the actual Spot price 
    # isn't explicitly in the Derivatives CSV. 
    # The derivatives CSV only has Futures data.
    
    # We can calculate the spread between Expiry Day Close of the Near contract
    # to see how wild it gets.

    df.dropna(subset=['Close Price', 'Expiry Date', 'Date'], inplace=True)
    
    dates = sorted(df['Date'].unique())
    records = []
    
    for d in dates:
        day_data = df[df['Date'] == d].sort_values('Expiry Date')
        if len(day_data) >= 1:
            near_fut = day_data.iloc[0]
            
            # Is this expiry day?
            if d == near_fut['Expiry Date']:
                records.append({
                    'Date': d,
                    'Near_Close': near_fut['Close Price']
                })
                
    res = pd.DataFrame(records)
    print("Expiry Day Near-Future Closes:")
    print(res)

except Exception as e:
    print(f"Error: {e}")
