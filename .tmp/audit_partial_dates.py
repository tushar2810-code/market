import os
import pandas as pd
from datetime import datetime

DATA_DIR = '.tmp/3y_data'

def audit():
    if not os.path.exists(DATA_DIR):
        print("Data dir not found")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_3Y.csv')]
    
    partials = []
    
    print(f"{'Symbol':<15} {'Rows':<6} {'Start Date':<12} {'End Date':<12}")
    print("-" * 50)
    
    for filename in files:
        symbol = filename.replace('_3Y.csv', '')
        path = os.path.join(DATA_DIR, filename)
        try:
            df = pd.read_csv(path)
            rows = len(df)
            
            # Filter for partials (e.g. < 600 trading days approx 2.5 years)
            # Full 3 years ~ 750 trading days.
            if rows < 600:
                # Find min date
                if 'FH_TIMESTAMP' in df.columns:
                    col = 'FH_TIMESTAMP'
                elif 'Date' in df.columns:
                    col = 'Date'
                else:
                    continue
                    
                # Parse date
                # Formats can be mixed? usually DD-Mon-YYYY
                dates = pd.to_datetime(df[col], format='%d-%b-%Y', errors='coerce').dropna()
                if dates.empty:
                    # Try YYYY-MM-DD
                    dates = pd.to_datetime(df[col], errors='coerce').dropna()
                
                if not dates.empty:
                    min_date = dates.min()
                    max_date = dates.max()
                    partials.append({
                        'Symbol': symbol,
                        'Rows': rows,
                        'Start': min_date,
                        'End': max_date
                    })

        except Exception as e:
            pass

    # Sort by start date
    partials.sort(key=lambda x: x['Start'])
    
    for p in partials:
        print(f"{p['Symbol']:<15} {p['Rows']:<6} {p['Start'].strftime('%Y-%m-%d'):<12} {p['End'].strftime('%Y-%m-%d'):<12}")

if __name__ == "__main__":
    audit()
