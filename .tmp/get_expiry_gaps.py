import pandas as pd
from datetime import datetime

# Read the full 3-year historical data which has both SPOT and FUTURES
try:
    df = pd.read_csv('.tmp/3y_data/RVNL_3Y.csv')
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'])
    df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'])
    
    # Filter for Expiry Days (where Timestamp matches Near Expiry Date)
    # The 3Y CSV usually contains pre-calculated 'SPREAD' (Far - Near). 
    # We need Spot vs Near. Let's see what columns are available.
    print(f"Columns available: {df.columns.tolist()}")
except Exception as e:
    print(f"Error reading 3Y data: {e}")

