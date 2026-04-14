import os
import sys
import time
import random
import json
from datetime import datetime
import pandas as pd
from nsefin import nse

CACHE_DIR = '.tmp/bhav_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

with open('.tmp/missing_dates_union.json', 'r') as f:
    missing_dates_str = json.load(f)

# Convert strings like '2022-08-01' back to date objects
dates_to_fetch = [datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates_str]

print(f"Total unique missing trading days to surgically patch: {len(dates_to_fetch)}")

def fetch_with_retry(dt, max_retries=5):
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    if os.path.exists(cache_path):
        try:
            return pd.read_csv(cache_path), True
        except:
            pass # corrupted

    sys.stdout.write(f"Fetching {d_str}...")
    sys.stdout.flush()

    for attempt in range(max_retries):
        try:
            # Gentle pacing to avoid IP block for 808 files
            time.sleep(random.uniform(2.0, 3.5))
            df = nse.get_fno_bhav_copy(date=dt)
            
            if df is None or len(df) < 100:
                sys.stdout.write(" [Holiday]\n")
                sys.stdout.flush()
                return None, False
                
            df.to_csv(cache_path, index=False)
            sys.stdout.write(" [OK]\n")
            sys.stdout.flush()
            return df, False
            
        except Exception as e:
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(random.uniform(10.0, 20.0))
            if attempt == max_retries - 1:
                sys.stdout.write(" [FAILED]\n")
                sys.stdout.flush()
            
    return None, False

fetched = 0

for dt in dates_to_fetch:
    df, cached = fetch_with_retry(dt)
    if df is not None and not cached:
         fetched += 1
        
print(f"\nFinished patching missing timelines. Downloaded {fetched} historical days.")
