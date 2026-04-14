import os
import sys
import time
import random
from datetime import date, timedelta
import pandas as pd
from nsefin import nse
import logging

# Reset logging for nsefin if it's too noisy
logging.basicConfig(level=logging.ERROR)

CACHE_DIR = '.tmp/bhav_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

start_date = date(2025, 5, 9)
end_date = date(2026, 4, 7)

dates_to_fetch = []
current = start_date
while current <= end_date:
    if current.weekday() < 5: 
        dates_to_fetch.append(current)
    current += timedelta(days=1)

print(f"Resuming: potential trading days left to scan: {len(dates_to_fetch)}")

def fetch_with_retry(dt, max_retries=6):
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    if os.path.exists(cache_path):
        try:
            return pd.read_csv(cache_path), True
        except:
            pass # corrupted cache file

    sys.stdout.write(f"Fetching {d_str}...")
    sys.stdout.flush()

    for attempt in range(max_retries):
        try:
            # We must be extremely patient
            time.sleep(random.uniform(3.0, 5.0))
            df = nse.get_fno_bhav_copy(date=dt)
            
            if df is None or len(df) < 100:
                # Could be a genuine holiday
                sys.stdout.write(f" [None/Holiday]\n")
                sys.stdout.flush()
                return None, False
                
            df.to_csv(cache_path, index=False)
            sys.stdout.write(f" [SUCCESS]\n")
            sys.stdout.flush()
            return df, False
            
        except Exception as e:
            msg = str(e)
            if "Max retries exceeded" in msg or "Connection to nsearchives.nseindia.com timed out" in msg:
                 sys.stdout.write(".")
                 sys.stdout.flush()
                 time.sleep(random.uniform(15.0, 30.0)) # huge penalty box wait
            else:
                 time.sleep(random.uniform(5.0, 8.0))
                 
            if attempt == max_retries - 1:
                sys.stdout.write(f" [FAILED PERMANENTLY]\n")
                sys.stdout.flush()
            
    return None, False

fetched = 0

for dt in dates_to_fetch:
    df, cached = fetch_with_retry(dt)
    if df is not None and not cached:
         fetched += 1
        
print(f"\nFinished caching cycle. Downloaded {fetched} new days.")
