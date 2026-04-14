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

# Convert strings to date objects
dates_to_eval = [datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates_str]

# Filter to > 2024-07-01
target_cutoff = datetime(2024, 7, 1).date()
modern_dates = [d for d in dates_to_eval if d > target_cutoff]

# Exclude ones we already have
dates_to_fetch = []
for dt in modern_dates:
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    if not os.path.exists(cache_path):
        dates_to_fetch.append(dt)

print(f"Total modern trading days to patch: {len(dates_to_fetch)}")

fetched = 0

for dt in dates_to_fetch:
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    
    sys.stdout.write(f"Fetching modern {d_str}...")
    sys.stdout.flush()
    
    for attempt in range(3):
        try:
            # Random pacing to avoid bans
            time.sleep(random.uniform(1.0, 2.0))
            df = nse.get_fno_bhav_copy(date=dt)
            if df is not None and not df.empty:
                df.to_csv(cache_path, index=False)
                sys.stdout.write(" [OK]\n")
                sys.stdout.flush()
                fetched += 1
                break
            else:
                sys.stdout.write(" [Empty/Holiday]\n")
                sys.stdout.flush()
                break
        except Exception as e:
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(3.0)
            if attempt == 2:
                sys.stdout.write(" [FAILED]\n")
                sys.stdout.flush()

print(f"\nFinished modern patching. Downloaded {fetched} days.")
