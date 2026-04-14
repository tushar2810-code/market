import os
import time
import random
import json
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime

CACHE_DIR = '.tmp/bhav_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

with open('.tmp/missing_dates_union.json', 'r') as f:
    missing_dates_str = json.load(f)

dates_to_fetch = [datetime.strptime(d, '%Y-%m-%d') for d in missing_dates_str]

# Filter out dates that have already been saved to the cache by ANY process
missing_dates = []
for dt in dates_to_fetch:
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    if not os.path.exists(cache_path):
        missing_dates.append(dt)

print(f"Remaining legacy trading days to fetch: {len(missing_dates)}")

h = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

def fetch_legacy(dt):
    # e.g. 05MAY2023
    d_str = dt.strftime('%d%b%Y').upper()
    yr = dt.strftime('%Y')
    mo = dt.strftime('%b').upper()
    url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{yr}/{mo}/fo{d_str}bhav.csv.zip"
    
    try:
        r = requests.get(url, headers=h, timeout=10)
        if r.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(r.content))
            filename = z.namelist()[0]
            with z.open(filename) as f2:
                df = pd.read_csv(f2)
                cache_path = os.path.join(CACHE_DIR, f"{dt.strftime('%d-%b-%Y')}.csv")
                df.to_csv(cache_path, index=False)
                return True
    except Exception as e:
        pass
    return False

fetched = 0
for dt in missing_dates:
    print(f"Fetching legacy: {dt.strftime('%Y-%m-%d')}...", end="")
    success = fetch_legacy(dt)
    if success:
        print(" [OK]")
        fetched += 1
    else:
        print(" [FAILED/HOLIDAY]")
    time.sleep(random.uniform(0.5, 1.0)) # Historical zips are less rate limited

print(f"Finished. Fetched {fetched} historical zip files.")
