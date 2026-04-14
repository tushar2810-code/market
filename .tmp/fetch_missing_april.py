import os
import sys
import time
from datetime import date
import pandas as pd
from nsefin import nse

CACHE_DIR = '.tmp/bhav_cache'

dates_to_fetch = [date(2026, 4, 8), date(2026, 4, 9), date(2026, 4, 10)]

for dt in dates_to_fetch:
    d_str = dt.strftime('%d-%b-%Y')
    cache_path = os.path.join(CACHE_DIR, f"{d_str}.csv")
    if not os.path.exists(cache_path):
        print(f"Fetching {d_str}...")
        df = nse.get_fno_bhav_copy(date=dt)
        if df is not None and not df.empty:
            df.to_csv(cache_path, index=False)
            print("Saved.")
        time.sleep(2)

print("Recent 3 days synced.")
