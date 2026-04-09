import os
import pandas as pd
import sys
import urllib.parse
from datetime import datetime

# Add execution dir to path to import fno_utils
sys.path.append(os.path.abspath('execution'))
try:
    from fno_utils import FNO_SYMBOLS
except ImportError:
    FNO_SYMBOLS = ["ADANIENT", "SWIGGY"] 

DATA_DIR = '.tmp/3y_data'
OUTPUT_FILE = 'manual_download_tasks.md'

def get_link(symbol, year):
    # Year is integer, e.g. 2023, 2024
    start_str = f"01-01-{year}"
    end_str = f"31-12-{year}"
    
    # If year is current year (2025), end date should be today to avoid future error?
    # NSE usually handles future dates by just returning up to today. 
    # But let's be safe.
    now = datetime.now()
    if year == now.year:
        end_str = now.strftime('%d-%m-%Y')
        
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getDerivativesHistoricalData&symbol={urllib.parse.quote(symbol)}&instrumentType=FUTSTK&fromDate={start_str}&toDate={end_str}&csv=true"
    return f"[Download {year} ({start_str} to {end_str})]({url})"

def run():
    if not os.path.exists(DATA_DIR):
        print("Data dir not found")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_3Y.csv')]
    tasks = []
    
    # Years to cover: 2023, 2024. 
    # 2025 is "Recent" and usually present, but we can add it for completeness if file is tiny.
    
    # 1. ADANIENT - Specific missing 2023
    tasks.append({
        "symbol": "ADANIENT",
        "reason": "Missing 2023 Data",
        "links": [get_link("ADANIENT", 2023), get_link("ADANIENT", 2024)]
    })
    
     # 2. SWIGGY
    tasks.append({
        "symbol": "SWIGGY",
        "reason": "Verify Data (2024-2025)",
        "links": [get_link("SWIGGY", 2024), get_link("SWIGGY", 2025)]
    })
    
    # 3. Scan others
    for f in files:
        sym = f.replace('_3Y.csv', '')
        if sym in ["ADANIENT", "SWIGGY"]: continue
        
        path = os.path.join(DATA_DIR, f)
        try:
            size_kb = os.path.getsize(path) / 1024
            if size_kb < 200: 
                # Likely partial. Provide 2023 and 2024 links.
                tasks.append({
                    "symbol": sym,
                    "reason": f"File small ({int(size_kb)}KB) - likely missing 2023/2024",
                    "links": [get_link(sym, 2024), get_link(sym, 2023)]
                })
        except:
            pass
            
    tasks.sort(key=lambda x: x['symbol'])

    with open(OUTPUT_FILE, 'w') as f:
        f.write("# Manual Download Tasks (Calendar Year Strategy)\n\n")
        f.write("Updated links to use specific **Calendar Years** (Jan-Dec) to improve reliability.\n\n")
        f.write("> **Tip**: If WiFi fails, switch to Mobile Data for a new IP.\n\n")
        
        f.write("## Instructions\n")
        f.write("1. Download the files.\n")
        f.write("2. Save to `.tmp/manual_data/`.\n")
        f.write("3. **Notify me** when done.\n\n")
        
        f.write("| Symbol | Download Links | Reason |\n")
        f.write("|---|---|---|\n")
        
        for t in tasks:
            links_md = "<br>".join(t['links'])
            f.write(f"| **{t['symbol']}** | {links_md} | {t['reason']} |\n")
            
    print(f"Generated {OUTPUT_FILE} with {len(tasks)} tasks.")

if __name__ == "__main__":
    run()
