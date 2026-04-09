import os
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta

DATA_DIR = '.tmp/3y_data'
OUTPUT_FILE = 'manual_download_tasks.md'
CURRENT_DATE = datetime(2026, 1, 31)

def get_link(symbol, start_dt, end_dt):
    from_str = start_dt.strftime('%d-%m-%Y')
    to_str = end_dt.strftime('%d-%m-%Y')
    url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getDerivativesHistoricalData&symbol={urllib.parse.quote(symbol)}&instrumentType=FUTSTK&fromDate={from_str}&toDate={to_str}&csv=true"
    return f"[Download {from_str} to {to_str}]({url})"

def run():
    if not os.path.exists(DATA_DIR):
        print("Data dir not found")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('_3Y.csv')]
    
    stale_symbols = []
    complete_new_listings = []
    
    print("Scanning for stale data...")
    
    for filename in files:
        symbol = filename.replace('_3Y.csv', '')
        path = os.path.join(DATA_DIR, filename)
        try:
            df = pd.read_csv(path)
            # Find date col
            col = None
            if 'FH_TIMESTAMP' in df.columns: col = 'FH_TIMESTAMP'
            elif 'Date' in df.columns: col = 'Date'
            
            if col:
                dates = pd.to_datetime(df[col], errors='coerce').dropna()
                if not dates.empty:
                    max_date = dates.max()
                    min_date = dates.min()
                    
                    # If End Date < Dec 2025 -> Missing Recent Year
                    if max_date < datetime(2025, 12, 1):
                        stale_symbols.append((symbol, max_date))
                    else:
                        # New Listing check
                        if len(df) < 600:
                            complete_new_listings.append((symbol, len(df)))
                        
        except:
            pass
            
    stale_symbols.sort(key=lambda x: x[0])
    complete_new_listings.sort(key=lambda x: x[0])

    with open(OUTPUT_FILE, 'w') as f:
        f.write("# Manual Download Tasks (Updated)\n\n")
        f.write("**Status Verification:**\n")
        f.write(f"- **{len(complete_new_listings)} Symbols** are **COMPLETE** (New FNO Listings, e.g. RVNL, SWIGGY). No action needed.\n")
        f.write(f"- **{len(stale_symbols)} Symbols** are **STALE** (Missing 2025-2026). See below.\n\n")
        
        f.write("## 1. Missing RECENT Data (Priority: High)\n")
        f.write("These symbols stopped updating in Jan 2025. Please download the **Current Year**.\n\n")
        f.write("| Symbol | Last Date | Download Year 1 (Recent) |\n")
        f.write("|---|---|---|\n")
        
        for sym, last_dt in stale_symbols:
            start_fetch = last_dt + timedelta(days=1)
            end_fetch = CURRENT_DATE
            link = get_link(sym, start_fetch, end_fetch)
            f.write(f"| **{sym}** | {last_dt.strftime('%Y-%m-%d')} | {link} |\n")
            
        f.write("\n## 2. Missing Older Data\n")
        f.write("*(Previous Adani/Year 2 links are still valid but lower priority if you want recent analysis)*\n")

    print(f"Updated Guide. Found {len(stale_symbols)} stale.")

if __name__ == "__main__":
    run()
