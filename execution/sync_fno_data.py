"""
Unified FNO Data Sync Tool.
Orchestrates the entire lifecycle: Download -> Merge -> Verify.
"""
import os
import sys
import time
import json
import logging
import random
import glob
import argparse
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

# --- Configuration ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FNOSync")

DATA_DIR = '.tmp/3y_data'
FNO_UTILS_PATH = os.path.join(os.path.dirname(__file__), 'fno_utils.py')

# Import FNO List
try:
    sys.path.append(os.path.dirname(__file__))
    from fno_utils import FNO_SYMBOLS
except ImportError:
    logger.error("Could not import FNO_SYMBOLS from fno_utils.py")
    sys.exit(1)

# Special Cases
PARTIAL_ACCEPTABLE = {
    'SWIGGY', 'WAAREEENER', 'JIOFIN', 'NUVAMA', 'UNOMINDA', 
    'TATAELXSI', 'TIINDIA', 'TORNTPOWER' # Known API limit
}

# --- Browser Logic ---
def get_browser_context(p):
    browser = p.firefox.launch(headless=True)
    context = browser.new_context(
        user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'DNT': '1'
        }
    )
    return browser, context

def init_session(page):
    try:
        page.goto("https://www.nseindia.com", timeout=60000)
        page.wait_for_timeout(3000)
        return True
    except Exception as e:
        logger.warning(f"Session init failed: {e}")
        return False

def download_symbol(symbol):
    """Downloads 3 years of data for a single symbol."""
    logger.info(f"Starting download for {symbol}...")
    
    with sync_playwright() as p:
        try:
            browser, context = get_browser_context(p)
        except Exception as e:
            logger.error(f"Browser launch failed for {symbol}: {e}")
            return False

        page = context.new_page()
        if not init_session(page):
            browser.close()
            return False

        all_dfs = []
        success = True

        # Years: 0 (Current), 1 (Last), 2 (2 years ago)
        for year in range(3):
            # Known Limitation Check
            if symbol == 'TORNTPOWER' and year > 0:
                logger.info(f"Skipping older data for {symbol} (Known Limitation)")
                continue

            end_date = datetime.now() - timedelta(days=year*365)
            start_date = end_date - timedelta(days=364)
            
            from_str = start_date.strftime('%d-%m-%Y')
            to_str = end_date.strftime('%d-%m-%Y')
            
            url = f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?functionName=getDerivativesHistoricalData&symbol={symbol}&instrumentType=FUTSTK&fromDate={from_str}&toDate={to_str}&csv=true"
            
            retry = 0
            year_done = False
            while retry < 3:
                try:
                    page.goto(url, timeout=30000)
                    content = page.locator("body").inner_text()
                    
                    if "Access Denied" in content or len(content) < 50:
                        retry += 1
                        time.sleep(2)
                        continue
                    
                    try:
                        data = json.loads(content)
                        if isinstance(data, dict):
                            # API Error / Metadata
                            logger.debug(f"[{symbol}] Got dict response: {str(data)[:50]}...")
                        elif isinstance(data, list):
                            df = pd.DataFrame(data)
                            if not df.empty and 'FH_INSTRUMENT' in df.columns:
                                df = df[df['FH_INSTRUMENT'] == 'FUTSTK']
                                all_dfs.append(df)
                                year_done = True
                                break
                    except json.JSONDecodeError:
                        pass # Retrying
                        
                    retry += 1
                    time.sleep(1)
                except Exception as e:
                    logger.debug(f"[{symbol}] Fetch error: {e}")
                    retry += 1
            
            if not year_done:
                logger.warning(f"[{symbol}] Failed to fetch year {year+1}")
                if symbol not in PARTIAL_ACCEPTABLE:
                    success = False
            
            time.sleep(random.uniform(1.0, 2.5))
        
        browser.close()

        # Save
        if all_dfs:
            try:
                combined = pd.concat(all_dfs, ignore_index=True)
                if 'FH_TIMESTAMP' in combined.columns and 'FH_EXPIRY_DT' in combined.columns:
                    combined = combined.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
                
                os.makedirs(DATA_DIR, exist_ok=True)
                out_path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
                combined.to_csv(out_path, index=False)
                logger.info(f"✓ Saved {symbol} ({len(combined)} rows)")
                return True
            except Exception as e:
                logger.error(f"[{symbol}] Save failed: {e}")
                return False
        else:
            logger.warning(f"[{symbol}] No data found")
            return False

# --- Orchestration ---
def run_sync(symbols=None, max_workers=4):
    target_symbols = symbols if symbols else FNO_SYMBOLS
    logger.info(f"Syncing {len(target_symbols)} symbols with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(download_symbol, sym): sym for sym in target_symbols}
        
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"[{sym}] Unexpected error: {e}")

# --- Verification ---
def verify_integrity():
    logger.info("Verifying data integrity...")
    files = glob.glob(os.path.join(DATA_DIR, "*_5Y.csv"))
    
    issues = []
    
    for f in sorted(files):
        symbol = os.path.basename(f).replace('_5Y.csv', '')
        try:
            df = pd.read_csv(f)
            if df.empty:
                issues.append((symbol, "EMPTY"))
                continue
                
            date_col = 'FH_TIMESTAMP' if 'FH_TIMESTAMP' in df.columns else 'TIMESTAMP'
            if date_col not in df.columns:
                issues.append((symbol, "NO_DATE_COL"))
                continue
                
            # Date Check
            dates = pd.Series(pd.to_datetime(df[date_col], dayfirst=True, errors='coerce').unique())
            dates = dates.sort_values().values
            
            if len(dates) < 10:
                issues.append((symbol, "TOO_FEW_ROWS"))
                continue
                
            # Gap Check
            diffs = (dates[1:] - dates[:-1]).astype('timedelta64[D]').astype(int)
            max_gap = diffs.max() if len(diffs) > 0 else 0
            
            if max_gap > 10:
                if symbol not in PARTIAL_ACCEPTABLE:
                    issues.append((symbol, f"GAP_{max_gap}D"))
            
        except Exception as e:
            issues.append((symbol, f"ERROR_{str(e)}"))

    if issues:
        logger.warning(f"Found {len(issues)} issues:")
        for sym, reason in issues:
            logger.warning(f"  {sym}: {reason}")
    else:
        logger.info("All files Verified Healthy.")

# --- CLI ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FNO Data Sync")
    parser.add_argument("--verify-only", action="store_true", help="Run only verification")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to sync")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    
    args = parser.parse_args()
    
    if args.verify_only:
        verify_integrity()
    else:
        run_sync(args.symbols, args.workers)
        verify_integrity()
