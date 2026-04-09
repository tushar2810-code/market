
import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta
import glob
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Add execution dir to path
sys.path.append(os.path.join(os.getcwd(), 'execution'))

try:
    from download_nse_robust import RobustNSESession
except ImportError:
    logger.error("Could not import RobustNSESession")
    sys.exit(1)

DATA_DIR = '.tmp/3y_data'
TEMP_DIR = '.tmp/partial_downloads'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

def check_and_retry():
    # 1. Check all files
    files = glob.glob(os.path.join(DATA_DIR, "*_3Y.csv"))
    stale_symbols = [] # List of (symbol, last_date)
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    logger.info(f"Checking {len(files)} files for staleness...")
    
    existing_symbols = set()

    for f in files:
        symbol = os.path.basename(f).replace('_3Y.csv', '')
        existing_symbols.add(symbol)
        try:
            df = pd.read_csv(f)
            if df.empty:
                logger.warning(f"{symbol}: Empty file")
                stale_symbols.append((symbol, None))
                continue
                
            # Clean column names
            df.columns = [c.strip() for c in df.columns]
            
            if 'FH_TIMESTAMP' not in df.columns and 'TIMESTAMP' not in df.columns:
                 logger.warning(f"{symbol}: No timestamp column")
                 stale_symbols.append((symbol, None))
                 continue
                 
            col = 'FH_TIMESTAMP' if 'FH_TIMESTAMP' in df.columns else 'TIMESTAMP'
            df[col] = pd.to_datetime(df[col], format='%d-%b-%Y', errors='coerce')
            
            last_date = df[col].max().date()
            days_diff = (today - last_date).days
            
            if days_diff > 3:
                logger.warning(f"{symbol}: Stale (Last: {last_date}, {days_diff} days ago)")
                stale_symbols.append((symbol, last_date))
            else:
                pass
                
        except Exception as e:
            logger.error(f"{symbol}: Error reading file: {e}")
            stale_symbols.append((symbol, None))

    if not stale_symbols:
        logger.info("All symbols are fresh!")
        return

    logger.info(f"Found {len(stale_symbols)} stale/missing symbols. Retrying with Robust Downloader (Incremental)...")
    
    # 2. Retry with Robust Downloader
    downloader = RobustNSESession()
    if not downloader.initialize():
        logger.error("Failed to initialize robust downloader")
        return

    success = 0
    
    sorted_stale = sorted(stale_symbols, key=lambda x: x[0])
    
    for sym, last_date in sorted_stale:
        # Determine range
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = datetime.now() - timedelta(days=365*3)
            
        end_date = yesterday
        
        if start_date > end_date:
            logger.info(f"{sym}: Up to date (Last: {last_date})")
            continue

        s_str = start_date.strftime('%d-%m-%Y')
        e_str = end_date.strftime('%d-%m-%Y')
        
        logger.info(f"Updating {sym} from {s_str} to {e_str}...")

        # Robust downloader saves as [symbol]_historical.csv (containing JSON)
        if downloader.download_symbol_data(sym, output_dir=TEMP_DIR, custom_start_date=s_str, custom_end_date=e_str):
             json_path = os.path.join(TEMP_DIR, f"{sym}_historical.csv")
             try:
                 with open(json_path, 'r', encoding='utf-8') as f:
                     content = f.read()
                     
                 # Parse JSON
                 try:
                     data = json.loads(content)
                 except json.JSONDecodeError:
                     logger.warning(f"{sym}: Invalid JSON response")
                     continue

                 # Handle potential API wrappers
                 if isinstance(data, dict) and 'data' in data:
                     data = data['data']
                 
                 if isinstance(data, list) and len(data) > 0:
                     new_df = pd.DataFrame(data)
                     
                     # Filter FUTSTK
                     if 'FH_INSTRUMENT' in new_df.columns:
                         new_df = new_df[new_df['FH_INSTRUMENT'] == 'FUTSTK']
                     
                     if not new_df.empty:
                         # Append to main file
                         main_path = os.path.join(DATA_DIR, f"{sym}_3Y.csv")
                         
                         if os.path.exists(main_path):
                             main_df = pd.read_csv(main_path)
                             # Ensure cols match potentially
                             updated_df = pd.concat([main_df, new_df], ignore_index=True)
                         else:
                             updated_df = new_df
                             
                         # Dedup
                         if 'FH_TIMESTAMP' in updated_df.columns and 'FH_EXPIRY_DT' in updated_df.columns:
                             updated_df = updated_df.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
                         
                         updated_df.to_csv(main_path, index=False)
                         logger.info(f"✓ Updated {sym} (+{len(new_df)} rows)")
                         success += 1
                     else:
                         logger.warning(f"{sym}: No FUTSTK data in response")
                 else:
                     logger.warning(f"{sym}: Empty data in response")
                     
                 # Cleanup
                 if os.path.exists(json_path):
                     os.remove(json_path)
                     
             except Exception as e:
                 logger.error(f"{sym}: Processing error: {e}")
        else:
            logger.error(f"Failed download {sym}")

    logger.info(f"Update Complete. Updated {success} symbols.")

if __name__ == "__main__":
    check_and_retry()
