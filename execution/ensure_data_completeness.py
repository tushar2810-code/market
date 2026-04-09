
import os
import sys
import pandas as pd
from datetime import datetime
import time
import random
import logging

# Ensure execution dir is in path
sys.path.append(os.path.abspath('execution'))

try:
    from fno_utils import FNO_SYMBOLS
except ImportError:
    print("Could not import FNO_SYMBOLS. Make sure execution/fno_utils.py exists.")
    FNO_SYMBOLS = []

from download_nse_robust import RobustNSESession
import download_by_expiry

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = '.tmp/3y_data'
CUSTOM_START_DATE = '01-01-2015'

def is_partial_data(filepath):
    """
    Check if a file likely contains partial data.
    Criteria:
    1. File doesn't exist (implied by caller).
    2. Size is very small (< 2KB).
    3. Row count is low (< 1000 rows ~ 4 years approx).
    """
    if not os.path.exists(filepath):
        return True, "Missing"
        
    if os.path.getsize(filepath) < 2000:
         return True, "Too Small"
         
    try:
        df = pd.read_csv(filepath)
        rows = len(df)
        if rows < 1000:
            return True, f"Low Rows ({rows})"
            
        # Optional: Check start date if needed, but row count is a good proxy for "Full History"
        
    except Exception as e:
        return True, f"Corrupt ({e})"
        
    return False, "Complete"

def download_year_by_year(downloader, symbol, output_dir, start_year=2015):
    """
    Downloads data year by year to avoid 404s on huge ranges or invalid start dates.
    Stops when it hits 3 consecutive years of no data or reaches current year.
    Returns the path to the combined file if successful.
    """
    current_year = datetime.now().year
    years_to_download = range(current_year, start_year - 1, -1) # Descending order
    
    temp_files = []
    consecutive_failures = 0
    
    logger.info(f"  > Starting incremental download for {symbol} ({start_year}-{current_year})...")
    
    for year in years_to_download:
        start_date = f"01-01-{year}"
        end_date = f"31-12-{year}"
        
        # Adjust for current year future
        if year == current_year:
            end_date = datetime.now().strftime('%d-%m-%Y')
            
        # Download for this specific year
        # We use a temporary subdirectory to not clutter
        year_dir = os.path.join(output_dir, 'temp_years')
        os.makedirs(year_dir, exist_ok=True)
        
        success = downloader.download_symbol_data(
            symbol,
            output_dir=year_dir,
            custom_start_date=start_date,
            custom_end_date=end_date
        )
        
        if success:
            # The file is saved as {symbol}_historical.csv in year_dir
            # We rename it to preserve it
            src = os.path.join(year_dir, f"{symbol}_historical.csv")
            dst = os.path.join(year_dir, f"{symbol}_{year}.csv")
            if os.path.exists(src):
                os.rename(src, dst)
                # Verify it has meaningful data
                try:
                    df = pd.read_csv(dst)
                    if len(df) > 5:
                        temp_files.append(dst)
                        consecutive_failures = 0
                        logger.info(f"    + {year}: Found {len(df)} rows")
                    else:
                        logger.warning(f"    ~ {year}: Empty/header only ({len(df)} rows). Retrying last month...")
                        # Fallback: Try downloading just Dec (or current month if current year)
                        # This handles "Listed in Nov/Dec" cases where full year request fails
                        fallback_start = f"01-12-{year}"
                        fallback_end = f"31-12-{year}"
                        if year == current_year:
                             fallback_end = datetime.now().strftime('%d-%m-%Y')
                             
                        success_fallback = downloader.download_symbol_data(
                            symbol,
                            output_dir=year_dir,
                            custom_start_date=fallback_start,
                            custom_end_date=fallback_end
                        )
                        
                        if success_fallback:
                            # It overwrites {symbol}_historical.csv, we rename to overwrite {symbol}_{year}.csv
                            fallback_src = os.path.join(year_dir, f"{symbol}_historical.csv")
                            if os.path.exists(fallback_src):
                                os.replace(fallback_src, dst)
                                df_fallback = pd.read_csv(dst)
                                if len(df_fallback) > 5:
                                    temp_files.append(dst)
                                    consecutive_failures = 0
                                    logger.info(f"    + {year} (Recovered): Found {len(df_fallback)} rows")
                                else:
                                    logger.warning(f"    - {year}: Fallback also empty.")
                                    consecutive_failures += 1
                        else:
                             consecutive_failures += 1
                except:
                    logger.warning(f"    ! {year}: Corrupt CSV")
                    consecutive_failures += 1
        else:
            logger.warning(f"    - {year}: No data/404")
            consecutive_failures += 1
            
        # If we failed 5 years in a row (e.g. spurious future years), stop.
        if consecutive_failures >= 5:
            logger.info(f"    ! Stopping usage of older years after {year}")
            break
            
        time.sleep(1) # Small delay between years
        
    if not temp_files:
        return None
        
    # Merge all collected files
    logger.info(f"  > Merging {len(temp_files)} files...")
    all_dfs = []
    for f in temp_files:
        try:
            df = pd.read_csv(f)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
            
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Deduplicate
        if 'FH_TIMESTAMP' in combined_df.columns:
             combined_df.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_OPTION_TYPE', 'FH_STRIKE_PRICE'], inplace=True)
        
        # Sort by Date
        # Try to parse date for sorting
        if 'FH_TIMESTAMP' in combined_df.columns:
            combined_df['__sort_date'] = pd.to_datetime(combined_df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
            combined_df.sort_values('__sort_date', inplace=True)
            combined_df.drop(columns=['__sort_date'], inplace=True)
            
        final_path = os.path.join(output_dir, f"{symbol}_3Y.csv")
        combined_df.to_csv(final_path, index=False)
        return final_path
        
    return None

def ensure_completeness():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Initialize Downloader
    cookies = os.environ.get('NSE_COOKIES')
    downloader = RobustNSESession(cookies)
    if not downloader.initialize():
        logger.error("Failed to initialize downloader. Check connection/cookies.")
        return

    logger.info(f"Auditing {len(FNO_SYMBOLS)} symbols for completeness...")
    
    # Process all symbols for 3-Year Migration
    updates_needed = []
    
    # First pass: Identify who needs update
    for symbol in FNO_SYMBOLS:
        fname = f"{symbol}_3Y.csv"
        fpath = os.path.join(DATA_DIR, fname)
        
        needs_update = False
        if not os.path.exists(fpath):
            needs_update = True
        else:
            try:
                # Quick check: does it have 2023 data?
                # We can check file size as proxy (a full 3Y file is usually >100KB)
                if os.path.getsize(fpath) < 200000: # < 200KB (1 year is ~140KB, so 3 years > 300KB)
                     needs_update = True
                     
                # Detailed check: Read first few lines or deduce from row count
                # 3 years ~ 750 trading days * 3 contracts ~ 2200 rows
                # If rows < 1500, we probably missing 2023
                # But let's verify row count
                # df = pd.read_csv(fpath)
                # if len(df) < 1500: needs_update = True
            except:
                needs_update = True
                
        if needs_update:
            updates_needed.append(symbol)
            
    logger.info(f"Found {len(updates_needed)} symbols needing 3-Year Migration (2023-Present).")
    
    success_count = 0
    fail_count = 0
    
    for i, symbol in enumerate(updates_needed):
        logger.info(f"[{i+1}/{len(updates_needed)}] Migrating {symbol} (Expiry Method 2023+)...")
        
        try:
            # Enforce start_year=2023
            download_by_expiry.main(symbol, start_year=2023)
            
            # Verify result
            final_path = os.path.join(DATA_DIR, f"{symbol}_3Y.csv")
            if os.path.exists(final_path) and os.path.getsize(final_path) > 10000:
                logger.info(f"✓ Migrated {symbol}")
                success_count += 1
            else:
                logger.error(f"Failed to migrate {symbol}")
                fail_count += 1
                
        except Exception as e:
             logger.error(f"Exception for {symbol}: {e}")
             fail_count += 1
             
        # Rate limit
        time.sleep(random.uniform(1.0, 3.0))

    logger.info(f"Migration Complete. Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    ensure_completeness()
