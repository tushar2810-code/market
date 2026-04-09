
import os
import sys
import logging
from download_nse_robust import RobustNSESession
from ensure_data_completeness import download_year_by_year

# Ensure execution dir is in path
sys.path.append(os.path.abspath('execution'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = '.tmp/3y_data'

def fetch_one(symbol):
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Force fresh session to avoid stale cookies from env
    downloader = RobustNSESession(cookies_string=None)
    if not downloader.initialize():
        logger.error("Failed to initialize.")
        return

    logger.info(f"Targeting {symbol}...")
    final_path = download_year_by_year(downloader, symbol, DATA_DIR, start_year=2015)
    
    if final_path:
        logger.info(f"SUCCESS: Saved to {final_path}")
    else:
        logger.error(f"FAILURE: Could not download {symbol}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        fetch_one(sys.argv[1])
    else:
        print("Usage: python fetch_one.py <SYMBOL>")
