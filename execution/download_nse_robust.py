"""
Robust NSE Historical Data Downloader.
Implements 'Visit-Then-Fetch' pattern to bypass NSE anti-scraping/session validation.
"""
import requests
import pandas as pd
import os
import time
import random
import logging
from datetime import datetime, timedelta
from fno_utils import FNO_SYMBOLS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RobustNSESession:
    def __init__(self, cookies_string=None):
        self.session = requests.Session()
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        self.session.headers.update(self.headers)
        
        # Inject provided browser cookies
        if cookies_string:
            for cookie in cookies_string.split('; '):
                if '=' in cookie:
                    name, value = cookie.split('=', 1)
                    self.session.cookies.set(name, value, domain='.nseindia.com')
                    self.session.cookies.set(name, value, domain='nseindia.com')
            logger.info("Injected provided browser cookies.")
    
    def initialize(self):
        """Visit homepage to initialize basic cookies."""
        try:
            logger.info("Initializing session (visiting homepage)...")
            self.session.get(self.base_url, timeout=30)
            logger.info("Session initialized.")
            return True
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False

    def download_symbol_data(self, symbol, days_back=365, output_dir='.tmp', custom_start_date=None, custom_end_date=None):
        """
        Download data for a specific symbol.
        1. Visit symbol page (warm-up)
        2. Fetch API data
        """
        # Date definitions
        if custom_start_date and custom_end_date:
             from_str = custom_start_date
             to_str = custom_end_date
        else:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            from_str = start_date.strftime('%d-%m-%Y')
            to_str = end_date.strftime('%d-%m-%Y')

        # 1. Warm-up Request (Visit Symbol Page)
        quote_url = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        try:
            # Update headers for page view
            self.session.headers.update({
                'Referer': self.base_url,
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate'
            })
            self.session.get(quote_url, timeout=20)
        except Exception as e:
            logger.warning(f"Warm-up failed for {symbol}: {e}")
            # Continue anyway, sometimes API still works if session is alive

        # 2. API Request
        api_url = f"{self.base_url}/api/NextApi/apiClient/GetQuoteApi"
        params = {
            'functionName': 'getDerivativesHistoricalData',
            'symbol': symbol,
            'instrumentType': 'FUTSTK',
            'year': '',
            'expiryDate': '',
            'strikePrice': '',
            'optionType': '',
            'fromDate': from_str,
            'toDate': to_str,
            'csv': 'true'
        }
        
        # Update headers for API call
        # Visit the quote page first (Critical for cookies/session validity)
        try:
            self.session.get(quote_url, headers=self.headers, timeout=10)
        except Exception:
            pass # Proceed even if this fails, though it might affect API

        # Update headers for API call
        # specific minimal headers that work
        api_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            # 'X-Requested-With': 'XMLHttpRequest', # Removed as test_simple.py works without it
            'Referer': quote_url,
        }
        
        try:
            response = self.session.get(api_url, params=params, headers=api_headers, timeout=30)
            
            if response.status_code == 200:
                # Check if valid JSON/Content
                if len(response.content) < 100:  # Too small, likely empty or error
                   logger.warning(f"Response too small for {symbol}: {len(response.content)} bytes")
                   return False

                # Save raw content (it's JSON usually)
                os.makedirs(output_dir, exist_ok=True)
                # Ensure filename is compatible with analysis script (expects _historical.csv)
                # But content is JSON. Analysis script handles this.
                output_file = os.path.join(output_dir, f"{symbol}_historical.csv")
                
                with open(output_file, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✓ Saved {symbol}")
                return True
            else:
                logger.warning(f"Failed {symbol}: HTTP {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error downloading {symbol}: {e}")
            return False

def run_download_job():
    cookies = os.environ.get('NSE_COOKIES')
    downloader = RobustNSESession(cookies)
    # Initialize ensures basic connectivity but respects injected cookies
    if not downloader.initialize():
        return

    # Filter out already downloaded files to save time
    output_dir = '.tmp'
    os.makedirs(output_dir, exist_ok=True)
    existing_files = os.listdir(output_dir)
    
    # Identify which symbols missing
    symbols_to_download = []
    for sym in FNO_SYMBOLS:
        # Check if file exists and is larger than 1KB (valid data)
        fname = f"{sym}_historical.csv"
        fpath = os.path.join(output_dir, fname)
        if fname not in existing_files or os.path.getsize(fpath) < 1000:
            symbols_to_download.append(sym)
    
    logger.info(f"Targeting {len(symbols_to_download)} symbols (skipping existing/valid)")
    
    success_count = 0
    fail_count = 0
    
    for i, symbol in enumerate(symbols_to_download):
        logger.info(f"[{i+1}/{len(symbols_to_download)}] Downloading {symbol}...")
        
        if downloader.download_symbol_data(symbol, output_dir=output_dir):
            success_count += 1
        else:
            fail_count += 1
            
        # Random delay to look human (1 to 3 seconds)
        time.sleep(random.uniform(1.0, 3.0))
        
        # Periodic re-init/home visit to stay fresh (every 20 symbols)
        if i > 0 and i % 20 == 0:
            downloader.initialize()
            time.sleep(2)

    logger.info(f"Batch Complete. Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    run_download_job()
