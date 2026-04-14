"""
Deep Dive NSE Historical Data Downloader (3 Years).
Fetches 3 years of data in 365-day chunks and saves as proper CSV.
"""
import requests
import pandas as pd
import os
import time
import random
import logging
from datetime import datetime, timedelta
import io
import json
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
            # REMOVED 'br' to avoid binary responses
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

    def fetch_data_chunk(self, symbol, start_date, end_date):
        """Fetch data for a specific date range."""
        from_str = start_date.strftime('%d-%m-%Y')
        to_str = end_date.strftime('%d-%m-%Y')

        # 1. Warm-up (Visit Symbol Page)
        quote_url = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        try:
            self.session.headers.update({
                'Referer': self.base_url,
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate'
            })
            self.session.get(quote_url, timeout=20)
        except Exception:
            pass # Ignore warm-up failures

        # 2. API Request
        api_url = f"{self.base_url}/api/NextApi/apiClient/GetQuoteApi"
        params = {
            'functionName': 'getDerivativesHistoricalData',
            'symbol': symbol,
            'instrumentType': 'FUTSTK',
            'fromDate': from_str,
            'toDate': to_str,
            'csv': 'true'
        }
        
        # Update headers for API
        api_headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': quote_url,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        
        try:
            response = self.session.get(api_url, params=params, headers=api_headers, timeout=30)
            if response.status_code == 200:
                # Try to parse JSON
                try:
                    data = response.json()
                    df = pd.DataFrame(data)
                    return df
                except json.JSONDecodeError:
                    # Maybe it returned CSV text?
                    return pd.read_csv(io.StringIO(response.text))
            else:
                logger.warning(f"Chunk failed ({from_str} to {to_str}): HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching chunk: {e}")
            return None

    def download_3y_data(self, symbol, output_dir='.tmp/3y_data'):
        """Download and merge 3 years of data."""
        all_dfs = []
        current_date = datetime.now()
        
        # 3 chunks of 365 days
        for i in range(3):
            end_date = current_date - timedelta(days=i*365)
            start_date = end_date - timedelta(days=364) # 365 days window roughly
            
            # Avoid overlap with next chunk if strictly 365
            # Chunk 1: Today to Today-365
            # Chunk 2: Today-365-1 to Today-365-1-365
            end_date = current_date - timedelta(days=(i * 365) + (1 if i > 0 else 0))
            start_date = end_date - timedelta(days=364)

            logger.info(f"  Fetching chunk {i+1}: {start_date.date()} to {end_date.date()}")
            
            df = self.fetch_data_chunk(symbol, start_date, end_date)
            if df is not None and not df.empty:
                # Basic cleaning
                if 'FH_INSTRUMENT' in df.columns:
                     df = df[df['FH_INSTRUMENT'] == 'FUTSTK']
                all_dfs.append(df)
            
            # Brief delay between chunks
            time.sleep(1)
            
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            # Remove duplicates if any
            if 'FH_TIMESTAMP' in combined_df.columns and 'FH_EXPIRY_DT' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
            
            # Save as CSV
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{symbol}_5Y.csv")
            combined_df.to_csv(output_file, index=False)
            logger.info(f"✓ Saved {symbol} 3Y data ({len(combined_df)} rows)")
            return True
        else:
            logger.warning(f"No data found for {symbol}")
            return False

def run_3y_download_job():
    cookies = os.environ.get('NSE_COOKIES')
    downloader = RobustNSESession(cookies)
    if not downloader.initialize():
        return

    output_dir = '.tmp/3y_data'
    
    # Identify candidates
    symbols_to_download = FNO_SYMBOLS
    
    # Filter existing?
    # For deep dive, maybe check if file exists and retry if small?
    
    logger.info(f"Starting 3-Year Deep Dive for {len(symbols_to_download)} symbols")
    
    for i, symbol in enumerate(symbols_to_download):
        logger.info(f"[{i+1}/{len(symbols_to_download)}] Processing {symbol}...")
        
        downloader.download_3y_data(symbol, output_dir)
        
        # Delay between symbols
        time.sleep(random.uniform(2.0, 4.0))
        
        # Re-init periodically
        if i > 0 and i % 10 == 0:
            downloader.initialize()
            time.sleep(2)

if __name__ == "__main__":
    run_3y_download_job()
