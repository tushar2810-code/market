import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import time

sys.path.append(os.path.join(os.getcwd(), 'execution'))
from download_nse_3y import RobustNSESession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_indices_nse():
    cookies = os.environ.get('NSE_COOKIES')
    downloader = RobustNSESession(cookies)
    if not downloader.initialize():
        return

    output_dir = '.tmp/3y_data'
    symbols = ['NIFTY', 'BANKNIFTY']
    
    for symbol in symbols:
        logger.info(f"Fetching 3Y data for {symbol} (FUTIDX)...")
        all_dfs = []
        current_date = datetime.now()
        
        # 3 chunks of 365 days
        for i in range(3):
            end_date = current_date - timedelta(days=(i * 365) + (1 if i > 0 else 0))
            start_date = end_date - timedelta(days=364)
            logger.info(f"  Chunk {i+1}: {start_date.date()} to {end_date.date()}")
            
            # Custom fetch for FUTIDX
            from_str = start_date.strftime('%d-%m-%Y')
            to_str = end_date.strftime('%d-%m-%Y')

            # Warm-up
            quote_url = f"{downloader.base_url}/get-quotes/derivatives?symbol={symbol}"
            try:
                downloader.session.headers.update({
                    'Referer': downloader.base_url,
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'navigate'
                })
                downloader.session.get(quote_url, timeout=20)
            except:
                pass

            api_url = f"{downloader.base_url}/api/NextApi/apiClient/GetQuoteApi"
            params = {
                'functionName': 'getDerivativesHistoricalData',
                'symbol': symbol,
                'instrumentType': 'FUTIDX',  # IMPORTANT: FUTIDX for NIFTY/BANKNIFTY
                'fromDate': from_str,
                'toDate': to_str,
                'csv': 'true'
            }
            
            api_headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': quote_url,
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
            }
            
            try:
                response = downloader.session.get(api_url, params=params, headers=api_headers, timeout=30)
                if response.status_code == 200:
                    import io, json
                    try:
                        data = response.json()
                        df = pd.DataFrame(data)
                    except json.JSONDecodeError:
                        df = pd.read_csv(io.StringIO(response.text))
                        
                    if not df.empty:
                        all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error fetching chunk: {e}")
                
            time.sleep(2)
            
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            if 'FH_TIMESTAMP' in combined_df.columns and 'FH_EXPIRY_DT' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
                
            # Sort chronologically
            combined_df['temp_sort'] = pd.to_datetime(combined_df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
            combined_df = combined_df.sort_values('temp_sort')
            combined_df.drop(columns=['temp_sort'], inplace=True)
            
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{symbol}_5Y.csv")
            combined_df.to_csv(output_file, index=False)
            logger.info(f"✓ Saved {symbol} 3Y data ({len(combined_df)} rows)")
        else:
            logger.warning(f"No data found for {symbol}")
            
if __name__ == "__main__":
    fetch_indices_nse()
