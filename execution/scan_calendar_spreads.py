import logging
import argparse
import concurrent.futures
import pandas as pd
from datetime import datetime
from shoonya_client import ShoonyaClient
from fno_utils import FNO_SYMBOLS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_expiry_dates(api, exchange, symbol):
    """
    Search for all Futures of a symbol and return sorted expiry dates.
    """
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)
    if not ret or 'values' not in ret:
        return []
    
    # Filter for exact symbol name match to avoid partial matches (e.g. LT vs LTIM, PNB vs PNBHOUSING)
    futures = [
        x for x in ret['values'] 
        if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX') and x['symname'] == symbol
    ]
    
    if not futures:
        return []
        
    # DEBUG: Print first item to see keys
    # print(f"DEBUG: Future keys: {futures[0].keys()}")
    # print(f"DEBUG: Future sample: {futures[0]}")
    # Found issue: likely key is not 'expd' or format issue.
    # Let's try to inspect safely. 


    # Sort by valid expiry date
    # exd format usually: '02-DEC-2021' or '27-JAN-2022'
    
    def parse_expiry(x):
        try:
             # Shoonya expiry format usually 'dd-MMM-yyyy' e.g. '28-JAN-2026'
             return datetime.strptime(x['exd'], '%d-%b-%Y')
        except:
             return datetime.max

    futures.sort(key=parse_expiry)
    
    # Return unique sorted futures objects
    return futures

def get_ltp(api, exchange, token):
    ret = api.get_quotes(exchange, token)
    if ret and 'lp' in ret:
        return float(ret['lp'])
    return None

def process_symbol(api, symbol, threshold, index, total):
    try:
        if index % 20 == 0:
            logger.info(f"Scanning progress: ~{index}/{total}")

        # 1. Get Spot Price
        spot_token = None
        search_res = api.searchscrip(exchange='NSE', searchtext=symbol)
        if search_res and 'values' in search_res:
            for res in search_res['values']:
                if res['tsym'] == f"{symbol}-EQ" or res['tsym'] == symbol:
                    spot_token = res['token']
                    break

        if not spot_token:
            return None

        spot_price = get_ltp(api, 'NSE', spot_token)
        if not spot_price:
            return None

        # 2. Get Futures — exclude today's expiry (settlement risk on expiry day)
        today = datetime.now().date()
        futures = get_expiry_dates(api, 'NFO', symbol)
        valid_futures = []
        for f in futures:
            try:
                exp_date = datetime.strptime(f['exd'], '%d-%b-%Y').date()
                if exp_date > today:          # strictly future, never same-day
                    valid_futures.append((exp_date, f))
            except:
                pass

        if len(valid_futures) < 2:
            return None

        valid_futures.sort(key=lambda x: x[0])
        near_dte, near_fut = valid_futures[0]
        _, far_fut = valid_futures[1]

        near_price = get_ltp(api, 'NFO', near_fut['token'])
        far_price = get_ltp(api, 'NFO', far_fut['token'])
        if not near_price or not far_price:
            return None

        lot_size = float(near_fut.get('ls', 0))
        if lot_size == 0:
            return None

        # 3. Spread = far_premium − near_premium = far_price − near_price
        spread_diff = (far_price - spot_price) - (near_price - spot_price)  # = far - near
        if spread_diff >= 0:          # only backwardation (near > far)
            return None

        days_to_near_expiry = (near_dte - today).days
        potential_gain = abs(spread_diff) * lot_size

        if potential_gain >= threshold:
            print(f">>> FOUND: {symbol} | DTE={days_to_near_expiry}d | "
                  f"Gain: {potential_gain:.0f} | Spread: {spread_diff:.2f}")

        return {
            'Symbol': symbol,
            'Spot': spot_price,
            'Lot_Size': int(lot_size),
            'Near_Expiry': near_fut['exd'],
            'Near_DTE': days_to_near_expiry,
            'Near_Price': near_price,
            'Far_Expiry': far_fut['exd'],
            'Far_Price': far_price,
            'Spread_Diff': round(spread_diff, 2),
            'Potential_Gain': round(potential_gain, 2),
        }
    except Exception as e:
        logger.error(f"Error scanning {symbol}: {e}")
        return None

def scan_spreads(limit=None, threshold=20000, start_index=0):
    client = ShoonyaClient()
    api = client.login()
    if not api:
        return

    symbols_to_scan = FNO_SYMBOLS
    if limit:
        symbols_to_scan = FNO_SYMBOLS[:limit]
    
    # Slice from start_index
    if start_index > 0:
        symbols_to_scan = symbols_to_scan[start_index:]

    logger.info(f"Scanning {len(symbols_to_scan)} symbols (from index {start_index}) for >{threshold} gain using threads...")
    
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Map futures
        future_to_symbol = {
            executor.submit(process_symbol, api, symbol, threshold, i+1, len(symbols_to_scan)): symbol 
            for i, symbol in enumerate(symbols_to_scan)
        }
        
        for future in concurrent.futures.as_completed(future_to_symbol):
            res = future.result()
            if res:
                results.append(res)

    # Save all results
    df = pd.DataFrame(results)
    if not df.empty:
        # Append to existing if resuming? No, overwriting for now or separate file.
        high_potential = df[df['Potential_Gain'] >= threshold].sort_values('Potential_Gain', ascending=False)
        if not high_potential.empty:
            print(f"\n*** HIGH POTENTIAL OPPORTUNITIES (>{threshold} Gain) ***")
            print(high_potential[['Symbol', 'Lot_Size', 'Near_Price', 'Far_Price', 'Spread_Diff', 'Potential_Gain']].to_string(index=False))
            return high_potential
    return pd.DataFrame()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan for Calendar Spread Opportunities')
    parser.add_argument('--limit', type=int, help='Limit number of symbols')
    parser.add_argument('--threshold', type=float, default=20000, help='Minimum potential gain')
    parser.add_argument('--start_index', type=int, default=0, help='Start index for resume')
    args = parser.parse_args()
    
    scan_spreads(args.limit, args.threshold, args.start_index)
