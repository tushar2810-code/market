from execution.shoonya_client import ShoonyaClient
from datetime import datetime
import json

def get_ltp(api, exchange, token):
    ret = api.get_quotes(exchange=exchange, token=str(token))
    if ret and ret.get('stat') == 'Ok':
        return float(ret['lp'])
    return None

def get_expiry_dates(api, exchange, symbol):
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)
    if not ret or 'values' not in ret:
        return []
    
    futures = [
        x for x in ret['values'] 
        if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX') and x['symname'] == symbol
    ]
    
    if not futures:
        return []

    def parse_expiry(x):
        try:
             return datetime.strptime(x['exd'], '%d-%b-%Y')
        except:
             return datetime.max

    futures.sort(key=parse_expiry)
    return futures

def get_rvnl_data():
    client = ShoonyaClient()
    client.login()
    api = client.api

    symbol = 'RVNL'
    print(f"Fetching Live Data for {symbol}...\n")
    
    # Get Spot
    spot_ret = api.searchscrip(exchange='NSE', searchtext=f"{symbol}-EQ")
    if spot_ret and spot_ret.get('stat') == 'Ok':
        spot_token = spot_ret['values'][0]['token']
        spot_price = get_ltp(api, 'NSE', spot_token)
        print(f"Spot Price          : {spot_price}")
    else:
        spot_price = None
        print("Could not get Spot Price")

    futures = get_expiry_dates(api, 'NFO', symbol)
    if len(futures) < 2:
        print("Not enough futures found.")
        return
        
    near_fut = futures[0]
    far_fut = futures[1]

    near_price = get_ltp(api, 'NFO', near_fut['token'])
    far_price = get_ltp(api, 'NFO', far_fut['token'])
    
    lot_size = float(near_fut.get('ls', 0))
    near_expiry = near_fut.get('exd', 'Unknown')
    far_expiry = far_fut.get('exd', 'Unknown')
    
    near_tsym = near_fut.get('tsym', 'Unknown')
    far_tsym = far_fut.get('tsym', 'Unknown')

    if near_price and far_price:
        spread = far_price - near_price
        print(f"\n--- LIVE RVNL DATA ---")
        print(f"Near Price ({near_tsym}): {near_price}")
        print(f"Far Price ({far_tsym}) : {far_price}")
        print(f"Live Spread         : {spread:.2f}")
        print(f"Lot Size            : {lot_size:,.0f}")
        print(f"1 Point Value       = ₹{lot_size:,.0f}")
        print(f"Total Gain/Loss/Pt  = ₹{lot_size:,.0f}")
        print(f"Spread Value        = ₹{abs(spread) * lot_size:,.0f}")
    else:
        print("Failed to get quotes.")

if __name__ == '__main__':
    get_rvnl_data()
