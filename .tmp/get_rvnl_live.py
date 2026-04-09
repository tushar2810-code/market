from execution.shoonya_client import ShoonyaClient
from execution.fno_utils import FNOUtils
import json

def get_rvnl_data():
    client = ShoonyaClient()
    client.login()

    symbol = 'RVNL'
    print(f"Searching for futures of {symbol}...")
    
    # Search symbols to get trading symbol and lot size
    res = client.api.searchscrip(exchange='NFO', searchtext=f'{symbol} FUT')
    if not res or res.get('stat') != 'Ok':
        print(f"Search failed for {symbol}: {res}")
        return

    # Filter for futures only
    futures = [v for v in res['values'] if '-FUT' in v['tsym']]
    
    # Sort by tsym to get near and far
    futures.sort(key=lambda x: x['tsym'])
    
    if len(futures) < 2:
        print(f"Not enough futures found: {futures}")
        return

    near = futures[0]
    far = futures[-1] # Usually 3 futures, picking the furthest one or middle one based on our setup
    # Often it's current month and next month. Let's look at tsyms
    print(f"Available Futures: {[f['tsym'] for f in futures]}")
    
    near = futures[0]
    far = futures[1] # The next month expiry
    
    near_token = near['token']
    near_tsym = near['tsym']
    lot_size = int(near['ls'])
    
    far_token = far['token']
    far_tsym = far['tsym']

    print(f"Near: {near_tsym}")
    print(f"Far : {far_tsym}")
    print(f"Lot Size: {lot_size}")

    # Get quotes
    near_quote = client.api.get_quotes(exchange='NFO', token=near_token)
    far_quote = client.api.get_quotes(exchange='NFO', token=far_token)

    near_price = float(near_quote.get('lp', 0)) if near_quote and near_quote.get('stat') == 'Ok' else 0
    far_price = float(far_quote.get('lp', 0)) if far_quote and far_quote.get('stat') == 'Ok' else 0

    if near_price and far_price:
        spread = far_price - near_price
        print(f"\n--- LIVE RVNL DATA ---")
        print(f"Near Price ({near_tsym}): {near_price}")
        print(f"Far Price ({far_tsym}) : {far_price}")
        print(f"Live Spread         : {spread:.2f}")
        print(f"Lot Size            : {lot_size}")
        print(f"1 Point Value       = ₹{lot_size}")
        print(f"Total Spread Value  = ₹{spread * lot_size:,.2f}")
    else:
        print("Failed to get quotes:")
        print("Near Quote:", json.dumps(near_quote, indent=2))
        print("Far Quote:", json.dumps(far_quote, indent=2))

if __name__ == '__main__':
    get_rvnl_data()
