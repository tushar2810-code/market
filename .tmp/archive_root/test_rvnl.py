import pandas as pd
import sys, os
sys.path.append('execution')
from shoonya_client import ShoonyaClient

def check_rvnl(sym_a, sym_b):
    print(f'\nCHECKING {sym_a} / {sym_b}')
    path_a = f'.tmp/3y_data/{sym_a}_3Y.csv'
    path_b = f'.tmp/3y_data/{sym_b}_3Y.csv'
    if not os.path.exists(path_a) or not os.path.exists(path_b):
        print('Missing data for one or both.')
        return
        
    df_a = pd.read_csv(path_a)
    df_b = pd.read_csv(path_b)
    df_a['FH_TIMESTAMP'] = pd.to_datetime(df_a['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df_b['FH_TIMESTAMP'] = pd.to_datetime(df_b['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    
    df_a = df_a.sort_values('FH_TIMESTAMP').groupby('FH_TIMESTAMP').first()
    df_b = df_b.sort_values('FH_TIMESTAMP').groupby('FH_TIMESTAMP').first()
    
    hist = df_a[['FH_CLOSING_PRICE']].join(df_b[['FH_CLOSING_PRICE']], how='inner', lsuffix='_A', rsuffix='_B')
    hist['RATIO'] = hist['FH_CLOSING_PRICE_A'] / hist['FH_CLOSING_PRICE_B']
    
    api = ShoonyaClient().login()
    def get_price(sym):
        ret = api.searchscrip(exchange='NFO', searchtext=sym)
        if not ret or 'values' not in ret: return None
        futs = [x for x in ret['values'] if x['instname'] == 'FUTSTK' and x['symname'] == sym]
        if not futs: return None
        futs.sort(key=lambda x: pd.to_datetime(x['exd'], format='%d-%b-%Y'))
        q = api.get_quotes(exchange='NFO', token=futs[0]['token'])
        if q and 'lp' in q: return float(q['lp'])
        return None
        
    price_a = get_price(sym_a)
    price_b = get_price(sym_b)
    if not price_a or not price_b: 
        print("Live price fetch failed.")
        return
    
    live_ratio = price_a / price_b
    print(f'Live Prices: {sym_a}={price_a}, {sym_b}={price_b}, Ratio={live_ratio:.4f}')
    
    last_date = hist.index[-1] + pd.Timedelta(days=1)
    new_row = pd.DataFrame({'RATIO': [live_ratio]}, index=[last_date])
    combined = pd.concat([hist[['RATIO']], new_row])
    
    for w in [20, 60, 120, 180]:
        if len(combined) > w:
            subset = combined['RATIO'].tail(w)
            z = (live_ratio - subset.mean()) / subset.std()
            print(f'{w}D Z-Score: {z:.2f}')

check_rvnl('RVNL', 'IRFC')
check_rvnl('RVNL', 'NBCC')
