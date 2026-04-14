from curl_cffi import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os

def get_nse_actions():
    session = requests.Session(impersonate="chrome")
    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except:
        pass

    all_data = []
    
    # 0 to 24 months
    dt1 = datetime.now()
    dt2 = dt1 - timedelta(days=700)
    
    # 24 to 36 months
    dt3 = dt2 - timedelta(days=400)
    
    ranges = [
        (dt2, dt1),
        (dt3, dt2)
    ]
    
    url = "https://www.nseindia.com/api/corporates-corporateActions"
    headers = {
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-actions',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    for start, end in ranges:
        params = {
            'index': 'equities',
            'from_date': start.strftime('%d-%m-%Y'),
            'to_date': end.strftime('%d-%m-%Y')
        }
        resp = session.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                all_data.extend(data)

    if not all_data:
        print("No data from NSE")
        return

    # Filter for FNO stocks
    fno_dir = '.tmp/3y_data'
    fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(fno_dir) if f.endswith('_3Y.csv')]
    
    df = pd.DataFrame(all_data)
    df = df[df['symbol'].isin(fno_tickers)]
    df = df.drop_duplicates(subset=['symbol', 'exDate', 'subject'])
    
    # Process actions
    results = []
    for _, row in df.iterrows():
        subj = str(row.get('subject', '')).lower()
        symbol = row.get('symbol')
        date = row.get('exDate')
        if date == '-' or not date:
            continue
            
        action_type = "Other"
        val = 0
        if "split" in subj or "sub-division" in subj or "sub division" in subj:
            action_type = "Split"
        elif "bonus" in subj:
            action_type = "Bonus"
        elif "dividend" in subj:
            action_type = "Dividend"
            
        if action_type in ["Split", "Bonus", "Dividend"]:
            results.append({
                'Ticker': symbol,
                'Date': date,
                'Type': action_type,
                'Description': row.get('subject')
            })
            
    res_df = pd.DataFrame(results)
    res_df['Date'] = pd.to_datetime(res_df['Date'], format='%d-%b-%Y')
    res_df = res_df.sort_values(by=['Ticker', 'Date'])
    
    res_df.to_csv('.tmp/nse_perfect_actions.csv', index=False)
    
    splits = res_df[res_df['Type'] == 'Split']
    bonuses = res_df[res_df['Type'] == 'Bonus']
    divs = res_df[res_df['Type'] == 'Dividend']
    
    print(f"Total: {len(res_df)}")
    print(f"Splits: {len(splits)}")
    print(f"Bonuses: {len(bonuses)}")
    print(f"Dividends: {len(divs)}")

if __name__ == "__main__":
    get_nse_actions()
