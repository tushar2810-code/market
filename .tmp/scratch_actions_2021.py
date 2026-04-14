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
    
    # 2021-01-01 to 2023-04-01 in chunks
    dt_end = datetime(2023, 4, 1)
    dt_start = datetime(2021, 1, 1)
    
    ranges = []
    cur_start = dt_start
    while cur_start < dt_end:
        cur_end = cur_start + timedelta(days=365)
        if cur_end > dt_end:
            cur_end = dt_end
        ranges.append((cur_start, cur_end))
        cur_start = cur_end
    
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
        try:
            resp = session.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    all_data.extend(data)
        except Exception as e:
            continue

    if not all_data:
        print("No NSE data found for 2021-2022. Trying YFinance fallback.")
        fetch_yf_actions()
        return

    # Filter for FNO stocks
    fno_dir = '.tmp/3y_data'
    fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(fno_dir) if f.endswith('_3Y.csv')]
    
    df = pd.DataFrame(all_data)
    if 'symbol' not in df.columns:
        print("Symbol column not in NSE data. Trying YFinance fallback.")
        fetch_yf_actions()
        return
        
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
            
    if results:
        res_df = pd.DataFrame(results)
        res_df['Date'] = pd.to_datetime(res_df['Date'], format='%d-%b-%Y')
        res_df = res_df.sort_values(by=['Ticker', 'Date'])
        
        res_df.to_csv('.tmp/nse_perfect_actions_2021.csv', index=False)
        print(f"Saved {len(res_df)} actions to .tmp/nse_perfect_actions_2021.csv")
    else:
        print("No actions matched criteria.")

def fetch_yf_actions():
    print("Fetching actions via YFinance...")
    import yfinance as yf
    fno_dir = '.tmp/3y_data'
    fno_tickers = [f.replace('_3Y.csv', '') for f in os.listdir(fno_dir) if f.endswith('_3Y.csv')]
    results = []
    
    start_dt = pd.Timestamp("2021-01-01", tz='UTC')
    end_dt = pd.Timestamp("2023-04-01", tz='UTC')
    
    for symbol in fno_tickers:
        try:
            t = yf.Ticker(f"{symbol}.NS")
            actions = t.actions
            if actions is not None and not actions.empty:
                actions.index = pd.to_datetime(actions.index, utc=True)
                actions = actions[(actions.index >= start_dt) & (actions.index <= end_dt)]
                for idx, row in actions.iterrows():
                    d = row.get('Dividends', 0)
                    s = row.get('Stock Splits', 0)
                    if s > 0:
                        results.append({'Ticker': symbol, 'Date': idx.strftime('%Y-%m-%d'), 'Type': 'Split/Bonus', 'Description': f'Split/Bonus {s}'})
                    if d > 0:
                        results.append({'Ticker': symbol, 'Date': idx.strftime('%Y-%m-%d'), 'Type': 'Dividend', 'Description': f'Dividend {d}'})
        except:
            pass
    if results:
        df = pd.DataFrame(results)
        df.to_csv('.tmp/nse_perfect_actions_2021.csv', index=False)
        print(f"Saved {len(df)} YFinance actions to .tmp/nse_perfect_actions_2021.csv")
    else:
        print("No YFinance actions found either.")

if __name__ == "__main__":
    get_nse_actions()
