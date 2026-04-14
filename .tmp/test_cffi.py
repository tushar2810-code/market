from curl_cffi import requests
import json
import pandas as pd
from datetime import datetime, timedelta

def get_nse_actions():
    session = requests.Session(impersonate="chrome")
    
    # First get cookies
    try:
        session.get("https://www.nseindia.com/", timeout=10)
    except Exception as e:
        print(f"Failed to get homepage: {e}")
        return

    # Check the API endpoint
    # The API for corporate actions requires date range or from_date/to_date
    date_to = datetime.now()
    date_from = date_to - timedelta(days=730) # 2 years max usually for NSE APi
    
    url = "https://www.nseindia.com/api/corporates-corporateActions"
    params = {
        'index': 'equities',
        'from_date': date_from.strftime('%d-%m-%Y'),
        'to_date': date_to.strftime('%d-%m-%Y')
    }
    headers = {
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-actions',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        resp = session.get(url, params=params, headers=headers, timeout=10)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                print(f"Got {len(data)} items")
            else:
                print(f"Got keys: {data.keys() if isinstance(data, dict) else 'unknown'}")
        else:
            print(resp.text[:200])
    except Exception as e:
        print(f"Failed to get API: {e}")

if __name__ == "__main__":
    get_nse_actions()
