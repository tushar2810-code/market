#!/usr/bin/env python3
"""
Scrape corporate actions from NSE webpage directly.
"""

from curl_cffi import requests as curl_requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import sys

sys.path.insert(0, "/Users/tushar/Documents/Antigravity FNO")
from execution.fno_utils import FNO_SYMBOLS

SESSION = curl_requests.Session(impersonate="chrome")


def fetch_page(page_num=0, date_range="3M"):
    """Fetch corporate actions page."""
    url = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
    params = {
        "page": page_num,
        "category": "equity",
        "dateRange": date_range,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        resp = SESSION.get(url, params=params, headers=headers, timeout=30)
        return resp.text
    except Exception as e:
        print(f"Error: {e}")
    return ""


def parse_html(html):
    """Parse corporate actions from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    data = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
        if cells and len(cells) == len(headers):
            row_dict = dict(zip(headers, cells))
            data.append(row_dict)
    return data


def main():
    print("Fetching corporate actions from NSE...")

    all_actions = []
    date_ranges = ["3M", "6M", "12M", "24M", "Forthcoming"]

    for date_range in date_ranges:
        print(f"Fetching {date_range}...")
        html = fetch_page(date_range=date_range)
        if html:
            data = parse_html(html)
            if data:
                print(f"  Got {len(data)} rows")
                all_actions.extend(data)
            else:
                print(f"  No data parsed")
        else:
            print(f"  No HTML")

    if all_actions:
        df = pd.DataFrame(all_actions)
        df = df.drop_duplicates()

        output_path = (
            "/Users/tushar/Documents/Antigravity FNO/.tmp/corporate_actions.csv"
        )
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} actions to {output_path}")
    else:
        print("Trying alternate method...")

        url = "https://www.nseindia.com/all-reports/corporate-filing"
        resp = SESSION.get(url, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"URL: {resp.url}")


if __name__ == "__main__":
    main()
