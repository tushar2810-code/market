"""
NSE Session Handler — Shared module for all NSE data fetching.

Handles cookies, rate limiting, and provides typed fetch methods for:
  - CM Bhavcopy (daily equity + delivery data)
  - Bulk/Block Deals
  - Insider Trading (PIT disclosures)
  - FII/DII daily flows

All data is cached to .tmp/ to avoid re-fetching the same date twice.
NSE throttles at ~3 req/sec — default sleep is 0.4s between requests.

Usage:
    from nse_session import NSEDataFetcher
    fetcher = NSEDataFetcher()
    df = fetcher.fetch_bhavcopy('2026-04-04')
    deals = fetcher.fetch_bulk_deals('2026-04-01', '2026-04-04')
"""

import io
import os
import time
import json
import zipfile
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.nseindia.com"
ARCHIVE_URL = "https://archives.nseindia.com"

# Cache directories
TMP_DIR = Path(".tmp")
BHAVCOPY_DIR = TMP_DIR / "bhavcopy"
BULK_DEALS_DIR = TMP_DIR / "bulk_deals"
INSIDER_DIR = TMP_DIR / "insider_data"
FII_DII_DIR = TMP_DIR / "fii_dii"

HEADERS_BROWSER = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

HEADERS_XHR = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Referer': 'https://www.nseindia.com/',
}


class NSEDataFetcher:
    """
    Unified NSE data fetcher with session management and caching.

    Session strategy:
    1. Try homepage visit to get fresh cookies
    2. Fall back to NSE_COOKIES env var if set
    3. All fetches rate-limited to 0.4s apart
    """

    def __init__(self, cookies_string: str = None, sleep_sec: float = 0.4):
        self.session = requests.Session()
        self.sleep_sec = sleep_sec
        self._last_request = 0.0
        self._initialized = False

        # Create cache dirs
        for d in [BHAVCOPY_DIR, BULK_DEALS_DIR, INSIDER_DIR, FII_DII_DIR]:
            d.mkdir(parents=True, exist_ok=True)

        # Load cookies from arg → env → homepage
        if cookies_string:
            self._load_cookie_string(cookies_string)
        elif os.environ.get('NSE_COOKIES'):
            self._load_cookie_string(os.environ['NSE_COOKIES'])

    def _load_cookie_string(self, cookie_str: str):
        for part in cookie_str.split('; '):
            if '=' in part:
                name, value = part.split('=', 1)
                self.session.cookies.set(name.strip(), value.strip(), domain='.nseindia.com')
        self._initialized = True
        logger.info(f"Loaded {len(self.session.cookies)} cookies from string")

    def _init_session(self) -> bool:
        """Visit NSE homepage to establish session cookies."""
        if self._initialized:
            return True
        try:
            logger.info("Initializing NSE session (visiting homepage)...")
            resp = self.session.get(f"{BASE_URL}/", headers=HEADERS_BROWSER, timeout=15)
            if resp.status_code == 200:
                self._initialized = True
                logger.info(f"Session ready. Cookies: {list(self.session.cookies.keys())}")
                time.sleep(1.5)  # NSE needs a moment after homepage
                return True
            else:
                logger.error(f"Homepage returned HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Session init failed: {e}")
            return False

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.sleep_sec:
            time.sleep(self.sleep_sec - elapsed)
        self._last_request = time.time()

    def _get_json(self, url: str, params: dict = None, referer: str = None):
        """GET request returning parsed JSON, with session init + rate limiting."""
        if not self._init_session():
            return None
        self._rate_limit()
        headers = dict(HEADERS_XHR)
        if referer:
            headers['Referer'] = referer
        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 401 or resp.status_code == 403:
                # Try re-initializing session once
                logger.warning(f"HTTP {resp.status_code} — re-initializing session")
                self._initialized = False
                if not self._init_session():
                    return None
                self._rate_limit()
                resp = self.session.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"GET {url} → HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"GET {url} failed: {e}")
            return None

    def _get_bytes(self, url: str, params: dict = None) :
        """GET request returning raw bytes (for CSV downloads)."""
        if not self._init_session():
            return None
        self._rate_limit()
        try:
            resp = self.session.get(url, params=params, headers=HEADERS_BROWSER, timeout=60)
            if resp.status_code == 200:
                return resp.content
            else:
                logger.warning(f"GET {url} → HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"GET {url} failed: {e}")
            return None

    # ─── BHAVCOPY (CM Equity + Delivery) ─────────────────────────────────────

    def fetch_bhavcopy(self, date_str: str):
        """
        Fetch NSE CM bhavcopy with delivery data for a given date.

        Args:
            date_str: Date in 'YYYY-MM-DD' format

        Returns:
            DataFrame with columns:
              SYMBOL, SERIES, CLOSE_PRICE, PREV_CLOSE, TOTAL_TRADED_QTY,
              DELIVERABLE_QTY, DELIVERY_PCT, TURNOVER_LACS, TIMESTAMP
        """
        date = datetime.strptime(date_str, '%Y-%m-%d')
        cache_path = BHAVCOPY_DIR / f"{date_str}.csv"

        if cache_path.exists():
            logger.info(f"Bhavcopy cache hit: {date_str}")
            return pd.read_csv(cache_path)

        # Try NSE archive URL first (no session required for most dates)
        # Format: sec_bhavdata_full_DDMMYYYY.csv
        date_fmt = date.strftime('%d%m%Y')
        archive_url = f"{ARCHIVE_URL}/products/content/sec_bhavdata_full_{date_fmt}.csv"

        self._rate_limit()
        try:
            resp = requests.get(archive_url, headers=HEADERS_BROWSER, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 1000:
                df = self._parse_bhavcopy_csv(resp.content.decode('utf-8', errors='ignore'))
                if df is not None and not df.empty:
                    df['TIMESTAMP'] = date_str
                    df.to_csv(cache_path, index=False)
                    logger.info(f"Bhavcopy fetched from archive: {date_str} ({len(df)} records)")
                    return df
        except Exception as e:
            logger.warning(f"Archive fetch failed for {date_str}: {e}")

        # Fall back to NSE API (requires session)
        date_api = date.strftime('%d-%b-%Y')
        archives_param = json.dumps([{
            "name": "CM - Bhavcopy(csv)",
            "type": "archives",
            "category": "capital-market",
            "section": "equities"
        }])
        url = f"{BASE_URL}/api/reports"
        params = {
            'archives': archives_param,
            'date': date_api,
            'type': 'equities',
            'mode': 'single'
        }
        data = self._get_bytes(url, params)
        if data and len(data) > 500:
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for name in zf.namelist():
                        if name.endswith('.csv'):
                            content = zf.read(name).decode('utf-8', errors='ignore')
                            df = self._parse_bhavcopy_csv(content)
                            if df is not None:
                                df['TIMESTAMP'] = date_str
                                df.to_csv(cache_path, index=False)
                                logger.info(f"Bhavcopy via API: {date_str} ({len(df)} records)")
                                return df
            except Exception:
                # Not a zip, try direct CSV
                df = self._parse_bhavcopy_csv(data.decode('utf-8', errors='ignore'))
                if df is not None:
                    df['TIMESTAMP'] = date_str
                    df.to_csv(cache_path, index=False)
                    return df

        logger.warning(f"Could not fetch bhavcopy for {date_str}")
        return None

    def _parse_bhavcopy_csv(self, content: str):
        """
        Parse NSE CM bhavcopy CSV. Handles two formats:
        1. Old archive format: SYMBOL,SERIES,DATE1,PREV_CLOSE,...,DELIV_QTY,DELIV_PER
        2. New format: SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,...,DELIV_QTY,DELIV_PER
        """
        try:
            df = pd.read_csv(StringIO(content), low_memory=False)
            df.columns = [c.strip().upper() for c in df.columns]

            # Standardize column names across both formats
            # Apply one rename at a time to avoid duplicate column conflicts
            col_map = {
                'TTL_TRD_QNTY': 'TOTAL_TRADED_QTY',
                'TOTTRDQTY': 'TOTAL_TRADED_QTY',
                'TOTAL_TRADED_QUANTITY': 'TOTAL_TRADED_QTY',
                'DELIV_QTY': 'DELIVERABLE_QTY',
                'DELIVERABLE_QUANTITY': 'DELIVERABLE_QTY',
                'DELIV_PER': 'DELIVERY_PCT',
                '% DELY QTY TO TRADED QTY': 'DELIVERY_PCT',
            }
            df.rename(columns=col_map, inplace=True)
            # Handle CLOSE: prefer CLOSE_PRICE, fall back to LAST_PRICE
            if 'CLOSE' not in df.columns:
                if 'CLOSE_PRICE' in df.columns:
                    df.rename(columns={'CLOSE_PRICE': 'CLOSE'}, inplace=True)
                elif 'LAST_PRICE' in df.columns:
                    df.rename(columns={'LAST_PRICE': 'CLOSE'}, inplace=True)
            # Drop any remaining duplicate-named columns by keeping first occurrence
            df = df.loc[:, ~df.columns.duplicated()]

            # Keep only EQ series (exclude BE, SM, N1 etc.)
            if 'SERIES' in df.columns:
                df = df[df['SERIES'].str.strip() == 'EQ']

            # Ensure required columns exist
            required = ['SYMBOL']
            if not all(c in df.columns for c in required):
                return None

            # Numeric conversions
            for col in ['CLOSE', 'PREV_CLOSE', 'TOTAL_TRADED_QTY', 'DELIVERABLE_QTY', 'DELIVERY_PCT', 'TURNOVER_LACS']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df['SYMBOL'] = df['SYMBOL'].str.strip()
            return df.dropna(subset=['SYMBOL'])
        except Exception as e:
            logger.error(f"Bhavcopy parse error: {e}")
            return None

    def fetch_bhavcopy_range(self, days: int = 25):
        """Fetch bhavcopy for last N trading days and concatenate."""
        frames = []
        date = datetime.now()
        fetched = 0
        attempts = 0

        while fetched < days and attempts < days + 15:
            attempts += 1
            # Skip weekends
            if date.weekday() >= 5:
                date -= timedelta(days=1)
                continue
            df = self.fetch_bhavcopy(date.strftime('%Y-%m-%d'))
            if df is not None and not df.empty:
                frames.append(df)
                fetched += 1
            date -= timedelta(days=1)

        if not frames:
            return None
        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"Fetched {fetched} days of bhavcopy ({len(combined)} total records)")
        return combined

    def _parse_deals_archive(self, content: str, deal_type: str):
        """Parse NSE bulk/block CSV archive."""
        from io import StringIO
        df = pd.read_csv(StringIO(content))
        df.columns = [c.strip() for c in df.columns]
        rename = {
            'Date': 'DATE', 'Symbol': 'SYMBOL', 'Security Name': 'SECURITY_NAME',
            'Client Name': 'CLIENT_NAME', 'Buy/Sell': 'BUY_SELL',
            'Quantity Traded': 'QUANTITY', 'Trade Price / Wght. Avg. Price': 'AVG_PRICE',
            'Remarks': 'REMARKS'
        }
        df.rename(columns=rename, inplace=True)
        df['DEAL_TYPE'] = deal_type
        df['QUANTITY'] = pd.to_numeric(df.get('QUANTITY', 0), errors='coerce')
        df['AVG_PRICE'] = pd.to_numeric(df.get('AVG_PRICE', 0), errors='coerce')
        return df

    # ─── BULK / BLOCK DEALS ───────────────────────────────────────────────────

    def _fetch_and_cache_today_bulk(self):
        """
        Fetch today's bulk deals from the NSE archive and save as a dated daily file.
        The archive bulk.csv always contains the most recent trading day's deals.
        Returns today's date string (YYYY-MM-DD) if successful, else None.
        """
        daily_dir = BULK_DEALS_DIR / "daily"
        daily_dir.mkdir(exist_ok=True)

        self._rate_limit()
        try:
            resp = requests.get(f"{ARCHIVE_URL}/content/equities/bulk.csv", headers=HEADERS_BROWSER, timeout=30)
            if resp.status_code != 200 or len(resp.content) < 200:
                return None
            df = self._parse_deals_archive(resp.content.decode('utf-8', errors='ignore'), 'BULK')
            if df is None or df.empty:
                return None

            # Detect the date in the file (all rows should share one date)
            try:
                df['DATE_DT'] = pd.to_datetime(df['DATE'], format='%d-%b-%Y', errors='coerce')
                file_date = df['DATE_DT'].dropna().max()
                if pd.isna(file_date):
                    return None
                date_str = file_date.strftime('%Y-%m-%d')
                df = df.drop(columns=['DATE_DT'])
            except Exception:
                date_str = datetime.now().strftime('%Y-%m-%d')

            daily_file = daily_dir / f"bulk_{date_str}.csv"
            df.to_csv(daily_file, index=False)
            logger.info(f"Bulk deals cached for {date_str}: {len(df)} records → {daily_file}")
            return date_str
        except Exception as e:
            logger.warning(f"Archive bulk daily fetch failed: {e}")
            return None

    def fetch_bulk_deals(self, from_date: str, to_date: str):
        """
        Fetch NSE bulk deals (>0.5% of equity in a single trade).

        Strategy: The NSE archive only provides the CURRENT day's bulk.csv —
        no dated historical files exist. So we save each day's fetch to a dated
        CSV (.tmp/bulk_deals/daily/bulk_YYYY-MM-DD.csv) and combine them for
        range queries. After 30 days of daily runs, the full 30d window is available.

        Args:
            from_date, to_date: 'YYYY-MM-DD' format

        Returns:
            DataFrame with: DATE, SYMBOL, SECURITY_NAME, CLIENT_NAME, BUY_SELL,
                            QUANTITY, AVG_PRICE, DEAL_TYPE
        """
        daily_dir = BULK_DEALS_DIR / "daily"
        daily_dir.mkdir(exist_ok=True)

        from_dt = pd.to_datetime(from_date)
        to_dt = pd.to_datetime(to_date)

        # Always fetch and cache today's archive data
        self._fetch_and_cache_today_bulk()

        # Load all available daily files in the requested range
        frames = []
        for csv_file in sorted(daily_dir.glob("bulk_*.csv")):
            try:
                date_part = csv_file.stem.replace("bulk_", "")
                file_dt = pd.to_datetime(date_part)
                if from_dt <= file_dt <= to_dt:
                    df_day = pd.read_csv(csv_file)
                    frames.append(df_day)
            except Exception:
                continue

        if frames:
            df = pd.concat(frames, ignore_index=True)
            logger.info(f"Bulk deals from archive: {len(df)} records ({len(frames)} days)")
            return df

        logger.warning("No cached bulk deal files found. Running daily will build 30-day history.")
        return None

    def fetch_block_deals(self, from_date: str, to_date: str):
        """
        Fetch NSE block deals (>5L shares or ₹10Cr in off-market window).
        Same schema as bulk deals with DEAL_TYPE='BLOCK'.
        """
        cache_key = f"block_{from_date}_{to_date}.json"
        cache_path = BULK_DEALS_DIR / cache_key

        if cache_path.exists():
            with open(cache_path) as f:
                data = json.load(f)
            return pd.DataFrame(data) if data else None

        from_nse = datetime.strptime(from_date, '%Y-%m-%d').strftime('%d-%m-%Y')
        to_nse = datetime.strptime(to_date, '%Y-%m-%d').strftime('%d-%m-%Y')

        url = f"{BASE_URL}/api/block-deals"
        params = {'from': from_nse, 'to': to_nse}
        data = self._get_json(url, params, referer=f"{BASE_URL}/market-data-reports/block-deals")

        if data is None:
            return None

        records = []
        if isinstance(data, dict):
            records = data.get('data', data.get('BLOCK_DEALS_DATA', []))
        elif isinstance(data, list):
            records = data

        if not records:
            with open(cache_path, 'w') as f:
                json.dump([], f)
            return None

        df = pd.DataFrame(records)
        df['DEAL_TYPE'] = 'BLOCK'

        col_map = {
            'TDATE': 'DATE', 'SYMBOL': 'SYMBOL', 'CLIENT_NAME': 'CLIENT_NAME',
            'BUY_SELL': 'BUY_SELL', 'QUANTITY_TRADED': 'QUANTITY', 'TRADE_PRICE': 'AVG_PRICE',
            'SCRIP_NAME': 'SECURITY_NAME',
        }
        df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

        with open(cache_path, 'w') as f:
            json.dump(df.to_dict('records'), f)

        logger.info(f"Block deals: {len(df)} deals from {from_date} to {to_date}")
        return df

    # ─── INSIDER TRADING (PIT) ────────────────────────────────────────────────

    def fetch_insider_trades(self, from_date: str, to_date: str):
        """
        Fetch SEBI PIT (Prohibition of Insider Trading) disclosures from NSE.

        Returns:
            DataFrame with: DATE, SYMBOL, PERSON_CATEGORY, TRANSACTION_TYPE,
                            QUANTITY, VALUE_LAKHS, MODE_OF_ACQUISITION, PERSON_NAME
        """
        cache_key = f"insider_{from_date}_{to_date}.json"
        cache_path = INSIDER_DIR / cache_key

        if cache_path.exists():
            with open(cache_path) as f:
                data = json.load(f)
            return pd.DataFrame(data) if data else None

        from_nse = datetime.strptime(from_date, '%Y-%m-%d').strftime('%d-%m-%Y')
        to_nse = datetime.strptime(to_date, '%Y-%m-%d').strftime('%d-%m-%Y')

        url = f"{BASE_URL}/api/corporates-pit"
        params = {
            'index': 'equities',
            'from_date': from_nse,
            'to_date': to_nse,
        }
        data = self._get_json(url, params, referer=f"{BASE_URL}/companies-listing/corporate-filings-insider-trading")

        if data is None:
            return None

        records = []
        if isinstance(data, dict):
            records = data.get('data', data.get('InsdTrdng', []))
        elif isinstance(data, list):
            records = data

        if not records:
            with open(cache_path, 'w') as f:
                json.dump([], f)
            return None

        df = pd.DataFrame(records)

        # Standardize columns from NSE's PIT format
        col_map = {
            'anDt': 'DATE', 'symbol': 'SYMBOL', 'acqName': 'PERSON_NAME',
            'pdesc': 'PERSON_CATEGORY', 'tdesc': 'TRANSACTION_TYPE',
            'befAcqSharesNo': 'SHARES_BEFORE', 'afterAcqSharesNo': 'SHARES_AFTER',
            'secAcq': 'QUANTITY', 'secVal': 'VALUE_LAKHS', 'mode': 'MODE',
        }
        df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

        # Convert value to numeric
        for col in ['QUANTITY', 'VALUE_LAKHS', 'SHARES_BEFORE', 'SHARES_AFTER']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        with open(cache_path, 'w') as f:
            json.dump(df.to_dict('records'), f, default=str)

        logger.info(f"Insider trades: {len(df)} disclosures from {from_date} to {to_date}")
        return df

    # ─── FII / DII FLOWS ─────────────────────────────────────────────────────

    def fetch_fii_dii_flows(self):
        """
        Fetch daily FII/DII cash + F&O activity from NSE.

        Returns:
            DataFrame with: DATE, FII_CASH_NET, FII_INDEX_FUT_NET, FII_INDEX_OPT_NET,
                            FII_STOCK_FUT_NET, DII_CASH_NET
        """
        today = datetime.now().strftime('%Y-%m-%d')
        cache_path = FII_DII_DIR / f"{today}.json"

        # FII/DII is typically the same for recent range — use today's cache
        if cache_path.exists():
            logger.info("FII/DII cache hit")
            with open(cache_path) as f:
                data = json.load(f)
            return pd.DataFrame(data) if data else None

        # NSE FII/DII live API
        url = f"{BASE_URL}/api/fiidiiTradeReact"
        data = self._get_json(url, referer=f"{BASE_URL}/market-data-reports/fii-dii-activity")

        if data is None:
            # Try older FII data endpoint
            url2 = f"{BASE_URL}/api/fii-stats-json"
            data = self._get_json(url2)

        if data is None:
            logger.warning("Could not fetch FII/DII data")
            return None

        records = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get('data', data.get('fiidii', data.get('Data', [])))

        if not records:
            return None

        df = pd.DataFrame(records)

        # Standardize NSE FII/DII column names
        col_map = {
            'date': 'DATE', 'Date': 'DATE',
            'fiiBuySell': 'FII_CASH_NET',
            'diiBuySell': 'DII_CASH_NET',
        }
        df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

        # NSE FII/DII has separate rows per category — pivot if needed
        if 'category' in df.columns or 'Category' in df.columns:
            df = self._pivot_fii_dii(df)

        # Ensure numeric
        for col in df.columns:
            if col != 'DATE':
                df[col] = pd.to_numeric(df[col], errors='coerce')

        with open(cache_path, 'w') as f:
            json.dump(df.to_dict('records'), f, default=str)

        logger.info(f"FII/DII data: {len(df)} rows")
        return df

    def _pivot_fii_dii(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle NSE's category-based FII/DII format and pivot to wide format."""
        # NSE typically returns rows like: [date, 'FII/FPI', buy, sell, net]
        cat_col = 'category' if 'category' in df.columns else 'Category'
        net_col = 'netVal' if 'netVal' in df.columns else 'NET'

        if cat_col not in df.columns or net_col not in df.columns:
            return df

        # Map category names to column names
        cat_map = {
            'FII/FPI': 'FII_CASH_NET',
            'DII': 'DII_CASH_NET',
        }

        pivoted = {}
        if 'DATE' in df.columns:
            dates = df['DATE'].unique()
            for date in dates:
                row = {'DATE': date}
                sub = df[df['DATE'] == date]
                for _, r in sub.iterrows():
                    cat = str(r.get(cat_col, '')).strip()
                    mapped = cat_map.get(cat, f"CAT_{cat}")
                    row[mapped] = pd.to_numeric(r.get(net_col, 0), errors='coerce')
                pivoted[date] = row

        return pd.DataFrame(list(pivoted.values())) if pivoted else df


if __name__ == "__main__":
    """Quick test — fetch today's data."""
    from dotenv import load_dotenv
    load_dotenv()

    fetcher = NSEDataFetcher()

    # Test bhavcopy (last trading day)
    today = datetime.now()
    last_trading = today - timedelta(days=1 if today.weekday() < 5 else (today.weekday() - 4))
    date_str = last_trading.strftime('%Y-%m-%d')
    print(f"\nFetching bhavcopy for {date_str}...")
    df = fetcher.fetch_bhavcopy(date_str)
    if df is not None:
        print(f"  Bhavcopy: {len(df)} records, columns: {list(df.columns[:8])}")
    else:
        print("  Bhavcopy: FAILED (may need NSE_COOKIES env var)")

    # Test FII/DII
    print("\nFetching FII/DII flows...")
    fii = fetcher.fetch_fii_dii_flows()
    if fii is not None:
        print(f"  FII/DII: {len(fii)} rows, columns: {list(fii.columns)}")
    else:
        print("  FII/DII: FAILED")

    print("\nDone. Data cached in .tmp/")
