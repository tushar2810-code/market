# Connect to Shoonya API & Fetch Historical Data

**Goal**: Authenticate with Shoonya API using Python and fetch historical candle data for backtesting.

**Inputs**:
- `SHOONYA_USER_ID`
- `SHOONYA_PASSWORD`
- `SHOONYA_TOTP_KEY` (for 2FA)
- `SHOONYA_API_KEY` (also known as Vendor Key or IMEI in some contexts, but typically API key for API access)
- `token.txt` (or similar) to cache the session if possible/needed.

**Tools/Scripts**:
- `execution/shoonya_auth.py`: Handles login and returns an authenticated API object or session token.
- `execution/fetch_historical.py`: Uses the auth object to fetch `get_time_price_series`.

**Procedure**:
1.  **Install SDK**: Ensure `NorenRestApi` or `shoonya-trading-api` (officially `NorenApi` wrapper) is installed.
    - *Note*: Often installed via `pip install NorenRestApi` or from a git repo. We will check availability.
2.  **Authenticate**:
    - Initialize `NorenApi`.
    - Login using `api.login(userid, password, twoFA, vendor_code, api_secret, imei)`.
    - *Self-Annealing*: If login fails, check TOTP generation.
3.  **Fetch Data**:
    - Use `api.get_time_price_series(exchange, token, start_time, end_time, interval)`.
    - Arguments:
        - `exchange`: 'NSE', 'NFO', etc.
        - `token`: Symbol token (needs `get_search` or master contract file to find token).
        - `interval`: '1', '5', '15', '30', '60' (in minutes).
4.  **Output**:
    - Save data to `.tmp/shoonya_data_{symbol}_{interval}.csv`.

**Edge Cases**:
- INVALID_CREDENTIALS
- RATE_LIMIT_EXCEEDED (Shoonya has limits).
- MISSING_DATA (Symbol not valid or no data for range).

