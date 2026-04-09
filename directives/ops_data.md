# DATA OPERATIONS — ACQUISITION & MAINTENANCE

Data is the foundation. Bad data = bad signals = bad trades. Garbage in, garbage out.

---

## DATA ARCHITECTURE

| Data Type | Location | Coverage | Freshness |
|---|---|---|---|
| FNO futures history (3Y) | `.tmp/3y_data/{SYMBOL}_3Y.csv` | 208 symbols, Jan 2023–present | Weekly sync |
| NSE equity bhavcopy | `.tmp/bhavcopy/YYYY-MM-DD.csv` | All NSE stocks + delivery % | Daily |
| Bulk/block deals | `.tmp/bulk_deals/bulk_*.json` | 30-day rolling | Daily |
| Insider trading (PIT) | `.tmp/insider_data/insider_*.json` | 30-day rolling | Daily |
| FII/DII flows | `.tmp/fii_dii/YYYY-MM-DD.json` | Daily | Daily (7 PM) |
| AI sentiment | `.tmp/ai_sentiment/{SYMBOL}_*.json` | Per transcript | On-demand |
| Signal history | `.tmp/antigravity_signals.db` | All signals, perpetual | Real-time |

---

## SHOONYA API (Live Prices)

Shoonya is the live data source for real-time pair and spread calculations.

### Authentication
```bash
# Credentials in .env:
# SHOONYA_USER_ID, SHOONYA_PASSWORD, SHOONYA_TOTP_KEY, SHOONYA_VENDOR_CODE, SHOONYA_API_KEY

python3 execution/shoonya_client.py   # Test login
```

**TOTP-based 2FA is automatic.** The `pyotp` library generates the 6-digit code from `SHOONYA_TOTP_KEY`. No manual input needed.

**Session timeout:** Shoonya sessions expire after ~24 hours. Re-login required daily. Scripts handle this automatically via `ShoonyaClient`.

**Rate limit:** ~3 requests/second max. All scripts have 0.4s sleep between requests.

### When Live Prices Are Required
Always fetch live prices via Shoonya before:
- Sizing a pair trade
- Checking calendar spread premiums
- Any signal that requires current Z-score

**NEVER use yesterday's closing price for a trade decision.** Intraday moves of 1–2 sigma are normal.

---

## FNO HISTORICAL DATA (3Y)

### Full Sync
```bash
python3 execution/sync_fno_data.py
```
Downloads/updates all 208 FNO symbols. Takes 30–60 minutes (NSE rate limits). Run weekly.

### Single Symbol
```bash
python3 execution/fetch_one.py RELIANCE
```

### Check What's Missing
```bash
python3 execution/verify_final_dataset.py
```
Reports: complete symbols, partial data symbols (< 3Y), missing symbols.

### Known Partial Data Situations (Expected)
- **TORNTPOWER:** No data before Jan 2025. API limitation. Accept partial data.
- **SWIGGY, WAAREEENER, JIOFIN:** New listings. < 3Y is correct.
- **SUZLON:** May have gaps due to FNO ban periods.
- **ULTRACEMCO:** Was missing — check status regularly.

### Data Format
```
FH_TIMESTAMP,FH_CLOSING_PRICE,FH_MARKET_LOT,FH_EXPIRY_DT,FH_UNDERLYING_VALUE,FH_INSTRUMENT
28-Jan-2026,2840.50,100,25-Feb-2026,2835.20,FUTSTK
```

`FH_UNDERLYING_VALUE` = spot price on that day. Required for premium calculations.

---

## NSE BHAVCOPY (Equity + Delivery Data)

Used by Module 1 (Volume Anomaly). Contains: symbol, OHLC, total traded quantity, deliverable quantity, delivery %.

### Fetch Strategy
`nse_session.py` tries two methods in order:
1. **NSE archive (no auth):** `archives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv`
   - Works for any historical date without session cookies
   - Preferred for past dates

2. **NSE API (auth required):** Falls back to live API if archive fails
   - Requires session cookies (`NSE_COOKIES` env var or homepage visit)

### NSE Session Cookies
```bash
# Set manually if auto-session fails:
export NSE_COOKIES="bm_sv=abc123; nseappid=xyz456; ..."
```

Get from: Browser → NSE website → DevTools → Network → Any request → Cookie header.

NSE refreshes their session validation ~2× per year. When you see 403 errors on fresh sessions, the fix is manual cookies.

---

## NSE LIVE DATA (Bulk Deals, Insider, FII/DII)

All three use the same NSEDataFetcher pattern with automatic caching.

```python
from execution.nse_session import NSEDataFetcher
fetcher = NSEDataFetcher()                    # Handles session + cookies
df = fetcher.fetch_bulk_deals('2026-03-01', '2026-04-05')
df = fetcher.fetch_insider_trades('2026-03-01', '2026-04-05')
df = fetcher.fetch_fii_dii_flows()
```

Results cached in `.tmp/` — won't re-fetch same date range.

---

## DATA QUALITY CHECKS

### Split / Bonus Detection
If a stock drops > 20% in one day while its sector moves < 5%, it's likely a split or bonus. The system auto-detects this in pair trade safety gate 4.

Manual check:
```bash
# If a Z-score suddenly jumps to ±5 or more overnight, check:
python3 execution/verify_and_pnl.py --symbol RELIANCE
```

### Stale Data Guard
All signal scanners reject data older than 3 days. If `.tmp/3y_data/{SYMBOL}_3Y.csv` is stale, the signal is VOID.

### Audit
```bash
python3 execution/audit_completeness.py   # Check all 208 symbols
python3 execution/audit_data.py           # Detailed quality report
```

---

## MAINTENANCE SCHEDULE

| Task | Frequency | Command |
|---|---|---|
| Sync FNO 3Y data | Weekly | `python3 execution/sync_fno_data.py` |
| Fetch today's bhavcopy | Daily | Handled by `generate_daily_report.py` |
| Verify data completeness | Weekly | `python3 execution/verify_final_dataset.py` |
| Ensure data completeness | Monthly | `python3 execution/ensure_data_completeness.py` |
| Retry missing symbols | After sync | `python3 execution/retry_missing_3y.py` |

---

## DIRECTORY RULES

**`.tmp/` is expendable.** Everything in `.tmp/` can be deleted and regenerated. If the directory gets large, clean `.tmp/bhavcopy/`, `.tmp/bulk_deals/`, `.tmp/insider_data/` beyond 60 days.

**Never delete:**
- `.tmp/3y_data/` — takes 30–60 min to re-download
- `.tmp/antigravity_signals.db` — contains signal history and backtest results
- `.tmp/ai_sentiment/` — contains paid API results

**Never commit to git:**
- `.env` — credentials
- `credentials.json`, `token.json` — Google OAuth
- `.tmp/` — intermediate data
