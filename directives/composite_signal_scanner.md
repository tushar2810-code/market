# Composite Signal Scanner — Daily Operation Directive

## Goal
Run all 7 signal modules, aggregate into composite scores, produce ranked output.

## When to Run
Every trading day at **7:30 PM IST** — after NSE publishes FII/DII data (~6:30-7:00 PM).

## Tools Required
- `execution/generate_daily_report.py` — master runner
- `execution/composite_scoring_engine.py` — aggregation
- `execution/signals_db.py` — storage (SQLite)
- All 7 module scanners (see below)

## Inputs
| Module | Script | Data Source | Notes |
|--------|--------|-------------|-------|
| 1. Volume Anomaly | scan_volume_anomalies.py | NSE bhavcopy archive | Needs 22+ days |
| 2. Insider Cluster | scan_insider_clusters.py | NSE PIT API | Needs session cookies |
| 3. Bulk/Block Deals | scan_bulk_block_deals.py | NSE bulk deals API | Needs session cookies |
| 4. Pairs Trading | scan_cointegrated_pairs.py | Shoonya live prices | Existing system |
| 5. FII/DII Flows | scan_fii_dii_flows.py | NSE FII/DII API | Available after 7 PM |
| 6. Seasonality | analyze_seasonality.py | Historical 3Y data | Validate once/month |
| 7. AI Sentiment | ai_sentiment_analyzer.py | Claude Haiku API | On-demand only |

## Standard Daily Run
```bash
# Fast: use cached module data from DB (if modules ran today)
python3 execution/generate_daily_report.py --capital 1000000

# Full: re-run all modules fresh (~10-15 min)
python3 execution/generate_daily_report.py --capital 1000000 --fresh

# With HTML report
python3 execution/generate_daily_report.py --capital 1000000 --html

# With push notification
python3 execution/generate_daily_report.py --capital 1000000 --notify
```

## Outputs
1. Terminal report with 7 sections (FII regime, seasonality, volume, insider, bulk, pairs, composite)
2. Top 10 stocks by composite score with module breakdown
3. Capital deployment estimate (Kelly-sized)
4. Optional: HTML report at `.tmp/reports/YYYY-MM-DD.html`
5. Optional: ntfy.sh push notification

## Trading Rules (from composite score)
```
Score >= 60  → STRONG_BUY  → Enter with 2× Kelly
Score 40-59  → BUY         → Enter with 1× Kelly
Score 20-39  → WATCHLIST   → Monitor only
Score -20 to 19 → NO_SIGNAL → Do nothing
Score -20 to -39 → SHORT   → Consider short
Score <= -40 → STRONG_SHORT → Short with 2× Kelly
```

## NSE Authentication Issues
NSE requires session cookies. If 403 errors:
1. Open NSE website in browser (any page)
2. Open DevTools → Network → Any API request → Copy Request Headers → find Cookie
3. Set: `export NSE_COOKIES="bm_sv=...; nseappid=..."` etc.
4. Or use the auto_cookie_scraper.py to get fresh cookies

## Module-Level Debugging
```bash
# Test each module independently
python3 execution/scan_volume_anomalies.py --no-db     # Test without saving
python3 execution/scan_fii_dii_flows.py                # FII data
python3 execution/scan_insider_clusters.py --days 14
python3 execution/scan_bulk_block_deals.py --days 30
```

## One-Time Seasonality Validation
```bash
# Run this once per month to re-validate seasonal patterns:
python3 execution/analyze_seasonality.py --validate
```
Patterns that fail p < 0.05 with n >= 50 are automatically excluded from scoring.

## Self-Annealing

If a module fails:
1. Check the error — usually NSE API format change or auth issue
2. Fix column names in nse_session.py if NSE changed their CSV structure
3. Test the fix: `python3 execution/scan_volume_anomalies.py --no-db`
4. Update this directive with the fix

Common issues:
- **Bhavcopy column names**: NSE periodically renames columns. Check `_parse_bhavcopy_csv()` in nse_session.py
- **FII data structure**: NSE changes their API response format. Check `_extract_from_row_format()` in scan_fii_dii_flows.py
- **Bulk deals pagination**: If >500 deals, NSE paginates — check if data is truncated

## Integration with antigravity_v3 System

Run BOTH systems daily — they serve different purposes:

| System | Focus | When |
|--------|-------|------|
| antigravity_v3_scanner.py | F&O pairs + calendar spreads | 9:15 AM (pre-market) |
| generate_daily_report.py | Equity stock picking signals | 7:30 PM (post-market) |
