# Medallion Lite — Master Strategy Directive

## System Architecture

```
generate_daily_report.py    → Master daily runner (7:30 PM IST)
composite_scoring_engine.py → Unified scoring (all 7 modules → 1 score)
signals_db.py               → SQLite: .tmp/antigravity_signals.db

Module 1: scan_volume_anomalies.py     → Volume spike + delivery scanner
Module 2: scan_insider_clusters.py     → Insider trading cluster detector
Module 3: scan_bulk_block_deals.py     → Bulk/block deal accumulation tracker
Module 4: scan_cointegrated_pairs.py   → Pair trading (existing, cointegration)
Module 5: scan_fii_dii_flows.py        → FII/DII flow momentum score
Module 6: analyze_seasonality.py       → Calendar pattern engine (t-test validated)
Module 7: ai_sentiment_analyzer.py     → Claude Haiku transcript analysis

Data fetching: nse_session.py          → Shared NSE session + all data fetchers
```

## Daily Execution

```bash
# Standard daily run at 7:30 PM IST (after FII data published)
python3 execution/generate_daily_report.py --capital 1000000

# With HTML report + push notification
python3 execution/generate_daily_report.py --capital 1000000 --html --notify

# Re-run all module scanners (if running fresh — ~10-15 min)
python3 execution/generate_daily_report.py --capital 1000000 --fresh

# Run individual modules
python3 execution/scan_volume_anomalies.py
python3 execution/scan_insider_clusters.py --days 30
python3 execution/scan_bulk_block_deals.py --days 30
python3 execution/scan_fii_dii_flows.py
python3 execution/analyze_seasonality.py --validate   # Run once to validate patterns
python3 execution/composite_scoring_engine.py
```

## Composite Score Trading Rules

| Score | Signal | Action |
|-------|--------|--------|
| ≥ 60 | STRONG_BUY | Enter with 2× Kelly position |
| 40-59 | BUY | Enter with 1× Kelly position |
| 20-39 | WATCHLIST | Monitor, do not trade yet |
| -20 to 19 | NO_SIGNAL | Flat. Do nothing. |
| -21 to -39 | SHORT | Consider short/exit long |
| ≤ -40 | STRONG_SHORT | Short with 2× Kelly |

## Module Score Ranges

| Module | Min | Max | Notes |
|--------|-----|-----|-------|
| 1. Volume Anomaly | -25 | +30 | STEALTH_ACCUMULATION = 30 |
| 2. Insider Cluster | -40 | +40 | Promoter buying = 25 pts |
| 3. Bulk/Block Deals | -35 | +30 | THRESHOLD_APPROACH = 30 |
| 4. Pairs Trading | 0 | +25 | Z-score > 2.0 |
| 5. FII/DII Flows | -25 | +15 | Market-wide adjustment |
| 6. Seasonality | -10 | +10 | Only validated patterns (p<0.05) |
| 7. AI Sentiment | -20 | +10 | Confirmatory only, not standalone |

## Safety Gates (MANDATORY — same as antigravity_v3.md)

1. **Max 70% capital deployed** at any time across ALL strategies
2. **Max 40% per strategy** — never over-concentrate in one module's signals
3. **3% daily drawdown kill switch** — close all, wait 48h
4. **FNO expiry is ALWAYS last Tuesday of month** — never Thursday
5. **No override the model** — if score says NO_SIGNAL, do nothing

## Data Dependencies

| Data Type | Source | Cache Location | Frequency |
|-----------|--------|----------------|-----------|
| CM Bhavcopy (equity + delivery) | NSE archive | `.tmp/bhavcopy/` | Daily |
| Bulk/Block deals | NSE API | `.tmp/bulk_deals/` | Daily |
| Insider trading (PIT) | NSE API | `.tmp/insider_data/` | Daily |
| FII/DII flows | NSE API | `.tmp/fii_dii/` | Daily after ~7 PM |
| FNO historical data (3Y) | NSE/Shoonya | `.tmp/3y_data/` | Weekly |
| Signals database | SQLite | `.tmp/antigravity_signals.db` | Persistent |

## NSE Authentication

NSE's API requires browser-like cookies. Two approaches:
1. **Auto**: `nse_session.py` visits NSE homepage to get session cookies
2. **Manual**: Set `NSE_COOKIES` env var with cookies from browser DevTools

If NSE returns 403, get fresh cookies:
```bash
# In browser: NSE website → DevTools → Network → Any request → Copy as cURL → extract cookies
export NSE_COOKIES="your_nse_cookie_string_here"
```

## One-Time Setup

```bash
# 1. Install dependencies
pip install anthropic scipy

# 2. Validate seasonality patterns (run once, saves to DB)
python3 execution/analyze_seasonality.py --validate

# 3. Initial data fetch (NSE cookies required)
python3 execution/nse_session.py  # Test NSE connection

# 4. First full run
python3 execution/generate_daily_report.py --fresh
```

## Error Handling

**NSE returns 403:**
→ NSE session expired. Get fresh cookies, set `NSE_COOKIES` env var.

**No bhavcopy data for date:**
→ NSE holiday or weekend. Check NSE calendar. Bhavcopy not published on holidays.

**AI sentiment fails:**
→ `ANTHROPIC_API_KEY` not set. Sentiment module defaults to 0 contribution. System still runs.

**No composite signals:**
→ Individual module scanners haven't run today. Use `--fresh` flag.

## Self-Annealing Protocol

When a scanner fails:
1. Read the error, identify root cause (network? auth? format change?)
2. Fix the script (usually cookies or column name change in NSE data)
3. Test the fix: `python3 execution/scan_volume_anomalies.py --no-db`
4. Update this directive with what changed and why
5. NSE changes their API format ~2x per year — be prepared to update column mappings

## Integration with Existing antigravity_v3 System

This system EXTENDS, not replaces, the existing strategy layers:

| Existing | New |
|----------|-----|
| Pair trading (scan_cointegrated_pairs.py) | Module 4 in composite scoring |
| Calendar spreads (scan_expiry_convergence.py) | Remain standalone, best used independently |
| Momentum RSI (momentum_rsi_strategy.py) | Remain standalone |
| Antigravity v3 scanner | Still runs pairs + expiry convergence daily |

The new composite engine adds signal intelligence for individual stock selection.
Run both: `antigravity_v3_scanner.py` for F&O and `generate_daily_report.py` for equity signals.
