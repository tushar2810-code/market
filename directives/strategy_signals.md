# MEDALLION LITE — 7-MODULE SIGNAL SYSTEM

This is the second engine. While Engine A extracts structural F&O edge, Engine B extracts intelligence from data that's publicly available but largely ignored by retail traders. The same data Jim Simons would have used — but he didn't have it at this scale in real time.

Run at **7:30 PM IST** every trading day, after FII data is published.

---

## THE ARCHITECTURE

```
NSE bhavcopy        → scan_volume_anomalies.py  (Module 1)
NSE PIT filings     → scan_insider_clusters.py  (Module 2)
NSE bulk deals      → scan_bulk_block_deals.py  (Module 3)
Existing pairs      → scan_cointegrated_pairs.py (Module 4)
NSE FII/DII data    → scan_fii_dii_flows.py      (Module 5)
3Y FNO history      → analyze_seasonality.py    (Module 6)
Earnings transcripts → ai_sentiment_analyzer.py (Module 7)
                              ↓
                 composite_scoring_engine.py
                              ↓
                   generate_daily_report.py
                    (terminal + HTML + ntfy.sh)
```

All signals are stored in SQLite: `.tmp/antigravity_signals.db`

---

## THE COMPOSITE SCORE

```
COMPOSITE = vol_score + insider_score + bulk_score + pairs_score
          + fii_score + seasonality_score + sentiment_score
```

| Module | Range | Notes |
|---|---|---|
| 1. Volume Anomaly | −25 to +30 | STEALTH_ACCUMULATION = max signal |
| 2. Insider Cluster | −40 to +40 | Promoter buying = highest weight |
| 3. Bulk/Block Deals | −35 to +30 | THRESHOLD_APPROACH = highest signal |
| 4. Pairs Trading | 0 to +25 | Z-score > 2.0 only |
| 5. FII/DII Flows | −25 to +15 | Market-wide, applies to ALL stocks |
| 6. Seasonality | −10 to +10 | Only statistically validated (p < 0.05) |
| 7. AI Sentiment | −20 to +10 | Confirmatory ONLY. Never standalone. |

---

## MODULE 1: VOLUME ANOMALY + DELIVERY SCANNER

**What it detects:** Institutional footprints in traded volume, before price moves.

**The logic:** Large buyers can't buy quietly. When 10x average volume flows through a stock with 60%+ delivery and the price doesn't move, someone large is absorbing supply. That's the "stealth accumulation" signal — the most powerful one in this system.

**Signal hierarchy:**

| Signal | Condition | Score |
|---|---|---|
| `STEALTH_ACCUMULATION` | Vol > 3×, Delivery > 60%, \|Price change\| < 1% | +30 |
| `BREAKOUT_BUYING` | Vol > 3×, Delivery > 50%, Price > +2% | +20 |
| `SYSTEMATIC_BUILDUP` | 5 consecutive days of rising volume, avg ratio > 1.5× | +15 |
| `MODERATE_ACCUMULATION` | Vol 2.5–3×, Delivery > 50% | +8 |
| `DISTRIBUTION` | Vol > 3×, Delivery > 40%, Price < −2% | −25 |
| `OPERATOR_NOISE` | Vol > 3×, Delivery < 30% | 0 (ignore) |

**Threshold:** 20-day rolling average volume as baseline. All thresholds are empirically derived.

**Data source:** NSE bhavcopy archive. No authentication needed for historical dates.

```bash
python3 execution/scan_volume_anomalies.py
python3 execution/scan_volume_anomalies.py --date 2026-04-04 --min-score 15
```

---

## MODULE 2: INSIDER TRADING CLUSTER DETECTOR

**What it detects:** Coordinated buying/selling by insiders with legally disclosed data.

**The logic:** Insiders know. They can't buy on undisclosed information, but they CAN buy during open trading windows — and they do, when they're confident. When a Promoter + Director + CFO all buy within 14 days, that's a cluster. Someone knows something good is coming.

**Weighting:**

| Category | Score per transaction |
|---|---|
| Promoter / Promoter Group | 25 pts |
| Director / MD / WTD | 15 pts |
| CEO / CFO / KMP | 10 pts |
| Multiple categories together | × 1.5 multiplier |
| Tight cluster (all within 3 days) | +10 pts bonus |

**Minimum transaction value:** ₹10L (filter retail-scale noise)

**Signals:**

| Signal | Condition | Score |
|---|---|---|
| `BUY_CLUSTER` | 2+ distinct insiders buying in 14 days | +15 to +40 |
| `PROMOTER_CONVICTION` | Promoter buys after > 10% stock decline | +25 |
| `SELL_CLUSTER` | 2+ distinct insiders selling in 14 days | −15 to −40 |

**Pre-results bonus:** If cluster occurs within 30 days before results, add 10 pts. They know.

```bash
python3 execution/scan_insider_clusters.py --days 30
python3 execution/scan_insider_clusters.py --from 2026-03-01 --to 2026-04-05
```

---

## MODULE 3: BULK/BLOCK DEAL ACCUMULATION TRACKER

**What it detects:** Same buyer appearing in the same stock multiple times — systematic institutional accumulation before an open offer or stake increase announcement.

**The logic:** SEBI open offer threshold is 25% equity. But 5%, 10%, 15% triggers disclosure requirements. When the same buyer shows up 3+ times in 30 days accumulating the same stock, they're building a strategic stake. Open offer announcements typically drive 15–40% price jumps.

**Signals:**

| Signal | Condition | Score |
|---|---|---|
| `SYSTEMATIC_ACCUMULATION` | Same buyer, 3+ deals in 30 days | +20 |
| `THRESHOLD_APPROACH` | Cumulative holding nearing SEBI threshold | +30 |
| `BLOCK_DEAL_INSTITUTIONAL` | Large block by known MF/FII | +10 |
| `SYSTEMATIC_DISTRIBUTION` | Same seller, 3+ deals | −20 |
| `PROMOTER_EXIT` | Promoter selling in bulk | −35 |

```bash
python3 execution/scan_bulk_block_deals.py --days 30
```

---

## MODULE 5: FII/DII FLOW MOMENTUM SCORE

**What it detects:** The REAL direction of institutional money — which is frequently misread by retail.

**The key pattern most retail traders get wrong:**

> **FII selling cash + FII buying futures = HEDGING, NOT EXITING.**
> This is a BULLISH signal, not bearish. FIIs are protecting gains on their cash portfolio using futures. They're not leaving. Most retail sees "FII sold ₹5000 Cr" and panics. The correct read is "FIIs hedged their longs — they still own the stocks."

**Composite FII score (daily):**
```
fii_score = (FII_cash_net × 0.4) + (FII_index_futures_net × 0.35) + (FII_options_net × 0.25)
5-day rolling sum of fii_score = REGIME
```

**Signals:**

| Signal | Condition | Score |
|---|---|---|
| `FII_REGIME_BULLISH` | 5-day rolling flips positive | +15 |
| `FII_REGIME_BEARISH` | 5-day rolling flips negative | −15 |
| `FII_HEDGE_SIGNAL` | Cash selling + Futures buying | +10 (contrarian bullish) |
| `FII_CAPITULATION` | Heavy selling in BOTH cash AND futures | −25 |

**Note:** FII score is market-wide. It adjusts every stock's composite score up or down.

**Data published:** ~7:00 PM IST by NSE. Run Engine B at 7:30 PM to capture it.

```bash
python3 execution/scan_fii_dii_flows.py
```

---

## MODULE 6: SEASONALITY & EXPIRY PATTERN ENGINE

**What it detects:** Calendar effects that repeat with statistical significance.

**The rules:**
- Pattern must have ≥ 50 occurrences
- T-test p-value < 0.05 (95% confidence)
- Must persist across multiple years — not one anomalous year

Invalid patterns are completely excluded. No discretionary overrides.

**How to run validation (monthly):**
```bash
python3 execution/analyze_seasonality.py --validate
```

**Get current day's seasonality score:**
```bash
python3 execution/analyze_seasonality.py --score
```

Validated patterns get a score contribution of ±5 to ±10 pts. The system treats these as minor adjustments, not primary signals.

---

## MODULE 7: AI SENTIMENT LAYER (Claude claude-haiku-4-5)

**What it detects:** Signals from unstructured text that no scanner can find.

**This layer is CONFIRMATORY ONLY.** It cannot generate standalone trade signals. It boosts or penalizes signals that already exist from modules 1–6.

**Scoring factors (each 1–10):**

| Factor | 1-3 (Bearish) | 7-10 (Bullish) |
|---|---|---|
| Management confidence | Heavy hedging, vague | Definitive, accountable |
| Forward guidance | "Cautiously optimistic" | Specific targets + timelines |
| Capex signals | Cost-cutting mode | Significant growth investment |
| Red flags | None detected | SERIOUS: auditor, related-party |
| Competitive positioning | Losing share, defensive | Taking share, offensive |

**Score → Contribution:**
- Red flags score ≥ 7 → **−20** (overrides everything)
- 3+ factors score ≥ 8 → **+10**
- Otherwise → 0

**Usage:**
```bash
cat earnings_call.txt | python3 execution/ai_sentiment_analyzer.py --symbol RELIANCE
python3 execution/ai_sentiment_analyzer.py --symbol INFY --file q3_transcript.txt
```

**Caching:** Results cached in `.tmp/ai_sentiment/`. Never re-calls API for same content.

---

## RUNNING THE FULL ENGINE

### Standard (fast — pulls today's module results from DB)
```bash
python3 execution/generate_daily_report.py --capital 1000000
```

### Fresh (re-runs all modules — 10–15 min)
```bash
python3 execution/generate_daily_report.py --capital 1000000 --fresh
```

### With HTML report and phone notification
```bash
python3 execution/generate_daily_report.py --capital 1000000 --html --notify
```

### Just the composite scores
```bash
python3 execution/composite_scoring_engine.py --top 20
```

---

## NSE DATA AUTHENTICATION

NSE requires browser-like cookies for API access. Two methods:

**Method 1 (Auto):** `nse_session.py` visits NSE homepage automatically. Works for fresh sessions.

**Method 2 (Manual — use when Method 1 gets 403):**
1. Open nseindia.com in browser
2. Open DevTools → Network → Any API call → Right-click → Copy as cURL
3. Extract the Cookie header value
4. `export NSE_COOKIES="bm_sv=abc123; nseappid=xyz..."` 
5. Re-run the scanner

NSE changes their session handling ~2× per year. When 403 errors start appearing, this is the fix.

**Bhavcopy archive (no auth needed):** Historical dates use `archives.nseindia.com` — no cookies required.

---

## SELF-IMPROVEMENT PROTOCOL

**Daily:**
After each trading day, once actual next-day returns are known, log backtest results:
```python
from execution.signals_db import SignalsDB
db = SignalsDB()
db.insert_backtest_result('MODULE1', 'STEALTH_ACCUMULATION', '2026-04-05', 'RELIANCE', 30, 0.024, 0.031, 0.045, True)
```

**Monthly:**
```bash
python3 -c "
from execution.signals_db import SignalsDB
db = SignalsDB()
for module in ['MODULE1','MODULE2','MODULE3','MODULE5']:
    for days in [30, 60, 90]:
        s = db.get_backtest_stats(module, days)
        print(f'{module} {days}d: {s}')
"
```

If any module's WR drops below 50% over 30+ signals, cut its score contribution by 30% until it recovers.

**If a module breaks (NSE API change, format change):**
1. Read the error
2. Fix the column mappings in `nse_session.py` or the scanner
3. Test: `python3 execution/scan_volume_anomalies.py --no-db`
4. Update this file with what changed
