# PAIR TRADING — STRATEGY PLAYBOOK

Profit from correlated stocks reverting to their historical spread. This is mean reversion. The edge is statistical, not mechanical — pairs usually revert, but not always.

---

## THE EDGE

Cointegrated stocks tend to revert to their long-run price relationship. When the spread stretches beyond 2 standard deviations, MOST pairs revert — but not all, not always.

**Honest backtest (3Y, 195 pair trades): 66% overall win rate.** Best pairs hit 80–85%; worst pairs (TCS/INFY: 47%) lose money. The stop loss at |Z| > 3.5 and the 20-day time stop exist precisely because pairs DO fail to revert.

The Hurst exponent of Indian FNO pairs is slightly above 0.5 (weakly trending). So we're betting on a rubber band that has been stretched. The rubber band usually snaps back. Sometimes it stretches further before a structural break ends the relationship entirely.

---

## QUALIFICATION CRITERIA (ALL FOUR MUST PASS)

A pair is only eligible to trade if it passes every one of these:

| Test | Tool | Threshold | Why |
|---|---|---|---|
| ADF Test (stationarity) | `statsmodels.adfuller` | p < 0.05 | Proves spread is stationary |
| Engle-Granger Cointegration | `coint()` | p < 0.05 | Long-run relationship exists |
| Hurst Exponent | `hurst` library | H < 0.5 | Mean-reverting, not trending |
| Half-Life | Ornstein-Uhlenbeck | < 40 days | Reverts fast enough to be tradeable |

Pairs that pass 3/4 are "Near-Miss" — lower position size (0.5× Kelly), flag as speculative.

**Run:** `python3 execution/scan_cointegrated_pairs.py`

---

## VALIDATED PAIRS (RANKED BY CONFIDENCE)

Source: Full cointegration scan (ADF + Engle-Granger + Hurst + OU half-life) followed by z-score backtest on 3Y FNO data. Results at `.tmp/pair_backtest_results.csv`, `.tmp/validated_pairs.csv`. Re-run quarterly.

**Critical:** Statistical cointegration gates are necessary but not sufficient. HINDUNILVR/DABUR passes 3/4 gates but has only 32% WR in backtest. Always verify WR before trading.

### Tier 1 — Verified (WR ≥ 65%, n ≥ 12)

| Pair | Sector | WR (3Y bt) | n | Stop Rate | Notes |
|---|---|---|---|---|---|
| IDFCFIRSTB/PNB | Banking | 88.2% | 17 | 0% | Best pair found. All exits on Z reversion. |
| LT/GMRAIRPORT | Infra | 76.9% | 13 | 31% | Use strict Z≥2.0 entry — high stop rate otherwise. |
| GMRAIRPORT/ADANIPORTS | Infra | 73.7% | 19 | 26% | Good infra pair. |
| AXISBANK/BANKBARODA | Banking | 68.0% | 25 | 16% | Largest reliable sample. Low stop rate. |

### Tier 2 — Borderline (WR 55–65%, use 0.5× Kelly)

| Pair | Sector | WR (3Y bt) | n | Notes |
|---|---|---|---|---|
| SBIN/BANKBARODA | Banking | 62.5% | 24 | Large sample. Marginal WR. 0.5× Kelly. |
| TATASTEEL/JSWSTEEL | Metals | 61.1% | 18 | Volatile — stops get hit frequently. |
| HDFCBANK/ICICIBANK | Banking | 60.9% | 23 | Commonly assumed "best pair" — borderline. |

### Pairs to AVOID (Confirmed Losers)

| Pair | WR | n | Why |
|---|---|---|---|
| HINDUNILVR/DABUR | 32% | 25 | Passes gates, loses money. Avoid. |
| GRASIM/DALBHARAT | 29% | 17 | Structural divergence. |
| HDFCLIFE/LICI | 40% | 10 | Sector news overrides mean reversion. |
| BIOCON/TORNTPHARM | 47% | 19 | No longer cointegrated. |
| NTPC/POWERGRID | 47% | 15 | Policy events break spread. |
| TCS/INFY | — | — | Failed cointegration scan. Do not trade. |

---

## SIGNAL DETECTION

### Step 1 — Compute Z-Score (ALWAYS use Cash-Neutral Spread)

```python
spread = (qty_a * price_a) - (qty_b * price_b)  # ← CORRECT
# NEVER: ratio = price_a / price_b               # ← WRONG, distorts PnL
```

Roll a 60-day mean and std on the spread. Z = (current - mean) / std.

Check all three windows: 20d, 30d, 60d. Signal triggers when any window hits threshold.

### Step 2 — Signal Strength Score

```
SSS = |Z-Score| × (1 + Correlation_60d)
```

| SSS | Action |
|---|---|
| > 8.0 | ULTRA — Max Kelly |
| 6.0–8.0 | STRONG — Full 1× Kelly |
| 4.0–6.0 | MODERATE — 0.5× Kelly |
| < 4.0 | Ignore |

### Step 3 — Multi-Window Confirmation

Signal is higher conviction if Z > 2.0 in 2 or more windows simultaneously.

### Entry
- Z > +2.0 → SELL Stock A / BUY Stock B
- Z < -2.0 → BUY Stock A / SELL Stock B

### Exit
- Z reverts to 0.0 (mean) → Full exit
- Conservative: Z reverts to ±0.5 → Exit (captures 75% of the move, lower risk)

### Stop Loss — STRUCTURAL, not Z-based

**Z-based hard stops are wrong for genuinely cointegrated pairs.** Backtests across all proven pairs show:
- No-stop or Z≥5 stop consistently gives HIGHER win rate than Z≥3.5
- Z≥2.5 stop cuts win rate in half (e.g. ICICIBANK/HDFCBANK: 90% → 50%)
- Tight stops lock in paper losses on trades that would have reverted

**The real stop is structural integrity. Check weekly while in a trade:**

```python
# Re-run on last 3 months of spread data
_, coint_p, _ = coint(recent_A, recent_B)   # last 63 trading days
_, adf_p, *_  = adfuller(recent_spread)

if coint_p > 0.20 and adf_p > 0.10:
    # Relationship is broken — EXIT IMMEDIATELY regardless of Z
    exit_now()
```

| Condition | Action |
|---|---|
| Rolling coint p < 0.15 (last 3M) | Z=-4 or -5 is OPPORTUNITY — hold or add |
| Rolling coint p > 0.20 (last 3M) | EXIT immediately — relationship broken |
| Days held > 30 (time stop) | Exit regardless of Z |

**Z=3.5 hard stop: REMOVED.** Only valid stops are profit (Z→0), time (30d), or structural break.

---

## 5 HARD SAFETY GATES (MANDATORY — REJECT SIGNAL IF ANY FAIL)

These were added after post-mortem analysis in Feb 2026. They prevent trading into structural breaks masquerading as opportunities.

1. **Data freshness ≤ 3 days.** If `.tmp/3y_data/{SYMBOL}_3Y.csv` is older than 3 days, signal is VOID.

2. **20-day rolling correlation > 0.3.** Below this, the pair has decoupled. Mean reversion does NOT apply. Signal is VOID.

3. **Live ratio within historical min/max ± 5%.** If today's ratio is outside the entire 3-year range, this is a structural break, not a dislocation. Signal is VOID.

4. **Split cross-validation.** Flag as "split artifact" ONLY if one stock moves > 20% while the other moves < 5% on the same day. If both stocks move together, it's a market event. Do not create a false regime break.

5. **Rolling structural integrity.** While in a trade, re-run cointegration on last 3M weekly. If coint p > 0.20 AND ADF p > 0.10 → exit immediately. A Z=-4 or -5 on a structurally intact pair is an opportunity, not a reason to stop. A Z=-2 on a broken pair is a reason to exit.

---

## ALWAYS FETCH LIVE PRICES BEFORE TRADING

```bash
python3 execution/live_user_pairs.py
```

Intraday moves shift Z-scores by 1–2 sigma. A −2σ signal at yesterday's close can be −4σ or neutral right now. Use `live_price_fetcher.py` or Shoonya API for current prices before sizing.

---

## EXECUTION SIZING

```bash
python3 execution/kelly_sizer.py \
  --wr 0.67 --avg-win 3.5 --avg-loss 3.0 \
  --capital 1000000 \
  --price-a 1800 --lot-a 350 \
  --price-b 1400 --lot-b 400 \
  --mode half
```

Both legs execute simultaneously. Slippage on one leg alone kills the trade.

---

## WHEN A PAIR BREAKS

Signs a pair is structurally breaking (not reverting):
- |Z| > 3.5 and NOT caused by an earnings event
- 20d correlation drops below 0.3
- One stock announces M&A, regulatory action, or promoter exit

Action: Close both legs immediately at market. Do not average down. Log the failure mode in this file.

---

## KNOWN FAILURE MODES

| Failure Mode | Symptom | Fix |
|---|---|---|
| Corporate action artifact | Z-score drops to −6 or below overnight | Check for splits/bonuses. Filter in data loader. |
| Sector decoupling | Correlation drops persistently over 20 days | Gate 2 catches this. Drop pair from universe. |
| One-sided M&A | Stock A gets acquisition bid → premium | Z-score reversal won't happen. Stop immediately. |
| Earnings surprise | Temporary break on results day | Wait 2 sessions before re-entering. |

---

## SELF-IMPROVEMENT PROTOCOL

**Weekly:** Re-run `scan_cointegrated_pairs.py`. Update Tier 1/Tier 2 tables above if rankings change.

**Monthly:** For each active pair, pull trade log from `signals_db.py`. Calculate WR. If WR drops below 55% over 20 trades, halve the Kelly allocation for that pair until it recovers.

**After each loss:** Identify which gate failed to catch it (or if it was a valid loss within the expected distribution). Add to Known Failure Modes if it was a new pattern.

---

## COMMAND REFERENCE

```bash
# Full universe scan (new pairs)
python3 execution/scan_cointegrated_pairs.py

# Proven pairs only (Tier 1 — highest confidence)
python3 execution/scan_proven_pairs.py

# Live Z-scores for specific pair
python3 execution/live_user_pairs.py

# Deep 15-year validation (Kalman + HMM)
python3 execution/renaissance_deep_dive.py --symA HDFCBANK --symB ICICIBANK

# Size a pair trade
python3 execution/kelly_sizer.py --wr 0.67 --avg-win 3.5 --avg-loss 3.0 \
  --capital 1000000 --price-a 1800 --lot-a 350 --price-b 1400 --lot-b 400
```
