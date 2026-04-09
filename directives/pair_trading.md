# Pair Trading Strategy (Inter-Commodity)

## Goal
Profit from the convergence of the price spread between two correlated assets (e.g., SUNPHARMA vs CIPLA) using statistical mean reversion.

## Inputs
- `SYMBOL1`: First asset (e.g., SUNPHARMA).
- `SYMBOL2`: Second asset (e.g., CIPLA).
- `LOOKBACK_PERIOD`: Timeframe for historical stats (e.g., 5 Years).
- `Z_ENTRY`: Z-Score threshold for entry (default: 2.0).
- `Z_EXIT`: Z-Score threshold for exit (default: 0.0 or 0.5).
- `Z_STOP`: Z-Score threshold for stop loss (default: 3.0).

## 🚨 CRITICAL PROTOCOLS (ZERO TOLERANCE)
**1. NO "LAZY MATH" (RATIOS VS SPREADS)**
- **NEVER** use "Price Ratio" (A/B) to estimate Z-Scores for actionable insights.
- **ALWAYS** use "Cash Neutral Spread" (`Qty1*Price1 - Qty2*Price2`) or `Price1 - (HedgeRatio * Price2)`.
- **Why?** Ratios distort PnL reality during extreme moves. The user trades LOTS, not ratios.

**2. VERIFY BEFORE SPEAKING**
- Universe Scans are "Leads", not "Facts".
- Before presenting a Z-score to the User, you MUST run a dedicated verification script (e.g., `get_jsw_status.py`) using **FUTURES** data if available.
- **Futures > Spot**: Spot Z-scores can mislead on PnL if the Futures discount is significant (e.g., JSW -4.0 vs -3.0).

**3. ALWAYS FETCH LIVE PRICES (MANDATORY)**
- **NEVER** use stale historical close prices when checking for trades.
- **ALWAYS** call Shoonya API via `ShoonyaClient` to get real-time futures prices.
- **Why?** Intraday moves can change Z-scores by 1-2 sigma. A -2σ signal at yesterday's close could be -4σ or neutral NOW.
- **Protocol**: When user asks about a pair trade, FIRST fetch live prices, THEN calculate Z-scores against historical rolling stats.
- **Script**: Use `execution/check_live_pair.py` as template or inline Shoonya calls.

**4. DATA HYGIENE (ANTI-ARTIFACT)**
- **Corporate Actions:** Always use `auto_adjust=True`.
- **Sanity Check:** If a backtest shows a Z-score < -6.0, it is 99% a Data Artifact (Split/Bonus missing).
- **Protocol:** You must Programmatically Filter out daily price drops > 25% that are not corroborated by the sector.
- **Rule:** "Garbage In, Garbage Out". If the chart looks like a cliff, verify it's not a Split before calling it a "Buy".

**5. HARD SAFETY GATES (POST-MORTEM FIX — Feb 2026)**
These gates are MANDATORY before any signal is presented to the user. They are enforced in `scan_pair_universe.py`.
- **Gate 1 — Data Freshness:** Historical data must be ≤3 days stale. If stale, signal is **VOID**. Never compute Z-scores against stale data.
- **Gate 2 — 20D Correlation:** 20-day rolling correlation must be > 0.3. If below, the pair has decoupled and mean reversion does NOT apply. Signal is **VOID**.
- **Gate 3 — Historical Range:** Live ratio must be within the historical min/max ± 5%. If outside, this is a structural break, not a dislocation. Signal is **VOID**.
- **Gate 4 — Split Cross-Validation:** A "split" is only flagged if ONE stock moves >20% while the OTHER moves <5% on the same day. If BOTH stocks crash (e.g., election day), it's a market event — do NOT create a regime break.
- **Gate 5 — Z-Score Cap:** Reject any signal with |Z| > 4.0. Extreme Z-scores indicate stale data or structural breaks, not opportunities.

## Tools/Scripts
- `execution/scan_proven_pairs.py`: **(PRIMARY)** Proven Pairs Scanner v3 — only trades individually validated 90%+ WR pairs with per-pair configs. Use `--monitor` to see all passing pairs.
- `execution/scan_pair_universe.py`: Universe-wide scan (lower conviction, used for research only).
- `execution/fetch_historical.py`: Fetches history and calculates stats.
- `execution/data_loader.py`: Centralized fetcher with Split detection & Noise filtering.

## Strategy Logic (Mean Reversion)

### 1. Calculate Statistics
- **Hedge Ratio**: Determine ratio to equalize notional value (Price A * Qty A ≈ Price B * Qty B).
  - Example: Bandhan (150) vs RBL (300) -> Ratio 2:1.
- **Spread** = (Ratio1 * Price1) - (Ratio2 * Price2)
- **Mean** = Average of Spread over Lookback Period
- **StdDev** = Standard Deviation of Spread over Lookback Period
- **Z-Score** = (Current Spread - Mean) / StdDev

### 2. Entry Rules
- **Short Spread** (Bet on narrowing):
  - Condition: **Z-Score > 2.0** (Overbought)
  - Action: SELL Symbol1 + BUY Symbol2
- **Long Spread** (Bet on widening):
  - Condition: **Z-Score < -2.0** (Oversold)
  - Action: BUY Symbol1 + SELL Symbol2

### 3. Exit Rules (Profit Taking)
- **Target**: **Z-Score returns to 0.0 (Mean)**
- **Conservative Target**: Z-Score returns to 0.5 or -0.5 (Capture 75% of move)

### 4. Risk Management (Stop Loss)
- **Stop Loss**: **Z-Score expands to 3.0** (Structural break in correlation)
- Action: Close both positions immediately.

### 4. Safety Filters (Anti-Crash Logic)
1. **Short-Term Volatility ("Falling Knife")**: Reject if spread moves > 7% in 3 days.
2. **Sustained Volatility ("Indigo Rule")**: Reject if spread moves > 15% in 15 days.
3. **News/Event Filter**: Use `search_web` to check for Earnings, Mergers, or Splits. Avoid trading into these events.

## 5. Level 4: Advanced Quantitative Validation (The "Jim Simons" Check)
Before confirming a "High Conviction" trade, run `execution/verify_survivors_15y.py`.
This engine uses:
1.  **Kalman Filter**: To estimate "True Price" and strip out noise.
2.  **Bayesian Sentinel (HMM)**: To detect "Hidden States" (Bull, Bear, Volatile).
    *   **Rule**: Block trades if the Market Regime is identified as "Volatile" (State with high variance).
3.  **Operator Activity**: Flag anomalous volume spikes (> 3x average) as potential manipulation.

**Goal**: Achieve > 65% Win Rate over 15 years including crashes (2008, 2020). If a pair fails this, do not trade size.
## Example (Sunpharma vs Cipla)
- Mean: 12
- StdDev: 163
- **Entry (Short)**: Spread > 338 (12 + 2*163)
- **Exit**: Spread < 12 (Mean)
- **Stop**: Spread > 501 (12 + 3*163)
