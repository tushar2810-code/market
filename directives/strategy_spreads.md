# CALENDAR SPREAD ARBITRAGE — STRATEGY PLAYBOOK

**Source of truth:** `.tmp/definitive_spread_backtest.csv` — generated from 3Y FNO data, all 210 symbols, with only technical data quality filtering (Futures/Spot ratio must be 0.85–1.15 to exclude stale underlying values from corporate action data errors). No business assumption filters. Re-run `backtest_spread_rolling.py` monthly.

---

## THE HONEST PICTURE

**Definitive backtest: 717 trades, 153 symbols, Jan 2023–Mar 2026**
- Overall win rate: **64.4%**
- Average PnL per trade: **+₹2,509** / **+0.36%**
- Premium range observed: **−15% to +15%** (NOT ±3% — high premiums are real and DO converge)
- Strategy works but is NOT a guaranteed arbitrage. Many symbols are LOSERS.

**Why some symbols fail to converge by expiry:**
1. Corporate action between entry and expiry (dividend above cost of carry, bonus)
2. Stock hits circuit limit on expiry day — exit price = circuit price, not spot
3. Near-month rollover dislocations from FII hedging activity (basis can widen before narrowing)
4. Data issue: stale FH_UNDERLYING_VALUE (filtered by the 0.85–1.15 ratio check)

**What the high premiums mean (user confirmed this):** 4–15% premiums are REAL. They occur around corporate events (ITC Hotels demerger created 15% contango in ITC Jan 2024 — converged by expiry). PSU stocks (SAIL) show extreme backwardation from FII futures selling. DO NOT cap premiums at 3%.

---

## EXPIRY RULE

**FNO expiry = LAST TUESDAY of every month.** Not Thursday. Every script uses this.

---

## EXIT RULES (MOST IMPORTANT SECTION)

**Do NOT hold to expiry by default.** Exit when in profit. This is both correct strategy AND improves capital efficiency.

**Exit priority:**
1. **Take profit early:** When premium has closed 50%+ from entry → exit immediately, redeploy capital
   - Entry: −0.8% → current: −0.4% → exit (captured 50%)
   - Entry: +1.2% → current: +0.6% → exit
2. **Strong take profit:** Premium closed 80%+ → exit regardless of DTE remaining
3. **Stop loss:** Premium EXPANDS 100%+ from entry level → exit
   - Entry: −0.8% → current: −1.6% → exit (something structural changed)
4. **Expiry day:** If still in trade, let it close naturally
5. **Forced exit:** Corporate action announcement between entry and expiry → exit same day

---

## STRATEGY LOGIC

```
Premium = (Futures Price − Spot Price) / Spot Price × 100

If Premium > +0.5%:  SELL Futures  (premium will decay toward 0)
If Premium < −0.5%:  BUY Futures   (discount will close toward 0)

Best entry window:  T-3 to T-8 (3–8 days before expiry)
Exit:               Take profit at 50%+ closure. Do not wait for T-0.
```

---

## DATA-BACKED SYMBOL RANKINGS (Jan 2023–Mar 2026)

Source: `definitive_spread_backtest.csv`. Methodology: near-month futures vs spot, entry at T-3 to T-8, hold to expiry (pessimistic floor — early exit would improve WR). Re-run monthly.

### Tier 1 — Reliable (WR ≥ 75%, n ≥ 8)

| Symbol | WR | n | Avg PnL/lot | Notes |
|--------|-----|---|-------------|-------|
| LTF | 100% | 8 | ₹4,922 | Very small sample — verify before sizing full |
| SAIL | 83% | 12 | ₹8,815 | PSU. Deep backwardation signals. High conviction. |
| HINDUNILVR | 82% | 11 | ₹4,977 | FMCG stability. Consistent premiums. |
| COLPAL | 78% | 9 | ₹2,975 | Defensive. Smaller avg gain but very reliable. |
| ITC | 77% | 17 | ₹2,606 | Largest sample in Tier 1. Most confident. Corporate events create large premiums that DO converge. |
| OBEROIRLTY | 75% | 12 | ₹14,374 | Highest avg gain. Real estate sector. |
| ICICIGI | 75% | 8 | ₹5,258 | Insurance. Small sample — treat as provisional. |

### Tier 2 — Borderline (WR 55–75%, n ≥ 8)

| Symbol | WR | n | Avg PnL/lot | Notes |
|--------|-----|---|-------------|-------|
| CIPLA | 67% | 9 | ₹7,251 | Decent WR, high avg PnL. |
| IEX | 64% | 11 | ₹7,295 | High avg PnL but WR is borderline. Use 0.5× Kelly. |
| SYNGENE | 62% | 13 | ₹4,132 | Pharma. Consistent. |
| BPCL | 60% | 15 | ₹890 | Passable WR, low avg gain. High volume. |
| DABUR | 60% | 15 | ₹852 | Similar to BPCL. Low expected value. |

### Symbols to AVOID (Losers — actual backtest results)

| Symbol | WR | n | Avg PnL/lot | Issue |
|--------|-----|---|-------------|-------|
| PAGEIND | 47.6% | 21 | −₹655 | Below 50% WR. Loses money. |
| HAVELLS | 44% | 9 | −₹2,164 | Consistent loser. |
| SIEMENS | 33% | 9 | −₹9,041 | Avoid. |
| GODREJCP | 39% | 13 | −₹3,031 | Avoid. |
| IOC | 40% | 10 | −₹3,877 | PSU oil — erratic premium behavior. |
| KOTAKBANK | 22% | 9 | −₹1,400 | Worst in dataset. |
| MPHASIS | 23% | 13 | −₹5,631 | IT sector calendar spreads unreliable. |

---

## ENTRY CRITERIA

| Condition | Required |
|-----------|---------|
| |Premium| | > 0.5% (below this, transaction costs eat the edge) |
| Days to expiry | T-3 to T-8 |
| Symbol | Tier 1 or Tier 2 from above |
| Spot price | Live from Shoonya (NEVER use yesterday's close) |
| No corporate action | Check for dividend record date, bonus, demerger before entry |

**No premium cap.** Premiums up to ±15% are real and observed in clean data.

---

## SIZING

| Tier | Kelly Mode | Rationale |
|------|-----------|-----------|
| Tier 1 (WR ≥ 75%) | 75% of Full Kelly | Proven edge |
| Tier 2 (WR 60–74%) | Half-Kelly | Marginal edge, protect downside |
| New symbol (n < 10) | 0.25× Kelly | Provisional until confirmed |

```bash
# Size a trade (example: ITC, 77% WR)
python3 execution/kelly_sizer.py \
  --wr 0.77 --avg-win 0.5 --avg-loss 0.4 \
  --capital 1000000 --price LIVE_SPOT --lot-size LOT_SIZE
```

---

## DAILY SCAN

```bash
# Scan near-month vs spot (primary strategy)
python3 execution/scan_expiry_convergence.py --threshold 0.5

# Near/far month calendar spreads (secondary — lower conviction)
python3 execution/scan_calendar_spreads.py --threshold 0.5

# Continuous alert monitor during market hours
python3 execution/spread_alert_monitor.py

# Historical reliability — run monthly and update this table
python3 execution/backtest_spread_rolling.py
```

---

## DATA QUALITY NOTE

The 3Y FNO data has known quality issues for some symbols in early periods:
- **TORNTPHARM:** FH_UNDERLYING_VALUE stuck at 3686.5 from Feb–Sep 2024 while stock was ~2600 (split adjustment artifact). DO NOT use TORNTPHARM for calendar spread until data is clean.
- **MCX:** FH_UNDERLYING_VALUE shows 9683 from Feb–Nov 2023 while futures trade at ~1400 (post-split). Excluded from backtest by ratio filter.
- **Any symbol where futures/spot ratio is outside 0.85–1.15:** Data quality issue, not a real premium.

The definitive backtest at `.tmp/definitive_spread_backtest.csv` only includes rows where ratio is 0.85–1.15. This is a DATA QUALITY filter, not a business assumption.

---

## SELF-IMPROVEMENT PROTOCOL

**Monthly:**
1. Run `backtest_spread_rolling.py` on clean data
2. Update the Symbol Rankings table above with new WR numbers
3. Demote any Tier 1 symbol that drops below 70% WR
4. Promote any Tier 2 symbol that crosses 75% WR with n ≥ 10

**After each trade:**
1. Log: entry premium %, exit premium %, DTE at entry, DTE at exit, actual PnL
2. Was this an early exit (profit) or held to expiry?
3. If loss: what prevented convergence? Document in Known Failure Modes.

**Quarterly:**
1. Check FNO delistings and lot size changes
2. Re-run definitive backtest with updated data
3. Update this file completely — stale numbers are dangerous
