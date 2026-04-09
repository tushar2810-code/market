# Antigravity FNO Portfolio Plan
## ₹50L → ₹75-80L by March 31, 2027

**Capital**: ₹50,00,000  
**Target**: ₹75-80L (50-60% return)  
**Timeline**: ~12 months (April 2026 – March 2027)  
**Monthly target**: 3.5% compounded (conservative), 4.0% (base)

---

## Capital Allocation

| Bucket | Amount | Purpose |
|--------|--------|---------|
| Active Pairs (margin deployed) | ₹20-25L max | 5-6 concurrent positions × ₹4-6L margin each |
| Liquid buffer | ₹25-30L | MTM protection, margin calls, opportunities |
| Overnight fund (idle) | ₹25-30L | Liquid MF @ 6.5% = ~₹2L passive income |

**Rule**: Never deploy more than 50% of capital as margin at any time.

---

## Watch List — Priority Order

### TIER 1: Trade on next signal

**1. ULTRACEMCO / GRASIM** ← CLOSEST TO ENTRY
- 3Y coint p=0.006, ADF p=0.013, HL=27.7d, 60d Corr=0.856
- 3M coint p=0.033 (recent structure intact)
- Backtest: 15 trades, 60% WR, avg win +35%, avg loss -16% → EV strongly positive
- Current 60d Z = -1.29 → Entry at -2.0 (0.7σ away)
- Entry: **BUY ULTRACEMCO / SELL GRASIM**
- Sizing: **10 lots ULTRA** (50×10=500 shares) vs **10 lots GRASIM** (250×10=2500 shares)
- Margin needed: ~₹6.5L
- Expected P&L per trade: ~₹1.5-2L (2.3× std × 10 lots)

**2. ICICIBANK / HDFCBANK**
- 3M coint p=0.004 (extremely strong short-term), ADF p=0.079 (spread stationary)
- 94.7% historical WR, 19 trades
- HL=124d (long) — but 30d time stop still captures most moves
- Current 60d Z = +0.48 → Entry at ±2.0
- Sizing: 5:10 ratio (700 ICICIB × 5 vs 550 HDFC × 10)
- Margin: ~₹5.5L

**3. LICHSGFIN / PFC**
- 3Y coint p=0.106, 3M p=0.111, HL=42.3d
- 100% historical WR (6 trades), avg return 8.55%
- Current Z ≈ 0 → Entry at ±2.0
- Sizing: **5 lots each** (5000 LICHSGFIN, 6500 PFC)
- Margin: ~₹6.5L

**4. NMDC / COALINDIA**
- 3M coint p=0.015 (strong), HL=28d
- 71% WR (14 trades), avg win +48%
- Current 30d Z = +1.02 → approaching entry
- Sizing: **20:25 lots** (6750 NMDC × 20 vs 1050 COAL × 25)
- Margin: ~₹7.5L

### TIER 2: Add after Tier 1 deployed

**5. TATAPOWER / NHPC** (once 6M structure recovers)
- Original: adf=0.011, coint=0.050, HL=10d, corr=0.707
- Currently 6M coint p=0.454 (broken) — wait for recovery
- Fast HL=10d means quick exits; size at 10 lots each

**6. LT / GMRAIRPORT** (watch for cointegration setup)
- 3Y coint p=0.003 (strongest in universe), needs Z to develop
- Size at 5 lots each when signal fires

---

## Entry Rules (ALL must be true)

1. |Z-score| ≥ 2.0 in **at least 2 of 3 windows** (20d, 30d, 60d)
2. 3M coint p < 0.20 OR ADF p < 0.10 (recent structure intact)
3. 20D return correlation ≥ 0.40
4. Live price ratio within historical ±5% range
5. Data freshness ≤ 3 trading days

**Exception**: ULTRACEMCO/GRASIM may enter on 60d Z alone if 3Y coint p < 0.01 (it's at 0.006 — proven long-run relationship).

---

## Exit Rules

| Trigger | Action |
|---------|--------|
| Z reverts to 0 (mean) | **Exit — take profit** |
| 30 days elapsed | **Exit regardless** (time decay of theta) |
| 3M coint p > 0.25 AND ADF p > 0.15 | **Emergency exit** (structural break) |
| Account drops to ₹42.5L | **STOP ALL TRADING** (15% max drawdown) |

---

## Position Sizing Formula

For each trade:
1. Use CSV lot size (not Shoonya live lot) for spread computation
2. Scale lots to keep margin ₹4-8L per position (not per leg)
3. Max concurrent positions: **6** (was 3 at ₹10L)
4. Never exceed 50% of capital as deployed margin

---

## Expected P&L by Strategy

| Strategy | Trades/Year | Avg P&L | Win Rate | Expected Contribution |
|----------|------------|---------|----------|----------------------|
| ULTRACEMCO/GRASIM | 4–6 | ₹1.75L | 60% | +₹4.2L |
| ICICIBANK/HDFCBANK | 4–6 | ₹1.25L | 92% | +₹5.75L |
| LICHSGFIN/PFC | 3–4 | ₹2.25L | 100% | +₹7.85L |
| NMDC/COALINDIA | 4–5 | ₹1.5L | 70% | +₹4.2L |
| TATAPOWER/NHPC | 2–3 | ₹1.0L | 65% | +₹1.6L |
| LT/GMRAIRPORT | 2–3 | ₹1.2L | 67% | +₹1.6L |
| Calendar/Expiry | opportunistic | ₹0.75L | 64% | +₹2.25L |
| **TOTAL** | | | | **~₹27.45L → +54.9%** |

Idle capital (₹27L avg) in overnight fund @ 6.5%: **+₹1.75L passive income**

**Grand total expected**: ₹50L + ₹27.45L + ₹1.75L = **₹79.2L** ✅

**Conservative case (65% of expected)**: ₹50L → ₹67.8L  
**Base case**: ₹50L → ₹79.2L ✅  
**Optimistic (market recovers, 8 trades/month)**: ₹50L → ₹88L

---

## Per-Trade P&L Mechanics

With 5× position size vs the original ₹10L plan:
- Each σ move on the spread is worth ~5× more in rupees
- ULTRA/GRASIM: 10 lots × 50 shares × ₹350 × 2.3σ = ~₹1.6L per full reversion
- ICICIB/HDFC: 5 lots × 700 shares × ₹600 × 2.3σ = ~₹1.5L per full reversion
- LICHSGFIN/PFC: 5 lots × 1000 shares × ₹90 × 2.3σ = ~₹1.0L per full reversion
- NMDC/COAL: 20 lots × 6750 shares × ₹40 × 2.3σ = ~₹2.5L per full reversion

---

## Monthly Action Plan

| Month | Action |
|-------|--------|
| Apr 2026 | No pair trades (market selloff, cointegration weak). Park ₹40L in liquid MF. Run scanner daily. |
| May 2026 | ULTRACEMCO/GRASIM likely first to fire (Z approaching -2.0). Enter 10 lots each if confirmed. |
| Jun 2026 | Market stabilises. ICICIBANK/HDFCBANK + LICHSGFIN/PFC structure strengthens. Add 2nd position. |
| Jul–Dec 2026 | Active trading phase: 3–6 trades/month across all Tier 1 pairs. Run 4-5 concurrent. |
| Jan–Mar 2027 | Lock in gains: reduce position sizes as ₹75L target approaches. Exit all by Mar 25. |

---

## Daily Monitoring

Run every trading day at 09:30 AM:
```bash
python3 execution/scan_valid_signals.py --verbose
```

When ULTRACEMCO/GRASIM hits Z = -1.8 in 60d window: **PREPARE entry orders.**
When ULTRACEMCO/GRASIM hits Z = -2.0: **ENTER immediately (10 lots each).**

---

## Risk Rules

- **Never** average into a losing position
- **Never** hold past 30 days (time stop is hard)
- **Never** trade when Shoonya is down (use yfinance prices only as monitor, not for entry)
- If 3+ positions hit losses simultaneously: exit the 2 weakest (lowest coint score)
- Drawdown limit: **₹7.5L max loss from peak** (15% of ₹50L) → stop and reassess
- Single position max loss: ₹2L (exit before margin call territory)

---

## Scanner Command Reference

```bash
# Daily signal check (run at 9:30 AM)
python3 execution/scan_valid_signals.py --verbose

# Deep dive before entering any trade
python3 execution/deepdive_pair.py ULTRACEMCO GRASIM

# Calendar spread check (run during market hours)
python3 execution/scan_calendar_spreads.py

# Expiry convergence (run 5-10 days before expiry)
python3 execution/scan_expiry_convergence.py
```

---

*Last updated: 2026-04-08*  
*Current account value: ₹50,00,000*  
*Open positions: 0*  
*YTD P&L: ₹0*
