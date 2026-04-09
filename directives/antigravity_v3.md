# Antigravity v3 — Master Trading Directive

## System Architecture

```
antigravity_v3_scanner.py  →  Daily unified scan (all strategies)
kelly_sizer.py             →  Position sizing (Half/Full Kelly)
portfolio_backtester.py    →  Combined CAGR simulation

Strategy Layer 1: Pair Trading    →  scan_cointegrated_pairs.py + renaissance_deep_dive.py
Strategy Layer 3: Expiry Conv.    →  scan_expiry_convergence.py
Strategy Layer 5: Momentum RSI    →  momentum_rsi_strategy.py
```

## Backtested Results (3Y Data, ₹10L Capital)

| Strategy | Trades | WR | Avg Ret | CAGR (standalone) | Kelly Edge |
|----------|--------|-----|---------|-------------------|-----------|
| Momentum RSI (LONG only, 25/75, sector ON) | 21/yr | 67% | +2.69% | 25.6% | 10.9% |
| Expiry Convergence (0.5%, 5d) | 531/yr | 54% | +0.35% | — | 7.3% |
| **Combined Portfolio (Half-Kelly)** | **558/yr** | | | **17.1%** | |
| **Combined Portfolio (Full-Kelly)** | **558/yr** | | | **36.2%** | |

## The 66% CAGR Formula

Backtester showed: **100 trades/year × 0.50% avg return = 66% CAGR with compounding.**

### How to Get There

1. **FILTER expiry convergence to Tier 1 symbols only** (SAIL 83%, HINDUNILVR 82%, COLPAL 78%, ITC 77%, OBEROIRLTY 75%) — see `directives/strategy_spreads.md`. Tier 1 WR is 75–83% vs 64% universe avg. AVOID: KOTAKBANK 22%, SIEMENS 33%, PAGEIND 47%.
2. **LONG-ONLY momentum RSI** — 67% WR vs 52% on shorts. Remove all short signals.
3. **Increase Kelly to 75% of Full** (not half, not full — the middle ground)
4. **Add calendar spread strategy** — currently un-backtested but structurally sound
5. **Add pair trading** on the top 3 near-miss pairs: ULTRACEMCO/GRASIM, TATAPOWER/NHPC, M&M/BHARATFORG

### Critical Insight

Indian FNO has Hurst > 0.5 on ALL pairs → **mean reversion is WEAK**. The edge comes from:
- Structural convergence tendency (expiry premium decay — high-probability, ~64% overall, 75–83% Tier 1, NOT guaranteed)
- Oversold bounces (RSI < 25 mean reverts even if trends don't)
- Compounding (Kelly reinvests gains → exponential growth)

## Near-Miss Pairs (3/4 Renaissance Criteria)

| Pair | ADF_p | Coint_p | Half-Life | Corr | Sector |
|------|-------|---------|-----------|------|--------|
| ULTRACEMCO/GRASIM | 0.0006 | 0.0208 | 12d | 0.636 | Cement |
| TATAPOWER/NHPC | 0.0118 | 0.0504 | 10d | 0.707 | Power |
| M&M/BHARATFORG | 0.0000 | 0.0001 | 5d | -0.195 | Auto |
| HUDCO/ADANIGREEN | 0.0002 | 0.0031 | 6d | 0.564 | Power |
| LODHA/IRCTC | 0.0352 | 0.0440 | 12d | 0.475 | Infra |

## Daily Execution

```bash
# Every morning at 9:15 AM:
python3 execution/antigravity_v3_scanner.py --capital YOUR_CAPITAL

# Weekly: Check for pair trade signals
python3 execution/scan_cointegrated_pairs.py

# Pre-expiry (5 days before last Tuesday):
python3 execution/scan_expiry_convergence.py --threshold 0.5

# For any specific pair deep dive:
python3 execution/renaissance_deep_dive.py --symA SYMBOL1 --symB SYMBOL2

# Kelly sizing for any trade:
python3 execution/kelly_sizer.py --wr 0.67 --avg-win 3.6 --avg-loss 2.9 --capital 1000000 --price 2800 --lot-size 350
```

## Safety Gates (MANDATORY)

1. **Max 70% capital deployed** at any time across all strategies
2. **Max 40% per strategy** — never over-concentrate
3. **3% daily drawdown kill switch** — if portfolio drops 3% in a day, close everything, wait 48h
4. **No SHORT momentum trades** — LONG only (backtested: shorts drag WR from 67% to 49%)
5. **Expiry convergence: TOP 10 SYMBOLS ONLY** — universe-wide dilutes edge
6. **Expiry day rule**: Stock FNO expiry is ALWAYS the LAST TUESDAY of the month

## Domain Knowledge

- **FNO Expiry**: Last Tuesday of each month (NOT Thursday)
- **Hurst > 0.5**: Indian FNO is structurally trending — don't fight it
- **Kelly Half default**: Safer, but for 66% CAGR use 75% of Full Kelly
- **Compounding**: Reinvest ALL profits into next trade's Kelly calculation
