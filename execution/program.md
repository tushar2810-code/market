# AutoResearch Program — Pairs/Calendar Backtest Optimization

## Objective

Maximize composite score:
```
score = net_pnl * (win_rate / 100) / max(1, -worst_month / 1e5)
```
Rewards: high returns AND high WR.
Penalizes: large monthly drawdowns.

## Invariants (NEVER change)

- `STARTING_CAP = 25,00,000` (Rs.25L)
- `BACKTEST_START = 2024-04-01`
- `BACKTEST_END = 2026-03-31`
- `CHARGE_RATE = 0.12` (12% of |gross|)
- `MARGIN_RATE = 0.15` (15% SPAN margin)
- `MAX_UTILISATION = 0.70` (70% capital deployed max)
- Data files in `.tmp/3y_data/`
- Calendar symbols: SAIL, HINDUNILVR, COLPAL, ITC, OBEROIRLTY

## Tunable Parameters (in params.json)

| Parameter | Default | Range | What it does |
|-----------|---------|-------|-------------|
| SSS_THRESHOLD | 2.0 | [1.5, 8.0] | Min Signal Strength Score to enter. Lower = more trades |
| Z_EXIT | 1.0 | [0.1, 2.0] | Z-score exit threshold. Lower = tighter profit target |
| PAIRS_TIME_STOP | 60 | [15, 90] | Max days in position before force exit |
| PAIRS_MIN_Z_ENTRY | 1.5 | [1.0, 2.5] | Min |Z| to enter. Lower = more signals |
| PAIRS_MAX_Z_ENTRY | 4.0 | [3.0, 6.0] | Max |Z| to enter. Higher = more signals |
| MAX_COMPOUND_SCALE | 2.5 | [1.0, 4.0] | Max lot multiplier from compounding |
| CORR_MIN | 0.3 | [0.1, 0.6] | Min 60d correlation to enter |
| STRUCT_BREAK_Z_MULT | 1.5 | [1.2, 3.0] | Exit if |Z| > X * entry |Z| |
| STRUCT_BREAK_CORR_FLOOR | 0.25 | [0.1, 0.5] | Exit if correlation drops below |
| SCALE_CONC_CAP | 0.15 | [0.05, 0.30] | Max notional per leg for scale calc |
| PAIRS_MAX_POSITIONS | 8 | [3, 12] | Max simultaneous pair positions |

## Optimization Rules

1. Change at most 2 parameters per iteration
2. Use gaussian perturbation (sigma = 15% of range)
3. Run full backtest with new params
4. If score improves: commit params.json via git
5. If score regresses: revert to previous best
6. Log every trial (accepted and rejected)
7. Never change the backtest engine, data loading, or charge calculations

## Parameter Interactions

- SSS_THRESHOLD x Z_EXIT: both affect trade count. Lowering both = many trades but lower WR
- STRUCT_BREAK_Z_MULT x PAIRS_TIME_STOP: struct break catches divergences early; time stop is the backstop
- CORR_MIN x STRUCT_BREAK_CORR_FLOOR: entry corr filter vs exit corr filter. Entry should be >= exit
- SCALE_CONC_CAP x MAX_COMPOUND_SCALE: both limit position size growth. Tighter = safer but less compounding
- PAIRS_MAX_POSITIONS x MAX_COMPOUND_SCALE: more positions + bigger positions = more capital needed

## Running

```bash
cd "/Users/tushar/Documents/Antigravity FNO"
python3 execution/autoresearch.py --iterations 50
```

Monitor progress: `.tmp/autoresearch_log.csv`
Best params: `.tmp/autoresearch_best.json`
