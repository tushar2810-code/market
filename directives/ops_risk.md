# RISK MANAGEMENT & POSITION SIZING

This is the most important directive. You can have 60% win rate and still go broke with bad sizing. Kelly solves this.

---

## HARD LIMITS (NO EXCEPTIONS)

```
Max capital deployed at any time:     70%
Max per strategy:                     40%
Max per single position:              15%
Daily drawdown kill switch:           -3%  → Close ALL. 48-hour cooldown.
Max Kelly mode:                       75% of Full Kelly
```

---

## KELLY CRITERION

### For Single Instrument
```bash
python3 execution/kelly_sizer.py \
  --wr 0.67 --avg-win 3.5 --avg-loss 3.0 \
  --capital 1000000 \
  --price 2800 --lot-size 350
```

### For Pair Trade
```bash
python3 execution/kelly_sizer.py \
  --wr 0.67 --avg-win 3.5 --avg-loss 3.0 \
  --capital 1000000 \
  --price-a 1800 --lot-a 350 \
  --price-b 1400 --lot-b 400
```

### Formula
```
kelly_full = (WR × (avg_win/avg_loss) − (1−WR)) / (avg_win/avg_loss)
position_size = 0.5 × kelly_full × capital   ← Half-Kelly (default)
```

### Kelly Modes
| Mode | Use When |
|---|---|
| Half-Kelly | Default. Always safe. |
| 75% of Full Kelly | Edge is proven over 50+ trades with consistent WR |
| Full Kelly | Never. Ruin risk is too high. |

---

## BACKTESTED WIN RATES (USE THESE FOR SIZING)

| Strategy | WR | Avg Win | Avg Loss | Kelly (Half) |
|---|---|---|---|---|
| Momentum RSI (LONG only) | 67% | 2.7% | 2.0% | ~8.5% capital |
| Calendar Spread (Tier 1) | 75–83% | 0.55% | 0.4% | ~9% capital |
| Pair Trading (Tier 1) | ~65–70% | 1.2% | 1.5% | ~4% capital |

Recalculate Kelly whenever WR changes by more than 5 percentage points.

---

## WHEN TO CUT POSITIONS

1. **Stop hit:** Pair Z > 3.5 → exit. Calendar spread premium expands 100% from entry → exit.
2. **Daily −3% drawdown:** Close everything. Wait 48 hours before re-entering.
3. **Structural break detected:** Corporate action, regulatory event, M&A on one leg.
4. **Data integrity failure:** Safety gate rejection. Do not trade.
5. **Model uncertainty:** If you're not sure, do nothing. The next signal will come.

---

## COMPOUNDING PROTOCOL

Reinvest ALL profits into next trade's Kelly calculation. Never withdraw during a drawdown. Let compounding work.

Recalculate capital available before every new position:
```
available_capital = total_capital × (1 - current_deployment%)
new_position_size = kelly_pct × available_capital
```
