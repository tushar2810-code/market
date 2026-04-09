# ALERTS & MONITORING

---

## WHAT GETS ALERTED

| Trigger | Priority | Channel |
|---|---|---|
| Calendar spread potential gain > ₹15,000 | High | ntfy.sh push |
| Composite score STRONG_BUY (≥60) | High | ntfy.sh push |
| FII regime flip (bearish→bullish or opposite) | Normal | ntfy.sh push |
| Insider BUY_CLUSTER detected | Normal | ntfy.sh push |
| Any pair Z-score > 2.5 (SSS > 6) | High | ntfy.sh push |

## SETUP

1. Install ntfy app: [Android](https://play.google.com/store/apps/details?id=io.heckel.ntfy) or [iOS](https://apps.apple.com/app/ntfy/id1625396347)
2. Subscribe to topic: value of `NTFY_TOPIC` in `.env`
3. `.env` must have: `NTFY_TOPIC=antigravity-fno-tushar`

## COMMANDS

```bash
# Continuous monitor during market hours (9:15–15:30, every 10 min)
python3 execution/spread_alert_monitor.py

# Test that notifications work
python3 execution/spread_alert_monitor.py --test-notify

# Single scan, no loop
python3 execution/spread_alert_monitor.py --once

# Custom interval (minutes)
python3 execution/spread_alert_monitor.py --interval 5

# Daily report with notification
python3 execution/generate_daily_report.py --notify
```

## CUSTOMIZING ALERTS

Edit `ALERT_RULES` dict in `execution/spread_alert_monitor.py`:
```python
ALERT_RULES = {
    'SYMBOL': {'type': 'spread', 'threshold': -20, 'label': 'Description'},
}
GENERAL_GAIN_THRESHOLD = 15000  # ₹
```

## ALERT COOLDOWN
Same alert won't re-fire for 30 minutes. State stored in `.tmp/alert_state.json`.
