---
description: Calendar spread alert monitoring system with phone notifications
---

# Calendar Spread Alert System

## Overview
Automated monitoring of FNO calendar spreads with push notifications to phone via [ntfy.sh](https://ntfy.sh).

## Alert Conditions
1. **RVNL:** Spread crosses **-15** → Urgent notification
2. **KFINTECH:** Spread crosses **-35** → Urgent notification
3. **Any FNO symbol:** Potential gain exceeds **₹15,000** → High priority notification

## Phone Setup
1. Install **ntfy** app on phone
   - [Android](https://play.google.com/store/apps/details?id=io.heckel.ntfy)
   - [iOS](https://apps.apple.com/app/ntfy/id1625396347)
2. Subscribe to topic: `antigravity-fno-tushar` (or value from `NTFY_TOPIC` in `.env`)

## Commands

### Start Continuous Monitor
```bash
python3 execution/spread_alert_monitor.py
```
Runs every 10 minutes during market hours (9:15-15:30 IST, Mon-Fri). Auto-pauses outside market hours.

### Test Notification
```bash
python3 execution/spread_alert_monitor.py --test-notify
```

### Dry Run (No Notifications)
```bash
python3 execution/spread_alert_monitor.py --test
```

### Single Scan
```bash
python3 execution/spread_alert_monitor.py --once
```

### Custom Interval
```bash
python3 execution/spread_alert_monitor.py --interval 5  # Every 5 min
```

## Configuration
- `NTFY_TOPIC` in `.env` — ntfy.sh topic name
- `SCAN_INTERVAL_MINUTES` — default 10 min between scans
- `ALERT_COOLDOWN_MINUTES` — same alert won't re-fire for 30 min
- Alert rules: Edit `ALERT_RULES` dict in `spread_alert_monitor.py`

## Files
- **Script:** `execution/spread_alert_monitor.py`
- **Log:** `.tmp/spread_alerts.log`
- **State:** `.tmp/alert_state.json` (cooldown tracker)

## Adding New Alert Conditions
Edit the `ALERT_RULES` dict in the script:
```python
ALERT_RULES = {
    'RVNL': {'type': 'spread', 'threshold': -15, 'label': 'RVNL spread < -15'},
    'KFINTECH': {'type': 'spread', 'threshold': -35, 'label': 'KFINTECH spread < -35'},
    # Add new symbol-specific rules here
}
GENERAL_GAIN_THRESHOLD = 15000  # ₹ threshold for any symbol
```
