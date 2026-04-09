"""
Calendar Spread Alert Monitor

Continuously monitors FNO calendar spreads during market hours and sends
push notifications to phone via ntfy.sh when alert conditions are met.

Alert Conditions:
  - RVNL spread crosses -15
  - KFINTECH spread crosses -35
  - Any symbol with potential gain > ₹15,000

Usage:
    python3 execution/spread_alert_monitor.py              # Run continuous monitor
    python3 execution/spread_alert_monitor.py --test        # Dry run (no notifications)
    python3 execution/spread_alert_monitor.py --test-notify # Send test notification
    python3 execution/spread_alert_monitor.py --once        # Single scan then exit

Environment:
    NTFY_TOPIC  - ntfy.sh topic name (in .env)
"""

import os
import sys
import time
import json
import logging
import argparse
import requests
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv

# Add execution dir to path
sys.path.append(os.path.join(os.path.dirname(__file__)))
from shoonya_client import ShoonyaClient
from fno_utils import FNO_SYMBOLS

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Config
NTFY_TOPIC = os.getenv('NTFY_TOPIC', 'antigravity-fno-tushar')
SCAN_INTERVAL_MINUTES = 5
ALERT_COOLDOWN_MINUTES = 30
LOG_FILE = os.path.join(os.path.dirname(__file__), '..', '.tmp', 'spread_alerts.log')
STATE_FILE = os.path.join(os.path.dirname(__file__), '..', '.tmp', 'alert_state.json')

# Alert conditions
ALERT_RULES = {
    'RVNL': {'type': 'spread', 'threshold': -15, 'label': 'RVNL spread < -15'},
    'KFINTECH': {'type': 'spread', 'threshold': -35, 'label': 'KFINTECH spread < -35'},
}
GENERAL_GAIN_THRESHOLD = 20000  # ₹20k for any symbol

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='a'),
    ]
)
logger = logging.getLogger('SpreadAlert')


# =============================================================================
# NOTIFICATION
# =============================================================================

def send_notification(title, message, priority='high', tags=None):
    """Send push notification via ntfy.sh."""
    try:
        # Strip emojis from title to avoid latin-1 encoding errors in HTTP headers
        clean_title = title.encode('ascii', 'ignore').decode('ascii').strip()
        if not clean_title:
            clean_title = 'Spread Alert'

        headers = {
            'Title': clean_title,
            'Priority': priority,
        }
        if tags:
            headers['Tags'] = ','.join(tags)

        resp = requests.post(
            f'https://ntfy.sh/{NTFY_TOPIC}',
            data=message.encode('utf-8'),
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info(f'✅ Notification sent: {title}')
            return True
        else:
            logger.error(f'❌ Notification failed ({resp.status_code}): {resp.text}')
            return False
    except Exception as e:
        logger.error(f'❌ Notification error: {e}')
        return False


# =============================================================================
# SPREAD SCANNING (reuses logic from scan_calendar_spreads.py)
# =============================================================================

def get_ltp(api, exchange, token):
    """Get last traded price."""
    ret = api.get_quotes(exchange, token)
    if ret and 'lp' in ret:
        return float(ret['lp'])
    return None


def scan_symbol(api, symbol):
    """Scan a single symbol for calendar spread opportunity."""
    try:
        # Get spot price
        spot_token = None
        search_res = api.searchscrip(exchange='NSE', searchtext=symbol)
        if search_res and 'values' in search_res:
            for res in search_res['values']:
                if res['tsym'] == f"{symbol}-EQ" or res['tsym'] == symbol:
                    spot_token = res['token']
                    break

        if not spot_token:
            return None

        spot_price = get_ltp(api, 'NSE', spot_token)
        if not spot_price:
            return None

        # Get futures
        ret = api.searchscrip(exchange='NFO', searchtext=symbol)
        if not ret or 'values' not in ret:
            return None

        futures = [
            x for x in ret['values']
            if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX')
            and x['symname'] == symbol
        ]

        if len(futures) < 2:
            return None

        def parse_expiry(x):
            try:
                return datetime.strptime(x['exd'], '%d-%b-%Y')
            except:
                return datetime.max

        futures.sort(key=parse_expiry)
        near_fut = futures[0]
        far_fut = futures[1]

        near_price = get_ltp(api, 'NFO', near_fut['token'])
        far_price = get_ltp(api, 'NFO', far_fut['token'])

        if not near_price or not far_price:
            return None

        lot_size = float(near_fut.get('ls', 0))
        near_premium = near_price - spot_price
        far_premium = far_price - spot_price
        spread_diff = far_premium - near_premium

        # Only negative spreads (far < near)
        if spread_diff >= 0:
            return None

        potential_gain = abs(spread_diff) * lot_size

        return {
            'symbol': symbol,
            'spot': spot_price,
            'lot_size': int(lot_size),
            'near_expiry': near_fut['exd'],
            'near_price': near_price,
            'far_expiry': far_fut['exd'],
            'far_price': far_price,
            'spread_diff': round(spread_diff, 2),
            'potential_gain': round(potential_gain, 2),
        }
    except Exception as e:
        return None


def run_scan(api):
    """Scan all FNO symbols and return results."""
    logger.info(f'Scanning {len(FNO_SYMBOLS)} symbols...')
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sym = {
            executor.submit(scan_symbol, api, sym): sym
            for sym in FNO_SYMBOLS
        }
        for future in concurrent.futures.as_completed(future_to_sym):
            res = future.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: x['potential_gain'], reverse=True)
    logger.info(f'Scan complete. {len(results)} negative spreads found.')
    return results


# =============================================================================
# ALERT STATE MANAGEMENT
# =============================================================================

def load_state():
    """Load alert state (cooldown tracking)."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'last_alerts': {}}


def save_state(state):
    """Persist alert state."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def should_alert(state, symbol, alert_key):
    """Check if we should fire an alert (respecting cooldown)."""
    key = f'{symbol}_{alert_key}'
    last_alerts = state.get('last_alerts', {})
    if key in last_alerts:
        last_time = datetime.fromisoformat(last_alerts[key])
        elapsed = (datetime.now() - last_time).total_seconds() / 60
        if elapsed < ALERT_COOLDOWN_MINUTES:
            return False
    return True


def mark_alerted(state, symbol, alert_key):
    """Record that we sent an alert."""
    key = f'{symbol}_{alert_key}'
    state.setdefault('last_alerts', {})[key] = datetime.now().isoformat()
    save_state(state)


# =============================================================================
# ALERT CHECKING
# =============================================================================

def check_alerts(results, state, dry_run=False):
    """Check scan results against alert conditions and fire notifications."""
    alerts_fired = 0

    for res in results:
        symbol = res['symbol']
        spread = res['spread_diff']
        gain = res['potential_gain']

        # Check symbol-specific spread rules
        if symbol in ALERT_RULES:
            rule = ALERT_RULES[symbol]
            if rule['type'] == 'spread' and spread <= rule['threshold']:
                
                # Special Step-Change Logic for RVNL
                # User wants alerts for every ₹1 change when < -15
                should_terminate_cooldown = False
                if symbol == 'RVNL':
                    last_val = state.get('rvnl_last_spread_val')
                    # If first time, or change >= 4.0
                    if last_val is None or abs(spread - last_val) >= 4.0:
                        should_terminate_cooldown = True
                        state['rvnl_last_spread_val'] = spread
                        save_state(state)
                    else:
                        # If change is small (< 1.0), enforce standard cooldown (or silence)
                        # Actually if change is small, we DO NOT want to alert, even if cooldown expired, 
                        # otherwise we get repetitive "-15.2" alerts every 30 mins.
                        # BUT user might want reminder. 
                        # Let's stick to: Alert if time cooldown passed OR shift >= 1.0
                        pass

                if should_terminate_cooldown or should_alert(state, symbol, 'spread'):
                    title = f'🔔 {symbol} Spread Alert!'
                    # Customize title for step change
                    if should_terminate_cooldown and symbol == 'RVNL':
                         if last_val is None:
                             change_dir = "Initial"
                         else:
                             change_dir = "Worsened" if spread < last_val else "Improved"
                         title = f'🔔 RVNL Move: {spread} ({change_dir})'

                    msg = (
                        f"{rule['label']}\n"
                        f"Current Spread: {spread}\n"
                        f"Near: ₹{res['near_price']} ({res['near_expiry']})\n"
                        f"Far: ₹{res['far_price']} ({res['far_expiry']})\n"
                        f"Lot: {res['lot_size']} | Gain: ₹{gain:,.0f}\n"
                        f"Time: {datetime.now().strftime('%H:%M:%S')}"
                    )
                    logger.info(f'🔔 ALERT: {title}\n{msg}')
                    if not dry_run:
                        send_notification(title, msg, priority='urgent', tags=['chart_with_upwards_trend', 'money_with_wings'])
                        mark_alerted(state, symbol, 'spread')
                    alerts_fired += 1

        # Check general gain threshold (for ALL symbols)
        if gain >= GENERAL_GAIN_THRESHOLD:
            if should_alert(state, symbol, 'gain15k'):
                title = f'💰 {symbol} — ₹{gain:,.0f} Calendar Spread!'
                msg = (
                    f"Potential Gain: ₹{gain:,.0f}\n"
                    f"Spread: {spread}\n"
                    f"Near: ₹{res['near_price']} ({res['near_expiry']})\n"
                    f"Far: ₹{res['far_price']} ({res['far_expiry']})\n"
                    f"Lot: {res['lot_size']}\n"
                    f"Time: {datetime.now().strftime('%H:%M:%S')}"
                )
                logger.info(f'💰 ALERT: {title}\n{msg}')
                if not dry_run:
                    send_notification(title, msg, priority='high', tags=['money_with_wings'])
                    mark_alerted(state, symbol, 'gain15k')
                alerts_fired += 1

    return alerts_fired


# =============================================================================
# MARKET HOURS CHECK
# =============================================================================

def is_market_hours():
    """Check if within NSE trading hours (9:15-15:30 IST, Mon-Fri)."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False, 'Weekend'
    market_open = now.replace(hour=9, minute=15, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)
    if now < market_open:
        return False, f'Pre-market (opens {market_open.strftime("%H:%M")})'
    if now > market_close:
        return False, f'Post-market (closed {market_close.strftime("%H:%M")})'
    return True, 'Market open'


# =============================================================================
# MAIN LOOP
# =============================================================================

def run_monitor(dry_run=False, single_run=False):
    """Main monitoring loop."""
    logger.info('=' * 60)
    logger.info(f'CALENDAR SPREAD ALERT MONITOR')
    logger.info(f'Topic: {NTFY_TOPIC}')
    logger.info(f'Interval: {SCAN_INTERVAL_MINUTES} min')
    logger.info(f'Cooldown: {ALERT_COOLDOWN_MINUTES} min')
    logger.info(f'Conditions:')
    for sym, rule in ALERT_RULES.items():
        logger.info(f'  {rule["label"]}')
    logger.info(f'  Any symbol > ₹{GENERAL_GAIN_THRESHOLD:,} gain')
    if dry_run:
        logger.info(f'  MODE: DRY RUN (no notifications)')
    logger.info('=' * 60)

    # Login once, reuse session
    client = ShoonyaClient()
    api = client.login()
    if not api:
        logger.error('Shoonya login failed. Exiting.')
        return

    state = load_state()
    cycle = 0

    while True:
        cycle += 1
        is_open, market_msg = is_market_hours()

        if not is_open and not single_run:
            logger.info(f'⏸️  {market_msg}. Sleeping 5 min...')
            time.sleep(300)
            continue

        logger.info(f'\n--- Scan Cycle #{cycle} @ {datetime.now().strftime("%H:%M:%S")} ---')

        try:
            results = run_scan(api)

            # Check for data issues — notify user and re-login
            if not results or len(results) == 0:
                err_msg = f'Scan returned 0 results at {datetime.now().strftime("%H:%M:%S")}. Re-logging in...'
                logger.warning(err_msg)
                if not dry_run and should_alert(state, '_SYSTEM_', 'no_data'):
                    send_notification(
                        'Data Issue - 0 Results (auto-recovering)',
                        err_msg,
                        priority='default',
                        tags=['warning'],
                    )
                    mark_alerted(state, '_SYSTEM_', 'no_data')
                # Re-login to recover from session expiry
                try:
                    logger.info('Re-logging in to Shoonya...')
                    api = client.login()
                    if api:
                        logger.info('Re-login successful.')
                    else:
                        logger.error('Re-login failed!')
                except Exception as login_err:
                    logger.error(f'Re-login error: {login_err}')
            else:
                # Print summary
                top5 = results[:5]
                logger.info(f'Top opportunities:')
                for r in top5:
                    flag = ''
                    if r['symbol'] in ALERT_RULES:
                        flag = f' [WATCHED: threshold={ALERT_RULES[r["symbol"]]["threshold"]}]'
                    logger.info(
                        f'  {r["symbol"]}: Spread={r["spread_diff"]} '
                        f'Gain=₹{r["potential_gain"]:,.0f}{flag}'
                    )

            # Check and fire alerts
            alerts = check_alerts(results, state, dry_run=dry_run)
            logger.info(f'Alerts fired: {alerts}')

        except Exception as e:
            err_msg = f'Scan error at {datetime.now().strftime("%H:%M:%S")}: {str(e)[:200]}'
            logger.error(err_msg)
            # Notify user about the error
            if not dry_run and should_alert(state, '_SYSTEM_', 'scan_error'):
                send_notification(
                    'Scan Error - Needs Attention',
                    err_msg,
                    priority='high',
                    tags=['x'],
                )
                mark_alerted(state, '_SYSTEM_', 'scan_error')
            # Try to re-login
            try:
                api = client.login()
            except:
                pass

        if single_run:
            logger.info('Single run complete. Exiting.')
            break

        logger.info(f'Next scan in {SCAN_INTERVAL_MINUTES} minutes...')
        time.sleep(SCAN_INTERVAL_MINUTES * 60)


def test_notification():
    """Send a test notification to verify ntfy.sh is working."""
    logger.info(f'Sending test notification to topic: {NTFY_TOPIC}')
    success = send_notification(
        title='🧪 Antigravity FNO — Test Alert',
        message=(
            f'If you see this, notifications are working!\n'
            f'Topic: {NTFY_TOPIC}\n'
            f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        ),
        priority='default',
        tags=['white_check_mark'],
    )
    if success:
        logger.info('✅ Test notification sent! Check your phone.')
    else:
        logger.error('❌ Test notification failed. Check your internet connection.')


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calendar Spread Alert Monitor')
    parser.add_argument('--test', action='store_true',
                        help='Dry run — scan without sending notifications')
    parser.add_argument('--test-notify', action='store_true',
                        help='Send a test notification')
    parser.add_argument('--once', action='store_true',
                        help='Run a single scan cycle then exit')
    parser.add_argument('--topic', type=str,
                        help='Override ntfy topic')
    parser.add_argument('--interval', type=int, default=SCAN_INTERVAL_MINUTES,
                        help=f'Scan interval in minutes (default: {SCAN_INTERVAL_MINUTES})')
    args = parser.parse_args()

    if args.topic:
        NTFY_TOPIC = args.topic

    if args.interval != SCAN_INTERVAL_MINUTES:
        SCAN_INTERVAL_MINUTES = args.interval

    if args.test_notify:
        test_notification()
    elif args.test:
        run_monitor(dry_run=True, single_run=True)
    elif args.once:
        run_monitor(dry_run=False, single_run=True)
    else:
        run_monitor()
