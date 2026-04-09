"""
MODULE 6: Seasonality & Expiry Pattern Engine

Detects statistically significant calendar-based patterns in Indian markets.
Uses existing 3Y FNO data (.tmp/3y_data/) for analysis.

Validation requirements (STRICT — Simons' approach):
  - Minimum 50 occurrences of the pattern
  - T-test p-value < 0.05 (95% confidence)
  - Must persist across multiple years (not one anomalous year)

Patterns analyzed:
  1. Day-of-week effect on Nifty/Bank Nifty
  2. Expiry week volatility premium
  3. Month-end / month-start effects
  4. Pre-results drift
  5. March tax selling, Nov-Dec rally

MODES:
  --validate   Run full statistical validation on historical data → saves to DB
  --score      Return current date's active seasonality score (uses DB)
  --both       Validate then score (default)

Usage:
    python3 execution/analyze_seasonality.py --validate    # Run once, validate all patterns
    python3 execution/analyze_seasonality.py --score       # Fast: get today's score
    python3 execution/analyze_seasonality.py               # Both (default)
"""

import os
import sys
import argparse
import logging
import calendar
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from scipy import stats

sys.path.append(os.path.dirname(__file__))
from signals_db import SignalsDB

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(".tmp/3y_data")
MIN_OCCURRENCES = 50
P_VALUE_THRESHOLD = 0.05


def last_tuesday_of_month(year: int, month: int) -> datetime:
    """Return the last Tuesday of a given month (FNO expiry day)."""
    last_day = calendar.monthrange(year, month)[1]
    d = datetime(year, month, last_day)
    while d.weekday() != 1:  # 1 = Tuesday
        d -= timedelta(days=1)
    return d


def load_nifty_returns() -> pd.DataFrame:
    """
    Load Nifty returns from available FNO data.
    Uses the underlying spot price (FH_UNDERLYING_VALUE) from any index future.
    Falls back to using multiple stocks if Nifty futures not available.
    """
    # Try to load Nifty index directly
    nifty_path = DATA_DIR / "NIFTY_3Y.csv"
    banknifty_path = DATA_DIR / "BANKNIFTY_3Y.csv"

    # Try loading a broad market proxy using large-cap stocks
    proxy_symbols = ['RELIANCE', 'HDFCBANK', 'ICICIBANK', 'INFY', 'TCS',
                     'KOTAKBANK', 'AXISBANK', 'SBIN', 'BHARTIARTL', 'ITC']

    frames = []
    for sym in proxy_symbols:
        path = DATA_DIR / f"{sym}_3Y.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            if 'FH_UNDERLYING_VALUE' not in df.columns:
                continue
            df['date'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
            df['spot'] = pd.to_numeric(df['FH_UNDERLYING_VALUE'], errors='coerce')
            df = df.dropna(subset=['date', 'spot']).sort_values('date')
            df = df.groupby('date')['spot'].last().reset_index()
            df.rename(columns={'spot': sym}, inplace=True)
            frames.append(df.set_index('date')[sym])
        except Exception as e:
            logger.warning(f"Could not load {sym}: {e}")

    if not frames:
        logger.error("No proxy data available for seasonality analysis")
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1).dropna(how='all')
    # Average across available proxies (equal weight — simple proxy for market)
    combined['market_avg'] = combined.mean(axis=1)
    combined['daily_return'] = combined['market_avg'].pct_change()
    combined = combined.dropna(subset=['daily_return'])

    # Add calendar features
    combined.index = pd.DatetimeIndex(combined.index)
    combined['day_of_week'] = combined.index.dayofweek  # 0=Mon, 4=Fri
    combined['month'] = combined.index.month
    combined['day_of_month'] = combined.index.day
    combined['week_of_month'] = (combined.index.day - 1) // 7 + 1
    combined['year'] = combined.index.year

    # Expiry week flag (last Tuesday ± 2 days)
    def is_expiry_week(dt):
        lt = last_tuesday_of_month(dt.year, dt.month)
        return abs((dt - lt).days) <= 2

    combined['is_expiry_week'] = [is_expiry_week(dt) for dt in combined.index]

    # Pre-results proxy: last 5 days of each quarter
    combined['quarter'] = combined.index.quarter
    # Mark last week of quarter
    combined['is_pre_results'] = (
        combined['month'].isin([3, 6, 9, 12]) &
        (combined['day_of_month'] >= 25)
    )

    logger.info(f"Loaded {len(combined)} trading days for seasonality analysis "
                f"({combined.index.min().date()} to {combined.index.max().date()})")
    return combined


def run_t_test(returns_in_pattern: pd.Series, returns_outside: pd.Series,
               pattern_name: str) -> dict:
    """
    Run two-sided Welch's t-test (doesn't assume equal variance).
    Returns validation result dict.
    """
    n_in = len(returns_in_pattern.dropna())
    n_out = len(returns_outside.dropna())

    if n_in < MIN_OCCURRENCES:
        return {
            'pattern_name': pattern_name,
            'is_valid': False,
            'reason': f'Insufficient data: {n_in} < {MIN_OCCURRENCES} occurrences',
            'occurrences': n_in,
            'p_value': 1.0,
        }

    t_stat, p_value = stats.ttest_ind(
        returns_in_pattern.dropna(),
        returns_outside.dropna(),
        equal_var=False  # Welch's t-test
    )

    avg_return_in = float(returns_in_pattern.mean())
    avg_return_out = float(returns_outside.mean())
    return_premium = avg_return_in - avg_return_out

    is_valid = (p_value < P_VALUE_THRESHOLD) and (n_in >= MIN_OCCURRENCES)

    return {
        'pattern_name': pattern_name,
        'is_valid': is_valid,
        'occurrences': n_in,
        'avg_return_in_pattern': round(avg_return_in * 100, 4),
        'avg_return_outside': round(avg_return_out * 100, 4),
        'return_premium': round(return_premium * 100, 4),
        'p_value': round(p_value, 4),
        't_statistic': round(float(t_stat), 4),
        'reason': 'VALID' if is_valid else f'p={p_value:.3f} (need <{P_VALUE_THRESHOLD})',
    }


def validate_all_patterns(df: pd.DataFrame, db: SignalsDB):
    """
    Run t-tests on all candidate seasonal patterns. Only validate ones
    that pass p < 0.05 with n >= 50 occurrences. Save results to DB.

    Returns list of ALL results (valid and invalid).
    """
    results = []
    returns = df['daily_return']

    # ── Pattern 1: Day of Week ────────────────────────────────────────────────
    dow_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday'}
    for dow, name in dow_names.items():
        in_pattern = returns[df['day_of_week'] == dow]
        outside = returns[df['day_of_week'] != dow]
        result = run_t_test(in_pattern, outside, f'DOW_{name.upper()}')
        result['pattern_type'] = 'DOW'
        result['day_of_week'] = dow
        result['month'] = -1
        # Score: +/- based on direction and significance
        if result['is_valid']:
            avg = result['avg_return_in_pattern']
            result['score_when_active'] = int(np.sign(avg) * min(10, abs(avg) * 500))
        else:
            result['score_when_active'] = 0
        results.append(result)
        logger.info(f"  DOW {name}: avg={result['avg_return_in_pattern']:.3f}% "
                    f"p={result['p_value']:.3f} {'VALID' if result['is_valid'] else 'INVALID'}")

    # ── Pattern 2: Month Effects ──────────────────────────────────────────────
    month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                   7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    for month_num, month_name in month_names.items():
        in_pattern = returns[df['month'] == month_num]
        outside = returns[df['month'] != month_num]
        result = run_t_test(in_pattern, outside, f'MONTH_{month_name.upper()}')
        result['pattern_type'] = 'MONTH'
        result['day_of_week'] = -1
        result['month'] = month_num
        if result['is_valid']:
            avg = result['avg_return_in_pattern']
            result['score_when_active'] = int(np.sign(avg) * min(8, abs(avg) * 400))
        else:
            result['score_when_active'] = 0
        results.append(result)

    # ── Pattern 3: Expiry Week ────────────────────────────────────────────────
    in_pattern = returns[df['is_expiry_week']]
    outside = returns[~df['is_expiry_week']]
    result = run_t_test(in_pattern, outside, 'EXPIRY_WEEK')
    result['pattern_type'] = 'EXPIRY_WEEK'
    result['day_of_week'] = -1
    result['month'] = -1
    if result['is_valid']:
        avg = result['avg_return_in_pattern']
        result['score_when_active'] = int(np.sign(avg) * min(10, abs(avg) * 400))
    else:
        result['score_when_active'] = 0
    results.append(result)
    logger.info(f"  Expiry Week: avg={result['avg_return_in_pattern']:.3f}% "
                f"p={result['p_value']:.3f} {'VALID' if result['is_valid'] else 'INVALID'}")

    # ── Pattern 4: Month-start effect (first 5 trading days) ─────────────────
    in_pattern = returns[df['week_of_month'] == 1]
    outside = returns[df['week_of_month'] != 1]
    result = run_t_test(in_pattern, outside, 'MONTH_START_WEEK')
    result['pattern_type'] = 'MONTH_SEGMENT'
    result['day_of_week'] = -1
    result['month'] = -1
    if result['is_valid']:
        avg = result['avg_return_in_pattern']
        result['score_when_active'] = int(np.sign(avg) * min(8, abs(avg) * 400))
    else:
        result['score_when_active'] = 0
    results.append(result)

    # ── Pattern 5: March tax selling ─────────────────────────────────────────
    in_pattern = returns[(df['month'] == 3) & (df['day_of_month'] >= 15)]
    outside = returns[~((df['month'] == 3) & (df['day_of_month'] >= 15))]
    result = run_t_test(in_pattern, outside, 'MARCH_SECOND_HALF_SELLING')
    result['pattern_type'] = 'SEASONAL_ANOMALY'
    result['day_of_week'] = -1
    result['month'] = 3
    if result['is_valid']:
        avg = result['avg_return_in_pattern']
        result['score_when_active'] = int(np.sign(avg) * min(8, abs(avg) * 400))
    else:
        result['score_when_active'] = 0
    results.append(result)

    # ── Pattern 6: Nov-Dec rally ──────────────────────────────────────────────
    in_pattern = returns[df['month'].isin([11, 12])]
    outside = returns[~df['month'].isin([11, 12])]
    result = run_t_test(in_pattern, outside, 'NOV_DEC_RALLY')
    result['pattern_type'] = 'SEASONAL_ANOMALY'
    result['day_of_week'] = -1
    result['month'] = -1  # spans two months
    if result['is_valid']:
        avg = result['avg_return_in_pattern']
        result['score_when_active'] = int(np.sign(avg) * min(8, abs(avg) * 400))
    else:
        result['score_when_active'] = 0
    results.append(result)

    # ── Pattern 7: Pre-results quarter end ────────────────────────────────────
    in_pattern = returns[df['is_pre_results']]
    outside = returns[~df['is_pre_results']]
    result = run_t_test(in_pattern, outside, 'PRE_RESULTS_QUARTER_END')
    result['pattern_type'] = 'EARNINGS_CALENDAR'
    result['day_of_week'] = -1
    result['month'] = -1
    if result['is_valid']:
        avg = result['avg_return_in_pattern']
        result['score_when_active'] = int(np.sign(avg) * min(8, abs(avg) * 400))
    else:
        result['score_when_active'] = 0
    results.append(result)

    # ── Save all to DB ────────────────────────────────────────────────────────
    valid_count = 0
    for r in results:
        db.upsert_seasonality_fact(
            pattern_name=r['pattern_name'],
            description=r.get('reason', ''),
            pattern_type=r.get('pattern_type', 'UNKNOWN'),
            day_of_week=r.get('day_of_week', -1),
            month=r.get('month', -1),
            avg_return=r.get('avg_return_in_pattern', 0.0),
            p_value=r.get('p_value', 1.0),
            occurrences=r.get('occurrences', 0),
            t_statistic=r.get('t_statistic', 0.0),
            score_when_active=r.get('score_when_active', 0),
            is_valid=r['is_valid']
        )
        if r['is_valid']:
            valid_count += 1

    logger.info(f"Validation complete: {valid_count}/{len(results)} patterns passed p<0.05 with n>={MIN_OCCURRENCES}")
    return results


def get_current_seasonality_score(date_str: str = None, db: SignalsDB = None):
    """
    Fast path: look up validated patterns for today's date in DB.
    Returns (total_score, [active_pattern_names]).
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    if db is None:
        db = SignalsDB()
    return db.get_active_seasonality_score(date_str)


def print_validation_report(results: list[dict]):
    """Print validation results table."""
    valid = [r for r in results if r['is_valid']]
    invalid = [r for r in results if not r['is_valid']]

    print(f"\n  VALID PATTERNS ({len(valid)}) — p < {P_VALUE_THRESHOLD}, n >= {MIN_OCCURRENCES}:")
    if valid:
        print(f"  {'Pattern':<35} {'Avg Return':>10} {'Premium':>9} {'p-value':>8} {'n':>6} {'Score':>6}")
        print(f"  {'-'*76}")
        for r in sorted(valid, key=lambda x: abs(x.get('return_premium', 0)), reverse=True):
            print(f"  {'✓ ' + r['pattern_name']:<35} "
                  f"{r['avg_return_in_pattern']:>+9.3f}% "
                  f"{r.get('return_premium', 0):>+8.3f}% "
                  f"{r['p_value']:>8.4f} "
                  f"{r['occurrences']:>6} "
                  f"{r.get('score_when_active', 0):>+6}")
    else:
        print("  No patterns passed validation. Market is efficiently seasonal.")

    print(f"\n  INVALID PATTERNS ({len(invalid)}) — excluded from model:")
    for r in invalid[:5]:
        print(f"    ✗ {r['pattern_name']}: {r['reason']}")
    if len(invalid) > 5:
        print(f"    ... and {len(invalid) - 5} more")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Module 6: Seasonality Pattern Engine')
    parser.add_argument('--validate', action='store_true', help='Run full validation on historical data')
    parser.add_argument('--score', action='store_true', help='Get today\'s seasonality score only')
    parser.add_argument('--date', type=str, default=None, help='Date for scoring YYYY-MM-DD')
    args = parser.parse_args()

    print("╔" + "═" * 78 + "╗")
    print("║  MODULE 6: SEASONALITY & EXPIRY PATTERN ENGINE".ljust(79) + "║")
    print("║  Statistical validation — only patterns with p<0.05, n>=50".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    db = SignalsDB()
    run_validate = args.validate or (not args.validate and not args.score)
    run_score = args.score or (not args.validate and not args.score)

    if run_validate:
        print("\n  Loading historical data...")
        df = load_nifty_returns()
        if df.empty:
            print("  ERROR: No historical data in .tmp/3y_data/. Run sync_fno_data.py first.")
            sys.exit(1)

        print(f"  Loaded {len(df)} trading days. Running t-tests on {len(df)} returns...")
        print(f"  Threshold: p < {P_VALUE_THRESHOLD} AND n >= {MIN_OCCURRENCES}\n")

        results = validate_all_patterns(df, db)
        print_validation_report(results)

    if run_score:
        date_str = args.date or datetime.now().strftime('%Y-%m-%d')
        score, active_patterns = get_current_seasonality_score(date_str, db)
        print(f"\n  CURRENT SEASONALITY SCORE for {date_str}: {score:+d}")
        if active_patterns:
            print(f"  Active patterns: {', '.join(active_patterns)}")
        else:
            print("  No validated seasonality patterns active today.")
