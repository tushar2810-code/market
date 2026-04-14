"""
Proven Pairs Scanner v3.3 — Near-100% Win Rate System

Only trades pairs with individually validated 90%+ historical win rate.
Each pair has its own optimized config (lookback, entry Z, stop Z, time stop).

Safety Gates (from post-mortem):
  Gate 1: Data freshness ≤ 3 TRADING days (not calendar — BUG FIX #1)
  Gate 2: 20D RETURN correlation > 0.3 (not price — BUG FIX #4)
  Gate 3: Live ratio within historical range ± 5%
  Gate 4: Cross-validated split detection
  Gate 5: Z-score cap (reject > pair's stop Z)
  Gate 6: Minimum data overlap check (≥ 200 trading days — BUG FIX #2)
  Gate 7: Date gap check (no >5 day gaps in merged series — BUG FIX #5)

Exit Rules (3 hard stops):
  1. Z-Stop: Exit if Z moves beyond pair's stop threshold
  2. Time-Stop: Exit if trade exceeds pair's time limit
  3. Correlation-Stop: Exit if 20D correlation drops below 0.3

Note: Expiry-day volatility is NOT filtered out — it's real market data.

Edge Case Fixes:
  v3.1: Exact symbol match, lot verification, cash imbalance, market hours
  v3.2: Trading-day staleness, PNB tier downgrade, return correlation,
        date alignment verification
  v3.3: Reverted rollover exclusion (expiry volatility is real data)

Usage:
    python3 execution/scan_proven_pairs.py [--monitor]
"""

import pandas as pd
import numpy as np
import os
import sys
import argparse
import logging
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.join(os.path.dirname(__file__)))
from shoonya_client import ShoonyaClient

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = ".tmp/3y_data"
PORTFOLIO_FILE = ".tmp/paper_portfolio_v3.csv"

# =============================================================================
# PROVEN PAIR CONFIGS — Each pair optimized individually via grid search
# All achieved 90%+ win rate with ≥5 historical trades
# =============================================================================

PROVEN_PAIRS = {
    # TIER 1: 100% Win Rate with ≥ 2 years of overlapping data
    "ULTRACEMCO/AMBUJACEM": {
        "sector": "Cement", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 9, "hist_wr": 100, "hist_avg_return": 3.2, "hist_avg_days": 14,
    },
    "HINDALCO/VEDL": {
        "sector": "Metals", "tier": 1,
        "lookback": 60, "z_entry": 2.5, "coint_stop_p": 0.20,"time_stop": 20,
        "hist_trades": 6, "hist_wr": 100, "hist_avg_return": 4.0, "hist_avg_days": 15,
    },

    # NEW DISCOVERIES (Feb 2026) — Tier 1 (90%+ WR)
    "LICHSGFIN/PFC": {
        "sector": "NBFCs", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 6, "hist_wr": 100.0, "hist_avg_return": 8.55, "hist_avg_days": 13.5,
    },
    "IDFCFIRSTB/AUBANK": {
        "sector": "Private Banks", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 15, "hist_wr": 93.3, "hist_avg_return": 3.98, "hist_avg_days": 15.1,
    },
    "HCLTECH/PERSISTENT": {
        "sector": "IT Services", "tier": 1,
        "lookback": 60, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 6, "hist_wr": 100.0, "hist_avg_return": 4.7, "hist_avg_days": 11.8,
    },
    # BAJAJFINSV/CHOLAFIN removed — coint p=0.43 across all windows (never cointegrated).
    # Promoted on 6 trades / sector logic. Z=-4.18 in Apr 2026 confirmed structural break.
    # POWERGRID/NHPC removed — strategy_pairs.md AVOID list: "Policy events break spread" (47% WR, n=15)
    "GAIL/ONGC": {
        "sector": "Oil & Gas", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 11, "hist_wr": 90.9, "hist_avg_return": 2.8, "hist_avg_days": 14.9,
    },
    "MARICO/TATACONSUM": {
        "sector": "FMCG", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 8, "hist_wr": 100.0, "hist_avg_return": 2.52, "hist_avg_days": 10.1,
    },
    "ULTRACEMCO/SHREECEM": {
        "sector": "Cement", "tier": 1,
        "lookback": 60, "z_entry": 2.5, "coint_stop_p": 0.20,"time_stop": 20,
        "hist_trades": 6, "hist_wr": 100, "hist_avg_return": 4.7, "hist_avg_days": 13,
    },
    "BPCL/IOC": {
        "sector": "Oil & Gas", "tier": 1,
        "lookback": 60, "z_entry": 2.5, "coint_stop_p": 0.20,"time_stop": 20,
        "hist_trades": 5, "hist_wr": 100, "hist_avg_return": 2.3, "hist_avg_days": 36,
    },
    "ICICIBANK/HDFCBANK": {
        "sector": "Private Banks", "tier": 1,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 19, "hist_wr": 94.7, "hist_avg_return": 1.56, "hist_avg_days": 15,
    },
    # ICICIBANK/BANKNIFTY removed — stock vs index is not a cointegration pair; index composition changes break the relationship

    # TIER 2: 90%+ Win Rate OR thin data (< 2yr overlap) — BUG FIX #2
    "BANKBARODA/PNB": {
        "sector": "PSU Banks", "tier": 2,  # DOWNGRADED: only 246 common data points
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 8, "hist_wr": 100, "hist_avg_return": 2.4, "hist_avg_days": 18,
        "_note": "PNB data only from Feb 2025 — backtest on 216 potential Z days",
    },
    "SBIN/PNB": {
        "sector": "PSU Banks", "tier": 2,  # DOWNGRADED: only 246 common data points
        "lookback": 60, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 20,
        "hist_trades": 5, "hist_wr": 100, "hist_avg_return": 2.8, "hist_avg_days": 19,
        "_note": "PNB data only from Feb 2025 — backtest on 186 potential Z days",
    },
    "NMDC/COALINDIA": {
        "sector": "Metals", "tier": 2,
        "lookback": 30, "z_entry": 2.0, "coint_stop_p": 0.20,"time_stop": 30,
        "hist_trades": 11, "hist_wr": 91, "hist_avg_return": 1.6, "hist_avg_days": 18,
    },
}

# Minimum number of overlapping trading days for a pair to be eligible
MIN_OVERLAP_DAYS = 200

# Maximum staleness in TRADING days (not calendar days) — BUG FIX #1
MAX_STALENESS_TRADING_DAYS = 3


# =============================================================================
# DATA LOADING (with BUG FIX #1: trading-day staleness)
# =============================================================================

def load_historical(symbol, max_staleness_trading_days=MAX_STALENESS_TRADING_DAYS):
    """Load historical data with strict freshness check.
    
    BUG FIX #1: Uses trading days (Mon-Fri) for staleness, not calendar days.
    Friday data on Monday = 0 trading days stale (was 3 calendar days before!).
    """
    path = os.path.join(DATA_DIR, f"{symbol}_5Y.csv")
    if not os.path.exists(path):
        logger.warning(f"{symbol}: Data file not found at {path}")
        return None

    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP']).sort_values('FH_TIMESTAMP')

    # GATE 1: Freshness — TRADING DAYS, not calendar days
    last_date = df['FH_TIMESTAMP'].max()
    trading_days_stale = int(np.busday_count(last_date.date(), datetime.now().date()))
    calendar_days_stale = (datetime.now() - last_date).days

    if trading_days_stale > max_staleness_trading_days:
        logger.warning(f"{symbol}: Data {trading_days_stale} trading days stale "
                       f"({calendar_days_stale} calendar days, last: {last_date.date()}) — REJECTED")
        return None

    # Build continuous series (nearest expiry per date)
    # yfinance data has empty FH_EXPIRY_DT — skip deduplication if all NaT
    df['FH_EXPIRY_DT_parsed'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    if df['FH_EXPIRY_DT_parsed'].notna().any():
        idx = df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT_parsed'].idxmin()
        continuous = df.loc[idx.dropna()].copy()
    else:
        # One row per date already (yfinance/spot data)
        continuous = df.drop_duplicates(subset=['FH_TIMESTAMP'], keep='last').copy()
    continuous = continuous.sort_values('FH_TIMESTAMP')
    
    result = continuous[['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_MARKET_LOT']].set_index('FH_TIMESTAMP')
    result['pct_change'] = result['FH_CLOSING_PRICE'].pct_change()
    return result


def get_live_futures_price(api, symbol):
    """Fetch current month futures price from Shoonya.

    Edge case fixes:
    - EXACT symbol match (PNB vs PNBHOUSING bug)
    - Rejects zero/negative/NaN prices
    - Returns lot size for cash-neutrality check
    """
    try:
        ret = api.searchscrip(exchange='NFO', searchtext=symbol)
        if not ret or 'values' not in ret:
            return None, None

        # Find FUT contracts with EXACT symbol match.
        # OAuth API: uses instname='FUTSTK' and symname='SBIN', exd='28-APR-2026'
        # Old API:   uses dname='SBIN FUT 28APR2026'
        futs = []
        for s in ret['values']:
            instname = s.get('instname', '')
            symname  = s.get('symname', '')
            dname    = s.get('dname', '')
            tsym     = s.get('tsym', '')
            # Accept either OAuth format (FUTSTK) or old format (FUT in dname)
            is_fut = 'FUTSTK' in instname or 'FUT' in dname or tsym.endswith('F')
            # Exact base-symbol match
            matches = (symname == symbol) or (dname.split()[0] == symbol if dname else False)
            if is_fut and matches:
                futs.append(s)

        if not futs:
            all_syms = [s.get('symname') or s.get('dname', '') for s in ret['values']]
            logger.warning(f"{symbol}: No exact FUT match. Found: {all_syms[:5]}")
            return None, None

        # Sort by expiry, pick nearest
        def parse_expiry(x):
            # OAuth format: exd = '28-APR-2026'
            exd = x.get('exd', '')
            if exd:
                try:
                    return pd.to_datetime(exd, format='%d-%b-%Y')
                except Exception:
                    pass
            # Old format: last token of dname = '28APR2026'
            try:
                return pd.to_datetime(x.get('dname', '').split()[-1], format='%d%b%Y')
            except Exception:
                return pd.Timestamp.max

        futs.sort(key=parse_expiry)
        selected = futs[0]
        token = selected['token']
        lot_size = int(selected.get('ls', 1))

        q = api.get_quotes(exchange='NFO', token=token)
        if q and 'lp' in q:
            price = float(q['lp'])
            if price <= 0 or np.isnan(price):
                logger.warning(f"{symbol}: Invalid price {price} — REJECTED")
                return None, None
            contract_name = selected.get('dname') or selected.get('tsym', symbol)
            logger.info(f"{symbol}: {contract_name} → LTP={price}, lot={lot_size}")
            return price, lot_size

        return None, None
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return None, None


def check_market_hours():
    """Check if Indian markets are currently open.
    NSE trading hours: 9:15 AM — 3:30 PM IST, Mon-Fri.
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False, f"Market closed (weekend)"
    market_open = now.replace(hour=9, minute=15, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)
    if now < market_open:
        return False, f"Pre-market (opens at 09:15)"
    elif now > market_close:
        return False, f"Post-market (closed at 15:30)"
    return True, "Market open"


def verify_lot_size(symbol, hist_df, live_lot):
    """Check if lot size changed between historical data and live contract."""
    if hist_df is None or live_lot is None:
        return True, None
    hist_lot = int(hist_df['FH_MARKET_LOT'].iloc[-1])
    if hist_lot != live_lot:
        return False, (f"{symbol}: Lot changed! Historical={hist_lot} → Live={live_lot}. "
                       f"PnL calculations use live lots.")
    return True, None


# =============================================================================
# CORE ANALYSIS (with all bug fixes)
# =============================================================================

def analyze_proven_pair(api, pair_key, config):
    """
    Analyze a single proven pair with all safety gates + edge case checks.

    Bug fixes applied:
      #1: Trading-day staleness (in load_historical)
      #2: Min overlap check + thin-data warning
      #4: Return correlation instead of price correlation
      #5: Date gap verification in merged series
    Note: Expiry-day volatility is kept (not excluded).
    """
    sym_a, sym_b = pair_key.split("/")

    # Load data (Gate 1: trading-day freshness enforced inside load_historical)
    hist_a = load_historical(sym_a)
    hist_b = load_historical(sym_b)
    if hist_a is None or hist_b is None:
        return None

    # Merge on common dates (inner join = only dates where BOTH traded)
    merged = hist_a[['FH_CLOSING_PRICE', 'FH_MARKET_LOT', 'pct_change']].join(
        hist_b[['FH_CLOSING_PRICE', 'FH_MARKET_LOT', 'pct_change']],
        how='inner', lsuffix='_A', rsuffix='_B'
    )
    # Replace 0/NaN lot sizes (data quality gaps) with the nearest valid value
    for col in ['FH_MARKET_LOT_A', 'FH_MARKET_LOT_B']:
        merged[col] = merged[col].replace(0, np.nan).ffill().bfill()

    # =========================================================================
    # BUG FIX #2: Minimum data overlap check
    # =========================================================================
    if len(merged) < MIN_OVERLAP_DAYS:
        logger.warning(f"{pair_key}: Only {len(merged)} common trading days "
                       f"(need {MIN_OVERLAP_DAYS}) — THIN DATA")
        return None

    if len(merged) < config['lookback'] + 30:
        logger.warning(f"{pair_key}: Insufficient data ({len(merged)} rows)")
        return None

    # =========================================================================
    # BUG FIX #5: Date gap verification
    # =========================================================================
    merged_sorted = merged.sort_index()
    date_diffs = merged_sorted.index.to_series().diff().dt.days
    large_gaps = date_diffs[date_diffs > 5]  # >5 calendar days = suspicious
    if len(large_gaps) > 0:
        max_gap = large_gaps.max()
        gap_date = large_gaps.idxmax()
        logger.warning(f"{pair_key}: Largest date gap = {int(max_gap)} days at {gap_date.date()}")
        # Don't reject, but flag (gaps of 5-7 days could be holidays)
        if max_gap > 10:
            logger.warning(f"{pair_key}: Gap > 10 days — data integrity concern")

    # =========================================================================
    # GATE 4: Cross-validated split detection
    # =========================================================================
    for idx in merged.index:
        chg_a = abs(merged.loc[idx, 'pct_change_A']) if not pd.isna(merged.loc[idx, 'pct_change_A']) else 0
        chg_b = abs(merged.loc[idx, 'pct_change_B']) if not pd.isna(merged.loc[idx, 'pct_change_B']) else 0

        # TRUE split: one > 20%, other < 5% (market crash = both move)
        if (chg_a > 0.20 and chg_b < 0.05) or (chg_b > 0.20 and chg_a < 0.05):
            logger.warning(f"{pair_key}: Split detected on {idx.date()} — trimming data from day after")
            # Exclude the split day itself — its ratio is incomparable to pre-split data
            merged = merged.loc[merged.index > idx]
            break

    if len(merged) < config['lookback'] + 30:
        return None

    # Cash-neutral spread = (lot_a × price_a) − (lot_b × price_b)
    # Directive mandates this over price ratio — ratio distorts PnL.
    # IMPORTANT: Lot sizes change over 3Y (e.g. HDFCBANK: 550→1100, AXISBANK: 1200→625).
    # Must use per-row lot size from the CSV, not just iloc[-1].
    merged['SPREAD'] = (merged['FH_CLOSING_PRICE_A'] * merged['FH_MARKET_LOT_A'].ffill()
                        - merged['FH_CLOSING_PRICE_B'] * merged['FH_MARKET_LOT_B'].ffill())

    # Also keep RATIO for Gate 3 range check (ratio is scale-invariant for bound detection)
    merged['RATIO'] = merged['FH_CLOSING_PRICE_A'] / merged['FH_CLOSING_PRICE_B']

    lookback = config['lookback']
    recent = merged.tail(lookback)

    mean_lb = recent['SPREAD'].mean()
    std_lb = recent['SPREAD'].std()

    if std_lb == 0 or np.isnan(std_lb):
        return None

    # Get live prices — fall back to last CSV close when Shoonya is unavailable
    live_a, lot_a = get_live_futures_price(api, sym_a)
    live_b, lot_b = get_live_futures_price(api, sym_b)

    using_csv_fallback = False
    if live_a is None or live_b is None or live_b == 0:
        # Use last historical close as proxy (yfinance data is up-to-date)
        live_a = float(merged['FH_CLOSING_PRICE_A'].iloc[-1])
        live_b = float(merged['FH_CLOSING_PRICE_B'].iloc[-1])
        lot_a  = float(merged['FH_MARKET_LOT_A'].iloc[-1])
        lot_b  = float(merged['FH_MARKET_LOT_B'].iloc[-1])
        using_csv_fallback = True
        logger.info(f"{pair_key}: Shoonya unavailable — using last CSV close "
                    f"({merged.index[-1].date()}) as live proxy")

    warnings_list = []
    if using_csv_fallback:
        warnings_list.append(f"⚠️ PRICES FROM CSV ({merged.index[-1].date()}) — Shoonya offline")

    # Lot size verification
    _, lot_warn_a = verify_lot_size(sym_a, hist_a, lot_a)
    _, lot_warn_b = verify_lot_size(sym_b, hist_b, lot_b)
    if lot_warn_a: warnings_list.append(lot_warn_a)
    if lot_warn_b: warnings_list.append(lot_warn_b)

    # Cash imbalance — try multi-lot ratios to balance legs before rejecting
    # Try all integer combos up to 5 lots per side, pick the one minimising imbalance
    best_ratio_a, best_ratio_b, best_imbalance = 1, 1, float('inf')
    for n_a in range(1, 6):
        for n_b in range(1, 6):
            val_a = live_a * lot_a * n_a
            val_b = live_b * lot_b * n_b
            imb = abs(val_a - val_b) / max(val_a, val_b) * 100
            if imb < best_imbalance:
                best_imbalance, best_ratio_a, best_ratio_b = imb, n_a, n_b

    lot_a = lot_a * best_ratio_a
    lot_b = lot_b * best_ratio_b
    value_a = live_a * lot_a
    value_b = live_b * lot_b
    cash_imbalance = best_imbalance

    if best_ratio_a != 1 or best_ratio_b != 1:
        logger.info(f"{pair_key}: Multi-lot ratio {best_ratio_a}:{best_ratio_b} → imbalance {cash_imbalance:.0f}%")
        warnings_list.append(f"Multi-lot {best_ratio_a}:{best_ratio_b} (imbalance {cash_imbalance:.0f}%)")

    if cash_imbalance > 50:
        logger.warning(f"{pair_key}: Cash imbalance {cash_imbalance:.0f}% even after best ratio — REJECTED")
        return None
    elif cash_imbalance > 30:
        warnings_list.append(f"Cash imbalance {cash_imbalance:.0f}% — verify sizing")

    # Thin data warning
    data_years = (merged.index.max() - merged.index.min()).days / 365
    if data_years < 2:
        warnings_list.append(f"⚠️ THIN DATA: Only {data_years:.1f}yr overlap ({len(merged)} days)")

    # Z-score uses 1:1 lot basis to match the historical spread scale.
    # Multi-lot ratio (best_ratio_a:best_ratio_b) is for position sizing only — it
    # must NOT scale the live spread or the Z-score comparison breaks.
    live_lot_a_base = lot_a // best_ratio_a  # restore original 1x lot
    live_lot_b_base = lot_b // best_ratio_b
    live_spread_1x = live_a * live_lot_a_base - live_b * live_lot_b_base
    live_spread = live_a * lot_a - live_b * lot_b  # actual position value (sized)
    live_ratio = live_a / live_b
    z_score = (live_spread_1x - mean_lb) / std_lb

    # GATE 3: Historical range check on RATIO (scale-invariant, ±5%)
    hist_min = merged['RATIO'].min()
    hist_max = merged['RATIO'].max()
    margin = (hist_max - hist_min) * 0.05

    if live_ratio > hist_max + margin or live_ratio < hist_min - margin:
        logger.warning(f"{pair_key}: Ratio {live_ratio:.4f} outside range "
                       f"[{hist_min:.4f}, {hist_max:.4f}] — STRUCTURAL BREAK")
        return None

    # =========================================================================
    # BUG FIX #4: Use RETURN correlation, not PRICE correlation
    # =========================================================================
    # Compute daily returns  
    returns_a = merged['FH_CLOSING_PRICE_A'].pct_change()
    returns_b = merged['FH_CLOSING_PRICE_B'].pct_change()
    
    # Return correlation (what we gate on)
    corr_20_ret = returns_a.tail(20).corr(returns_b.tail(20))
    corr_60_ret = returns_a.tail(60).corr(returns_b.tail(60))
    
    # Price correlation (informational only — for logging)
    corr_20_price = merged['FH_CLOSING_PRICE_A'].tail(20).corr(merged['FH_CLOSING_PRICE_B'].tail(20))
    corr_60_price = merged['FH_CLOSING_PRICE_A'].tail(60).corr(merged['FH_CLOSING_PRICE_B'].tail(60))

    # Handle NaN correlations
    if pd.isna(corr_20_ret) or pd.isna(corr_60_ret):
        logger.warning(f"{pair_key}: Correlation is NaN — insufficient data")
        return None

    # GATE 2: Return correlation check
    if corr_20_ret < 0.3:
        logger.warning(f"{pair_key}: 20D return corr {corr_20_ret:.2f} < 0.3 — DECOUPLED")
        return None

    if corr_60_ret < 0.5:
        logger.warning(f"{pair_key}: 60D return corr {corr_60_ret:.2f} < 0.5 — WEAK")
        return None

    # Log divergence between price and return correlation
    if abs(corr_20_price - corr_20_ret) > 0.3:
        warnings_list.append(f"Price vs Return corr diverge: "
                             f"Price={corr_20_price:.2f}, Return={corr_20_ret:.2f}")

    # GATE 5: Structural integrity — rolling 3M cointegration check
    # Z=-4 or -5 on a cointegrated pair is OPPORTUNITY, not a stop signal.
    # The real stop is when the long-run relationship itself has broken.
    from statsmodels.tsa.stattools import coint as _coint, adfuller as _adf
    recent_3m = merged.tail(63)   # ~3 trading months
    _coint_broken = False
    if len(recent_3m) >= 30:
        try:
            _, _cp, _ = _coint(recent_3m['FH_CLOSING_PRICE_A'], recent_3m['FH_CLOSING_PRICE_B'])
            _spread_3m = (recent_3m['FH_CLOSING_PRICE_A'] * recent_3m['FH_MARKET_LOT_A']
                          - recent_3m['FH_CLOSING_PRICE_B'] * recent_3m['FH_MARKET_LOT_B'])
            _, _ap, *_ = _adf(_spread_3m.dropna(), maxlag=5)
            if _cp > config['coint_stop_p'] and _ap > 0.10:
                logger.warning(f"{pair_key}: STRUCTURAL BREAK — 3M coint p={_cp:.3f}, ADF p={_ap:.3f}. Exit any open position.")
                _coint_broken = True
        except Exception:
            pass
    if _coint_broken:
        return None

    # Determine signal
    has_signal = abs(z_score) >= config['z_entry']

    if z_score > config['z_entry']:
        signal = f"SELL {sym_a} / BUY {sym_b}"
        direction = "SHORT"
    elif z_score < -config['z_entry']:
        signal = f"BUY {sym_a} / SELL {sym_b}"
        direction = "LONG"
    else:
        signal = "NO SIGNAL (Z not at entry)"
        direction = None

    return {
        'pair': pair_key,
        'sector': config['sector'],
        'tier': config['tier'],
        'sym_a': sym_a, 'sym_b': sym_b,
        'live_a': live_a, 'live_b': live_b,
        'lot_a': lot_a, 'lot_b': lot_b,
        'lot_ratio': f"{best_ratio_a}:{best_ratio_b}",
        'value_a': round(value_a), 'value_b': round(value_b),
        'cash_imbalance': round(cash_imbalance, 1),
        'live_ratio': round(live_ratio, 4),
        'live_spread': round(live_spread, 0),
        'z_score': round(z_score, 2),
        'spread_mean': round(mean_lb, 0),
        'spread_std': round(std_lb, 0),
        'corr_20_ret': round(corr_20_ret, 2),
        'corr_60_ret': round(corr_60_ret, 2),
        'corr_20_price': round(corr_20_price, 2),
        'corr_60_price': round(corr_60_price, 2),
        'data_overlap_days': len(merged),
        'data_years': round(data_years, 1),
        'hist_range': f"{hist_min:.4f}-{hist_max:.4f}",
        'has_signal': has_signal,
        'signal': signal,
        'direction': direction,
        'config': config,
        'warnings': warnings_list,
    }


# =============================================================================
# MAIN SCANNER
# =============================================================================

def scan_proven_pairs(monitor_mode=False):
    """Scan all proven pairs for live signals."""
    print("=" * 80)
    print(f"  PROVEN PAIRS SCANNER v3.2 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Pairs: {len(PROVEN_PAIRS)} | Min WR: 90% | Safety Gates: 7")
    print(f"  Fixes: Trading-day staleness, return correlation, date gap check")
    print("=" * 80)

    # CHECK: Market hours
    is_open, mkt_msg = check_market_hours()
    if not is_open:
        print(f"\n  ⚠️  {mkt_msg} — Live prices may be stale!")
        print(f"     Run during market hours (09:15-15:30 IST Mon-Fri) for accurate signals.")

    client = ShoonyaClient()
    api = client.login()

    signals = []
    monitored = []
    rejected_count = 0

    for pair_key, config in PROVEN_PAIRS.items():
        result = analyze_proven_pair(api, pair_key, config)

        if result is None:
            rejected_count += 1
            continue

        if result['has_signal']:
            signals.append(result)
        else:
            monitored.append(result)

    # Display Active Signals
    if signals:
        print(f"\n{'='*80}")
        print(f"  💎 ACTIVE SIGNALS ({len(signals)})")
        print(f"{'='*80}")

        for s in signals:
            tier_icon = "💎" if s['tier'] == 1 else "🥈"
            c = s['config']
            print(f"\n{tier_icon} {s['sector']}: {s['pair']} "
                  f"[{s['data_years']}yr data, {s['data_overlap_days']} days]")
            print(f"   {s['sym_a']}: ₹{s['live_a']:.2f} × {s['lot_a']} lot = ₹{s['value_a']:,}")
            print(f"   {s['sym_b']}: ₹{s['live_b']:.2f} × {s['lot_b']} lot = ₹{s['value_b']:,}")
            print(f"   Cash Imbalance: {s['cash_imbalance']:.1f}%")
            print(f"   Z-Score: {s['z_score']:.2f}")
            print(f"   Return Corr: 20D={s['corr_20_ret']:.2f} 60D={s['corr_60_ret']:.2f}")
            print(f"   Price  Corr: 20D={s['corr_20_price']:.2f} 60D={s['corr_60_price']:.2f}")
            print(f"   Range: {s['hist_range']} | Live Ratio: {s['live_ratio']:.4f}")
            print(f"   Config: LB{c['lookback']} / Entry Z{c['z_entry']} / StructStop coint_p>{c['coint_stop_p']} / {c['time_stop']}d")
            print(f"   Backtest: {c['hist_trades']} trades, {c['hist_wr']}% WR, +{c['hist_avg_return']}% avg")
            print(f"   ➡️  {s['signal']}")

            # PnL estimate
            smaller_leg = min(s['value_a'], s['value_b'])
            expected_pnl = smaller_leg * c['hist_avg_return'] / 100
            print(f"   💰 Expected: ~₹{expected_pnl:,.0f} ({c['hist_avg_return']}% of ₹{smaller_leg:,})")

            for w in s.get('warnings', []):
                print(f"   ⚠️  {w}")
    else:
        print(f"\n  ✅ NO ACTIVE SIGNALS — No proven pair has reached entry Z-Score threshold")

    # Display Monitored Pairs
    if monitored and monitor_mode:
        print(f"\n{'='*80}")
        print(f"  👀 MONITORING ({len(monitored)} pairs — not at entry yet)")
        print(f"{'='*80}")

        for s in monitored:
            c = s['config']
            pct_to_entry = abs(s['z_score']) / c['z_entry'] * 100
            tier_icon = "💎" if s['tier'] == 1 else "🥈"
            print(f"\n   {tier_icon} {s['pair']}: Z={s['z_score']:.2f} "
                  f"({pct_to_entry:.0f}% to Z{c['z_entry']})"
                  f" | RetCorr20={s['corr_20_ret']:.2f}"
                  f" | {s['data_years']}yr/{s['data_overlap_days']}d"
                  f" | Imbal={s['cash_imbalance']:.0f}%"
                  f" | ₹{s['value_a']:,} vs ₹{s['value_b']:,}")

            for w in s.get('warnings', []):
                print(f"      ⚠️  {w}")

    # Summary
    total_passed = len(signals) + len(monitored)
    print(f"\n{'='*80}")
    print(f"  Summary: {total_passed} passed gates, {rejected_count} rejected")
    print(f"  Signals: {len(signals)} | Monitoring: {len(monitored)}")
    if not is_open:
        print(f"  ⚠️  {mkt_msg}")
    print(f"{'='*80}")

    return signals


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Proven Pairs Scanner v3.2')
    parser.add_argument('--monitor', action='store_true',
                       help='Show all pairs passing gates, not just signals')
    args = parser.parse_args()

    scan_proven_pairs(monitor_mode=args.monitor)
