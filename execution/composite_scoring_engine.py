"""
Composite Scoring Engine — The Heart of Medallion Lite

Aggregates all 7 modules into one unified score per stock per day.
This is the single model Simons ran at RenTech — everything feeds into one system,
so improvement in one area automatically lifts the whole.

Score ranges:
  Module 1 (Volume Anomaly):    -25 to +30
  Module 2 (Insider Cluster):   -40 to +40
  Module 3 (Bulk/Block Deals):  -35 to +30
  Module 4 (Pairs Trading):     0 to +25  [uses existing scan_cointegrated_pairs.py]
  Module 5 (FII/DII Flows):    -25 to +15  [market-wide, applied to all signals]
  Module 6 (Seasonality):      -10 to +10  [market-wide, applied to all signals]
  Module 7 (AI Sentiment):     -20 to +10  [per-stock, confirmatory only]

Trading rules:
  COMPOSITE >= 60  → STRONG BUY  (2x position)
  COMPOSITE 40-59  → BUY         (1x position)
  COMPOSITE 20-39  → WATCHLIST
  COMPOSITE -20 to 19 → NO SIGNAL
  COMPOSITE -21 to -39 → SHORT signal
  COMPOSITE <= -40 → STRONG SHORT

Usage:
    python3 execution/composite_scoring_engine.py
    python3 execution/composite_scoring_engine.py --date 2026-04-04 --top 20
    python3 execution/composite_scoring_engine.py --capital 1000000
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(__file__))
from signals_db import SignalsDB
from kelly_sizer import kelly_fraction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─── Score ranges per module ──────────────────────────────────────────────────
# These are the MAXIMUM possible contributions from each module.
# Positive = bullish. Negative = bearish.
MODULE_WEIGHTS = {
    'volume':      {'min': -25, 'max': 30,  'name': 'Volume Anomaly'},
    'insider':     {'min': -40, 'max': 40,  'name': 'Insider Cluster'},
    'bulk':        {'min': -35, 'max': 30,  'name': 'Bulk/Block Deals'},
    'pairs':       {'min':   0, 'max': 25,  'name': 'Pairs Trading'},
    'fii':         {'min': -25, 'max': 15,  'name': 'FII/DII Flows'},
    'seasonality': {'min': -10, 'max': 10,  'name': 'Seasonality'},
    'sentiment':   {'min': -20, 'max': 10,  'name': 'AI Sentiment'},
}

# Trading signal thresholds
STRONG_BUY_THRESHOLD = 60
BUY_THRESHOLD = 40
WATCHLIST_THRESHOLD = 20
SHORT_THRESHOLD = -20
STRONG_SHORT_THRESHOLD = -40


def get_signal_type(score: int) -> str:
    if score >= STRONG_BUY_THRESHOLD:
        return 'STRONG_BUY'
    elif score >= BUY_THRESHOLD:
        return 'BUY'
    elif score >= WATCHLIST_THRESHOLD:
        return 'WATCHLIST'
    elif score >= SHORT_THRESHOLD:
        return 'NO_SIGNAL'
    elif score >= STRONG_SHORT_THRESHOLD:
        return 'SHORT'
    else:
        return 'STRONG_SHORT'


def get_position_multiplier(signal_type: str) -> float:
    """Position sizing relative to base Kelly position."""
    return {
        'STRONG_BUY': 2.0,
        'BUY': 1.0,
        'WATCHLIST': 0.0,
        'NO_SIGNAL': 0.0,
        'SHORT': -1.0,
        'STRONG_SHORT': -2.0,
    }.get(signal_type, 0.0)


def run_composite_scan(date_str: str = None, capital: float = 1_000_000,
                        top_n: int = 20, run_fresh: bool = False):
    """
    Main composite engine. Pulls scores from all modules and aggregates.

    Priority:
    1. If run_fresh=True, run all scanners first (slow, ~15 min)
    2. Otherwise, pull from DB (fast — assumes modules already ran today)

    Args:
        date_str:   Date to score 'YYYY-MM-DD' (default: today)
        capital:    Trading capital in ₹ for Kelly sizing
        top_n:      Return top N stocks by score
        run_fresh:  Re-run all scanners before aggregating

    Returns:
        List of composite score dicts, sorted by score descending
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')

    db = SignalsDB()

    if run_fresh:
        logger.info("Running all scanners fresh (this takes ~10-15 minutes)...")
        _run_all_scanners(date_str, capital)

    # ── Collect signals from DB ───────────────────────────────────────────────

    # Market-wide adjustments (FII + Seasonality) — same for all stocks
    fii_regime = db.get_fii_regime(date_str)
    fii_market_score = fii_regime['score'] if fii_regime else 0
    fii_signal_type = fii_regime['signal_type'] if fii_regime else 'NO_DATA'

    seasonality_score, active_patterns = db.get_active_seasonality_score(date_str)

    # Per-stock signals
    volume_signals = {s['symbol']: s for s in db.get_volume_signals(date_str)}
    insider_signals = {s['symbol']: s for s in db.get_insider_signals(date_str)}
    bulk_signals = {}
    for s in db.get_bulk_signals(date_str):
        sym = s['symbol']
        if sym not in bulk_signals or abs(s['score']) > abs(bulk_signals[sym]['score']):
            bulk_signals[sym] = s

    # Pairs signals from existing antigravity_v3_scanner (pull from DB if stored, else 0)
    # For now, pairs contribution is handled by the existing scanner — composite adds 0
    # TODO: integrate pair scan output into DB when scan_cointegrated_pairs saves results

    # ── Build composite scores ────────────────────────────────────────────────
    # Get all symbols with at least one signal today
    all_symbols = set(volume_signals) | set(insider_signals) | set(bulk_signals)

    if not all_symbols:
        logger.warning(f"No signals found for {date_str}. Run the individual module scanners first.")
        return []

    results = []
    for symbol in all_symbols:
        vol_score = volume_signals.get(symbol, {}).get('score', 0) or 0
        ins_score = insider_signals.get(symbol, {}).get('score', 0) or 0
        bulk_score_val = bulk_signals.get(symbol, {}).get('score', 0) or 0
        pairs_score = 0   # From existing pair scanner (not yet integrated)

        # AI sentiment — use cached result if available
        sentiment_score = 0
        try:
            from ai_sentiment_analyzer import get_cached_sentiment
            cached = get_cached_sentiment(symbol)
            if cached:
                sentiment_score = cached.get('composite_contribution', 0) or 0
        except Exception:
            pass

        # Market-wide adjustments
        fii_score = fii_market_score
        seas_score = seasonality_score

        composite = (vol_score + ins_score + bulk_score_val + pairs_score +
                     fii_score + seas_score + sentiment_score)

        signal_type = get_signal_type(composite)
        position_multiplier = get_position_multiplier(signal_type)

        # Track active signals for the report
        active_signals = []
        if vol_score != 0:
            vs = volume_signals.get(symbol, {})
            active_signals.append(f"Vol:{vs.get('signal_type','?')}({vol_score:+d})")
        if ins_score != 0:
            is_ = insider_signals.get(symbol, {})
            active_signals.append(f"Insider:{is_.get('signal_type','?')}({ins_score:+d})")
        if bulk_score_val != 0:
            bs = bulk_signals.get(symbol, {})
            active_signals.append(f"Bulk:{bs.get('signal_type','?')}({bulk_score_val:+d})")
        if fii_score != 0:
            active_signals.append(f"FII:{fii_signal_type}({fii_score:+d})")
        if seas_score != 0:
            active_signals.append(f"Seasonal({seas_score:+d})")
        if sentiment_score != 0:
            active_signals.append(f"Sentiment({sentiment_score:+d})")

        # Kelly-based position sizing (using conservative stats for single stock)
        kelly_size = 0.0
        if signal_type in ('STRONG_BUY', 'BUY'):
            # Use conservative estimates until per-module backtests are available
            base_wr = 0.55 + (composite - BUY_THRESHOLD) * 0.002  # Scales with score
            base_wr = min(0.75, max(0.50, base_wr))
            kf, kh, _ = kelly_fraction(base_wr, 0.035, 0.030)
            kelly_size = kh * position_multiplier * 100  # % of capital

        result = {
            'date': date_str,
            'symbol': symbol,
            'composite_score': composite,
            'signal_type': signal_type,
            'position_multiplier': position_multiplier,
            'kelly_pct': round(kelly_size, 1),
            'module_breakdown': {
                'volume': vol_score,
                'insider': ins_score,
                'bulk': bulk_score_val,
                'pairs': pairs_score,
                'fii': fii_score,
                'seasonality': seas_score,
                'sentiment': sentiment_score,
            },
            'active_signals': active_signals,
            'fii_regime': fii_regime.get('regime', 'UNKNOWN') if fii_regime else 'UNKNOWN',
            'active_patterns': active_patterns,
        }
        results.append(result)

        # Save to DB
        db.upsert_composite_score(
            date=date_str, symbol=symbol, composite_score=composite,
            vol_score=vol_score, insider_score=ins_score, bulk_score=bulk_score_val,
            pairs_score=pairs_score, fii_score=fii_score, seasonality_score=seas_score,
            sentiment_score=sentiment_score, signal_type=signal_type,
            active_signals=active_signals
        )

    results.sort(key=lambda x: x['composite_score'], reverse=True)

    # Return top N positive + any strong negatives
    positives = [r for r in results if r['composite_score'] > 0][:top_n]
    negatives = [r for r in results if r['composite_score'] < SHORT_THRESHOLD][:5]

    logger.info(f"Composite scan: {len(positives)} bullish, {len(negatives)} short signals "
                f"out of {len(all_symbols)} stocks with any signal")
    return positives + negatives


def _run_all_scanners(date_str: str, capital: float):
    """Run all individual module scanners and store results to DB."""
    from_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

    try:
        logger.info("Running Module 1: Volume Anomaly Scanner...")
        from scan_volume_anomalies import run_volume_scan
        run_volume_scan(date_str=date_str, save_to_db=True)
    except Exception as e:
        logger.warning(f"Module 1 failed: {e}")

    try:
        logger.info("Running Module 2: Insider Cluster Detector...")
        from scan_insider_clusters import run_insider_scan
        run_insider_scan(from_date, date_str, save_to_db=True)
    except Exception as e:
        logger.warning(f"Module 2 failed: {e}")

    try:
        logger.info("Running Module 3: Bulk/Block Deal Tracker...")
        from scan_bulk_block_deals import run_bulk_deal_scan
        run_bulk_deal_scan(from_date, date_str, save_to_db=True)
    except Exception as e:
        logger.warning(f"Module 3 failed: {e}")

    try:
        logger.info("Running Module 5: FII/DII Flow Score...")
        from scan_fii_dii_flows import run_fii_scan
        run_fii_scan(save_to_db=True)
    except Exception as e:
        logger.warning(f"Module 5 failed: {e}")

    try:
        logger.info("Running Module 6: Seasonality Score...")
        from analyze_seasonality import get_current_seasonality_score
        get_current_seasonality_score(date_str)
    except Exception as e:
        logger.warning(f"Module 6 failed: {e}")


def print_composite_report(results: list[dict], capital: float = 1_000_000,
                            fii_regime: str = 'UNKNOWN', active_patterns: list = None):
    """Print the unified composite score report."""
    if not results:
        print("  No composite signals generated. Run module scanners first.")
        print("  Use --fresh flag to run all scanners: python3 composite_scoring_engine.py --fresh")
        return

    strong_buys = [r for r in results if r['signal_type'] == 'STRONG_BUY']
    buys = [r for r in results if r['signal_type'] == 'BUY']
    watchlist = [r for r in results if r['signal_type'] == 'WATCHLIST']
    shorts = [r for r in results if r['signal_type'] in ('SHORT', 'STRONG_SHORT')]

    print(f"\n  FII REGIME: {fii_regime}  |  Active Patterns: {', '.join(active_patterns or []) or 'None'}")
    print(f"\n  SIGNAL SUMMARY:")
    print(f"    Strong Buy:  {len(strong_buys)}")
    print(f"    Buy:         {len(buys)}")
    print(f"    Watchlist:   {len(watchlist)}")
    print(f"    Short:       {len(shorts)}")

    if strong_buys or buys:
        print(f"\n  TOP BUY SIGNALS:")
        print(f"  {'Symbol':<12} {'Score':>6} {'Signal':<14} {'Kelly%':>7} {'Vol':>6} {'Ins':>5} {'Bulk':>6} {'FII':>5} {'Active Signals'}")
        print(f"  {'-'*110}")
        for r in (strong_buys + buys)[:15]:
            mb = r['module_breakdown']
            sigs = ' | '.join(r['active_signals'][:3])
            flag = '>>>' if r['signal_type'] == 'STRONG_BUY' else ' > '
            print(f"  {flag} {r['symbol']:<10} {r['composite_score']:>6} "
                  f"{r['signal_type']:<14} {r['kelly_pct']:>6.1f}% "
                  f"{mb['volume']:>+5} {mb['insider']:>+4} {mb['bulk']:>+5} {mb['fii']:>+4}  "
                  f"{sigs}")

    if watchlist:
        print(f"\n  WATCHLIST ({len(watchlist)} stocks):")
        for r in watchlist[:10]:
            print(f"    {r['symbol']:<12} Score: {r['composite_score']:>4}  Signals: {' | '.join(r['active_signals'][:2])}")

    if shorts:
        print(f"\n  SHORT SIGNALS:")
        for r in shorts:
            mb = r['module_breakdown']
            print(f"  [!] {r['symbol']:<12} {r['composite_score']:>6} {r['signal_type']:<14}  "
                  f"Vol:{mb['volume']:>+4} Ins:{mb['insider']:>+4} Bulk:{mb['bulk']:>+5}")

    # Capital allocation
    actionable = strong_buys + buys + shorts
    if actionable:
        total_kelly = sum(r['kelly_pct'] for r in actionable if r['kelly_pct'] > 0)
        total_kelly = min(total_kelly, 70)  # Safety cap
        print(f"\n  Estimated capital deployment: {total_kelly:.1f}% of ₹{capital:,.0f}")
        print(f"  (Capped at 70% per safety gate)")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Composite Scoring Engine — All 7 Modules')
    parser.add_argument('--date', type=str, default=None, help='Date YYYY-MM-DD (default: today)')
    parser.add_argument('--capital', type=float, default=1_000_000, help='Capital in ₹')
    parser.add_argument('--top', type=int, default=20, help='Top N stocks to show')
    parser.add_argument('--fresh', action='store_true',
                        help='Re-run all scanners before aggregating (slow ~15 min)')
    args = parser.parse_args()

    now = datetime.now()
    date_str = args.date or now.strftime('%Y-%m-%d')

    print("╔" + "═" * 78 + "╗")
    print("║  COMPOSITE SCORING ENGINE — MEDALLION LITE".ljust(79) + "║")
    print(f"║  All 7 modules → one unified score → ranked signals".ljust(79) + "║")
    print(f"║  Date: {date_str}  Capital: ₹{args.capital:,.0f}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    if args.fresh:
        print("\n  Running all scanners (this takes ~10-15 minutes)...")

    results = run_composite_scan(
        date_str=date_str,
        capital=args.capital,
        top_n=args.top,
        run_fresh=args.fresh
    )

    db = SignalsDB()
    fii = db.get_fii_regime(date_str)
    fii_regime = fii['regime'] if fii else 'UNKNOWN'
    _, patterns = db.get_active_seasonality_score(date_str)

    print_composite_report(results, args.capital, fii_regime, patterns)
