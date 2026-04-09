"""
Daily Report Generator — Antigravity Medallion Lite

Runs at 7:30 PM IST (after FII data is published) and produces:
  1. Top 10 stocks by composite score (with module breakdown)
  2. Active pair trade signals (Z-score > 2.0)
  3. Insider cluster alerts (last 7 days)
  4. Bulk deal accumulation alerts
  5. FII regime status + 10-day flow history
  6. Active seasonality factors
  7. Rolling 30/60/90-day backtesting hit rates

Output:
  - Terminal (always)
  - HTML report to .tmp/reports/YYYY-MM-DD.html (optional)
  - ntfy.sh push notification (optional, uses .env config)

Usage:
    python3 execution/generate_daily_report.py
    python3 execution/generate_daily_report.py --capital 2000000 --html
    python3 execution/generate_daily_report.py --date 2026-04-04 --notify
    python3 execution/generate_daily_report.py --fresh  # re-run all scanners first
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(__file__))
from signals_db import SignalsDB
from composite_scoring_engine import run_composite_scan, get_signal_type, SHORT_THRESHOLD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REPORT_DIR = Path(".tmp/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def get_pairs_signals():
    """
    Pull active pair trade signals from the existing scan_cointegrated_pairs system.
    Returns a list of pairs with Z-score > 2.0.
    """
    try:
        import subprocess
        import json as _json
        # Check if pair scan results exist
        pairs_path = Path(".tmp/pair_scan_results.csv")
        if not pairs_path.exists():
            return []
        import pandas as pd
        df = pd.read_csv(pairs_path)
        # Filter active signals
        if 'z_score' not in df.columns and 'Z_score' not in df.columns:
            return []
        z_col = 'z_score' if 'z_score' in df.columns else 'Z_score'
        df[z_col] = df[z_col].abs()
        active = df[df[z_col] >= 2.0].sort_values(z_col, ascending=False)
        results = []
        for _, row in active.head(10).iterrows():
            results.append({
                'pair': f"{row.get('symbol_a', row.get('sym_a', '?'))}/{row.get('symbol_b', row.get('sym_b', '?'))}",
                'z_score': float(row[z_col]),
                'signal': row.get('direction', row.get('signal', 'N/A')),
                'sector': row.get('sector', 'N/A'),
                'half_life': row.get('half_life', row.get('hl', 'N/A')),
            })
        return results
    except Exception as e:
        logger.warning(f"Could not load pair signals: {e}")
        return []


def send_ntfy_notification(title: str, message: str, priority: str = 'default'):
    """Send push notification via ntfy.sh."""
    import os, requests
    topic = os.environ.get('NTFY_TOPIC')
    server = os.environ.get('NTFY_SERVER', 'https://ntfy.sh')
    if not topic:
        return
    try:
        requests.post(
            f"{server}/{topic}",
            data=message.encode('utf-8'),
            headers={
                'Title': title,
                'Priority': priority,
                'Tags': 'chart_with_upwards_trend'
            },
            timeout=10
        )
        logger.info(f"ntfy notification sent: {title}")
    except Exception as e:
        logger.warning(f"ntfy notification failed: {e}")


def generate_html_report(date_str: str, composite_results: list, fii_result: dict,
                          insider_signals: list, bulk_signals: list,
                          pairs_signals: list, seasonality_score: int,
                          active_patterns: list, backtest_stats: list) -> str:
    """Generate HTML report and save to .tmp/reports/{date}.html"""

    signal_colors = {
        'STRONG_BUY': '#00c853',
        'BUY': '#69f0ae',
        'WATCHLIST': '#ffeb3b',
        'NO_SIGNAL': '#9e9e9e',
        'SHORT': '#ff7043',
        'STRONG_SHORT': '#d32f2f',
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Antigravity Medallion Lite — {date_str}</title>
<style>
  body {{ font-family: 'Courier New', monospace; background: #0d0d0d; color: #e0e0e0; margin: 20px; }}
  h1 {{ color: #00bcd4; border-bottom: 2px solid #00bcd4; }}
  h2 {{ color: #4fc3f7; margin-top: 30px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
  th {{ background: #1a1a2e; color: #00bcd4; padding: 8px; text-align: left; }}
  td {{ padding: 6px 8px; border-bottom: 1px solid #222; }}
  .strong-buy {{ color: #00c853; font-weight: bold; }}
  .buy {{ color: #69f0ae; }}
  .watchlist {{ color: #ffeb3b; }}
  .short {{ color: #ff7043; }}
  .strong-short {{ color: #d32f2f; font-weight: bold; }}
  .no-signal {{ color: #9e9e9e; }}
  .regime-bullish {{ color: #00c853; font-weight: bold; }}
  .regime-bearish {{ color: #d32f2f; font-weight: bold; }}
  .regime-neutral {{ color: #ffeb3b; }}
  .score-bar-container {{ background: #222; height: 8px; border-radius: 4px; width: 100px; display: inline-block; }}
  .score-bar {{ background: #00bcd4; height: 8px; border-radius: 4px; }}
  .meta {{ color: #666; font-size: 0.8em; }}
</style>
</head>
<body>
<h1>ANTIGRAVITY MEDALLION LITE — DAILY REPORT</h1>
<p class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M IST')} | Date: {date_str}</p>
"""

    # FII Regime
    regime = fii_result.get('regime', 'UNKNOWN')
    regime_class = f"regime-{regime.lower()}"
    html += f"""
<h2>1. MARKET REGIME</h2>
<table>
<tr><th>FII Regime</th><td class="{regime_class}">{regime}</td></tr>
<tr><th>Signal</th><td>{fii_result.get('signal_type', 'N/A')}</td></tr>
<tr><th>FII Cash Net</th><td>{fii_result.get('fii_cash_net', 0):+,.0f} Cr</td></tr>
<tr><th>FII Futures Net</th><td>{fii_result.get('fii_fut_net', 0):+,.0f} Cr</td></tr>
<tr><th>5-Day Rolling Score</th><td>{fii_result.get('rolling_5d_score', 0):+,.0f}</td></tr>
<tr><th>Active Seasonality</th><td>{', '.join(active_patterns) if active_patterns else 'None'} ({seasonality_score:+d} pts)</td></tr>
</table>
"""

    # Composite scores top 10
    top_10 = [r for r in composite_results if r['composite_score'] > 0][:10]
    html += "<h2>2. TOP 10 STOCKS BY COMPOSITE SCORE</h2>"
    if top_10:
        html += """<table>
<tr><th>#</th><th>Symbol</th><th>Score</th><th>Signal</th><th>Vol</th><th>Insider</th><th>Bulk</th><th>FII</th><th>Seasonal</th><th>Active Signals</th></tr>
"""
        for i, r in enumerate(top_10, 1):
            mb = r['module_breakdown']
            sig_class = r['signal_type'].lower().replace('_', '-')
            sigs = '<br>'.join(r.get('active_signals', [])[:4])
            html += f"""<tr>
<td>{i}</td>
<td><b>{r['symbol']}</b></td>
<td><span class="{sig_class}">{r['composite_score']}</span></td>
<td class="{sig_class}">{r['signal_type']}</td>
<td>{mb['volume']:+d}</td><td>{mb['insider']:+d}</td><td>{mb['bulk']:+d}</td>
<td>{mb['fii']:+d}</td><td>{mb['seasonality']:+d}</td>
<td style="font-size:0.8em">{sigs}</td>
</tr>"""
        html += "</table>"
    else:
        html += "<p>No composite signals above threshold today.</p>"

    # Pairs signals
    html += "<h2>3. ACTIVE PAIRS SIGNALS</h2>"
    if pairs_signals:
        html += "<table><tr><th>Pair</th><th>Z-Score</th><th>Signal</th><th>Sector</th><th>Half-Life</th></tr>"
        for p in pairs_signals:
            html += f"<tr><td>{p['pair']}</td><td>{p['z_score']:.2f}</td><td>{p['signal']}</td><td>{p['sector']}</td><td>{p['half_life']}d</td></tr>"
        html += "</table>"
    else:
        html += "<p>No active pairs signals (Z-score > 2.0).</p>"

    # Insider signals (last 7 days)
    html += "<h2>4. INSIDER CLUSTER ALERTS (Last 7 Days)</h2>"
    if insider_signals:
        html += "<table><tr><th>Symbol</th><th>Signal</th><th>Insiders</th><th>Value (L)</th><th>Score</th></tr>"
        for s in insider_signals[:10]:
            color = 'buy' if s['score'] > 0 else 'short'
            html += (f"<tr><td>{s['symbol']}</td><td class='{color}'>{s['signal_type']}</td>"
                     f"<td>{s['insider_count']}</td><td>{s['total_value_lakhs']:.0f}</td>"
                     f"<td>{s['score']:+d}</td></tr>")
        html += "</table>"
    else:
        html += "<p>No insider clusters detected in the last 7 days.</p>"

    # Bulk deal alerts
    html += "<h2>5. BULK/BLOCK DEAL ACCUMULATION ALERTS</h2>"
    if bulk_signals:
        html += "<table><tr><th>Symbol</th><th>Signal</th><th>Client</th><th>Deals</th><th>Value (Cr)</th><th>Score</th></tr>"
        for s in bulk_signals[:10]:
            color = 'buy' if s['score'] > 0 else 'short'
            html += (f"<tr><td>{s['symbol']}</td><td class='{color}'>{s['signal_type']}</td>"
                     f"<td>{s['client_name'][:25]}</td><td>{s['deal_count']}</td>"
                     f"<td>{s['total_value_cr']:.1f}</td><td>{s['score']:+d}</td></tr>")
        html += "</table>"
    else:
        html += "<p>No systematic bulk/block deal accumulation detected.</p>"

    # Backtest stats
    if backtest_stats:
        html += "<h2>6. BACKTESTING DASHBOARD</h2>"
        html += "<table><tr><th>Module</th><th>Period</th><th>Trades</th><th>Win Rate</th><th>Avg 5d Return</th></tr>"
        for stat in backtest_stats:
            html += (f"<tr><td>{stat['module']}</td><td>{stat['days_back']}d</td>"
                     f"<td>{stat['trades']}</td>"
                     f"<td>{stat['win_rate']:.1%}</td>"
                     f"<td>{stat['avg_return_5d']:.2%}</td></tr>")
        html += "</table>"

    html += f"""
<hr>
<p class="meta">Antigravity Medallion Lite | Data: NSE India | Generated by Claude Code</p>
</body></html>"""

    output_path = REPORT_DIR / f"{date_str}.html"
    with open(output_path, 'w') as f:
        f.write(html)
    logger.info(f"HTML report saved: {output_path}")
    return str(output_path)


def run_daily_report(date_str: str = None, capital: float = 1_000_000,
                      run_fresh: bool = False, output_html: bool = False,
                      notify: bool = False) -> dict:
    """
    Master daily report runner. Collects all signals, formats, outputs.

    Args:
        date_str:    Report date (default: today)
        capital:     Trading capital in ₹
        run_fresh:   Re-run all module scanners first
        output_html: Also save HTML report
        notify:      Send ntfy.sh push notification

    Returns:
        Dict with all signals and metadata
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')

    db = SignalsDB()
    now = datetime.now()

    # ── Header ────────────────────────────────────────────────────────────────
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print(f"║  ANTIGRAVITY MEDALLION LITE — DAILY SIGNAL REPORT".ljust(79) + "║")
    print(f"║  {now.strftime('%Y-%m-%d %H:%M IST')} | Capital: ₹{capital:,.0f}".ljust(79) + "║")
    print("╚" + "═" * 78 + "╝")

    # ── Section 1: FII Regime ─────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 5: FII/DII REGIME STATUS")
    print(f"{'━'*80}")

    fii_result = {}
    try:
        from scan_fii_dii_flows import run_fii_scan, print_fii_report
        fii_result = run_fii_scan(save_to_db=True)
        print_fii_report(fii_result)
    except Exception as e:
        logger.warning(f"FII scan failed: {e}")
        fii_regime_db = db.get_fii_regime(date_str)
        if fii_regime_db:
            fii_result = fii_regime_db
            print(f"  FII Regime (from DB): {fii_result.get('regime', 'UNKNOWN')}")
        else:
            print("  FII data unavailable — defaulting to NEUTRAL (0 points)")

    # ── Section 2: Seasonality ────────────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 6: ACTIVE SEASONALITY PATTERNS")
    print(f"{'━'*80}")

    seasonality_score, active_patterns = db.get_active_seasonality_score(date_str)
    if active_patterns:
        print(f"  Active patterns: {', '.join(active_patterns)}")
        print(f"  Score contribution: {seasonality_score:+d} points")
    else:
        print("  No validated seasonality patterns active today.")
        print("  (Run: python3 execution/analyze_seasonality.py --validate to validate patterns)")

    # ── Section 3: Volume Anomalies ───────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 1: VOLUME ANOMALY + DELIVERY SIGNALS")
    print(f"{'━'*80}")

    vol_signals = []
    if run_fresh:
        try:
            from scan_volume_anomalies import run_volume_scan, print_volume_report
            vol_signals = run_volume_scan(date_str=date_str, save_to_db=True)
            print_volume_report(vol_signals)
        except Exception as e:
            logger.warning(f"Volume scan failed: {e}")
    else:
        vol_signals = db.get_volume_signals(date_str, min_score=10)
        if vol_signals:
            from scan_volume_anomalies import print_volume_report
            print_volume_report(vol_signals)
        else:
            print("  No volume signals in DB for today. Run with --fresh to scan now.")

    # ── Section 4: Insider Clusters ───────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 2: INSIDER TRADING CLUSTER ALERTS (Last 7 Days)")
    print(f"{'━'*80}")

    insider_signals = db.get_insider_signals(date_str, days_back=7)
    if insider_signals:
        from scan_insider_clusters import print_insider_report
        print_insider_report(insider_signals)
    else:
        print("  No insider clusters detected. (Will refresh on --fresh)")

    # ── Section 5: Bulk/Block Deals ───────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 3: BULK/BLOCK DEAL ACCUMULATION ALERTS (Last 7 Days)")
    print(f"{'━'*80}")

    bulk_signals = db.get_bulk_signals(date_str, days_back=7)
    if bulk_signals:
        from scan_bulk_block_deals import print_bulk_report
        print_bulk_report(bulk_signals)
    else:
        print("  No bulk/block deal signals. (Will refresh on --fresh)")

    # ── Section 6: Pair Trade Signals ─────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  MODULE 4: ACTIVE PAIRS SIGNALS (Module 4 — Cointegration)")
    print(f"{'━'*80}")

    pairs_signals = get_pairs_signals()
    if pairs_signals:
        print(f"  {'Pair':<30} {'Z-Score':>8} {'Signal':<35} {'Sector'}")
        print(f"  {'-'*85}")
        for p in pairs_signals:
            z = p['z_score']
            flag = '>>>' if z >= 2.5 else ' > '
            print(f"  {flag} {p['pair']:<28} {z:>+7.2f}  {p['signal']:<35} {p['sector']}")
    else:
        print("  No active pair signals (Z-score > 2.0). Run scan_cointegrated_pairs.py")

    # ── Section 7: Composite Scores ───────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  COMPOSITE SCORING ENGINE — TOP 10 SIGNALS")
    print(f"{'━'*80}")

    composite_results = run_composite_scan(
        date_str=date_str, capital=capital, top_n=20, run_fresh=run_fresh
    )

    if composite_results:
        top_10 = [r for r in composite_results if r['composite_score'] > 0][:10]
        shorts = [r for r in composite_results if r['composite_score'] < SHORT_THRESHOLD]

        if top_10:
            print(f"\n  TOP 10 STOCKS BY COMPOSITE SCORE:")
            print(f"  {'#':<3} {'Symbol':<12} {'Score':>6} {'Signal':<14} {'Vol':>5} {'Ins':>5} {'Bulk':>6} {'Active Signals'}")
            print(f"  {'-'*90}")
            for i, r in enumerate(top_10, 1):
                mb = r['module_breakdown']
                sigs = ' | '.join(r['active_signals'][:2])
                flag = '>>>' if r['signal_type'] == 'STRONG_BUY' else (' > ' if r['signal_type'] == 'BUY' else '   ')
                print(f"  {flag} {i:<2} {r['symbol']:<12} {r['composite_score']:>6} "
                      f"{r['signal_type']:<14} {mb['volume']:>+4} {mb['insider']:>+4} {mb['bulk']:>+5}  {sigs}")

        if shorts:
            print(f"\n  SHORT SIGNALS:")
            for r in shorts[:5]:
                mb = r['module_breakdown']
                print(f"  [!] {r['symbol']:<12} Score: {r['composite_score']:>5}  "
                      f"{r['signal_type']:<14}  {' | '.join(r['active_signals'][:2])}")

        # Capital deployment
        actionable = [r for r in composite_results if r['signal_type'] in ('STRONG_BUY', 'BUY')]
        total_kelly = min(sum(r.get('kelly_pct', 0) for r in actionable), 70)
        print(f"\n  Estimated capital deployment: {total_kelly:.1f}% of ₹{capital:,.0f}")
    else:
        print("  No composite signals. Run with --fresh to scan all modules.")

    # ── Section 8: Backtest Dashboard ─────────────────────────────────────────
    print(f"\n{'━'*80}")
    print(f"  ROLLING BACKTEST DASHBOARD")
    print(f"{'━'*80}")

    backtest_stats = []
    for module in ['MODULE1', 'MODULE2', 'MODULE3', 'COMPOSITE']:
        for days in [30, 60, 90]:
            stats = db.get_backtest_stats(module, days)
            if stats.get('trades', 0) > 0:
                backtest_stats.append(stats)
                print(f"  {module:<10} {days}d: {stats['trades']:>4} trades | "
                      f"WR: {stats['win_rate']:.1%} | "
                      f"Avg 5d ret: {stats['avg_return_5d']:.2%}")

    if not backtest_stats:
        print("  No backtest data yet. Backtest results are logged automatically as signals generate.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*80}")
    print(f"  DAILY SUMMARY")
    print(f"{'═'*80}")

    strong_buys = [r for r in composite_results if r['signal_type'] == 'STRONG_BUY'] if composite_results else []
    buys = [r for r in composite_results if r['signal_type'] == 'BUY'] if composite_results else []

    print(f"  Strong Buy signals: {len(strong_buys)}")
    print(f"  Buy signals:        {len(buys)}")
    print(f"  Pair signals:       {len(pairs_signals)}")
    print(f"  Insider alerts:     {len(insider_signals)}")
    print(f"  Bulk deal alerts:   {len(bulk_signals)}")
    print(f"  FII Regime:         {fii_result.get('regime', 'UNKNOWN')}")
    print(f"  Seasonality Score:  {seasonality_score:+d}")
    print(f"\n  Next run: Tomorrow at 7:30 PM IST (after FII data published)")
    print(f"{'═'*80}\n")

    # ── Optional outputs ──────────────────────────────────────────────────────
    if output_html:
        html_path = generate_html_report(
            date_str, composite_results, fii_result, insider_signals,
            bulk_signals, pairs_signals, seasonality_score, active_patterns, backtest_stats
        )
        print(f"  HTML report: {html_path}")

    if notify:
        top_names = ', '.join(r['symbol'] for r in strong_buys[:3])
        msg = (f"FII: {fii_result.get('regime', 'N/A')} | "
               f"Strong Buys: {len(strong_buys)} {('— ' + top_names) if top_names else ''} | "
               f"Pairs: {len(pairs_signals)}")
        send_ntfy_notification(
            title=f"Antigravity {date_str}",
            message=msg,
            priority='high' if strong_buys else 'default'
        )

    return {
        'date': date_str,
        'composite_results': composite_results,
        'fii_result': fii_result,
        'insider_signals': insider_signals,
        'bulk_signals': bulk_signals,
        'pairs_signals': pairs_signals,
        'seasonality_score': seasonality_score,
        'active_patterns': active_patterns,
    }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description='Antigravity Daily Report Generator')
    parser.add_argument('--date', type=str, default=None, help='Report date YYYY-MM-DD')
    parser.add_argument('--capital', type=float, default=1_000_000, help='Capital in ₹')
    parser.add_argument('--fresh', action='store_true',
                        help='Re-run all module scanners (slow ~10-15 min)')
    parser.add_argument('--html', action='store_true', help='Also generate HTML report')
    parser.add_argument('--notify', action='store_true', help='Send ntfy.sh push notification')
    args = parser.parse_args()

    run_daily_report(
        date_str=args.date,
        capital=args.capital,
        run_fresh=args.fresh,
        output_html=args.html,
        notify=args.notify
    )
