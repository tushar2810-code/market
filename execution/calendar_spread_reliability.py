"""
Calendar Spread Reliability Analyzer
=====================================
Checks if SBICARD and RVNL calendar spreads converge reliably.

Logic:
- Near-month futures ALWAYS converge to spot at expiry (settlement rule)
- The question: does the spread (far - near) narrow as we approach near expiry?
- We analyze the current term structure to estimate convergence quality

Also checks: historical spot volatility, short interest indicators,
and expected P&L scenarios.
"""
import sys, os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from shoonya_client import ShoonyaClient

def get_all_futures_data(api, symbol):
    """Get spot + all futures months with full quotes."""
    # Spot
    search = api.searchscrip(exchange='NSE', searchtext=symbol)
    spot_price = None
    if search and 'values' in search:
        for r in search['values']:
            if r['tsym'] == f"{symbol}-EQ" or r['tsym'] == symbol:
                q = api.get_quotes(exchange='NSE', token=r['token'])
                if q and 'lp' in q:
                    spot_price = float(q['lp'])
                break

    # Futures
    ret = api.searchscrip(exchange='NFO', searchtext=symbol)
    if not ret or 'values' not in ret:
        return spot_price, []

    futures = [
        x for x in ret['values']
        if (x['instname'] == 'FUTSTK' or x['instname'] == 'FUTIDX') and x['symname'] == symbol
    ]

    now = datetime.now()
    valid = []
    for f in futures:
        try:
            exp = datetime.strptime(f['exd'], '%d-%b-%Y')
            if exp >= now - timedelta(days=1):
                q = api.get_quotes(exchange='NFO', token=f['token'])
                if q:
                    valid.append({
                        'expiry': f['exd'],
                        'exp_dt': exp,
                        'dte': max((exp - now).days, 1),
                        'ltp': float(q.get('lp', 0)),
                        'oi': int(q.get('oi', 0)),
                        'volume': int(q.get('v', 0)),
                        'lot_size': int(f.get('ls', 0)),
                        'bid': float(q.get('bp1', 0)),
                        'ask': float(q.get('sp1', 0)),
                        'prev_close': float(q.get('c', 0)),
                        'open': float(q.get('o', 0)),
                        'high': float(q.get('h', 0)),
                        'low': float(q.get('l', 0)),
                    })
        except:
            pass

    valid.sort(key=lambda x: x['exp_dt'])
    return spot_price, valid


def analyze_reliability(api, symbol, prev_spread=None):
    """Deep reliability analysis for a calendar spread stock."""
    print(f"\n{'█'*80}")
    print(f"  CALENDAR SPREAD RELIABILITY: {symbol}")
    print(f"{'█'*80}")

    spot, futures = get_all_futures_data(api, symbol)
    if not spot or len(futures) < 2:
        print(f"  [ERROR] Insufficient data for {symbol}")
        return

    print(f"\n  ┌─ TERM STRUCTURE ─────────────────────────────────────────────────┐")
    print(f"  │ Spot: ₹{spot:,.2f}")
    print(f"  │")

    for i, f in enumerate(futures):
        premium = f['ltp'] - spot
        prem_pct = (premium / spot) * 100
        ann_basis = (premium / spot) * (365 / f['dte']) * 100
        oi_lots = f['oi'] // f['lot_size'] if f['lot_size'] > 0 else 0
        label = ['NEAR', 'FAR', '3RD', '4TH'][i] if i < 4 else f'M{i+1}'
        print(f"  │ {label:>4}: ₹{f['ltp']:>10,.2f}  Exp: {f['expiry']}  DTE: {f['dte']:>3}d  "
              f"Prem: {premium:>+8.2f} ({prem_pct:>+5.2f}%)  Ann: {ann_basis:>+7.2f}%  "
              f"OI: {f['oi']:>12,}  ({oi_lots:,} lots)")
    print(f"  └──────────────────────────────────────────────────────────────────┘")

    near = futures[0]
    far = futures[1]

    near_prem = near['ltp'] - spot
    far_prem = far['ltp'] - spot
    spread = far['ltp'] - near['ltp']

    # ===== 1. CONVERGENCE MECHANICS =====
    print(f"\n  ┌─ CONVERGENCE MECHANICS ──────────────────────────────────────────┐")
    print(f"  │")
    print(f"  │ Current Spread (Far - Near): {spread:+.2f}")
    if prev_spread is not None:
        print(f"  │ Previous Entry Spread:       {prev_spread:+.2f}")
        print(f"  │ Spread Change Since Last:    {spread - prev_spread:+.2f}")
    print(f"  │")

    # At near expiry: near → spot (strong tendency, ~64-83% WR depending on symbol, NOT guaranteed)
    # Far becomes the new near. Based on term structure, estimate far's value
    # when it becomes the near month.
    # If term structure is consistent, far will trade at the same discount
    # that near currently trades at when far has the same DTE as near does now.
    #
    # Example: Near is 9 DTE at -22 prem. Far is 38 DTE at -54 prem.
    # At near expiry (9 days from now), Far will be ~29 DTE.
    # If the discount pattern holds, far at 29 DTE should be roughly:
    # interpolate between current near (9d, -22) and far (38d, -54)

    # Linear interpolation of premium as function of DTE
    remaining_far_dte = far['dte'] - near['dte']
    if near['dte'] > 0 and far['dte'] > near['dte']:
        # Daily premium decay rate (prem per day)
        near_decay_rate = near_prem / near['dte']  # prem per day
        far_decay_rate = far_prem / far['dte']

        # Method 1: Assume far decays at its own rate
        far_prem_at_near_expiry_own_rate = far_decay_rate * remaining_far_dte
        far_price_est_own = spot + far_prem_at_near_expiry_own_rate

        # Method 2: Assume far adopts near's decay rate when it gets closer
        # (near currently decays at -2.44/day, far at -1.42/day)
        # As far approaches expiry, its decay accelerates. But at 29 DTE,
        # it's still far enough that its current rate is more likely.

        # Method 3: Use term structure ratio
        # Near prem / spot = -3.2%. At 9 DTE.
        # Far prem / spot = -7.9%. At 38 DTE.
        # Rate of premium per DTE = (-7.9 - (-3.2)) / (38 - 9) = -0.16% per day
        # At 29 DTE: -3.2 + (-0.16 * 20) = -6.4% → far price = spot * (1 - 0.064)

        print(f"  │ NEAR-MONTH CONVERGENCE (guaranteed):")
        print(f"  │   Near at {near_prem:+.2f} premium, {near['dte']}d to expiry")
        print(f"  │   At expiry: Near → Spot (converges {abs(near_prem):.2f} in {near['dte']}d)")
        print(f"  │   Short leg P&L: {near_prem:+.2f}/share × {near['lot_size']} = ₹{near_prem * near['lot_size']:+,.0f}")
        print(f"  │   (You SELL at {near['ltp']:.2f}, buy back at ~{spot:.2f} at expiry)")
        print(f"  │")
        print(f"  │ FAR-MONTH EVOLUTION (estimated):")
        print(f"  │   Far at {far_prem:+.2f} premium, {far['dte']}d to expiry")
        print(f"  │   At near expiry, Far will have {remaining_far_dte}d to go")
        print(f"  │   Estimated Far premium at near expiry: {far_prem_at_near_expiry_own_rate:+.2f}")
        print(f"  │   Estimated Far price: ₹{far_price_est_own:,.2f}")
        far_leg_pnl = far_price_est_own - far['ltp']
        print(f"  │   Long leg P&L: {far_leg_pnl:+.2f}/share × {near['lot_size']} = ₹{far_leg_pnl * near['lot_size']:+,.0f}")

        # Total estimated P&L
        # Short near: sell at near_price, close at spot = near_price - spot = near_prem (negative = loss)
        # Long far: buy at far_price, close at far_est = far_est - far_price
        near_pnl = near['ltp'] - spot  # This is near_prem, which is negative (loss on short)
        far_pnl = far_price_est_own - far['ltp']
        total_pnl = near_pnl + far_pnl
        total_pnl_lot = total_pnl * near['lot_size']

        print(f"  │")
        print(f"  │ ╔═══════════════════════════════════════════════════════════════╗")
        print(f"  │ ║ ESTIMATED P&L AT NEAR EXPIRY (hold to settlement)            ║")
        print(f"  │ ║   Short Near:  {near_pnl:+8.2f}/share  (₹{near_pnl * near['lot_size']:>+10,.0f}){'':>13}║")
        print(f"  │ ║   Long Far:    {far_pnl:+8.2f}/share  (₹{far_pnl * near['lot_size']:>+10,.0f}){'':>13}║")
        print(f"  │ ║   ─────────────────────────────────────────{'':>19}║")
        print(f"  │ ║   NET:         {total_pnl:+8.2f}/share  (₹{total_pnl_lot:>+10,.0f}){'':>13}║")
        print(f"  │ ╚═══════════════════════════════════════════════════════════════╝")

    print(f"  └──────────────────────────────────────────────────────────────────┘")

    # ===== 2. RISK ANALYSIS =====
    print(f"\n  ┌─ RISK ANALYSIS ───────────────────────────────────────────────────┐")

    # Max adverse spread (how much can spread widen?)
    # In the worst case, far drops more than near.
    # Use intraday range as proxy for daily risk.
    near_range = near['high'] - near['low'] if near['high'] > 0 else 0
    far_range = far['high'] - far['low'] if far['high'] > 0 else 0

    print(f"  │")
    print(f"  │ Today's Range  — Near: ₹{near_range:.2f}  Far: ₹{far_range:.2f}")
    print(f"  │ If far drops {far_range:.2f} while near is flat, spread widens by ₹{far_range:.2f}")
    print(f"  │ Max 1-day adverse = ₹{(near_range + far_range) * near['lot_size']:,.0f} (both legs move against)")
    print(f"  │")

    # OI analysis — high OI in near = many shorts rolling out
    near_oi_lots = near['oi'] // near['lot_size'] if near['lot_size'] > 0 else 0
    far_oi_lots = far['oi'] // far['lot_size'] if far['lot_size'] > 0 else 0
    oi_ratio = far['oi'] / near['oi'] if near['oi'] > 0 else 0

    print(f"  │ OI Structure:")
    print(f"  │   Near: {near['oi']:>12,} ({near_oi_lots:>8,} lots)")
    print(f"  │   Far:  {far['oi']:>12,} ({far_oi_lots:>8,} lots)")
    print(f"  │   Far/Near OI Ratio: {oi_ratio:.2f}")
    if oi_ratio > 0.3:
        print(f"  │   ⚠ High far-month OI = significant rollover activity")
    else:
        print(f"  │   ✓ Low far-month OI = less crowded trade")
    print(f"  │")

    # Volume ratio
    vol_ratio = far['volume'] / near['volume'] if near['volume'] > 0 else 0
    print(f"  │ Volume Structure:")
    print(f"  │   Near: {near['volume']:>12,}  Far: {far['volume']:>12,}")
    print(f"  │   Far/Near Vol Ratio: {vol_ratio:.2f}")
    print(f"  │")

    # Bid-ask analysis
    near_ba = near['ask'] - near['bid'] if near['ask'] > 0 else 0
    far_ba = far['ask'] - far['bid'] if far['ask'] > 0 else 0
    total_slippage = (near_ba + far_ba) * near['lot_size']
    print(f"  │ Bid-Ask Spread (slippage cost):")
    print(f"  │   Near: ₹{near_ba:.2f}  Far: ₹{far_ba:.2f}")
    print(f"  │   Total entry+exit slippage: ~₹{total_slippage * 2:,.0f} (both legs, round trip)")
    print(f"  └──────────────────────────────────────────────────────────────────┘")

    # ===== 3. RELIABILITY VERDICT =====
    print(f"\n  ┌─ RELIABILITY VERDICT ─────────────────────────────────────────────┐")
    print(f"  │")

    # Factors for reliability
    factors = []

    # 1. Is the term structure consistently in backwardation?
    all_discount = all(f['ltp'] < spot for f in futures)
    progressive = True
    for i in range(1, len(futures)):
        if futures[i]['ltp'] > futures[i-1]['ltp']:
            progressive = False
            break

    if all_discount and progressive:
        factors.append(("Term structure", "STRONG", "Progressive backwardation — all months below spot, deeper with time"))
    elif all_discount:
        factors.append(("Term structure", "MODERATE", "All months below spot but not perfectly progressive"))
    else:
        factors.append(("Term structure", "WEAK", "Mixed — not all months in backwardation"))

    # 2. Is the near month already priced close to spot? (convergence started)
    near_gap_pct = abs(near_prem / spot) * 100
    if near_gap_pct < 1:
        factors.append(("Near convergence", "STRONG", f"Near is only {near_gap_pct:.2f}% from spot — already converging"))
    elif near_gap_pct < 3:
        factors.append(("Near convergence", "MODERATE", f"Near is {near_gap_pct:.2f}% from spot"))
    else:
        factors.append(("Near convergence", "WEAK", f"Near is {near_gap_pct:.2f}% from spot — still wide gap"))

    # 3. Liquidity
    if near['oi'] > 5000000 and far['oi'] > 1000000:
        factors.append(("Liquidity", "STRONG", f"Deep OI on both legs"))
    elif near['oi'] > 1000000:
        factors.append(("Liquidity", "MODERATE", f"Decent OI"))
    else:
        factors.append(("Liquidity", "WEAK", f"Thin OI — slippage risk"))

    # 4. Spread vs slippage
    spread_abs = abs(spread)
    if total_slippage > 0 and spread_abs > total_slippage * 4:
        factors.append(("Spread/Cost", "STRONG", f"Spread ₹{spread_abs:.2f} >> slippage ₹{total_slippage:.2f}"))
    elif total_slippage > 0 and spread_abs > total_slippage * 2:
        factors.append(("Spread/Cost", "MODERATE", f"Spread ₹{spread_abs:.2f} > slippage ₹{total_slippage:.2f}"))
    else:
        factors.append(("Spread/Cost", "WEAK", f"Spread ₹{spread_abs:.2f} barely covers slippage ₹{total_slippage:.2f}"))

    # 5. Repetition (user says they've done this before)
    if prev_spread is not None:
        if abs(spread - prev_spread) < abs(prev_spread) * 0.2:
            factors.append(("Repeatability", "STRONG", f"Current spread ({spread:.2f}) similar to last ({prev_spread:.2f})"))
        else:
            factors.append(("Repeatability", "MODERATE", f"Spread changed from {prev_spread:.2f} to {spread:.2f}"))

    # Print factors
    strong = sum(1 for _, s, _ in factors if s == "STRONG")
    moderate = sum(1 for _, s, _ in factors if s == "MODERATE")
    weak = sum(1 for _, s, _ in factors if s == "WEAK")

    for name, strength, detail in factors:
        icon = "✓" if strength == "STRONG" else "~" if strength == "MODERATE" else "✗"
        print(f"  │  {icon} {name:>18}: {strength:>8} — {detail}")

    print(f"  │")
    total_score = strong * 3 + moderate * 1 + weak * 0
    max_score = len(factors) * 3
    reliability_pct = (total_score / max_score) * 100 if max_score > 0 else 0

    if reliability_pct >= 70:
        verdict = "HIGH RELIABILITY — Trade with confidence"
    elif reliability_pct >= 50:
        verdict = "MODERATE RELIABILITY — Trade with caution"
    else:
        verdict = "LOW RELIABILITY — Risky, consider skipping"

    print(f"  │  Reliability Score: {reliability_pct:.0f}% ({strong} strong, {moderate} moderate, {weak} weak)")
    print(f"  │  Verdict: {verdict}")
    print(f"  │")

    # ===== KEY RISK =====
    print(f"  │  ⚠ KEY RISK:")
    print(f"  │  The spread WILL converge at near expiry (futures settle to spot).")
    print(f"  │  But you capture ONLY the near-month convergence.")
    print(f"  │  The far month stays discounted until IT approaches expiry.")
    print(f"  │  True profit = near_convergence_gain - far_month_time_decay")
    if total_pnl < 0:
        print(f"  │")
        print(f"  │  ⚠⚠ ESTIMATED P&L IS NEGATIVE (₹{total_pnl_lot:+,.0f})")
        print(f"  │  The near-month loss on short exceeds far-month gain.")
        print(f"  │  This happens when near is deeply discounted — you're SHORT")
        print(f"  │  at a discount and it RISES to spot at settlement (loss).")
    print(f"  └──────────────────────────────────────────────────────────────────┘")

    return {
        'symbol': symbol,
        'spot': spot,
        'spread': spread,
        'near_prem': near_prem,
        'far_prem': far_prem,
        'est_pnl': total_pnl_lot,
        'reliability_pct': reliability_pct,
    }


if __name__ == '__main__':
    client = ShoonyaClient()
    api = client.login()
    if not api:
        print("[FATAL] Login failed")
        exit(1)

    print(f"\n{'='*80}")
    print(f"  CALENDAR SPREAD RELIABILITY CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*80}")

    # SBICARD — user says last spread was -33
    r1 = analyze_reliability(api, 'SBICARD', prev_spread=-33)

    # RVNL — user says last spread was -34
    r2 = analyze_reliability(api, 'RVNL', prev_spread=-34)

    # Also check JUBLFOOD since it was #3 and a CLASSIC setup
    r3 = analyze_reliability(api, 'JUBLFOOD')

    # Summary
    print(f"\n\n{'='*80}")
    print(f"  COMPARATIVE SUMMARY")
    print(f"{'='*80}")
    print(f"  {'Stock':>12} | {'Spread':>8} | {'Est P&L/Lot':>12} | {'Reliability':>12} | Note")
    print(f"  {'-'*12}-+-{'-'*8}-+-{'-'*12}-+-{'-'*12}-+-{'-'*30}")
    for r in [r1, r2, r3]:
        if r:
            note = "CLASSIC" if r['near_prem'] > 0 and r['far_prem'] < 0 else "Both discount" if r['near_prem'] < 0 else ""
            print(f"  {r['symbol']:>12} | {r['spread']:>+8.2f} | ₹{r['est_pnl']:>+10,.0f} | {r['reliability_pct']:>10.0f}% | {note}")
