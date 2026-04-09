"""
Kelly Criterion Position Sizer for Antigravity v3.

Takes historical trade stats and current capital → optimal lot count.

Usage:
    # Standalone
    python3 execution/kelly_sizer.py --wr 0.83 --avg-win 4.9 --avg-loss 5.7 --capital 1000000 --price 1800 --lot-size 350

    # As library
    from kelly_sizer import kelly_optimal_lots
    lots = kelly_optimal_lots(wr=0.83, avg_win=0.049, avg_loss=0.057, capital=1000000, price=1800, lot_size=350)
"""

import argparse
import json


def kelly_fraction(win_rate, avg_win_pct, avg_loss_pct):
    """
    Calculate the Kelly fraction — optimal fraction of capital to risk.
    
    Kelly% = (WR * AvgWin/AvgLoss - (1-WR)) / (AvgWin/AvgLoss)
    
    Returns: (kelly_full, kelly_half, edge_per_trade)
    """
    if avg_loss_pct == 0:
        return 0, 0, 0

    b = avg_win_pct / avg_loss_pct  # Win/Loss ratio
    p = win_rate
    q = 1 - win_rate

    kelly = (p * b - q) / b

    # Edge per trade (expected value)
    edge = p * avg_win_pct - q * avg_loss_pct

    # Clamp: never bet more than 25% or negative
    kelly_full = max(0, min(kelly, 0.25))
    kelly_half = kelly_full / 2  # Half-Kelly (safer)

    return kelly_full, kelly_half, edge


def kelly_optimal_lots(wr, avg_win, avg_loss, capital, price, lot_size, mode='half'):
    """
    Calculate optimal lot count based on Kelly Criterion.
    
    Args:
        wr: Win rate (0 to 1)
        avg_win: Average winning return (e.g., 0.049 for 4.9%)
        avg_loss: Average losing return (e.g., 0.057 for 5.7%) — always positive
        capital: Total trading capital in ₹
        price: Current price of the instrument
        lot_size: Lot size of the instrument
        mode: 'full' for full Kelly, 'half' for half-Kelly (default, safer)
    
    Returns: dict with lots, capital_deployed, kelly_fraction, edge
    """
    kelly_full, kelly_half, edge = kelly_fraction(wr, avg_win, avg_loss)
    
    kelly_pct = kelly_half if mode == 'half' else kelly_full
    
    if kelly_pct <= 0:
        return {
            'lots': 0,
            'kelly_fraction': 0,
            'kelly_half': 0,
            'kelly_full': kelly_full,
            'edge_per_trade': edge,
            'capital_to_deploy': 0,
            'capital_per_lot': price * lot_size,
            'reason': 'No edge — Kelly is zero or negative'
        }
    
    capital_to_deploy = capital * kelly_pct
    capital_per_lot = price * lot_size
    optimal_lots = int(capital_to_deploy / capital_per_lot)
    
    # Minimum 1 lot if there's an edge
    if optimal_lots == 0 and edge > 0:
        optimal_lots = 1
    
    return {
        'lots': optimal_lots,
        'kelly_fraction': kelly_pct,
        'kelly_half': kelly_half,
        'kelly_full': kelly_full,
        'edge_per_trade': edge,
        'capital_to_deploy': capital_to_deploy,
        'capital_per_lot': capital_per_lot,
        'actual_deployed': optimal_lots * capital_per_lot,
        'pct_of_capital': (optimal_lots * capital_per_lot) / capital * 100
    }


def kelly_for_pair(wr, avg_win, avg_loss, capital,
                   price_a, lot_a, price_b, lot_b,
                   ratio_a=1, ratio_b=1, mode='half'):
    """
    Calculate Kelly sizing for a PAIR TRADE (two legs).
    Capital per trade = notional of BOTH legs.

    Args:
        ratio_a, ratio_b: Multi-lot ratio from cash-neutrality solver (e.g. 2, 3).
                          lot_a and lot_b should be the BASE (1x) lot sizes.
                          Final lots = lot_a * ratio_a and lot_b * ratio_b.

    Returns: dict with lots_a, lots_b, total deployment
    """
    kelly_full, kelly_half, edge = kelly_fraction(wr, avg_win, avg_loss)
    kelly_pct = kelly_half if mode == 'half' else kelly_full

    if kelly_pct <= 0:
        return {
            'lots_a': 0, 'lots_b': 0,
            'kelly_fraction': 0, 'edge': edge,
            'reason': 'No edge'
        }

    capital_to_deploy = capital * kelly_pct

    # One "unit" = ratio_a lots of A + ratio_b lots of B (cash-neutral sizing)
    cost_per_unit = (price_a * lot_a * ratio_a) + (price_b * lot_b * ratio_b)
    optimal_units = int(capital_to_deploy / cost_per_unit)

    if optimal_units == 0 and edge > 0:
        optimal_units = 1

    final_lots_a = optimal_units * ratio_a
    final_lots_b = optimal_units * ratio_b
    total_deployed = optimal_units * cost_per_unit

    return {
        'lots_a': final_lots_a,
        'lots_b': final_lots_b,
        'units': optimal_units,
        'ratio': f"{ratio_a}:{ratio_b}",
        'kelly_fraction': kelly_pct,
        'kelly_full': kelly_full,
        'edge_per_trade': edge,
        'cost_per_unit': cost_per_unit,
        'total_deployed': total_deployed,
        'pct_of_capital': total_deployed / capital * 100
    }


def print_kelly_report(result, label=""):
    """Pretty-print Kelly sizing result."""
    print(f"\n{'═'*60}")
    print(f"  KELLY CRITERION — {label}")
    print(f"{'═'*60}")
    
    if result.get('reason'):
        print(f"  ❌ {result['reason']}")
        return
    
    print(f"  Kelly Fraction:   {result['kelly_full']:.1%} (full) / {result['kelly_fraction']:.1%} (half)")
    print(f"  Edge Per Trade:   {result['edge_per_trade']:.2%}")
    
    if 'lots_a' in result:
        # Pair trade
        print(f"  Optimal Units:    {result['units']} (A: {result['lots_a']} lots, B: {result['lots_b']} lots)")
        print(f"  Cost Per Unit:    ₹{result['cost_per_unit']:,.0f}")
        print(f"  Total Deployed:   ₹{result['total_deployed']:,.0f}")
    else:
        # Single trade
        print(f"  Optimal Lots:     {result['lots']}")
        print(f"  Capital Per Lot:  ₹{result['capital_per_lot']:,.0f}")
        print(f"  Total Deployed:   ₹{result['actual_deployed']:,.0f}")
    
    print(f"  % of Capital:     {result['pct_of_capital']:.1f}%")
    print(f"{'═'*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kelly Criterion Position Sizer')
    parser.add_argument('--wr', type=float, required=True, help='Win rate (0-1)')
    parser.add_argument('--avg-win', type=float, required=True, help='Average win %% (e.g., 4.9)')
    parser.add_argument('--avg-loss', type=float, required=True, help='Average loss %% (e.g., 5.7)')
    parser.add_argument('--capital', type=float, required=True, help='Total capital in ₹')
    parser.add_argument('--price', type=float, help='Current price (single instrument)')
    parser.add_argument('--lot-size', type=int, help='Lot size (single instrument)')
    parser.add_argument('--price-a', type=float, help='Price of leg A (pair trade)')
    parser.add_argument('--lot-a', type=int, help='Lot size of leg A')
    parser.add_argument('--price-b', type=float, help='Price of leg B (pair trade)')
    parser.add_argument('--lot-b', type=int, help='Lot size of leg B')
    parser.add_argument('--mode', choices=['full', 'half'], default='half', help='Kelly mode')
    args = parser.parse_args()
    
    # Convert percentages to fractions
    avg_win = args.avg_win / 100
    avg_loss = args.avg_loss / 100
    
    if args.price_a and args.price_b:
        # Pair trade mode
        result = kelly_for_pair(
            wr=args.wr, avg_win=avg_win, avg_loss=avg_loss,
            capital=args.capital,
            price_a=args.price_a, lot_a=args.lot_a,
            price_b=args.price_b, lot_b=args.lot_b,
            mode=args.mode
        )
        print_kelly_report(result, "PAIR TRADE")
    elif args.price and args.lot_size:
        # Single instrument mode
        result = kelly_optimal_lots(
            wr=args.wr, avg_win=avg_win, avg_loss=avg_loss,
            capital=args.capital,
            price=args.price, lot_size=args.lot_size,
            mode=args.mode
        )
        print_kelly_report(result, "SINGLE INSTRUMENT")
    else:
        # Just show the Kelly fraction
        kf, kh, edge = kelly_fraction(args.wr, avg_win, avg_loss)
        print(f"\n  Kelly Full:  {kf:.1%}")
        print(f"  Kelly Half:  {kh:.1%}")
        print(f"  Edge/Trade:  {edge:.2%}")
