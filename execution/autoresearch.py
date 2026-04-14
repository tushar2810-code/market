#!/usr/bin/env python3
"""
AutoResearch — Karpathy-style ratchet optimization for pairs/calendar backtest.

One-directional improvement: every committed change is a validated improvement.
Uses git as the decision engine: improve → commit, regress → revert.

Usage:
    python autoresearch.py --iterations 50
    python autoresearch.py --iterations 50 --resume
"""

import json
import os
import subprocess
import csv
import random
import copy
import argparse
from datetime import datetime

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
PARAMS_FILE  = os.path.join(SCRIPT_DIR, 'params.json')
LOG_FILE     = os.path.join(PROJECT_ROOT, '.tmp', 'autoresearch_log.csv')
RESULTS_FILE = os.path.join(PROJECT_ROOT, '.tmp', 'autoresearch_results.json')
BEST_FILE    = os.path.join(PROJECT_ROOT, '.tmp', 'autoresearch_best.json')

# Parameter ranges — (min, max)
PARAM_RANGES = {
    'SSS_THRESHOLD':          (1.5, 8.0),
    'Z_EXIT':                 (0.1, 2.0),
    'PAIRS_TIME_STOP':        (15, 45),
    'PAIRS_MIN_Z_ENTRY':      (1.0, 2.5),
    'PAIRS_MAX_Z_ENTRY':      (3.0, 6.0),
    'MAX_COMPOUND_SCALE':     (1.0, 4.0),
    'CORR_MIN':               (0.1, 0.6),
    'STRUCT_BREAK_Z_MULT':    (1.2, 3.0),
    'STRUCT_BREAK_CORR_FLOOR':(0.1, 0.5),
    'SCALE_CONC_CAP':         (0.05, 0.30),
    'PAIRS_MAX_POSITIONS':    (3, 12),
}

# These must be rounded to int
INT_PARAMS = {'PAIRS_TIME_STOP', 'PAIRS_MAX_POSITIONS'}

# Default starting parameters
DEFAULTS = {
    'SSS_THRESHOLD': 2.0,
    'Z_EXIT': 1.0,
    'PAIRS_TIME_STOP': 30,
    'PAIRS_MIN_Z_ENTRY': 1.5,
    'PAIRS_MAX_Z_ENTRY': 4.0,
    'MAX_COMPOUND_SCALE': 2.5,
    'CORR_MIN': 0.3,
    'STRUCT_BREAK_Z_MULT': 1.5,
    'STRUCT_BREAK_CORR_FLOOR': 0.25,
    'SCALE_CONC_CAP': 0.15,
    'PAIRS_MAX_POSITIONS': 8,
}


def load_params():
    if os.path.exists(PARAMS_FILE):
        with open(PARAMS_FILE) as f:
            return json.load(f)
    return copy.deepcopy(DEFAULTS)


def save_params(params):
    with open(PARAMS_FILE, 'w') as f:
        json.dump(params, f, indent=2)


def run_backtest():
    """Run backtest_mega.py and parse results from JSON output."""
    result = subprocess.run(
        ['python3', 'execution/backtest_mega.py'],
        capture_output=True, text=True, timeout=600,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  [ERROR] Backtest failed:\n{result.stderr[-500:]}")
        return None

    if not os.path.exists(RESULTS_FILE):
        print(f"  [ERROR] No results file produced")
        return None

    with open(RESULTS_FILE) as f:
        return json.load(f)


def compute_score(r):
    """
    Maximize wealth creation. WR as quality multiplier (sqrt to avoid gaming).
    """
    equity = r['final_equity']
    wr = r['win_rate'] / 100
    return equity * (wr ** 0.5)


def propose_mutation(params):
    """Pick 1-2 parameters, apply gaussian perturbation within valid ranges."""
    new = copy.deepcopy(params)
    n_changes = random.choices([1, 2], weights=[0.7, 0.3])[0]
    keys = random.sample(list(PARAM_RANGES.keys()), n_changes)
    desc_parts = []

    for k in keys:
        lo, hi = PARAM_RANGES[k]
        old_val = new.get(k, DEFAULTS.get(k, (lo + hi) / 2))
        sigma = (hi - lo) * 0.15
        new_val = old_val + random.gauss(0, sigma)
        new_val = max(lo, min(hi, new_val))
        if k in INT_PARAMS:
            new_val = int(round(new_val))
        else:
            new_val = round(new_val, 3)
        new[k] = new_val
        desc_parts.append(f"{k}: {old_val}->{new_val}")

    return new, "; ".join(desc_parts)


def log_trial(iteration, params, results, score, desc, accepted):
    """Append trial to CSV log."""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    file_exists = os.path.exists(LOG_FILE)

    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                'iteration', 'timestamp', 'accepted', 'score',
                'net_pnl', 'win_rate', 'n_trades', 'min_monthly_pnl',
                'description', 'params_json',
            ])
        writer.writerow([
            iteration,
            datetime.now().isoformat(),
            accepted,
            round(score, 0),
            round(results['net_pnl'], 0) if results else 0,
            round(results['win_rate'], 1) if results else 0,
            results.get('n_trades', 0) if results else 0,
            round(results.get('min_monthly_pnl', 0), 0) if results else 0,
            desc,
            json.dumps(params),
        ])


def git_commit(params, score, desc):
    """Commit params.json with score in message."""
    subprocess.run(
        ['git', 'add', PARAMS_FILE],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    msg = f"autoresearch: score={score:.0f} | {desc}"
    subprocess.run(
        ['git', 'commit', '-m', msg],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )


def git_revert_params():
    """Revert params.json to last committed version."""
    subprocess.run(
        ['git', 'checkout', '--', PARAMS_FILE],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )


def main():
    parser = argparse.ArgumentParser(description='AutoResearch ratchet optimizer')
    parser.add_argument('--iterations', type=int, default=50)
    parser.add_argument('--resume', action='store_true',
                        help='Resume from existing params.json')
    args = parser.parse_args()

    print("=" * 70)
    print("  AutoResearch — Ratchet Optimization")
    print(f"  Iterations: {args.iterations}")
    print("=" * 70)

    # Initialize or resume
    if args.resume and os.path.exists(PARAMS_FILE):
        params = load_params()
        print(f"\n  Resuming from existing params.json")
    else:
        params = copy.deepcopy(DEFAULTS)
        save_params(params)
        print(f"\n  Starting from defaults")

    # Baseline run
    print(f"\n  [iter 0] Running baseline...")
    results = run_backtest()
    if results is None:
        print("  Baseline failed. Fix errors first.")
        return

    best_score = compute_score(results)
    best_params = copy.deepcopy(params)
    log_trial(0, params, results, best_score, "baseline", accepted=True)
    git_commit(params, best_score, "baseline")

    print(f"  Baseline: score={best_score:.0f}  "
          f"net=Rs.{results['net_pnl']/1e5:.2f}L  "
          f"WR={results['win_rate']:.1f}%  "
          f"trades={results['n_trades']}")

    # Save best
    with open(BEST_FILE, 'w') as f:
        json.dump({'score': best_score, 'params': best_params,
                   'results': results}, f, indent=2)

    # Ratchet loop
    accepted_count = 0
    for i in range(1, args.iterations + 1):
        new_params, desc = propose_mutation(params)
        save_params(new_params)

        results = run_backtest()
        if results is None:
            print(f"  [iter {i}] FAILED — reverting ({desc})")
            save_params(params)
            log_trial(i, new_params, {}, 0, f"FAILED: {desc}", accepted=False)
            continue

        score = compute_score(results)

        if score > best_score:
            improvement = score - best_score
            print(f"  [iter {i}] IMPROVED +{improvement:.0f}: "
                  f"score {best_score:.0f}->{score:.0f}  "
                  f"net=Rs.{results['net_pnl']/1e5:.2f}L  "
                  f"WR={results['win_rate']:.1f}%  ({desc})")
            best_score = score
            params = new_params
            best_params = copy.deepcopy(params)
            git_commit(params, score, desc)
            accepted_count += 1

            with open(BEST_FILE, 'w') as f:
                json.dump({'score': best_score, 'params': best_params,
                           'results': results}, f, indent=2)
        else:
            print(f"  [iter {i}] rejected: {score:.0f} <= {best_score:.0f}  ({desc})")
            save_params(params)  # revert to previous best

        log_trial(i, new_params, results, score, desc, score > best_score)

    # Summary
    print(f"\n{'='*70}")
    print(f"  DONE: {args.iterations} iterations, {accepted_count} accepted")
    print(f"  Best score: {best_score:.0f}")
    print(f"  Best params: {json.dumps(best_params, indent=2)}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
