#!/usr/bin/env python3
"""Code review graph — visualize commits, changes, WR impact."""

import subprocess
import json
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt

def get_commits():
    """Fetch all commits."""
    cmd = "git log --pretty=format:'%H|%an|%s|%ai' --all"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    commits = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split('|')
        commits.append({
            'hash': parts[0],
            'author': parts[1],
            'message': parts[2],
            'date': parts[3]
        })
    return commits

def get_file_changes(commit_hash):
    """Get files changed in commit."""
    cmd = f"git show --name-only --pretty='' {commit_hash}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip().split('\n')

def build_graph():
    """Build review graph."""
    commits = get_commits()

    # Map commits to impact
    impact = defaultdict(lambda: {'files': 0, 'msg': '', 'date': ''})

    for commit in commits:
        files = get_file_changes(commit['hash'])
        impact[commit['hash']] = {
            'files': len([f for f in files if f]),
            'msg': commit['message'][:50],
            'date': commit['date'][:10],
            'author': commit['author']
        }

    return commits, impact

def plot_graph(commits, impact):
    """Visualize commit impact timeline."""
    if not commits:
        print("No commits found.")
        return

    dates = [c['date'][:10] for c in commits]
    file_counts = [impact[c['hash']]['files'] for c in commits]

    fig, ax = plt.subplots(figsize=(14, 6), facecolor='#0a0a0a')
    ax.set_facecolor('#111111')

    ax.plot(range(len(commits)), file_counts, marker='o', color='#00d4ff', linewidth=2)
    ax.fill_between(range(len(commits)), file_counts, alpha=0.2, color='#00d4ff')

    ax.set_xlabel('Commit Timeline', color='#e0e0e0')
    ax.set_ylabel('Files Changed', color='#e0e0e0')
    ax.set_title('Code Review Graph — Commit Impact', color='#e0e0e0', fontweight='bold')
    ax.grid(True, alpha=0.3, color='#444')
    ax.tick_params(colors='#e0e0e0')

    # Add labels for every 5th commit
    ticks = range(0, len(commits), max(1, len(commits)//10))
    ax.set_xticks(ticks)
    ax.set_xticklabels([dates[i] if i < len(dates) else '' for i in ticks], rotation=45)

    plt.tight_layout()
    plt.savefig('.tmp/review_graph.png', dpi=120, facecolor='#0a0a0a')
    print("Graph saved: .tmp/review_graph.png")

    return fig

if __name__ == '__main__':
    commits, impact = build_graph()
    if commits:
        print(f"\nTotal commits: {len(commits)}")
        print(f"Avg files/commit: {sum(d['files'] for d in impact.values())/len(impact):.1f}")
        print("\nLatest 5 commits:")
        for c in commits[-5:]:
            h = c['hash'][:8]
            print(f"  {h} | {impact[c['hash']]['files']} files | {impact[c['hash']]['msg']}")
        plot_graph(commits, impact)
