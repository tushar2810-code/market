import pandas as pd
import os

files_to_check = ['.tmp/pair_discovery_results.csv', '.tmp/pair_scan_results.csv', '.tmp/universe_deep_scan.csv', 'execution/scan_proven_pairs.py']

for file in files_to_check:
    if os.path.exists(file):
        print(f"\nChecking {file}:")
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(file)
                if 'Pair' in df.columns:
                    target_pairs = df[df['Pair'].str.contains('PFC|RECLTD', na=False)]
                    if not target_pairs.empty:
                        print(target_pairs[['Pair', 'Z_Score', 'Win_Rate', 'Trades'] if 'Win_Rate' in df.columns else target_pairs.columns])
                elif 'pair' in df.columns:
                    target_pairs = df[df['pair'].str.contains('PFC|RECLTD', na=False)]
                    if not target_pairs.empty:
                        print(target_pairs)
            except Exception as e:
                print(f"Error reading {file}: {e}")
        elif file.endswith('.py'):
            with open(file, 'r') as f:
                lines = f.readlines()
            for line in lines:
                if ('PFC' in line or 'RECLTD' in line) and '"' in line and '/' in line:
                    print(line.strip())

