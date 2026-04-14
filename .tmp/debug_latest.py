import pandas as pd
import glob
import os

files = glob.glob('.tmp/5y_data/*_5Y.csv')
latest_dates = []

for f in files:
    df = pd.read_csv(f)
    if 'FH_TIMESTAMP' in df.columns:
        dates = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
        if not dates.isna().all():
            latest = dates.max()
            latest_dates.append(latest)

if latest_dates:
    overall_max = max(latest_dates)
    print("Latest date across all files:", overall_max)
    print("Files not reaching April 2026:", sum(1 for d in latest_dates if d < pd.Timestamp('2026-04-01')))
    # Let's count how many dates there are in 2026
    df_rel = pd.read_csv('.tmp/5y_data/RELIANCE_5Y.csv')
    df_rel['date'] = pd.to_datetime(df_rel['FH_TIMESTAMP'], errors='coerce')
    counts = df_rel[df_rel['date'].dt.year == 2026]['date'].dt.month.value_counts().sort_index()
    print("\nRELIANCE 2026 month counts:")
    print(counts)
