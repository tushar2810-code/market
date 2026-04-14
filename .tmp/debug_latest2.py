import pandas as pd
df_rel = pd.read_csv('.tmp/5y_data/RELIANCE_5Y.csv')
df_rel['date'] = pd.to_datetime(df_rel['FH_TIMESTAMP'], errors='coerce')
counts = df_rel[df_rel['date'].dt.year == 2025]['date'].dt.month.value_counts().sort_index()
print("\nRELIANCE 2025 month counts:")
print(counts)
