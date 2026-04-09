import pandas as pd
import os

f1 = '.tmp/3y_data_backup/ADANIENT_3Y.csv'
f2 = '.tmp/3y_data/ADANIENT_3Y.csv'

print("Merging ADANIENT files...")
dfs = []
try:
    if os.path.exists(f1): 
        d1 = pd.read_csv(f1)
        dfs.append(d1)
        print(f"  Backup (Year 1): {len(d1)} rows")
    
    if os.path.exists(f2): 
        d2 = pd.read_csv(f2)
        dfs.append(d2)
        print(f"  Current (Year 2): {len(d2)} rows")

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        # FH_TIMESTAMP formatted as '30-Jan-2025' or similar. 
        combined = combined.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
        combined.to_csv(f2, index=False)
        print(f"✓ Merged ADANIENT: {len(combined)} rows")
    else:
        print("No ADANIENT data found.")
except Exception as e:
    print(f"Merge error: {e}")
