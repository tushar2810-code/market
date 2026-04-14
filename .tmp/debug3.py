import pandas as pd
df = pd.read_csv('.tmp/3y_data/360ONE_3Y.csv')
date_obj1 = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
print("Format1 nulls:", date_obj1.isna().sum())
date_obj2 = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%y', errors='coerce')
print("Format2 nulls:", date_obj2.isna().sum())
date_obj3 = pd.to_datetime(df['FH_TIMESTAMP'], errors='coerce')
print("Format3 nulls:", date_obj3.isna().sum())

# Let's see the duplicate drop!
df['date_obj'] = pd.to_datetime(df['FH_TIMESTAMP'], errors='coerce')
df['exp_obj'] = pd.to_datetime(df['FH_EXPIRY_DT'], errors='coerce')
dedup = df.sort_values(by=['date_obj', 'exp_obj']).drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT'], keep='last')
print("Dedup shape:", dedup.shape)
