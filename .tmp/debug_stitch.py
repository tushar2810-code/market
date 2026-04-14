import pandas as pd

df = pd.read_csv('.tmp/3y_data/360ONE_3Y.csv')
print("3Y shape:", df.shape)

combined = pd.concat([df], ignore_index=True)
combined.columns = [c.strip() for c in combined.columns]
combined['date_obj'] = pd.to_datetime(combined['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
combined = combined.dropna(subset=['date_obj'])
print("After dropna date:", combined.shape)

combined['exp_obj'] = pd.to_datetime(combined['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
combined = combined.sort_values(by=['date_obj', 'exp_obj']).drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT'], keep='last')
print("After dedup:", combined.shape)
