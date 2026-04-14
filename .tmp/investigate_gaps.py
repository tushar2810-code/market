import pandas as pd
df = pd.read_csv('.tmp/5y_data/SAMMAANCAP_5Y.csv')
dates = pd.to_datetime(df['FH_TIMESTAMP'], format='mixed', dayfirst=True, errors='coerce').dropna()
ym = list(dates.dt.to_period('M').unique().astype(str))
print('YM for SAMMAANCAP:', ym)

