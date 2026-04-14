from nsefin import nse
from datetime import date
df = nse.get_fno_bhav_copy(date=date(2026, 4, 1))
if df is not None:
    futures = df[(df['symbol'] == 'RELIANCE') & (df['category'] == 'STF')]
    print(futures[['date', 'category', 'symbol', 'expiry', 'strike', 'right']])
else:
    print("No data returned!")
