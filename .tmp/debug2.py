import pandas as pd

df_3y = pd.read_csv('.tmp/3y_data/360ONE_3Y.csv')
if 'FH_TIMESTAMP' not in df_3y.columns:
    print("FH_TIMESTAMP missing")

import os
print("Does 5Y exist?", os.path.exists('.tmp/5y_data/360ONE_5Y.csv'))
if os.path.exists('.tmp/5y_data/360ONE_5Y.csv'):
    df_5y = pd.read_csv('.tmp/5y_data/360ONE_5Y.csv')
    print("5Y shape:", df_5y.shape)
