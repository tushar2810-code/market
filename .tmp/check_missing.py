import os
import sys
# Add parent dir to path to find fno_utils if needed, assuming picking up from execution dir context
sys.path.append('/Users/tushar/Documents/Antigravity FNO/execution')
from fno_utils import FNO_SYMBOLS

data_dir = '/Users/tushar/Documents/Antigravity FNO/.tmp/3y_data'
existing_files = os.listdir(data_dir)
downloaded_symbols = [f.replace('_3Y.csv', '') for f in existing_files if f.endswith('_3Y.csv')]

missing = set(FNO_SYMBOLS) - set(downloaded_symbols)
print(f"Total Symbols: {len(FNO_SYMBOLS)}")
print(f"Downloaded: {len(downloaded_symbols)}")
print(f"Missing: {len(missing)}")
print("Missing Symbols:", sorted(list(missing)))
