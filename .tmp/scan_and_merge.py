import os
import pandas as pd
import glob
import re

MANUAL_DIR = '.tmp/manual_data'
MASTER_DIR = '.tmp/3y_data'

def run_merge():
    # 1. Scan for files
    if not os.path.exists(MANUAL_DIR):
        print(f"Directory {MANUAL_DIR} not found. please create it.")
        return

    files = glob.glob(os.path.join(MANUAL_DIR, "*.csv"))
    if not files:
        print(f"No CSV files found in {MANUAL_DIR}.")
        return

    print(f"Found {len(files)} manual files. Processing...")
    
    merged_count = 0
    
    for fpath in files:
        fname = os.path.basename(fpath)
        # Expected format: SYMBOL_Y2.csv or SYMBOL_Year2.csv or just SYMBOL.csv
        # Extract symbol
        # Simple heuristic: Split by underscore, take first part.
        symbol = fname.split('_')[0].upper()
        
        target_file = os.path.join(MASTER_DIR, f"{symbol}_3Y.csv")
        
        try:
            new_df = pd.read_csv(fpath)
            if new_df.empty:
                print(f"Skipping empty file: {fname}")
                continue
                
            combined = new_df
            
            # Load existing if any
            if os.path.exists(target_file):
                old_df = pd.read_csv(target_file)
                combined = pd.concat([old_df, new_df], ignore_index=True)
                
            # Dedup
            if 'FH_TIMESTAMP' in combined.columns and 'FH_EXPIRY_DT' in combined.columns:
                combined = combined.drop_duplicates(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT', 'FH_SYMBOL'])
            elif 'Date' in combined.columns and 'Expiry' in combined.columns:
                combined = combined.drop_duplicates(subset=['Date', 'Expiry', 'Symbol'])
                
            # Save
            combined.to_csv(target_file, index=False)
            print(f"✓ Merged {symbol}: Total {len(combined)} rows")
            merged_count += 1
            
        except Exception as e:
            print(f"Error merging {fname}: {e}")

    print(f"\nSuccessfully merged {merged_count} symbols.")

if __name__ == "__main__":
    run_merge()
