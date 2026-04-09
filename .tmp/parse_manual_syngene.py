import pandas as pd
import os

RAW_FILE = '.tmp/syngene_raw.txt'
OUTPUT_FILE = '.tmp/3y_data/SYNGENE_3Y.csv'

def parse():
    if not os.path.exists(RAW_FILE):
        print("Raw file not found")
        return

    # User provided tab/space separated data with same cols as Swiggy
    col_names = [
        "Date", "Expiry", "OptionType", "Strike", "Open", "High", "Low", "Close", "Last", "Settle", 
        "Volume", "Value", "Premium", "OI", "ChangeOI"
    ]
    
    try:
        # Try tab separator first
        df = pd.read_csv(RAW_FILE, sep='\t', names=col_names, header=None)
        
        if len(df.columns) < 15:
             # Fallback
             df = pd.read_csv(RAW_FILE, delim_whitespace=True, names=col_names, header=None)

        # Clean numeric columns (remove commas)
        for col in ["Volume", "Value", "Premium", "OI", "ChangeOI"]:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        final_df = pd.DataFrame()
        final_df['FH_SYMBOL'] = 'SYNGENE'
        final_df['FH_EXPIRY_DT'] = df['Expiry']
        final_df['FH_TIMESTAMP'] = df['Date']
        final_df['FH_OPENING_PRICE'] = df['Open']
        final_df['FH_TRADE_HIGH_PRICE'] = df['High']
        final_df['FH_TRADE_LOW_PRICE'] = df['Low']
        final_df['FH_CLOSING_PRICE'] = df['Close']
        final_df['FH_LAST_TRADED_PRICE'] = df['Last']
        final_df['FH_SETTLE_PRICE'] = df['Settle']
        final_df['FH_TOT_TRADED_QTY'] = df['Volume']
        final_df['FH_TOT_TRADED_VAL'] = df['Value'] * 100000 
        final_df['FH_OPEN_INT'] = df['OI']
        final_df['FH_CHANGE_IN_OI'] = df['ChangeOI']
        
        # SYNGENE Lot size (approx check or default)
        final_df['FH_MARKET_LOT'] = 500 # Default/Placeholder
        
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"Parsed SYNGENE data: {len(final_df)} rows saved to {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"Parse error: {e}")

if __name__ == "__main__":
    parse()
