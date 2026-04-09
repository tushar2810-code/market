import requests
import zipfile
import io
import pandas as pd
import os

def fetch_fno_universe():
    url = "https://api.shoonya.com/NFO_symbols.txt.zip"
    print(f"Downloading Master Contract from {url}...")
    
    try:
        r = requests.get(url)
        r.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # Usually contains 'NFO_symbols.txt'
            file_list = z.namelist()
            target_file = [f for f in file_list if 'NFO_symbols' in f][0]
            print(f"Extracting {target_file}...")
            
            with z.open(target_file) as f:
                # Read CSV/TXT
                # Shoonya format is comma separated
                df = pd.read_csv(f)
                
                # Check columns. Typically: Exchange,Token,LotSize,Symbol,TradingSymbol,Expiry,Instrument,OptionType,StrikePrice,TickSize
                # Shoonya Header often: Exchange,Token,LotSize,Symbol,TradingSymbol,Expiry,Instrument,OptionType,StrikePrice,TickSize
                # Or similar structure. Let's inspect or assume standard keys.
                # Standard Shoonya Keys: 'Exchange', 'Token', 'LotSize', 'Symbol', 'TradingSymbol', 'Expiry', 'Instrument', 'OptionType', 'StrikePrice', 'TickSize'
                
                # Filter for Futures Stocks
                # Instrument == 'FUTSTK'
                if 'Instrument' in df.columns:
                    fno_stocks = df[df['Instrument'] == 'FUTSTK']
                elif 'InstrumentName' in df.columns: # Sometimes header varies
                     fno_stocks = df[df['InstrumentName'] == 'FUTSTK']
                else:
                    # Fallback: Check first row to guess
                    print("Unknown columns:", df.columns)
                    return

                # Get unique Symbols
                # Column 'Symbol' usually contains the underlying name e.g. 'RELIANCE'
                unique_symbols = sorted(fno_stocks['Symbol'].unique().tolist())
                
                # Filter out Test symbols
                unique_symbols = [s for s in unique_symbols if 'NSETEST' not in s]
                
                print(f"Found {len(unique_symbols)} FNO Stocks.")
                
                # Save to .tmp
                output_dir = ".tmp"
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                output_path = os.path.join(output_dir, "FNO_universe_jan.csv")
                pd.DataFrame(unique_symbols, columns=["Symbol"]).to_csv(output_path, index=False)
                print(f"Saved list to {output_path}")
                
                # Update fno_utils.py
                update_utils_file(unique_symbols)

    except Exception as e:
        print(f"Error fetching universe: {e}")

def update_utils_file(symbols):
    utils_path = "execution/fno_utils.py"
    
    # Format the list string
    # We want it to look like:
    # FNO_SYMBOLS = [
    #     "AARTIIND", "ABB", ...
    # ]
    
    list_str = "FNO_SYMBOLS = [\n"
    line = "    "
    for i, sym in enumerate(symbols):
        line += f'"{sym}", '
        if (i + 1) % 8 == 0: # Wrap every 8 items
            list_str += line + "\n"
            line = "    "
    
    if line.strip():
        list_str += line + "\n"
    list_str += "]\n"

    content = f"""# List of liquid FNO stocks (Auto-generated)
# Count: {len(symbols)}
{list_str}
def clean_symbol(symbol):
    \"\"\"Ensure symbol is uppercase.\"\"\"
    return symbol.upper()
"""
    
    with open(utils_path, "w") as f:
        f.write(content)
    print(f"Updated {utils_path}")

if __name__ == "__main__":
    fetch_fno_universe()
