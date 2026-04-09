
import sys
import os

# Add execution dir to path
sys.path.append(os.path.join(os.getcwd(), 'execution'))

from sync_fno_data import run_sync

symbols = [
    'IOC', 'SBIN', 'DRREDDY', 'SUNPHARMA', 'PFC', 'CHOLAFIN', 'TCS', 'ONGC', 'IRFC', 
    'JINDALSTEL', 'HINDUNILVR', 'MARUTI', 'SHREECEM', 'ICICIPRULI', 'MUTHOOTFIN', 
    'MARICO', 'HINDPETRO', 'ABB', 'POLYCAB', 'AXISBANK', 'INFY', 'SBILIFE', 
    'COALINDIA', 'NESTLEIND', 'CIPLA', 'PNB', 'HAVELLS', 'ITC', 'ICICIBANK', 
    'ULTRACEMCO', 'RECLTD', 'BANKBARODA', 'AMBUJACEM', 'DABUR', 'SIEMENS', 
    'LUPIN', 'SHRIRAMFIN', 'BRITANNIA', 'CDSL', 'MANAPPURAM', 'BEL', 'HDFCLIFE', 
    'KOTAKBANK', 'TECHM', 'TATASTEEL', 'BPCL', 'HEROMOTOCO', 'KFINTECH', 'M&M', 
    'WIPRO', 'JSWSTEEL', 'BAJAJFINSV', 'HDFCBANK', 'VEDL', 'NMDC', 'CAMS', 
    'BAJFINANCE', 'BAJAJ-AUTO', 'TVSMOTOR', 'HCLTECH', 'HINDALCO', 'OIL', 'HAL'
]

if __name__ == "__main__":
    print(f"Starting sync for {len(symbols)} symbols...")
    run_sync(symbols=symbols, max_workers=4)
