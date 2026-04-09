
import csv
import os
from datetime import datetime

LOG_FILE = ".tmp/active_trades_log.csv"

def log_trade(pair, timeframe, z_score, ratio, signal_type):
    # Ensure file exists with headers
    file_exists = os.path.exists(LOG_FILE)
    
    with open(LOG_FILE, 'a', newline='') as csvfile:
        fieldnames = ['Timestamp', 'Pair', 'Timeframe', 'Z_Score', 'Ratio', 'Signal']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            
        writer.writerow({
            'Timestamp': datetime.now().isoformat(),
            'Pair': pair,
            'Timeframe': f"{timeframe} Days",
            'Z_Score': f"{z_score:.2f}",
            'Ratio': f"{ratio:.4f}",
            'Signal': signal_type
        })
    print(f"Logged active trade: {pair} ({signal_type}, Z={z_score:.2f})")

if __name__ == "__main__":
    # Test
    log_trade("TEST_PAIR", 20, -2.5, 0.5, "BUY TEST")
