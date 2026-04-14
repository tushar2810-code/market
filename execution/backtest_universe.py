
import pandas as pd
import numpy as np
import glob
import os
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backtest_universe.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DATA_DIR = ".tmp/3y_data"
# We'll save a full CSV with every single symbol found
OUTPUT_FILE = "universe_backtest_results.csv"

def analyze_symbol(filepath):
    symbol = os.path.basename(filepath).replace("_5Y.csv", "")
    try:
        df = pd.read_csv(filepath)
        
        # Standardize columns
        df.columns = [c.strip() for c in df.columns]
        
        # Parse Dates
        for col in ['FH_EXPIRY_DT', 'FH_TIMESTAMP']:
            df[col] = pd.to_datetime(df[col], format='%d-%b-%Y', errors='coerce')
            
        df = df.dropna(subset=['FH_EXPIRY_DT', 'FH_TIMESTAMP'])
        df = df.sort_values(['FH_TIMESTAMP', 'FH_EXPIRY_DT'])
        
        unique_expiries = sorted(df['FH_EXPIRY_DT'].dropna().unique())
        
        if len(unique_expiries) < 2:
            return None

        cycle_metrics = []

        for i in range(len(unique_expiries) - 1):
            near_expiry = unique_expiries[i]
            far_expiry = unique_expiries[i+1]
            
            prev_expiry = unique_expiries[i-1] if i > 0 else df['FH_TIMESTAMP'].min()
            
            mask = (df['FH_TIMESTAMP'] <= near_expiry) & (df['FH_TIMESTAMP'] > prev_expiry)
            period_data = df[mask]
            
            if period_data.empty:
                continue

            # Join Near and Far
            near_data = period_data[period_data['FH_EXPIRY_DT'] == near_expiry].set_index('FH_TIMESTAMP')
            far_data = period_data[period_data['FH_EXPIRY_DT'] == far_expiry].set_index('FH_TIMESTAMP')
            
            combined = near_data[['FH_CLOSING_PRICE', 'FH_MARKET_LOT']].join(
                far_data[['FH_CLOSING_PRICE']], 
                how='inner', 
                lsuffix='_NEAR', 
                rsuffix='_FAR'
            )
            
            # --- 1. DATA SANITIZATION ---
            # Drop rows where price is ≤ 1 (Data Gap/Error)
            combined = combined[
                (combined['FH_CLOSING_PRICE_NEAR'] > 1) & 
                (combined['FH_CLOSING_PRICE_FAR'] > 1)
            ].copy()

            if combined.empty:
                continue
                
            # --- 2. SPLIT/DIVIDEND HANDLING (Regime Detection) ---
            # Detect Price Shocks > 25% (Splits/Bonus) to reset Rolling Stats
            combined['PRICE_PCT_CHANGE'] = combined['FH_CLOSING_PRICE_NEAR'].pct_change().abs()
            combined['IS_SPLIT'] = combined['PRICE_PCT_CHANGE'] > 0.25
            combined['REGIME_ID'] = combined['IS_SPLIT'].cumsum()
            
            combined['SPREAD'] = combined['FH_CLOSING_PRICE_FAR'] - combined['FH_CLOSING_PRICE_NEAR']
            combined['DTE'] = (near_expiry - combined.index).days
            
            # --- 3. ROLLING STATS (Resets on Regime Change) ---
            window = 20
            
            combined['ROLLING_MEAN'] = combined.groupby('REGIME_ID')['SPREAD'].transform(
                lambda x: x.rolling(window=window, min_periods=10).mean()
            )
            combined['ROLLING_STD'] = combined.groupby('REGIME_ID')['SPREAD'].transform(
                lambda x: x.rolling(window=window, min_periods=10).std()
            )
            
            # Z-Score
            combined['Z_SCORE'] = (combined['SPREAD'] - combined['ROLLING_MEAN']) / combined['ROLLING_STD'].replace(0, np.nan)
            
            combined = combined.dropna(subset=['Z_SCORE', 'ROLLING_STD', 'FH_MARKET_LOT'])
            
            if combined.empty:
                continue

            # --- 4. OPPORTUNITY IDENTIFICATION ---
            # Events: Z < -2.0 (Significant Deviation)
            combined['IS_DEVIATED'] = combined['Z_SCORE'] < -2.0
            combined['EVENT_ID'] = (combined['IS_DEVIATED'] != combined['IS_DEVIATED'].shift()).cumsum()
            
            deviated_events = combined[combined['IS_DEVIATED']]
            
            if deviated_events.empty:
                continue
                
            # Analyze each event
            event_stats = deviated_events.groupby('EVENT_ID').agg({
                'Z_SCORE': 'min', # Deepest Point
                'ROLLING_STD': 'mean', # Volatility
                'FH_MARKET_LOT': 'first', # Catch split-adjusted lot size
                'DTE': 'first'
            })
            
            for _, row in event_stats.iterrows():
                peak_z = row['Z_SCORE']
                sigma_in_points = row['ROLLING_STD']
                lot_size = row['FH_MARKET_LOT']
                
                # SIGMA VALUE (INR)
                sigma_value_inr = sigma_in_points * lot_size
                
                # REALISTIC POTENTIAL (Peak -> -1.0 Sigma)
                if peak_z > -1.0: 
                    continue
                    
                captured_z_units = abs(peak_z - (-1.0))
                potential_gain_inr = captured_z_units * sigma_value_inr
                
                cycle_metrics.append({
                    'peak_z': peak_z,
                    'sigma_value': sigma_value_inr,
                    'potential_gain': potential_gain_inr
                })
            
        if not cycle_metrics:
            return None
            
        metrics_df = pd.DataFrame(cycle_metrics)
        
        return {
            'Symbol': symbol,
            'Median_Peak_Z': metrics_df['peak_z'].median(), # Median "Bottom"
            'Avg_Sigma_Value': metrics_df['sigma_value'].mean(), # Value of 1.0 Sigma
            'Avg_Potential_Gain': metrics_df['potential_gain'].mean(), # Avg Trade Potential
            'Max_Potential_Gain': metrics_df['potential_gain'].max(),
            'Event_Count': len(metrics_df), # Reliability
            'Avg_Lot_Size': metrics_df['sigma_value'].mean()
        }

    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        return None

def main():
    files = glob.glob(os.path.join(DATA_DIR, "*_5Y.csv"))
    logger.info(f"Found {len(files)} files. Starting Split-Adjusted Sigma Analysis...")
    
    results = []
    
    with ProcessPoolExecutor(max_workers=8) as executor:
        future_to_file = {executor.submit(analyze_symbol, f): f for f in files}
        
        count = 0
        for future in as_completed(future_to_file):
            res = future.result()
            if res:
                results.append(res)
            count += 1
            if count % 20 == 0:
                logger.info(f"Processed {count}/{len(files)}...")

    final_df = pd.DataFrame(results)
    
    if not final_df.empty:
        # Sort by Potential Gain
        final_df = final_df.sort_values('Avg_Potential_Gain', ascending=False)
        
        # Rounding
        cols = ['Median_Peak_Z', 'Avg_Sigma_Value', 'Avg_Potential_Gain', 'Max_Potential_Gain', 'Event_Count']
        final_df[cols] = final_df[cols].round(1)
        
        # Save FULL list
        final_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Saved results to {OUTPUT_FILE}")
        
        print("\nTOP 20 SIGMA OPPORTUNITIES (Split-Adjusted):")
        print(final_df[['Symbol', 'Median_Peak_Z', 'Avg_Sigma_Value', 'Avg_Potential_Gain', 'Event_Count']].head(20).to_string(index=False))
    else:
        logger.error("No results generated.")

if __name__ == "__main__":
    main()
