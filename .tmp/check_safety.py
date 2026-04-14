import pandas as pd
import numpy as np
import yfinance as yf
import math
import warnings
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

warnings.filterwarnings('ignore')

target_pairs = [
    ("BAJAJFINSV", "MUTHOOTFIN"),
    ("KOTAKBANK", "BANDHANBNK"),
    ("BAJAJFINSV", "M&MFIN"),
    ("M&MFIN", "MUTHOOTFIN"),
    ("BANDHANBNK", "IDFCFIRSTB"),
    ("DIXON", "POLYCAB"),
    ("BHEL", "SIEMENS")
]

def load_continuous(sym):
    df = pd.read_csv(f'.tmp/5y_data/{sym}_5Y.csv')
    df.columns = [c.strip() for c in df.columns]
    df['FH_TIMESTAMP'] = pd.to_datetime(df['FH_TIMESTAMP'], format='%d-%b-%Y', errors='coerce')
    df['FH_EXPIRY_DT'] = pd.to_datetime(df['FH_EXPIRY_DT'], format='%d-%b-%Y', errors='coerce')
    df = df.dropna(subset=['FH_TIMESTAMP', 'FH_EXPIRY_DT']).sort_values('FH_TIMESTAMP')
    c = df.loc[df.groupby('FH_TIMESTAMP')['FH_EXPIRY_DT'].idxmin()].copy()
    c['FH_MARKET_LOT'] = c['FH_MARKET_LOT'].replace(0, np.nan).ffill().bfill()
    return c[['FH_TIMESTAMP', 'FH_CLOSING_PRICE', 'FH_MARKET_LOT']].set_index('FH_TIMESTAMP')

def _ou_half_life(spread: np.ndarray) -> float:
    y = spread[1:] - spread[:-1]
    x = spread[:-1]
    try:
        res   = OLS(y, add_constant(x)).fit()
        theta = -res.params[1]
        return math.log(2) / theta if theta > 0 else float('inf')
    except Exception:
        return float('inf')

print(f"{'PAIR':<22} | {'ADF(1Y)':<8} | {'COINT(1Y)':<9} | {'COINT(6M)':<9} | {'HL(d)':<6} | {'COR(20)':<7} | {'DECISION'}")
print("-" * 85)

for a, b in target_pairs:
    try:
        c_a = load_continuous(a)
        c_b = load_continuous(b)
        m = c_a.join(c_b, how='inner', lsuffix='_a', rsuffix='_b')
        m['spread'] = (m['FH_CLOSING_PRICE_a'] * m['FH_MARKET_LOT_a'] - 
                       m['FH_CLOSING_PRICE_b'] * m['FH_MARKET_LOT_b'])
        
        sub_1y = m.tail(252)
        try:
            _, adf_p, *_ = adfuller(sub_1y['spread'].dropna().values, maxlag=5)
        except:
            adf_p = 1.0

        try:
            _, c1y, _ = coint(sub_1y['FH_CLOSING_PRICE_a'], sub_1y['FH_CLOSING_PRICE_b'])
        except:
            c1y = 1.0

        sub_6m = m.tail(126)
        try:
            _, c6m, _ = coint(sub_6m['FH_CLOSING_PRICE_a'], sub_6m['FH_CLOSING_PRICE_b'])
        except:
            c6m = 1.0

        hl = _ou_half_life(m['spread'].dropna().values)

        ret_a = m['FH_CLOSING_PRICE_a'].pct_change()
        ret_b = m['FH_CLOSING_PRICE_b'].pct_change()
        corr_20d = ret_a.tail(20).corr(ret_b.tail(20))

        # DECISION LOGIC matches scan_valid_signals.py thresholds
        ADF_P_MAX = 0.10
        MAX_HALF_LIFE = 50
        MIN_CORR = 0.40
        recent_ok = c6m < 0.10
        spread_stat = adf_p < ADF_P_MAX
        any_coint = c1y < 0.15 or recent_ok or spread_stat
        
        rej = []
        if not any_coint: rej.append('No Coint')
        if hl > MAX_HALF_LIFE: rej.append(f'Slow HL')
        if corr_20d < MIN_CORR: rej.append(f'Low Corr')
        
        decision = 'PASS (SAFEST)' if not rej else 'REJECT: ' + ', '.join(rej)
        
        name = a[:10] + "/" + b[:10]
        print(f"{name:<22} | {adf_p:<8.3f} | {c1y:<9.3f} | {c6m:<9.3f} | {hl:<6.1f} | {corr_20d:<7.2f} | {decision}")
    except Exception as e:
        print(f"{a}/{b} ERROR: {e}")
