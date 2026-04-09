"""
Update 3Y data using Yahoo Finance spot prices.

Fetches 3Y of daily OHLCV for all FNO symbols from Yahoo Finance (.NS suffix).
Saves in the same CSV format as the NSE futures CSVs so all scanners work unchanged.

Spot price ≈ near-month futures within 0.3-0.5% (basis). This is acceptable for
Z-score computation — the spread and its mean/std are computed consistently.

Lot sizes are preserved from the existing CSV files. If no existing file, lot=1.

Usage:
    python3 execution/update_data_yfinance.py            # all FNO symbols
    python3 execution/update_data_yfinance.py --pairs    # proven pairs only (fast)
    python3 execution/update_data_yfinance.py --sym RELIANCE HDFCBANK
"""

import os
import sys
import argparse
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(".tmp/3y_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = (datetime.now() - timedelta(days=3*365+60)).strftime("%Y-%m-%d")  # 3Y + buffer
END_DATE   = datetime.now().strftime("%Y-%m-%d")

PROVEN_PAIR_SYMS = [
    'ULTRACEMCO','AMBUJACEM','HINDALCO','VEDL','LICHSGFIN','PFC',
    'IDFCFIRSTB','AUBANK','HCLTECH','PERSISTENT','GAIL','ONGC',
    'MARICO','TATACONSUM','SHREECEM','BPCL','IOC','ICICIBANK',
    'HDFCBANK','BANKBARODA','PNB','SBIN','NMDC','COALINDIA',
]

sys.path.append(os.path.dirname(__file__))
try:
    from fno_utils import FNO_SYMBOLS
except Exception:
    FNO_SYMBOLS = PROVEN_PAIR_SYMS


def get_existing_lot(symbol: str) -> int:
    """Extract the last known lot size from existing CSV. Falls back to 1."""
    path = DATA_DIR / f"{symbol}_3Y.csv"
    if not path.exists():
        return 1
    try:
        df = pd.read_csv(path, usecols=lambda c: c.strip() in ['FH_MARKET_LOT'])
        df.columns = [c.strip() for c in df.columns]
        if 'FH_MARKET_LOT' in df.columns:
            val = pd.to_numeric(df['FH_MARKET_LOT'], errors='coerce').replace(0, np.nan).dropna()
            if not val.empty:
                return int(val.iloc[-1])
    except Exception:
        pass
    return 1


def fetch_and_save(symbol: str) -> bool:
    """Download 3Y of yfinance data for one symbol and save as 3Y CSV."""
    ticker = f"{symbol}.NS"
    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE,
                         auto_adjust=True, progress=False)
        if df is None or df.empty or len(df) < 100:
            logger.warning(f"{symbol}: insufficient data ({len(df) if df is not None else 0} rows)")
            return False

        df = df.reset_index()
        # Flatten MultiIndex columns if present (yfinance 0.2+)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if c[1] == '' else c[0] for c in df.columns]

        df['FH_TIMESTAMP']     = pd.to_datetime(df['Date']).dt.strftime('%d-%b-%Y')
        df['FH_CLOSING_PRICE'] = pd.to_numeric(df['Close'], errors='coerce')
        df['FH_OPENING_PRICE'] = pd.to_numeric(df['Open'],  errors='coerce')
        df['FH_TRADE_HIGH_PRICE'] = pd.to_numeric(df['High'], errors='coerce')
        df['FH_TRADE_LOW_PRICE']  = pd.to_numeric(df['Low'],  errors='coerce')
        df['FH_TOT_TRADED_VAL']   = pd.to_numeric(df.get('Volume', 0), errors='coerce')
        df['FH_UNDERLYING_VALUE']  = df['FH_CLOSING_PRICE']  # spot IS underlying
        df['FH_INSTRUMENT']  = 'FUTSTK'
        df['FH_SYMBOL']      = symbol
        df['FH_EXPIRY_DT']   = ''          # not applicable for spot proxy
        df['SOURCE']         = 'YFINANCE'

        lot = get_existing_lot(symbol)
        df['FH_MARKET_LOT'] = lot

        out_cols = [
            'FH_TIMESTAMP','FH_SYMBOL','FH_INSTRUMENT','FH_EXPIRY_DT',
            'FH_CLOSING_PRICE','FH_OPENING_PRICE','FH_TRADE_HIGH_PRICE',
            'FH_TRADE_LOW_PRICE','FH_TOT_TRADED_VAL','FH_UNDERLYING_VALUE',
            'FH_MARKET_LOT','SOURCE',
        ]
        out = df[[c for c in out_cols if c in df.columns]].dropna(subset=['FH_CLOSING_PRICE'])
        out = out.sort_values('FH_TIMESTAMP')

        out_path = DATA_DIR / f"{symbol}_3Y.csv"
        out.to_csv(out_path, index=False)
        logger.info(f"{symbol}: {len(out)} rows  lot={lot}  → {out_path.name}")
        return True

    except Exception as e:
        logger.error(f"{symbol}: {e}")
        return False


def run(symbols: list) -> None:
    total = len(symbols)
    logger.info(f"Downloading {total} symbols from Yahoo Finance ({START_DATE} → {END_DATE})")

    # Batch download for speed (yfinance handles multiple tickers efficiently)
    batch_size = 50
    ok = 0
    for i in range(0, total, batch_size):
        batch = symbols[i:i+batch_size]
        tickers = " ".join(f"{s}.NS" for s in batch)
        logger.info(f"Batch {i//batch_size+1}: {batch}")
        try:
            raw = yf.download(tickers, start=START_DATE, end=END_DATE,
                              auto_adjust=True, progress=False, group_by='ticker')
        except Exception as e:
            logger.error(f"Batch download failed: {e} — falling back to individual")
            raw = None

        for sym in batch:
            try:
                if raw is not None and isinstance(raw.columns, pd.MultiIndex):
                    ticker = f"{sym}.NS"
                    if ticker in raw.columns.get_level_values(0):
                        df_sym = raw[ticker].dropna(how='all').reset_index()
                        df_sym.columns = [c[0] if isinstance(c, tuple) else c for c in df_sym.columns]
                    else:
                        df_sym = pd.DataFrame()
                else:
                    df_sym = pd.DataFrame()

                if df_sym.empty or len(df_sym) < 100:
                    # individual fallback
                    ok += 1 if fetch_and_save(sym) else 0
                    continue

                # Save using batch data
                df_sym['FH_TIMESTAMP'] = pd.to_datetime(df_sym['Date']).dt.strftime('%d-%b-%Y')
                df_sym['FH_CLOSING_PRICE'] = pd.to_numeric(df_sym['Close'], errors='coerce')
                df_sym['FH_OPENING_PRICE'] = pd.to_numeric(df_sym.get('Open', 0), errors='coerce')
                df_sym['FH_TRADE_HIGH_PRICE'] = pd.to_numeric(df_sym.get('High', 0), errors='coerce')
                df_sym['FH_TRADE_LOW_PRICE']  = pd.to_numeric(df_sym.get('Low', 0), errors='coerce')
                df_sym['FH_TOT_TRADED_VAL']   = pd.to_numeric(df_sym.get('Volume', 0), errors='coerce')
                df_sym['FH_UNDERLYING_VALUE']  = df_sym['FH_CLOSING_PRICE']
                df_sym['FH_INSTRUMENT'] = 'FUTSTK'
                df_sym['FH_SYMBOL']     = sym
                df_sym['FH_EXPIRY_DT']  = ''
                df_sym['FH_MARKET_LOT'] = get_existing_lot(sym)
                df_sym['SOURCE']        = 'YFINANCE'

                out_cols = [
                    'FH_TIMESTAMP','FH_SYMBOL','FH_INSTRUMENT','FH_EXPIRY_DT',
                    'FH_CLOSING_PRICE','FH_OPENING_PRICE','FH_TRADE_HIGH_PRICE',
                    'FH_TRADE_LOW_PRICE','FH_TOT_TRADED_VAL','FH_UNDERLYING_VALUE',
                    'FH_MARKET_LOT','SOURCE',
                ]
                out = df_sym[[c for c in out_cols if c in df_sym.columns]].dropna(subset=['FH_CLOSING_PRICE'])
                out = out.sort_values('FH_TIMESTAMP')
                out_path = DATA_DIR / f"{sym}_3Y.csv"
                out.to_csv(out_path, index=False)
                logger.info(f"{sym}: {len(out)} rows  lot={get_existing_lot(sym)}")
                ok += 1
            except Exception as e:
                logger.warning(f"{sym}: batch parse failed ({e}), trying individual")
                ok += 1 if fetch_and_save(sym) else 0

    logger.info(f"\nDone: {ok}/{total} symbols updated in {DATA_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pairs', action='store_true', help='Proven pairs only (24 symbols, fast)')
    parser.add_argument('--sym', nargs='+', help='Specific symbols to update')
    args = parser.parse_args()

    if args.sym:
        syms = [s.upper() for s in args.sym]
    elif args.pairs:
        syms = PROVEN_PAIR_SYMS
    else:
        syms = list(dict.fromkeys(FNO_SYMBOLS + PROVEN_PAIR_SYMS))  # dedup, preserve order

    run(syms)
