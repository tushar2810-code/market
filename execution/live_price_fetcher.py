import logging
try:
    from nsepython import nse_quote, nse_quote_meta
except ImportError:
    pass

import yfinance as yf

logger = logging.getLogger(__name__)

class LivePriceFetcher:
    """
    Alternative to ShoonyaClient for fetching live prices.
    Attempts to fetch live FNO/Spot prices from NSE India (via nsepython).
    Falls back to Yahoo Finance (yfinance) for spot prices if NSE blocks the request.
    """
    
    def __init__(self):
        logger.info("Initializing LivePriceFetcher (NSE/YFinance Fallback)")

    def get_fno_price(self, symbol):
        """
        Attempts to fetch live price in this order:
        1. NSE FNO (Futures)
        2. NSE Spot (Equity)
        3. YFinance Spot
        """
        # Try nsepython for derivatives first
        try:
            # Note: NSE India sometimes blocks automated requests and returns empty data
            data = nse_quote(symbol)
            if data and 'stocks' in data and len(data['stocks']) > 0:
                # Find current month futures
                futs = [x for x in data['stocks'] if x['metadata']['instrumentType'] in ['Stock Futures', 'FUTSTK']]
                if futs:
                    return float(futs[0]['metadata']['lastPrice'])
            
            # If futures fail, try spot from nsepython
            if data and 'priceInfo' in data and 'lastPrice' in data['priceInfo']:
                return float(data['priceInfo']['lastPrice'])
        except Exception as e:
            logger.debug(f"NSEPython fetch failed for {symbol}: {e}")

        # Fallback to yfinance (Spot Price)
        try:
            yf_symbol = f"{symbol}.NS"
            ticker = yf.Ticker(yf_symbol)
            
            # Use fast info or history for the latest spot price
            # info['currentPrice'] is sometimes missing during market hours
            todays_data = ticker.history(period='1d')
            if not todays_data.empty:
                return float(todays_data['Close'].iloc[-1])
            
            if 'currentPrice' in ticker.info:
                return float(ticker.info['currentPrice'])
        except Exception as e:
            logger.error(f"YFinance fallback failed for {symbol}: {e}")
            
        return None

if __name__ == "__main__":
    fetcher = LivePriceFetcher()
    for sym in ["SUNPHARMA", "RELIANCE"]:
        p = fetcher.get_fno_price(sym)
        print(f"{sym} Live Price: {p}")
