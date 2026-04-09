# Calendar Spread Trading

## Goal
Identify and trade calendar spreads where:
1.  **Near Month Future** is at a **Premium** (> Spot).
2.  **Far Month Future** is at a **Discount** (< Spot).
This "Sell Premium / Buy Discount" strategy exploits the convergence of these prices.

## Inputs
- `SYMBOLS`: List of FNO stocks to scan.

## Tools/Scripts

### 1. Scan for Opportunities
Run this to find potential trades matching the criteria.
```bash
python execution/scan_calendar_spreads.py --threshold 0.5
```
- **Output**: List of stocks where Near Premium > 0 and Far Premium < 0 (or close to it).
- Saves results to `.tmp/calendar_spreads.csv`.

### 2. Monitor & Alert
Run this to watch a specific spread.
```bash
python execution/monitor_spread.py <SYMBOL> <NEAR_MONTH> <FAR_MONTH> <MEAN> <STD_DEV>
```

### 3. Log Trade
Log entry and exit of trades.
```bash
python execution/log_trade.py ENTRY <SYMBOL> <SPREAD> <QUANTITY>
python execution/log_trade.py EXIT <SYMBOL> <SPREAD> <QUANTITY>
```

## Strategy
- **Condition**: Backwardation in the Far Month relative to Spot, while Near Month is in Contango (or higher Premium).
- **Execution**:
    - **Sell** Near Month Future (Capture Premium).
    - **Buy** Far Month Future (Capture Discount).
- **Profit Source**: Convergence of Near Premium to 0 and Far Discount to 0 (or narrowing of the spread).

## Domain Knowledge
- **Expiry Rule**: The expiry of a stock derivative is ALWAYS on the **last Tuesday** of the month (NOT Thursday). All time-to-expiry (TTE) and settlement logic must use the last Tuesday.
