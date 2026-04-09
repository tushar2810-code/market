# FNO Data Acquisition Directive

## Goal
Maintain a complete, error-free, 3-year sliding window of historical FNO data for all 208 liquid scrips.

## Inputs
- **Symbols List**: `execution/fno_utils.py`
- **Source**: NSE India Website (Historical Derivatives API)

## Workflow
1. **Assessment**: Scan `.tmp/3y_data` for missing or stale files.
2. **Acquisition**: 
   - Use `execution/sync_fno_data.py` to fetch data.
   - Strategy: Parallel Playwright browsers (Headless Firefox).
   - Frequency: Monthly or On-Demand.
3. **Validation**: Check for date gaps > 10 days and row counts.
4. **Report**: Generate a health report.

## Critical constraints & Edge Cases
### 1. API Limitations
- **Blocking**: NSE aggressively blocks automated requests. Headers (User-Agent, Referer) and session wram-up are required.
- **Invalid Content**: The API frequently returns generic HTML error pages or JSON error dicts instead of data lists. The script must handle these gracefully (retry/skip).

### 2. Specific Symbol Quirks
- **TORNTPOWER**: Historical data before Jan 2025 is **unavailable** via the standard API. Do not attempt infinite retries for 2023/2024; accept partial data.
- **New Listings**: Symbols like `SWIGGY`, `WAAREEENER`, `JIOFIN` are recent entrants. Partial data (< 3 years) is expected and correct.
- **Re-entrants**: `SUZLON` may have gaps due to periods of being out of FNO ban/list.

## Tools
- `execution/sync_fno_data.py`: The unified orchestrator. Handles downloading, merging, and verification.
