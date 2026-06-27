# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs + ATR/ADR Filter
# =============================================================================
import pandas as pd
from pathlib import Path
from datetime import date

# ────────────────────────────────────────────────
#   CONFIG
# ────────────────────────────────────────────────
INPUT_PATH   = Path("RS_Data/rs_stocks.csv")
OUTPUT_PATH  = Path("RS_Data/RS80_Price30_within27pct_52wh.csv")
ARCHIVE_DIR  = Path("52wh")

RS_THRESHOLD    = 80.0
PRICE_THRESHOLD = 25.0
MAX_PCT_BELOW   = 27        # You can change this
MIN_AVGVOL10    = 400_000

DEBUG_TICKER = "VSH"        # Change for testing


# NEW: ATR & ADR Filter
MIN_ATR = 2.5
MIN_ADR = 2.5
# ────────────────────────────────────────────────

def parse_volume(x):
    if pd.isna(x):
        return None
    x = str(x).strip().upper()
    if x.endswith('K'):
        return float(x[:-1]) * 1_000
    if x.endswith('M'):
        return float(x[:-1]) * 1_000_000
    if x.endswith('B'):
        return float(x[:-1]) * 1_000_000_000
    return float(x)


def debug_ticker(df, ticker):
    """Enhanced debug"""
    row = df[df['Ticker'] == ticker]
    if row.empty:
        print(f"\nDEBUG: {ticker} → NOT FOUND")
        return
    
    row = row.iloc[0]
    print(f"\n=== DEBUG: {ticker} ===")
    print(f"Price           : ${row['Price']:,.2f}")
    print(f"52W High        : ${row['52WKH']:,.2f}")
    print(f"% from 52WH     : {row.get('%_From_52WKH', 'N/A')}%")
    print(f"RS Percentile   : {row['RS Percentile']:.1f}")
    print(f"10d Avg Volume  : {row['AvgVol10']:,.0f}")
    print(f"SMA50           : ${row.get('SMA50', 'N/A'):,.2f}")
    print(f"SMA200          : ${row.get('SMA200', 'N/A'):,.2f}")
    
    if pd.notna(row.get('SMA50')) and pd.notna(row.get('SMA200')):
        print(f"SMA50 > SMA200  : {row['SMA50'] > row['SMA200']}")
    if pd.notna(row.get('Price')) and pd.notna(row.get('SMA50')):
        print(f"Price > SMA50   : {row['Price'] > row['SMA50']}")
    if pd.notna(row.get('Price')) and pd.notna(row.get('SMA200')):
        print(f"Price > SMA200  : {row['Price'] > row['SMA200']}")
    
    print("-" * 60)

def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    # Convert numeric columns
    numeric_cols = ['Price', '52WKH', 'RS Percentile', 'AvgVol10', 'ATR', 'ADR',
                    'SMA50', 'SMA200']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop invalid rows
    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile', 'AvgVol10'])

    # Calculate % from 52-week high
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # === MAIN FILTER with ATR/ADR ===
    mask = (
        (df['%_From_52WKH'] >= -MAX_PCT_BELOW) &     
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['AvgVol10'] >= MIN_AVGVOL10)
    )

    # Apply ATR & ADR filter only on Stocks (not ETFs)
    stock_mask = (df['Sector'] != 'ETF') & (df['Sector'].notna())
    df.loc[stock_mask, 'Passes_ATR_ADR'] = (
        (df.loc[stock_mask, 'ATR'] >= MIN_ATR) & 
        (df.loc[stock_mask, 'ADR'] >= MIN_ADR)
    )
    # ETFs automatically pass ATR/ADR filter
    df['Passes_ATR_ADR'] = df['Passes_ATR_ADR'].fillna(True)

    # Final combined filter
    mask = mask & df['Passes_ATR_ADR']

    filtered = df[mask].copy()
    
    # Debug specific ticker
    debug_ticker(df, DEBUG_TICKER)        # checks original loaded data

    print(f"\nAfter filters:")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • Within {MAX_PCT_BELOW}% of 52-week high")
    print(f"  • 10d Avg Vol ≥ {MIN_AVGVOL10:,}")
    print(f"  • ATR ≥ {MIN_ATR}  AND  ADR ≥ {MIN_ADR}  (Stocks only)")
    print(f"→ {len(filtered):,} stocks remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Desired columns
    # Keep 52WH scanner output aligned with latest RS_Data/rs_stocks.csv visibility fields.
    # %_From_52WKH is scanner-specific and is appended at the end for 52WH context.
    desired = [
        'Rank', 'Ticker', 'Price', 'DVol', 'Sector', 'Industry',
        'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'ATR', 'ADR', 'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'MCAP',
        'IPO', 'SMA50', 'SMA200', 'SMA10W', 'SMA30W', 'History_Days',
        'Gap (%)', 'Latest Volume', '9M+ Volume', 'HVE', 'HVE Date', 'HVE Volume',
        '%_From_52WKH'
    ]

    available = [c for c in desired if c in filtered.columns]
    result = filtered[available].copy()

    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)

    # Save daily output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput saved → {OUTPUT_PATH}")

    # Save dated archive output
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_path = ARCHIVE_DIR / f"52wh_{date.today().strftime('%m%d%Y')}.csv"
    result.to_csv(archive_path, index=False)
    print(f"Archive saved → {archive_path}")

    print(f"Total rows saved: {len(result):,}")

    print("\nFirst 10 rows:")
    preview_cols = [
        'Rank', 'Ticker', 'Price', 'ATR', 'ADR', 'RS Percentile',
        '%_From_52WKH', 'IPO', 'Gap (%)', 'Latest Volume', '9M+ Volume', 'HVE'
    ]
    preview_cols = [c for c in preview_cols if c in result.columns]
    print(result.head(10)[preview_cols].to_string(index=False))


if __name__ == "__main__":
    main()
