# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs + Strong Trend Alignment
#   SMA50 > SMA200 + Price > SMA50 + Price > SMA200
# =============================================================================
import pandas as pd
from pathlib import Path

# ────────────────────────────────────────────────
#   CONFIG
# ────────────────────────────────────────────────
INPUT_PATH   = Path("RS_Data/rs_stocks.csv")
OUTPUT_PATH  = Path("RS_Data/RS80_Price30_within22pct_52wh_SMA_Aligned.csv")

RS_THRESHOLD          = 80.0
PRICE_THRESHOLD       = 30.0
MAX_PCT_BELOW_52WH    = 22
MIN_AVGVOL10          = 450_000

DEBUG_TICKER = "NVDA"        # Change for testing
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
    numeric_cols = ['Price', '52WKH', 'RS Percentile', 'AvgVol10', 
                   'SMA50', 'SMA200', 'SMA10W', 'SMA30W']
    
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop invalid rows
    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile', 'AvgVol10', 'SMA50', 'SMA200'])

    # Calculations
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH'] * 100).round(2)

    debug_ticker(df, DEBUG_TICKER)

    # === MAIN FILTER ===
    mask = (
        (df['%_From_52WKH'] >= -MAX_PCT_BELOW_52WH) &      # Near 52-week high
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['AvgVol10'] >= MIN_AVGVOL10) &
        
        # === TREND ALIGNMENT CONDITIONS ===
        (df['SMA50'] >= df['SMA200']) &                    # SMA50 above SMA200
        (df['Price'] > df['SMA50']) &                      # Price above SMA50
        (df['Price'] > df['SMA200'])                       # Price above SMA200
    )

    filtered = df[mask].copy()

    print(f"\nAfter all filters:")
    print(f"  • Within {MAX_PCT_BELOW_52WH}% of 52-week high")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • 10d Avg Vol ≥ {MIN_AVGVOL10:,}")
    print(f"  • SMA50 ≥ SMA200")
    print(f"  • Price > SMA50")
    print(f"  • Price > SMA200")
    print(f"→ {len(filtered):,} stocks remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Output columns
    desired_cols = [
        'Rank', 'Ticker', 'Price', 'DVol', 'Sector', 'Industry',
        'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'MCAP',
        'Earning_Date', 'SMA50', 'SMA200', 'SMA10W', 'SMA30W',
        '%_From_52WKH'
    ]

    available = [c for c in desired_cols if c in filtered.columns]
    result = filtered[available].copy()

    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)

    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput saved → {OUTPUT_PATH}")
    print(f"Total rows saved: {len(result):,}")

    print("\nTop 10 stocks:")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
