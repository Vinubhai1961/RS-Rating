# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs
#   Overwrites the output file every run
# scripts/filter_52wh.py
# =============================================================================

import pandas as pd
from pathlib import Path

# ────────────────────────────────────────────────
#   CONFIG
# ────────────────────────────────────────────────
INPUT_PATH   = Path("RS_Data/rs_stocks.csv")
OUTPUT_PATH  = Path("RS_Data/RS80_Price30_within27pct_52wh.csv")

RS_THRESHOLD    = 80.0
PRICE_THRESHOLD = 30.0
MAX_PCT_BELOW   = 27.0
# ────────────────────────────────────────────────


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)

    print(f"→ Loaded {len(df):,} rows")

    # Ensure numeric columns
    numeric_cols = ['Price', '52WKH', 'RS Percentile']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows we cannot calculate properly
    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile'])

    # Calculate % below 52-week high
    df['%_From_52WKH'] = ((df['52WKH'] - df['Price']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Apply all filters
    mask = (
        (df['%_From_52WKH'] >= 0) &
        (df['%_From_52WKH'] <= MAX_PCT_BELOW) &
        (df['RS Percentile'] > RS_THRESHOLD) &
        (df['Price'] > PRICE_THRESHOLD)
    )

    filtered = df[mask].copy()

    print(f"After filters ({MAX_PCT_BELOW}% from 52WH + RS > {RS_THRESHOLD} + Price > {PRICE_THRESHOLD}):")
    print(f"→ {len(filtered):,} rows remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Define exact column order you requested
    desired = [
        'Rank', 'Ticker', 'Price', 'DVol',
        'Sector', 'Industry',
        'RS Percentile',
        '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'AvgVol', 'AvgVol10',
        '52WKH', '52WKL', 'MCAP',
        '%_From_52WKH'
    ]

    # Keep only columns that actually exist
    available = [c for c in desired if c in filtered.columns]
    result = filtered[available]

    # Sort by RS Percentile descending
    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)
    #result = result.sort_values(by=['%_From_52WKH', 'Rank'], ascending=[True, True]).reset_index(drop=True)

    # Overwrite output
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput overwritten → {OUTPUT_PATH}")
    print(f"Total rows saved: {len(result):,}")

    # Show preview
    print("\nFirst 10 rows:")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
