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
MAX_PCT_BELOW   = 28.0
MIN_AVGVOL10    = 500_000           # ← new: minimum 10-day average volume
# ────────────────────────────────────────────────


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)

    print(f"→ Loaded {len(df):,} rows")

    # Ensure numeric columns – added 'AvgVol10'
    numeric_cols = ['Price', '52WKH', 'RS Percentile', 'AvgVol10']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows we cannot calculate properly – added 'AvgVol10'
    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile', 'AvgVol10'])

    # Calculate % below 52-week high
    df['%_From_52WKH'] = ((df['52WKH'] - df['Price']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Apply all filters – added volume condition
    
    mask = (
    (df['%_From_52WKH'] >= 0) &
    (df['%_From_52WKH'] <= MAX_PCT_BELOW) &
    (df['RS Percentile'] >= RS_THRESHOLD) &  # ← Changed from > to >=
    (df['Price'] >= PRICE_THRESHOLD) &       # ← Changed from > to >=
    (df['AvgVol10'] >= MIN_AVGVOL10)         # ← Changed from > to >=
    )

    filtered = df[mask].copy()

    # Updated print – shows all active filters
    print(f"After filters:")
    print(f"  • within {MAX_PCT_BELOW}% of 52-week high")
    print(f"  • RS Percentile > {RS_THRESHOLD}")
    print(f"  • Price > ${PRICE_THRESHOLD:,}")
    print(f"  • 10-day Avg Volume > {MIN_AVGVOL10:,} shares")
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

    available = [c for c in desired if c in filtered.columns]
    result = filtered[available]

    # Sort by RS Percentile descending (you can switch to % closeness if preferred)
    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)
    # Alternative (closest to high first):
    # result = result.sort_values(by=['%_From_52WKH', 'RS Percentile'], ascending=[True, False]).reset_index(drop=True)

    # Overwrite output
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput overwritten → {OUTPUT_PATH}")
    print(f"Total rows saved: {len(result):,}")

    # Show preview
    print("\nFirst 10 rows:")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
