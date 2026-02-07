# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs
#   Overwrites the output file every run
# =============================================================================

import pandas as pd

# ────────────────────────────────────────────────
#   CONFIG – change these values if needed
# ────────────────────────────────────────────────
INPUT_FILE  = "within_27pct_of_52wh.csv"
OUTPUT_FILE = "RS80_Price30_within27pct_52wh.csv"   # will be overwritten each run

RS_THRESHOLD   = 80.0
PRICE_THRESHOLD = 30.0
# ────────────────────────────────────────────────

def main():
    print("Reading input file ...")
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"→ Loaded {len(df):,} rows")
    except FileNotFoundError:
        print(f"Error: File not found → {INPUT_FILE}")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Rename the percentage column (your preferred name)
    if 'pct_below_52wh' in df.columns:
        df = df.rename(columns={'pct_below_52wh': '%_From_52WKH'})
    elif 'pct_of_high' in df.columns:  # fallback if name is different
        df = df.rename(columns={'pct_of_high': '%_From_52WKH'})

    # Round percentage to 2 decimal places
    if '%_From_52WKH' in df.columns:
        df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Apply filters
    mask = (
        (df['RS Percentile'] > RS_THRESHOLD) &
        (df['Price'] > PRICE_THRESHOLD)
    )

    filtered = df[mask].copy()

    print(f"After filters (RS > {RS_THRESHOLD}, Price > {PRICE_THRESHOLD}): {len(filtered):,} rows")

    if len(filtered) == 0:
        print("→ No stocks match the criteria.")
        return

    # Define exact column order you requested
    desired_cols = [
        'Rank', 'Ticker', 'Price', 'DVol',
        'Sector', 'Industry',
        'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'AvgVol', 'AvgVol10',
        '52WKH', '52WKL', 'MCAP',
        '%_From_52WKH'
    ]

    # Keep only columns that actually exist
    existing_cols = [c for c in desired_cols if c in filtered.columns]
    final = filtered[existing_cols]

    # Sort by RS Percentile descending (strongest first)
    final = final.sort_values('RS Percentile', ascending=False)

    # Overwrite the output file
    final.to_csv(OUTPUT_FILE, index=False)
    print(f"\nOutput file overwritten → {OUTPUT_FILE}")
    print(f"Total stocks saved: {len(final):,}")

    # Preview first few rows
    print("\nFirst 8 rows (preview):")
    print(final.head(8).to_string(index=False))


if __name__ == "__main__":
    main()
