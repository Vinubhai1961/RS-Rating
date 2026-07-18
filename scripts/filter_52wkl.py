# =============================================================================
#   RS70 + Strong 52WKL Recovery Filter 
#   (Strictly excludes stocks near 52W High)
# =============================================================================
import pandas as pd
from pathlib import Path
from datetime import date

# ────────────────────────────────────────────────
#   CONFIG
# ────────────────────────────────────────────────
INPUT_PATH   = Path("RS_Data/rs_stocks.csv")
OUTPUT_PATH  = Path("RS_Data/RS70_Price30_52WKL.csv")
ARCHIVE_DIR  = Path("52wkl")

RS_THRESHOLD     = 70.0
PRICE_THRESHOLD  = 30.0
MIN_RECOVERY_PCT = 70.0
MAX_PCT_TO_HIGH  = -25.0      # ← Can make stricter (-30 or -35)

MIN_AVGVOL10 = 400_000
MIN_ATR = 2.5
MIN_ADR = 2.5

DEBUG_TICKER = None
# ────────────────────────────────────────────────

def parse_volume(x):
    if pd.isna(x):
        return None
    x = str(x).strip().upper()
    if x.endswith('K'): return float(x[:-1]) * 1_000
    if x.endswith('M'): return float(x[:-1]) * 1_000_000
    if x.endswith('B'): return float(x[:-1]) * 1_000_000_000
    return float(x)


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    numeric_cols = ['Price', '52WKH', '52WKL', 'RS Percentile', 'AvgVol10', 'ATR', 'ADR']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['Price', '52WKH', '52WKL', 'RS Percentile', 'AvgVol10'])

    # Calculate metrics
    df['%_From_52WKL'] = ((df['Price'] - df['52WKL']) / df['52WKL']) * 100
    df['%_From_52WKL'] = df['%_From_52WKL'].round(2)
    
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    df['Recovery_Score'] = df['%_From_52WKL'].clip(lower=0)

    # === STRICT FILTER ===
    mask = (
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['%_From_52WKL'] >= MIN_RECOVERY_PCT) &
        (df['%_From_52WKL'] <= 600) &                    # Cap unrealistic recoveries
        (df['%_From_52WKH'] <= MAX_PCT_TO_HIGH) &        # Strong exclusion of near-high stocks
        (df['AvgVol10'] >= MIN_AVGVOL10) &
        (df['52WKL'] > 1) & 
        (df['Price'] > df['52WKL'])
    )

    # ATR/ADR Filter
    stock_mask = (df['Sector'] != 'ETF') & (df['Sector'].notna())
    df.loc[stock_mask, 'Passes_ATR_ADR'] = (
        (df.loc[stock_mask, 'ATR'] >= MIN_ATR) & 
        (df.loc[stock_mask, 'ADR'] >= MIN_ADR)
    )
    df['Passes_ATR_ADR'] = df['Passes_ATR_ADR'].fillna(True)

    mask = mask & df['Passes_ATR_ADR']
    filtered = df[mask].copy()

    print(f"\nAfter filters:")
    print(f"  • RS ≥ {RS_THRESHOLD} | Price ≥ ${PRICE_THRESHOLD}")
    print(f"  • Recovery ≥ {MIN_RECOVERY_PCT}% (capped at 600%)")
    print(f"  • At least {abs(MAX_PCT_TO_HIGH)}% **below** 52W High (strict)")
    print(f"→ {len(filtered):,} stocks remain")

    if len(filtered) == 0:
        print("No stocks match criteria.")
        return

    desired = [
        'Rank', 'Ticker', 'Price', 'Sector', 'Industry', 'RS Percentile',
        'ATR', 'ADR', 'AvgVol10', '52WKH', '52WKL', 
        '%_From_52WKL', '%_From_52WKH', 'Recovery_Score'
    ]

    result = filtered[[c for c in desired if c in filtered.columns]].copy()
    result = result.sort_values('Recovery_Score', ascending=False).reset_index(drop=True)

    # Save
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput saved → {OUTPUT_PATH}")

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_path = ARCHIVE_DIR / f"52wkl_{date.today().strftime('%m%d%Y')}.csv"
    result.to_csv(archive_path, index=False)

    # Preview
    print("\nFirst 10 rows:")
    preview = result.head(10)[['Ticker', 'Price', '%_From_52WKL', '%_From_52WKH', 'Recovery_Score', 'RS Percentile']]
    print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
