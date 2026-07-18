# =============================================================================
#   Filter RS ≥ 70, Price ≥ $30, Strong Recovery from 52W Low
#   Recovery ≥ 70% from 52WKL + At least 25% below 52WKH (No 52WH overlap)
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
MIN_RECOVERY_PCT = 70.0      # Minimum % recovered from 52-week low
MAX_PCT_TO_HIGH  = -25.0     # Must be AT LEAST 25% below 52W High

MIN_AVGVOL10 = 400_000

# ATR & ADR Filter
MIN_ATR = 2.5
MIN_ADR = 2.5

DEBUG_TICKER = None         # Set to a ticker for debugging, or None
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
    if not ticker:
        return
    row = df[df['Ticker'] == ticker]
    if row.empty:
        print(f"\nDEBUG: {ticker} → NOT FOUND")
        return
    row = row.iloc[0]
    print(f"\n=== DEBUG: {ticker} ===")
    print(f"Price           : ${row['Price']:,.2f}")
    print(f"52W Low         : ${row['52WKL']:,.2f}")
    print(f"Recovery %      : {row.get('%_From_52WKL', 'N/A')}%")
    print(f"52W High        : ${row['52WKH']:,.2f}")
    print(f"% from 52WH     : {row.get('%_From_52WKH', 'N/A')}%")
    print(f"RS Percentile   : {row['RS Percentile']:.1f}")
    print(f"10d Avg Volume  : {row.get('AvgVol10', 'N/A'):,.0f}")
    print(f"ATR / ADR       : {row.get('ATR'):.2f} / {row.get('ADR'):.2f}")
    print("-" * 60)


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    # Convert numeric columns
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

    # === MAIN FILTER - Strict Recovery Focus (No Near-High Stocks) ===
    mask = (
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['%_From_52WKL'] >= MIN_RECOVERY_PCT) &
        (df['%_From_52WKH'] <= MAX_PCT_TO_HIGH) &      # Exclude anything close to 52WH
        (df['AvgVol10'] >= MIN_AVGVOL10)
    )

    # ATR/ADR Filter (Stocks only)
    stock_mask = (df['Sector'] != 'ETF') & (df['Sector'].notna())
    df.loc[stock_mask, 'Passes_ATR_ADR'] = (
        (df.loc[stock_mask, 'ATR'] >= MIN_ATR) & 
        (df.loc[stock_mask, 'ADR'] >= MIN_ADR)
    )
    df['Passes_ATR_ADR'] = df['Passes_ATR_ADR'].fillna(True)

    mask = mask & df['Passes_ATR_ADR']
    filtered = df[mask].copy()

    debug_ticker(filtered, DEBUG_TICKER)

    print(f"\nAfter filters:")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • Recovery from 52W Low ≥ {MIN_RECOVERY_PCT}%")
    print(f"  • At least {abs(MAX_PCT_TO_HIGH)}% below 52W High (excludes 52WH stocks)")
    print(f"  • 10d Avg Vol ≥ {MIN_AVGVOL10:,}")
    print(f"  • ATR ≥ {MIN_ATR} AND ADR ≥ {MIN_ADR} (Stocks only)")
    print(f"→ {len(filtered):,} stocks remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Output columns
    desired = [
        'Rank', 'Ticker', 'Price', 'Prev_Close', 'DVol', 'Sector', 'Industry',
        'RS Percentile', 'ATR', 'ADR', 'AvgVol10', '52WKH', '52WKL',
        'Earning_Date', 'MCAP', 'IPO', 'SMA20', 'SMA50', 'SMA200',
        '%_From_52WKL', '%_From_52WKH', 'Recovery_Score'
    ]

    available = [c for c in desired if c in filtered.columns]
    result = filtered[available].copy()
    result = result.sort_values('Recovery_Score', ascending=False).reset_index(drop=True)

    # Save outputs
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput saved → {OUTPUT_PATH}")

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_path = ARCHIVE_DIR / f"52wkl_{date.today().strftime('%m%d%Y')}.csv"
    result.to_csv(archive_path, index=False)
    print(f"Archive saved → {archive_path}")

    print(f"Total rows saved: {len(result):,}")

    # Preview
    print("\nFirst 10 rows:")
    preview_cols = ['Rank', 'Ticker', 'Price', '%_From_52WKL', '%_From_52WKH', 
                   'Recovery_Score', 'RS Percentile', 'ATR', 'ADR']
    preview_cols = [c for c in preview_cols if c in result.columns]
    print(result.head(10)[preview_cols].to_string(index=False))


if __name__ == "__main__":
    main()
