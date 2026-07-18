# =============================================================================
#   Filter RS ≥ 70, Price ≥ $30, Strong Recovery from 52W Low + ATR/ADR
#   Recovery ≥ 70% from 52WKL, and at least 25% below 52WKH
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
MIN_RECOVERY_PCT = 70.0      # % recovered from 52-week low
MAX_PCT_TO_HIGH  = -25.0     # Must be at least 25% below 52WKH

MIN_AVGVOL10 = 400_000

# ATR & ADR Filter
MIN_ATR = 2.5
MIN_ADR = 2.5

DEBUG_TICKER = "VSH"        # Change for testing
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
    print(f"52W Low         : ${row['52WKL']:,.2f}")
    print(f"52W High        : ${row['52WKH']:,.2f}")
    print(f"Recovery %      : {row.get('%_From_52WKL', 'N/A')}%")
    print(f"% from 52WH     : {row.get('%_From_52WKH', 'N/A')}%")
    print(f"RS Percentile   : {row['RS Percentile']:.1f}")
    print(f"10d Avg Volume  : {row['AvgVol10']:,.0f}")
    print(f"ATR             : {row.get('ATR', 'N/A')}")
    print(f"ADR             : {row.get('ADR', 'N/A')}")
    print("-" * 60)


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    # Convert numeric columns
    numeric_cols = ['Price', 'Prev_Close', '52WKH', '52WKL', 'RS Percentile', 
                    'AvgVol10', 'ATR', 'ADR', 'SMA20', 'SMA50', 'SMA200']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop invalid rows
    df = df.dropna(subset=['Price', '52WKH', '52WKL', 'RS Percentile', 'AvgVol10'])

    # Calculate key metrics
    df['%_From_52WKL'] = ((df['Price'] - df['52WKL']) / df['52WKL']) * 100
    df['%_From_52WKL'] = df['%_From_52WKL'].round(2)
    
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Recovery Score (simple but effective: % recovered from low)
    df['Recovery_Score'] = df['%_From_52WKL'].clip(lower=0)

    # === MAIN FILTER ===
    mask = (
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['%_From_52WKL'] >= MIN_RECOVERY_PCT) &
        (df['%_From_52WKH'] <= MAX_PCT_TO_HIGH) &   # At least 25% below 52WH
        (df['AvgVol10'] >= MIN_AVGVOL10)
    )

    # Apply ATR & ADR filter only on Stocks (not ETFs)
    stock_mask = (df['Sector'] != 'ETF') & (df['Sector'].notna())
    df.loc[stock_mask, 'Passes_ATR_ADR'] = (
        (df.loc[stock_mask, 'ATR'] >= MIN_ATR) & 
        (df.loc[stock_mask, 'ADR'] >= MIN_ADR)
    )
    # ETFs automatically pass
    df['Passes_ATR_ADR'] = df['Passes_ATR_ADR'].fillna(True)

    # Final filter
    mask = mask & df['Passes_ATR_ADR']
    filtered = df[mask].copy()

    # Debug specific ticker
    debug_ticker(df, DEBUG_TICKER)

    print(f"\nAfter filters:")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • Recovery from 52W Low ≥ {MIN_RECOVERY_PCT}%")
    print(f"  • At least {abs(MAX_PCT_TO_HIGH)}% below 52W High")
    print(f"  • 10d Avg Vol ≥ {MIN_AVGVOL10:,}")
    print(f"  • ATR ≥ {MIN_ATR}  AND  ADR ≥ {MIN_ADR}  (Stocks only)")
    print(f"→ {len(filtered):,} stocks remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Desired columns (aligned with your existing style)
    desired = [
        'Rank', 'Ticker', 'Price', 'Prev_Close', 'DVol', 'Sector', 'Industry',
        'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'ATR', 'ADR', 'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'Earning_Date', 'MCAP',
        'IPO', 'SMA20', 'SMA50', 'SMA200', 'SMA10W', 'SMA30W', 'History_Days',
        'Gap (%)', 'Latest Volume', '9M+ Volume', 'HVE', 'HVE Date', 'HVE Volume',
        '%_From_52WKL', '%_From_52WKH', 'Recovery_Score'
    ]

    available = [c for c in desired if c in filtered.columns]
    result = filtered[available].copy()

    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)

    # Save daily output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput saved → {OUTPUT_PATH}")

    # Save dated archive
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_path = ARCHIVE_DIR / f"52wkl_{date.today().strftime('%m%d%Y')}.csv"
    result.to_csv(archive_path, index=False)
    print(f"Archive saved → {archive_path}")

    print(f"Total rows saved: {len(result):,}")

    # Preview
    print("\nFirst 10 rows:")
    preview_cols = [
        'Rank', 'Ticker', 'Price', '52WKL', '%_From_52WKL', '52WKH', '%_From_52WKH',
        'Recovery_Score', 'RS Percentile', 'ATR', 'ADR', 'Earning_Date'
    ]
    preview_cols = [c for c in preview_cols if c in result.columns]
    print(result.head(10)[preview_cols].to_string(index=False))


if __name__ == "__main__":
    main()
