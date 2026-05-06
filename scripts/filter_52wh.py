# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs
#   (Including new all-time highs + up to 28% pullback)
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
MAX_PCT_BELOW   = 28.0          # ← as per this script
MIN_AVGVOL10    = 450_000

DEBUG_TICKER = "NVDA"        # Change or add more if needed
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
    """Debug specific ticker"""
    row = df[df['Ticker'] == ticker]
    if row.empty:
        print(f"\nDEBUG: {ticker} → NOT FOUND in source data")
        return
    
    row = row.iloc[0]
    price = row['Price']
    high = row['52WKH']
    rs = row['RS Percentile']
    vol = row['AvgVol10']
    
    pct_from_high = ((price - high) / high * 100).round(2) if pd.notna(high) and pd.notna(price) else None

    print(f"\n=== DEBUG: {ticker} ===")
    print(f"Price           : ${price:,.2f}")
    print(f"52W High        : ${high:,.2f}")
    print(f"% from 52WH     : {pct_from_high}%")
    print(f"RS Percentile   : {rs:.1f}")
    print(f"10d Avg Volume  : {vol:,.0f}")
    print("-" * 50)

    if pct_from_high is not None and pct_from_high >= -MAX_PCT_BELOW:
        print("→ 52WH Distance : PASSED")
    else:
        print(f"→ 52WH Distance : FAILED (too far below {MAX_PCT_BELOW}%)")


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    # Convert numeric columns
    numeric_cols = ['Price', '52WKH', 'RS Percentile', 'AvgVol10', 'SMA50', 'SMA200']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop invalid rows
    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile', 'AvgVol10'])

    # === CORRECTED: % From 52-Week High ===
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Debug BBOX.NS
    debug_ticker(df, DEBUG_TICKER)

    # === MAIN FILTER (Fixed) ===
    mask = (
        (df['%_From_52WKH'] >= -MAX_PCT_BELOW) &     # New highs + up to 28% pullback
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['AvgVol10'] >= MIN_AVGVOL10)
    )

    filtered = df[mask].copy()

    print(f"\nAfter filters:")
    print(f"  • within {MAX_PCT_BELOW}% pullback from 52-week high (including new highs)")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • 10-day Avg Volume ≥ {MIN_AVGVOL10:,} shares")
    print(f"→ {len(filtered):,} rows remain")

    if len(filtered) == 0:
        print("No stocks match the current criteria.")
        return

    # Desired columns
    desired = [
        'Rank', 'Ticker', 'Price', 'DVol',
        'Sector', 'Industry',
        'RS Percentile',
        '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'AvgVol', 'AvgVol10',
        '52WKH', '52WKL', 'MCAP',
        'Earning_Date', 'SMA50', 'SMA200', 'SMA10W', 'SMA30W',
        '%_From_52WKH'
    ]

    available = [c for c in desired if c in filtered.columns]
    result = filtered[available]

    result = result.sort_values('RS Percentile', ascending=False).reset_index(drop=True)

    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput overwritten → {OUTPUT_PATH}")
    print(f"Total rows saved: {len(result):,}")

    print("\nFirst 10 rows:")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
