# =============================================================================
#   Filter high-RS, higher-priced stocks near 52-week highs
# =============================================================================
import pandas as pd
from pathlib import Path

# ────────────────────────────────────────────────
#   CONFIG
# ────────────────────────────────────────────────
INPUT_PATH   = Path("RS_Data/rs_stocks.csv")
OUTPUT_PATH  = Path("RS_Data/RS80_Price30_within27pct_52wh.csv")

RS_THRESHOLD    = 77.0
PRICE_THRESHOLD = 30.0
MAX_PCT_BELOW   = 25.0
MIN_AVGVOL10    = 300_000

DEBUG_TICKER = "HINDCOPPER.NS"
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
    row = df[df['Ticker'] == ticker]
    if row.empty:
        print(f"\nDEBUG: {ticker} → NOT FOUND in source data")
        return
    
    row = row.iloc[0]
    price = row['Price']
    rs = row['RS Percentile']
    vol = row['AvgVol10']
    high = row['52WKH']
    
    if pd.notna(high) and pd.notna(price):
        pct_from_high = ((price - high) / high * 100).round(2)   # Changed sign
        pct_below = max(0, -pct_from_high)                       # For display
    else:
        pct_from_high = None
        pct_below = None

    print(f"\n=== DEBUG: {ticker} ===")
    print(f"Price          : ${price:,.2f}")
    print(f"RS Percentile  : {rs:.1f}")
    print(f"10d Avg Vol    : {vol:,.0f}")
    print(f"52W High       : ${high:,.2f}")
    print(f"% from 52WH    : {pct_from_high}%")   # Can be negative (new high)
    print("-" * 40)

    issues = []
    if pct_from_high is None or pct_from_high < -MAX_PCT_BELOW:   # Allow new highs
        issues.append(f"• Too far from 52WH ({pct_from_high}%)")
    if rs < RS_THRESHOLD:
        issues.append(f"• RS Percentile = {rs} (must be ≥ {RS_THRESHOLD})")
    if price < PRICE_THRESHOLD:
        issues.append(f"• Price = ${price:,.2f} (must be ≥ ${PRICE_THRESHOLD})")
    if vol < MIN_AVGVOL10:
        issues.append(f"• AvgVol10 = {vol:,.0f} (must be ≥ {MIN_AVGVOL10:,})")

    if not issues:
        print("→ PASSED ALL FILTERS ✓")
    else:
        print("→ FAILED FILTERS:")
        for issue in issues:
            print(issue)


def main():
    if not INPUT_PATH.exists():
        print(f"Error: Input file not found → {INPUT_PATH}")
        return

    print("Reading source file ...")
    df = pd.read_csv(INPUT_PATH)
    print(f"→ Loaded {len(df):,} rows")

    # Convert columns
    numeric_cols = ['Price', '52WKH', 'RS Percentile', 'AvgVol10']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'AvgVol10':
                df[col] = df[col].apply(parse_volume)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['Price', '52WKH', 'RS Percentile', 'AvgVol10'])

    # Calculate % from 52-week high (negative = new high)
    df['%_From_52WKH'] = ((df['Price'] - df['52WKH']) / df['52WKH']) * 100
    df['%_From_52WKH'] = df['%_From_52WKH'].round(2)

    # Debug specific ticker
    debug_ticker(df, DEBUG_TICKER)

    # Updated filter - allow new highs
    mask = (
        (df['%_From_52WKH'] >= -MAX_PCT_BELOW) &      # ← Fixed: allow up to 27% above
        (df['RS Percentile'] >= RS_THRESHOLD) &
        (df['Price'] >= PRICE_THRESHOLD) &
        (df['AvgVol10'] >= MIN_AVGVOL10)
    )

    filtered = df[mask].copy()

    print(f"\nAfter filters:")
    print(f"  • within {MAX_PCT_BELOW}% of 52-week high (including new highs)")
    print(f"  • RS Percentile ≥ {RS_THRESHOLD}")
    print(f"  • Price ≥ ${PRICE_THRESHOLD:,}")
    print(f"  • 10-day Avg Volume ≥ {MIN_AVGVOL10:,} shares")
    print(f"→ {len(filtered):,} rows remain")

    # ... rest of your code (column selection, sorting, save) ...
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
    result = filtered[available].sort_values('RS Percentile', ascending=False).reset_index(drop=True)

    result.to_csv(OUTPUT_PATH, index=False)
    print(f"\nOutput overwritten → {OUTPUT_PATH}")
    print(f"Total rows saved: {len(result):,}")

    print("\nFirst 10 rows:")
    print(result.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
