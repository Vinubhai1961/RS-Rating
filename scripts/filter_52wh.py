import pandas as pd
from pathlib import Path
import sys

# ────────────────────────────────────────────────
#   You only change this line when you want different threshold
THRESHOLD_PCT = 27
# ────────────────────────────────────────────────

INPUT_FILE  = "RS_Data/rs_stocks.csv"
OUTPUT_FILE = f"RS_Data/within_{THRESHOLD_PCT}pct_of_52wh.csv"

df = pd.read_csv(INPUT_FILE)

# Make sure columns are numeric
df['Price']  = pd.to_numeric(df['Price'],  errors='coerce')
df['52WKH']  = pd.to_numeric(df['52WKH'], errors='coerce')

# Drop rows we can't calculate percentage for
df = df.dropna(subset=['Price', '52WKH'])

# Calculate how far below the high (in %)
df['pct_below_52wh'] = ((df['52WKH'] - df['Price']) / df['52WKH']) * 100

# Keep only stocks within the chosen range (including exactly at high = 0%)
filtered = df[
    (df['pct_below_52wh'] >= 0) &
    (df['pct_below_52wh'] <= THRESHOLD_PCT)
].copy()

# Optional: sort by overall RS strength (most people prefer this)
filtered = filtered.sort_values('RS Percentile', ascending=False)

# Add a helper column people often like to see
filtered['pct_of_52wh'] = (filtered['Price'] / filtered['52WKH']) * 100

# Save — one file only
filtered.to_csv(OUTPUT_FILE, index=False)

print(f"Done.")
print(f"Threshold : ≤ {THRESHOLD_PCT}% below 52-week high")
print(f"Kept      : {len(filtered):,} / {len(df):,} tickers")
print(f"Output    : {OUTPUT_FILE}")
print(f"Top ticker: {filtered.iloc[0]['Ticker'] if not filtered.empty else '—'}")
