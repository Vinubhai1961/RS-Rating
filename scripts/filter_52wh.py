import pandas as pd
from pathlib import Path

# Config
INPUT = "RS_Data/rs_stocks.csv"
THRESHOLD = 25          # Change to 27 if you prefer your original wording
FOLDER_NAME = f"0-{THRESHOLD}%_52WK"

df = pd.read_csv(INPUT)

# Clean + compute
df['52WKH'] = pd.to_numeric(df['52WKH'], errors='coerce')
df['Price']  = pd.to_numeric(df['Price'], errors='coerce')
df = df.dropna(subset=['52WKH', 'Price'])

df['pct_below_52wh'] = ((df['52WKH'] - df['Price']) / df['52WKH']) * 100
df['pct_of_high']    = (df['Price'] / df['52WKH']) * 100                # Useful column

filtered = df[
    (df['pct_below_52wh'] >= 0) & 
    (df['pct_below_52wh'] <= THRESHOLD)
].copy()

# Sort by strength
filtered = filtered.sort_values('RS Percentile', ascending=False)

# Save
out_dir = Path("RS_Data") / FOLDER_NAME
out_dir.mkdir(parents=True, exist_ok=True)

filtered.to_csv(out_dir / "filtered_stocks.csv", index=False)
filtered.to_csv(out_dir / f"filtered_{THRESHOLD}pct.csv", index=False)  # backup name

print(f"✅ Saved {len(filtered):,} tickers → {out_dir}")
print(f"Top 5: {filtered['Ticker'].head().tolist()}")
