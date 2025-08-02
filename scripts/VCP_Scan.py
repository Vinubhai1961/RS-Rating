import pandas as pd
from pathlib import Path
import re

# Define directories
source_dir = Path("archive")
output_dir = Path("IBD-20")
output_dir.mkdir(parents=True, exist_ok=True)

# Find latest rs_stocks_YYYYMMDD.csv
csv_files = sorted(source_dir.glob("rs_stocks_*.csv"))

if not csv_files:
    raise FileNotFoundError("No rs_stocks_*.csv files found in archive/")

# Extract date for sorting and naming
def extract_date(f):
    match = re.search(r'rs_stocks_(\d{8})\.csv', f.name)
    return int(match.group(1)) if match else 0

latest_file = max(csv_files, key=extract_date)
date_str = re.search(r'rs_stocks_(\d{8})\.csv', latest_file.name).group(1)
output_file = output_dir / f"vcp_{date_str}.csv"

# Load the latest CSV
df = pd.read_csv(latest_file)

# Convert numeric columns
numeric_cols = ["RS Percentile", "Price", "52WKH", "52WKL", "DVol", "AvgVol10"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Filtering logic
filtered_df = df[
    (df["Price"] >= 0.75 * df["52WKH"]) &
    (df["Price"] >= 2 * df["52WKL"]) &
    (df["RS Percentile"] > 85) &
    (
        (df["DVol"] > 1.5 * df["AvgVol10"]) |
        (df["DVol"] > df["AvgVol10"])
    )
]

# Output full original columns, filtered
filtered_df.to_csv(output_file, index=False)
print(f"✅ Filtered stock list saved to: {output_file}")
