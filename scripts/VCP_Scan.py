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

# Extract date from filename
def extract_date(f):
    match = re.search(r'rs_stocks_(\d{8})\.csv', f.name)
    return int(match.group(1)) if match else 0

latest_file = max(csv_files, key=extract_date)
date_str = re.search(r'rs_stocks_(\d{8})\.csv', latest_file.name).group(1)
output_file = output_dir / f"vcp_{date_str}.csv"

# Load CSV
df = pd.read_csv(latest_file)

# Convert numeric columns
numeric_cols = ["RS Percentile", "Price", "52WKH", "52WKL", "DVol", "AvgVol10", "MCAP"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Apply filters
filtered_df = df[
    (df["Price"] >= 0.75 * df["52WKH"]) &
    (df["Price"] >= 1.2 * df["52WKL"]) &
    (df["RS Percentile"] > 85) &
    (
        (df["DVol"] > 1.5 * df["AvgVol"]) |
        (df["DVol"] > df["AvgVol"])
    )
]

# Remove unwanted Sector values and missing MCAP
filtered_df = filtered_df[
    (filtered_df["Sector"].fillna("").str.upper() != "ETF") &
    (filtered_df["Sector"].fillna("").str.strip() != "") &
    (~filtered_df["MCAP"].isna())
]

# Save output
filtered_df.to_csv(output_file, index=False)
print(f"✅ Filtered stock list saved to: {output_file}")
