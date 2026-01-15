"""
Title: Sector RS Report Generator
Author: Dipen Patel
Last Updated: 2025-08-04

Description:
This script filters top-performing industry sectors based on Relative Strength (RS) over time.
It reads the most recent `rs_industries_*.csv` file from the archive directory and identifies:

Section                  | Criteria
-------------------------|--------------------------------------------------------------
🔹 Leading Sectors        | RS > 80 across all periods
🔸 Top Moving Sectors     | RS ≥ 75 + strictly improving (6M < 3M < 1M < Now)
🔹 Breakout Sectors       | RS ≥ 80 now, with weak 3M or 6M history (< 40)
🕵️ Watchlist (NEW)        | Strictly improving trend, but RS < 75 (momentum still developing)

The results are saved to: `IBD-20/rs_top_sectors_opportunities_<DATE>.csv`
"""
from datetime import datetime
import pandas as pd
import os
import glob
import re

def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def add_section_label(df, label):
    df = df.copy()
    df.insert(0, "Section", label)
    return df

def find_latest_rs_file(archive_path="archive"):
    pattern = os.path.join(archive_path, "rs_stocks_*.csv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError("❌ No RS files found in archive/")

    def extract_date_for_sort(filepath):
        match = re.search(r'rs_stocks_(\d{8})\.csv', filepath)
        if not match:
            raise ValueError(f"❌ Invalid filename: {filepath}")

        raw = match.group(1)
        for fmt in ("%Y%m%d", "%m%d%Y"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue

        raise ValueError(f"❌ Invalid date format: {filepath}")

    return max(files, key=extract_date_for_sort)

def extract_date_from_filename(filepath):
    """
    Extract date from rs_stocks_XXXXXXXX.csv filename.
    Supports MMDDYYYY and YYYYMMDD.
    Returns date as YYYYMMDD string.
    """
    match = re.search(r'rs_stocks_(\d{8})\.csv', filepath)
    if not match:
        raise ValueError(f"❌ Could not extract date from: {filepath}")

    raw = match.group(1)

    # Try YYYYMMDD first
    try:
        dt = datetime.strptime(raw, "%Y%m%d")
        return dt.strftime("%Y%m%d")
    except ValueError:
        pass

    # Try MMDDYYYY
    try:
        dt = datetime.strptime(raw, "%m%d%Y")
        return dt.strftime("%Y%m%d")
    except ValueError:
        pass

    raise ValueError(f"❌ Invalid date format in filename: {filepath}")

def generate_sector_report(source_file: str, output_file: str):
    df = pd.read_csv(source_file)
    
    # Debug: Print columns
    print(f"📋 Columns in {source_file}: {df.columns.tolist()}")
    
    required_cols = ['RS', '1 M_RS', '3M_RS', '6M_RS']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {missing_cols}")

    df_clean = df.dropna(subset=required_cols)

    print("\n📊 Preview of RS Values:\n", df_clean[["Industry"] + required_cols].head(10))

    # Debug: How many meet each condition
    print("\n🔍 Individual Condition Counts:")
    print("🔹 RS > 80 now:", (df_clean['RS'] > 80).sum())
    print("🔸 Strictly Improving RS:", (
        (df_clean['RS'] > df_clean['1 M_RS']) &
        (df_clean['1 M_RS'] > df_clean['3M_RS']) &
        (df_clean['3M_RS'] > df_clean['6M_RS'])
    ).sum())
    print("🔹 Breakout Candidates (RS ≥ 80 and weak past):", (
        (df_clean['RS'] >= 80) &
        ((df_clean['3M_RS'] < 40) | (df_clean['6M_RS'] < 40))
    ).sum())

    # 🔹 Section 1: Leading Sectors
    leading_df = df_clean[
        (df_clean['RS'] > 80) &
        (df_clean['1 M_RS'] > 80) &
        (df_clean['3M_RS'] > 80) &
        (df_clean['6M_RS'] > 80)
    ]
    leading_df = leading_df.sort_values(by='RS', ascending=False)
    leading_df = add_section_label(leading_df, "🔹 RS > 80: Leading Sectors")

    # 🔸 Section 2: Top Moving Sectors
    improving_df = df_clean[
        (df_clean['RS'] >= 75) &
        (df_clean['RS'] > df_clean['1 M_RS']) &
        (df_clean['1 M_RS'] > df_clean['3M_RS']) &
        (df_clean['3M_RS'] > df_clean['6M_RS'])
    ]
    improving_df = improving_df.sort_values(by='RS', ascending=False)
    improving_df = add_section_label(improving_df, "🔸 RS ≥ 75: Top Moving Sectors")

    # 🔹 Section 3: Breakout Sectors
    breakout_df = df_clean[
        (df_clean['RS'] >= 80) &
        ((df_clean['3M_RS'] < 40) | (df_clean['6M_RS'] < 40))
    ]
    breakout_df = breakout_df.sort_values(by='RS', ascending=False)
    breakout_df = add_section_label(breakout_df, "🔹 RS ≥ 80: Breakout Sectors")

    # 🕵️ Watchlist: Improving but RS < 75
    watchlist_df = df_clean[
        (df_clean['RS'] < 75) &
        (df_clean['RS'] > df_clean['1 M_RS']) &
        (df_clean['1 M_RS'] > df_clean['3M_RS']) &
        (df_clean['3M_RS'] > df_clean['6M_RS'])
    ]
    watchlist_df = watchlist_df.sort_values(by='RS', ascending=False)
    watchlist_df = add_section_label(watchlist_df, "🕵️ Improving Watchlist (RS < 75)")

    # Summary
    print(f"\n📌 Matched Counts:")
    print(f"🔹 Leading Sectors: {len(leading_df)}")
    print(f"🔸 Top Moving Sectors: {len(improving_df)}")
    print(f"🔹 Breakout Sectors: {len(breakout_df)}")
    print(f"🕵️ Watchlist: {len(watchlist_df)}")

    # Combine all
    combined_df = pd.concat([leading_df, improving_df, breakout_df, watchlist_df], ignore_index=True)
    final_columns = ['Section', 'Industry', 'RS', '1 M_RS', '3M_RS', '6M_RS']
    combined_df = combined_df[final_columns]

    ensure_dir(output_file)

    if combined_df.empty:
        print("⚠️ No sectors met the filter criteria. No report generated.")
    else:
        combined_df.to_csv(output_file, index=False)
        print(f"✅ Sector RS report saved to {output_file}")

# Main execution
if __name__ == "__main__":
    latest_csv = find_latest_industry_file()
    print(f"📅 Latest RS file detected: {os.path.basename(latest_csv)}")
    date_str = extract_date_from_filename(latest_csv)
    output_path = f"IBD-20/rs_top_sectors_opportunities_{date_str}.csv"
    generate_sector_report(latest_csv, output_path)
