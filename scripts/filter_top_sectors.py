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

def find_latest_industry_file(archive_path="archive"):
    pattern = os.path.join(archive_path, "rs_industries_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError("❌ No RS industry files found in archive/")
    return files[0]

def extract_date_from_filename(filepath):
    match = re.search(r'rs_industries_(\d{8})\.csv', filepath)
    if not match:
        raise ValueError(f"❌ Could not extract date from: {filepath}")
    return match.group(1)

def generate_sector_report(source_file: str, output_file: str):
    df = pd.read_csv(source_file)

    # Required columns
    required_cols = ['Relative Strength', '1 Month Ago', '3 Months Ago', '6 Months Ago']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {missing_cols}")

    df_clean = df.dropna(subset=required_cols)

    print("\n📊 Preview of RS Values:\n", df_clean[["Industry"] + required_cols].head(10))

    # 🔍 Debug individual filters
    print("\n🔍 Individual Condition Counts:")
    print("🔹 RS > 95 now:", (df_clean['Relative Strength'] > 95).sum())
    print("🔹 1M > 95:", (df_clean['1 Month Ago'] > 95).sum())
    print("🔹 3M > 95:", (df_clean['3 Months Ago'] > 95).sum())
    print("🔹 6M > 95:", (df_clean['6 Months Ago'] > 95).sum())
    print("🔸 Strictly Improving RS:", (
        (df_clean['Relative Strength'] > df_clean['1 Month Ago']) &
        (df_clean['1 Month Ago'] > df_clean['3 Months Ago']) &
        (df_clean['3 Months Ago'] > df_clean['6 Months Ago'])
    ).sum())
    print("🔹 Breakout Candidates:", (
        (df_clean['Relative Strength'] >= 90) &
        ((df_clean['3 Months Ago'] < 50) | (df_clean['6 Months Ago'] < 50))
    ).sum())

    # 🔹 Section 1: Leading Sectors (slightly relaxed)
    leading_df = df_clean[
        (df_clean['Relative Strength'] > 90) &
        (df_clean['1 Month Ago'] > 90) &
        (df_clean['3 Months Ago'] > 90) &
        (df_clean['6 Months Ago'] > 90)
    ]
    leading_df = leading_df.sort_values(by='Relative Strength', ascending=False)
    leading_df = add_section_label(leading_df, "🔹 RS > 90: Leading Sectors")

    # 🔸 Section 2: Top Moving Sectors (RS improving trend)
    improving_df = df_clean[
        (df_clean['Relative Strength'] >= 85) &
        (df_clean['Relative Strength'] > df_clean['1 Month Ago']) &
        (df_clean['1 Month Ago'] > df_clean['3 Months Ago']) &
        (df_clean['3 Months Ago'] > df_clean['6 Months Ago'])
    ]
    improving_df = improving_df.sort_values(by='Relative Strength', ascending=False)
    improving_df = add_section_label(improving_df, "🔸 RS ≥ 85: Top Moving Sectors")

    # 🔹 Section 3: Breakout Sectors
    breakout_df = df_clean[
        (df_clean['Relative Strength'] >= 90) &
        ((df_clean['3 Months Ago'] < 50) | (df_clean['6 Months Ago'] < 50))
    ]
    breakout_df = breakout_df.sort_values(by='Relative Strength', ascending=False)
    breakout_df = add_section_label(breakout_df, "🔹 RS ≥ 90: Breakout Sectors")

    print(f"\n📌 Matched Counts:")
    print(f"🔹 Leading Sectors: {len(leading_df)}")
    print(f"🔸 Top Moving Sectors: {len(improving_df)}")
    print(f"🔹 Breakout Sectors: {len(breakout_df)}")

    # Combine and export
    combined_df = pd.concat([leading_df, improving_df, breakout_df], ignore_index=True)
    final_columns = ['Section', 'Industry', 'Relative Strength',
                     '1 Month Ago', '3 Months Ago', '6 Months Ago']
    combined_df = combined_df[final_columns]

    ensure_dir(output_file)

    if combined_df.empty:
        print("⚠️ No sectors met the filter criteria. No report generated.")
    else:
        combined_df.to_csv(output_file, index=False)
        print(f"✅ Sector RS report saved to {output_file}")

if __name__ == "__main__":
    latest_csv = find_latest_industry_file()
    date_str = extract_date_from_filename(latest_csv)
    output_path = f"IBD-20/rs_top_sectors_opportunities_{date_str}.csv"
    generate_sector_report(latest_csv, output_path)
