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
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError("❌ No RS files found in archive/")
    return files[0]

def extract_date_from_filename(filepath):
    match = re.search(r'rs_stocks_(\d{8})\.csv', filepath)
    if not match:
        raise ValueError(f"❌ Could not extract date from: {filepath}")
    return match.group(1)

def generate_opportunity_report(source_file: str, output_file: str):
    df = pd.read_csv(source_file)

    df_clean = df.dropna(subset=[
        'Relative Strength Percentile',
        '1 Month Ago Percentile',
        '3 Months Ago Percentile',
        '6 Months Ago Percentile',
        'Price'
    ])

    # 🔹 Section 1: Leading Stocks (RS > 95 for all 4 and Price > 20)
    leading_df = df_clean[
        (df_clean['Price'] > 20) &
        (df_clean['Relative Strength Percentile'] > 95) &
        (df_clean['1 Month Ago Percentile'] > 95) &
        (df_clean['3 Months Ago Percentile'] > 95) &
        (df_clean['6 Months Ago Percentile'] > 95)
    ]
    leading_df = leading_df.sort_values(by=['Relative Strength Percentile', 'Rank'], ascending=[False, True])
    leading_df = add_section_label(leading_df, "🔹 RS > 95: Leading Stocks")

    # 🔸 Section 2: Improving RS ≥ 85
    improving_df = df_clean[
        (df_clean['Relative Strength Percentile'] >= 85) &
        (df_clean['Relative Strength Percentile'] > df_clean['1 Month Ago Percentile']) &
        (df_clean['1 Month Ago Percentile'] > df_clean['3 Months Ago Percentile']) &
        (df_clean['3 Months Ago Percentile'] > df_clean['6 Months Ago Percentile'])
    ]
    improving_df = improving_df.sort_values(by=['Relative Strength Percentile', 'Rank'], ascending=[False, True])
    improving_df = add_section_label(improving_df, "🔸 RS ≥ 85: Top Movers")

    # 🔹 Section 3: Breakout Candidates
    breakout_df = df_clean[
        (df_clean['Relative Strength Percentile'] >= 90) &
        ((df_clean['3 Months Ago Percentile'] < 50) | (df_clean['6 Months Ago Percentile'] < 50))
    ]
    breakout_df = breakout_df.sort_values(by=['Relative Strength Percentile', 'Rank'], ascending=[False, True])
    breakout_df = add_section_label(breakout_df, "🔹 Breakout: New Leader")

    # Combine all sections
    combined_df = pd.concat([leading_df, improving_df, breakout_df], ignore_index=True)

    # Output selected columns only
    final_columns = ['Section', 'Ticker', 'Price', 'Relative Strength Percentile',
                     '1 Month Ago Percentile', '3 Months Ago Percentile', '6 Months Ago Percentile',
                     'Sector', 'Industry']
    combined_df = combined_df[final_columns]

    ensure_dir(output_file)
    combined_df.to_csv(output_file, index=False)
    print(f"✅ Combined RS opportunities report saved to {output_file}")

if __name__ == "__main__":
    # Auto-detect input file and extract date
    latest_csv = find_latest_rs_file()
    date_str = extract_date_from_filename(latest_csv)
    output_path = f"IBD-20/rs_opportunities_{date_str}.csv"

    generate_opportunity_report(latest_csv, output_path)
