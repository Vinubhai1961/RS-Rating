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
        raise FileNotFoundError("❌ No RS stock files found in archive/")
    return files[0]

def find_latest_industry_file(archive_path="archive"):
    pattern = os.path.join(archive_path, "rs_industries_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError("❌ No RS industry files found in archive/")
    return files[0]

def extract_date_from_filename(filepath):
    match = re.search(r'_(\d{8})\.csv', filepath)
    if not match:
        raise ValueError(f"❌ Could not extract date from: {filepath}")
    return match.group(1)

def generate_opportunity_report(stock_file: str, industry_file: str, output_file: str):
    # ========== STOCK SECTION ==========
    df = pd.read_csv(stock_file)

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

    stock_df = pd.concat([leading_df, improving_df, breakout_df], ignore_index=True)
    stock_df['Type'] = 'Stock'

    # ========== INDUSTRY SECTION ==========
    industry = pd.read_csv(industry_file)
    industry.columns = industry.columns.str.strip()  # remove whitespace
    industry = industry.rename(columns={
        'Relative Strength': 'Relative Strength Percentile',
        '1 Month Ago': '1 Month Ago Percentile',
        '3 Months Ago': '3 Months Ago Percentile',
        '6 Months Ago': '6 Months Ago Percentile'
    })

    industry_clean = industry.dropna(subset=[
        'Relative Strength Percentile',
        '1 Month Ago Percentile',
        '3 Months Ago Percentile',
        '6 Months Ago Percentile'
    ])

    # 🔹 Leading Sectors
    ind_leading = industry_clean[
        (industry_clean['Relative Strength Percentile'] > 95) &
        (industry_clean['1 Month Ago Percentile'] > 95) &
        (industry_clean['3 Months Ago Percentile'] > 95) &
        (industry_clean['6 Months Ago Percentile'] > 95)
    ]
    ind_leading = ind_leading.sort_values(by='Relative Strength Percentile', ascending=False)
    ind_leading = add_section_label(ind_leading, "🔹 RS > 95: Leading Sectors")

    # 🔸 Top Moving Sectors
    ind_moving = industry_clean[
        (industry_clean['Relative Strength Percentile'] >= 85) &
        (industry_clean['Relative Strength Percentile'] > industry_clean['1 Month Ago Percentile']) &
        (industry_clean['1 Month Ago Percentile'] > industry_clean['3 Months Ago Percentile']) &
        (industry_clean['3 Months Ago Percentile'] > industry_clean['6 Months Ago Percentile'])
    ]
    ind_moving = ind_moving.sort_values(by='Relative Strength Percentile', ascending=False)
    ind_moving = add_section_label(ind_moving, "🔸 RS ≥ 85: Top Moving Sectors")

    # 🔹 Breakout Sectors
    ind_breakout = industry_clean[
        (industry_clean['Relative Strength Percentile'] >= 90) &
        ((industry_clean['3 Months Ago Percentile'] < 50) | (industry_clean['6 Months Ago Percentile'] < 50))
    ]
    ind_breakout = ind_breakout.sort_values(by='Relative Strength Percentile', ascending=False)
    ind_breakout = add_section_label(ind_breakout, "🔹 RS ≥ 90: Breakout Sectors")

    industry_df = pd.concat([ind_leading, ind_moving, ind_breakout], ignore_index=True)
    industry_df = industry_df.rename(columns={'Industry': 'Ticker'})  # unify with stock format
    industry_df['Price'] = ''  # filler
    industry_df['Sector'] = industry_df['Sector'].fillna('')
    industry_df['Rank'] = industry_df['Rank'].fillna('')
    industry_df['Type'] = 'Sector'

    # Reorder columns
    all_df = pd.concat([stock_df, industry_df], ignore_index=True)

    final_columns = ['Section', 'Type', 'Ticker', 'Price', 'Relative Strength Percentile',
                     '1 Month Ago Percentile', '3 Months Ago Percentile', '6 Months Ago Percentile',
                     'Sector', 'Industry', 'Rank']
    all_df = all_df[final_columns]

    # Save combined CSV
    ensure_dir(output_file)
    all_df.to_csv(output_file, index=False)
    print(f"✅ Combined RS stock + sector report saved to {output_file}")

    # Append stock summary as comments
    with open(output_file, "a") as f:
        f.write("\n# === STOCK SUMMARY ===\n")
        f.write("# section-1: RS > 95 for all timeframes and Price > 20\n")
        f.write("# " + ", ".join(leading_df['Ticker'].tolist()) + "\n\n")

        f.write("# section-2: RS ≥ 85 and improving trend\n")
        f.write("# " + ", ".join(improving_df['Ticker'].tolist()) + "\n\n")

        f.write("# section-3: RS ≥ 90 with breakout pattern\n")
        f.write("# " + ", ".join(breakout_df['Ticker'].tolist()) + "\n")

if __name__ == "__main__":
    latest_stock_csv = find_latest_rs_file()
    latest_ind_csv = find_latest_industry_file()
    date_str = extract_date_from_filename(latest_stock_csv)
    output_path = f"IBD-20/rs_opportunities_{date_str}.csv"

    generate_opportunity_report(latest_stock_csv, latest_ind_csv, output_path)
