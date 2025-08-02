import pandas as pd
import os
import glob
import re

def ensure_dir(path):
    """Ensure the directory for the output file exists."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

def add_section_label(df, label):
    """Add a Section column to the DataFrame with the specified label."""
    df = df.copy()
    df.insert(0, "Section", label)
    return df

def find_latest_rs_file(archive_path="archive"):
    """Find the latest rs_stocks_YYYYMMDD.csv file in the archive directory."""
    pattern = os.path.join(archive_path, "rs_stocks_*.csv")
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError("❌ No RS files found in archive/")
    return files[0]

def extract_date_from_filename(filepath):
    """Extract the date (YYYYMMDD) from the rs_stocks_YYYYMMDD.csv filename."""
    match = re.search(r'rs_stocks_(\d{8})\.csv', filepath)
    if not match:
        raise ValueError(f"❌ Could not extract date from: {filepath}")
    return match.group(1)

def generate_opportunity_report(source_file: str, output_file: str):
    """Generate an opportunity report based on RS criteria with three sections."""
    df = pd.read_csv(source_file)

    # Clean data by dropping rows with missing values in required fields
    df_clean = df.dropna(subset=[
        'Ticker', 'Price', 'RS Percentile',
        '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'DVol', 'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'MCAP',
        'Sector', 'Industry', 'IPO'
    ])

    # Section 1: Leading Stocks
    # Scans for market leaders with consistently high relative strength (RS Percentile > 90)
    # across current, 1-month, 3-month, and 6-month timeframes, ensuring mid- to large-cap
    # size (MCAP > 1,000M), high liquidity (optional DVol > 1.5 * AvgVol10), and prices
    # near 52-week highs (Price >= 0.9 * 52WKH, > 1.5 * 52WKL). Ideal for stable,
    # high-quality investments with strong, sustained performance.
    leading_df = df_clean[
        (df_clean['Price'] > 20) &
        (df_clean['RS Percentile'] > 90) &
        (df_clean['1M_RS Percentile'] > 90) &
        (df_clean['3M_RS Percentile'] > 90) &
        (df_clean['6M_RS Percentile'] > 90) &
        (df_clean['MCAP'] > 1000) &
        (df_clean['Price'] >= 0.9 * df_clean['52WKH']) &
        (df_clean['Price'] > 1.5 * df_clean['52WKL'])
    ]
    # Optional volume filter: only apply if sufficient stocks remain
    if len(leading_df) > 5:  # Arbitrary threshold to ensure enough candidates
        leading_df = leading_df[leading_df['DVol'] > 1.5 * leading_df['AvgVol10']]
    leading_df = leading_df.copy()
    leading_df['Score'] = (0.5 * leading_df['RS Percentile'] +
                           0.3 * (leading_df['DVol'] / leading_df['AvgVol']) +
                           0.2 * (leading_df['Price'] / leading_df['52WKH']))
    leading_df = leading_df.sort_values(by='Score', ascending=False)
    leading_df = add_section_label(leading_df, "Leading Stocks (RS > 90)")

    # Exclude Section 1 stocks
    df_remaining = df_clean[~df_clean.index.isin(leading_df.index)]

    # Section 2: Top Movers
    # Scans for stocks with improving relative strength (RS ≥ 85, RS > 1M > 3M > 6M,
    # with significant gain > 20 over 6 months), recent volume surges (DVol > 1.5 * AvgVol10),
    # prices near 52-week highs (Price >= 0.95 * 52WKH), and mid- to large-cap size
    # (MCAP > 1,000M). Ideal for momentum-driven growth investors seeking stocks with
    # accelerating performance and strong market interest.
    improving_df = df_remaining[
        (df_remaining['Price'] > 20) &
        (df_remaining['RS Percentile'] >= 85) &
        (df_remaining['RS Percentile'] > df_remaining['1M_RS Percentile']) &
        (df_remaining['1M_RS Percentile'] > df_remaining['3M_RS Percentile']) &
        (df_remaining['3M_RS Percentile'] > df_remaining['6M_RS Percentile']) &
        (df_remaining['RS Percentile'] - df_remaining['6M_RS Percentile'] > 20) &
        (df_remaining['DVol'] > 1.5 * df_remaining['AvgVol10']) &
        (df_remaining['Price'] >= 0.95 * df_remaining['52WKH']) &
        (df_remaining['MCAP'] > 1000)
    ]
    improving_df = improving_df.copy()
    improving_df['Score'] = (0.4 * (improving_df['RS Percentile'] - improving_df['6M_RS Percentile']) +
                            0.3 * (improving_df['DVol'] / improving_df['AvgVol10']) +
                            0.3 * (improving_df['Price'] / improving_df['52WKH']))
    improving_df = improving_df.sort_values(by='Score', ascending=False)
    improving_df = add_section_label(improving_df, "Top Movers (RS ≥ 85, Improving)")

    # Exclude Section 2 stocks
    df_remaining = df_remaining[~df_remaining.index.isin(improving_df.index)]

    # Section 3: Breakout: New Leader
    # Scans for emerging leaders with recent RS surges (RS ≥ 80, 1M/3M/6M in 50–99,
    # RS - 3M > 15, 1M - 3M > 10, 3M > 6M), strong volume spikes (DVol > 2 * AvgVol10,
    # AvgVol10 > 300,000), prices at or near 52-week highs (Price >= 0.98 * 52WKH,
    # > 2 * 52WKL), and mid- to large-cap size (MCAP > 1,000M). Ideal for aggressive
    # investors seeking high-growth breakout candidates with strong market confirmation.
    breakout_df = df_remaining[
        (df_remaining['Price'] > 20) &
        (df_remaining['RS Percentile'] >= 80) &
        (df_remaining['1M_RS Percentile'].between(50, 99)) &
        (df_remaining['3M_RS Percentile'].between(50, 99)) &
        (df_remaining['6M_RS Percentile'].between(50, 99)) &
        (df_remaining['3M_RS Percentile'] > df_remaining['6M_RS Percentile']) &
        (df_remaining['DVol'] > 1.5 * df_remaining['AvgVol10']) &
        (df_remaining['AvgVol10'] > 300000) &
        (df_remaining['RS Percentile'] - df_remaining['3M_RS Percentile'] > 15) &
        (df_remaining['1M_RS Percentile'] - df_remaining['3M_RS Percentile'] > 10) &
       # (df_remaining['Price'] >= 0.98 * df_remaining['52WKH']) &
        (df_remaining['Price'] > 0.7 * df_remaining['52WKL']) &
        (df_remaining['MCAP'] > 1000)
    ]
    breakout_df = breakout_df.copy()
    breakout_df['Score'] = (0.4 * (breakout_df['RS Percentile'] - breakout_df['1M_RS Percentile']) +
                           0.3 * (breakout_df['DVol'] / breakout_df['AvgVol10']) +
                           0.3 * (breakout_df['Price'] / breakout_df['52WKH']))
    breakout_df = breakout_df.sort_values(by='Score', ascending=False)
    breakout_df = add_section_label(breakout_df, "Breakout: New Leader")

    # Combine all sections
    combined_df = pd.concat([leading_df, improving_df, breakout_df], ignore_index=True)

    # Output selected columns, including new fields for transparency
    final_columns = [
        'Section', 'Ticker', 'Price', 'RS Percentile',
        '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'DVol', 'AvgVol10', '52WKH', 'MCAP', 'Sector', 'Industry'
    ]
    combined_df = combined_df[final_columns]

    # Save the report
    ensure_dir(output_file)
    combined_df.to_csv(output_file, index=False)
    print(f"✅ Combined RS opportunities report saved to {output_file}")

    # Save ticker summary with section descriptions
    summary_path = output_file.replace(".csv", "_summary.txt")
    with open(summary_path, "w") as f:
        f.write("Leading Stocks (RS > 90):\n")
        f.write("Scans for market leaders with RS Percentile > 90 across all timeframes, mid- to large-cap (MCAP > 1,000M), high liquidity (optional DVol > 1.5 * AvgVol10), and prices near 52-week highs (Price >= 0.9 * 52WKH, > 1.5 * 52WKL). Ideal for stable, high-quality investments.\n")
        f.write(", ".join(leading_df['Ticker'].tolist()) + "\n\n")

        f.write("Top Movers (RS ≥ 85, Improving):\n")
        f.write("Scans for stocks with improving RS (RS ≥ 85, RS > 1M > 3M > 6M, gain > 20 over 6M), recent volume surges (DVol > 1.5 * AvgVol10), prices near 52-week highs (Price >= 0.95 * 52WKH), and mid- to large-cap (MCAP > 1,000M). Ideal for momentum-driven growth.\n")
        f.write(", ".join(improving_df['Ticker'].tolist()) + "\n\n")

        f.write("Breakout: New Leader:\n")
        f.write("Scans for emerging leaders with recent RS surges (RS ≥ 80, 1M/3M/6M in 50–99, RS - 3M > 15, 1M - 3M > 10, 3M > 6M), strong volume spikes (DVol > 2 * AvgVol10, AvgVol10 > 300,000), prices at or near 52-week highs (Price >= 0.98 * 52WKH, > 2 * 52WKL), and mid- to large-cap (MCAP > 1,000M). Ideal for high-growth breakout strategies.\n")
        f.write(", ".join(breakout_df['Ticker'].tolist()) + "\n")

    print(f"✅ Ticker summary saved to {summary_path}")

if __name__ == "__main__":
    # Auto-detect input file and extract date
    latest_csv = find_latest_rs_file()
    date_str = extract_date_from_filename(latest_csv)
    output_path = f"IBD-20/rs_opportunities_{date_str}.csv"
    generate_opportunity_report(latest_csv, output_path)
