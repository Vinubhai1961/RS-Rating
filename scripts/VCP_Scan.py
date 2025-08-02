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

def generate_vcp_report(source_file: str, output_file: str):
    """Generate a VCP report based on price and RS criteria."""
    df = pd.read_csv(source_file)

    # Clean data by dropping rows with missing values in required fields
    df_clean = df.dropna(subset=[
        'Ticker', 'Price', 'RS Percentile', '52WKH', '52WKL'
    ])

    # VCP Scan
    # Scans for stocks in a Volatility Contraction Pattern, with prices within 0–25% of the
    # 52-week high (Price >= 0.75 * 52WKH), at least 100% up from the 52-week low
    # (Price >= 2 * 52WKL), and strong relative strength (RS Percentile > 85). Ideal for
    # aggressive growth investors seeking breakout candidates.
    vcp_df = df_clean[
        (df_clean['Price'] >= 0.75 * df_clean['52WKH']) &
        (df_clean['Price'] <= df_clean['52WKH']) &
        (df_clean['Price'] >= 2 * df_clean['52WKL']) &
        (df_clean['RS Percentile'] > 85)
    ]
    vcp_df = vcp_df.copy()
    vcp_df['Score'] = (0.5 * vcp_df['RS Percentile'] +
                       0.3 * (vcp_df['Price'] / vcp_df['52WKH']) +
                       0.2 * (vcp_df['Price'] / vcp_df['52WKL']))
    vcp_df = vcp_df.sort_values(by='Score', ascending=False)
    vcp_df = add_section_label(vcp_df, "VCP Candidates")

    # Output selected columns
    final_columns = [
        'Section', 'Ticker', 'Price', 'RS Percentile',
        '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
        'DVol', 'AvgVol10', '52WKH', 'MCAP', 'Sector', 'Industry'
    ]
    vcp_df = vcp_df[final_columns]

    # Save the report
    ensure_dir(output_file)
    vcp_df.to_csv(output_file, index=False)
    print(f"✅ VCP report saved to {output_file}")

    # Save ticker summary
    summary_path = output_file.replace(".csv", "_summary.txt")
    with open(summary_path, "w") as f:
        f.write("VCP Candidates:\n")
        f.write("Scans for stocks in a Volatility Contraction Pattern, with prices within 0–25% of the 52-week high (Price >= 0.75 * 52WKH), at least 100% up from the 52-week low (Price >= 2 * 52WKL), and strong relative strength (RS Percentile > 85). Ideal for aggressive growth investors seeking breakout candidates.\n")
        f.write(", ".join(vcp_df['Ticker'].tolist()) + "\n")

    print(f"✅ Ticker summary saved to {summary_path}")

if __name__ == "__main__":
    # Auto-detect input file and extract date
    latest_csv = find_latest_rs_file()
    date_str = extract_date_from_filename(latest_csv)
    output_path = f"IBD-20/vcp_opportunities_{date_str}.csv"
    generate_vcp_report(latest_csv, output_path)
