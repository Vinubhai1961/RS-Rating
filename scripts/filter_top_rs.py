import pandas as pd
import os

def ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def add_section_label(df, label):
    df = df.copy()
    df.insert(0, "Section", label)
    return df

def generate_opportunity_report(source_file: str, output_file: str):
    df = pd.read_csv(source_file)

    df_clean = df.dropna(subset=[
        'Relative Strength Percentile',
        '1 Month Ago Percentile',
        '3 Months Ago Percentile',
        '6 Months Ago Percentile'
    ])

    # 🔸 Section 1: Improving RS ≥ 85
    improving_df = df_clean[
        (df_clean['Relative Strength Percentile'] >= 85) &
        (df_clean['Relative Strength Percentile'] > df_clean['1 Month Ago Percentile']) &
        (df_clean['1 Month Ago Percentile'] > df_clean['3 Months Ago Percentile']) &
        (df_clean['3 Months Ago Percentile'] > df_clean['6 Months Ago Percentile'])
    ]
    improving_df = improving_df.sort_values(by=['Relative Strength Percentile', 'Rank'], ascending=[False, True])
    improving_df = add_section_label(improving_df, "🔸 RS ≥ 85: Top Movers")

    # 🔹 Section 2: Breakout Candidates
    breakout_df = df_clean[
        (df_clean['Relative Strength Percentile'] >= 90) &
        ((df_clean['3 Months Ago Percentile'] < 50) | (df_clean['6 Months Ago Percentile'] < 50))
    ]
    breakout_df = breakout_df.sort_values(by=['Relative Strength Percentile', 'Rank'], ascending=[False, True])
    breakout_df = add_section_label(breakout_df, "🔹 Breakout: New Leader")

    # Combine both sections
    combined_df = pd.concat([improving_df, breakout_df], ignore_index=True)

    # Output selected columns only
    final_columns = ['Section', 'Ticker', 'Price', 'Relative Strength Percentile',
                     '1 Month Ago Percentile', '3 Months Ago Percentile', '6 Months Ago Percentile',
                     'Sector', 'Industry']
    combined_df = combined_df[final_columns]

    ensure_dir(output_file)
    combined_df.to_csv(output_file, index=False)
    print(f"✅ Combined RS opportunities report saved to {output_file}")

if __name__ == "__main__":
    date_str = "07252025"  # Replace with dynamic version for automation
    source = f"archive/rs_stocks_{date_str}.csv"
    output = f"IBD-20/rs_opportunities_{date_str}.csv"
    generate_opportunity_report(source, output)
