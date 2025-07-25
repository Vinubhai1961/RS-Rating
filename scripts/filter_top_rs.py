import pandas as pd
import sys
import os
from datetime import datetime

def calc_consistency(row):
    rs_values = [
        row['Relative Strength Percentile'],
        row.get('1 Month Ago Percentile', None),
        row.get('3 Months Ago Percentile', None),
        row.get('6 Months Ago Percentile', None)
    ]
    valid_values = [v for v in rs_values if pd.notnull(v)]
    return sum(valid_values) / len(valid_values) if len(valid_values) >= 2 else None

def main():
    # Get current date for file naming
    current_date = datetime.today().strftime('%m%d%Y')
    
    # Default input and output paths
    input_file = f"rs_results/archive/rs_stocks_{current_date}.csv"
    output_file = f"IBD-20/top_rs_stocks_{current_date}.csv"
    
    # Override with command-line arguments if provided
    if len(sys.argv) == 5 and sys.argv[1] == '--input' and sys.argv[3] == '--output':
        input_file = sys.argv[2]
        output_file = sys.argv[4]
    
    # Load CSV
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)
    data = pd.read_csv(input_file)
    
    # Calculate Consistency Score
    data['Consistency Score'] = data.apply(calc_consistency, axis=1)
    
    # Filter for RS 90–99 and Consistency Score > 80
    filtered = data[
        (data['Relative Strength Percentile'] >= 90) &
        (data['Relative Strength Percentile'] <= 99) &
        (data['Consistency Score'] > 80) &
        (data[['1 Month Ago Percentile', '3 Months Ago Percentile', '6 Months Ago Percentile']].notnull().sum(axis=1) >= 1)
    ]
    
    # Sort by Consistency Score and Rank, select top 20
    top_20 = filtered.sort_values(by=['Consistency Score', 'Rank'], ascending=[False, True]).head(20)
    
    # Select relevant columns and round Consistency Score
    columns = ['Rank', 'Ticker', 'Price', 'Sector', 'Industry', 'Relative Strength Percentile', 'Consistency Score']
    top_20 = top_20[columns].round({'Consistency Score': 2})
    
    # Create IBD-20 directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save to CSV
    top_20.to_csv(output_file, index=False)
    print(f"Top 20 stocks saved to {output_file}")

if __name__ == "__main__":
    main()
