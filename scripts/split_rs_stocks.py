import pandas as pd
import os
import math

# Configuration
INPUT_CSV = "RS_Data/rs_stocks.csv"
OUTPUT_DIR = "RS_Data"
CHUNK_SIZE = 4500

def split_csv():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Read the CSV file
    try:
        df = pd.read_csv(INPUT_CSV)
        print(f"Read {len(df)} records from {INPUT_CSV}")
    except FileNotFoundError:
        print(f"Error: {INPUT_CSV} not found")
        exit(1)
    
    # Calculate number of chunks
    total_rows = len(df)
    num_chunks = math.ceil(total_rows / CHUNK_SIZE)
    
    # Split and save chunks
    for i in range(num_chunks):
        start_idx = i * CHUNK_SIZE
        end_idx = min((i + 1) * CHUNK_SIZE, total_rows)
        chunk = df.iloc[start_idx:end_idx]
        output_path = os.path.join(OUTPUT_DIR, f"rs_stocks_part{i+1}.csv")
        chunk.to_csv(output_path, index=False, float_format='%.2f', na_rep="")
        print(f"Saved {len(chunk)} rows to {output_path}")

if __name__ == "__main__":
    split_csv()
