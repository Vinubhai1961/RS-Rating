#!/usr/bin/env python3
import os
import logging
import shutil
from datetime import datetime
import argparse
import arcticdb as adb

def validate_arctic_data(arctic_root, log_file):
    """Validate ArcticDB data and log top 10 tickers by data point count."""
    
    # Setup logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    # Ensure URI format is correct
    if not arctic_root.startswith("lmdb://"):
        arctic_root = f"lmdb://{arctic_root}"
    
    # Try connecting to ArcticDB
    try:
        arctic = adb.Arctic(arctic_root)
        if not arctic.has_library("prices"):
            msg = f"ArcticDB library 'prices' not found at {arctic_root}"
            logging.warning(msg)
            print(msg)
            return
        lib = arctic.get_library("prices")
    except Exception as e:
        msg = f"Failed to access ArcticDB at {arctic_root}: {str(e)}"
        logging.warning(msg)
        print(msg)
        return

    # Collect symbol data
    ticker_counts = []
    valid_tickers = 0
    expected_tickers = 8900  # approximate expected total

    for symbol in lib.list_symbols():
        try:
            df = lib.read(symbol).data
            count = len(df)
            if count >= 2:
                valid_tickers += 1
            latest_ts = df["datetime"].max()
            latest_date = datetime.fromtimestamp(latest_ts).strftime("%Y-%m-%d")
            ticker_counts.append((symbol, count, latest_date))
        except Exception as e:
            logging.info(f"Validation failed for {symbol}: {str(e)}")

    # Summary
    total_tickers = len(ticker_counts)
    print(f"Total tickers in ArcticDB: {total_tickers}")
    print(f"Valid tickers (≥2 data points): {valid_tickers}")
    logging.info(f"Total tickers: {total_tickers}")
    logging.info(f"Valid tickers (≥2 data points): {valid_tickers}")

    if valid_tickers < expected_tickers * 0.5:
        msg = f"Warning: Only {valid_tickers}/{expected_tickers} tickers have sufficient data (<50%)"
        logging.warning(msg)
        print(msg)

    # Top 10 tickers by row count
    ticker_counts.sort(key=lambda x: x[1], reverse=True)
    top_10 = ticker_counts[:10]
    print("\nTop 10 tickers by data point count:")
    logging.info("Top 10 tickers by data point count:")
    for symbol, count, latest in top_10:
        print(f"{symbol}: {count} rows, latest date = {latest}")
        logging.info(f"{symbol}: {count} rows, latest = {latest}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate ArcticDB prices library")
    parser.add_argument("--log-file", default="logs/validate_arctic.log", help="Log file path")
    parser.add_argument("--arctic-path", default="tmp/arctic_db", help="Path to ArcticDB root (no lmdb:// needed)")
    args = parser.parse_args()

    # Ensure log directory exists
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)

    # Log the validation start time
    with open(args.log_file, 'a') as f:
        f.write(f"\n--- Validation started at {datetime.now()} ---\n")

    validate_arctic_data(args.arctic_path, args.log_file)
