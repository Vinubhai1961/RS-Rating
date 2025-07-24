#!/usr/bin/env python3
import os
import time
import json
import argparse
import logging
from tqdm import tqdm
from datetime import datetime
import pandas as pd
from yahooquery import Ticker
import arcticdb as adb

def fetch_historical_data(tickers, arctic, log_file):
    """Fetch 2 years of historical data and store in ArcticDB."""
    max_retries = 3
    batch_size = 200
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)

    total_batches = (len(tickers) + batch_size - 1) // batch_size
    est_time_per_batch = 21.15
    est_total_time = total_batches * est_time_per_batch / 60
    logging.info(f"Starting fetch for {len(tickers)} tickers in {total_batches} batches of {batch_size}, estimated time: {est_total_time:.2f} minutes")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches,
                  desc=f"Processing batches (size {batch_size}, ~{est_time_per_batch:.2f}s each)"):
        batch_start_time = time.time()
        batch = tickers[i:i + batch_size]
        batch_success = 0
        batch_skipped = 0

        for ticker in batch:
            for attempt in range(max_retries):
                try:
                    data = Ticker(ticker).history(period="2y")
                    if ticker in data.index.get_level_values(0):
                        df = data.loc[ticker].reset_index()
                        if df.empty:
                            skipped_tickers.append((ticker, "Empty DataFrame"))
                            batch_skipped += 1
                            break
                        df = df[["date", "close"]].rename(columns={"date": "datetime"})
                        # Force timezone-naive conversion
                        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(None)
                        df["datetime"] = df["datetime"].astype(int) // 10**9  # Convert to Unix timestamp
                        lib.write(ticker, df)
                        success_tickers.append(ticker)
                        batch_success += 1
                        break  # Success, move to next ticker
                    else:
                        skipped_tickers.append((ticker, f"No data in YahooQuery index (attempt {attempt+1})"))
                        batch_skipped += 1
                        break
                except Exception as e:
                    if attempt == max_retries - 1:
                        error_msg = f"Ticker {ticker} failed after {max_retries} attempts: {str(e)}"
                        logging.error(error_msg)
                        failed_tickers.append((ticker, error_msg))
                    else:
                        logging.warning(f"Retrying ticker {ticker} (attempt {attempt+2}/{max_retries}) after error: {str(e)}")
                        time.sleep(2)

        batch_time = time.time() - batch_start_time
        logging.info(f"✅ Completed batch {i//batch_size + 1}/{total_batches} - {batch_success} success, {batch_skipped} skipped, in {batch_time:.2f}s")

    # Write failed/skipped tickers to log
    with open(log_file, "a") as f:
        if skipped_tickers:
            f.write("\n--- Skipped Tickers ---\n")
            for ticker, reason in skipped_tickers:
                f.write(f"{ticker}: {reason}\n")
        if failed_tickers:
            f.write("\n--- Failed Tickers ---\n")
            for ticker, error in failed_tickers:
                f.write(f"{ticker}: {error}\n")

    # Final summary to both log and stdout
    total_success = len(success_tickers)
    total_skipped = len(skipped_tickers)
    total_failed = len(failed_tickers)
    logging.info(f"\n=== Fetch Summary ===")
    logging.info(f"Successful: {total_success}")
    logging.info(f"Skipped (no/empty data): {total_skipped}")
    logging.info(f"Failed after retries: {total_failed}")
    print(f"\n✅ Fetch complete! Success: {total_success}, Skipped: {total_skipped}, Failed: {total_failed}")
    
def load_ticker_list(file_path, partition=None, total_partitions=None):
    """Load tickers from JSON and optionally split into partitions."""
    with open(file_path, "r") as f:
        data = json.load(f)

    tickers = list(data.keys())

    if partition is not None and total_partitions:
        chunk_size = len(tickers) // total_partitions
        start = partition * chunk_size
        end = None if partition == total_partitions - 1 else start + chunk_size
        tickers = tickers[start:end]
        logging.info(f"Partition {partition}/{total_partitions}: {len(tickers)} tickers")

    return tickers

def main():
    parser = argparse.ArgumentParser(description="Fetch historical data into ArcticDB")
    parser.add_argument("input_file", help="Path to ticker_price.json")
    parser.add_argument("--log-file", default="logs/failed_tickers.log", help="Path to log file")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db", help="Directory to store ArcticDB")
    parser.add_argument("--partition", type=int, default=None, help="Partition index (0-based)")
    parser.add_argument("--total-partitions", type=int, default=None, help="Total number of partitions")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)

if __name__ == "__main__":
    main()
