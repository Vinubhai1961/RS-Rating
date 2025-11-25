#!/usr/bin/env python3
import os
import time
import json
import argparse
import logging
from tqdm import tqdm
from yahooquery import Ticker
import pandas as pd
import arcticdb as adb

def fetch_historical_data(tickers, arctic, log_file):
    max_retries = 3
    batch_size = 200
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    logging.info(f"Starting fetch for {len(tickers)} tickers in {total_batches} batches of {batch_size}")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Fetching batches"):
        batch = tickers[i:i + batch_size]
        batch_success = 0
        batch_skipped = 0
        batch_start_time = time.time()

        for attempt in range(max_retries):
            try:
                data = Ticker(batch).history(period="2y")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Batch {i//batch_size + 1} failed: {str(e)}")
                    failed_tickers.extend([(t, str(e)) for t in batch])
                    data = None
                else:
                    logging.warning(f"Retrying batch {i//batch_size + 1} (attempt {attempt+2}/{max_retries}) after error: {str(e)}")
                    time.sleep(2)

        if data is None:
            continue  # skip this batch

        for ticker in batch:
            try:
                if ticker not in data.index.get_level_values(0):
                    skipped_tickers.append((ticker, "No data returned"))
                    batch_skipped += 1
                    continue

                df = data.loc[ticker].reset_index()

                if df.empty:
                    skipped_tickers.append((ticker, "Empty DataFrame"))
                    batch_skipped += 1
                    continue

                # Convert date column to Unix timestamp in seconds
                df = df.rename(columns={"date": "datetime"})
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True).astype(int) // 10**9

                lib.write(ticker, df)
                success_tickers.append(ticker)
                batch_success += 1

            except Exception as e:
                failed_tickers.append((ticker, str(e)))

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

    logging.info("\n=== Fetch Summary ===")
    logging.info(f"Successful: {len(success_tickers)}")
    logging.info(f"Skipped: {len(skipped_tickers)}")
    logging.info(f"Failed: {len(failed_tickers)}")
    print(f"\n✅ Fetch complete! Success: {len(success_tickers)}, Skipped: {len(skipped_tickers)}, Failed: {len(failed_tickers)}")

def load_ticker_list(file_path, partition=None, total_partitions=None):
    with open(file_path, "r") as f:
        data = json.load(f)
    tickers = [item["ticker"] for item in data]  # Extract tickers from array

    if partition is not None and total_partitions:
        chunk_size = len(tickers) // total_partitions
        start = partition * chunk_size
        end = None if partition == total_partitions - 1 else start + chunk_size
        tickers = tickers[start:end]
        logging.info(f"Partition {partition}/{total_partitions}: {len(tickers)} tickers")

    return tickers

def main():
    parser = argparse.ArgumentParser(description="Fetch Yahoo historical data and store in ArcticDB")
    parser.add_argument("input_file", help="Path to ticker_price.json")
    parser.add_argument("--log-file", default="logs/fetch_log.log", help="Path to log file")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db", help="Directory for ArcticDB")
    parser.add_argument("--partition", type=int, default=None, help="Partition index (0-based)")
    parser.add_argument("--total-partitions", type=int, default=None, help="Total number of partitions")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(filename=args.log_file, level=logging.INFO, format="%(asctime)s - %(message)s")

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")  # Fixed typo: arctic_db-path to arctic_db_path
    fetch_historical_data(tickers, arctic, args.log_file)

if __name__ == "__main__":
    main()
