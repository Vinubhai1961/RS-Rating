#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import argparse
import logging
import traceback
from datetime import datetime
import pandas as pd
from yahooquery import Ticker
import sys
import time
from tqdm.auto import tqdm
import arcticdb as adb

def validate_arctic_data(arctic_lib, log_file):
    """Validate ArcticDB data by logging top 10 tickers by data point count."""
    arctic = adb.Arctic("lmdb://tmp/arctic_db")
    if not arctic.has_library("prices"):
        logging.error("ArcticDB library 'prices' not found for validation")
        return
    lib = arctic.get_library("prices")
    
    ticker_counts = []
    for symbol in lib.list_symbols():
        try:
            data = lib.read(symbol).data
            count = len(data)
            latest_date = datetime.fromtimestamp(data["datetime"].max()).strftime("%Y-%m-%d")
            ticker_counts.append((symbol, count, latest_date))
        except Exception as e:
            logging.info(f"Validation failed for {symbol}: {str(e)}")
    
    ticker_counts.sort(key=lambda x: x[1], reverse=True)
    top_10 = ticker_counts[:10]
    
    logging.info("Top 10 tickers by data point count:")
    for ticker, count, latest_date in top_10:
        logging.info(f"Ticker: {ticker}, Data Points: {count}, Latest Date: {latest_date}")

def fetch_historical_data(tickers, arctic_lib, log_file):
    """Fetch 2 years of historical data and store in ArcticDB."""
    max_retries = 3
    batch_size = 200
    failed_tickers = []
    
    arctic = adb.Arctic("lmdb://tmp/arctic_db")
    if not arctic.has_library("prices"):
        arctic.create_library("prices")
    lib = arctic.get_library("prices", create_if_missing=True)
    
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    est_time_per_batch = 21.15
    est_total_time = total_batches * est_time_per_batch / 60
    logging.info(f"Starting fetch for {len(tickers)} tickers in {total_batches} batches of {batch_size}, estimated time: {est_total_time:.2f} minutes")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, 
                  desc=f"Processing batches (size {batch_size}, ~{est_time_per_batch:.2f}s each)"):
        batch_start_time = time.time()
        batch = tickers[i:i + batch_size]
        for attempt in range(max_retries):
            try:
                data = Ticker(batch).history(period="2y")
                for ticker in batch:
                    if ticker in data.index.get_level_values(0):
                        df = data.loc[ticker].reset_index()
                        df = df[["date", "close"]].rename(columns={"date": "datetime"})
                        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None).astype(int) // 10**9
                        lib.write(ticker, df)
                    else:
                        failed_tickers.append((ticker, f"No data on attempt {attempt + 1}"))
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    error_msg = f"Batch {i//batch_size + 1} failed after {max_retries} attempts: {str(e)}\n{traceback.format_exc()}"
                    logging.error(error_msg)
                    failed_tickers.extend((t, error_msg) for t in batch)
                else:
                    time.sleep(10)  # Increased delay for rate limits
        batch_time = time.time() - batch_start_time
        logging.info(f"Completed batch {i//batch_size + 1}/{total_batches} ({len(batch)} tickers) in {batch_time:.2f} seconds")

    if failed_tickers:
        with open(log_file, "a") as f:
            for ticker, error in failed_tickers:
                f.write(f"{ticker}: {error}\n")
    
    validate_arctic_data(arctic_lib, log_file)

def main(input_file, log_file, partition, total_partitions):
    """Fetch historical data for a partition and store in ArcticDB."""
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} not found")
        sys.exit(1)
    with open(input_file, "r") as f:
        data = json.load(f)
    
    tickers = list(data.keys())
    partition_size = len(tickers) // total_partitions + (1 if len(tickers) % total_partitions else 0)
    start_idx = partition * partition_size
    end_idx = min((partition + 1) * partition_size, len(tickers))
    partition_tickers = tickers[start_idx:end_idx]
    if "SPY" not in partition_tickers:
        partition_tickers.append("SPY")
    
    os.makedirs("tmp/arctic_db", exist_ok=True)
    fetch_historical_data(partition_tickers, "tmp/arctic_db/prices", log_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical data for a partition and store in ArcticDB")
    parser.add_argument("input_file", help="Path to ticker_price.json")
    parser.add_argument("--log-file", default="logs/failed_tickers.log", help="Log file for errors")
    parser.add_argument("--partition", type=int, default=0, help="Partition number (0-based)")
    parser.add_argument("--total-partitions", type=int, default=4, help="Total number of partitions")
    args = parser.parse_args()
    main(args.input_file, args.log_file, args.partition, args.total_partitions)
