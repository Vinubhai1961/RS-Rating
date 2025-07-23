#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from yahooquery import Ticker
import sys
import time
from tqdm.auto import tqdm
import arcticdb as adb

def fetch_historical_data(tickers, arctic_lib, log_file):
    """Fetch 2 years of historical data and store in ArcticDB."""
    max_retries = 3
    batch_size = 200
    failed_tickers = []
    
    # Setup ArcticDB
    arctic = adb.Arctic("lmdb://tmp/arctic_db")
    if not arctic.has_library("prices"):
        arctic.create_library("prices")
    lib = arctic.get_library("prices", create_if_missing=True)
    
    # Calculate total batches and log pre-run info
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    logging.info(f"Starting partition with {len(tickers)} tickers, batch size: {batch_size}, total batches: {total_batches}")
    estimated_batch_time = 25  # Initial estimate in seconds
    logging.info(f"Estimated time per batch: ~{estimated_batch_time} seconds")
    logging.info(f"Estimated total time: ~{(total_batches * estimated_batch_time) // 60} minutes {(total_batches * estimated_batch_time) % 60} seconds")

    batch_times = []
    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Processing batches"):
        batch_start = time.time()
        batch = tickers[i:i + batch_size]
        logging.info(f"Processing batch {i // batch_size + 1}/{total_batches} ({len(batch)} tickers)")
        
        for attempt in range(max_retries):
            try:
                data = Ticker(batch).history(period="2y")
                for ticker in batch:
                    if ticker in data.index.get_level_values(0):
                        df = data.loc[ticker].reset_index()
                        df = df[["date", "close"]].rename(columns={"date": "datetime"})
                        df["datetime"] = pd.to_datetime(df["datetime"]).astype(int) // 10**9
                        lib.write(ticker, df)
                    else:
                        failed_tickers.append((ticker, f"No data on attempt {attempt + 1}"))
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    failed_tickers.extend((t, str(e)) for t in batch)
                else:
                    time.sleep(5)
        
        batch_time = time.time() - batch_start
        batch_times.append(batch_time)
        avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else estimated_batch_time
        remaining_batches = total_batches - (i // batch_size + 1)
        eta_seconds = int(remaining_batches * avg_batch_time)
        logging.info(f"Batch {i // batch_size + 1} completed in {batch_time:.2f} seconds, "
                     f"ETA for partition: ~{eta_seconds // 60} minutes {eta_seconds % 60} seconds")

    if failed_tickers:
        with open(log_file, "a") as f:
            for ticker, error in failed_tickers:
                f.write(f"{ticker}: {error}\n")

def main(input_file, log_file, partition, total_partitions):
    """Fetch historical data for a partition and store in ArcticDB."""
    # Setup logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    # Load ticker_price.json
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} not found")
        sys.exit(1)
    with open(input_file, "r") as f:
        data = json.load(f)
    
    # Split tickers for partition
    tickers = list(data.keys())
    logging.info(f"Total tickers in input: {len(tickers)}, partitions: {total_partitions}")
    partition_size = len(tickers) // total_partitions + (1 if len(tickers) % total_partitions else 0)
    start_idx = partition * partition_size
    end_idx = min((partition + 1) * partition_size, len(tickers))
    partition_tickers = tickers[start_idx:end_idx]
    if "SPY" not in partition_tickers and partition == 0:
        partition_tickers.append("SPY")  # Include SPY in first partition
    logging.info(f"Partition {partition} processing tickers {start_idx} to {end_idx-1} ({len(partition_tickers)} tickers)")
    
    # Fetch and store data
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
