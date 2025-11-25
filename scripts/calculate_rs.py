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
    batch_size = 200
    max_retries = 3
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    print(f"Fetching {len(tickers):,} tickers → {total_batches} batches of {batch_size}")
    logging.info(f"Starting fetch: {len(tickers)} tickers")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Batches"):
        batch = tickers[i:i + batch_size]
        data = None

        for attempt in range(max_retries):
            try:
                # YOUR WINNING LINE — NEVER CHANGE THIS
                data = Ticker(batch).history(period="2y", interval="1d")
                if data is not None and not data.empty:
                    break
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Batch failed permanently: {e}")
                    failed_tickers.extend([(t, str(e)) for t in batch])
                else:
                    logging.warning(f"Retry {attempt+1} for batch...")
                    time.sleep(2)

        if data is None or data.empty:
            continue

        for ticker in batch:
            try:
                # YOUR WINNING LINE — NEVER CHANGE THIS EITHER
                if ticker not in data.index.get_level_values(0):
                    skipped_tickers.append((ticker, "No data"))
                    continue

                df = data.loc[ticker].copy()
                if isinstance(df, pd.Series):
                    df = df.to_frame().T

                if df.empty:
                    skipped_tickers.append((ticker, "Empty"))
                    continue

                df = df.reset_index()
                if 'date' not in df.columns:
                    df = df.reset_index()

                # Use adjusted close as 'close'
                df["close"] = df.get("adjclose", df.get("close"))

                # Final schema
                df = df[["date", "open", "high", "low", "close", "volume"]].copy()
                df["datetime"] = pd.to_datetime(df["date"], utc=True).astype("int64") // 1_000_000_000
                df = df.drop(columns=["date"])
                df = df[["datetime", "open", "high", "low", "close", "volume"]]

                lib.write(ticker, df)
                success_tickers.append(ticker)

            except Exception as e:
                failed_tickers.append((ticker, str(e)))

        # Progress
        tqdm.write(f"Batch {i//batch_size + 1}/{total_batches} → OK: {len(success_tickers)} | Skip: {len(skipped_tickers)}")

    print(f"\nDONE! Success: {len(success_tickers):,} | Skipped: {len(skipped_tickers)} | Failed: {len(failed_tickers)}")


def load_ticker_list(file_path, partition=None, total_partitions=None):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    tickers = [item["ticker"] if isinstance(item, dict) else item for item in data]

    if partition is not None and total_partitions and total_partitions > 1:
        chunk = len(tickers) // total_partitions
        start = partition * chunk
        end = None if partition == total_partitions - 1 else start + chunk
        tickers = tickers[start:end]
        print(f"Partition {partition + 1}/{total_partitions}: {len(tickers):,} tickers")

    return tickers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="Path to ticker JSON")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db", help="e.g. tmp/arctic_db_0")
    parser.add_argument("--log-file", default="logs/fetch.log")
    parser.add_argument("--partition", type=int)
    parser.add_argument("--total-partitions", type=int)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(filename=args.log_file, level=logging.INFO,
                        format="%(asctime)s | %(message)s")
    logging.getLogger().addHandler(logging.StreamHandler())

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    if not tickers:
        print("No tickers!")
        return

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)


if __name__ == "__main__":
    main()
