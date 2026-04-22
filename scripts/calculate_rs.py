#!/usr/bin/env python3
import os
import time
import json
import argparse
import logging
from tqdm import tqdm
from yahooquery import Ticker
import pandas as pd
import numpy as np
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

        # ================= FETCH WITH RETRY =================
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
            continue

        # ================= PROCESS EACH TICKER =================
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

                # ================= CLEANING PIPELINE =================
                df = df.rename(columns={"date": "datetime"})
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

                # Sort (critical)
                df = df.sort_values("datetime")

                # Remove duplicates
                before_dupes = len(df)
                df = df.drop_duplicates(subset=["datetime"])
                dupes_removed = before_dupes - len(df)

                # Replace inf values
                df = df.replace([np.inf, -np.inf], np.nan)

                # Count NaNs before drop
                nan_count = df["close"].isna().sum()

                # Drop NaN closes
                df = df.dropna(subset=["close"])

                # Remove zero/negative prices (critical)
                invalid_price_count = (df["close"] <= 0).sum()
                df = df[df["close"] > 0]

                # Optional: forward fill small gaps (safe)
                df["close"] = df["close"].ffill()

                # Minimum data check (important for RS)
                if len(df) < 5:
                    skipped_tickers.append((ticker, f"Too few valid rows: {len(df)}"))
                    batch_skipped += 1
                    continue

                # Convert datetime to UNIX timestamp (seconds)
                df["datetime"] = df["datetime"].astype("int64") // 10**9

                # ================= WRITE TO DB =================
                lib.write(ticker, df)
                success_tickers.append(ticker)
                batch_success += 1

                # ================= DEBUG LOGGING =================
                if nan_count > 0 or invalid_price_count > 0 or dupes_removed > 0:
                    logging.info(
                        f"{ticker}: cleaned → "
                        f"NaN_removed={nan_count}, "
                        f"invalid_price_removed={invalid_price_count}, "
                        f"duplicates_removed={dupes_removed}, "
                        f"final_rows={len(df)}"
                    )

            except Exception as e:
                failed_tickers.append((ticker, str(e)))

        batch_time = time.time() - batch_start_time
        logging.info(
            f"✅ Completed batch {i//batch_size + 1}/{total_batches} - "
            f"{batch_success} success, {batch_skipped} skipped, in {batch_time:.2f}s"
        )

    # ================= FINAL LOGGING =================
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

    tickers = [item["ticker"] for item in data]

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
    parser.add_argument("--partition", type=int, default=None)
    parser.add_argument("--total-partitions", type=int, default=None)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        filemode="a"
    )

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)


if __name__ == "__main__":
    main()
