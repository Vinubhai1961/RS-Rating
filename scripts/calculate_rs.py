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
    batch_size = 200                    # Reduced from 200 → safer for Yahoo
    delay_between_batches = 1.5         # Critical: avoid bans
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    logging.info(f"Starting fetch for {len(tickers):,} tickers in {total_batches} batches of {batch_size}")
    print(f"Fetching {len(tickers):,} tickers → batches of {batch_size} (with delays)")

    pbar = tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Batches")

    for i in pbar:
        batch = tickers[i:i + batch_size]
        data = None

        for attempt in range(max_retries):
            try:
                # yahooquery auto-retries internally, but we control batch size + delay
                t = Ticker(batch, asynchronous=True, max_workers=8, progress=False)
                data = t.history(period="2y", interval="1d")
                if data is not None and not data.empty:
                    break
            except Exception as e:
                wait = 5 * (attempt + 1)
                logging.warning(f"Batch failed (attempt {attempt+1}): {e} — waiting {wait}s")
                time.sleep(wait)

        if data is None or data.empty:
            failed_tickers.extend([(t, "No response") for t in batch])
            continue

        batch_success = 0
        for ticker in batch:
            try:
                if ticker not in data.index.get_level_values('symbol'):
                    skipped_tickers.append((ticker, "Not in response"))
                    continue

                df = data.xs(ticker, level='symbol').copy()

                if df.empty or len(df) < 10:
                    skipped_tickers.append((ticker, f"Too few rows: {len(df)}"))
                    continue

                # Critical fix: Yahoo gives 'adjclose', Arctic expects 'close'
                df = df.rename(columns={
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',           # unadjusted
                    'adjclose': 'close',        # we want adjusted close
                    'volume': 'volume'
                })

                # Use only needed columns
                df = df[['open', 'high', 'low', 'close', 'volume']].reset_index()
                df['datetime'] = pd.to_datetime(df['date'], utc=True).astype('int64') // 10**9
                df = df.drop(columns=['date'])

                # Final schema
                df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

                lib.write(ticker, df, metadata={"source": "yahooquery", "fetched": time.time()})
                success_tickers.append(ticker)
                batch_success += 1

            except Exception as e:
                failed_tickers.append((ticker, str(e)))

        # Critical: delay between batches
        time.sleep(delay_between_batches)

        pbar.set_postfix({
            "OK": len(success_tickers),
            "Skip": len(skipped_tickers),
            "Fail": len(failed_tickers)
        })

    # Final log
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"\n=== Fetch Summary {time.strftime('%Y-%m-%d %H:%M')} ===\n")
        f.write(f"Success: {len(success_tickers)}\n")
        f.write(f"Skipped: {len(skipped_tickers)}\n")
        f.write(f"Failed: {len(failed_tickers)}\n")
        if skipped_tickers:
            f.write("\n--- Skipped ---\n")
            for t, r in skipped_tickers[:50]:
                f.write(f"{t}: {r}\n")
        if failed_tickers:
            f.write("\n--- Failed ---\n")
            for t, e in failed_tickers[:50]:
                f.write(f"{t}: {e}\n")

    print(f"\nDONE! Success: {len(success_tickers):,} | Skipped: {len(skipped_tickers)} | Failed: {len(failed_tickers)}")
    logging.info("Fetch completed.")


def load_ticker_list(file_path, partition=None, total_partitions=None):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tickers = [item["ticker"] for item in data if "ticker" in item]

    if partition is not None and total_partitions:
        chunk = len(tickers) // total_partitions
        start = partition * chunk
        end = None if partition == total_partitions - 1 else start + chunk
        tickers = tickers[start:end]
        logging.info(f"Partition {partition + 1}/{total_partitions}: {len(tickers)} tickers")

    return tickers


def main():
    parser = argparse.ArgumentParser(description="Fetch 2Y daily data → ArcticDB (yahooquery)")
    parser.add_argument("input_file", help="Path to ticker JSON file")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db/", help="ArcticDB path")
    parser.add_argument("--log-file", default="logs/fetch.log", help="Log file")
    parser.add_argument("--partition", type=int, help="Partition index (0-based)")
    parser.add_argument("--total-partitions", type=int, help="Total partitions")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Also print to console

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    if not tickers:
        print("No tickers loaded!")
        return

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)


if __name__ == "__main__":
    main()
