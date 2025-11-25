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
    delay_between_batches = 1.0
    max_retries = 3
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    print(f"Starting fetch: {len(tickers):,} tickers → {total_batches} batches of {batch_size}")
    logging.info(f"Fetch started: {len(tickers)} tickers, {total_batches} batches")

    pbar = tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Batches")

    for i in pbar:
        batch = tickers[i:i + batch_size]
        data = None
        success = False

        # === Retry Logic ===
        for attempt in range(max_retries):
            try:
                ticker_obj = Ticker(batch, asynchronous=True, max_workers=10, progress=False)
                data = ticker_obj.history(period="2y", interval="1d")

                if data and len(data) > 0:
                    success = True
                    break
                else:
                    raise Exception("Empty response from Yahoo")

            except Exception as e:
                wait = (2 ** attempt) + 1  # 1s, 3s, 7s
                logging.warning(f"Batch {i//batch_size + 1} | Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    logging.error(f"Batch permanently failed after {max_retries} attempts")
                    failed_tickers.extend([(tk, f"Request failed: {e}") for tk in batch])

        if not success:
            time.sleep(delay_between_batches)
            continue

        # === Process Successful Batch ===
        batch_success = 0
        for ticker in batch:
            try:
                if ticker not in data:
                    skipped_tickers.append((ticker, "Not in response"))
                    continue

                raw = data[ticker]

                # Handle both dict and DataFrame responses
                if isinstance(raw, dict):
                    if not raw:
                        skipped_tickers.append((ticker, "Empty dict"))
                        continue
                    df = pd.DataFrame(raw)
                else:
                    df = raw.copy()

                if df.empty or len(df) < 20:
                    skipped_tickers.append((ticker, f"Too few rows: {len(df)}"))
                    continue

                df = df.reset_index()

                # Ensure 'date' column exists
                if 'date' not in df.columns:
                    if df.index.name == 'date':
                        df = df.reset_index()
                    else:
                        skipped_tickers.append((ticker, "No date column"))
                        continue

                # Use adjusted close
                df["close"] = df.get("adjclose", df.get("close"))

                # Final clean DataFrame
                df = df[["date", "open", "high", "low", "close", "volume"]].copy()
                df["datetime"] = pd.to_datetime(df["date"], utc=True).astype("int64") // 1_000_000_000
                df = df.drop(columns=["date"])
                df = df[["datetime", "open", "high", "low", "close", "volume"]]

                # Write to ArcticDB
                lib.write(ticker, df, metadata={"source": "yahooquery", "fetched": time.time()})
                success_tickers.append(ticker)
                batch_success += 1

            except Exception as e:
                failed_tickers.append((ticker, f"Parse error: {e}"))

        time.sleep(delay_between_batches)
        pbar.set_postfix({
            "OK": len(success_tickers),
            "Skip": len(skipped_tickers),
            "Fail": len(failed_tickers)
        })

    # === Final Summary ===
    total = len(success_tickers) + len(skipped_tickers) + len(failed_tickers)
    print(f"\nFETCH COMPLETE!")
    print(f"   Success: {len(success_tickers):,} / {total:,}")
    print(f"   Skipped: {len(skipped_tickers)}")
    print(f"   Failed : {len(failed_tickers)}")

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"FETCH SUMMARY - {time.strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Success: {len(success_tickers)} | Skipped: {len(skipped_tickers)} | Failed: {len(failed_tickers)}\n")
        if failed_tickers:
            f.write("\n--- Failed Tickers ---\n")
            for t, e in failed_tickers[:100]:
                f.write(f"{t}: {e}\n")


def load_ticker_list(file_path, partition=None, total_partitions=None):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    tickers = [item["ticker"] for item in data if isinstance(item, dict) and "ticker" in item]

    if partition is not None and total_partitions is not None and total_partitions > 1:
        chunk = len(tickers) // total_partitions
        start = partition * chunk
        end = None if partition == total_partitions - 1 else start + chunk
        tickers = tickers[start:end]
        print(f"Partition {partition + 1}/{total_partitions}: {len(tickers):,} tickers")

    return tickers


def main():
    parser = argparse.ArgumentParser(description="Fetch 2Y daily data → ArcticDB (yahooquery)")
    parser.add_argument("input_file", help="Path to ticker JSON file (e.g. ticker_price.json)")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db", help="Output folder, e.g. tmp/arctic_db_0")
    parser.add_argument("--log-file", default="logs/fetch.log", help="Log file")
    parser.add_argument("--partition", type=int, help="Partition index (0-based)")
    parser.add_argument("--total-partitions", type=int, help="Total number of partitions")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s | %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    if not tickers:
        print("No tickers found!")
        return

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)


if __name__ == "__main__":
    main()
