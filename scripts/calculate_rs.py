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


def is_valid_data(data) -> bool:
    """Safely check if yahooquery returned usable data"""
    if data is None:
        return False
    if isinstance(data, dict):
        return len(data) > 0
    if isinstance(data, pd.DataFrame):
        return not data.empty
    return False


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
    pbar = tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Batches")

    for i in pbar:
        batch = tickers[i:i + batch_size]
        data = None
        success = False

        for attempt in range(max_retries):
            try:
                t = Ticker(batch, asynchronous=True, max_workers=10, progress=False, timeout=30)
                raw = t.history(period="2y", interval="1d")

                if is_valid_data(raw):
                    data = raw
                    success = True
                    break
                else:
                    raise ValueError("Empty or invalid response")

            except Exception as e:
                wait = 2 * (2 ** attempt)  # 2s, 4s, 8s
                logging.warning(f"Batch {i//batch_size + 1} | Try {attempt+1}/{max_retries} → {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    failed_tickers.extend([(tk, str(e)) for tk in batch])

        if not success:
            time.sleep(delay_between_batches)
            continue

        # Process batch
        for ticker in batch:
            try:
                if ticker not in data:
                    skipped_tickers.append((ticker, "Not returned"))
                    continue

                item = data[ticker]

                # Convert dict → DataFrame
                if isinstance(item, dict):
                    if not item:
                        skipped_tickers.append((ticker, "Empty dict"))
                        continue
                    df = pd.DataFrame(item)
                else:
                    df = item.copy() if isinstance(item, pd.DataFrame) else pd.DataFrame()

                if df.empty or len(df) < 30:
                    skipped_tickers.append((ticker, f"Short data: {len(df)}"))
                    continue

                df = df.reset_index()
                if 'date' not in df.columns and getattr(df.index, 'name', None) in ('date', 'Date'):
                    df = df.reset_index()

                if 'date' not in df.columns:
                    skipped_tickers.append((ticker, "No date column"))
                    continue

                # Use adjusted close
                df["close"] = df.get("adjclose", df.get("close"))
                if df["close"].isna().all():
                    skipped_tickers.append((ticker, "No price"))
                    continue

                df = df[["date", "open", "high", "low", "close", "volume"]].copy()
                df["datetime"] = pd.to_datetime(df["date"], utc=True).astype("int64") // 1_000_000_000
                df = df.drop(columns=["date"])
                df = df[["datetime", "open", "high", "low", "close", "volume"]]

                lib.write(ticker, df)
                success_tickers.append(ticker)

            except Exception as e:
                failed_tickers.append((ticker, f"Parse error: {e}"))

        time.sleep(delay_between_batches)
        pbar.set_postfix({
            "OK": len(success_tickers),
            "Skip": len(skipped_tickers),
            "Fail": len(failed_tickers)
        })

    # Final summary
    print(f"\nFETCH SUCCESS!")
    print(f"   Success : {len(success_tickers):,} tickers")
    print(f"   Skipped : {len(skipped_tickers)}")
    print(f"   Failed  : {len(failed_tickers)}")


def load_ticker_list(file_path, partition=None, total_partitions=None):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tickers = [item["ticker"] if isinstance(item, dict) else item for item in data if item]

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

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s | %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    if not tickers:
        print("No tickers!")
        return

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log_file)


if __name__ == "__main__":
    main()
