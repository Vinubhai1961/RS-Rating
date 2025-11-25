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
    max_retries = 4
    delay_base = 3  # seconds

    success_total = 0
    skipped_total = 0
    failed_total = 0

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    print(f"Starting fetch: {len(tickers):,} tickers → {total_batches} batches of {batch_size}")
    pbar = tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Batches", leave=True)

    for i in pbar:
        batch = tickers[i:i + batch_size]
        data = None
        batch_success = batch_skipped = batch_failed = 0

        # Retry loop with exponential backoff
        for attempt in range(max_retries):
            try:
                data = Ticker(batch).history(period="2y", interval="1d")
                if data is not None and not data.empty:
                    break
            except Exception as e:
                wait = delay_base * (2 ** attempt)
                logging.warning(f"Batch {i//batch_size + 1} | Retry {attempt + 1}/{max_retries} in {wait}s → {e}")
                time.sleep(wait)

        if data is None or data.empty:
            batch_failed = len(batch)
            failed_total += batch_failed
            pbar.set_postfix({"OK": success_total, "Skip": skipped_total, "Fail": failed_total})
            continue

        # Process each ticker in batch
        for ticker in batch:
            try:
                if ticker not in data.index.get_level_values(0):
                    skipped_total += 1
                    batch_skipped += 1
                    continue

                df = data.loc[ticker].copy()
                if isinstance(df, pd.Series):
                    df = df.to_frame().T
                if df.empty:
                    skipped_total += 1
                    batch_skipped += 1
                    continue

                df = df.reset_index()
                if 'date' not in df.columns:
                    df = df.reset_index()

                # CRITICAL: Use adjusted close
                df["close"] = df.get("adjclose", df.get("close"))

                df = df[["date", "open", "high", "low", "close", "volume"]].copy()
                df["datetime"] = pd.to_datetime(df["date"], utc=True).astype("int64") // 1_000_000_000
                df = df.drop(columns=["date"])
                df = df[["datetime", "open", "high", "low", "close", "volume"]]

                lib.write(ticker, df)
                success_total += 1
                batch_success += 1

            except Exception as e:
                failed_total += 1
                batch_failed += 1
                logging.error(f"{ticker}: {e}")

        # Live update
        pbar.set_postfix({
            "OK": success_total,
            "Skip": skipped_total,
            "Fail": failed_total,
            "Rate": f"{success_total / max(1, (time.time() - pbar.start_t)):.1f}/s"
        })

    # Final result
    print(f"\nFETCH COMPLETE!")
    print(f"   Success : {success_total:,} tickers")
    print(f"   Skipped : {skipped_total}")
    print(f"   Failed  : {failed_total}")
    logging.info(f"FINAL: Success={success_total} | Skip={skipped_total} | Fail={failed_total}")


def load_ticker_list, main() → same as before
# (copy from previous version — no change needed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db")
    parser.add_argument("--log-file", default="logs/fetch.log")
    parser.add_argument("--partition", type=int)
    parser.add_argument("--total-partitions", type=int)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    logging.basicConfig(
        filename=args.log,
        level=logging.INFO,
        format="%(asctime)s | %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    if not tickers:
        print("No tickers!")
        return

    # Add start time for rate
    tqdm.tqdm.start_t = time.time()

    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    fetch_historical_data(tickers, arctic, args.log)


if __name__ == "__main__":
    main()
