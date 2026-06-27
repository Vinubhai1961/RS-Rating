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


# ================== SPECIAL TICKERS (Force Save Short History) ==================
# Keep this small. Add IPO/SPAC/benchmark tickers here only when you intentionally
# want to save symbols with fewer than MIN_VALID_ROWS rows.
SPECIAL_TICKERS = {"SPCX", "SPY", "QQQ"}

# USA should be tolerant, but still avoid saving broken one-row symbols.
MIN_VALID_ROWS = 2

# Benchmark/index tickers to print extra Yahoo/debug diagnostics for.
# This helps catch cases where stocks update to a newer date, but SPY/QQQ
# stays one trading day behind and causes aligned RS to stop early.
DEBUG_BENCHMARK_TICKERS = {"SPY", "QQQ"}


def debug_benchmark_frame(ticker: str, df: pd.DataFrame, stage: str):
    """Print and log tail rows for benchmark symbols such as SPY/QQQ."""
    if ticker not in DEBUG_BENCHMARK_TICKERS:
        return

    try:
        cols = [
            c for c in [
                "datetime", "date", "open", "high", "low",
                "close", "adjclose", "volume"
            ]
            if c in df.columns
        ]

        if cols:
            tail_txt = df[cols].tail(10).to_string(index=False)
        else:
            tail_txt = df.tail(10).to_string(index=False)

        msg = f"\nDEBUG {ticker} {stage} tail rows:\n{tail_txt}"
        print(msg)
        logging.info(msg)

    except Exception as e:
        logging.warning(f"DEBUG {ticker} {stage} failed: {e}")


def fetch_historical_data(tickers, arctic, log_file):
    max_retries = 3
    batch_size = 200
    failed_tickers = []
    skipped_tickers = []
    success_tickers = []

    lib = arctic.get_library("prices", create_if_missing=True)
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    logging.info(f"Starting fetch for {len(tickers)} tickers in {total_batches} batches of {batch_size}")
    logging.info(f"Special tickers bypassing short-history rule: {SPECIAL_TICKERS}")
    logging.info(f"Minimum valid rows required: {MIN_VALID_ROWS}")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Fetching batches"):
        batch = tickers[i:i + batch_size]
        batch_success = 0
        batch_skipped = 0
        batch_failed = 0
        batch_start_time = time.time()

        batch_skipped_list = []
        batch_failed_list = []

        # Fetch data
        for attempt in range(max_retries):
            try:
                #data = Ticker(batch).history(period="2y")
                data = Ticker(batch).history(period="max")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Batch {i//batch_size + 1} failed after {max_retries} attempts: {str(e)}")
                    for t in batch:
                        failed_tickers.append((t, str(e)))
                    batch_failed = len(batch)
                    batch_failed_list.extend(batch)
                    data = None
                else:
                    logging.warning(f"Retrying batch {i//batch_size + 1} (attempt {attempt + 2}/{max_retries}) after error: {str(e)}")
                    time.sleep(2)

        if data is None:
            batch_time = time.time() - batch_start_time
            logging.info(
                f"❌ Batch {i//batch_size + 1}/{total_batches} failed completely "
                f"in {batch_time:.2f}s | Success: {batch_success} | "
                f"Skipped: {batch_skipped} | Failed: {batch_failed}"
            )
            continue

        # Process each ticker
        for ticker in batch:
            try:
                if ticker not in data.index.get_level_values(0):
                    reason = "No data returned from Yahoo"
                    skipped_tickers.append((ticker, reason))
                    batch_skipped_list.append(ticker)
                    batch_skipped += 1
                    continue

                df = data.loc[ticker].reset_index()

                # Extra diagnostics for benchmark data freshness.
                debug_benchmark_frame(ticker, df, "RAW_FROM_YAHOO")

                if df.empty:
                    reason = "Empty DataFrame"
                    skipped_tickers.append((ticker, reason))
                    batch_skipped_list.append(ticker)
                    batch_skipped += 1
                    continue

                # Cleaning Pipeline
                df = df.rename(columns={"date": "datetime"})
                df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
                df = df.sort_values("datetime")
                before_dupes = len(df)
                df = df.drop_duplicates(subset=["datetime"])
                dupes_removed = before_dupes - len(df)

                df = df.replace([np.inf, -np.inf], np.nan)
                nan_count = df["close"].isna().sum()
                df = df.dropna(subset=["close"])
                invalid_price_count = (df["close"] <= 0).sum()
                df = df[df["close"] > 0]
                df["close"] = df["close"].ffill()

                final_rows = len(df)
                is_special = ticker in SPECIAL_TICKERS

                # Keep USA reasonably strict. Only SPECIAL_TICKERS can bypass.
                if final_rows < MIN_VALID_ROWS and not is_special:
                    reason = f"Too few valid rows: {final_rows}"
                    skipped_tickers.append((ticker, reason))
                    batch_skipped_list.append(ticker)
                    batch_skipped += 1
                    continue

                if final_rows < 30:
                    if is_special:
                        logging.info(f"✅ Force saved {ticker} ({final_rows} days)")
                    else:
                        logging.warning(f"⚠️ {ticker}: Limited history ({final_rows} days)")

                latest_clean_date = pd.to_datetime(df["datetime"], utc=True, errors="coerce").max()

                # Show cleaned benchmark tail before converting datetime to UNIX seconds.
                debug_benchmark_frame(ticker, df, "CLEANED_BEFORE_WRITE")

                df["datetime"] = df["datetime"].astype("int64") // 10**9

                if ticker in DEBUG_BENCHMARK_TICKERS:
                    msg = (
                        f"DEBUG {ticker} SAVING_TO_DB: rows={len(df)}, "
                        f"last_date={latest_clean_date.date() if pd.notna(latest_clean_date) else 'NaT'}, "
                        f"last_close={df['close'].iloc[-1] if len(df) else 'NaN'}"
                    )
                    print(msg)
                    logging.info(msg)

                lib.write(ticker, df)
                success_tickers.append(ticker)
                batch_success += 1

                if nan_count > 0 or invalid_price_count > 0 or dupes_removed > 0:
                    logging.info(
                        f"{ticker}: cleaned → NaN_removed={nan_count}, "
                        f"invalid_price_removed={invalid_price_count}, "
                        f"duplicates_removed={dupes_removed}, final_rows={final_rows}"
                    )

            except Exception as e:
                failed_tickers.append((ticker, str(e)))
                batch_failed_list.append(ticker)
                logging.warning(f"❌ Failed {ticker}: {str(e)}")
                batch_failed += 1

        batch_time = time.time() - batch_start_time

        logging.info(
            f"✅ Batch {i//batch_size + 1}/{total_batches} completed in {batch_time:.2f}s | "
            f"Success: {batch_success} | Skipped: {batch_skipped} | Failed: {batch_failed}"
        )

        if batch_skipped_list:
            logging.info(f"   Skipped tickers: {batch_skipped_list}")

        if batch_failed_list:
            logging.info(f"   Failed tickers: {batch_failed_list}")

    # ================= FINAL SUMMARY (Always shows skipped details) =================
    logging.info("\n=== FINAL FETCH SUMMARY ===")
    logging.info(f"Successful: {len(success_tickers)}")
    logging.info(f"Skipped: {len(skipped_tickers)}")
    logging.info(f"Failed: {len(failed_tickers)}")

    print(f"\n✅ Fetch complete! Success: {len(success_tickers)}, Skipped: {len(skipped_tickers)}, Failed: {len(failed_tickers)}")

    if skipped_tickers:
        print("\n--- Skipped Tickers ---")
        logging.info("--- Skipped Tickers ---")
        for ticker, reason in skipped_tickers:
            line = f"{ticker}: {reason}"
            print(line)
            logging.info(line)

    if failed_tickers:
        print("\n--- Failed Tickers ---")
        logging.info("--- Failed Tickers ---")
        for ticker, error in failed_tickers:
            line = f"{ticker}: {error}"
            print(line)
            logging.info(line)

    # Append final summary to shared log file
    with open(log_file, "a") as f:
        f.write("\n" + "="*80 + "\n")
        f.write(f"FINAL SUMMARY - Partition finished at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Successful: {len(success_tickers)} | Skipped: {len(skipped_tickers)} | Failed: {len(failed_tickers)}\n")
        if skipped_tickers:
            f.write("--- Skipped Tickers ---\n")
            for ticker, reason in skipped_tickers:
                f.write(f"{ticker}: {reason}\n")
        if failed_tickers:
            f.write("--- Failed Tickers ---\n")
            for ticker, error in failed_tickers:
                f.write(f"{ticker}: {error}\n")
        f.write("="*80 + "\n")

    print(f"Full log saved to: {log_file}")


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
    parser.add_argument("--log-file", default="logs/fetch_price_history_rs.log", help="Base log file name")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db", help="Directory for ArcticDB")
    parser.add_argument("--partition", type=int, default=None)
    parser.add_argument("--total-partitions", type=int, default=None)
    args = parser.parse_args()

    # ================== DYNAMIC LOG FILE NAME ==================
    if args.partition is not None:
        # Create partition-specific log: fetch_price_history_rs_0.log, fetch_price_history_rs_1.log, etc.
        base_name = os.path.splitext(os.path.basename(args.log_file))[0]
        log_file = f"logs/{base_name}_{args.partition}.log"
        logging.info(f"Partition {args.partition}: Using dedicated log file -> {log_file}")
    else:
        log_file = args.log_file

    # Ensure directories exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    os.makedirs(args.arctic_db_path, exist_ok=True)

    # Setup logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        filemode="a"          # append mode (safe for parallel runs)
    )

    logging.info(f"Starting fetch for partition: {args.partition if args.partition is not None else 'ALL'}")

    tickers = load_ticker_list(args.input_file, args.partition, args.total_partitions)
    arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
    
    fetch_historical_data(tickers, arctic, log_file)


if __name__ == "__main__":
    main()
