#!/usr/bin/env python3
import os
import json
import argparse
import logging
import time
from datetime import datetime

LOG_FILE = "logs/merge_ticker_info.log"
EXCLUDED_SYMBOLS_FILE = "data/excluded_symbols.txt"

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def load_excluded_symbols(path=EXCLUDED_SYMBOLS_FILE):
    if not os.path.exists(path):
        logging.warning("%s not found. No excluded symbols removed during merge.", path)
        return set()

    excluded = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            symbol = line.split("|")[0].strip().upper()

            if symbol:
                excluded.add(symbol)

    logging.info("Loaded excluded symbols: %s", len(excluded))
    return excluded


def count_stock_etf_rows(data):
    stock_count = 0
    etf_count = 0
    unknown_count = 0

    for _, payload in data.items():
        info = payload.get("info", {})
        ticker_type = str(info.get("type", "Unknown")).strip()

        if ticker_type == "ETF":
            etf_count += 1
        elif ticker_type == "Stock":
            stock_count += 1
        else:
            unknown_count += 1

    return stock_count, etf_count, unknown_count


def merge_ticker_info_files(artifacts_dir, expected_parts=None):
    output_file = os.path.join("data", "ticker_info.json")
    merged_data = {}

    if not os.path.exists(artifacts_dir):
        logging.error("Input directory %s does not exist", artifacts_dir)
        return

    logging.info("Searching for ticker_info_part_*.json files in %s", artifacts_dir)

    part_files = sorted([
        f for f in os.listdir(artifacts_dir)
        if f.startswith("ticker_info_part_") and f.endswith(".json")
    ])

    if not part_files:
        logging.error("No ticker_info_part_*.json files found in %s", artifacts_dir)
        return

    logging.info("Found %s part files to merge: %s", len(part_files), part_files)

    if expected_parts is not None and len(part_files) < expected_parts:
        logging.warning(
            "Expected %s part files, but found only %s",
            expected_parts,
            len(part_files)
        )

    duplicate_count = 0
    duplicate_symbols = []

    for filename in part_files:
        file_path = os.path.join(artifacts_dir, filename)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)

            logging.info("Loaded %s tickers from %s", len(part_data), filename)

            for symbol, info in part_data.items():
                symbol_norm = str(symbol).upper().strip()

                if symbol_norm in merged_data:
                    duplicate_count += 1
                    duplicate_symbols.append(symbol_norm)

                merged_data[symbol_norm] = info

        except json.JSONDecodeError as e:
            logging.error("Failed to parse %s: %s", filename, e)
        except Exception as e:
            logging.error("Error reading %s: %s", filename, e)

    if not merged_data:
        logging.error("No valid data merged from part files. Skipping output file creation.")
        return

    before_exclusion_count = len(merged_data)

    before_stock_count, before_etf_count, before_unknown_count = count_stock_etf_rows(merged_data)

    excluded_symbols = load_excluded_symbols()

    removed_excluded = sorted([
        symbol for symbol in merged_data
        if symbol.upper() in excluded_symbols
    ])

    if excluded_symbols:
        merged_data = {
            symbol: info
            for symbol, info in merged_data.items()
            if symbol.upper() not in excluded_symbols
        }

    after_exclusion_count = len(merged_data)

    after_stock_count, after_etf_count, after_unknown_count = count_stock_etf_rows(merged_data)

    os.makedirs("data", exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2, sort_keys=True)

    logging.info("============================================================")
    logging.info("MERGE TICKER INFO SUMMARY")
    logging.info("============================================================")
    logging.info("Part files merged: %s", len(part_files))
    logging.info("Rows before exclusion: %s", before_exclusion_count)
    logging.info("Stock count before exclusion: %s", before_stock_count)
    logging.info("ETF count before exclusion: %s", before_etf_count)
    logging.info("Unknown count before exclusion: %s", before_unknown_count)
    logging.info("Excluded symbols loaded: %s", len(excluded_symbols))
    logging.info("Removed excluded symbols: %s", len(removed_excluded))
    logging.info("Rows after exclusion: %s", after_exclusion_count)
    logging.info("Final Stock count: %s", after_stock_count)
    logging.info("Final ETF count: %s", after_etf_count)
    logging.info("Final Unknown type count: %s", after_unknown_count)
    logging.info("Duplicate symbols seen while merging: %s", duplicate_count)
    logging.info("Sample duplicate symbols: %s", duplicate_symbols[:50])
    logging.info("Sample removed excluded symbols: %s", removed_excluded[:100])
    logging.info("Output file: %s", output_file)
    logging.info("============================================================")

    print("")
    print("============================================================")
    print("MERGE TICKER INFO SUMMARY")
    print("============================================================")
    print(f"Part files merged: {len(part_files)}")
    print(f"Rows before exclusion: {before_exclusion_count}")
    print(f"Stock count before exclusion: {before_stock_count}")
    print(f"ETF count before exclusion: {before_etf_count}")
    print(f"Unknown count before exclusion: {before_unknown_count}")
    print(f"Excluded symbols loaded: {len(excluded_symbols)}")
    print(f"Removed excluded symbols: {len(removed_excluded)}")
    print(f"Rows after exclusion: {after_exclusion_count}")
    print(f"Final Stock count: {after_stock_count}")
    print(f"Final ETF count: {after_etf_count}")
    print(f"Final Unknown type count: {after_unknown_count}")
    print(f"Duplicate symbols seen while merging: {duplicate_count}")
    print(f"Sample removed excluded symbols: {removed_excluded[:100]}")
    print("============================================================")


def main(artifacts_dir, expected_parts=None):
    start_time = time.time()
    start_time_str = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")

    logging.info("Starting ticker info merge process at %s", start_time_str)

    merge_ticker_info_files(artifacts_dir, expected_parts)

    elapsed_time = time.time() - start_time
    logging.info("Ticker info merge completed. Elapsed time: %.1fs", elapsed_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge ticker info partition files into a single JSON file."
    )

    parser.add_argument(
        "artifacts_dir",
        help="Directory containing ticker info partition files"
    )

    parser.add_argument(
        "--part-total",
        type=int,
        default=None,
        help="Expected number of part files"
    )

    args = parser.parse_args()

    main(args.artifacts_dir, args.part_total)
