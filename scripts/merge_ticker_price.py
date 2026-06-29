#!/usr/bin/env python3
import os
import json
import argparse
import logging
import time
from datetime import datetime
from collections import Counter

LOG_FILE = "logs/merge_ticker_price.log"
OUTPUT_FILE = "data/ticker_price.json"
EXCLUDED_SYMBOLS_FILE = "data/excluded_symbols.txt"

REQUIRED_FIELDS = [
    "Price", "industry", "sector", "type",
    "DVol", "AvgVol", "AvgVol10",
    "52WKL", "52WKH", "MCAP"
]

BAD_VALUES = {"", "n/a", "na", "nan", "none", "null", "-", "unknown"}

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def normalize(value):
    if value is None:
        return ""
    return str(value).strip()


def is_missing(value):
    return normalize(value).lower() in BAD_VALUES


def load_excluded_symbols(path=EXCLUDED_SYMBOLS_FILE):
    """
    Supports:
      SYMBOL|ETF|reason|Security Name
    and plain one-symbol-per-line format.
    """
    if not os.path.exists(path):
        logging.warning("%s not found. No excluded symbols removed during price merge.", path)
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


def is_valid_price_item(item, filename):
    if not isinstance(item, dict):
        logging.warning("Invalid item in %s: not a dict", filename)
        return False

    ticker = normalize(item.get("ticker")).upper()

    if not ticker:
        logging.warning("Invalid item in %s: missing ticker", filename)
        return False

    if "info" not in item or not isinstance(item["info"], dict):
        logging.warning("Invalid data for %s in %s: missing info dict", ticker, filename)
        return False

    info = item["info"]

    missing_fields = [field for field in REQUIRED_FIELDS if field not in info]

    if missing_fields:
        logging.warning("Missing fields for %s in %s: %s", ticker, filename, missing_fields)
        return False

    price = info.get("Price")
    if not isinstance(price, (int, float)) or price <= 0:
        logging.warning("Invalid Price for %s in %s: %s", ticker, filename, price)
        return False

    for field in ["DVol", "AvgVol", "AvgVol10"]:
        value = info.get(field)

        if value is not None and (not isinstance(value, int) or value < 0):
            logging.warning("Invalid %s for %s in %s: %s", field, ticker, filename, value)
            return False

    for field in ["52WKL", "52WKH"]:
        value = info.get(field)

        if value is not None and (not isinstance(value, (int, float)) or value <= 0):
            logging.warning("Invalid %s for %s in %s: %s", field, ticker, filename, value)
            return False

    mcap = info.get("MCAP")
    if mcap is not None and (not isinstance(mcap, (int, float)) or mcap < 0):
        logging.warning("Invalid MCAP for %s in %s: %s", ticker, filename, mcap)
        return False

    return True


def count_price_rows(data):
    stock_count = 0
    etf_count = 0
    unknown_count = 0
    missing_sector = []
    missing_industry = []
    missing_both = []
    type_counter = Counter()

    for item in data:
        ticker = normalize(item.get("ticker")).upper()
        info = item.get("info", {})

        ticker_type = normalize(info.get("type")) or "Unknown"
        type_counter[ticker_type] += 1

        if ticker_type == "ETF":
            etf_count += 1
        elif ticker_type == "Stock":
            stock_count += 1
        else:
            unknown_count += 1

        sector_missing = is_missing(info.get("sector"))
        industry_missing = is_missing(info.get("industry"))

        if sector_missing:
            missing_sector.append(ticker)

        if industry_missing:
            missing_industry.append(ticker)

        if sector_missing and industry_missing:
            missing_both.append(ticker)

    return {
        "stock_count": stock_count,
        "etf_count": etf_count,
        "unknown_count": unknown_count,
        "type_counter": dict(type_counter),
        "missing_sector": missing_sector,
        "missing_industry": missing_industry,
        "missing_both": missing_both,
    }


def merge_price_files(artifacts_dir, expected_parts=None):
    merged_by_ticker = {}

    if not os.path.exists(artifacts_dir):
        logging.error("Input directory %s does not exist", artifacts_dir)
        return

    logging.info("Searching for ticker_price_part_*.json files in %s", artifacts_dir)

    part_files = sorted([
        f for f in os.listdir(artifacts_dir)
        if f.startswith("ticker_price_part_") and f.endswith(".json")
    ])

    if not part_files:
        logging.error("No ticker_price_part_*.json files found in %s", artifacts_dir)
        return

    logging.info("Found %s part files to merge: %s", len(part_files), part_files)

    if expected_parts is not None and len(part_files) < expected_parts:
        logging.warning("Expected %s part files, but found only %s", expected_parts, len(part_files))

    invalid_count = 0
    duplicate_count = 0
    duplicate_symbols = []

    for filename in part_files:
        file_path = os.path.join(artifacts_dir, filename)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)

            if not isinstance(part_data, list):
                logging.error("%s must contain a list. Skipping.", filename)
                continue

            logging.info("Loaded %s rows from %s", len(part_data), filename)

            for item in part_data:
                if not is_valid_price_item(item, filename):
                    invalid_count += 1
                    continue

                ticker = normalize(item.get("ticker")).upper()
                item["ticker"] = ticker

                if ticker in merged_by_ticker:
                    duplicate_count += 1
                    duplicate_symbols.append(ticker)

                merged_by_ticker[ticker] = item

        except json.JSONDecodeError as e:
            logging.error("Failed to parse %s: %s", filename, e)
        except Exception as e:
            logging.error("Error reading %s: %s", filename, e)

    if not merged_by_ticker:
        logging.error("No valid data merged from part files. Skipping output file creation.")
        return

    before_exclusion_count = len(merged_by_ticker)
    before_stats = count_price_rows(list(merged_by_ticker.values()))

    excluded_symbols = load_excluded_symbols()

    removed_excluded = sorted([
        ticker for ticker in merged_by_ticker
        if ticker.upper() in excluded_symbols
    ])

    if excluded_symbols:
        merged_by_ticker = {
            ticker: item
            for ticker, item in merged_by_ticker.items()
            if ticker.upper() not in excluded_symbols
        }

    merged_data = [
        merged_by_ticker[ticker]
        for ticker in sorted(merged_by_ticker.keys())
    ]

    after_exclusion_count = len(merged_data)
    after_stats = count_price_rows(merged_data)

    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2)

    logging.info("============================================================")
    logging.info("MERGE TICKER PRICE SUMMARY")
    logging.info("============================================================")
    logging.info("Part files merged: %s", len(part_files))
    logging.info("Rows before exclusion: %s", before_exclusion_count)
    logging.info("Stock count before exclusion: %s", before_stats["stock_count"])
    logging.info("ETF count before exclusion: %s", before_stats["etf_count"])
    logging.info("Unknown count before exclusion: %s", before_stats["unknown_count"])
    logging.info("Excluded symbols loaded: %s", len(excluded_symbols))
    logging.info("Removed excluded symbols: %s", len(removed_excluded))
    logging.info("Rows after exclusion: %s", after_exclusion_count)
    logging.info("Final Stock count: %s", after_stats["stock_count"])
    logging.info("Final ETF count: %s", after_stats["etf_count"])
    logging.info("Final Unknown type count: %s", after_stats["unknown_count"])
    logging.info("Final type counter: %s", after_stats["type_counter"])
    logging.info("Invalid rows skipped: %s", invalid_count)
    logging.info("Duplicate symbols seen while merging: %s", duplicate_count)
    logging.info("Sample duplicate symbols: %s", duplicate_symbols[:50])
    logging.info("Sample removed excluded symbols: %s", removed_excluded[:100])
    logging.info("Remaining missing sector count: %s", len(after_stats["missing_sector"]))
    logging.info("Remaining missing industry count: %s", len(after_stats["missing_industry"]))
    logging.info("Remaining missing both count: %s", len(after_stats["missing_both"]))
    logging.info("Sample missing sector: %s", after_stats["missing_sector"][:50])
    logging.info("Sample missing industry: %s", after_stats["missing_industry"][:50])
    logging.info("Sample missing both: %s", after_stats["missing_both"][:50])
    logging.info("Output file: %s", OUTPUT_FILE)
    logging.info("============================================================")

    print("")
    print("============================================================")
    print("MERGE TICKER PRICE SUMMARY")
    print("============================================================")
    print(f"Part files merged: {len(part_files)}")
    print(f"Rows before exclusion: {before_exclusion_count}")
    print(f"Stock count before exclusion: {before_stats['stock_count']}")
    print(f"ETF count before exclusion: {before_stats['etf_count']}")
    print(f"Unknown count before exclusion: {before_stats['unknown_count']}")
    print(f"Excluded symbols loaded: {len(excluded_symbols)}")
    print(f"Removed excluded symbols: {len(removed_excluded)}")
    print(f"Rows after exclusion: {after_exclusion_count}")
    print(f"Final Stock count: {after_stats['stock_count']}")
    print(f"Final ETF count: {after_stats['etf_count']}")
    print(f"Final Unknown type count: {after_stats['unknown_count']}")
    print(f"Invalid rows skipped: {invalid_count}")
    print(f"Duplicate symbols seen while merging: {duplicate_count}")
    print(f"Remaining missing sector count: {len(after_stats['missing_sector'])}")
    print(f"Remaining missing industry count: {len(after_stats['missing_industry'])}")
    print(f"Remaining missing both count: {len(after_stats['missing_both'])}")
    print(f"Sample removed excluded symbols: {removed_excluded[:100]}")
    print(f"Sample missing sector: {after_stats['missing_sector'][:50]}")
    print(f"Sample missing industry: {after_stats['missing_industry'][:50]}")
    print("============================================================")


def main(artifacts_dir, expected_parts=None):
    start_time = time.time()
    start_time_str = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")

    logging.info("Starting price merge process at %s", start_time_str)

    merge_price_files(artifacts_dir, expected_parts)

    elapsed_time = time.time() - start_time
    logging.info("Price merge completed. Elapsed time: %.1fs", elapsed_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge ticker price partition files into a single JSON file."
    )

    parser.add_argument(
        "artifacts_dir",
        help="Directory containing ticker price partition files"
    )

    parser.add_argument(
        "--part-total",
        type=int,
        default=None,
        help="Expected number of part files"
    )

    args = parser.parse_args()

    main(args.artifacts_dir, args.part_total)
