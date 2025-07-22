#!/usr/bin/env python3
import json
import os
import glob
import logging
import time
import argparse
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/merge_ticker_info.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def ensure_dirs():
    """Ensure required directories exist."""
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)

def merge_ticker_info(artifacts_dir):
    """Merge ticker info from partition files into a single JSON file."""
    output_file = os.path.join("data", "ticker_info.json")
    all_data = []
    expected_total = 6383  # Sum of updated symbols: 1742 + 1386 + 1683 + 1523

    # Validate input directory
    if not os.path.exists(artifacts_dir):
        logging.error(f"Input directory {artifacts_dir} does not exist")
        return

    logging.info(f"Searching for ticker_info_part_*.json files in {artifacts_dir}")

    # Find all ticker_info_part_*.json files
    pattern = os.path.join(artifacts_dir, "ticker_info_part_*.json")
    part_files = sorted(glob.glob(pattern))
    if not part_files:
        logging.error(f"No ticker_info_part_*.json files found in {artifacts_dir}")
        return

    logging.info(f"Found {len(part_files)} part files to merge: {part_files}")

    # Merge data from each part file
    for file_path in part_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)
                if not isinstance(part_data, list):
                    logging.error(f"Expected list in {file_path}, got {type(part_data)}")
                    continue
                logging.info(f"Loaded {len(part_data)} entries from {file_path}")
                all_data.extend(part_data)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")

    if not all_data:
        logging.error("No valid data merged from part files. Skipping output file creation.")
        return

    # Deduplicate by symbol
    unique_data = {}
    for item in all_data:
        if not isinstance(item, dict) or "symbol" not in item:
            logging.warning(f"Skipping invalid entry: {item}")
            continue
        symbol = item["symbol"]
        unique_data[symbol] = item  # Keep the last entry for each symbol

    logging.info(f"Total unique symbols after deduplication: {len(unique_data)}")

    # Verify expected total
    if len(unique_data) != expected_total:
        logging.warning(
            f"Expected {expected_total} symbols, but merged {len(unique_data)}. "
            f"Possible duplicates or missing data in partition files."
        )

    # Write merged data to output file
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(list(unique_data.values()), f, indent=2, sort_keys=True)
        logging.info(f"Merged {len(unique_data)} symbols into {output_file}")
    except Exception as e:
        logging.error(f"Error writing to {output_file}: {e}")

def main(artifacts_dir="data"):
    """Main function to execute the merge process."""
    start_time = time.time()
    start_time_str = datetime.now(timezone.utc).strftime("%I:%M %p UTC on %A, %B %d, %Y")
    logging.info(f"Starting merge process at {start_time_str}")

    ensure_dirs()
    merge_ticker_info(artifacts_dir)

    elapsed_time = time.time() - start_time
    logging.info(f"Merge process completed. Elapsed time: {elapsed_time:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ticker info partition files into a single JSON file.")
    parser.add_argument("artifacts_dir", nargs="?", default="data", help="Directory containing ticker info partition files")
    args = parser.parse_args()

    main(args.artifacts_dir)
