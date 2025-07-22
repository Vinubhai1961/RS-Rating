#!/usr/bin/env python3
import os
import json
import argparse
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/merge_ticker_price.log"), logging.StreamHandler()]
)

def merge_price_files(artifacts_dir, expected_parts=None):
    output_file = os.path.join("data", "ticker_price.json")
    merged_data = {}

    # Validate input directory
    if not os.path.exists(artifacts_dir):
        logging.error(f"Input directory {artifacts_dir} does not exist")
        return

    logging.info(f"Searching for ticker_price_part_*.json files in {artifacts_dir}")

    # Find all ticker_price_part_*.json files
    part_files = sorted([f for f in os.listdir(artifacts_dir) if f.startswith("ticker_price_part_") and f.endswith(".json")])
    if not part_files:
        logging.error(f"No ticker_price_part_*.json files found in {artifacts_dir}")
        return

    logging.info(f"Found {len(part_files)} part files to merge: {part_files}")
    if expected_parts is not None and len(part_files) < expected_parts:
        logging.warning(f"Expected {expected_parts} part files, but found only {len(part_files)}")

    # Merge data from each part file
    for filename in part_files:
        file_path = os.path.join(artifacts_dir, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)
                logging.info(f"Loaded {len(part_data)} tickers from {filename}")
                # Merge data, updating with the latest values for any duplicate keys
                merged_data.update(part_data)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse {filename}: {e}")
        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")

    if not merged_data:
        logging.error("No valid data merged from part files. Skipping output file creation.")
        return

    # Write the merged data to the output file with sorted keys
    os.makedirs("data", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2, sort_keys=True)
    logging.info(f"Merged data saved to {output_file} with {len(merged_data)} entries")

def main(artifacts_dir, expected_parts=None):
    start_time = time.time()
    start_time_str = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting price merge process at {start_time_str}")

    merge_price_files(artifacts_dir, expected_parts)

    elapsed_time = time.time() - start_time
    logging.info(f"Price merge completed. Elapsed time: {elapsed_time:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ticker price partition files into a single JSON file.")
    parser.add_argument("artifacts_dir", help="Directory containing ticker price partition files")
    parser.add_argument("--part-total", type=int, default=None, help="Expected number of part files (optional)")
    args = parser.parse_args()

    main(args.artifacts_dir, args.part_total)
