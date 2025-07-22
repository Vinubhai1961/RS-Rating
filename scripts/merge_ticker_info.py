#!/usr/bin/env python3
import json
import os
import glob
import logging
import time
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%levelname]s] %(message)s",
    handlers=[
        logging.FileHandler("logs/merge_ticker_info.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def ensure_dirs():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)

def merge_ticker_info(artifacts_dir):
    output_file = os.path.join("data", "ticker_info.json")
    merged_data = {}

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
                logging.info(f"Loaded {len(part_data)} entries from {file_path}")
                # Merge data, updating with the latest values for any duplicate keys
                merged_data.update(part_data)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")

    if not merged_data:
        logging.error("No valid data merged from part files. Skipping output file creation.")
        return

    # Write the merged data to the output file with sorted keys
    os.makedirs("data", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2, sort_keys=True)
    logging.info(f"Merged data saved to {output_file} with {len(merged_data)} entries")

def main(artifacts_dir="data"):
    start_time = time.time()
    start_time_str = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting merge process at {start_time_str}")

    merge_ticker_info(artifacts_dir)

    elapsed_time = time.time() - start_time
    logging.info(f"Merge process completed. Elapsed time: {elapsed_time:.1f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ticker info partition files into a single JSON file.")
    parser.add_argument("artifacts_dir", nargs="?", default="data", help="Directory containing ticker info partition files")
    args = parser.parse_args()

    main(args.artifacts_dir)
