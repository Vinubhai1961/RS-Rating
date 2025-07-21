#!/usr/bin/env python3
import json
import os
import glob
import logging
from datetime import datetime

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
LOG_PATH = "logs/merge_ticker_info.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def ensure_dirs():
    os.makedirs("logs", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_part_files():
    pattern = os.path.join(OUTPUT_DIR, "ticker_info_part_*.json")
    part_files = glob.glob(pattern)
    if not part_files:
        logging.error("No ticker_info_part_*.json files found!")
        return {}
    merged_data = {}
    for file_path in part_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)
                merged_data.update(part_data)
                logging.info(f"Loaded {len(part_data)} entries from {file_path}")
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
    return merged_data

def save_merged_data(merged_data):
    try:
        with open(TICKER_INFO_FILE, "w", encoding="utf-8") as f:
            json.dump(merged_data, f, indent=2)
        logging.info(f"Saved {len(merged_data)} entries to {TICKER_INFO_FILE}")
    except Exception as e:
        logging.error(f"Error saving {TICKER_INFO_FILE}: {e}")

def main():
    start_time = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    ensure_dirs()
    logging.info(f"Starting merge process at {start_time}")

    merged_data = load_part_files()
    if not merged_data:
        logging.error("No data to merge. Process aborted.")
        return

    save_merged_data(merged_data)
    logging.info("Merge process completed.")

if __name__ == "__main__":
    main()
