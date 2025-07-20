#!/usr/bin/env python3
import json
import glob
import os
import sys
import logging
from datetime import datetime

OUTPUT_FILE = "data/ticker_info.json"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/merge_ticker_info.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def quality(info):
    s = info.get("sector", "").lower()
    i = info.get("industry", "").lower()
    return int(s not in ("", "n/a", "unknown") and i not in ("", "n/a", "unknown"))

def is_valid_json(file_path):
    """Validate if a file contains valid JSON."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)
        return True
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {file_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return False

def merge_ticker_info(source_dir="artifacts"):
    os.makedirs("logs", exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Log the start time of the merge process
    start_time = datetime.now().strftime("%I:%M %p %Z on %A, %B %d, %Y")
    logging.info(f"Starting merge process at {start_time}")
    
    # Use the provided source directory and look for ticker_info.json in any subdirectory
    search_pattern = os.path.join(source_dir, "**", "ticker_info.json")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        logging.warning(f"No ticker_info.json parts found in {source_dir}!")
        return

    logging.info(f"Found {len(files)} ticker_info.json files to merge.")
    
    merged = {}
    for file_path in files:
        logging.info(f"Processing {file_path}...")
        if not is_valid_json(file_path):
            logging.warning(f"Skipping {file_path} due to invalid JSON.")
            continue
            
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                part_data = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load {file_path}: {e}")
            continue

        for sym, rec in part_data.items():
            if sym not in merged or quality(rec["info"]) > quality(merged.get(sym, {}).get("info", {})):
                merged[sym] = rec
            elif quality(rec["info"]) == quality(merged.get(sym, {}).get("info", {})):
                # Preserve existing type if quality is equal
                merged[sym]["info"]["type"] = rec["info"].get("type", merged[sym]["info"].get("type", "Other"))

    if not merged:
        logging.warning("No valid data to merge!")
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, sort_keys=True)

    logging.info(f"Merged {len(files)} files into {OUTPUT_FILE}")
    logging.info(f"Total entries: {len(merged)}")

if __name__ == "__main__":
    source_dir = sys.argv[1] if len(sys.argv) > 1 else "artifacts"
    merge_ticker_info(source_dir)
