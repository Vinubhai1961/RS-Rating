#!/usr/bin/env python3
import json
import glob
import os
import sys
import logging
from datetime import datetime

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
UNRESOLVED_TICKERS_FILE = os.path.join(OUTPUT_DIR, "unresolved_tickers.txt")
PARTITION_SUMMARY_FILE = os.path.join(OUTPUT_DIR, "partition_summary.json")

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
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Log the start time of the merge process
    start_time = datetime.now().strftime("%I:%M %p %Z on %A, %B %d, %Y")
    logging.info(f"Starting merge process at {start_time}")
    
    # Look for ticker_info_part_*.json files
    search_pattern = os.path.join(source_dir, "**", "ticker_info_part_*.json")
    ticker_files = glob.glob(search_pattern, recursive=True)
    
    if not ticker_files:
        logging.warning(f"No ticker_info_part_*.json parts found in {source_dir}!")
        # Fallback: Check for partition_summary.json to copy as ticker_info.json
        summary_pattern = os.path.join(source_dir, "**", "partition_summary.json")
        summary_files = glob.glob(summary_pattern, recursive=True)
        if summary_files:
            src_summary = summary_files[0]
            logging.info(f"Using {src_summary} as fallback for {TICKER_INFO_FILE}")
            if is_valid_json(src_summary):
                try:
                    with open(src_summary, "r", encoding="utf-8") as f_in, \
                         open(TICKER_INFO_FILE, "w", encoding="utf-8") as f_out:
                        data = json.load(f_in)
                        json.dump(data, f_out, indent=2, sort_keys=True)
                except Exception as e:
                    logging.error(f"Failed to copy {src_summary}: {e}")
            else:
                logging.warning(f"Skipping {src_summary} due to invalid JSON.")
        else:
            logging.error("No valid JSON files found to create ticker_info.json!")
            return
    else:
        logging.info(f"Found {len(ticker_files)} ticker_info_part_*.json files to merge.")
        merged = {}
        for file_path in ticker_files:
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
                    merged[sym]["info"]["type"] = rec["info"].get("type", merged[sym]["info"].get("type", "Other"))

        if not merged:
            logging.warning("No valid data to merge!")
            return

        with open(TICKER_INFO_FILE, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, sort_keys=True)
        logging.info(f"Merged {len(ticker_files)} files into {TICKER_INFO_FILE}")
        logging.info(f"Total entries: {len(merged)}")

    # Copy the first unresolved_tickers.txt and partition_summary.json
    tickers_pattern = os.path.join(source_dir, "**", "unresolved_tickers.txt")
    tickers_files = glob.glob(tickers_pattern, recursive=True)
    if tickers_files:
        src_tickers = tickers_files[0]
        logging.info(f"Copying {src_tickers} to {UNRESOLVED_TICKERS_FILE}")
        try:
            with open(src_tickers, "r", encoding="utf-8") as f_in, \
                 open(UNRESOLVED_TICKERS_FILE, "w", encoding="utf-8") as f_out:
                f_out.writelines(line for line in f_in if line.strip())
        except Exception as e:
            logging.error(f"Failed to copy {src_tickers}: {e}")

    summary_pattern = os.path.join(source_dir, "**", "partition_summary.json")
    summary_files = glob.glob(summary_pattern, recursive=True)
    if summary_files:
        src_summary = summary_files[0]
        logging.info(f"Copying {src_summary} to {PARTITION_SUMMARY_FILE}")
        if is_valid_json(src_summary):
            try:
                with open(src_summary, "r", encoding="utf-8") as f_in, \
                     open(PARTITION_SUMMARY_FILE, "w", encoding="utf-8") as f_out:
                    data = json.load(f_in)
                    json.dump(data, f_out, indent=2)
            except Exception as e:
                logging.error(f"Failed to copy {src_summary}: {e}")
        else:
            logging.warning(f"Skipping {src_summary} due to invalid JSON.")

if __name__ == "__main__":
    source_dir = sys.argv[1] if len(sys.argv) > 1 else "artifacts"
    merge_ticker_info(source_dir)
