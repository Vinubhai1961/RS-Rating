#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import glob
import logging
from yahooquery import Ticker
from datetime import datetime
import random
from typing import Dict, Any  # Ensure this import is present

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
UNRESOLVED_TICKERS_FILE = os.path.join(OUTPUT_DIR, "unresolved_tickers.txt")
LOG_PATH = "logs/retry_tickers.log"
BATCH_SIZE = 200
MAX_BATCH_RETRIES = 1
BATCH_DELAY_RANGE = (2, 5)

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

def load_unresolved_tickers(source_dir="artifacts"):
    unresolved = set()
    pattern = os.path.join(source_dir, "**", "unresolved_tickers.txt")
    files = glob.glob(pattern, recursive=True)
    for file_path in files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                unresolved.update(line.strip() for line in f if line.strip())
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
    return list(unresolved)

def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def fetch_ticker_data(symbols: list) -> Dict[str, Any]:
    try:
        yq = Ticker([yahoo_symbol(s) for s in symbols], asynchronous=True, validate=True)
        mods = yq.get_modules(["summaryProfile", "quoteType"])
        return mods
    except Exception as e:
        logging.warning(f"Failed to fetch batch data: {e}")
        return {}

def process_batch(batch, existing):
    for attempt in range(MAX_BATCH_RETRIES):
        mods = fetch_ticker_data(batch)
        if mods:
            break
        wait = (2 ** attempt) + random.uniform(0, 2)
        logging.warning(f"Batch error (attempt {attempt+1}). Retrying in {wait:.1f}s.")
        time.sleep(wait)
    else:
        logging.error(f"Batch failed after {MAX_BATCH_RETRIES} attempts.")
        return 0, batch

    failed = set(mods.get("failed", []))
    updated = 0
    unresolved = []

    for symbol in batch:
        if symbol in failed:
            unresolved.append(symbol)
            continue
        entry = mods.get(symbol) or mods.get(yahoo_symbol(symbol))
        if isinstance(entry, dict):
            prof = entry.get("summaryProfile", {})
            data = {
                "info": {
                    "industry": prof.get("industry", "n/a"),
                    "sector": prof.get("sector", "n/a"),
                    "type": "Other"
                }
            }
            if not existing.get(symbol) or is_incomplete(existing[symbol]) and data["info"]["sector"] not in GOOD_VALUES and data["info"]["industry"] not in GOOD_VALUES:
                existing[symbol] = data
                updated += 1
            else:
                unresolved.append(symbol)
    return updated, unresolved

def is_incomplete(info_dict: Dict[str, Any]) -> bool:
    info = info_dict.get("info", {})
    sector = str(info.get("sector", "")).strip().lower()
    industry = str(info.get("industry", "")).strip().lower()
    return sector in GOOD_VALUES or industry in GOOD_VALUES

def retry_unresolved_tickers(source_dir="artifacts"):
    ensure_dirs()
    start_time = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting retry process at {start_time}")

    unresolved = load_unresolved_tickers(source_dir)
    if not unresolved:
        logging.warning("No unresolved tickers to retry.")
        return

    logging.info(f"Retrying {len(unresolved)} unresolved tickers.")
    existing = {}
    if os.path.exists(TICKER_INFO_FILE):
        with open(TICKER_INFO_FILE, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                logging.warning("Existing ticker_info.json corrupt; starting fresh.")

    batches = [unresolved[i:i + BATCH_SIZE] for i in range(0, len(unresolved), BATCH_SIZE)]
    newly_resolved = {}
    remaining_unresolved = []

    for idx, batch in enumerate(batches, 1):
        updated, unresolved_batch = process_batch(batch, existing)
        newly_resolved.update({s: existing[s] for s in batch if s not in unresolved_batch})
        remaining_unresolved.extend(unresolved_batch)
        logging.info(f"Batch {idx}/{len(batches)} - Updated: {updated} | Unresolved: {len(unresolved_batch)}")
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.1f}s before next batch...")
            time.sleep(delay)

    if newly_resolved:
        existing.update(newly_resolved)
        with open(TICKER_INFO_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        logging.info(f"Added {len(newly_resolved)} newly resolved tickers to {TICKER_INFO_FILE}")

    remaining_unresolved = [s for s in unresolved if s not in newly_resolved]
    with open(UNRESOLVED_TICKERS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(remaining_unresolved))
    logging.info(f"Updated {UNRESOLVED_TICKERS_FILE} with {len(remaining_unresolved)} remaining unresolved tickers.")

    logging.info("Retry process completed.")

if __name__ == "__main__":
    import sys
    source_dir = sys.argv[1] if len(sys.argv) > 1 else "artifacts"
    retry_unresolved_tickers(source_dir)
