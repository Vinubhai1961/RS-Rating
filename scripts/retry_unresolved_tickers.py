#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import glob
import logging
from yahooquery import Ticker
from datetime import datetime

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
UNRESOLVED_TICKERS_FILE = os.path.join(OUTPUT_DIR, "unresolved_tickers.txt")
LOG_PATH = "logs/retry_tickers.log"
BATCH_SIZE = 200

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

def fetch_ticker_data(symbol: str) -> Dict[str, Any]:
    try:
        yq = Ticker(yahoo_symbol(symbol), asynchronous=True, validate=True)
        mods = yq.get_modules(["summaryProfile", "quoteType"])
        entry = mods.get(symbol) or mods.get(yahoo_symbol(symbol))
        if not isinstance(entry, dict):
            return None
        prof = entry.get("summaryProfile") or {}
        return {
            "info": {
                "industry": prof.get("industry", "n/a"),
                "sector": prof.get("sector", "n/a"),
                "type": "Other"
            }
        }
    except Exception as e:
        logging.warning(f"Failed to fetch data for {symbol}: {e}")
        return None

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

    newly_resolved = {}
    for i in range(0, len(unresolved), BATCH_SIZE):
        batch = unresolved[i:i + BATCH_SIZE]
        for symbol in batch:
            data = fetch_ticker_data(symbol)
            if data:
                newly_resolved[symbol] = data

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
    source_dir = sys.argv[1] if len(sys.argv) > 1 else "artifacts"
    retry_unresolved_tickers(source_dir)
