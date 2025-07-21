#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import argparse
import logging
from yahooquery import Ticker
from tqdm import tqdm
from datetime import datetime

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
TICKER_PRICE_PART_FILE = os.path.join(OUTPUT_DIR, f"ticker_price_part_%d.json")
LOG_PATH = "logs/build_ticker_price.log"
BATCH_SIZE = 150
BATCH_DELAY_RANGE = (2, 5)
MAX_BATCH_RETRIES = 3
PRICE_THRESHOLD = 5.0

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

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def load_ticker_info():
    if not os.path.exists(TICKER_INFO_FILE):
        logging.error(f"{TICKER_INFO_FILE} not found!")
        return {}
    with open(TICKER_INFO_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.error("Invalid JSON in ticker_info.json")
            return {}

def partition_tickers(tickers, part_index, part_total):
    per_part = len(tickers) // part_total
    start = part_index * per_part
    end = start + per_part if part_index < part_total - 1 else len(tickers)
    return tickers[start:end]

def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def fetch_price_history(symbol: str) -> float:
    try:
        yq = Ticker(yahoo_symbol(symbol))
        hist = yq.history(period="1d")
        return hist['close'].iloc[-1] if not hist.empty else None
    except Exception as e:
        logging.warning(f"Failed to fetch price history for {symbol}: {e}")
        return None

def process_batch(batch, ticker_info):
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            prices = {}
            for symbol in batch:
                price = fetch_price_history(symbol)
                if price is not None and price >= PRICE_THRESHOLD:
                    info = ticker_info.get(symbol, {}).get("info", {})
                    prices[symbol] = {
                        "info": {
                            "industry": info.get("industry", "n/a"),
                            "sector": info.get("sector", "n/a"),
                            "type": info.get("type", "Unknown"),  # Will be Stock/ETF from ticker_info
                            "Price": price
                        }
                    }
            return len(prices), [s for s in batch if s not in prices]
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Batch error (attempt {attempt+1}): {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)
    else:
        logging.error(f"Batch failed after {MAX_BATCH_RETRIES} attempts.")
        return 0, batch

def main(part_index=None, part_total=None, verbose=False):
    start_time = time.time()
    ensure_dirs()
    setup_logging(verbose)

    start_time_str = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting price build for part {part_index} at {start_time_str}")

    ticker_info = load_ticker_info()
    if not ticker_info:
        logging.error("No ticker_info.json found to process.")
        return

    qualified_tickers = list(ticker_info.keys())
    logging.info(f"Found {len(qualified_tickers)} tickers from ticker_info.json.")

    if part_index is not None and part_total is not None:
        part_tickers = partition_tickers(qualified_tickers, part_index, part_total)
        logging.info(f"Processing part {part_index}/{part_total} with {len(part_tickers)} tickers.")
    else:
        part_tickers = qualified_tickers

    batches = [part_tickers[i:i + BATCH_SIZE] for i in range(0, len(part_tickers), BATCH_SIZE)]
    all_prices = {}

    for idx, batch in enumerate(tqdm(batches, desc="Processing Price Batches"), 1):
        updated, _ = process_batch(batch, ticker_info)
        all_prices.update({k: v for k, v in all_prices.items() if v.get("info", {}).get("Price", 0) >= PRICE_THRESHOLD})
        logging.info("  Batch %d/%d - Fetched prices for %d tickers", idx, len(batches), updated)
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.2f}s before next batch...")
            time.sleep(delay)

    output_file = TICKER_PRICE_PART_FILE % part_index
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_prices, f, indent=2)
    logging.info(f"Saved {len(all_prices)} prices to {output_file}")

    elapsed = time.time() - start_time
    logging.info("Price build completed. Elapsed: %.1fs", elapsed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ticker_price.json from ticker_info.json.")
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--part-total", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    main(part_index=args.part_index, part_total=args.part_total, verbose=args.verbose)
