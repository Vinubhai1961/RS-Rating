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
import time
import random
import pandas as pd

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
TICKER_PRICE_PART_FILE = os.path.join(OUTPUT_DIR, f"ticker_price_part_%d.json")
LOG_PATH = "logs/build_ticker_price.log"
BATCH_SIZE = 250
BATCH_DELAY_RANGE = (15, 20)
MAX_BATCH_RETRIES = 3
MAX_RETRY_TIMEOUT = 120
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

def process_batch(batch, ticker_info, is_retry_batch=False):
    prices = {}
    failure_reasons = {"below_threshold": 0, "no_data": 0, "error": 0}
    yahoo_symbols = [yahoo_symbol(symbol) for symbol in batch]
    
    # Process tickers individually to avoid batch failures
    for symbol in batch:
        yahoo_sym = yahoo_symbol(symbol)
        for attempt in range(MAX_BATCH_RETRIES):
            try:
                yq = Ticker(yahoo_sym)
                summary_details = yq.summary_detail
                details = summary_details.get(yahoo_sym, {})
                logging.debug(f"Ticker {symbol} {'(retry)' if is_retry_batch else ''} - summary_details: {list(details.keys()) if details else 'None'}")
                
                # Use regularMarketPrice or previousClose for price
                price = details.get("regularMarketPrice", details.get("previousClose", None))
                if price is None:
                    logging.debug(f"No valid price data for {symbol} {'(retry)' if is_retry_batch else ''}: {details}")
                    failure_reasons["no_data"] += 1
                    break
                
                if price >= PRICE_THRESHOLD:
                    info = ticker_info.get(symbol, {}).get("info", {})
                    if not details:
                        logging.debug(f"No summary details for {symbol} {'(retry)' if is_retry_batch else ''}")
                        failure_reasons["no_data"] += 1
                        break
                    prices[symbol] = {
                        "info": {
                            "industry": info.get("industry", "n/a"),
                            "sector": info.get("sector", "n/a"),
                            "type": info.get("type", "Unknown"),
                            "Price": price,
                            "volume": details.get("volume", None),
                            "averageVolume": details.get("averageVolume", None),
                            "averageVolume10days": details.get("averageVolume10days", None),
                            "marketCap": details.get("marketCap", None),
                            "fiftyTwoWeekLow": details.get("fiftyTwoWeekLow", None),
                            "fiftyTwoWeekHigh": details.get("fiftyTwoWeekHigh", None)
                        }
                    }
                else:
                    logging.debug(f"Skipping {symbol} {'(retry)' if is_retry_batch else ''}: Price {price} below threshold")
                    failure_reasons["below_threshold"] += 1
                break  # Success, move to next ticker
            except Exception as e:
                if "429" in str(e) or "curl" in str(e).lower():
                    wait = min((2 ** attempt) * random.uniform(5, 10), MAX_RETRY_TIMEOUT)
                    logging.warning(f"Ticker {symbol} {'(retry)' if is_retry_batch else ''} error (attempt {attempt+1}/{MAX_BATCH_RETRIES}): {str(e)}. Retrying in {wait:.1f}s.")
                    time.sleep(wait)
                    if attempt == MAX_BATCH_RETRIES - 1:
                        logging.debug(f"Max retries reached for {symbol} {'(retry)' if is_retry_batch else ''}: {str(e)}")
                        failure_reasons["error"] += 1
                else:
                    logging.debug(f"Ticker {symbol} {'(retry)' if is_retry_batch else ''} failed: {str(e)}")
                    failure_reasons["error"] += 1
                    break
    
    failed_tickers = [s for s in batch if s not in prices]
    logging.info(f"Batch failure reasons {'(retry)' if is_retry_batch else ''}: {failure_reasons}")
    if failed_tickers:
        logging.debug(f"Failed tickers {'(retry)' if is_retry_batch else ''}: {failed_tickers[:10]}{'...' if len(failed_tickers) > 10 else ''}")
    return len(prices), failed_tickers, prices

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
    all_failed_tickers = []

    for idx, batch in enumerate(tqdm(batches, desc="Processing Price Batches"), 1):
        updated, failed_tickers, prices = process_batch(batch, ticker_info)
        all_prices.update(prices)
        all_failed_tickers.extend(failed_tickers)
        logging.info(f"Batch {idx}/{len(batches)} - Fetched prices for {updated} tickers")
        if failed_tickers:
            logging.debug(f"Batch {idx}: Failed tickers: {failed_tickers[:10]}{'...' if len(failed_tickers) > 10 else ''}")
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.1f}s before next batch...")
            time.sleep(delay)

    # Process failed tickers in batches at the end
    if all_failed_tickers:
        logging.info(f"Processing {len(all_failed_tickers)} failed tickers in batches...")
        failed_batches = [all_failed_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_failed_tickers), BATCH_SIZE)]
        for idx, batch in enumerate(tqdm(failed_batches, desc="Processing Failed Tickers"), 1):
            updated, newly_failed, prices = process_batch(batch, ticker_info, is_retry_batch=True)
            all_prices.update(prices)
            logging.info(f"Failed Tickers Batch {idx}/{len(failed_batches)} - Fetched prices for {updated} tickers")
            if newly_failed:
                logging.debug(f"Failed Tickers Batch {idx}: Still failed: {newly_failed[:10]}{'...' if len(newly_failed) > 10 else ''}")
            if idx < len(failed_batches):
                delay = random.uniform(*BATCH_DELAY_RANGE)
                logging.debug(f"Sleeping {delay:.1f}s before next failed batch...")
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
