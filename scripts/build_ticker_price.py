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

def process_batch(batch, ticker_info):
    start_time = time.time()
    total_wait = 0
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            prices = {}
            failure_reasons = {"below_threshold": 0, "no_data": 0, "error": 0}
            yahoo_symbols = [yahoo_symbol(symbol) for symbol in batch]
            
            try:
                yq = Ticker(yahoo_symbols)
                hist = yq.history(period="5d")  # Use 5d to ensure data availability
                summary_details = yq.summary_detail
                logging.debug(f"Batch API response - hist type: {type(hist)}, keys: {list(hist.index.get_level_values(0)) if isinstance(hist, pd.DataFrame) else hist}")
                logging.debug(f"Batch API response - summary_details: {list(summary_details.keys())}")
            except Exception as e:
                logging.warning(f"Batch API call failed: {str(e)}. Processing tickers individually.")
                hist = {}
                summary_details = {}
                for symbol in batch:
                    yahoo_sym = yahoo_symbol(symbol)
                    try:
                        yq_single = Ticker(yahoo_sym)
                        hist_single = yq_single.history(period="5d")
                        summary_single = yq_single.summary_detail
                        hist[yahoo_sym] = hist_single
                        summary_details[yahoo_sym] = summary_single.get(yahoo_sym, {})
                        logging.debug(f"Individual ticker {symbol} - hist: {hist_single.shape if isinstance(hist_single, pd.DataFrame) else hist_single}")
                        logging.debug(f"Individual ticker {symbol} - summary_details: {list(summary_single.keys())}")
                    except Exception as e:
                        logging.debug(f"Individual ticker {symbol} failed: {str(e)}")
                        failure_reasons["error"] += 1
                        continue
            
            for symbol in batch:
                yahoo_sym = yahoo_symbol(symbol)
                try:
                    price = None
                    if yahoo_sym in hist and isinstance(hist[yahoo_sym], pd.DataFrame) and not hist[yahoo_sym].empty:
                        if 'close' in hist[yahoo_sym].columns:
                            price = hist[yahoo_sym]['close'].iloc[-1]
                        else:
                            logging.debug(f"No 'close' column for {symbol}: {hist[yahoo_sym].columns}")
                            failure_reasons["no_data"] += 1
                            continue
                    else:
                        logging.debug(f"No price data for {symbol}: {hist.get(yahoo_sym, 'No history data')}")
                        failure_reasons["no_data"] += 1
                        continue
                    if price >= PRICE_THRESHOLD:
                        info = ticker_info.get(symbol, {}).get("info", {})
                        details = summary_details.get(yahoo_sym, {})
                        if not details:
                            logging.debug(f"No summary details for {symbol}")
                            failure_reasons["no_data"] += 1
                            continue
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
                        logging.debug(f"Skipping {symbol}: Price {price} below threshold")
                        failure_reasons["below_threshold"] += 1
                except Exception as e:
                    logging.debug(f"Failed to process {symbol}: {str(e)}")
                    failure_reasons["error"] += 1
                    continue
            
            failed_tickers = [s for s in batch if s not in prices]
            logging.info(f"Batch failure reasons: {failure_reasons}")
            if failed_tickers:
                logging.debug(f"Failed tickers: {failed_tickers[:10]}{'...' if len(failed_tickers) > 10 else ''}")
            return len(prices), failed_tickers, prices
        except Exception as e:
            if "429" in str(e) or "curl" in str(e).lower():
                wait = min((2 ** attempt) * random.uniform(5, 10), MAX_RETRY_TIMEOUT - total_wait)
                total_wait += wait
                if total_wait >= MAX_RETRY_TIMEOUT:
                    logging.warning(f"Max retry timeout reached for batch after {total_wait:.1f}s. Skipping.")
                    break
                logging.warning(f"Batch error (attempt {attempt+1}/{MAX_BATCH_RETRIES}): {str(e)}. Retrying in {wait:.1f}s.")
                time.sleep(wait)
            else:
                logging.error(f"Unexpected error in batch: {str(e)}. Processing tickers individually.")
                prices = {}
                failure_reasons = {"below_threshold": 0, "no_data": 0, "error": 0}
                for symbol in batch:
                    try:
                        yahoo_sym = yahoo_symbol(symbol)
                        yq = Ticker(yahoo_sym)
                        hist = yq.history(period="5d")
                        details = yq.summary_detail.get(yahoo_sym, {})
                        if isinstance(hist, pd.DataFrame) and not hist.empty and 'close' in hist.columns:
                            price = hist['close'].iloc[-1]
                        else:
                            logging.debug(f"No valid price data for {symbol}: {hist}")
                            failure_reasons["no_data"] += 1
                            continue
                        if price >= PRICE_THRESHOLD:
                            info = ticker_info.get(symbol, {}).get("info", {})
                            if not details:
                                logging.debug(f"No summary details for {symbol}")
                                failure_reasons["no_data"] += 1
                                continue
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
                            logging.debug(f"Skipping {symbol}: Price {price} below threshold")
                            failure_reasons["below_threshold"] += 1
                    except Exception as e:
                        logging.debug(f"Individual ticker {symbol} failed: {str(e)}")
                        failure_reasons["error"] += 1
                        continue
                failed_tickers = [s for s in batch if s not in prices]
                logging.info(f"Batch failure reasons (individual processing): {failure_reasons}")
                if failed_tickers:
                    logging.debug(f"Failed tickers: {failed_tickers[:10]}{'...' if len(failed_tickers) > 10 else ''}")
                return len(prices), failed_tickers, prices
    return 0, batch, {}

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
            updated, newly_failed, prices = process_batch(batch, ticker_info)
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
