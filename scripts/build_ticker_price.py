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

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
TICKER_PRICE_PART_FILE = os.path.join(OUTPUT_DIR, "ticker_price_part_%d.json")
UNRESOLVED_PRICE_TICKERS = os.path.join(OUTPUT_DIR, "unresolved_price_tickers.txt")
LOG_PATH = "logs/build_ticker_price.log"
BATCH_SIZE = 250
BATCH_DELAY_RANGE = (20, 30)  # Increased for two API calls
MAX_BATCH_RETRIES = 3
MAX_RETRY_TIMEOUT = 120
RETRY_SUBPASS = True
PRICE_THRESHOLD = 5.0  # Hardcoded minimum price threshold

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
            failure_reasons = {"no_price": 0, "no_summary": 0, "below_threshold": 0, "error": 0}
            yahoo_symbols = [yahoo_symbol(symbol) for symbol in batch]
            yq = Ticker(yahoo_symbols)
            hist = yq.history(period="1d")
            summary_details = yq.summary_detail
            
            for symbol in batch:
                yahoo_sym = yahoo_symbol(symbol)
                try:
                    # Extract price from history
                    price = None
                    if yahoo_sym in hist.index.get_level_values(0):
                        price = hist.loc[yahoo_sym]['close'].iloc[-1] if not hist.loc[yahoo_sym].empty else None
                    
                    # Extract summary details
                    summary = summary_details.get(yahoo_sym, {}) if isinstance(summary_details, dict) else {}
                    
                    # Validate both datasets
                    if price is None or not isinstance(price, (int, float)):
                        logging.debug(f"Skipping {symbol}: No or invalid price data")
                        failure_reasons["no_price"] += 1
                        continue
                    if not summary or any(key not in summary for key in ["volume", "averageVolume", "averageVolume10days", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "marketCap"]):
                        logging.debug(f"Skipping {symbol}: Missing summary data")
                        failure_reasons["no_summary"] += 1
                        continue
                    if price < PRICE_THRESHOLD:
                        logging.debug(f"Skipping {symbol}: Price {price} below threshold {PRICE_THRESHOLD}")
                        failure_reasons["below_threshold"] += 1
                        continue
                    
                    # Validate numerical fields
                    if not isinstance(summary["volume"], int) or summary["volume"] < 0:
                        logging.debug(f"Skipping {symbol}: Invalid volume {summary['volume']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    if not isinstance(summary["averageVolume"], int) or summary["averageVolume"] < 0:
                        logging.debug(f"Skipping {symbol}: Invalid averageVolume {summary['averageVolume']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    if not isinstance(summary["averageVolume10days"], int) or summary["averageVolume10days"] < 0:
                        logging.debug(f"Skipping {symbol}: Invalid averageVolume10days {summary['averageVolume10days']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    if not isinstance(summary["fiftyTwoWeekLow"], (int, float)) or summary["fiftyTwoWeekLow"] <= 0:
                        logging.debug(f"Skipping {symbol}: Invalid fiftyTwoWeekLow {summary['fiftyTwoWeekLow']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    if not isinstance(summary["fiftyTwoWeekHigh"], (int, float)) or summary["fiftyTwoWeekHigh"] <= 0:
                        logging.debug(f"Skipping {symbol}: Invalid fiftyTwoWeekHigh {summary['fiftyTwoWeekHigh']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    if not isinstance(summary["marketCap"], (int, float)) or summary["marketCap"] < 0:
                        logging.debug(f"Skipping {symbol}: Invalid marketCap {summary['marketCap']}")
                        failure_reasons["no_summary"] += 1
                        continue
                    
                    # Combine data with rounded numerical values
                    info = ticker_info.get(symbol, {}).get("info", {})
                    prices[symbol] = {
                        "info": {
                            "industry": info.get("industry", "n/a"),
                            "sector": info.get("sector", "n/a"),
                            "type": info.get("type", "Unknown"),
                            "Price": round(price, 2),
                            "DVol": summary.get("volume"),
                            "AvgVol": summary.get("averageVolume"),
                            "AvgVol10": summary.get("averageVolume10days"),
                            "52WKL": round(summary.get("fiftyTwoWeekLow", 0), 2),
                            "52WKH": round(summary.get("fiftyTwoWeekHigh", 0), 2),
                            "MCAP": round(summary.get("marketCap", 0), 2)
                        }
                    }
                except Exception as e:
                    logging.debug(f"Failed to process {symbol}: {e}")
                    failure_reasons["error"] += 1
            
            failed_tickers = [s for s in batch if s not in prices]
            logging.info(f"Batch failure reasons: {failure_reasons}")
            return len(prices), failed_tickers, prices
        except Exception as e:
            if "429" in str(e) or "curl" in str(e).lower():
                wait = min((2 ** attempt) * random.uniform(5, 10), MAX_RETRY_TIMEOUT - total_wait)
                total_wait += wait
                if total_wait >= MAX_RETRY_TIMEOUT:
                    logging.warning(f"Max retry timeout reached for batch after {total_wait:.1f}s. Skipping.")
                    break
                logging.warning(f"Batch error (attempt {attempt+1}/{MAX_BATCH_RETRIES}): {e}. Retrying in {wait:.1f}s.")
                time.sleep(wait)
            else:
                logging.error(f"Unexpected error in batch: {e}. Aborting batch.")
                break
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
    all_failed = []

    for idx, batch in enumerate(tqdm(batches, desc="Processing Price Batches"), 1):
        updated, failed_tickers, prices = process_batch(batch, ticker_info)
        all_prices.update(prices)
        all_failed.extend(failed_tickers)
        logging.info(f"Batch {idx}/{len(batches)} - Fetched data for {updated} tickers")
        if failed_tickers:
            logging.debug(f"Batch {idx}: Failed tickers: {failed_tickers}")
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.1f}s before next batch...")
            time.sleep(delay)

    if RETRY_SUBPASS and all_failed:
        unresolved_unique = sorted(set(all_failed))
        logging.info(f"Retry sub-pass for {len(unresolved_unique)} unresolved tickers...")
        retry_batches = [unresolved_unique[i:i + BATCH_SIZE] for i in range(0, len(unresolved_unique), BATCH_SIZE)]
        for idx, batch in enumerate(tqdm(retry_batches, desc="Retry Price Batches"), 1):
            updated, failed_tickers, prices = process_batch(batch, ticker_info)
            all_prices.update(prices)
            logging.info(f"Retry Batch {idx}/{len(retry_batches)} - Fetched data for {updated} tickers")
            if failed_tickers:
                logging.debug(f"Retry Batch {idx}: Failed tickers: {failed_tickers}")
            time.sleep(random.uniform(5, 10))

    unresolved_final = sorted(set(all_failed))
    with open(UNRESOLVED_PRICE_TICKERS, "w") as f:
        f.write("\n".join(unresolved_final))
    logging.info(f"Saved {len(unresolved_final)} unresolved tickers to {UNRESOLVED_PRICE_TICKERS}")

    output_file = TICKER_PRICE_PART_FILE % part_index
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_prices, f, indent=None)  # No indentation for compact output
    logging.info(f"Saved {len(all_prices)} entries to {output_file}")

    elapsed = time.time() - start_time
    logging.info("Price build completed. Elapsed: %.1fs", elapsed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ticker_price.json from ticker_info.json.")
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--part-total", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    main(part_index=args.part_index, part_total=args.part_total, verbose=args.verbose)
