#!/usr/bin/env python3
import os
import logging
import random
import time
from datetime import datetime
import pandas as pd
from yahooquery import Ticker
from tqdm import tqdm
import re

# Configurable Defaults
BATCH_SIZE = 100
BATCH_DELAY_RANGE = (2, 5)
MAX_BATCH_RETRIES = 2
GOOD_VALUES = {"unknown", "n/a", ""}
SYMBOL_REGEX = re.compile(r"^[A-Z]{1,5}$")
LOG_PATH = "logs/retry_unresolved_tickers.log"
LOG_MAX_BYTES = 2_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > LOG_MAX_BYTES:
        base, ext = os.path.splitext(LOG_PATH)
        rotated = f"{base}-{int(time.time())}{ext or '.log'}"
        os.replace(LOG_PATH, rotated)

def setup_logging():
    rotate_log_if_needed()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def load_unresolved_tickers() -> list[str]:
    unresolved_file = os.path.join("data", "unresolved_tickers.txt")
    if os.path.exists(unresolved_file):
        with open(unresolved_file, "r", encoding="utf-8") as f:
            tickers = [line.strip() for line in f if line.strip()]
            logging.info(f"Loaded {len(tickers)} unresolved tickers")
            return tickers
    logging.warning(f"{unresolved_file} not found.")
    return []

def fetch_nasdaq_symbols():
    url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    try:
        df = pd.read_csv(url, sep="|")
        keep = (df['Test Issue'] == 'N') & (df['Symbol'].str.fullmatch(SYMBOL_REGEX.pattern))
        symbols_data = df.loc[keep].to_dict(orient="records")
        logging.info(f"Fetched {len(symbols_data)} NASDAQ symbols")
        return {rec["Symbol"]: rec for rec in symbols_data}
    except Exception as e:
        logging.error(f"Failed to fetch NASDAQ symbols: {str(e)}")
        return {}

def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def quality(sector: str, industry: str) -> int:
    if not sector or not industry:
        return 0
    st = str(sector).strip().lower()
    it = str(industry).strip().lower()
    if st in GOOD_VALUES or it in GOOD_VALUES:
        return 0
    return 1

def extract_info(mods: dict, symbol: str, nasdaq_data_map: dict):
    entry = mods.get(symbol) or mods.get(yahoo_symbol(symbol))
    if not isinstance(entry, dict):
        return None, None, None
    prof = entry.get("summaryProfile") or {}
    industry = prof.get("industry")
    sector = prof.get("sector")
    etf_flag = nasdaq_data_map.get(symbol, {}).get("ETF", "N")
    type_value = "ETF" if etf_flag == "Y" else "Stock"
    return sector, industry, type_value

def process_batch(batch, nasdaq_data_map):
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            yq = Ticker([yahoo_symbol(s) for s in batch], asynchronous=True, validate=True)
            mods = yq.get_modules(["summaryProfile", "quoteType"])
            logging.info(f"Batch API response type: {type(mods)}")
            break
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Batch error (attempt {attempt+1}): {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)
    else:
        logging.error(f"Batch failed after {MAX_BATCH_RETRIES} attempts.")
        return [], batch

    failed = set(mods.get("failed") or [])
    resolved_tickers = []
    unresolved = []

    for symbol in tqdm(batch, desc="Symbols", leave=False):
        if symbol in failed:
            unresolved.append(symbol)
            logging.warning(f"Ticker {symbol} failed in API response")
            continue

        sector, industry, type_value = extract_info(mods, symbol, nasdaq_data_map)
        if sector and industry and quality(sector, industry):
            resolved_tickers.append(symbol)
            logging.info(f"Resolved ticker: {symbol}")
        else:
            unresolved.append(symbol)
            logging.warning(f"Ticker {symbol} has no valid sector/industry")

    return resolved_tickers, unresolved

def retry_tickers(unresolved_tickers, nasdaq_data_map):
    resolved_tickers = []
    batches = [unresolved_tickers[i:i + BATCH_SIZE] for i in range(0, len(unresolved_tickers), BATCH_SIZE)]
    
    for idx, batch in enumerate(tqdm(batches, desc="Retry Batches"), 1):
        resolved, unresolved_batch = process_batch(batch, nasdaq_data_map)
        resolved_tickers.extend(resolved)
        logging.info(f"Batch {idx}/{len(batches)} - Resolved: {len(resolved)} | Unresolved: {len(unresolved_batch)}")
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.2f}s before next batch...")
            time.sleep(delay)

    return resolved_tickers

def main():
    start_time = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    setup_logging()
    logging.info(f"Starting retry process at {start_time}")

    unresolved_tickers = load_unresolved_tickers()
    if not unresolved_tickers:
        logging.warning("No unresolved tickers to process.")
        return

    nasdaq_data_map = fetch_nasdaq_symbols()
    logging.info(f"Retrieved {len(nasdaq_data_map)} eligible symbols.")

    resolved_tickers = retry_tickers(unresolved_tickers, nasdaq_data_map)
    logging.info(f"Resolved {len(resolved_tickers)} tickers.")

    # Update unresolved tickers file
    with open(os.path.join("data", "unresolved_tickers.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(ticker for ticker in unresolved_tickers if ticker not in resolved_tickers))
    logging.info(f"Updated unresolved_tickers.txt with {len(unresolved_tickers) - len(resolved_tickers)} remaining tickers")

if __name__ == "__main__":
    main()
