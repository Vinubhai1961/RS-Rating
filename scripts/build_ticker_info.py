#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import re
import time
import math
import argparse
import requests
import random
from io import StringIO
import pandas as pd
from yahooquery import Ticker
import logging
from tqdm import tqdm
from typing import List, Dict, Any
from datetime import datetime

# -------------------- Configurable Defaults --------------------
BASE_OUTPUT_PATH = "data/ticker_info"
UNRESOLVED_LIST_PATH = "data/unresolved_tickers.txt"
PARTITION_SUMMARY_PATH = "data/partition_summary.json"
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
BATCH_SIZE = 200  # For Phase 1
PRICE_BATCH_SIZE = 150  # For Phase 2
BATCH_DELAY_RANGE = (2, 5)  # Randomized delay between batches
RETRY_SUBPASS = True
MAX_BATCH_RETRIES = 3
SYMBOL_REGEX = re.compile(r"^[A-Z]{1,5}$")
GOOD_VALUES = {"unknown", "n/a", ""}  # Treated as not good
LOG_PATH = "logs/build_ticker_price.log"
LOG_MAX_BYTES = 2_000_000
PRICE_THRESHOLD = 5.0
# ---------------------------------------------------------------

def ensure_dirs():
    os.makedirs(os.path.dirname(BASE_OUTPUT_PATH), exist_ok=True)
    os.makedirs("logs", exist_ok=True)

def rotate_log_if_needed():
    if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > LOG_MAX_BYTES:
        base, ext = os.path.splitext(LOG_PATH)
        rotated = f"{base}-{int(time.time())}{ext or '.log'}"
        os.replace(LOG_PATH, rotated)

def setup_logging(verbose: bool):
    rotate_log_if_needed()
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def load_existing(part_index: int = None, phase: int = 1) -> Dict[str, Any]:
    if phase == 1:
        if part_index is not None:
            file_path = f"{BASE_OUTPUT_PATH}_part_{part_index}.json"
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError:
                        logging.warning(f"Existing JSON corrupt for partition {part_index}; starting fresh.")
                        return {}
            return {}
        else:
            if os.path.exists(BASE_OUTPUT_PATH + ".json"):
                with open(BASE_OUTPUT_PATH + ".json", "r") as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError:
                        logging.warning("Existing JSON corrupt; starting fresh.")
                        return {}
            return {}
    else:  # Phase 2
        if part_index is not None:
            file_path = f"data/ticker_price_part_{part_index}.json"
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError:
                        logging.warning(f"Existing price JSON corrupt for partition {part_index}; starting fresh.")
                        return {}
            return {}
        else:
            if os.path.exists("data/ticker_price.json"):
                with open("data/ticker_price.json", "r") as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError:
                        logging.warning("Existing price JSON corrupt; starting fresh.")
                        return {}
            return {}

def save(data: Dict[str, Any], part_index: int = None, phase: int = 1):
    if phase == 1:
        if part_index is not None:
            file_path = f"{BASE_OUTPUT_PATH}_part_{part_index}.json"
        else:
            file_path = BASE_OUTPUT_PATH + ".json"
    else:  # Phase 2
        if part_index is not None:
            file_path = f"data/ticker_price_part_{part_index}.json"
        else:
            file_path = "data/ticker_price.json"
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def fetch_nasdaq_symbols(limit=6000) -> List[Dict[str, str]]:
    logging.info("Fetching NASDAQ symbol master list (limited to ~6,000)...")
    resp = requests.get(NASDAQ_URL, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), sep='|')
    keep = (df['Test Issue'] == 'N') & (df['Symbol'].str.fullmatch(SYMBOL_REGEX.pattern))
    symbols_data = df.loc[keep].to_dict(orient="records")
    symbols_data = symbols_data[:min(limit, len(symbols_data))]  # Limit to ~6,000
    logging.info("Retrieved %d eligible symbols.", len(symbols_data))
    return symbols_data

def is_incomplete(info_dict: Dict[str, Any], phase: int = 1) -> bool:
    info = info_dict.get("info", {})
    if phase == 1:
        sector = str(info.get("sector", "")).strip().lower()
        industry = str(info.get("industry", "")).strip().lower()
        return (sector in GOOD_VALUES) or (industry in GOOD_VALUES)
    else:  # Phase 2
        price = info.get("Price")
        return price is None or price < PRICE_THRESHOLD

def needs_update(symbol: str, existing: Dict[str, Any], force_refresh: bool, phase: int = 1) -> bool:
    if symbol not in existing:
        return True
    if force_refresh:
        return True
    return is_incomplete(existing[symbol], phase)

def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def extract_info(mods: Dict[str, Any], symbol: str) -> tuple:
    entry = mods.get(symbol) or mods.get(yahoo_symbol(symbol))
    if not isinstance(entry, dict):
        return None, None, None
    prof = entry.get("summaryProfile") or {}
    industry = prof.get("industry")
    sector = prof.get("sector")
    return sector, industry, None  # Type will come from NASDAQ data

def fetch_price(symbol: str) -> float:
    try:
        hist = Ticker(yahoo_symbol(symbol)).history(period="1d")
        return hist['close'].iloc[-1] if not hist.empty else None
    except Exception as e:
        logging.warning(f"Failed to fetch price for {symbol}: {e}")
        return None

def partition(lst: List[str], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def quality(sector: str, industry: str) -> int:
    if not sector or not industry:
        return 0
    st = sector.lower()
    it = industry.lower()
    if st in GOOD_VALUES or it in GOOD_VALUES:
        return 0
    return 1

def process_batch(batch, existing, nasdaq_data_map, phase: int = 1):
    batch_size = BATCH_SIZE if phase == 1 else PRICE_BATCH_SIZE
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            if phase == 1:
                yq = Ticker([yahoo_symbol(s) for s in batch], asynchronous=True, validate=True)
                mods = yq.get_modules(["summaryProfile", "quoteType"])
            else:  # Phase 2
                prices = {}
                for symbol in batch:
                    price = fetch_price(symbol)
                    if price is not None and price >= PRICE_THRESHOLD:
                        prices[symbol] = {"info": {"Price": price}}
                return len(prices), [s for s in batch if s not in prices]
            break
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Batch error (attempt {attempt+1}): {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)
    else:
        logging.error(f"Batch failed after {MAX_BATCH_RETRIES} attempts.")
        return 0, batch

    if phase == 1:
        failed = set(mods.get("failed") or [])
        updated = 0
        unresolved = []

        for symbol in tqdm(batch, desc="Symbols", leave=False):
            if symbol in failed:
                if symbol not in existing:
                    existing[symbol] = {"info": {"industry": "n/a", "sector": "n/a", "type": nasdaq_data_map[symbol].get("Security Type", "Other")}}
                unresolved.append(symbol)
                continue

            sector, industry, _ = extract_info(mods, symbol)
            sec_type = nasdaq_data_map[symbol].get("Security Type", "Other")
            if sector and industry and quality(sector, industry):
                prev = existing.get(symbol)
                if (not prev) or (prev["info"]["sector"] != sector) or (prev["info"]["industry"] != industry):
                    existing[symbol] = {"info": {"industry": industry, "sector": sector, "type": sec_type}}
                    updated += 1
            else:
                if symbol not in existing:
                    existing[symbol] = {"info": {"industry": "n/a", "sector": "n/a", "type": sec_type}}
                unresolved.append(symbol)

        return updated, unresolved
    return 0, []  # Placeholder for Phase 2, handled above

def write_partition_summary(summary: Dict[str, Any]):
    with open(PARTITION_SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

def main(part_index=None, part_total=None, phase=None, max_batches=None,
         force_refresh=False, verbose=False):
    start_time = time.time()
    ensure_dirs()
    setup_logging(verbose)

    existing = load_existing(part_index, phase)
    if phase == 1:
        nasdaq_data = fetch_nasdaq_symbols()
        nasdaq_data_map = {rec["Symbol"]: rec for rec in nasdaq_data}
        all_symbols = [rec["Symbol"] for rec in nasdaq_data]

        if part_index is not None and part_total is not None:
            per_part = math.ceil(len(all_symbols) / part_total)
            start = part_index * per_part
            end = min(start + per_part, len(all_symbols))
            symbols_slice = all_symbols[start:end]
            logging.info("Partition %d/%d: %d symbols",
                         part_index + 1, part_total, len(symbols_slice))
        else:
            symbols_slice = all_symbols

        todo = [s for s in symbols_slice if needs_update(s, existing, force_refresh, phase)]
        logging.info("Symbols needing update in this slice: %d", len(todo))

        batches = list(partition(todo, BATCH_SIZE))
        if max_batches:
            batches = batches[:max_batches]
            logging.info("Limiting to first %d batches (test mode).", max_batches)

        all_unresolved = []
        updated_total = 0

        for idx, batch in enumerate(tqdm(batches, desc="Processing Batches"), 1):
            updated, unresolved = process_batch(batch, existing, nasdaq_data_map, phase)
            updated_total += updated
            all_unresolved.extend(unresolved)
            save(existing, part_index, phase)
            logging.info("  Batch %d/%d - Updated: %d | Unresolved: %d | Updated total: %d",
                         idx, len(batches), updated, len(unresolved), updated_total)
            if idx < len(batches):
                delay = random.uniform(*BATCH_DELAY_RANGE)
                logging.debug(f"Sleeping {delay:.2f}s before next batch...")
                time.sleep(delay)

        if RETRY_SUBPASS and all_unresolved:
            unresolved_unique = sorted(set(sym for sym in all_unresolved
                                           if is_incomplete(existing.get(sym, {}), phase)))
            if unresolved_unique:
                logging.info("Retry sub-pass for %d unresolved symbols ...", len(unresolved_unique))
                for batch in tqdm(list(partition(unresolved_unique, BATCH_SIZE)),
                                  desc="Retry Batches"):
                    updated, retry_unres = process_batch(batch, existing, nasdaq_data_map, phase)
                    updated_total += updated
                    save(existing, part_index, phase)
                    logging.info("  Retry batch updated: %d | still unresolved: %d | Updated total: %d",
                                 updated, len(retry_unres), updated_total)
                    time.sleep(random.uniform(2, 4))

        unresolved_final = sorted(sym for sym, v in existing.items() if is_incomplete(v, phase))
        with open(UNRESOLVED_LIST_PATH, "w") as f:
            f.write("\n".join(unresolved_final))

        save(existing, part_index, phase)

        elapsed = time.time() - start_time
        summary = {
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "partition_index": part_index,
            "partition_total": part_total,
            "symbols_in_slice": len(symbols_slice),
            "symbols_needing_update": len(todo),
            "batches_run": len(batches),
            "total_updated": updated_total,
            "unresolved_after_run": len(unresolved_final),
            "entries_in_file": len(existing),
            "elapsed_seconds": round(elapsed, 2)
        }
        write_partition_summary(summary)

        logging.info("Done (Phase 1). Total entries: %d | Unresolved: %d | Updated this run: %d | Elapsed: %.1fs",
                     len(existing), len(unresolved_final), updated_total, elapsed)
    else:  # Phase 2
        ticker_info = load_existing(None, 1)  # Load merged ticker_info.json
        if not ticker_info:
            logging.error("No ticker_info.json found for Phase 2.")
            return

        qualified_tickers = list(ticker_info.keys())
        if part_index is not None and part_total is not None:
            per_part = math.ceil(len(qualified_tickers) / part_total)
            start = part_index * per_part
            end = min(start + per_part, len(qualified_tickers))
            part_tickers = qualified_tickers[start:end]
            logging.info("Partition %d/%d: %d tickers",
                         part_index + 1, part_total, len(part_tickers))
        else:
            part_tickers = qualified_tickers

        batches = list(partition(part_tickers, PRICE_BATCH_SIZE))
        if max_batches:
            batches = batches[:max_batches]
            logging.info("Limiting to first %d batches (test mode).", max_batches)

        all_prices = {}

        for idx, batch in enumerate(tqdm(batches, desc="Processing Price Batches"), 1):
            updated, _ = process_batch(batch, all_prices, None, phase)
            all_prices.update({k: v for k, v in all_prices.items() if v.get("info", {}).get("Price", 0) >= PRICE_THRESHOLD})
            logging.info("  Batch %d/%d - Fetched prices for %d tickers", idx, len(batches), updated)
            if idx < len(batches):
                delay = random.uniform(*BATCH_DELAY_RANGE)
                logging.debug(f"Sleeping {delay:.2f}s before next batch...")
                time.sleep(delay)

        save(all_prices, part_index, phase)
        logging.info("Done (Phase 2). Saved %d prices to %s", len(all_prices), f"data/ticker_price_part_{part_index}.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ticker info and price data.")
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--part-total", type=int, required=True)
    parser.add_argument("--phase", type=int, choices=[1, 2], required=True)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    main(part_index=args.part_index, part_total=args.part_total, phase=args.phase,
         max_batches=args.max_batches, force_refresh=args.force_refresh, verbose=args.verbose)
