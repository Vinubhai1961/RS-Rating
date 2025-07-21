#!/usr/bin/env python3
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
BATCH_SIZE = 250
BATCH_DELAY_RANGE = (2, 5)
RETRY_SUBPASS = True
MAX_BATCH_RETRIES = 2
SYMBOL_REGEX = re.compile(r"^[A-Z]{1,5}$")
GOOD_VALUES = {"unknown", "n/a", ""}
LOG_PATH = "logs/build_ticker_info.log"
LOG_MAX_BYTES = 2_000_000
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

def load_existing(part_index: int = None) -> Dict[str, Any]:
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

def save(data: Dict[str, Any], part_index: int = None):
    if part_index is not None:
        file_path = f"{BASE_OUTPUT_PATH}_part_{part_index}.json"
    else:
        file_path = BASE_OUTPUT_PATH + ".json"
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def fetch_nasdaq_symbols() -> List[Dict[str, str]]:
    logging.info("Fetching NASDAQ symbol master list ...")
    resp = requests.get(NASDAQ_URL, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), sep='|')
    keep = (df['Test Issue'] == 'N') & (df['Symbol'].str.fullmatch(SYMBOL_REGEX.pattern))
    symbols_data = df.loc[keep].to_dict(orient="records")
    logging.info("Retrieved %d eligible symbols.", len(symbols_data))
    return symbols_data

def is_incomplete(info_dict: Dict[str, Any]) -> bool:
    info = info_dict.get("info", {})
    sector = str(info.get("sector", "")).strip().lower()
    industry = str(info.get("industry", "")).strip().lower()
    return sector in GOOD_VALUES or industry in GOOD_VALUES

def needs_update(symbol: str, existing: Dict[str, Any], force_refresh: bool) -> bool:
    if symbol not in existing:
        return True
    if force_refresh:
        return True
    return is_incomplete(existing[symbol])

def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")

def extract_info(mods: Dict[str, Any], symbol: str, nasdaq_data_map: Dict[str, Dict[str, str]]):
    entry = mods.get(symbol) or mods.get(yahoo_symbol(symbol))
    if not isinstance(entry, dict):
        return None, None, None
    prof = entry.get("summaryProfile") or {}
    industry = prof.get("industry")
    sector = prof.get("sector")
    etf_flag = nasdaq_data_map.get(symbol, {}).get("ETF", "N")
    type_value = "ETF" if etf_flag == "Y" else "Stock"
    return sector, industry, type_value

def partition(lst: List[str], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def quality(sector: str, industry: str) -> int:
    if not sector or not industry:
        return 0
    st = sector.lower()
    it = industry.lower()
    if st in GOOD_VALUES or it in GOOD_VALUES:
        return 0
    return 1

def process_batch(batch, existing, nasdaq_data_map):
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            yq = Ticker([yahoo_symbol(s) for s in batch], asynchronous=True, validate=True)
            mods = yq.get_modules(["summaryProfile", "quoteType"])
            break
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"Batch error (attempt {attempt+1}): {e}. Retrying in {wait:.1f}s.")
            time.sleep(wait)
    else:
        logging.error(f"Batch failed after {MAX_BATCH_RETRIES} attempts.")
        return 0, batch

    failed = set(mods.get("failed") or [])
    updated = 0
    unresolved = []

    for symbol in tqdm(batch, desc="Symbols", leave=False):
        if symbol in failed:
            if symbol not in existing:
                existing[symbol] = {"info": {"industry": "n/a", "sector": "n/a", "type": nasdaq_data_map[symbol].get("ETF", "N") == "Y" and "ETF" or "Stock"}}
            unresolved.append(symbol)
            continue

        sector, industry, type_value = extract_info(mods, symbol, nasdaq_data_map)
        if sector and industry and quality(sector, industry):
            prev = existing.get(symbol)
            if (not prev) or (prev["info"]["sector"] != sector) or (prev["info"]["industry"] != industry):
                existing[symbol] = {"info": {"industry": industry, "sector": sector, "type": type_value}}
                updated += 1
        else:
            if symbol not in existing:
                existing[symbol] = {"info": {"industry": "n/a", "sector": "n/a", "type": type_value or (nasdaq_data_map[symbol].get("ETF", "N") == "Y" and "ETF" or "Stock")}}
            unresolved.append(symbol)

    return updated, unresolved

def write_partition_summary(summary: Dict[str, Any]):
    with open(PARTITION_SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

def main(part_index=None, part_total=None, max_batches=None,
         force_refresh=False, verbose=False):

    start_time = time.time()
    ensure_dirs()
    setup_logging(verbose)

    existing = load_existing(part_index)
    nasdaq_data = fetch_nasdaq_symbols()
    nasdaq_data_map = {rec["Symbol"]: rec for rec in nasdaq_data}
    all_symbols = [rec["Symbol"] for rec in nasdaq_data]

    if part_index is not None and part_total is not None:
        per_part = math.ceil(len(all_symbols) / part_total)
        start = part_index * per_part
        end = min(start + per_part, len(all_symbols))
        symbols_slice = all_symbols[start:end]
        logging.info("Partition %d/%d: %d symbols",
                     part_index+1, part_total, len(symbols_slice))
    else:
        symbols_slice = all_symbols

    todo = [s for s in symbols_slice if needs_update(s, existing, force_refresh)]
    logging.info("Symbols needing update in this slice: %d", len(todo))

    batches = list(partition(todo, BATCH_SIZE))
    if max_batches:
        batches = batches[:max_batches]
        logging.info("Limiting to first %d batches (test mode).", max_batches)

    all_unresolved = []
    updated_total = 0

    for idx, batch in enumerate(tqdm(batches, desc="Processing Batches"), 1):
        updated, unresolved = process_batch(batch, existing, nasdaq_data_map)
        updated_total += updated
        all_unresolved.extend(unresolved)
        save(existing, part_index)
        logging.info("  Batch %d/%d - Updated: %d | Unresolved: %d | Updated total: %d",
                     idx, len(batches), updated, len(unresolved), updated_total)
        if idx < len(batches):
            delay = random.uniform(*BATCH_DELAY_RANGE)
            logging.debug(f"Sleeping {delay:.2f}s before next batch...")
            time.sleep(delay)

    if RETRY_SUBPASS and all_unresolved:
        unresolved_unique = sorted(set(sym for sym in all_unresolved
                                       if is_incomplete(existing.get(sym, {}))))
        if unresolved_unique:
            logging.info("Retry sub-pass for %d unresolved symbols ...", len(unresolved_unique))
            for batch in tqdm(list(partition(unresolved_unique, BATCH_SIZE)),
                              desc="Retry Batches"):
                updated, retry_unres = process_batch(batch, existing, nasdaq_data_map)
                updated_total += updated
                save(existing, part_index)
                logging.info("  Retry batch updated: %d | still unresolved: %d | Updated total: %d",
                             updated, len(retry_unres), updated_total)
                time.sleep(random.uniform(2, 4))

    unresolved_final = sorted(sym for sym, v in existing.items() if is_incomplete(v))
    with open(UNRESOLVED_LIST_PATH, "w") as f:
        f.write("\n".join(unresolved_final))

    save(existing, part_index)

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

    logging.info("Done. Total entries: %d | Unresolved: %d | Updated this run: %d | Elapsed: %.1fs",
                 len(existing), len(unresolved_final), updated_total, elapsed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build / update ticker_info.json from NASDAQ master list.")
    parser.add_argument("--part-index", type=int, default=None)
    parser.add_argument("--part-total", type=int, default=None)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    main(args.part_index, args.part_total, args.max_batches,
         force_refresh=args.force_refresh, verbose=args.verbose)
