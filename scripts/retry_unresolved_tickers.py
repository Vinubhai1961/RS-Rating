#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from io import StringIO

import pandas as pd
import requests
from yahooquery import Ticker

LOG_FILE = "logs/retry_unresolved_tickers.log"
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"

BAD_VALUES = {"", "n/a", "na", "nan", "none", "null", "-", "unknown"}


def setup_logging():
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def is_bad(value):
    if value is None:
        return True
    return str(value).strip().lower() in BAD_VALUES


def yahoo_symbol(symbol):
    return symbol.replace(".", "-")


def load_json(path, default):
    if not os.path.exists(path):
        return default

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def load_lines(path):
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        return [
            line.strip().upper()
            for line in f
            if line.strip()
        ]


def load_excluded_symbols(path):
    """
    Supports data/excluded_symbols.txt format:

    SYMBOL|ETF|reason|Security Name

    Also supports plain one-symbol-per-line format.
    """
    if not os.path.exists(path):
        return set()

    out = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            symbol = line.split("|")[0].strip().upper()
            if symbol:
                out.add(symbol)

    return out


def fetch_nasdaq_map():
    try:
        resp = requests.get(NASDAQ_URL, timeout=60)
        resp.raise_for_status()

        df = pd.read_csv(StringIO(resp.text), sep="|")

        return {
            str(row["Symbol"]).upper(): row.to_dict()
            for _, row in df.iterrows()
            if str(row.get("Symbol", "")).strip()
        }

    except Exception as e:
        logging.warning("Could not fetch NASDAQ map: %s", e)
        return {}


def extract_type(symbol, nasdaq_map):
    row = nasdaq_map.get(symbol, {})
    etf = str(row.get("ETF", "N")).strip().upper()
    return "ETF" if etf == "Y" else "Stock"


def get_summary_profile(symbol):
    ysym = yahoo_symbol(symbol)

    try:
        yq = Ticker(ysym, asynchronous=False, validate=True)
        mods = yq.get_modules(["summaryProfile", "quoteType"])

        entry = mods.get(ysym) or mods.get(symbol)

        if not isinstance(entry, dict):
            return None, None

        profile = entry.get("summaryProfile") or {}

        sector = profile.get("sector")
        industry = profile.get("industry")

        return sector, industry

    except Exception as e:
        logging.debug("%s retry failed: %s", symbol, e)
        return None, None


def is_incomplete(row):
    info = row.get("info", {})
    return is_bad(info.get("sector")) or is_bad(info.get("industry"))


def main(data_dir):
    setup_logging()

    ticker_info_path = os.path.join(data_dir, "ticker_info.json")
    unresolved_path = os.path.join(data_dir, "unresolved_tickers.txt")
    excluded_path = os.path.join(data_dir, "excluded_symbols.txt")

    ticker_info = load_json(ticker_info_path, {})
    unresolved = load_lines(unresolved_path)
    excluded = load_excluded_symbols(excluded_path)
    nasdaq_map = fetch_nasdaq_map()

    logging.info("============================================================")
    logging.info("RETRY UNRESOLVED TICKERS")
    logging.info("============================================================")
    logging.info("ticker_info entries: %s", len(ticker_info))
    logging.info("unresolved loaded: %s", len(unresolved))
    logging.info("excluded loaded: %s", len(excluded))
    logging.info("============================================================")

    skipped_excluded = []
    recovered = []
    still_unresolved = []

    # Deduplicate while preserving order
    seen = set()
    unresolved_unique = []

    for symbol in unresolved:
        if symbol not in seen:
            unresolved_unique.append(symbol)
            seen.add(symbol)

    for symbol in unresolved_unique:
        if symbol in excluded:
            skipped_excluded.append(symbol)
            continue

        current = ticker_info.get(symbol, {"info": {}})

        if not is_incomplete(current):
            recovered.append(symbol)
            continue

        security_type = extract_type(symbol, nasdaq_map)

        if security_type == "ETF":
            ticker_info[symbol] = {
                "info": {
                    "sector": "ETF",
                    "industry": "ETF",
                    "type": "ETF"
                }
            }
            recovered.append(symbol)
            logging.info("%s recovered as ETF from NASDAQ map", symbol)
            continue

        sector, industry = get_summary_profile(symbol)

        if not is_bad(sector) and not is_bad(industry):
            ticker_info[symbol] = {
                "info": {
                    "sector": sector,
                    "industry": industry,
                    "type": "Stock"
                }
            }
            recovered.append(symbol)
            logging.info("%s recovered: sector=%s | industry=%s", symbol, sector, industry)
        else:
            # Preserve existing fallback if present
            if symbol not in ticker_info:
                ticker_info[symbol] = {
                    "info": {
                        "sector": "n/a",
                        "industry": "n/a",
                        "type": security_type
                    }
                }

            still_unresolved.append(symbol)

        time.sleep(0.15)

    save_json(ticker_info_path, ticker_info)

    with open(unresolved_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(still_unresolved)))

    logging.info("============================================================")
    logging.info("RETRY SUMMARY")
    logging.info("============================================================")
    logging.info("Unresolved before retry: %s", len(unresolved_unique))
    logging.info("Recovered: %s", len(recovered))
    logging.info("Skipped because excluded: %s", len(skipped_excluded))
    logging.info("Still unresolved: %s", len(still_unresolved))
    logging.info("Sample recovered: %s", recovered[:50])
    logging.info("Sample skipped excluded: %s", skipped_excluded[:50])
    logging.info("Sample still unresolved: %s", still_unresolved[:50])
    logging.info("============================================================")

    print("")
    print("============================================================")
    print("RETRY UNRESOLVED TICKERS SUMMARY")
    print("============================================================")
    print(f"Unresolved before retry: {len(unresolved_unique)}")
    print(f"Recovered: {len(recovered)}")
    print(f"Skipped because excluded: {len(skipped_excluded)}")
    print(f"Still unresolved: {len(still_unresolved)}")
    print(f"Sample recovered: {recovered[:50]}")
    print(f"Sample skipped excluded: {skipped_excluded[:50]}")
    print(f"Sample still unresolved: {still_unresolved[:50]}")
    print("============================================================")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/retry_unresolved_tickers.py data")
        sys.exit(1)

    main(sys.argv[1])
