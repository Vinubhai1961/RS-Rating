#!/usr/bin/env python3
import json
import logging
import os
from collections import Counter

import pandas as pd

SOURCE_FILE = "source/USA_Tickers_ALL.csv"
OUTPUT_FILE = "data/ticker_price_master.json"
LOG_FILE = "logs/build_ticker_price_master.log"

BAD_VALUES = {"", "n/a", "na", "nan", "none", "null", "-"}


def setup():
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def clean(value, default="n/a"):
    if value is None or pd.isna(value):
        return default

    value = str(value).strip()
    if value.lower() in BAD_VALUES:
        return default

    return value


def find_col(df, candidates):
    lower_map = {c.lower().strip(): c for c in df.columns}

    for candidate in candidates:
        key = candidate.lower().strip()
        if key in lower_map:
            return lower_map[key]

    return None


def normalize_type(value):
    val = clean(value, "").lower()

    if val in {"etf", "fund"}:
        return "ETF"

    if val in {"stock", "common stock", "common"}:
        return "Stock"

    if val in {"yes", "true", "1"}:
        return "ETF"

    if val in {"no", "false", "0"}:
        return "Stock"

    return "Stock"


def main():
    setup()

    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"{SOURCE_FILE} not found")

    df = pd.read_csv(SOURCE_FILE)
    logging.info("Loaded %s rows from %s", len(df), SOURCE_FILE)
    logging.info("Columns found: %s", list(df.columns))

    symbol_col = find_col(df, ["Symbol", "Ticker", "symbol", "ticker"])
    sector_col = find_col(df, ["Sector", "sector"])
    industry_col = find_col(df, ["Industry", "industry"])
    etf_col = find_col(df, ["ETF", "Is ETF", "Is_ETF", "type", "Type", "Asset Type"])

    if not symbol_col:
        raise ValueError("Could not find Symbol/Ticker column in USA_Tickers_ALL.csv")

    master = {}
    duplicates = []
    type_counter = Counter()

    for _, row in df.iterrows():
        ticker = clean(row.get(symbol_col), "").upper()
        if not ticker:
            continue

        if ticker in master:
            duplicates.append(ticker)
            continue

        sector = clean(row.get(sector_col), "n/a") if sector_col else "n/a"
        industry = clean(row.get(industry_col), "n/a") if industry_col else "n/a"
        ticker_type = normalize_type(row.get(etf_col)) if etf_col else "Stock"

        if ticker_type == "ETF":
            if sector.lower() in BAD_VALUES:
                sector = "ETF"
            if industry.lower() in BAD_VALUES:
                industry = "ETF"

        master[ticker] = {
            "sector": sector,
            "industry": industry,
            "type": ticker_type,
            "Source": "TradingView_Master"
        }

        type_counter[ticker_type] += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(master, f, indent=2)

    blank_sector = [t for t, v in master.items() if v["sector"].lower() in BAD_VALUES]
    blank_industry = [t for t, v in master.items() if v["industry"].lower() in BAD_VALUES]

    logging.info("============================================================")
    logging.info("TICKER PRICE MASTER BUILD SUMMARY")
    logging.info("============================================================")
    logging.info("Master rows written: %s", len(master))
    logging.info("Type count: %s", dict(type_counter))
    logging.info("Duplicate skipped count: %s", len(duplicates))
    logging.info("Blank sector count: %s", len(blank_sector))
    logging.info("Blank industry count: %s", len(blank_industry))
    logging.info("Sample duplicates: %s", duplicates[:25])
    logging.info("Sample blank sector: %s", blank_sector[:25])
    logging.info("Sample blank industry: %s", blank_industry[:25])
    logging.info("Output: %s", OUTPUT_FILE)
    logging.info("============================================================")

    print(f"✅ Created {OUTPUT_FILE}")
    print(f"Total master tickers: {len(master)}")


if __name__ == "__main__":
    main()
