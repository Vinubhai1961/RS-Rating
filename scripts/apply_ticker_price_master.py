#!/usr/bin/env python3
import json
import logging
import os
from collections import Counter

PRICE_FILE = "data/ticker_price.json"
MASTER_FILE = "data/ticker_price_master.json"
LOG_FILE = "logs/apply_ticker_price_master.log"

BAD_VALUES = {"", "n/a", "na", "nan", "none", "null", "-"}


def setup():
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def is_good(value):
    if value is None:
        return False
    return str(value).strip().lower() not in BAD_VALUES


def load_json(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    setup()

    price_data = load_json(PRICE_FILE)
    master_data = load_json(MASTER_FILE)

    logging.info("Loaded ticker_price rows: %s", len(price_data))
    logging.info("Loaded master rows: %s", len(master_data))

    matched = 0
    missing_master = []

    sector_updates = []
    industry_updates = []
    type_updates = []

    type_counter_before = Counter()
    type_counter_after = Counter()

    for row in price_data:
        ticker = str(row.get("ticker", "")).upper().strip()
        info = row.get("info", {})

        if not ticker:
            continue

        old_type = info.get("type", "Unknown")
        type_counter_before[old_type] += 1

        master = master_data.get(ticker)
        if not master:
            missing_master.append(ticker)
            type_counter_after[info.get("type", "Unknown")] += 1
            continue

        matched += 1

        old_sector = info.get("sector", "n/a")
        old_industry = info.get("industry", "n/a")
        old_type = info.get("type", "Unknown")

        new_sector = master.get("sector")
        new_industry = master.get("industry")
        new_type = master.get("type")

        if is_good(new_sector) and old_sector != new_sector:
            info["sector"] = new_sector
            sector_updates.append((ticker, old_sector, new_sector))

        if is_good(new_industry) and old_industry != new_industry:
            info["industry"] = new_industry
            industry_updates.append((ticker, old_industry, new_industry))

        if is_good(new_type) and old_type != new_type:
            info["type"] = new_type
            type_updates.append((ticker, old_type, new_type))

        type_counter_after[info.get("type", "Unknown")] += 1

    price_data = sorted(price_data, key=lambda x: x.get("ticker", ""))

    with open(PRICE_FILE, "w", encoding="utf-8") as f:
        json.dump(price_data, f, indent=2)

    bad_sector = []
    bad_industry = []

    for row in price_data:
        ticker = row.get("ticker")
        info = row.get("info", {})
        if not is_good(info.get("sector")):
            bad_sector.append(ticker)
        if not is_good(info.get("industry")):
            bad_industry.append(ticker)

    logging.info("============================================================")
    logging.info("APPLY TICKER PRICE MASTER SUMMARY")
    logging.info("============================================================")
    logging.info("ticker_price rows: %s", len(price_data))
    logging.info("master rows: %s", len(master_data))
    logging.info("matched tickers: %s", matched)
    logging.info("missing in master: %s", len(missing_master))
    logging.info("sector updates: %s", len(sector_updates))
    logging.info("industry updates: %s", len(industry_updates))
    logging.info("type updates: %s", len(type_updates))
    logging.info("type count before: %s", dict(type_counter_before))
    logging.info("type count after: %s", dict(type_counter_after))
    logging.info("remaining blank/n-a sector: %s", len(bad_sector))
    logging.info("remaining blank/n-a industry: %s", len(bad_industry))
    logging.info("sample missing in master: %s", missing_master[:50])
    logging.info("sample blank sector: %s", bad_sector[:50])
    logging.info("sample blank industry: %s", bad_industry[:50])
    logging.info("============================================================")

    logging.info("Sample sector updates:")
    for item in sector_updates[:50]:
        logging.info("%s | sector: %s -> %s", item[0], item[1], item[2])

    logging.info("Sample industry updates:")
    for item in industry_updates[:50]:
        logging.info("%s | industry: %s -> %s", item[0], item[1], item[2])

    logging.info("Sample type updates:")
    for item in type_updates[:50]:
        logging.info("%s | type: %s -> %s", item[0], item[1], item[2])

    print("✅ Applied TradingView master corrections to data/ticker_price.json")
    print(f"Matched: {matched}")
    print(f"Sector updates: {len(sector_updates)}")
    print(f"Industry updates: {len(industry_updates)}")
    print(f"Type updates: {len(type_updates)}")
    print(f"Remaining blank sector: {len(bad_sector)}")
    print(f"Remaining blank industry: {len(bad_industry)}")


if __name__ == "__main__":
    main()
