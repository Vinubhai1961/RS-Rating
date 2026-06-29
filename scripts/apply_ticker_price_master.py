#!/usr/bin/env python3
import json
import logging
import os
from collections import Counter

PRICE_FILE = "data/ticker_price.json"
MASTER_FILE = "data/ticker_price_master.json"
LOG_FILE = "logs/apply_ticker_price_master.log"

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


def normalize(value):
    if value is None:
        return ""
    return str(value).strip()


def is_good(value):
    return normalize(value).lower() not in BAD_VALUES


def load_json(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def get_master_info(master_data, ticker):
    """
    Supports both formats:

    Format 1:
    {
      "AA": {
        "sector": "Basic Materials",
        "industry": "Aluminum",
        "type": "Stock"
      }
    }

    Format 2:
    [
      {
        "ticker": "AA",
        "info": {
          "sector": "Basic Materials",
          "industry": "Aluminum",
          "type": "Stock"
        }
      }
    ]
    """

    if isinstance(master_data, dict):
        return master_data.get(ticker, {})

    if isinstance(master_data, list):
        for row in master_data:
            if str(row.get("ticker", "")).upper().strip() == ticker:
                return row.get("info", {})

    return {}


def main():
    setup_logging()

    price_data = load_json(PRICE_FILE)
    master_data = load_json(MASTER_FILE)

    if not isinstance(price_data, list):
        raise ValueError("data/ticker_price.json must be a list")

    matched = 0
    missing_in_master = []

    sector_updates = []
    industry_updates = []
    type_updates = []

    type_counter_before = Counter()
    type_counter_after = Counter()

    rows_seen = set()
    duplicate_price_rows = []

    for row in price_data:
        ticker = normalize(row.get("ticker")).upper()

        if not ticker:
            continue

        if ticker in rows_seen:
            duplicate_price_rows.append(ticker)
        rows_seen.add(ticker)

        info = row.setdefault("info", {})

        old_sector = info.get("sector", "n/a")
        old_industry = info.get("industry", "n/a")
        old_type = info.get("type", "Unknown")

        type_counter_before[normalize(old_type) or "Unknown"] += 1

        master = get_master_info(master_data, ticker)

        if not master:
            missing_in_master.append(ticker)
            type_counter_after[normalize(info.get("type")) or "Unknown"] += 1
            continue

        matched += 1

        new_sector = master.get("sector", "n/a")
        new_industry = master.get("industry", "n/a")
        new_type = master.get("type", "Unknown")

        # -------------------------------------------------
        # SAFE RULE:
        # Only fill missing values.
        # Never overwrite existing valid Yahoo/current values.
        # -------------------------------------------------

        if not is_good(old_sector) and is_good(new_sector):
            info["sector"] = new_sector
            sector_updates.append((ticker, old_sector, new_sector))

        if not is_good(old_industry) and is_good(new_industry):
            info["industry"] = new_industry
            industry_updates.append((ticker, old_industry, new_industry))

        if not is_good(old_type) and is_good(new_type):
            info["type"] = new_type
            type_updates.append((ticker, old_type, new_type))

        type_counter_after[normalize(info.get("type")) or "Unknown"] += 1

    price_data = sorted(price_data, key=lambda x: normalize(x.get("ticker")).upper())

    save_json(PRICE_FILE, price_data)

    remaining_blank_sector = []
    remaining_blank_industry = []
    remaining_bad_type = []

    for row in price_data:
        ticker = normalize(row.get("ticker")).upper()
        info = row.get("info", {})

        if not is_good(info.get("sector")):
            remaining_blank_sector.append(ticker)

        if not is_good(info.get("industry")):
            remaining_blank_industry.append(ticker)

        if not is_good(info.get("type")):
            remaining_bad_type.append(ticker)

    coverage = (matched / len(price_data) * 100) if price_data else 0

    logging.info("============================================================")
    logging.info("APPLY TICKER PRICE MASTER SUMMARY")
    logging.info("============================================================")
    logging.info("Mode: FILL MISSING ONLY")
    logging.info("ticker_price rows: %s", len(price_data))
    logging.info("matched master tickers: %s", matched)
    logging.info("coverage: %.2f%%", coverage)
    logging.info("missing in master: %s", len(missing_in_master))
    logging.info("duplicate ticker_price rows: %s", len(duplicate_price_rows))
    logging.info("sector filled: %s", len(sector_updates))
    logging.info("industry filled: %s", len(industry_updates))
    logging.info("type filled: %s", len(type_updates))
    logging.info("type count before: %s", dict(type_counter_before))
    logging.info("type count after: %s", dict(type_counter_after))
    logging.info("remaining blank/n-a sector: %s", len(remaining_blank_sector))
    logging.info("remaining blank/n-a industry: %s", len(remaining_blank_industry))
    logging.info("remaining bad type: %s", len(remaining_bad_type))
    logging.info("sample missing in master: %s", missing_in_master[:50])
    logging.info("sample duplicate ticker_price rows: %s", duplicate_price_rows[:50])
    logging.info("sample remaining blank sector: %s", remaining_blank_sector[:50])
    logging.info("sample remaining blank industry: %s", remaining_blank_industry[:50])
    logging.info("sample remaining bad type: %s", remaining_bad_type[:50])
    logging.info("============================================================")

    logging.info("Sample sector fills:")
    for ticker, old, new in sector_updates[:50]:
        logging.info("%s | sector: %s -> %s", ticker, old, new)

    logging.info("Sample industry fills:")
    for ticker, old, new in industry_updates[:50]:
        logging.info("%s | industry: %s -> %s", ticker, old, new)

    logging.info("Sample type fills:")
    for ticker, old, new in type_updates[:50]:
        logging.info("%s | type: %s -> %s", ticker, old, new)

    print("✅ Applied TradingView master using FILL-MISSING-ONLY mode")
    print(f"Rows: {len(price_data)}")
    print(f"Matched master tickers: {matched}")
    print(f"Coverage: {coverage:.2f}%")
    print(f"Sector filled: {len(sector_updates)}")
    print(f"Industry filled: {len(industry_updates)}")
    print(f"Type filled: {len(type_updates)}")
    print(f"Remaining blank sector: {len(remaining_blank_sector)}")
    print(f"Remaining blank industry: {len(remaining_blank_industry)}")
    print(f"Missing in master: {len(missing_in_master)}")


if __name__ == "__main__":
    main()
