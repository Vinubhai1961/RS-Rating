#!/usr/bin/env python3
import json
import logging
import os
from collections import Counter

PRICE_FILE = "data/ticker_price.json"
MASTER_FILE = "data/ticker_price_master.json"
EXCLUDE_FILE = "source/problematic_stocks.txt"
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


def normalize_compare(value):
    return normalize(value).lower()


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


def load_exclusion_list(path):
    if not os.path.exists(path):
        logging.warning("%s not found. No problematic tickers excluded.", path)
        return set()

    with open(path, "r", encoding="utf-8") as f:
        tickers = {
            line.strip().upper()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        }

    logging.info("Loaded problematic exclusion tickers: %s", len(tickers))
    return tickers


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
            if normalize(row.get("ticker")).upper() == ticker:
                return row.get("info", row)

    return {}


def group_by_first_letter(tickers, letters=("A", "B", "C", "D")):
    groups = {letter: [] for letter in letters}

    for ticker in tickers:
        ticker = normalize(ticker).upper()
        if not ticker:
            continue

        first = ticker[0]
        if first in groups:
            groups[first].append(ticker)

    return groups


def print_grouped_tickers(title, groups):
    logging.info("============================================================")
    logging.info(title)
    logging.info("============================================================")

    print("")
    print("============================================================")
    print(title)
    print("============================================================")

    for letter in ["A", "B", "C", "D"]:
        tickers = groups.get(letter, [])
        line = ", ".join(tickers) if tickers else "(none)"

        logging.info("%s count: %s", letter, len(tickers))
        logging.info("%s tickers: %s", letter, line)

        print(f"{letter} count: {len(tickers)}")
        print(f"{letter} tickers: {line}")


def main():
    setup_logging()

    price_data = load_json(PRICE_FILE)
    master_data = load_json(MASTER_FILE)
    exclude_tickers = load_exclusion_list(EXCLUDE_FILE)

    if not isinstance(price_data, list):
        raise ValueError("data/ticker_price.json must be a list")

    matched = 0
    missing_in_master = []

    sector_updates = []
    industry_updates = []
    type_updates = []

    sector_mismatches = []
    industry_mismatches = []

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
        # MISMATCH REPORT ONLY:
        # Track differences when both existing and master are valid.
        # Do NOT overwrite valid ticker_price.json values.
        # -------------------------------------------------
        if (
            is_good(old_sector)
            and is_good(new_sector)
            and normalize_compare(old_sector) != normalize_compare(new_sector)
        ):
            sector_mismatches.append((ticker, old_sector, new_sector))

        if (
            is_good(old_industry)
            and is_good(new_industry)
            and normalize_compare(old_industry) != normalize_compare(new_industry)
        ):
            industry_mismatches.append((ticker, old_industry, new_industry))

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

    # -------------------------------------------------
    # Remove problematic tickers from final output
    # -------------------------------------------------
    before_exclusion_count = len(price_data)

    excluded_rows = [
        normalize(row.get("ticker")).upper()
        for row in price_data
        if normalize(row.get("ticker")).upper() in exclude_tickers
    ]

    price_data = [
        row
        for row in price_data
        if normalize(row.get("ticker")).upper() not in exclude_tickers
    ]

    logging.info("============================================================")
    logging.info("PROBLEMATIC TICKER EXCLUSION SUMMARY")
    logging.info("============================================================")
    logging.info("Rows before exclusion: %s", before_exclusion_count)
    logging.info("Problematic tickers loaded: %s", len(exclude_tickers))
    logging.info("Excluded tickers count: %s", len(excluded_rows))
    logging.info("Rows after exclusion: %s", len(price_data))
    logging.info("Excluded tickers: %s", excluded_rows)
    logging.info("============================================================")

    print("")
    print("============================================================")
    print("PROBLEMATIC TICKER EXCLUSION SUMMARY")
    print("============================================================")
    print(f"Rows before exclusion: {before_exclusion_count}")
    print(f"Problematic tickers loaded: {len(exclude_tickers)}")
    print(f"Excluded tickers count: {len(excluded_rows)}")
    print(f"Rows after exclusion: {len(price_data)}")
    print(f"Excluded tickers: {', '.join(excluded_rows) if excluded_rows else '(none)'}")
    print("============================================================")

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

    coverage = (matched / before_exclusion_count * 100) if before_exclusion_count else 0

    logging.info("============================================================")
    logging.info("APPLY TICKER PRICE MASTER SUMMARY")
    logging.info("============================================================")
    logging.info("Mode: FILL MISSING ONLY + EXCLUDE PROBLEMATIC TICKERS")
    logging.info("ticker_price rows before exclusion: %s", before_exclusion_count)
    logging.info("ticker_price rows after exclusion: %s", len(price_data))
    logging.info("matched master tickers: %s", matched)
    logging.info("coverage before exclusion: %.2f%%", coverage)
    logging.info("missing in master: %s", len(missing_in_master))
    logging.info("duplicate ticker_price rows: %s", len(duplicate_price_rows))
    logging.info("sector filled: %s", len(sector_updates))
    logging.info("industry filled: %s", len(industry_updates))
    logging.info("type filled: %s", len(type_updates))
    logging.info("sector mismatches report-only: %s", len(sector_mismatches))
    logging.info("industry mismatches report-only: %s", len(industry_mismatches))
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

    logging.info("Sample sector mismatches report-only:")
    for ticker, old, new in sector_mismatches[:100]:
        logging.info("%s | ticker_price sector: %s | master sector: %s", ticker, old, new)

    logging.info("Sample industry mismatches report-only:")
    for ticker, old, new in industry_mismatches[:100]:
        logging.info("%s | ticker_price industry: %s | master industry: %s", ticker, old, new)

    # -------------------------------------------------
    # Group missing tickers A/B/C/D for screen + logs
    # -------------------------------------------------
    sector_groups = group_by_first_letter(remaining_blank_sector)
    industry_groups = group_by_first_letter(remaining_blank_industry)

    print_grouped_tickers(
        "MISSING SECTOR TICKERS BY LETTER A/B/C/D",
        sector_groups
    )

    print_grouped_tickers(
        "MISSING INDUSTRY TICKERS BY LETTER A/B/C/D",
        industry_groups
    )

    # -------------------------------------------------
    # Print mismatch details on screen
    # -------------------------------------------------
    print("")
    print("============================================================")
    print("SECTOR MISMATCHES REPORT ONLY")
    print("============================================================")
    print(f"Total sector mismatches: {len(sector_mismatches)}")
    for ticker, old, new in sector_mismatches[:100]:
        print(f"{ticker}: ticker_price='{old}' | master='{new}'")

    print("")
    print("============================================================")
    print("INDUSTRY MISMATCHES REPORT ONLY")
    print("============================================================")
    print(f"Total industry mismatches: {len(industry_mismatches)}")
    for ticker, old, new in industry_mismatches[:100]:
        print(f"{ticker}: ticker_price='{old}' | master='{new}'")

    # -------------------------------------------------
    # Final console summary
    # -------------------------------------------------
    print("")
    print("============================================================")
    print("APPLY TICKER PRICE MASTER FINAL SUMMARY")
    print("============================================================")
    print("Mode: FILL MISSING ONLY + EXCLUDE PROBLEMATIC TICKERS")
    print(f"Rows before exclusion: {before_exclusion_count}")
    print(f"Rows after exclusion: {len(price_data)}")
    print(f"Matched master tickers: {matched}")
    print(f"Coverage before exclusion: {coverage:.2f}%")
    print(f"Missing in master: {len(missing_in_master)}")
    print(f"Duplicate ticker_price rows: {len(duplicate_price_rows)}")
    print(f"Problematic tickers loaded: {len(exclude_tickers)}")
    print(f"Excluded tickers count: {len(excluded_rows)}")
    print(f"Sector filled: {len(sector_updates)}")
    print(f"Industry filled: {len(industry_updates)}")
    print(f"Type filled: {len(type_updates)}")
    print(f"Sector mismatches report-only: {len(sector_mismatches)}")
    print(f"Industry mismatches report-only: {len(industry_mismatches)}")
    print(f"Remaining blank sector: {len(remaining_blank_sector)}")
    print(f"Remaining blank industry: {len(remaining_blank_industry)}")
    print(f"Remaining bad type: {len(remaining_bad_type)}")
    print("============================================================")
    print("✅ Applied TradingView master using FILL-MISSING-ONLY mode")


if __name__ == "__main__":
    main()
