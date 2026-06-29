#!/usr/bin/env python3
import os
import json
import math
import pandas as pd

SOURCE_FILE = "source/USA_Tickers_ALL.csv"
EXISTING_PRICE_FILE = "data/ticker_price.json"
OUTPUT_FILE = "data/ticker_price_master.json"

os.makedirs("data", exist_ok=True)


def clean_text(value, default="n/a"):
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    value = str(value).strip()
    if value == "" or value.lower() in {"nan", "none", "null"}:
        return default
    return value


def safe_float(value, default=0):
    try:
        if value is None or pd.isna(value):
            return default
        return round(float(value), 2)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def normalize_type(row, existing_type=None):
    # Flexible support if TradingView file later has ETF / Type column
    for col in ["type", "Type", "ETF", "Is ETF", "Fund", "Asset Type"]:
        if col in row:
            val = clean_text(row.get(col), "").lower()
            if val in {"etf", "fund"}:
                return "ETF"
            if val in {"stock", "common stock"}:
                return "Stock"
            if val in {"true", "yes", "1"}:
                return "ETF"
            if val in {"false", "no", "0"}:
                return "Stock"

    if existing_type in {"Stock", "ETF"}:
        return existing_type

    return "Stock"


def load_existing_price():
    if not os.path.exists(EXISTING_PRICE_FILE):
        return {}

    with open(EXISTING_PRICE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        item.get("ticker", "").upper(): item.get("info", {})
        for item in data
        if item.get("ticker")
    }


def main():
    df = pd.read_csv(SOURCE_FILE)
    existing = load_existing_price()

    master_rows = []

    for _, row in df.iterrows():
        ticker = clean_text(row.get("Symbol"), "").upper()
        if not ticker:
            continue

        old = existing.get(ticker, {})

        sector = clean_text(row.get("Sector"), old.get("sector", "n/a"))
        industry = clean_text(row.get("Industry"), old.get("industry", "n/a"))
        ticker_type = normalize_type(row, old.get("type"))

        item = {
            "ticker": ticker,
            "info": {
                "Price": safe_float(row.get("Price"), old.get("Price", 0)),
                "industry": industry,
                "sector": sector,
                "type": ticker_type,
                "DVol": safe_int(row.get("Volume, 1 day"), old.get("DVol", 0)),
                "AvgVol": old.get("AvgVol", 0),
                "AvgVol10": old.get("AvgVol10", 0),
                "52WKL": safe_float(row.get("Low, 52 weeks"), old.get("52WKL", 0)),
                "52WKH": safe_float(row.get("High, 52 weeks"), old.get("52WKH", 0)),
                "MCAP": old.get("MCAP", 0),
                "Earning_Date": old.get("Earning_Date"),
                "Price_Source": "tradingview_master"
            }
        }

        master_rows.append(item)

    master_rows = sorted(master_rows, key=lambda x: x["ticker"])

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(master_rows, f, indent=2)

    print(f"✅ Created {OUTPUT_FILE}")
    print(f"Total tickers: {len(master_rows)}")


if __name__ == "__main__":
    main()
