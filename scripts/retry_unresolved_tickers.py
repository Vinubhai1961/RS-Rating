#!/usr/bin/env python3
import os
import logging
from datetime import datetime
import pandas as pd
from yahooquery import Ticker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/retry_unresolved_tickers.log"),
        logging.StreamHandler()
    ]
)

def load_unresolved_tickers() -> list[str]:
    unresolved_file = os.path.join("data", "unresolved_tickers.txt")
    if os.path.exists(unresolved_file):
        with open(unresolved_file, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    logging.warning(f"{unresolved_file} not found.")
    return []

def fetch_nasdaq_symbols():
    url = "https://www.nasdaq.com/market-activity/stocks/screener"
    # Placeholder logic
    logging.info("Fetching NASDAQ symbol master list ...")
    return ["AAPL", "MSFT"]  # Replace with real fetching logic if needed

def retry_tickers(unresolved_tickers):
    yq = Ticker(unresolved_tickers)
    data = yq.summary_detail
    eligible_tickers = []
    for ticker in unresolved_tickers:
        if ticker in data and data[ticker].get("marketCap", 0) > 0:
            eligible_tickers.append(ticker)
    return eligible_tickers

def main():
    start_time = datetime.now().strftime("%I:%M %p EDT on %A, %B %d, %Y")
    logging.info(f"Starting retry process at {start_time}")

    unresolved_tickers = load_unresolved_tickers()
    logging.info(f"Retrying {len(unresolved_tickers)} unresolved tickers.")

    if not unresolved_tickers:
        logging.warning("No unresolved tickers to process.")
        return

    nasdaq_symbols = fetch_nasdaq_symbols()
    logging.info(f"Retrieved {len(nasdaq_symbols)} eligible symbols.")

    resolved_tickers = retry_tickers(unresolved_tickers)
    logging.info(f"Resolved {len(resolved_tickers)} tickers.")

    with open(os.path.join("data", "unresolved_tickers.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(ticker for ticker in unresolved_tickers if ticker not in resolved_tickers))

if __name__ == "__main__":
    main()
