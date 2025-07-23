#!/usr/bin/env python3
import logging
from datetime import datetime
import arcticdb as adb

def validate_arctic_data(arctic_lib, log_file):
    """Validate ArcticDB data by logging top 10 tickers by data point count."""
    # Setup logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    # Access ArcticDB
    arctic = adb.Arctic("lmdb://tmp/arctic_db")
    if not arctic.has_library("prices"):
        logging.error("ArcticDB library 'prices' not found for validation")
        return
    
    lib = arctic.get_library("prices")
    
    # Get all symbols and count data points
    ticker_counts = []
    for symbol in lib.list_symbols():
        try:
            data = lib.read(symbol).data
            count = len(data)
            latest_date = datetime.fromtimestamp(data["datetime"].max()).strftime("%Y-%m-%d")
            ticker_counts.append((symbol, count, latest_date))
        except Exception as e:
            logging.info(f"Validation failed for {symbol}: {str(e)}")
    
    # Sort by data point count (descending) and select top 10
    ticker_counts.sort(key=lambda x: x[1], reverse=True)
    top_10 = ticker_counts[:10]
    
    # Log top 10 tickers
    logging.info("Global validation - Top 10 tickers by data point count:")
    for ticker, count, latest_date in top_10:
        logging.info(f"Ticker: {ticker}, Data Points: {count}, Latest Date: {latest_date}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Validate ArcticDB data")
    parser.add_argument("--log-file", default="logs/failed_tickers.log", help="Log file for validation output")
    args = parser.parse_args()
    validate_arctic_data("tmp/arctic_db/prices", args.log_file)
