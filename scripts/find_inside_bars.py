#!/usr/bin/env python3
"""
Find tickers with Inside Bar (latest day inside previous day) 
from RS_Data/rs_stocks.csv (only RS Percentile >= threshold)
using ArcticDB price data.
"""

import argparse
import logging
from pathlib import Path

import arcticdb as adb
import pandas as pd
from tqdm import tqdm


def parse_arguments():
    parser = argparse.ArgumentParser(description="Find Inside Bar tickers from RS stocks")
    parser.add_argument("--arctic-db-path", required=True, help="Path to ArcticDB (lmdb folder)")
    parser.add_argument("--input-csv", default="RS_Data/rs_stocks.csv",
                        help="Path to rs_stocks.csv")
    parser.add_argument("--output-dir", default="RS_Data",
                        help="Directory to save IB_Stocks_*.csv")
    parser.add_argument("--log-file", default="logs/failed_ib_tickers.log",
                        help="Log file for skipped/failed tickers")
    parser.add_argument("--rs-threshold", type=float, default=75.0,
                        help="Minimum RS Percentile to consider (default: 75.0)")
    parser.add_argument("--date", required=True,
                        help="Date string for filename e.g. 01282026")
    return parser.parse_args()


def main():
    args = parse_arguments()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(args.log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    logger.info(f"Starting Inside Bar scan | RS >= {args.rs_threshold} | date={args.date}")

    # Connect to ArcticDB
    try:
        arctic = adb.Arctic(f"lmdb://{args.arctic_db_path}")
        if not arctic.has_library("prices"):
            raise ValueError("Library 'prices' not found in ArcticDB")
        lib = arctic.get_library("prices")
    except Exception as e:
        logger.error(f"Failed to open ArcticDB: {e}")
        return

    # Read RS stocks and filter high RS tickers
    try:
        df_rs = pd.read_csv(args.input_csv)
        if "RS Percentile" not in df_rs.columns:
            raise ValueError("'RS Percentile' column not found in input CSV")
        
        df_high = df_rs[df_rs["RS Percentile"] >= args.rs_threshold].copy()
        tickers = df_high["Ticker"].unique().tolist()
        logger.info(f"Found {len(tickers)} tickers with RS Percentile >= {args.rs_threshold}")
    except Exception as e:
        logger.error(f"Failed to read/filter input CSV: {e}")
        return

    inside_bar_tickers = []

    for ticker in tqdm(tickers, desc="Scanning tickers"):
        try:
            item = lib.read(ticker)
            if item is None or item.data is None or item.data.empty:
                logger.debug(f"No data for {ticker}")
                continue

            df_price = item.data

            # Ensure we have datetime as column and sort
            if "datetime" not in df_price.columns:
                logger.debug(f"No 'datetime' column for {ticker}")
                continue

            df_price = df_price.sort_values("datetime").reset_index(drop=True)

            if len(df_price) < 2:
                logger.debug(f"Too few bars ({len(df_price)}) for {ticker}")
                continue

            # Last two rows
            prev = df_price.iloc[-2]
            curr = df_price.iloc[-1]

            # Required columns
            if not all(col in df_price.columns for col in ["high", "low"]):
                logger.debug(f"Missing high/low columns for {ticker}")
                continue

            # Strict inside bar definition
            if curr["high"] < prev["high"] and curr["low"] > prev["low"]:
                inside_bar_tickers.append(ticker)

        except Exception as e:
            logger.warning(f"Error processing {ticker}: {str(e)}")

    # Build result
    if inside_bar_tickers:
        result_df = df_rs[df_rs["Ticker"].isin(inside_bar_tickers)]
        logger.info(f"Found {len(result_df)} inside bar tickers (RS >= {args.rs_threshold})")
    else:
        result_df = df_rs.head(0)  # empty dataframe with same columns
        logger.info("No inside bars found")

    # Save result
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"IB_Stocks_{args.date}.csv"
    output_path = output_dir / filename

    result_df.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path} ({len(result_df)} rows)")


if __name__ == "__main__":
    main()
