#!/usr/bin/env python3
import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import arcticdb as adb
from tqdm.auto import tqdm

try:
    from pandas_market_calendars import get_calendar
except ImportError:
    get_calendar = None
    logging.warning("pandas_market_calendars not installed. Falling back to consecutive days for RSRATING.csv.")

def quarters_perf(closes: pd.Series, n: int) -> float:
    days = n * 63
    available_data = closes[-min(len(closes), days):]
    if len(available_data) < 1:
        return np.nan
    elif len(available_data) == 1:
        return 0.0  # For IPOs, use 0% change as baseline
    pct_change = available_data.pct_change().dropna()
    return (pct_change + 1).cumprod().iloc[-1] - 1 if not pct_change.empty else np.nan

def strength(closes: pd.Series) -> float:
    perfs = [quarters_perf(closes, i) for i in range(1, 5)]
    valid_perfs = [p for p in perfs if not np.isnan(p)]
    if not valid_perfs:
        return np.nan
    weights = [0.4, 0.2, 0.2, 0.2][:len(valid_perfs)]
    total_weight = sum(weights)
    weights = [w / total_weight for w in weights] if total_weight > 0 else weights
    return sum(w * p for w, p in zip(weights, valid_perfs))

def relative_strength(closes: pd.Series, closes_ref: pd.Series) -> float:
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    if np.isnan(rs_stock) or np.isnan(rs_ref):
        logging.info(f"NaN RS for ticker with {len(closes)} days, ref with {len(closes_ref)} days")
        return np.nan
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    return round(rs, 2) if rs <= 590 else np.nan

def load_arctic_db(data_dir):
    try:
        if not os.path.exists(data_dir):
            raise Exception(f"ArcticDB directory {data_dir} does not exist")
        arctic = adb.Arctic(f"lmdb://{data_dir}")
        if not arctic.has_library("prices"):
            raise Exception(f"No 'prices' library found in {data_dir}")
        lib = arctic.get_library("prices")
        symbols = lib.list_symbols()
        logging.info(f"Found {len(symbols)} symbols in {data_dir}")
        return lib, symbols
    except Exception as e:
        logging.error(f"Database error in {data_dir}: {str(e)}")
        print(f"❌ ArcticDB error in {data_dir}: {str(e)}")
        return None

def generate_tradingview_csv(df_stocks, output_dir, ref_data, percentile_values=None, use_trading_days=True):
    if percentile_values is None:
        percentile_values = [98, 89, 69, 49, 29, 9, 1]
    
    lines = []
    latest_date = datetime.fromtimestamp(ref_data["datetime"].max())
    logging.info(f"Using latest date {latest_date} for RSRATING.csv")

    if use_trading_days and get_calendar is not None:
        try:
            calendar = get_calendar('NYSE')
            trading_days = calendar.schedule(start_date=latest_date - timedelta(days=7), end_date=latest_date)
            if len(trading_days) < 5:
                raise ValueError(f"Insufficient trading days found: {len(trading_days)}")
            dates = trading_days.index[-5:].strftime('%Y%m%dT')
            logging.info(f"Selected trading days: {', '.join(dates)}")
        except Exception as e:
            logging.warning(f"Failed to get trading days: {str(e)}. Falling back to consecutive days.")
            use_trading_days = False
    
    if not use_trading_days or get_calendar is None:
        dates = [(latest_date - timedelta(days=i)).strftime('%Y%m%dT') for i in range(4, -1, -1)]
        logging.info(f"Using consecutive days: {', '.join(dates)}")

    first_rs_values = {}
    for percentile in percentile_values:
        matching_rows = df_stocks[df_stocks["Relative Strength Percentile"] == percentile]
        if not matching_rows.empty:
            first_rs = matching_rows.iloc[0]["Relative Strength"]
            first_rs_values[percentile] = first_rs
            logging.info(f"Selected RS {first_rs} for percentile {percentile}")
        else:
            first_rs_values[percentile] = 0
            logging.warning(f"No stocks found for percentile {percentile}, defaulting to 0")

    for percentile in sorted(percentile_values, reverse=True):
        rs_value = first_rs_values[percentile]
        for date_str in dates:
            csv_row = f"{date_str},0,1000,0,{rs_value},0\n"
            lines.append(csv_row)

    csv_content = ''.join(lines)
    with open(os.path.join(output_dir, "RSRATING.csv"), "w") as csv_file:
        csv_file.write(csv_content)
    logging.info(f"Generated RSRATING.csv with {len(lines)} rows")
    return csv_content

def main(arctic_db_path, reference_ticker, output_dir, log_file, metadata_file=None, percentiles=None):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    logging.info("Starting RS calculation process")

    result = load_arctic_db(arctic_db_path)
    if not result:
        logging.error("Failed to load ArcticDB. Exiting.")
        print("❌ Failed to load ArcticDB. See logs.")
        sys.exit(1)

    lib, tickers = result
    
    if reference_ticker not in tickers:
        logging.error(f"Reference ticker {reference_ticker} not found")
        print(f"❌ Reference ticker {reference_ticker} not found in ArcticDB.")
        sys.exit(1)

    # Validate reference ticker data
    ref_data = lib.read(reference_ticker).data
    ref_closes = pd.Series(ref_data["close"].values, index=pd.to_datetime(ref_data["datetime"], unit='s'))
    if len(ref_closes) < 20:
        logging.error(f"Reference ticker {reference_ticker} has insufficient data ({len(ref_closes)} days)")
        print(f"❌ Not enough reference ticker data.")
        sys.exit(1)

    # Pre-check insufficient data tickers
    insufficient_tickers = []
    for ticker in tickers:
        if ticker == reference_ticker:
            continue
        try:
            data = lib.read(ticker).data
            closes = pd.Series(data["close"].values, index=pd.to_datetime(data["datetime"], unit='s'))
            if len(closes) < 1:
                insufficient_tickers.append(ticker)
        except Exception:
            insufficient_tickers.append(ticker)
    logging.info(f"Found {len(insufficient_tickers)} tickers with no data: {insufficient_tickers[:5]}...")

    metadata_df = pd.DataFrame()
    if metadata_file and os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)
            metadata = [
                {
                    "Ticker": t,
                    "Price": round(float(data[t].get("info", {}).get("Price", np.nan)), 2),
                    "Sector": data[t].get("info", {}).get("sector"),
                    "Industry": data[t].get("info", {}).get("industry"),
                    "Type": data[t].get("info", {}).get("type")
                }
                for t in data
            ]
            metadata_df = pd.DataFrame(metadata)
            if "Ticker" not in metadata_df.columns or metadata_df.empty:
                logging.warning(f"Metadata file {metadata_file} invalid or lacks 'Ticker' column. Proceeding without metadata.")
                metadata_df = pd.DataFrame()
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.error(f"Invalid metadata file {metadata_file}: {str(e)}. Proceeding without metadata.")
            metadata_df = pd.DataFrame()

    logging.info(f"Starting RS calculation for {len(tickers)} tickers")
    print(f"🔍 Processing {len(tickers)} tickers...")

    rs_results = []
    valid_rs_count = 0
    for ticker in tqdm(tickers, desc="Calculating RS"):
        if ticker == reference_ticker:
            continue
        try:
            data = lib.read(ticker).data
            closes = pd.Series(data["close"].values, index=pd.to_datetime(data["datetime"], unit='s'))
            if len(closes) < 2:
                rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan))
                continue
            rs = relative_strength(closes, ref_closes)
            rs_1m = relative_strength(closes[:-20], ref_closes[:-20]) if len(closes) > 20 else np.nan
            rs_3m = relative_strength(closes[:-60], ref_closes[:-60]) if len(closes) > 60 else np.nan
            rs_6m = relative_strength(closes[:-120], ref_closes[:-120]) if len(closes) > 120 else np.nan
            rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m))
            if not np.isnan(rs):
                valid_rs_count += 1
        except Exception as e:
            logging.info(f"{ticker}: Failed to process ({str(e)})")
            rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan))

    df_stocks = pd.DataFrame(rs_results, columns=["Ticker", "Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"])
    if not metadata_df.empty and "Ticker" in metadata_df.columns:
        df_stocks = df_stocks.merge(metadata_df, on="Ticker", how="left")
    else:
        df_stocks = df_stocks
        if not metadata_df.empty:
            logging.warning("Metadata file lacks 'Ticker' column. Skipping merge.")

    if df_stocks.empty:
        logging.warning("No tickers processed due to errors or empty data")
        print("⚠️ No RS results calculated. Check if ArcticDB has data or reference ticker.")
        sys.exit(1)

    # Calculate percentiles (0-99 range) only for non-NaN values
    for col in ["Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"]:
        valid_values = df_stocks[col].dropna()
        if not valid_values.empty:
            df_stocks.loc[valid_values.index, f"{col} Percentile"] = pd.qcut(valid_values.rank(method="min") - 1, 100, labels=False, duplicates="drop")
        else:
            df_stocks[f"{col} Percentile"] = np.nan

    df_stocks = df_stocks.sort_values("Relative Strength", ascending=False, na_position="last").reset_index(drop=True)
    df_stocks["Rank"] = df_stocks.index + 1

    # Add IPO flag for tickers with less than 20 days
    df_stocks["IPO"] = df_stocks["Ticker"].apply(lambda t: "Yes" if len(lib.read(t).data) < 20 else "No")

    df_stocks.loc[df_stocks["Type"] == "ETF", "Industry"] = "ETF"
    df_stocks.loc[df_stocks["Type"] == "ETF", "Sector"] = "ETF"

    # Save rs_stocks.csv with blanks for NaN and IPO flag
    df_stocks[["Rank", "Ticker", "Price", "Sector", "Industry", "Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago", "IPO"]].to_csv(
        os.path.join(output_dir, "rs_stocks.csv"), index=False, na_rep="")

    # Aggregate by industry with Ticker list
    df_industries = df_stocks.groupby("Industry").agg({
        "Relative Strength": "mean",
        "1 Month Ago": "mean",
        "3 Months Ago": "mean",
        "6 Months Ago": "mean",
        "Sector": "first",
        "Ticker": lambda x: ",".join(x)
    }).reset_index()

    # Round RS means to 2 decimal places, handle NaN
    for col in ["Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"]:
        df_industries[col] = df_industries[col].round(2).fillna("")

    df_industries = df_industries.sort_values("Relative Strength", ascending=False, na_position="last").reset_index(drop=True)
    df_industries["Rank"] = df_industries.index + 1

    # Save rs_industries.csv with Ticker column
    df_industries[["Rank", "Industry", "Sector", "Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago", "Ticker"]].to_csv(
        os.path.join(output_dir, "rs_industries.csv"), index=False, na_rep="")

    # Generate TradingView-compatible RSRATING.csv
    generate_tradingview_csv(df_stocks, output_dir, ref_data, percentiles)

    logging.info(f"✅ RS calculation completed. {len(df_stocks)} tickers processed, {valid_rs_count} with valid RS.")
    print(f"\n✅ RS calculation completed. {len(df_stocks)} tickers written.")
    print(f"📄 Output files:")
    print(f" - rs_stocks.csv")
    print(f" - rs_industries.csv")
    print(f" - RSRATING.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate RS from ArcticDB")
    parser.add_argument("--arctic-db-path", default="data/arctic_db/prices", help="Path to ArcticDB root (no scheme)")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker symbol")
    parser.add_argument("--output-dir", default="output", help="Directory to save results")
    parser.add_argument("--log-file", default="logs/failed_logs.log", help="Log file path")
    parser.add_argument("--metadata-file", default=None, help="Optional ticker metadata JSON file")
    parser.add_argument("--percentiles", default="98,89,69,49,29,9,1", help="Comma-separated list of percentiles for RSRATING.csv")
    args = parser.parse_args()

    percentiles = [int(p) for p in args.percentiles.split(",")]
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    main(args.arctic_db_path, args.reference_ticker, args.output_dir, args.log_file, args.metadata_file, percentiles)
