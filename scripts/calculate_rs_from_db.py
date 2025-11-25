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
    slice_len = min(len(closes), days + 1)  # +1 for n intervals
    available_data = closes[-slice_len:]
    if len(available_data) < 2:  # Need at least 2 for ratio
        return 0.0 if len(available_data) == 1 else np.nan
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


def short_relative_strength(closes: pd.Series, closes_ref: pd.Series, days: int) -> float:
    if len(closes) < days + 1 or len(closes_ref) < days + 1:
        return np.nan
    stock_ret = closes.iloc[-1] / closes.iloc[-days] - 1
    ref_ret = closes_ref.iloc[-1] / closes_ref.iloc[-days] - 1
    rs = (1 + stock_ret) / (1 + ref_ret) * 100
    return round(rs, 2) if rs <= 590 else np.nan  # Keep your cap


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
        print(f"ArcticDB error in {data_dir}: {str(e)}")
        return None


def generate_tradingview_csv(df_stocks, output_dir, ref_data, percentile_values=None, use_trading_days=True):
    if percentile_values is None:
        percentile_values = [98, 89, 69, 49, 29, 9, 1]

    latest_ts = ref_data["datetime"].max()
    latest_date = datetime.fromtimestamp(latest_ts).date()
    logging.info(f"Latest market date (NYSE): {latest_date}")

    # === Get last 5 trading days (NYSE calendar) ===
    dates = []
    if use_trading_days and get_calendar:
        try:
            cal = get_calendar('NYSE')
            sched = cal.schedule(start_date=latest_date - timedelta(days=20),
                               end_date=latest_date + timedelta(days=2))
            valid_dates = [d.date() for d in sched.index if d.date() <= latest_date]
            dates = [d.strftime('%Y%m%dT') for d in valid_dates[-5:]]
            logging.info(f"NYSE trading days used: {', '.join(dates)}")
        except Exception as e:
            logging.warning(f"NYSE calendar failed ({e}) → using consecutive days")

    if len(dates) < 5:
        dates = [(latest_date - timedelta(days=i)).strftime('%Y%m%dT') for i in range(4, -1, -1)]
        logging.info(f"Fallback consecutive dates: {', '.join(dates)}")

    # === CORRECT: Threshold = lowest RS in that percentile group ===
    valid_rs = df_stocks["RS"].dropna().sort_values(ascending=False).reset_index(drop=True)
    total = len(valid_rs)

    rs_map = {}
    for p in percentile_values:
        if total == 0:
            rs_map[p] = 100.0
            continue
        top_n = max(1, round(total * (100 - p) / 100.0))
        threshold_rs = valid_rs.iloc[min(top_n - 1, total - 1)]
        rs_map[p] = round(float(threshold_rs), 2)

    # === Generate CSV ===
    lines = []
    for p in sorted(percentile_values, reverse=True):
        rs_val = rs_map[p]
        for d in dates:
            lines.append(f"{d},0,1000,0,{rs_val},0\n")

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "RSRATING.csv")
    with open(path, "w") as f:
        f.write(''.join(lines))

    logging.info(f"RSRATING.csv (NYSE) generated → {path}")
    print("=== NYSE RSRATING.csv thresholds (now correct) ===")
    for p in sorted(percentile_values, reverse=True):
        print(f"  {p:2}th percentile → Raw RS ≥ {rs_map[p]:6.2f}")

    return ''.join(lines)


def main(arctic_db_path, reference_ticker, output_dir, log_file, metadata_file=None, percentiles=None, debug=False):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    logging.info("Starting RS calculation process")

    result = load_arctic_db(arctic_db_path)
    if not result:
        logging.error("Failed to load ArcticDB. Exiting.")
        print("Failed to load ArcticDB. See logs.")
        sys.exit(1)

    lib, tickers = result

    if reference_ticker not in tickers:
        logging.error(f"Reference ticker {reference_ticker} not found")
        print(f"Reference ticker {reference_ticker} not found in ArcticDB.")
        sys.exit(1)

    # Validate reference ticker data
    ref_data = lib.read(reference_ticker).data
    ref_closes = pd.Series(ref_data["close"].values, index=pd.to_datetime(ref_data["datetime"], unit='s'))
    if len(ref_closes) < 20:
        logging.error(f"Reference ticker {reference_ticker} has insufficient data ({len(ref_closes)} days)")
        print(f"Not enough reference ticker data.")
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

    # === METADATA LOADING (100% unchanged from your original) ===
    metadata_df = pd.DataFrame()
    if metadata_file and os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r") as f:
                data = json.load(f)
            logging.info(f"Metadata file structure: {type(data).__name__}")
            if isinstance(data, dict):
                metadata = [
                    {
                        "Ticker": t,
                        "Price": round(float(data[t].get("info", {}).get("Price", np.nan)), 2),
                        "DVol": data[t].get("info", {}).get("DVol"),
                        "Sector": data[t].get("info", {}).get("sector"),
                        "Industry": data[t].get("info", {}).get("industry"),
                        "AvgVol": data[t].get("info", {}).get("AvgVol"),
                        "AvgVol10": data[t].get("info", {}).get("AvgVol10"),
                        "52WKH": data[t].get("info", {}).get("52WKH"),
                        "52WKL": data[t].get("info", {}).get("52WKL"),
                        "MCAP": data[t].get("info", {}).get("MCAP"),
                        "Type": data[t].get("info", {}).get("type")
                    }
                    for t in data
                ]
            elif isinstance(data, list):
                metadata = [
                    {
                        "Ticker": item.get("ticker"),
                        "Price": round(float(item.get("info", {}).get("Price", np.nan)), 2),
                        "DVol": item.get("info", {}).get("DVol"),
                        "Sector": item.get("info", {}).get("sector"),
                        "Industry": item.get("info", {}).get("industry"),
                        "AvgVol": item.get("info", {}).get("AvgVol"),
                        "AvgVol10": item.get("info", {}).get("AvgVol10"),
                        "52WKH": item.get("info", {}).get("52WKH"),
                        "52WKL": item.get("info", {}).get("52WKL"),
                        "MCAP": item.get("info", {}).get("MCAP"),
                        "Type": item.get("info", {}).get("type")
                    }
                    for item in data
                ]
            else:
                raise ValueError(f"Unsupported metadata format: {type(data).__name__}")
            metadata_df = pd.DataFrame(metadata)
            if "Ticker" not in metadata_df.columns or metadata_df.empty:
                logging.warning(f"Metadata file {metadata_file} invalid or lacks 'Ticker' column. Proceeding without metadata.")
                metadata_df = pd.DataFrame()
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.error(f"Invalid metadata file {metadata_file}: {str(e)}. Proceeding without metadata.")
            metadata_df = pd.DataFrame()

    logging.info(f"Starting RS calculation for {len(tickers)} tickers")
    print(f"Processing {len(tickers)} tickers...")

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
            rs_1m = short_relative_strength(closes, ref_closes, 21)
            rs_3m = short_relative_strength(closes, ref_closes, 63)
            rs_6m = short_relative_strength(closes, ref_closes, 126)
            rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m))
            if not np.isnan(rs):
                valid_rs_count += 1
        except Exception as e:
            logging.info(f"{ticker}: Failed to process ({str(e)})")
            rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan))

    df_stocks = pd.DataFrame(rs_results, columns=["Ticker", "RS", "1M_RS", "3M_RS", "6M_RS"])
    if not metadata_df.empty and "Ticker" in metadata_df.columns:
        df_stocks = df_stocks.merge(metadata_df, on="Ticker", how="left")

    if df_stocks.empty:
        logging.warning("No tickers processed due to errors or empty data")
        print("No RS results calculated. Check if ArcticDB has data or reference ticker.")
        sys.exit(1)

    # Calculate percentiles (0-99 range)
    for col in ["RS", "1M_RS", "3M_RS", "6M_RS"]:
        valid_values = df_stocks[col].dropna()
        if not valid_values.empty:
            df_stocks.loc[valid_values.index, f"{col} Percentile"] = (valid_values.rank(pct=True, method='min') * 99).astype(int)
        else:
            df_stocks[f"{col} Percentile"] = np.nan

    df_stocks = df_stocks.sort_values("RS", ascending=False, na_position="last").reset_index(drop=True)
    df_stocks["Rank"] = df_stocks.index + 1

    df_stocks["IPO"] = df_stocks.apply(
        lambda row: "Yes" if row["Type"] == "Stock" and len(lib.read(row["Ticker"]).data) < 20 else "No", axis=1
    )

    df_stocks.loc[df_stocks["Type"] == "ETF", "Industry"] = "ETF"
    df_stocks.loc[df_stocks["Type"] == "ETF", "Sector"] = "ETF"

    # Save main files
    os.makedirs(output_dir, exist_ok=True)
    df_stocks[["Rank", "Ticker", "Price", "DVol", "Sector", "Industry", "RS Percentile", "1M_RS Percentile",
               "3M_RS Percentile", "6M_RS Percentile", "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP", "IPO"]] \
        .to_csv(os.path.join(output_dir, "rs_stocks.csv"), index=False, na_rep="")

    # Industry aggregation
    df_industries = df_stocks.groupby("Industry").agg({
        "RS Percentile": "mean", "1M_RS Percentile": "mean",
        "3M_RS Percentile": "mean", "6M_RS Percentile": "mean",
        "Sector": "first",
        "Ticker": lambda x: ",".join(sorted(x, key=lambda t: float(df_stocks.loc[df_stocks["Ticker"] == t, "MCAP"].iloc[0] or 0), reverse=True))
    }).reset_index()

    for col in ["RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile"]:
        df_industries[col] = df_industries[col].fillna(0).round().astype(int)

    df_industries = df_industries.sort_values("RS Percentile", ascending=False).reset_index(drop=True)
    df_industries["Rank"] = df_industries.index + 1
    df_industries = df_industries.rename(columns={
        "RS Percentile": "RS", "1M_RS Percentile": "1 M_RS",
        "3M_RS Percentile": "3M_RS", "6M_RS Percentile": "6M_RS"
    })
    df_industries[["Rank", "Industry", "Sector", "RS", "1 M_RS", "3M_RS", "6M_RS", "Ticker"]] \
        .to_csv(os.path.join(output_dir, "rs_industries.csv"), index=False)

    generate_tradingview_csv(df_stocks, output_dir, ref_data, percentiles)

    logging.info(f"RS calculation completed. {len(df_stocks)} tickers processed, {valid_rs_count} with valid RS.")
    print(f"\nRS calculation completed. {len(df_stocks)} tickers written.")
    print(f"Output files:")
    print(f" - rs_stocks.csv")
    print(f" - rs_industries.csv")
    print(f" - RSRATING.csv")

    # =================================================================
    # FULL DEBUG MODE: All tickers → split debug-rs-*.csv in logs/debug_rs/
    # =================================================================
    if debug:
        print("\nStarting FULL DEBUG export for ALL tickers...")
        debug_dir = os.path.join(os.path.dirname(log_file), "debug_rs")
        os.makedirs(debug_dir, exist_ok=True)

        debug_records = []
        strength_ref_cached = strength(ref_closes)  # Cache once

        for row in tqdm(df_stocks.itertuples(), total=len(df_stocks), desc="Debug Export"):
            ticker = row.Ticker
            try:
                data = lib.read(ticker).data
                closes = pd.Series(data["close"].values, index=pd.to_datetime(data["datetime"], unit='s'))
                num_days = len(closes)
                start_date = pd.to_datetime(data["datetime"].min(), unit='s').date()
                end_date = pd.to_datetime(data["datetime"].max(), unit='s').date()
                sufficient = "Sufficient" if num_days >= 252 else f"Short: {num_days}d"

                perf_3m = quarters_perf(closes, 1)
                perf_6m = quarters_perf(closes, 2)
                perf_9m = quarters_perf(closes, 3)
                perf_12m = quarters_perf(closes, 4)
                strength_stock = strength(closes)
                rs = getattr(row, 'RS', np.nan)
                recomputed = relative_strength(closes, ref_closes)

                debug_records.append({
                    'Ticker': ticker,
                    'Days': num_days,
                    'Start_Date': start_date,
                    'End_Date': end_date,
                    'Sufficient': sufficient,
                    '3M_Return': round(perf_3m, 4) if not np.isnan(perf_3m) else None,
                    '6M_Return': round(perf_6m, 4) if not np.isnan(perf_6m) else None,
                    '9M_Return': round(perf_9m, 4) if not np.isnan(perf_9m) else None,
                    '12M_Return': round(perf_12m, 4) if not np.isnan(perf_12m) else None,
                    'Strength_Stock': round(strength_stock, 4) if not np.isnan(strength_stock) else None,
                    'Strength_Ref': round(strength_ref_cached, 4),
                    'RS': rs,
                    'Recomputed_RS': recomputed,
                    'Match': "OK" if abs((rs or 0) - (recomputed or 0)) < 0.1 else "MISMATCH"
                })
            except Exception as e:
                debug_records.append({
                    'Ticker': ticker, 'Days': 0, 'Start_Date': None, 'End_Date': None,
                    'Sufficient': 'Error', '3M_Return': None, '6M_Return': None,
                    '9M_Return': None, '12M_Return': None, 'Strength_Stock': None,
                    'Strength_Ref': round(strength_ref_cached, 4), 'RS': rs,
                    'Recomputed_RS': None, 'Match': f"Error: {e}"
                })

        # Split into chunks of 3990
        chunk_size = 3990
        total = len(debug_records)
        for i in range(0, total, chunk_size):
            chunk = debug_records[i:i + chunk_size]
            part = (i // chunk_size) + 1
            path = os.path.join(debug_dir, f"debug-rs-{part}.csv")
            pd.DataFrame(chunk).to_csv(path, index=False)
            print(f"  → {path}  ({len(chunk)} rows)")

        print(f"\nFULL DEBUG EXPORT DONE! {total:,} records → logs/debug_rs/debug-rs-*.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate RS from ArcticDB")
    parser.add_argument("--arctic-db-path", default="data/arctic_db/prices", help="Path to ArcticDB root (no scheme)")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker symbol")
    parser.add_argument("--output-dir", default="output", help="Directory to save results")
    parser.add_argument("--log-file", default="logs/failed_logs.log", help="Log file path")
    parser.add_argument("--metadata-file", default=None, help="Optional ticker metadata JSON file")
    parser.add_argument("--percentiles", default="98,89,69,49,29,9,1", help="Comma-separated list of percentiles for RSRATING.csv")
    parser.add_argument("--debug", action="store_true", help="Enable FULL debug export (all tickers → split CSVs)")
    args = parser.parse_args()

    percentiles = [int(p) for p in args.percentiles.split(",")]
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    main(args.arctic_db_path, args.reference_ticker, args.output_dir, args.log_file,
         args.metadata_file, percentiles, args.debug)
