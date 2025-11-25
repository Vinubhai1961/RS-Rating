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
    logging.warning("pandas_market_calendars not installed → falling back to business days.")


def calculate_rs_mansfield(closes: pd.Series, ref_closes: pd.Series) -> float:
    """
    Exact Mansfield Relative Strength Rating (as used in best TradingView scripts)
    Weights: 40% 3M, 20% 6M, 20% 9M, 20% 12M → based on price ratios
    """
    if len(closes) < 64 or len(ref_closes) < 64:
        return np.nan

    lengths = [63, 126, 189, 252]
    weights = [0.4, 0.2, 0.2, 0.2]

    def weighted_perf(series: pd.Series) -> float:
        total = 0.0
        series = series.sort_index()
        latest = series.iloc[-1]
        for length, weight in zip(lengths, weights):
            if len(series) > length:
                ratio = latest / series.iloc[-1 - length]
            else:
                ratio = latest / series.iloc[0]
            total += weight * ratio
        return total

    stock_perf = weighted_perf(closes)
    ref_perf = weighted_perf(ref_closes)

    if ref_perf <= 0 or np.isnan(stock_perf) or np.isnan(ref_perf):
        return np.nan

    rs = (stock_perf / ref_perf) * 100
    return round(rs, 2)


def short_relative_strength(closes: pd.Series, ref_closes: pd.Series, days: int) -> float:
    if len(closes) < days + 1 or len(ref_closes) < days + 1:
        return np.nan
    stock_ret = closes.iloc[-1] / closes.iloc[-days] - 1
    ref_ret = ref_closes.iloc[-1] / ref_closes.iloc[-days] - 1
    if ref_ret == -1:
        return np.nan
    rs = (1 + stock_ret) / (1 + ref_ret) * 100
    return round(rs, 2)


def load_arctic_db(data_dir):
    try:
        if not os.path.exists(data_dir):
            raise Exception(f"ArcticDB directory {data_dir} does not exist")
        arctic = adb.Arctic(f"lmdb://{data_dir}")
        if "prices" not in arctic.list_libraries():
            raise Exception("No 'prices' library found")
        lib = arctic.get_library("prices")
        symbols = lib.list_symbols()
        logging.info(f"Loaded {len(symbols)} symbols from {data_dir}")
        return lib, symbols
    except Exception as e:
        logging.error(f"ArcticDB error: {e}")
        print(f"ArcticDB load failed: {e}")
        return None, None


def get_last_trading_days(latest_date: datetime.date, n: int = 5):
    """Robust NYSE trading day fallback"""
    if get_calendar:
        try:
            cal = get_calendar('NYSE')
            sched = cal.schedule(start_date=latest_date - timedelta(days=30), end_date=latest_date)
            dates = [d.date() for d in sched.index[-n:]]
            return [d.strftime("%Y%m%dT") for d in dates]
        except:
            pass

    # Fallback: business days (excludes weekends)
    dates = pd.bdate_range(end=latest_date, periods=n + 10).date[-n:]
    return [d.strftime("%Y%m%dT") for d in dates]


def generate_tradingview_csv(df_stocks, output_dir, ref_data, percentile_values=None):
    if percentile_values is None:
        percentile_values = [98, 89, 69, 49, 29, 9, 1]

    latest_ts = ref_data["datetime"].max()
    latest_date = datetime.fromtimestamp(latest_ts).date()
    trading_days = get_last_trading_days(latest_date, 5)

    # Correct threshold: RS value at the bottom of top (100-p)%
    valid_rs = df_stocks["RS"].dropna().sort_values(ascending=False).reset_index(drop=True)
    total = len(valid_rs)

    thresholds = {}
    for p in percentile_values:
        if total == 0:
            thresholds[p] = 100.0
        else:
            rank = max(0, min(int(np.ceil(total * (100 - p) / 100.0)) - 1, total - 1))
            thresholds[p] = round(float(valid_rs.iloc[rank]), 2)

    # Generate RSRATING.csv
    lines = []
    for p in sorted(percentile_values, reverse=True):
        rs_val = thresholds[p]
        for d in trading_days:
            lines.append(f"{d},0,1000,0,{rs_val},0\n")

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "RSRATING.csv")
    with open(path, "w") as f:
        f.write("".join(lines))

    logging.info(f"RSRATING.csv generated → {path}")
    print("=== RSRATING.csv Thresholds (TradingView Ready) ===")
    for p in sorted(percentile_values, reverse=True):
        print(f"  {p:2}th percentile → RS ≥ {thresholds[p]:6.2f}")

    return thresholds


def main(arctic_db_path, reference_ticker, output_dir, log_file, metadata_file=None, percentiles=None, debug=False):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger().addHandler(console)

    logging.info("=== Starting Mansfield RS Calculation ===")
    print("Loading ArcticDB...")

    lib, tickers = load_arctic_db(arctic_db_path)
    if not lib:
        sys.exit(1)

    if reference_ticker not in tickers:
        print(f"Reference ticker {reference_ticker} not found!")
        sys.exit(1)

    ref_data = lib.read(reference_ticker).data
    ref_closes = pd.Series(ref_data["close"].values, index=pd.to_datetime(ref_data["datetime"], unit='s')).sort_index()

    if len(ref_closes) < 253:
        print(f"Reference ticker has only {len(ref_closes)} days. Need ~1 year.")
        sys.exit(1)

    # Load metadata
    metadata_df = pd.DataFrame()
    if metadata_file and os.path.exists(metadata_file):
        try:
            with open(metadata_file) as f:
                data = json.load(f)
            if isinstance(data, list):
                metadata_df = pd.DataFrame([{
                    "Ticker": item.get("ticker"),
                    "Price": round(item.get("info", {}).get("Price", np.nan), 2) if item.get("info", {}).get("Price") not in [None, ""] else np.nan,
                    "DVol": item.get("info", {}).get("DVol"),
                    "Sector": item.get("info", {}).get("sector"),
                    "Industry": item.get("info", {}).get("industry"),
                    "AvgVol": item.get("info", {}).get("AvgVol"),
                    "AvgVol10": item.get("info", {}).get("AvgVol10"),
                    "52WKH": item.get("info", {}).get("52WKH"),
                    "52WKL": item.get("info", {}).get("52WKL"),
                    "MCAP": item.get("info", {}).get("MCAP"),
                    "Type": item.get("info", {}).get("type", "Stock")
                } for item in data])
            elif isinstance(data, dict):
                metadata_df = pd.DataFrame([{
                    "Ticker": t,
                    "Price": round(data[t].get("info", {}).get("Price", np.nan), 2),
                    "Type": data[t].get("info", {}).get("type", "Stock"),
                    "Sector": data[t].get("info", {}).get("sector"),
                    "Industry": data[t].get("info", {}).get("industry"),
                    "MCAP": data[t].get("info", {}).get("MCAP"),
                } for t in data])
        except Exception as e:
            logging.warning(f"Metadata failed: {e}")

    print(f"Calculating RS for {len(tickers):,} tickers vs {reference_ticker}...")
    results = []

    for ticker in tqdm(tickers, desc="RS Calc"):
        if ticker == reference_ticker:
            continue
        try:
            data = lib.read(ticker).data
            closes = pd.Series(data["close"].values, index=pd.to_datetime(data["datetime"], unit='s')).sort_index()
            if len(closes) < 2:
                continue

            rs = calculate_rs_mansfield(closes, ref_closes)
            rs_1m = short_relative_strength(closes, ref_closes, 21)
            rs_3m = short_relative_strength(closes, ref_closes, 63)
            rs_6m = short_relative_strength(closes, ref_closes, 126)

            results.append((ticker, rs, rs_1m, rs_3m, rs_6m, len(closes)))
        except:
            continue

    df = pd.DataFrame(results, columns=["Ticker", "RS", "1M_RS", "3M_RS", "6M_RS", "Days"])
    if not metadata_df.empty:
        df = df.merge(metadata_df, on="Ticker", how="left")

    # Percentiles
    for col in ["RS", "1M_RS", "3M_RS", "6M_RS"]:
        valid = df[col].dropna()
        if not valid.empty:
            df[f"{col} Percentile"] = (valid.rank(pct=True) * 99).round().astype(int)
        else:
            df[f"{col} Percentile"] = np.nan

    df = df.sort_values("RS", ascending=False).reset_index(drop=True)
    df["Rank"] = df.index + 1

    # Save main files
    os.makedirs(output_dir, exist_ok=True)
    df[["Rank", "Ticker", "RS", "RS Percentile", "Price", "DVol", "Sector", "Industry", "MCAP", "Days"]].to_csv(
        os.path.join(output_dir, "rs_stocks.csv"), index=False, na_rep="")

    # Industries
    industry = df.groupby("Industry").agg({
        "RS Percentile": "mean",
        "Sector": "first",
        "Ticker": lambda x: ",".join(x)
    }).round(0).astype({"RS Percentile": int}).sort_values("RS Percentile", ascending=False)
    industry["Rank"] = range(1, len(industry) + 1)
    industry[["Rank", "Industry", "Sector", "RS Percentile"]].rename(columns={"RS Percentile": "RS"}).to_csv(
        os.path.join(output_dir, "rs_industries.csv"), index=False)

    # Generate RSRATING.csv
    thresholds = generate_tradingview_csv(df, output_dir, ref_data, percentiles)

    print(f"\nSUCCESS! {len(df):,} stocks processed")
    print(f"   → rs_stocks.csv")
    print(f"   → rs_industries.csv")
    print(f"   → RSRATING.csv (ready for TradingView)")

    if debug:
        print("\n=== DEBUG TOP 10 ===")
        for _, row in df.head(10).iterrows():
            print(f"{row['Rank']:3} | {row['Ticker']:6} | RS: {row['RS']:6.2f} | Perc: {row['RS Percentile']:2} | Days: {row['Days']}")

        # Save debug
        df.head(50).to_csv(os.path.join(output_dir, "debug_top50.csv"), index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mansfield RS Rating → TradingView Ready")
    parser.add_argument("--arctic-db-path", default="data/arctic_db/prices")
    parser.add_argument("--reference-ticker", default="SPY")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--log-file", default="logs/rs_calc.log")
    parser.add_argument("--metadata-file", default=None)
    parser.add_argument("--percentiles", default="98,89,69,49,29,9,1")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    percentiles = [int(x) for x in args.percentiles.split(",")]
    main(args.arctic_db_path, args.reference_ticker, args.output_dir,
         args.log_file, args.metadata_file, percentiles, args.debug)
