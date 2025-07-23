#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import json
import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from yahooquery import Ticker
import sys
import time
from tqdm.auto import tqdm

def quarters_perf(closes: pd.Series, n: int) -> float:
    """Calculate performance for the last n quarters (n*63 days)."""
    days = n * 63
    if len(closes) < 2:
        return np.nan
    available_data = closes[-min(len(closes), days):]
    pct_change = available_data.pct_change().dropna()
    return (pct_change + 1).cumprod()[-1] - 1 if not pct_change.empty else np.nan

def strength(closes: pd.Series) -> float:
    """Calculate weighted performance over 4 quarters."""
    perfs = [quarters_perf(closes, i) for i in range(1, 5)]
    valid_perfs = [p for p in perfs if not np.isnan(p)]
    if not valid_perfs:
        return np.nan
    weights = [0.4, 0.2, 0.2, 0.2][:len(valid_perfs)]
    total_weight = sum(weights)
    weights = [w / total_weight for w in weights] if total_weight > 0 else weights
    return sum(w * p for w, p in zip(weights, valid_perfs))

def relative_strength(closes: pd.Series, closes_ref: pd.Series) -> float:
    """Calculate RS relative to reference ticker."""
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    if np.isnan(rs_stock) or np.isnan(rs_ref):
        return np.nan
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    return round(rs, 2) if rs <= 590 else np.nan

def fetch_historical_data(tickers, output_file, log_file):
    """Fetch 2 years of historical data with retries."""
    max_retries = 3
    batch_size = 100
    history = {}
    failed_tickers = []
    
    # Calculate total batches for progress bar
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    logging.info(f"Processing {len(tickers)} tickers in {total_batches} batches of {batch_size}")

    for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Processing batches"):
        batch = tickers[i:i + batch_size]
        for attempt in range(max_retries):
            try:
                data = Ticker(batch).history(period="2y")
                for ticker in batch:
                    if ticker in data.index.get_level_values(0):
                        df = data.loc[ticker]
                        history[ticker] = [
                            {"close": row["close"], "datetime": int(row.name.timestamp())}
                            for _, row in df.iterrows()
                        ]
                    else:
                        failed_tickers.append((ticker, f"No data on attempt {attempt + 1}"))
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    failed_tickers.extend((t, str(e)) for t in batch)
                else:
                    time.sleep(5)

    with open(output_file, "w") as f:
        json.dump(history, f)
    if failed_tickers:
        with open(log_file, "a") as f:
            for ticker, error in failed_tickers:
                f.write(f"{ticker}: {error}\n")
    return history

def main(input_file, min_percentile, reference_ticker, output_dir, log_file):
    """Calculate RS and generate CSVs."""
    # Setup logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    # Load ticker_price.json
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} not found")
        sys.exit(1)
    with open(input_file, "r") as f:
        data = json.load(f)
    
    # Extract metadata
    metadata = [
        {
            "Ticker": t,
            "Price": data[t]["info"]["Price"],
            "Sector": data[t]["info"]["sector"],
            "Industry": data[t]["info"]["industry"],
            "Type": data[t]["info"]["type"]
        }
        for t in data
    ]
    metadata_df = pd.DataFrame(metadata)
    
    # Fetch historical data
    os.makedirs("tmp", exist_ok=True)
    history_file = "tmp/ticker_history.json"
    tickers = list(data.keys())
    if reference_ticker not in tickers:
        logging.error(f"Reference ticker {reference_ticker} not found in {input_file}")
        sys.exit(1)
    history = fetch_historical_data(tickers, history_file, log_file)
    
    # Validate reference ticker data
    if reference_ticker not in history or len(history[reference_ticker]) < 20:
        logging.error(f"Reference ticker {reference_ticker} has insufficient data ({len(history.get(reference_ticker, []))} days)")
        sys.exit(1)
    
    # Calculate RS
    rs_results = []
    ref_closes = pd.Series(
        [c["close"] for c in history.get(reference_ticker, [])],
        index=[c["datetime"] for c in history.get(reference_ticker, [])]
    )
    for ticker in tickers:
        closes = pd.Series(
            [c["close"] for c in history.get(ticker, [])],
            index=[c["datetime"] for c in history.get(ticker, [])]
        )
        if len(closes) < 2:
            logging.info(f"{ticker}: Skipped, insufficient data ({len(closes)} days)")
            continue
        rs = relative_strength(closes, ref_closes)
        rs_1m = relative_strength(closes[:-20], ref_closes[:-20]) if len(closes) > 20 else rs
        rs_3m = relative_strength(closes[:-60], ref_closes[:-60]) if len(closes) > 60 else rs
        rs_6m = relative_strength(closes[:-120], ref_closes[:-120]) if len(closes) > 120 else rs
        rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m))
    
    # Create df_stocks
    df_stocks = pd.DataFrame(rs_results, columns=["Ticker", "Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"])
    df_stocks = df_stocks.merge(metadata_df, on="Ticker")
    df_stocks = df_stocks.dropna(subset=["Relative Strength"])
    if df_stocks.empty:
        logging.warning("No tickers with valid RS data after filtering")
        df_stocks.to_csv(os.path.join(output_dir, "rs_stocks.csv"), index=False)
        pd.DataFrame(columns=["Rank", "Industry", "Sector", "Relative Strength", "Percentile", 
                              "1 Month Ago", "3 Months Ago", "6 Months Ago", "Tickers", "Price"]).to_csv(
            os.path.join(output_dir, "rs_industries.csv"), index=False
        )
        pd.DataFrame(columns=["symbol", "rsrating", "time"]).to_csv(
            os.path.join(output_dir, "RSRATING.csv"), index=False
        )
        os.remove(history_file)
        os.rmdir("tmp")
        return
    
    # Compute percentiles
    for col in ["Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"]:
        df_stocks[f"{col} Percentile"] = pd.qcut(df_stocks[col], 100, labels=False, duplicates="drop")
    df_stocks = df_stocks[df_stocks["Relative Strength Percentile"] >= min_percentile]
    
    # Rank and finalize
    df_stocks = df_stocks.sort_values("Relative Strength", ascending=False).reset_index(drop=True)
    df_stocks["Rank"] = df_stocks.index + 1
    df_stocks = df_stocks[["Rank", "Ticker", "Price", "Sector", "Industry", "Relative Strength", 
                           "Relative Strength Percentile", "1 Month Ago Percentile", 
                           "3 Months Ago Percentile", "6 Months Ago Percentile"]]
    df_stocks.columns = ["Rank", "Ticker", "Price", "Sector", "Industry", "Relative Strength", 
                         "Percentile", "1 Month Ago", "3 Months Ago", "6 Months Ago"]
    df_stocks.to_csv(os.path.join(output_dir, "rs_stocks.csv"), index=False)
    
    # Adjust ETF metadata
    df_stocks.loc[df_stocks["Type"] == "ETF", "Industry"] = "ETF"
    df_stocks.loc[df_stocks["Type"] == "ETF", "Sector"] = "ETF"
    
    # Create df_industries
    df_industries = df_stocks.groupby("Industry").agg({
        "Relative Strength": "mean",
        "1 Month Ago": "mean",
        "3 Months Ago": "mean",
        "6 Months Ago": "mean",
        "Price": "mean",
        "Sector": "first",
        "Ticker": lambda x: ",".join(x.sort_values(by=df_stocks.loc[x.index, "Relative Strength"], ascending=False))
    }).reset_index()
    df_industries = df_industries[df_industries["Ticker"].str.split(",").str.len() > 1]
    
    # Compute industry percentiles
    for col in ["Relative Strength", "1 Month Ago", "3 Months Ago", "6 Months Ago"]:
        df_industries[f"{col} Percentile"] = pd.qcut(df_industries[col], 100, labels=False, duplicates="drop")
    df_industries = df_industries.sort_values("Relative Strength", ascending=False).reset_index(drop=True)
    df_industries["Rank"] = df_industries.index + 1
    df_industries = df_industries[["Rank", "Industry", "Sector", "Relative Strength", 
                                  "Relative Strength Percentile", "1 Month Ago Percentile", 
                                  "3 Months Ago Percentile", "6 Months Ago Percentile", "Tickers", "Price"]]
    df_industries.columns = ["Rank", "Industry", "Sector", "Relative Strength", 
                            "Percentile", "1 Month Ago", "3 Months Ago", "6 Months Ago", "Tickers", "Price"]
    df_industries.to_csv(os.path.join(output_dir, "rs_industries.csv"), index=False)
    
    # Generate RSRATING.csv
    latest_date = max([max([c["datetime"] for c in history[t]]) for t in history])
    latest_date = datetime.fromtimestamp(latest_date).strftime("%Y-%m-%d")
    dates = [
        latest_date,
        (datetime.fromtimestamp(latest_date) - timedelta(days=20)).strftime("%Y-%m-%d"),
        (datetime.fromtimestamp(latest_date) - timedelta(days=60)).strftime("%Y-%m-%d"),
        (datetime.fromtimestamp(latest_date) - timedelta(days=120)).strftime("%Y-%m-%d")
    ]
    rsrating_rows = []
    for _, row in df_stocks.iterrows():
        for period, date in zip(["Percentile", "1 Month Ago", "3 Months Ago", "6 Months Ago"], dates):
            if not pd.isna(row[period]):
                rsrating_rows.append((row["Ticker"], row[period], date))
    pd.DataFrame(rsrating_rows, columns=["symbol", "rsrating", "time"]).to_csv(
        os.path.join(output_dir, "RSRATING.csv"), index=False
    )
    
    # Clean up
    os.remove(history_file)
    os.rmdir("tmp")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Relative Strength and generate CSVs")
    parser.add_argument("input_file", help="Path to ticker_price.json")
    parser.add_argument("--min-percentile", type=int, default=85, help="Minimum percentile for filtering")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker for RS")
    parser.add_argument("--output-dir", default="data", help="Output directory for CSVs")
    parser.add_argument("--log-file", default="logs/failed_tickers.log", help="Log file for errors")
    args = parser.parse_args()
    main(args.input_file, args.min_percentile, args.reference_ticker, args.output_dir, args.log_file)
