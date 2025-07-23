import json
import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import arcticdb as adb
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

def main(input_file, min_percentile, reference_ticker, output_dir, log_file):
    """Calculate RS from ArcticDB and generate CSVs."""
    # Setup logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    
    # Load ticker_price.json
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} not found")
        return
    
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
    
    # Load data from ArcticDB
    arctic = adb.Arctic("lmdb://tmp/arctic_db")
    if not arctic.has_library("prices"):
        logging.error("ArcticDB library 'prices' not found")
        return
    lib = arctic.get_library("prices")
    
    tickers = list(data.keys())
    logging.info(f"Calculating RS for {len(tickers)} tickers")
    logging.info(f"Estimated RS calculation time: ~{(len(tickers) * 0.01) // 60} minutes {int((len(tickers) * 0.01) % 60)} seconds")
    
    if reference_ticker not in tickers:
        logging.error(f"Reference ticker {reference_ticker} not found in {input_file}")
        return
    
    # Calculate RS
    rs_results = []
    ref_data = lib.read(reference_ticker).data
    ref_closes = pd.Series(
        ref_data["close"].values,
        index=ref_data["datetime"].values
    )
    if len(ref_closes) < 20:
        logging.error(f"Reference ticker {reference_ticker} has insufficient data ({len(ref_closes)} days)")
        return
    
    for ticker in tqdm(tickers, desc="Calculating RS"):
        if ticker == reference_ticker:
            continue
        try:
            data = lib.read(ticker).data
            closes = pd.Series(
                data["close"].values,
                index=data["datetime"].values
            )
            if len(closes) < 2:
                logging.info(f"{ticker}: Skipped, insufficient data ({len(closes)} days)")
                continue
            rs = relative_strength(closes, ref_closes)
            rs_1m = relative_strength(closes[:-20], ref_closes[:-20]) if len(closes) > 20 else rs
            rs_3m = relative_strength(closes[:-60], ref_closes[:-60]) if len(closes) > 60 else rs
            rs_6m = relative_strength(closes[:-120], ref_closes[:-120]) if len(closes) > 120 else rs
            rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m))
        except Exception as e:
            logging.info(f"{ticker}: Failed to process ({str(e)})")
    
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
    latest_date = datetime.fromtimestamp(ref_data["datetime"].max()).strftime("%Y-%m-%d")
    dates = [
        latest_date,
        (datetime.fromtimestamp(ref_data["datetime"].max()) - timedelta(days=20)).strftime("%Y-%m-%d"),
        (datetime.fromtimestamp(ref_data["datetime"].max()) - timedelta(days=60)).strftime("%Y-%m-%d"),
        (datetime.fromtimestamp(ref_data["datetime"].max()) - timedelta(days=120)).strftime("%Y-%m-%d")
    ]
    rsrating_rows = []
    for _, row in df_stocks.iterrows():
        for period, date in zip(["Percentile", "1 Month Ago", "3 Months Ago", "6 Months Ago"], dates):
            if not pd.isna(row[period]):
                rsrating_rows.append((row["Ticker"], row[period], date))
    pd.DataFrame(rsrating_rows, columns=["symbol", "rsrating", "time"]).to_csv(
        os.path.join(output_dir, "RSRATING.csv"), index=False
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Relative Strength from ArcticDB and generate CSVs")
    parser.add_argument("input_file", help="Path to ticker_price.json")
    parser.add_argument("--min-percentile", type=int, default=85, help="Minimum percentile for filtering")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker for RS")
    parser.add_argument("--output-dir", default="data", help="Output directory for CSVs")
    parser.add_argument("--log-file", default="logs/failed_tickers.log", help="Log file for errors")
    args = parser.parse_args()
    main(args.input_file, args.min_percentile, args.reference_ticker, args.output_dir, args.log_file)
