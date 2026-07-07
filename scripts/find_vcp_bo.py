#!/usr/bin/env python3
import os
import argparse
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import arcticdb as adb


DEFAULTS = {
    "pivot_left": 3,
    "pivot_right": 3,
    "min_contractions": 2,
    "max_contractions": 6,
    "first_min_pct": 10.0,
    "first_max_pct": 35.0,
    "final_min_pct": 3.0,
    "final_max_pct": 10.0,
    "tightening_ratio": 0.90,
    "min_base_bars": 15,
    "max_base_bars": 325,
    "min_breakout_vol_x": 1.40,
    "vdu_max_x": 1.00,
    "max_extended_pct": 12.0,
}


def safe_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan


def load_arctic_db(path):
    arctic = adb.Arctic(f"lmdb://{path}")
    if not arctic.has_library("prices"):
        raise RuntimeError(f"No 'prices' library found at {path}")
    lib = arctic.get_library("prices")
    return lib, lib.list_symbols()


def normalize_price_df(data):
    required = {"datetime", "open", "high", "low", "close", "volume"}
    if not required.issubset(set(data.columns)):
        return pd.DataFrame()

    df = pd.DataFrame({
        "Open": data["open"].values,
        "High": data["high"].values,
        "Low": data["low"].values,
        "Close": data["close"].values,
        "Volume": data["volume"].values,
    }, index=pd.to_datetime(data["datetime"], unit="s", errors="coerce"))

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    df = df[(df["High"] > 0) & (df["Low"] > 0) & (df["Close"] > 0) & (df["Volume"] > 0)]
    df = df.sort_index()
    return df


def find_pivots(df, left, right):
    highs = df["High"].values
    lows = df["Low"].values

    pivot_highs = []
    pivot_lows = []

    for i in range(left, len(df) - right):
        window_high = highs[i - left:i + right + 1]
        window_low = lows[i - left:i + right + 1]

        if highs[i] == np.nanmax(window_high):
            pivot_highs.append(i)

        if lows[i] == np.nanmin(window_low):
            pivot_lows.append(i)

    return pivot_highs, pivot_lows


def build_contractions(df, pivot_highs, pivot_lows):
    contractions = []

    for ph in pivot_highs:
        future_lows = [pl for pl in pivot_lows if pl > ph]
        if not future_lows:
            continue

        pl = future_lows[0]

        peak = safe_float(df.iloc[ph]["High"])
        trough = safe_float(df.iloc[pl]["Low"])

        if peak <= 0 or trough <= 0 or trough >= peak:
            continue

        depth = (peak - trough) / peak * 100.0

        contractions.append({
            "peak_idx": ph,
            "trough_idx": pl,
            "peak_date": df.index[ph],
            "trough_date": df.index[pl],
            "peak": peak,
            "trough": trough,
            "depth_pct": depth,
        })

    return contractions


def validate_vcp(df, contractions, cfg):
    if len(contractions) < cfg["min_contractions"]:
        return False, {}

    if len(contractions) > cfg["max_contractions"]:
        contractions = contractions[-cfg["max_contractions"]:]

    base_start = contractions[0]["peak_idx"]
    base_end = len(df) - 1
    base_len = base_end - base_start + 1

    if base_len < cfg["min_base_bars"] or base_len > cfg["max_base_bars"]:
        return False, {}

    depths = [c["depth_pct"] for c in contractions]
    first = depths[0]
    final = depths[-1]

    if not (cfg["first_min_pct"] <= first <= cfg["first_max_pct"]):
        return False, {}

    if not (cfg["final_min_pct"] <= final <= cfg["final_max_pct"]):
        return False, {}

    for i in range(1, len(depths)):
        if depths[i] > depths[i - 1] * cfg["tightening_ratio"]:
            return False, {}

    pivot = max(c["peak"] for c in contractions[-2:])

    return True, {
        "pivot": pivot,
        "base_len": base_len,
        "contraction_count": len(contractions),
        "depths": depths,
        "first_contraction_pct": first,
        "final_contraction_pct": final,
        "base_start": df.index[base_start],
        "base_end": df.index[base_end],
    }


def detect_breakout(df, vcp_info, cfg):
    close = safe_float(df["Close"].iloc[-1])
    volume = safe_float(df["Volume"].iloc[-1])
    pivot = safe_float(vcp_info["pivot"])

    if len(df) < 50 or close <= 0 or pivot <= 0:
        return False, {}

    avg_vol50 = safe_float(df["Volume"].tail(50).mean())
    if avg_vol50 <= 0:
        return False, {}

    vol_x = volume / avg_vol50
    extended_pct = (close - pivot) / pivot * 100.0

    last5_avg_vol = safe_float(df["Volume"].tail(5).mean())
    vdu_x = last5_avg_vol / avg_vol50 if avg_vol50 > 0 else np.nan

    is_bo = (
        close > pivot
        and vol_x >= cfg["min_breakout_vol_x"]
        and extended_pct <= cfg["max_extended_pct"]
        and vdu_x <= cfg["vdu_max_x"]
    )

    return is_bo, {
        "price": close,
        "volume": volume,
        "avg_vol50": avg_vol50,
        "vol_x_50d": vol_x,
        "extended_pct": extended_pct,
        "vdu_x": vdu_x,
    }


def scan_one_ticker(ticker, data, cfg):
    df = normalize_price_df(data)

    if df.empty or len(df) < 80:
        return None

    pivot_highs, pivot_lows = find_pivots(
        df,
        cfg["pivot_left"],
        cfg["pivot_right"],
    )

    contractions = build_contractions(df, pivot_highs, pivot_lows)
    is_vcp, vcp_info = validate_vcp(df, contractions, cfg)

    if not is_vcp:
        return None

    is_bo, bo_info = detect_breakout(df, vcp_info, cfg)

    return {
        "Ticker": ticker,
        "Date": df.index[-1].date(),
        "Price": round(bo_info.get("price", df["Close"].iloc[-1]), 2),
        "Volume": int(bo_info.get("volume", df["Volume"].iloc[-1])),
        "AvgVol50": round(bo_info.get("avg_vol50", np.nan), 0),
        "Vol_x_50d": round(bo_info.get("vol_x_50d", np.nan), 2),
        "Pivot": round(vcp_info["pivot"], 2),
        "Extended_%": round(bo_info.get("extended_pct", np.nan), 2),
        "VCP": "YES",
        "BO": "YES" if is_bo else "NO",
        "Contractions": vcp_info["contraction_count"],
        "Contraction_Depths": " / ".join(f"{x:.1f}%" for x in vcp_info["depths"]),
        "First_Contraction_%": round(vcp_info["first_contraction_pct"], 2),
        "Final_Contraction_%": round(vcp_info["final_contraction_pct"], 2),
        "Base_Length": vcp_info["base_len"],
        "Base_Start": vcp_info["base_start"].date(),
        "Base_End": vcp_info["base_end"].date(),
        "VDU_x": round(bo_info.get("vdu_x", np.nan), 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Find VCP and breakout stocks from ArcticDB daily data")
    parser.add_argument("--arctic-db-path", default="tmp/arctic_db")
    parser.add_argument("--input-csv", default="RS_Data/rs_stocks.csv")
    parser.add_argument("--output-dir", default="RS_Data")
    parser.add_argument("--log-file", default="logs/failed_vcp_tickers.log")
    parser.add_argument("--date", default=datetime.now().strftime("%m%d%Y"))
    parser.add_argument("--rs-threshold", type=float, default=80.0)
    parser.add_argument("--min-price", type=float, default=30.0)
    parser.add_argument("--only-stocks", action="store_true", default=True)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    if not os.path.exists(args.input_csv):
        raise FileNotFoundError(f"Input CSV not found: {args.input_csv}")

    rs_df = pd.read_csv(args.input_csv)

    required = {"Ticker", "Price", "RS Percentile"}
    missing = required - set(rs_df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns from {args.input_csv}: {missing}")

    scan_df = rs_df.copy()

    scan_df["Price"] = pd.to_numeric(scan_df["Price"], errors="coerce")
    scan_df["RS Percentile"] = pd.to_numeric(scan_df["RS Percentile"], errors="coerce")

    scan_df = scan_df[
        (scan_df["Price"] >= args.min_price)
        & (scan_df["RS Percentile"] >= args.rs_threshold)
    ].copy()

    if "Type" in scan_df.columns:
        scan_df = scan_df[scan_df["Type"].astype(str).str.upper().ne("ETF")].copy()
    elif "Sector" in scan_df.columns:
        scan_df = scan_df[scan_df["Sector"].astype(str).str.upper().ne("ETF")].copy()

    lib, symbols = load_arctic_db(args.arctic_db_path)
    symbol_set = set(symbols)

    results = []

    print("=== VCP + BO SCANNER START ===")
    print(f"Candidates after RS/Price filter: {len(scan_df):,}")

    for _, meta in scan_df.iterrows():
        ticker = str(meta["Ticker"]).strip()

        if ticker not in symbol_set:
            logging.info(f"{ticker}: not found in ArcticDB")
            continue

        try:
            data = lib.read(ticker).data
            row = scan_one_ticker(ticker, data, DEFAULTS)

            if row is None:
                continue

            for col in [
                "Rank", "Price", "DVol", "Sector", "Industry",
                "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
                "ATR", "ADR", "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP", "IPO",
                "SMA50", "SMA200", "SMA10W", "SMA30W",
                "History_Days", "Gap (%)", "Latest Volume", "9M+ Volume", "HVE", "HVE Date", "HVE Volume"
            ]:
                if col in meta.index and col not in row:
                    row[col] = meta[col]

            results.append(row)

        except Exception as e:
            logging.info(f"{ticker}: exception: {e}")

    out = pd.DataFrame(results)

    vcp_path = os.path.join(args.output_dir, "VCP_Stocks.csv")
    bo_path = os.path.join(args.output_dir, "VCP_BO_Stocks.csv")

    preferred_cols = [
        "Ticker", "Date", "Price", "Volume", "AvgVol50", "Vol_x_50d",
        "Pivot", "Extended_%", "VCP", "BO",
        "Contractions", "Contraction_Depths",
        "First_Contraction_%", "Final_Contraction_%",
        "Base_Length", "Base_Start", "Base_End", "VDU_x",
        "Rank", "Sector", "Industry",
        "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
        "ATR", "ADR", "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP", "IPO",
        "SMA50", "SMA200", "SMA10W", "SMA30W",
        "History_Days", "Gap (%)", "Latest Volume", "9M+ Volume", "HVE", "HVE Date", "HVE Volume"
    ]

    if out.empty:
        print("No VCP candidates found. No VCP files created.")
        return

    out = out.sort_values(
        ["BO", "RS Percentile", "Vol_x_50d", "Extended_%"],
        ascending=[False, False, False, True],
        na_position="last",
    )

    available_cols = [c for c in preferred_cols if c in out.columns]
    out[available_cols].to_csv(vcp_path, index=False, na_rep="")

    bo = out[out["BO"].astype(str).str.upper().eq("YES")].copy()

    print("\n=== VCP + BO SCANNER COMPLETE ===")
    print(f"VCP candidates: {len(out):,}")
    print(f"Breakouts: {len(bo):,}")
    print(f"Saved: {vcp_path}")

    if not bo.empty:
        bo[available_cols].to_csv(bo_path, index=False, na_rep="")
        print(f"Saved: {bo_path}")
    else:
        print("No breakout candidates found. No VCP_BO_Stocks.csv created.")


if __name__ == "__main__":
    main()
