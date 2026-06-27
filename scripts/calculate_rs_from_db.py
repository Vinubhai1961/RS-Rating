#!/usr/bin/env python3
import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytz
import arcticdb as adb
from tqdm.auto import tqdm

try:
    from pandas_market_calendars import get_calendar
except ImportError:
    get_calendar = None
    logging.warning("pandas_market_calendars not installed. Falling back to consecutive days for RSRATING.csv.")


# ====================== Unified Missing RS Logger ======================
def log_missing_rs(ticker: str, message: str, log_path: str):
    """Append a debug line to the single Missing_RS.log file"""
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ticker}] {message}\n")


# ====================== ENHANCED RS DEBUG ======================
def align_series(closes, closes_ref):
    return pd.DataFrame({"stock": closes, "ref": closes_ref}).dropna().sort_index()


def debug_alignment(ticker, closes, closes_ref, df, log_path):
    log_missing_rs(ticker, f"ALIGNMENT → stock={len(closes)}, ref={len(closes_ref)}, aligned={len(df)}", log_path)
    if len(df) < min(len(closes), len(closes_ref)) * 0.9:
        log_missing_rs(ticker, "⚠️ ALIGNMENT WARNING: Significant date mismatch!", log_path)
    if len(df) > 5:
        tail_dates = [d.strftime("%Y-%m-%d") for d in df.index[-5:]]
        log_missing_rs(ticker, f"Last aligned dates: {tail_dates}", log_path)


def debug_returns(ticker, df, days, label, log_path, ref_ticker="SPY"):
    if len(df) < days + 1:
        log_missing_rs(ticker, f"{label} → INSUFFICIENT DATA", log_path)
        return None, None
    old_date = df.index[-days - 1]
    new_date = df.index[-1]
    s_old = df["stock"].iloc[-days - 1]
    s_new = df["stock"].iloc[-1]
    r_old = df["ref"].iloc[-days - 1]
    r_new = df["ref"].iloc[-1]
    s_ret = s_new / s_old - 1
    r_ret = r_new / r_old - 1
    log_missing_rs(
        ticker,
        f"{label:>2} → {old_date.date()} → {new_date.date()} | "
        f"Stock: {s_old:.2f} → {s_new:.2f} ({s_ret:+6.2%}) | "
        f"{ref_ticker}: {r_old:.2f} → {r_new:.2f} ({r_ret:+6.2%})",
        log_path
    )
    return s_ret, r_ret


def validate_rs(ticker, rs, s_ret, r_ret, label, log_path):
    if s_ret is None or r_ret is None or pd.isna(rs):
        return
    if s_ret > r_ret and rs < 100:
        log_missing_rs(ticker, f"⚠️ {label} INCONSISTENT: Stock > Ref but RS < 100", log_path)
    if s_ret < r_ret and rs > 100:
        log_missing_rs(ticker, f"⚠️ {label} INCONSISTENT: Stock < Ref but RS > 100", log_path)
    if abs(s_ret) > 2 or abs(r_ret) > 2:
        log_missing_rs(ticker, f"⚠️ {label} EXTREME MOVE (>200%) — possible bad data", log_path)


def debug_trend(ticker, rs_1m, rs_3m, rs_6m, log_path):
    if pd.notna(rs_1m) and pd.notna(rs_3m) and pd.notna(rs_6m):
        if rs_1m > rs_3m > rs_6m:
            log_missing_rs(ticker, "Trend: Accelerating 🚀", log_path)
        elif rs_1m < rs_3m < rs_6m:
            log_missing_rs(ticker, "Trend: Decelerating 📉", log_path)
        else:
            log_missing_rs(ticker, "Trend: Mixed", log_path)


def quarters_perf(closes: pd.Series, n: int) -> float:
    days = n * 63
    slice_len = min(len(closes), days + 1)
    available_data = closes[-slice_len:]
    if len(available_data) < 2:
        return 0.0 if len(available_data) == 1 else np.nan
    pct_change = available_data.pct_change(fill_method=None).dropna()
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
    return round(rs, 2) if rs <= 700 else 700.0


def short_relative_strength(closes: pd.Series, closes_ref: pd.Series, days: int) -> float:
    if len(closes) < days + 5 or len(closes_ref) < days + 5:
        return np.nan
    df = pd.DataFrame({"stock": closes, "ref": closes_ref}).dropna().sort_index()
    if len(df) < days + 1:
        return np.nan
    price_old = df["stock"].iloc[-days - 1]
    price_new = df["stock"].iloc[-1]
    ref_old = df["ref"].iloc[-days - 1]
    ref_new = df["ref"].iloc[-1]
    if price_new <= 0 or ref_new <= 0 or price_old <= 0 or ref_old <= 0:
        return np.nan
    if pd.isna(price_old) or pd.isna(price_new) or pd.isna(ref_old) or pd.isna(ref_new):
        return np.nan
    stock_ret = price_new / price_old - 1
    ref_ret = ref_new / ref_old - 1
    if abs(ref_ret) < 0.0001:
        return np.nan if stock_ret <= 0 else 999.0
    rs = (1 + stock_ret) / (1 + ref_ret) * 100
    return round(rs, 2) if rs <= 700 else 700.0


def calculate_smas(closes: pd.Series):
    sma50 = sma200 = sma10w = sma30w = np.nan
    closes = closes.dropna().sort_index()
    if len(closes) >= 50:
        sma50 = round(closes.rolling(window=50).mean().iloc[-1], 2)
    if len(closes) >= 200:
        sma200 = round(closes.rolling(window=200).mean().iloc[-1], 2)
    if len(closes) >= 30:
        weekly_closes = closes.resample('W').last().dropna()
        if len(weekly_closes) >= 10:
            sma10w = round(weekly_closes.rolling(window=10).mean().iloc[-1], 2)
        if len(weekly_closes) >= 30:
            sma30w = round(weekly_closes.rolling(window=30).mean().iloc[-1], 2)
    return sma50, sma200, sma10w, sma30w


def calculate_atr_adr_from_dataframe(data: pd.DataFrame, ticker: str, period: int = 14):
    try:
        required_cols = {"high", "low", "close", "datetime"}
        if not required_cols.issubset(set(data.columns)):
            return np.nan, np.nan
        df = pd.DataFrame({
            "high": data["high"].values,
            "low": data["low"].values,
            "close": data["close"].values,
        }, index=pd.to_datetime(data["datetime"], unit="s")).sort_index()
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["high", "low", "close"])
        df = df[(df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)]
        if len(df) < period + 1:
            return np.nan, np.nan
        tr0 = df["high"] - df["low"]
        tr1 = (df["high"] - df["close"].shift(1)).abs()
        tr2 = (df["low"] - df["close"].shift(1)).abs()
        tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]
        adr = (df["high"] - df["low"]).rolling(window=period).mean().iloc[-1]
        return round(float(atr), 4), round(float(adr), 4)
    except Exception as e:
        logging.warning(f"ATR/ADR calculation failed for {ticker}: {e}")
        return np.nan, np.nan


def safe_float(value):
    if value is None or value == "":
        return np.nan
    try:
        if isinstance(value, str):
            value = value.strip()
            if value.endswith("K"): return float(value[:-1]) * 1_000
            if value.endswith("M"): return float(value[:-1]) * 1_000_000
            if value.endswith("B"): return float(value[:-1]) * 1_000_000_000
        return float(value)
    except Exception:
        return np.nan


def load_metadata(metadata_file):
    metadata_df = pd.DataFrame()
    if not metadata_file or not os.path.exists(metadata_file):
        logging.warning(f"Metadata file not found or not provided: {metadata_file}")
        return metadata_df
    try:
        with open(metadata_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        records = []
        if isinstance(data, list):
            iterator = [(item.get("ticker"), item.get("info", {}) or {}) for item in data]
        elif isinstance(data, dict):
            iterator = [(ticker, (payload or {}).get("info", {}) or {}) for ticker, payload in data.items()]
        else:
            raise ValueError(f"Unsupported metadata format: {type(data).__name__}")
        for ticker, info in iterator:
            if not ticker: continue
            records.append({
                "Ticker": ticker,
                "Price": round(safe_float(info.get("Price")), 2) if pd.notna(safe_float(info.get("Price"))) else np.nan,
                "DVol": info.get("DVol", ""),
                "Sector": info.get("sector", info.get("Sector", "")),
                "Industry": info.get("industry", info.get("Industry", "")),
                "AvgVol": info.get("AvgVol", ""),
                "AvgVol10": info.get("AvgVol10", ""),
                "52WKH": info.get("52WKH"),
                "52WKL": info.get("52WKL"),
                "MCAP": info.get("MCAP"),
                "Type": info.get("type", info.get("Type", "Stock")),
                "Earning_Date": info.get("Earning_Date")
            })
        metadata_df = pd.DataFrame(records)
        if metadata_df.empty or "Ticker" not in metadata_df.columns:
            return pd.DataFrame()
        metadata_df = metadata_df.drop_duplicates(subset=["Ticker"], keep="first")
        logging.info(f"Metadata loaded: {len(metadata_df):,} tickers")
        return metadata_df
    except Exception as e:
        logging.error(f"Invalid metadata file {metadata_file}: {str(e)}")
        return pd.DataFrame()


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
        return None, None


def generate_tradingview_csv(df_stocks, output_dir, ref_data, percentile_values=None, use_trading_days=True):
    if percentile_values is None:
        percentile_values = [98, 89, 69, 49, 29, 9, 1]
    latest_ts = ref_data["datetime"].max()
    latest_date = datetime.fromtimestamp(latest_ts).date()
    logging.info(f"Latest market date (NYSE): {latest_date}")
    dates = []
    if use_trading_days and get_calendar:
        try:
            cal = get_calendar('NYSE')
            sched = cal.schedule(start_date=latest_date - timedelta(days=20),
                               end_date=latest_date + timedelta(days=2))
            valid_dates = [d.date() for d in sched.index if d.date() <= latest_date]
            dates = [d.strftime('%Y%m%dT') for d in valid_dates[-5:]]
        except Exception as e:
            logging.warning(f"NYSE calendar failed: {e}")
    if len(dates) < 5:
        dates = [(latest_date - timedelta(days=i)).strftime('%Y%m%dT') for i in range(4, -1, -1)]
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
    lines = []
    for p in sorted(percentile_values, reverse=True):
        rs_val = rs_map[p]
        for d in dates:
            lines.append(f"{d},0,1000,0,{rs_val},0\n")
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "RSRATING.csv")
    with open(path, "w") as f:
        f.write(''.join(lines))
    logging.info(f"RSRATING.csv generated → {path}")
    return ''.join(lines)


def build_rs_threshold_map(df_stocks, column, percentile_values):
    valid_rs = df_stocks[column].dropna().sort_values(ascending=False).reset_index(drop=True)
    total = len(valid_rs)
    rs_map = {}
    for p in percentile_values:
        if total == 0:
            rs_map[p] = 100.0
            continue
        top_n = max(1, round(total * (100 - p) / 100.0))
        threshold_rs = valid_rs.iloc[min(top_n - 1, total - 1)]
        rs_map[p] = round(float(threshold_rs), 2)
    return rs_map


def generate_pine_thresholds(df_stocks, output_dir, percentile_values):
    threshold_sets = {"usa": "RS", "usa1m": "1M_RS", "usa3m": "3M_RS", "usa6m": "6M_RS"}
    lines = ["// Auto-generated RS Rating thresholds - do not edit manually\n"]
    lines.append(f"// Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n")
    for prefix, col in threshold_sets.items():
        rs_map = build_rs_threshold_map(df_stocks, col, percentile_values)
        lines.append(f"// {col} thresholds\n")
        for p in sorted(percentile_values, reverse=True):
            label = f"{prefix}{p:02d}"
            lines.append(f'{label} = input.float({rs_map[p]:.2f}, "{prefix.upper()} {p}th → RS ≥", group="{prefix.upper()} Thresholds")\n')
        lines.append("\n")
    path = os.path.join(output_dir, "RS-Rating-pine.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("".join(lines))
    print(f"RS-Rating-pine.csv generated → {path}")


def main(arctic_db_path, reference_ticker, output_dir, log_file, metadata_file=None, percentiles=None, debug=False):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
    logging.info("Starting RS calculation process")

    debug_rs_dir = os.path.join(os.path.dirname(log_file), "debug_rs")
    os.makedirs(debug_rs_dir, exist_ok=True)
    missing_rs_log = os.path.join(debug_rs_dir, "Missing_RS.log")
    open(missing_rs_log, "w", encoding="utf-8").close()

    result = load_arctic_db(arctic_db_path)
    if not result:
        logging.error("Failed to load ArcticDB.")
        sys.exit(1)

    lib, tickers = result
    if reference_ticker not in tickers:
        logging.error(f"Reference ticker {reference_ticker} not found")
        sys.exit(1)

    ref_data = lib.read(reference_ticker).data
    ref_closes = pd.Series(ref_data["close"].values, index=pd.to_datetime(ref_data["datetime"], unit='s')).sort_index()

    metadata_df = load_metadata(metadata_file)

    rs_results = []
    valid_rs_count = 0
    quality_stats = {
        "total_processed": 0, "valid_rs": 0, "missing_rs": 0,
        "missing_1m_rs": 0, "missing_3m_rs": 0, "missing_6m_rs": 0,
        "missing_atr": 0, "missing_adr": 0, "missing_sma50": 0,
        "missing_sma200": 0, "short_history_lt_252": 0, "ipo_lt_50_days": 0, "exceptions": 0,
    }

    for ticker in tqdm(tickers, desc="Calculating RS"):
        if ticker == reference_ticker:
            continue
        try:
            data = lib.read(ticker).data
            closes = pd.Series(data["close"].values, index=pd.to_datetime(data["datetime"], unit='s')).sort_index()

            quality_stats["total_processed"] += 1
            if len(closes) < 252:
                quality_stats["short_history_lt_252"] += 1

            log_missing_rs(ticker, f"=== Debug for {ticker} ===", missing_rs_log)
            if len(closes) > 0:
                log_missing_rs(ticker, f"Rows: {len(closes)} | Start={closes.index[0].date()} | End={closes.index[-1].date()}", missing_rs_log)
            log_missing_rs(ticker, f"Has_1M={len(closes)>=22}, Has_3M={len(closes)>=64}, Has_6M={len(closes)>=127}, Has_12M={len(closes)>=253}", missing_rs_log)

            if len(closes) < 2:
                log_missing_rs(ticker, "NOT ENOUGH DATA (<2 rows)", missing_rs_log)
                rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, len(closes), None, np.nan, np.nan))
                continue

            sma50, sma200, sma10w, sma30w = calculate_smas(closes)
            rs = relative_strength(closes, ref_closes)
            atr, adr = calculate_atr_adr_from_dataframe(data, ticker)

            df_aligned = align_series(closes, ref_closes)
            debug_alignment(ticker, closes, ref_closes, df_aligned, missing_rs_log)
            s1, r1 = debug_returns(ticker, df_aligned, 21, "1M", missing_rs_log, reference_ticker)
            s3, r3 = debug_returns(ticker, df_aligned, 63, "3M", missing_rs_log, reference_ticker)
            s6, r6 = debug_returns(ticker, df_aligned, 126, "6M", missing_rs_log, reference_ticker)

            rs_1m = short_relative_strength(closes, ref_closes, 21)
            rs_3m = short_relative_strength(closes, ref_closes, 63)
            rs_6m = short_relative_strength(closes, ref_closes, 126)

            validate_rs(ticker, rs_1m, s1, r1, "1M", missing_rs_log)
            validate_rs(ticker, rs_3m, s3, r3, "3M", missing_rs_log)
            validate_rs(ticker, rs_6m, s6, r6, "6M", missing_rs_log)
            debug_trend(ticker, rs_1m, rs_3m, rs_6m, missing_rs_log)

            log_missing_rs(ticker, f"FINAL → RS={rs}, 1M={rs_1m}, 3M={rs_3m}, 6M={rs_6m} | SMA50={sma50}, SMA200={sma200}, SMA10W={sma10w}, SMA30W={sma30w}, ATR={atr}, ADR={adr}", missing_rs_log)
            log_missing_rs(ticker, "-" * 60, missing_rs_log)

            earning_date = None
            if not metadata_df.empty:
                meta_row = metadata_df[metadata_df["Ticker"] == ticker]
                if not meta_row.empty:
                    earning_date = meta_row.iloc[0].get("Earning_Date")

            rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m, sma50, sma200, sma10w, sma30w, len(closes), earning_date, atr, adr))

            if not np.isnan(rs):
                valid_rs_count += 1
                quality_stats["valid_rs"] += 1
            else:
                quality_stats["missing_rs"] += 1
            if pd.isna(rs_1m): quality_stats["missing_1m_rs"] += 1
            if pd.isna(rs_3m): quality_stats["missing_3m_rs"] += 1
            if pd.isna(rs_6m): quality_stats["missing_6m_rs"] += 1
            if pd.isna(atr): quality_stats["missing_atr"] += 1
            if pd.isna(adr): quality_stats["missing_adr"] += 1
            if pd.isna(sma50): quality_stats["missing_sma50"] += 1
            if pd.isna(sma200): quality_stats["missing_sma200"] += 1

        except Exception as e:
            log_missing_rs(ticker, f"EXCEPTION: {e}", missing_rs_log)
            log_missing_rs(ticker, "-" * 60, missing_rs_log)
            quality_stats["exceptions"] += 1
            quality_stats["missing_rs"] += 1
            rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, None, np.nan, np.nan))

    df_stocks = pd.DataFrame(rs_results, columns=["Ticker", "RS", "1M_RS", "3M_RS", "6M_RS", "SMA50", "SMA200", "SMA10W", "SMA30W", "History_Days", "Earning_Date", "ATR", "ADR"])

    if not metadata_df.empty and "Ticker" in metadata_df.columns:
        df_stocks = df_stocks.merge(metadata_df, on="Ticker", how="left")

    if df_stocks.empty:
        print("No RS results calculated.")
        sys.exit(1)

    for col in ["RS", "1M_RS", "3M_RS", "6M_RS"]:
        valid_values = df_stocks[col].dropna()
        if not valid_values.empty:
            df_stocks.loc[valid_values.index, f"{col} Percentile"] = (valid_values.rank(pct=True, method='min') * 99).astype(int)
        else:
            df_stocks[f"{col} Percentile"] = np.nan

    df_stocks = df_stocks.sort_values("RS", ascending=False, na_position="last").reset_index(drop=True)
    df_stocks["Rank"] = df_stocks.index + 1

    # IPO Logic: mark symbols with fewer than 50 available trading rows as IPO.
    # Keep ETFs as ETF sector/industry, but do not mark ETFs as IPO.
    df_stocks["IPO"] = "No"
    if "History_Days" in df_stocks.columns:
        df_stocks.loc[df_stocks["History_Days"].lt(50), "IPO"] = "Yes"
        quality_stats["ipo_lt_50_days"] = int(df_stocks["IPO"].eq("Yes").sum())

    if "Type" in df_stocks.columns:
        etf_mask = df_stocks["Type"].astype(str).str.upper().eq("ETF")
        df_stocks.loc[etf_mask, ["Sector", "Industry"]] = "ETF"
        df_stocks.loc[etf_mask, "IPO"] = "No"

    # Ensure required fields exist before output/groupby, so USA behaves safely
    # even when metadata is missing or partially populated.
    for col in [
        "Sector",
        "Industry",
        "RS Percentile",
        "1M_RS Percentile",
        "3M_RS Percentile",
        "6M_RS Percentile",
    ]:
        if col not in df_stocks.columns:
            df_stocks[col] = np.nan

    df_stocks["Sector"] = df_stocks["Sector"].fillna("Unknown").replace("", "Unknown")
    df_stocks["Industry"] = df_stocks["Industry"].fillna("Unknown").replace("", "Unknown")

    final_columns = ["Rank", "Ticker", "Price", "DVol", "Sector", "Industry",
                     "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
                     "ATR", "ADR", "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP", "IPO",
                     "SMA50", "SMA200", "SMA10W", "SMA30W", "Earning_Date"]
    available_cols = [col for col in final_columns if col in df_stocks.columns]
    os.makedirs(output_dir, exist_ok=True)
    df_stocks[available_cols].to_csv(os.path.join(output_dir, "rs_stocks.csv"), index=False, na_rep="")

    # Industry Table (your original logic)
    df_industries = df_stocks.groupby("Industry", dropna=False).agg({
        "RS Percentile": "mean",
        "1M_RS Percentile": "mean",
        "3M_RS Percentile": "mean",
        "6M_RS Percentile": "mean",
        "Sector": "first",
        "Ticker": lambda x: ",".join(df_stocks[df_stocks["Ticker"].isin(x)].sort_values("RS", ascending=False)["Ticker"])
    }).reset_index()

    for col in ["RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile"]:
        df_industries[col] = df_industries[col].fillna(0).round().astype(int)
    df_industries = df_industries.sort_values("RS Percentile", ascending=False).reset_index(drop=True)
    df_industries["Rank"] = df_industries.index + 1
    df_industries.rename(columns={"RS Percentile": "RS", "1M_RS Percentile": "1 M_RS", "3M_RS Percentile": "3M_RS", "6M_RS Percentile": "6M_RS"}, inplace=True)
    df_industries[["Rank", "Industry", "Sector", "RS", "1 M_RS", "3M_RS", "6M_RS", "Ticker"]].to_csv(os.path.join(output_dir, "rs_industries.csv"), index=False)

    # Sector Table
    df_sectors = df_stocks.groupby("Sector", dropna=False).agg({
        "RS Percentile": "mean",
        "1M_RS Percentile": "mean",
        "3M_RS Percentile": "mean",
        "6M_RS Percentile": "mean",
        "Ticker": lambda x: ",".join(df_stocks[df_stocks["Ticker"].isin(x)].sort_values("RS", ascending=False)["Ticker"])
    }).reset_index()

    for col in ["RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile"]:
        df_sectors[col] = df_sectors[col].fillna(0).round().astype(int)
    df_sectors = df_sectors.sort_values("RS Percentile", ascending=False).reset_index(drop=True)
    df_sectors["Rank"] = df_sectors.index + 1
    df_sectors.rename(columns={"RS Percentile": "RS", "1M_RS Percentile": "1 M_RS", "3M_RS Percentile": "3M_RS", "6M_RS Percentile": "6M_RS"}, inplace=True)
    df_sectors[["Rank", "Sector", "RS", "1 M_RS", "3M_RS", "6M_RS", "Ticker"]].to_csv(os.path.join(output_dir, "rs_sectors.csv"), index=False)

    # Industry Leaders
    leader_source = df_stocks.copy()
    leader_source["Industry"] = leader_source["Industry"].fillna("Unknown")
    leader_source["Sector"] = leader_source["Sector"].fillna("Unknown")
    leader_source = leader_source.dropna(subset=["RS Percentile"])
    industry_leaders = leader_source.sort_values(["Industry", "RS Percentile", "3M_RS Percentile", "1M_RS Percentile"], ascending=[True, False, False, False]).groupby("Industry", group_keys=False).head(5).copy()
    industry_leaders["IndustryRank"] = industry_leaders.groupby("Industry").cumcount() + 1
    leader_columns = ["IndustryRank", "Ticker", "Price", "Sector", "Industry", "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile", "ATR", "ADR"]
    available_leader_cols = [col for col in leader_columns if col in industry_leaders.columns]
    industry_leaders[available_leader_cols].to_csv(os.path.join(output_dir, "industry_leaders.csv"), index=False, na_rep="")

    generate_tradingview_csv(df_stocks, output_dir, ref_data, percentiles)
    generate_pine_thresholds(df_stocks, output_dir, percentiles)

    print("\n=== USA RS CALCULATION COMPLETE ===")
    print(f"Valid RS: {valid_rs_count:,} / {len(df_stocks):,}")
    print(f"IPO (<50 trading days): {int(df_stocks['IPO'].eq('Yes').sum()):,}")
    print(f"Output directory: {output_dir}")
    print("Generated files: rs_stocks.csv, rs_industries.csv, rs_sectors.csv, industry_leaders.csv, RSRATING.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate RS from ArcticDB - Enhanced USA Version")
    parser.add_argument("--arctic-db-path", default="data/arctic_db/prices", help="Path to ArcticDB")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--log-file", default="logs/failed_logs.log", help="Log file")
    parser.add_argument("--metadata-file", default=None, help="Metadata JSON file")
    parser.add_argument("--percentiles", default="98,89,69,49,29,9,1", help="Percentiles")
    parser.add_argument("--debug", action="store_true", help="Enable debug")
    args = parser.parse_args()
    percentiles = [int(p) for p in args.percentiles.split(",")]
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    main(args.arctic_db_path, args.reference_ticker, args.output_dir, args.log_file, args.metadata_file, percentiles, args.debug)