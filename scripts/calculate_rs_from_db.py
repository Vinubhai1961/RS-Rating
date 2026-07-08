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
    sma20 = sma50 = sma200 = sma10w = sma30w = np.nan
    closes = closes.dropna().sort_index()
    if len(closes) >= 20:
        sma20 = round(closes.rolling(window=20).mean().iloc[-1], 2)
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
    return sma20, sma50, sma200, sma10w, sma30w


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




def calculate_gap_pct_from_dataframe(data: pd.DataFrame) -> float:
    """
    Calculate latest bullish gap-up percentage.

    User definition:
      - Yesterday close = previous close
      - Today open gaps above yesterday close
      - Today close must finish above today open

    Gap (%) = (today open - previous close) / previous close * 100

    Returns 0.00 when the latest bar is not a bullish gap-up.
    Returns NaN only when open/close data is missing or invalid.
    """
    try:
        required_cols = {"datetime", "open", "close"}
        if not required_cols.issubset(set(data.columns)):
            return np.nan

        df = pd.DataFrame({
            "open": data["open"].values,
            "close": data["close"].values,
        }, index=pd.to_datetime(data["datetime"], unit="s", errors="coerce")).sort_index()

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["open", "close"])
        df = df[(df["open"] > 0) & (df["close"] > 0)]

        if len(df) < 2:
            return np.nan

        latest_open = float(df["open"].iloc[-1])
        latest_close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])

        if prev_close <= 0:
            return np.nan

        is_bullish_gap_up = latest_open > prev_close and latest_close > latest_open
        if not is_bullish_gap_up:
            return 0.00

        return round(((latest_open - prev_close) / prev_close) * 100.0, 2)
    except Exception:
        return np.nan


def calculate_9m_plus_volume_from_dataframe(data: pd.DataFrame, min_volume: int = 9_000_000):
    """
    Check whether the latest trading session volume is at least min_volume.

    Returns:
      - Latest Volume: latest available daily volume
      - 9M+ Volume: YES when latest volume >= min_volume, else NO

    Note: this is NOT 9-month volume. It is a daily liquidity/event flag
    for tickers trading 9 million+ shares today/latest bar.
    """
    try:
        required_cols = {"datetime", "volume"}
        if not required_cols.issubset(set(data.columns)):
            return np.nan, "NO"

        df = pd.DataFrame({
            "Date": pd.to_datetime(data["datetime"], unit="s", errors="coerce"),
            "volume": data["volume"].values,
        })

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["Date", "volume"])
        df = df[df["volume"] > 0].sort_values("Date")

        if df.empty:
            return np.nan, "NO"

        latest_volume = int(df["volume"].iloc[-1])
        vol9m_plus = "YES" if latest_volume >= int(min_volume) else "NO"
        return latest_volume, vol9m_plus
    except Exception:
        return np.nan, "NO"


def write_9m_plus_volume_output(df_stocks: pd.DataFrame, vol9m_output_dir: str):
    """Write daily 9M+ volume scan under 9M_Vol/9M_Vol_MMDDYYYY.csv."""
    os.makedirs(vol9m_output_dir, exist_ok=True)
    today = datetime.now().strftime("%m%d%Y")
    path = os.path.join(vol9m_output_dir, f"9M_Vol_{today}.csv")

    if "9M+ Volume" in df_stocks.columns:
        out = df_stocks[df_stocks["9M+ Volume"].astype(str).str.upper().eq("YES")].copy()
    else:
        out = df_stocks.iloc[0:0].copy()

    cols = [
        "Rank", "Ticker", "Type", "Price", "DVol", "Sector", "Industry",
        "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
        "Latest Volume", "9M+ Volume", "History_Days", "Gap (%)",
        "HVE", "HVE Date", "HVE Volume", "IPO", "ATR", "ADR",
        "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP",
        "SMA50", "SMA200", "SMA10W", "SMA30W"
    ]
    cols = [c for c in cols if c in out.columns]

    sort_cols = [c for c in ["RS Percentile", "Latest Volume"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")

    out[cols].to_csv(path, index=False, na_rep="")
    return path

def build_hve_record_from_dataframe(data: pd.DataFrame, ticker: str) -> dict:
    """
    Build Highest Volume Ever stats from the already-loaded ArcticDB dataframe.
    Output columns intentionally match the standalone HVE prototype plus:
    Type and HVE Age (Days) are added later in write_hve_outputs().
    """
    empty = {
        "Ticker": ticker,
        "Rows": np.nan,
        "Years": np.nan,
        "Start": "",
        "End": "",
        "Latest Volume": np.nan,
        "HVE": "NO",
        "HVE Date": "",
        "HVE Volume": np.nan,
        "HVE Age (Days)": np.nan,
    }

    try:
        required_cols = {"datetime", "volume"}
        if not required_cols.issubset(set(data.columns)):
            return empty

        df = pd.DataFrame({
            "Date": pd.to_datetime(data["datetime"], unit="s", errors="coerce"),
            "volume": data["volume"].values,
        })

        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["Date", "volume"])
        df = df[df["volume"] > 0].sort_values("Date")

        if len(df) < 2:
            return empty

        rows = int(len(df))
        start_ts = df["Date"].iloc[0]
        end_ts = df["Date"].iloc[-1]
        years = round((end_ts - start_ts).days / 365.25, 2)
        latest_volume = int(df["volume"].iloc[-1])

        highest_idx = df["volume"].idxmax()
        highest_row = df.loc[highest_idx]
        highest_volume = int(highest_row["volume"])
        highest_date = highest_row["Date"].date()

        hve_age_days = (end_ts.date() - highest_date).days

        return {
            "Ticker": ticker,
            "Rows": rows,
            "Years": years,
            "Start": start_ts.date(),
            "End": end_ts.date(),
            "Latest Volume": latest_volume,
            "HVE": "YES" if latest_volume == highest_volume else "NO",
            "HVE Date": highest_date,
            "HVE Volume": highest_volume,
            "HVE Age (Days)": hve_age_days,
        }
    except Exception:
        return empty


def write_hve_outputs(df_stocks: pd.DataFrame, hve_history: list, hve_output_dir: str, latest_market_date=None):
    """
    Writes:
      HVE/History_HVE.csv
      HVE/Stock_HVE_MMDDYYYY.csv
      HVE/ETF_HVE_MMDDYYYY.csv

    Stock/ETF HVE files keep the same column style as rs_stocks.csv plus HVE detail columns.
    """
    os.makedirs(hve_output_dir, exist_ok=True)

    history_cols = [
        "Ticker", "Type", "Rows", "Years", "Start", "End", "Latest Volume",
        "HVE", "HVE Date", "HVE Volume", "HVE Age (Days)"
    ]
    hve_df = pd.DataFrame(hve_history)

    if hve_df.empty:
        hve_df = pd.DataFrame(columns=history_cols)
    else:
        # Add Type from metadata/rs_stocks so History_HVE can split Stock vs ETF clearly.
        # Prefer the explicit Type column, and fall back to Sector == ETF when needed.
        type_source_cols = [c for c in ["Ticker", "Type", "Sector"] if c in df_stocks.columns]
        if "Ticker" in type_source_cols:
            type_map = df_stocks[type_source_cols].drop_duplicates(subset=["Ticker"])
            hve_df = hve_df.merge(type_map, on="Ticker", how="left")
        else:
            hve_df["Type"] = "Stock"

        if "Type" not in hve_df.columns:
            hve_df["Type"] = "Stock"
        if "Sector" not in hve_df.columns:
            hve_df["Sector"] = ""

        hve_type = hve_df["Type"].astype(str).str.strip().str.upper()
        hve_sector = hve_df["Sector"].astype(str).str.strip().str.upper()
        hve_df["Type"] = np.where((hve_type == "ETF") | (hve_sector == "ETF"), "ETF", "Stock")

        # Sector is only used as a fallback helper above; keep History_HVE output clean.
        if "Sector" in hve_df.columns and "Sector" not in history_cols:
            hve_df = hve_df.drop(columns=["Sector"])

        for col in history_cols:
            if col not in hve_df.columns:
                hve_df[col] = np.nan if col in ["Rows", "Years", "Latest Volume", "HVE Volume", "HVE Age (Days)"] else ""

        hve_df = hve_df[history_cols]
        hve_df = hve_df.sort_values(["HVE", "Rows"], ascending=[False, False])

    hve_df.to_csv(os.path.join(hve_output_dir, "History_HVE.csv"), index=False)

    today = datetime.now().strftime("%m%d%Y")
    hve_today = df_stocks[df_stocks.get("HVE", "NO").astype(str).str.upper().eq("YES")].copy()

    # Daily HVE files should only include symbols whose HVE happened on the
    # latest market/reference date. This prevents stale 06/25 bars from showing
    # inside a 06/26 HVE report when a ticker did not update to the latest day.
    if latest_market_date is not None and "HVE Date" in hve_today.columns:
        hve_dates = pd.to_datetime(hve_today["HVE Date"], errors="coerce").dt.date
        hve_today = hve_today[hve_dates.eq(latest_market_date)].copy()

    # Keep HVE daily files in the same column format as 9M_Vol output.
    base_cols = [
        "Rank", "Ticker", "Type", "Price", "DVol", "Sector", "Industry",
        "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
        "Latest Volume", "9M+ Volume", "History_Days", "Gap (%)",
        "HVE", "HVE Date", "HVE Volume", "IPO", "ATR", "ADR",
        "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP",
        "SMA50", "SMA200", "SMA10W", "SMA30W"
    ]
    base_cols = [c for c in base_cols if c in hve_today.columns]

    if "Type" in hve_today.columns:
        etf_mask = hve_today["Type"].astype(str).str.upper().eq("ETF")
    else:
        etf_mask = hve_today["Sector"].astype(str).str.upper().eq("ETF")

    stock_hve = hve_today[~etf_mask].copy()
    etf_hve = hve_today[etf_mask].copy()

    stock_path = os.path.join(hve_output_dir, f"Stock_HVE_{today}.csv")
    etf_path = os.path.join(hve_output_dir, f"ETF_HVE_{today}.csv")

    stock_hve[base_cols].to_csv(stock_path, index=False, na_rep="")
    etf_hve[base_cols].to_csv(etf_path, index=False, na_rep="")

    return hve_df, stock_hve, etf_hve, stock_path, etf_path

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


# ====================== SECTOR OPPORTUNITY HELPERS ======================
def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a numeric Series for col, preserving index and returning NaN if missing."""
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def add_visual_setup_columns(df_stocks: pd.DataFrame) -> pd.DataFrame:
    """
    Add chart-friendly opportunity columns without hard-filtering on volume or ADR.

    Philosophy:
      - Strong trend + RS + near-high are hard filters.
      - Volume and ADR are visual review fields, not exclusion rules.
      - Tightness is scored from ATR% only when ATR and price are available.
    """
    df = df_stocks.copy()

    # Prefer metadata Price, but fall back to the last close from ArcticDB.
    price = _numeric_series(df, "Price")
    last_close = _numeric_series(df, "Last_Close")
    df["Setup_Price"] = price.where(price.notna() & (price > 0), last_close)

    high_52w = _numeric_series(df, "52WKH")
    sma50 = _numeric_series(df, "SMA50")
    sma200 = _numeric_series(df, "SMA200")
    atr = _numeric_series(df, "ATR")

    df["Dist_52WH_%"] = np.where(
        (df["Setup_Price"] > 0) & (high_52w > 0),
        ((high_52w - df["Setup_Price"]) / high_52w * 100).clip(lower=0),
        np.nan,
    )
    df["ATR_%"] = np.where(
        (df["Setup_Price"] > 0) & (atr > 0),
        atr / df["Setup_Price"] * 100,
        np.nan,
    )

    df["Trend_OK"] = (
        (df["Setup_Price"] > sma50)
        & (sma50 > sma200)
        & sma50.notna()
        & sma200.notna()
    )
    df["Near_52WH_20pct"] = df["Dist_52WH_%"].le(20)

    def tightness_label(atr_pct):
        if pd.isna(atr_pct):
            return "Unknown"
        if atr_pct < 1.5:
            return "Very Tight"
        if atr_pct < 2.5:
            return "Tight"
        if atr_pct < 4.0:
            return "Normal"
        return "Loose"

    df["Tightness"] = df["ATR_%"].apply(tightness_label)

    def setup_type(row):
        price = row.get("Setup_Price")
        sma50_v = row.get("SMA50")
        dist = row.get("Dist_52WH_%")
        atr_pct = row.get("ATR_%")
        trend_ok = bool(row.get("Trend_OK"))

        if pd.isna(price) or pd.isna(dist):
            return "Needs Chart Review"
        if trend_ok and pd.notna(sma50_v) and price > sma50_v * 1.15 and dist <= 3:
            return "Extended Near High"
        if trend_ok and dist <= 1:
            return "Breakout Zone"
        if trend_ok and dist <= 5:
            return "Near Breakout"
        if trend_ok and dist <= 10 and pd.notna(atr_pct) and atr_pct <= 2.5:
            return "Tight Base"
        if pd.notna(sma50_v) and sma50_v > 0 and trend_ok and sma50_v <= price <= sma50_v * 1.05:
            return "Pullback to 50D"
        if trend_ok and dist <= 20:
            return "Stage 2 Watch"
        return "Needs Chart Review"

    df["Setup_Type"] = df.apply(setup_type, axis=1)

    # Score components, 0-100. No volume/ADR hard filter.
    rs_score = _numeric_series(df, "RS Percentile").clip(0, 99) / 99 * 100
    rs_1m_score = _numeric_series(df, "1M_RS Percentile").clip(0, 99) / 99 * 100
    rs_3m_score = _numeric_series(df, "3M_RS Percentile").clip(0, 99) / 99 * 100
    distance_score = (100 - (df["Dist_52WH_%"].clip(lower=0, upper=20) / 20 * 100)).fillna(0)
    trend_score = np.where(df["Trend_OK"], 100, 0)

    df["Leader_Score"] = (
        0.35 * rs_score.fillna(0)
        + 0.20 * rs_1m_score.fillna(0)
        + 0.15 * rs_3m_score.fillna(0)
        + 0.15 * distance_score
        + 0.15 * trend_score
    ).round(2)

    return df


def generate_sector_opportunities(df_stocks: pd.DataFrame, df_sectors: pd.DataFrame, output_dir: str, top_sectors: int = 5, top_per_sector: int = 5):
    """
    Create sector_opportunities.csv:
      - Uses top ranked sectors from rs_sectors.csv logic.
      - Finds top 5 opportunities inside each leading sector.
      - Hard filters: RS>=90, Price>SMA50>SMA200, within 20% of 52WH.
      - Does NOT hard-filter Volume > Avg or ADR > 2%.
    """
    df = add_visual_setup_columns(df_stocks)

    sector_rank_map = df_sectors[["Rank", "Sector"]].dropna().copy()
    sector_rank_map = sector_rank_map[~sector_rank_map["Sector"].isin(["ETF", "Unknown", ""])]
    leading_sectors = sector_rank_map.head(top_sectors)
    allowed_sectors = set(leading_sectors["Sector"].astype(str))
    rank_lookup = dict(zip(leading_sectors["Sector"], leading_sectors["Rank"]))

    candidates = df[df["Sector"].astype(str).isin(allowed_sectors)].copy()
    candidates = candidates[
        (_numeric_series(candidates, "RS Percentile") >= 90)
        & candidates["Trend_OK"]
        & candidates["Near_52WH_20pct"]
    ].copy()

    if candidates.empty:
        out_cols = [
            "SectorRank", "Sector", "LeaderRank", "Ticker", "Leader_Score", "Setup_Type",
            "Tightness", "Setup_Price", "Dist_52WH_%", "ATR_%", "RS Percentile",
            "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile", "IPO",
            "Gap (%)", "HVE", "SMA50", "SMA200", "SMA10W", "SMA30W", "Industry"
        ]
        pd.DataFrame(columns=out_cols).to_csv(os.path.join(output_dir, "sector_opportunities.csv"), index=False)
        return pd.DataFrame(columns=out_cols)

    candidates["SectorRank"] = candidates["Sector"].map(rank_lookup).astype(int)
    candidates = candidates.sort_values(
        ["SectorRank", "Leader_Score", "RS Percentile", "3M_RS Percentile", "1M_RS Percentile"],
        ascending=[True, False, False, False, False],
        na_position="last",
    )
    candidates["LeaderRank"] = candidates.groupby("Sector").cumcount() + 1
    opportunities = candidates[candidates["LeaderRank"] <= top_per_sector].copy()

    out_cols = [
        "SectorRank", "Sector", "LeaderRank", "Ticker", "Leader_Score", "Setup_Type",
        "Tightness", "Setup_Price", "Dist_52WH_%", "ATR_%", "RS Percentile",
        "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile", "IPO",
        "Gap (%)", "HVE", "SMA50", "SMA200", "SMA10W", "SMA30W", "Industry"
    ]
    out_cols = [c for c in out_cols if c in opportunities.columns]

    for c in ["Setup_Price", "Dist_52WH_%", "ATR_%", "Leader_Score"]:
        if c in opportunities.columns:
            opportunities[c] = pd.to_numeric(opportunities[c], errors="coerce").round(2)

    opportunities[out_cols].to_csv(os.path.join(output_dir, "sector_opportunities.csv"), index=False, na_rep="")
    return opportunities[out_cols]


def main(arctic_db_path, reference_ticker, output_dir, log_file, metadata_file=None, percentiles=None, debug=False, hve_output_dir="HVE", vol9m_output_dir="9M_Vol", min_daily_volume=9_000_000):
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
    latest_market_date = datetime.fromtimestamp(ref_data["datetime"].max()).date()

    metadata_df = load_metadata(metadata_file)

    rs_results = []
    hve_history = []
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
            gap_pct = calculate_gap_pct_from_dataframe(data)
            hve_record = build_hve_record_from_dataframe(data, ticker)
            latest_volume, vol9m_plus = calculate_9m_plus_volume_from_dataframe(data, min_daily_volume)
            hve_history.append(hve_record)

            quality_stats["total_processed"] += 1
            if len(closes) < 252:
                quality_stats["short_history_lt_252"] += 1

            log_missing_rs(ticker, f"=== Debug for {ticker} ===", missing_rs_log)
            if len(closes) > 0:
                log_missing_rs(ticker, f"Rows: {len(closes)} | Start={closes.index[0].date()} | End={closes.index[-1].date()}", missing_rs_log)
            log_missing_rs(ticker, f"Has_1M={len(closes)>=22}, Has_3M={len(closes)>=64}, Has_6M={len(closes)>=127}, Has_12M={len(closes)>=253}", missing_rs_log)

            if len(closes) < 2:
                log_missing_rs(ticker, "NOT ENOUGH DATA (<2 rows)", missing_rs_log)
                rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, len(closes), np.nan, np.nan, gap_pct, latest_volume, vol9m_plus, hve_record.get("HVE", "NO"), hve_record.get("HVE Date", ""), hve_record.get("HVE Volume", np.nan), None, np.nan, np.nan))
                continue

            sma20, sma50, sma200, sma10w, sma30w = calculate_smas(closes)
            prev_close = round(float(closes.iloc[-2]), 4) if len(closes) >= 2 else np.nan
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

            log_missing_rs(ticker, f"FINAL → RS={rs}, 1M={rs_1m}, 3M={rs_3m}, 6M={rs_6m} | SMA20={sma20}, SMA50={sma50}, SMA200={sma200}, SMA10W={sma10w}, SMA30W={sma30w}, ATR={atr}, ADR={adr}", missing_rs_log)
            log_missing_rs(ticker, "-" * 60, missing_rs_log)

            earning_date = None
            if not metadata_df.empty:
                meta_row = metadata_df[metadata_df["Ticker"] == ticker]
                if not meta_row.empty:
                    earning_date = meta_row.iloc[0].get("Earning_Date")

            rs_results.append((ticker, rs, rs_1m, rs_3m, rs_6m, sma20, sma50, sma200, sma10w, sma30w, len(closes), round(float(closes.iloc[-1]), 4), prev_close, gap_pct, latest_volume, vol9m_plus, hve_record.get("HVE", "NO"), hve_record.get("HVE Date", ""), hve_record.get("HVE Volume", np.nan), earning_date, atr, adr))

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
            rs_results.append((ticker, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, "NO", "", np.nan, None, np.nan, np.nan))

    df_stocks = pd.DataFrame(rs_results, columns=["Ticker", "RS", "1M_RS", "3M_RS", "6M_RS", "SMA20", "SMA50", "SMA200", "SMA10W", "SMA30W", "History_Days", "Last_Close", "Prev_Close", "Gap (%)", "Latest Volume", "9M+ Volume", "HVE", "HVE Date", "HVE Volume", "Earning_Date", "ATR", "ADR"])

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
    df_stocks["IPO"] = "NO"
    if "History_Days" in df_stocks.columns:
        df_stocks.loc[df_stocks["History_Days"].lt(50), "IPO"] = "YES"
        quality_stats["ipo_lt_50_days"] = int(df_stocks["IPO"].eq("YES").sum())

    if "Type" in df_stocks.columns:
        etf_mask = df_stocks["Type"].astype(str).str.upper().eq("ETF")
        df_stocks.loc[etf_mask, ["Sector", "Industry"]] = "ETF"
        df_stocks.loc[etf_mask, "IPO"] = "NO"

    # HVE should mean highest-volume-ever on the latest market date, not a stale
    # ticker-specific latest bar. Keep HVE Date / HVE Volume visible as history,
    # but turn HVE to NO when the record date is not the market/reference date.
    if "HVE" in df_stocks.columns and "HVE Date" in df_stocks.columns:
        hve_dates = pd.to_datetime(df_stocks["HVE Date"], errors="coerce").dt.date
        stale_hve_mask = df_stocks["HVE"].astype(str).str.upper().eq("YES") & ~hve_dates.eq(latest_market_date)
        df_stocks.loc[stale_hve_mask, "HVE"] = "NO"

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

    final_columns = ["Rank", "Ticker", "Price", "Prev_Close", "DVol", "Sector", "Industry",
                     "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile",
                     "ATR", "ADR", "AvgVol", "AvgVol10", "52WKH", "52WKL", "MCAP", "IPO",
                     "SMA20", "SMA50", "SMA200", "SMA10W", "SMA30W", "Earning_Date",
                     "History_Days", "Gap (%)", "Latest Volume", "9M+ Volume", "HVE", "HVE Date", "HVE Volume"]

    # ====================== ROBUST EARNING_DATE HANDLING ======================
    # Fix column name conflict from pandas merge (Earning_Date_x / _y)
    if "Earning_Date_x" in df_stocks.columns:
        df_stocks = df_stocks.rename(columns={"Earning_Date_x": "Earning_Date"})
    elif "Earning_Date_y" in df_stocks.columns:
        df_stocks = df_stocks.rename(columns={"Earning_Date_y": "Earning_Date"})

    # Only select existing columns
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
    leader_columns = ["IndustryRank", "Ticker", "Price", "Sector", "Industry", "RS Percentile", "1M_RS Percentile", "3M_RS Percentile", "6M_RS Percentile", "ATR", "ADR", "Gap (%)", "IPO", "HVE"]
    available_leader_cols = [col for col in leader_columns if col in industry_leaders.columns]
    industry_leaders[available_leader_cols].to_csv(os.path.join(output_dir, "industry_leaders.csv"), index=False, na_rep="")

    # Sector Opportunities: top 5 visual-friendly setups from each leading sector.
    # No hard filters for Volume > Average or ADR > 2%; those remain visual review items.
    sector_opportunities = generate_sector_opportunities(
        df_stocks,
        df_sectors,
        output_dir,
        top_sectors=5,
        top_per_sector=5,
    )

    hve_df, stock_hve, etf_hve, stock_hve_path, etf_hve_path = write_hve_outputs(
        df_stocks,
        hve_history,
        hve_output_dir,
    )
    vol9m_path = write_9m_plus_volume_output(df_stocks, vol9m_output_dir)

    generate_tradingview_csv(df_stocks, output_dir, ref_data, percentiles)
    generate_pine_thresholds(df_stocks, output_dir, percentiles)

    print("\n=== USA RS CALCULATION COMPLETE ===")
    print(f"Valid RS: {valid_rs_count:,} / {len(df_stocks):,}")
    print(f"IPO (<50 trading days): {int(df_stocks['IPO'].eq('YES').sum()):,}")
    print(f"Sector opportunities: {len(sector_opportunities):,}")
    print(f"HVE today: {int(df_stocks['HVE'].astype(str).str.upper().eq('YES').sum()):,}")
    print(f"Output directory: {output_dir}")
    print("Generated files: rs_stocks.csv, rs_industries.csv, rs_sectors.csv, industry_leaders.csv, sector_opportunities.csv, RSRATING.csv")
    print(f"HVE files: {os.path.join(hve_output_dir, 'History_HVE.csv')}, {stock_hve_path}, {etf_hve_path}")
    print(f"9M+ Volume file: {vol9m_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate RS from ArcticDB - Enhanced USA Version")
    parser.add_argument("--arctic-db-path", default="data/arctic_db/prices", help="Path to ArcticDB")
    parser.add_argument("--reference-ticker", default="SPY", help="Reference ticker")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--log-file", default="logs/failed_logs.log", help="Log file")
    parser.add_argument("--metadata-file", default=None, help="Metadata JSON file")
    parser.add_argument("--percentiles", default="99,98,95,90,85,80,75,70,60,50,40,30,20,10,5,1", help="Comma-separated percentile thresholds")
    parser.add_argument("--hve-output-dir", default="HVE", help="Output directory for HVE files")
    parser.add_argument("--vol9m-output-dir", default="9M_Vol", help="Output directory for 9M+ daily volume files")
    parser.add_argument("--min-daily-volume", type=int, default=9000000, help="Minimum latest daily volume for 9M+ Volume YES flag")
    parser.add_argument("--debug", action="store_true", help="Enable debug")
    args = parser.parse_args()
    percentiles = sorted({int(p.strip()) for p in args.percentiles.split(",") if p.strip()}, reverse=True)
    os.makedirs(os.path.dirname(args.log_file), exist_ok=True)
    main(args.arctic_db_path, args.reference_ticker, args.output_dir, args.log_file, args.metadata_file, percentiles, args.debug, args.hve_output_dir, args.vol9m_output_dir, args.min_daily_volume)
