#!/usr/bin/env python3
"""
Build yearly Market Breadth from RS_Data/rs_stocks.csv.

Approved scope:
  - Stocks only: Sector != ETF
  - Output: market_breadth/market_breadth_YYYY.csv
  - Transposed yearly output: Metric on rows, dates on columns
  - Archive is read-only: another workflow already saves archive/rs_stocks_MM-DD-YYYY.csv
  - Use 5th previous available archive file for 5-day +/-20% breadth when available
"""

import argparse
import re
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


DATE_FMT_OUTPUT = "%m/%d/%Y"
DATE_FMT_FILE = "%m-%d-%Y"


OUTPUT_COLUMNS = [
    "Date",
    "Total Stocks",
    "UP 4.5% Today",
    "Down 4.5% Today",
    "Net 4.5% Today",
    "UP 4.5% Today %",
    "Down 4.5% Today %",
    "UP 20% in 5Days",
    "Down 20% in 5Days",
    "UP 20% in 5Days %",
    "Down 20% in 5Days %",
    "Above 20SMA",
    "Below 20SMA",
    "Above 20SMA %",
    "Below 20SMA %",
    "Above 50SMA",
    "Below 50SMA",
    "Above 50SMA %",
    "Below 50SMA %",
    "Above 200SMA",
    "Below 200SMA",
    "Above 200SMA %",
    "Below 200SMA %",
    "52WKH",
    "52WKL",
    "Net 52WH/L",
    "0-25% 52WKH",
    "0-25% 52WKH %",
    "0-25% 52WKL",
    "0-25% 52WKL %",
]


REQUIRED_COLUMNS = [
    "Ticker",
    "Price",
    "Prev_Close",
    "Sector",
    "SMA20",
    "SMA50",
    "SMA200",
    "52WKH",
    "52WKL",
]


def parse_run_date(value: str | None, timezone: str) -> date:
    """Return run date. Supports YYYY-MM-DD override; otherwise local date."""
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.now(ZoneInfo(timezone)).date()


def parse_volume_like_number(value):
    """Convert numeric strings such as 400K/1.2M/3B if they appear in price fields."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float, np.number)):
        return float(value)
    text = str(value).strip().replace(",", "").upper()
    if text == "":
        return np.nan
    try:
        if text.endswith("K"):
            return float(text[:-1]) * 1_000
        if text.endswith("M"):
            return float(text[:-1]) * 1_000_000
        if text.endswith("B"):
            return float(text[:-1]) * 1_000_000_000
        return float(text)
    except Exception:
        return np.nan


def numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_volume_like_number)
    return df


def pct(count, total):
    if total <= 0 or pd.isna(count):
        return ""
    return round((float(count) / float(total)) * 100.0, 2)


def count_true(series: pd.Series) -> int:
    return int(series.fillna(False).sum())


def load_current_stocks(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns in rs_stocks.csv: " + ", ".join(missing)
        )

    df = numeric(df, ["Price", "Prev_Close", "SMA20", "SMA50", "SMA200", "52WKH", "52WKL"])

    # Approved stock-only rule: Sector != ETF.
    df = df[df["Sector"].astype(str).str.strip().str.upper() != "ETF"].copy()
    df = df.reset_index(drop=True)
    return df


def parse_archive_date(path: Path):
    """Supports rs_stocks_MM-DD-YYYY.csv and legacy rs_stocks_MMDDYYYY.csv."""
    name = path.name

    m = re.match(r"^rs_stocks_(\d{2})-(\d{2})-(\d{4})\.csv$", name)
    if m:
        mm, dd, yyyy = m.groups()
        return date(int(yyyy), int(mm), int(dd))

    m = re.match(r"^rs_stocks_(\d{2})(\d{2})(\d{4})\.csv$", name)
    if m:
        mm, dd, yyyy = m.groups()
        return date(int(yyyy), int(mm), int(dd))

    return None


def find_5th_previous_archive(archive_dir: Path, run_dt: date) -> Path | None:
    if not archive_dir.exists():
        return None

    dated_files = []
    for path in archive_dir.glob("rs_stocks_*.csv"):
        file_dt = parse_archive_date(path)
        if file_dt is not None and file_dt < run_dt:
            dated_files.append((file_dt, path))

    dated_files.sort(key=lambda x: x[0])
    if len(dated_files) < 5:
        return None

    return dated_files[-5][1]


def calculate_5d_counts(current_df: pd.DataFrame, archive_5d_path: Path | None):
    if archive_5d_path is None:
        return "", "", "", ""

    old_df = pd.read_csv(archive_5d_path)
    if "Ticker" not in old_df.columns or "Price" not in old_df.columns:
        return "", "", "", ""

    old_df = numeric(old_df[["Ticker", "Price"]].copy(), ["Price"])
    old_df = old_df.rename(columns={"Price": "Price_5D_Ago"})

    merged = current_df[["Ticker", "Price"]].merge(old_df, on="Ticker", how="inner")
    if merged.empty:
        return "", "", "", ""

    up_20_5d = count_true(merged["Price"] >= merged["Price_5D_Ago"] * 1.20)
    down_20_5d = count_true(merged["Price"] <= merged["Price_5D_Ago"] * 0.80)
    total = len(current_df)

    return up_20_5d, down_20_5d, pct(up_20_5d, total), pct(down_20_5d, total)


def build_breadth_row(df: pd.DataFrame, run_dt: date, archive_5d_path: Path | None) -> dict:
    total = int(len(df))

    up_45 = count_true(df["Price"] >= df["Prev_Close"] * 1.045)
    down_45 = count_true(df["Price"] <= df["Prev_Close"] * 0.955)

    up_20_5d, down_20_5d, up_20_5d_pct, down_20_5d_pct = calculate_5d_counts(df, archive_5d_path)

    above_20 = count_true(df["Price"] > df["SMA20"])
    below_20 = count_true(df["Price"] < df["SMA20"])
    above_50 = count_true(df["Price"] > df["SMA50"])
    below_50 = count_true(df["Price"] < df["SMA50"])
    above_200 = count_true(df["Price"] > df["SMA200"])
    below_200 = count_true(df["Price"] < df["SMA200"])

    new_52wh = count_true(df["Price"] >= df["52WKH"])
    new_52wl = count_true(df["Price"] <= df["52WKL"])

    within_25_wh = count_true(df["Price"] >= df["52WKH"] * 0.75)
    within_25_wl = count_true(df["Price"] <= df["52WKL"] * 1.25)

    return {
        "Date": run_dt.strftime(DATE_FMT_OUTPUT),
        "Total Stocks": total,
        "UP 4.5% Today": up_45,
        "Down 4.5% Today": down_45,
        "Net 4.5% Today": up_45 - down_45,
        "UP 4.5% Today %": pct(up_45, total),
        "Down 4.5% Today %": pct(down_45, total),
        "UP 20% in 5Days": up_20_5d,
        "Down 20% in 5Days": down_20_5d,
        "UP 20% in 5Days %": up_20_5d_pct,
        "Down 20% in 5Days %": down_20_5d_pct,
        "Above 20SMA": above_20,
        "Below 20SMA": below_20,
        "Above 20SMA %": pct(above_20, total),
        "Below 20SMA %": pct(below_20, total),
        "Above 50SMA": above_50,
        "Below 50SMA": below_50,
        "Above 50SMA %": pct(above_50, total),
        "Below 50SMA %": pct(below_50, total),
        "Above 200SMA": above_200,
        "Below 200SMA": below_200,
        "Above 200SMA %": pct(above_200, total),
        "Below 200SMA %": pct(below_200, total),
        "52WKH": new_52wh,
        "52WKL": new_52wl,
        "Net 52WH/L": new_52wh - new_52wl,
        "0-25% 52WKH": within_25_wh,
        "0-25% 52WKH %": pct(within_25_wh, total),
        "0-25% 52WKL": within_25_wl,
        "0-25% 52WKL %": pct(within_25_wl, total),
    }


def _empty_transposed_output() -> pd.DataFrame:
    """Create blank transposed output with approved metric order."""
    return pd.DataFrame({"Metric": OUTPUT_COLUMNS[1:]})


def _normalize_date_column_name(value: str) -> str:
    """Normalize parseable date-like column names to MM/DD/YYYY; preserve others."""
    try:
        dt = pd.to_datetime(str(value), errors="raise")
        return dt.strftime(DATE_FMT_OUTPUT)
    except Exception:
        return str(value)


def _convert_date_rows_to_metric_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert old layout:
        Date, Total Stocks, Above 20SMA, ...
    into new layout:
        Metric, 07/08/2026, 07/09/2026, ...
    """
    if df.empty or "Date" not in df.columns:
        return _empty_transposed_output()

    # Keep only approved output columns that exist in the old layout.
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[OUTPUT_COLUMNS].copy()
    df["Date"] = df["Date"].astype(str)

    records = []
    for metric in OUTPUT_COLUMNS[1:]:
        record = {"Metric": metric}
        for _, row in df.iterrows():
            date_col = _normalize_date_column_name(row["Date"])
            record[date_col] = row.get(metric, "")
        records.append(record)

    out = pd.DataFrame(records)
    return _sort_transposed_columns(out)


def _sort_transposed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep Metric first, then sort date columns chronologically."""
    if "Metric" not in df.columns:
        df["Metric"] = OUTPUT_COLUMNS[1:]

    date_cols = [c for c in df.columns if c != "Metric"]

    def sort_key(col):
        parsed = pd.to_datetime(col, format=DATE_FMT_OUTPUT, errors="coerce")
        if pd.isna(parsed):
            parsed = pd.to_datetime(col, errors="coerce")
        return parsed if not pd.isna(parsed) else pd.Timestamp.max

    date_cols = sorted(date_cols, key=sort_key)
    return df[["Metric"] + date_cols]


def upsert_yearly_output(row: dict, output_dir: Path, run_dt: date) -> Path:
    """
    Write/update yearly market breadth in transposed format:

        Metric,07/08/2026,07/09/2026,...
        Total Stocks,8450,8461,...
        UP 4.5% Today,312,280,...

    If an older Date-row format file already exists, convert it automatically.
    If today's column already exists, replace it so workflow reruns do not duplicate data.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"market_breadth_{run_dt.year}.csv"
    date_col = row["Date"]

    existing = _empty_transposed_output()

    if output_path.exists() and output_path.stat().st_size > 0:
        try:
            raw = pd.read_csv(output_path, dtype=str)
            if "Metric" in raw.columns:
                existing = raw.copy()
            elif "Date" in raw.columns:
                # Backward-compatible: convert previous horizontal format once.
                existing = _convert_date_rows_to_metric_rows(raw)
            else:
                existing = _empty_transposed_output()
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            existing = _empty_transposed_output()

    # Ensure every approved metric exists exactly once and in approved order.
    metric_order = pd.DataFrame({"Metric": OUTPUT_COLUMNS[1:]})
    existing = existing.drop_duplicates(subset=["Metric"], keep="last") if "Metric" in existing.columns else _empty_transposed_output()
    existing = metric_order.merge(existing, on="Metric", how="left")

    # Replace today's column values.
    for metric in OUTPUT_COLUMNS[1:]:
        existing.loc[existing["Metric"] == metric, date_col] = row.get(metric, "")

    existing = _sort_transposed_columns(existing)
    existing.to_csv(output_path, index=False)
    return output_path

def main():
    parser = argparse.ArgumentParser(description="Build market breadth from RS_Data/rs_stocks.csv")
    parser.add_argument("--input", default="RS_Data/rs_stocks.csv", help="Current rs_stocks.csv path")
    parser.add_argument("--archive-dir", default="archive", help="Archive folder for rs_stocks_MM-DD-YYYY.csv")
    parser.add_argument("--output-dir", default="market_breadth", help="Output folder for market_breadth_YYYY.csv")
    parser.add_argument("--date", default=None, help="Optional run date override: YYYY-MM-DD")
    parser.add_argument("--timezone", default="America/New_York", help="Timezone for default run date")
    args = parser.parse_args()

    input_path = Path(args.input)
    archive_dir = Path(args.archive_dir)
    output_dir = Path(args.output_dir)
    run_dt = parse_run_date(args.date, args.timezone)

    current_df = load_current_stocks(input_path)

    # Print stock tickers making a new 52-week high during this workflow run.
    # Uses the same rule as the market breadth calculation: Price >= 52WKH.
    new_52wh_tickers = (
        current_df.loc[
            current_df["Price"].notna()
            & current_df["52WKH"].notna()
            & (current_df["Price"] >= current_df["52WKH"]),
            "Ticker",
        ]
        .dropna()
        .astype(str)
        .str.strip()
    )
    new_52wh_tickers = new_52wh_tickers[new_52wh_tickers.ne("")].drop_duplicates()

    print("\n=== STOCKS MAKING 52-WEEK HIGH ===")
    if new_52wh_tickers.empty:
        print("None")
    else:
        for ticker in new_52wh_tickers:
            print(ticker)

    # Archive is read-only. Another workflow is responsible for saving
    # archive/rs_stocks_MM-DD-YYYY.csv. For 5-day breadth, use the 5th
    # previous available archive snapshot before run_dt.
    archive_5d_path = find_5th_previous_archive(archive_dir, run_dt)
    row = build_breadth_row(current_df, run_dt, archive_5d_path)

    output_path = upsert_yearly_output(row, output_dir, run_dt)

    print("\n=== MARKET BREADTH COMPLETE ===")
    print(f"Date: {row['Date']}")
    print(f"Stocks counted: {row['Total Stocks']:,}")
    print(f"Output: {output_path}")
    if archive_5d_path:
        print(f"5-day comparison archive: {archive_5d_path}")
    else:
        print("5-day comparison archive: not enough prior archives yet")
    print(f"UP 4.5% Today: {row['UP 4.5% Today']:,} | Down 4.5% Today: {row['Down 4.5% Today']:,} | Net: {row['Net 4.5% Today']:,}")
    print(f"Above SMA20/50/200: {row['Above 20SMA']:,} / {row['Above 50SMA']:,} / {row['Above 200SMA']:,}")
    print(f"52WKH: {row['52WKH']:,} | 52WKL: {row['52WKL']:,} | Net 52WH/L: {row['Net 52WH/L']:,}")


if __name__ == "__main__":
    main()
