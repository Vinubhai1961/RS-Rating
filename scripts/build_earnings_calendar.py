# scripts/build_earnings_calendar.py
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

BASE_DIR = Path(".")
ARCHIVE_DIR = BASE_DIR / "archive"
OUTPUT_DIR = BASE_DIR / "Earnings"

TODAY_SOURCE = ARCHIVE_DIR / "rs_stocks_05042026.csv"

BASE_COLS = [
    "Rank", "Ticker", "Price", "Sector", "Industry",
    "RS Percentile", "52WKH", "52WKL", "EarningDate"
]
DAY_COLS = [f"E_Day{i}" for i in range(1, 7)]


def parse_date_from_filename(path: Path):
    s = path.stem
    digits = "".join(ch for ch in s if ch.isdigit())
    return datetime.strptime(digits[-8:], "%m%d%Y").date()


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def read_source(path: Path):
    df = pd.read_csv(path)
    df.columns = [c.strip().replace(" ", "") for c in df.columns]

    rename_map = {
        "EarningDate": "EarningDate",
        "Earning_Date": "EarningDate",
        "RSPercentile": "RS Percentile",
        "RSPercentileAvg": "RS Percentile",
    }
    df = df.rename(columns=rename_map)

    if "Ticker" not in df.columns:
        raise ValueError(f"Ticker column missing in {path}")

    # ✅ Clean + enforce valid earnings only
    if "EarningDate" in df.columns:
        df["EarningDate"] = pd.to_datetime(df["EarningDate"], errors="coerce")
        df = df[df["EarningDate"].notna()].copy()

    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile", "Rank"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_price_map(path: Path):
    if not path.exists():
        return {}
    df = read_source(path)
    price_col = "Close" if "Close" in df.columns else "Price"
    if price_col not in df.columns:
        raise ValueError(f"No Close/Price column found in {path}")
    return df.set_index("Ticker")[price_col].to_dict()


def build_future_file(current_date, offset_days):
    future_date = current_date + timedelta(days=offset_days)
    return ARCHIVE_DIR / f"rs_stocks_{future_date.strftime('%m%d%Y')}.csv"


def main():
    run_date = parse_date_from_filename(TODAY_SOURCE)
    out_path = month_output_path(run_date)

    df = read_source(TODAY_SOURCE)

    # ✅ Technical filter
    df = df[
        (df["Price"].fillna(-1) > df["SMA200"].fillna(float("inf"))) &
        (df["Price"].fillna(-1) > df["SMA30W"].fillna(float("inf")))
    ].copy()

    # ✅ Earnings already cleaned in read_source, but safe to keep
    if "EarningDate" in df.columns:
        df = df[df["EarningDate"].notna()].copy()

    out = df[BASE_COLS].copy()
    out = out.rename(columns={"EarningDate": "Earning_Date"})

    # Initialize day columns
    for col in DAY_COLS:
        out[col] = pd.NA

    tickers = out["Ticker"].astype(str)

    # Fill future prices
    for i in range(1, 7):
        future_file = build_future_file(run_date, i)
        price_map = get_price_map(future_file)
        out[f"E_Day{i}"] = tickers.map(price_map)

    # =========================
    # ✅ Merge with existing file
    # =========================
    if out_path.exists():
        existing = pd.read_csv(out_path)

        if "Ticker" in existing.columns:
            existing = existing.set_index("Ticker")
            out = out.set_index("Ticker")

            # ✅ Critical fix: align dtype
            existing = existing.astype(str)
            out = out.astype(str)

            existing.update(out)

            merged = existing.reset_index()
            new_rows = out.loc[~out.index.isin(existing.index)].reset_index()

            final_df = pd.concat([merged, new_rows], ignore_index=True)
        else:
            final_df = out.reset_index()
    else:
        final_df = out.reset_index()

    # Final formatting
    final_df = final_df[
        ["Rank", "Ticker", "Price", "Sector", "Industry",
         "RS Percentile", "52WKH", "52WKL", "Earning_Date"] + DAY_COLS
    ]

    final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")
    final_df.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
