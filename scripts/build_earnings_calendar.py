from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# Adjust these if your structure differs
ROOT_DIR = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT_DIR / "archive"          # rs_stocks_*.csv
OUTPUT_DIR = ROOT_DIR / "Earnings"         # May_2026_Earnings.csv

DAY_COLS = [f"E_Day{i}" for i in range(1, 7)]

# Input and output share the same column name Earning_Date
BASE_COLS = [
    "Rank",
    "Ticker",
    "Price",
    "Sector",
    "Industry",
    "RS Percentile",
    "52WKH",
    "52WKL",
    "Earning_Date",
]


def parse_date_from_filename(path: Path) -> datetime.date:
    """Extract date from rs_stocks_*.csv filename like rs_stocks_05042026.csv."""
    stem = path.stem
    digits = "".join(ch for ch in stem if ch.isdigit())
    return datetime.strptime(digits[-8:], "%m%d%Y").date()


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def read_source(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def filter_earnings_universe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply SMA filters and require a real earnings date.
    Uses Earning_Date from rs_stocks_*.csv.
    """
    cond_price = df["Price"] > df["SMA200"]
    cond_30w = df["Price"] > df["SMA30W"]

    ed = df["Earning_Date"].astype(str).str.strip()
    cond_earn = (
        ed.ne("No")
        & ed.ne("")
        & ed.str.lower().ne("black")
    )

    return df[cond_price & cond_30w & cond_earn].copy()


def future_file(run_date, offset_days: int) -> Path:
    d = run_date + timedelta(days=offset_days)
    return ARCHIVE_DIR / f"rs_stocks_{d.strftime('%m%d%Y')}.csv"


def price_map_for_file(path: Path):
    """Build a ticker -> Price dict from a future rs_stocks file."""
    if not path.exists():
        return {}
    df = read_source(path)
    return df.set_index("Ticker")["Price"].to_dict()


def main():
    # Use the latest rs_stocks_*.csv in archive as today's source
    rs_files = sorted(ARCHIVE_DIR.glob("rs_stocks_*.csv"))
    if not rs_files:
        raise FileNotFoundError(f"No rs_stocks_*.csv files found in {ARCHIVE_DIR}")

    today_source = rs_files[-1]
    run_date = parse_date_from_filename(today_source)
    out_path = month_output_path(run_date)

    src = read_source(today_source)
    base_df = filter_earnings_universe(src)

    # Take only the core columns, keep Earning_Date name as-is
    cur = base_df[BASE_COLS].copy()

    # Initialize E_Day1..E_Day6 as NaN
    for c in DAY_COLS:
        cur[c] = pd.NA

    tickers = cur["Ticker"].astype(str)

    # Fill E_Day1..E_Day6 from future daily files
    for i in range(1, 7):
        fpath = future_file(run_date, i)
        pmap = price_map_for_file(fpath)
        if not pmap:
            continue
        cur[f"E_Day{i}"] = tickers.map(pmap)

    # If monthly file exists, only carry forward E_Day values
    # for tickers that are still in today's filtered universe.
    if out_path.exists():
        old = pd.read_csv(out_path)
        old = old.set_index("Ticker")
        cur = cur.set_index("Ticker")

        for c in DAY_COLS:
            if c in old.columns:
                cur[c] = cur[c].fillna(old[c])

        final_df = cur.reset_index()
    else:
        final_df = cur.reset_index()

    # Final column order and sort
    final_df = final_df[BASE_COLS + DAY_COLS]
    final_df.sort_values(["Earning_Date", "Rank"], inplace=True, na_position="last")

    # Overwrite monthly output file
    final_df.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
