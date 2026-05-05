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
    digits = "".join(ch for ch in path.stem if ch.isdigit())
    return datetime.strptime(digits[-8:], "%m%d%Y").date()

def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"

def read_source(path: Path):
    df = pd.read_csv(path)
    # Use exact column names from your file
    # Rank, Ticker, Price, Sector, Industry, "RS Percentile", "52WKH", "52WKL",
    # EarningDate, SMA200, SMA30W, ...
    return df

def filter_earnings_universe(df: pd.DataFrame) -> pd.DataFrame:
    # SMA filters
    cond_price = df["Price"] > df["SMA200"]
    cond_30w = df["Price"] > df["SMA30W"]

    # Drop non‑reporters: EarningDate == 'No' or blank/NaN
    ed = df["EarningDate"].astype(str).str.strip()
    cond_earn = (ed.ne("No")) & (ed.ne("")) & (ed.str.lower().ne("black"))

    return df[cond_price & cond_30w & cond_earn].copy()

def future_file(run_date, offset_days):
    d = run_date + timedelta(days=offset_days)
    return ARCHIVE_DIR / f"rs_stocks_{d.strftime('%m%d%Y')}.csv"

def price_map_for_file(path: Path):
    if not path.exists():
        return {}
    df = read_source(path)
    # Assuming same schema: use Price as close
    return df.set_index("Ticker")["Price"].to_dict()

def main():
    run_date = parse_date_from_filename(TODAY_SOURCE)
    out_path = month_output_path(run_date)

    src = read_source(TODAY_SOURCE)
    base_df = filter_earnings_universe(src)

    # Start fresh for *this* day’s universe:
    cur = base_df[BASE_COLS].copy()
    cur = cur.rename(columns={"EarningDate": "EarningDate"})  # keep name consistent

    # Initialize day columns as NaN
    for c in DAY_COLS:
        cur[c] = pd.NA

    # Fill E_Day1..E_Day6 from future files
    tickers = cur["Ticker"].astype(str)
    for i in range(1, 7):
        f = future_file(run_date, i)
        pmap = price_map_for_file(f)
        cur[f"E_Day{i}"] = tickers.map(pmap)

    # If month file exists, we only want to preserve E_Day columns for the same tickers,
    # but *not* keep tickers that no longer satisfy earnings + SMA filters.
    if out_path.exists():
        old = pd.read_csv(out_path)
        # Restrict old to tickers still in today's filtered universe
        old = old.set_index("Ticker")
        cur = cur.set_index("Ticker")

        # For tickers in both, keep existing E_Day values where current is NaN
        for c in DAY_COLS:
            if c in old.columns:
                cur[c] = cur[c].fillna(old[c])

        final_df = cur.reset_index()
    else:
        final_df = cur.reset_index()

    # Final column order
    final_df = final_df[
        ["Rank", "Ticker", "Price", "Sector", "Industry",
         "RS Percentile", "52WKH", "52WKL", "EarningDate"] + DAY_COLS
    ]
    final_df.sort_values(["EarningDate", "Rank"], inplace=True, na_position="last")

    final_df.to_csv(out_path, index=False)

if __name__ == "__main__":
    main()
