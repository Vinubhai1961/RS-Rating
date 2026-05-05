from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import traceback

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
    try:
        s = path.stem
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) < 8:
            raise ValueError(f"Not enough digits in filename: {s}")
        date_str = digits[-8:]
        dt = datetime.strptime(date_str, "%m%d%Y").date()
        print(f"[DEBUG] Parsed date from {path.name} → {dt}")
        return dt
    except Exception as e:
        print(f"[ERROR] Failed to parse date from {path}: {e}")
        raise


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def read_source(path: Path):
    print(f"[DEBUG] Reading source: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    df = pd.read_csv(path)
    print(f"[DEBUG] Columns before cleaning: {list(df.columns)}")
    
    df.columns = [c.strip().replace(" ", "") for c in df.columns]
    print(f"[DEBUG] Columns after cleaning: {list(df.columns)}")

    rename_map = {
        "EarningDate": "EarningDate",
        "Earning_Date": "EarningDate",
        "RSPercentile": "RS Percentile",
        "RSPercentileAvg": "RS Percentile",
    }
    df = df.rename(columns=rename_map)

    if "EarningDate" in df.columns:
        df["EarningDate"] = pd.to_datetime(df["EarningDate"], errors="coerce")
        valid_earnings = df["EarningDate"].notna().sum()
        print(f"[DEBUG] Valid EarningDate rows: {valid_earnings}/{len(df)}")

    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile", "Rank"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_price_map(path: Path):
    print(f"[DEBUG] Loading price map from: {path}")
    if not path.exists():
        print(f"[WARNING] Price file missing: {path}")
        return {}

    try:
        df = read_source(path)  # reuse cleaning logic
        price_col = "Close" if "Close" in df.columns else "Price"
        if price_col not in df.columns:
            print(f"[WARNING] No price column in {path}. Available: {list(df.columns)}")
            return {}
        price_dict = df.set_index("Ticker")[price_col].to_dict()
        print(f"[DEBUG] Loaded {len(price_dict)} prices from {path.name}")
        return price_dict
    except Exception as e:
        print(f"[ERROR] Failed to load price map {path}: {e}")
        return {}


def main():
    try:
        run_date = parse_date_from_filename(TODAY_SOURCE)
        out_path = month_output_path(run_date)
        print(f"[INFO] Run date: {run_date} | Output: {out_path}")

        # === Load today's data ===
        df = read_source(TODAY_SOURCE)
        print(f"[DEBUG] Total rows loaded: {len(df)}")

        # === Technical filter ===
        print("[DEBUG] Applying technical filter (Price > SMA200 & SMA30W)...")
        tech_filter = (
            (df["Price"].fillna(-1) > df["SMA200"].fillna(float("inf"))) &
            (df["Price"].fillna(-1) > df["SMA30W"].fillna(float("inf")))
        )
        df = df[tech_filter].copy()
        print(f"[DEBUG] After technical filter: {len(df)} rows")

        # === Earnings filter ===
        if "EarningDate" in df.columns:
            df = df[df["EarningDate"].notna()].copy()
            print(f"[DEBUG] After earnings date filter: {len(df)} rows")

        if len(df) == 0:
            print("[ERROR] No rows left after filtering! Check technical conditions or data.")
            return

        # === Prepare base output ===
        out = df[BASE_COLS].copy()
        out = out.rename(columns={"EarningDate": "Earning_Date"})
        out["Earning_Date"] = pd.to_datetime(out["Earning_Date"], errors="coerce")

        # Initialize E_Day columns
        for col in DAY_COLS:
            out[col] = pd.NA

        print(f"[DEBUG] Starting E_Day price lookup for {len(out)} tickers...")

        # === Fill E_Day columns ===
        for i in range(1, 7):
            day_prices = []
            target_date = run_date + timedelta(days=i)
            file_path = ARCHIVE_DIR / f"rs_stocks_{target_date.strftime('%m%d%Y')}.csv"

            price_map = get_price_map(file_path)

            for _, row in out.iterrows():
                ticker = row["Ticker"]
                price = price_map.get(ticker, pd.NA)
                day_prices.append(price)

            out[f"E_Day{i}"] = day_prices
            print(f"[DEBUG] Filled E_Day{i} using {target_date} → {len(price_map)} prices available")

        # === Final output ===
        final_df = out.reset_index(drop=True)
        final_df = final_df[
            ["Rank", "Ticker", "Price", "Sector", "Industry",
             "RS Percentile", "52WKH", "52WKL", "Earning_Date"] + DAY_COLS
        ]

        final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")

        print(f"[SUCCESS] Final rows: {len(final_df)}")
        final_df.to_csv(out_path, index=False)
        print(f"[SUCCESS] File saved: {out_path}")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
