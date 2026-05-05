from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import traceback

BASE_DIR = Path(".")
ARCHIVE_DIR = BASE_DIR / "archive"
OUTPUT_DIR = BASE_DIR / "Earnings"

BASE_COLS = [
    "Rank", "Ticker", "Price", "Sector", "Industry",
    "RS Percentile", "52WKH", "52WKL", "EarningDate"
]
DAY_COLS = [f"E_Day{i}" for i in range(1, 7)]


def get_today_source():
    """Dynamically get today's rs_stocks file"""
    today_str = datetime.now().strftime("%m%d%Y")
    file_path = ARCHIVE_DIR / f"rs_stocks_{today_str}.csv"
    
    if not file_path.exists():
        files = sorted(ARCHIVE_DIR.glob("rs_stocks_*.csv"), reverse=True)
        if files:
            file_path = files[0]
            print(f"[WARNING] Today's file not found. Using latest: {file_path.name}")
        else:
            raise FileNotFoundError(f"No rs_stocks_*.csv files found in {ARCHIVE_DIR}")
    
    print(f"[INFO] Using source file: {file_path.name}")
    return file_path


def parse_date_from_filename(path: Path):
    try:
        s = path.stem
        digits = "".join(ch for ch in s if ch.isdigit())
        date_str = digits[-8:]
        dt = datetime.strptime(date_str, "%m%d%Y").date()
        print(f"[DEBUG] Parsed run date: {dt}")
        return dt
    except Exception:
        return datetime.now().date()


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def read_source(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_csv(path)
    df.columns = [c.strip().replace(" ", "") for c in df.columns]

    rename_map = {
        "EarningDate": "EarningDate",
        "Earning_Date": "EarningDate",
        "RSPercentile": "RS Percentile",
        "RSPercentileAvg": "RS Percentile",
    }
    df = df.rename(columns=rename_map)

    if "EarningDate" in df.columns:
        df["EarningDate"] = pd.to_datetime(df["EarningDate"], errors="coerce")

    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile", "Rank"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_price_map(date_obj):
    file_path = ARCHIVE_DIR / f"rs_stocks_{date_obj.strftime('%m%d%Y')}.csv"
    if not file_path.exists():
        return {}
    try:
        df = read_source(file_path)
        price_col = "Close" if "Close" in df.columns else "Price"
        return df.set_index("Ticker")[price_col].to_dict() if price_col in df.columns else {}
    except:
        return {}


def main():
    try:
        TODAY_SOURCE = get_today_source()
        run_date = parse_date_from_filename(TODAY_SOURCE)
        out_path = month_output_path(run_date)

        print(f"[INFO] Run Date: {run_date} | Monthly File: {out_path.name}")

        # === Load today's data ===
        df_today = read_source(TODAY_SOURCE)
        print(f"[DEBUG] Loaded {len(df_today)} rows from today's source")

        # Technical + Earnings filter
        tech_filter = (
            (df_today["Price"].fillna(-1) > df_today.get("SMA200", float("inf")).fillna(float("inf"))) &
            (df_today["Price"].fillna(-1) > df_today.get("SMA30W", float("inf")).fillna(float("inf")))
        )
        df_new = df_today[tech_filter].copy()

        if "EarningDate" in df_new.columns:
            df_new = df_new[df_new["EarningDate"].notna()].copy()

        if len(df_new) == 0:
            print("[INFO] No new stocks passed filters.")
            return

        df_new = df_new[BASE_COLS].copy()
        df_new = df_new.rename(columns={"EarningDate": "Earning_Date"})
        df_new["Earning_Date"] = pd.to_datetime(df_new["Earning_Date"], errors="coerce")

        print(f"[INFO] {len(df_new)} potential new earnings candidates")

        # === Load existing monthly file ===
        existing_df = pd.DataFrame()
        if out_path.exists():
            existing_df = pd.read_csv(out_path)
            print(f"[INFO] Loaded existing file with {len(existing_df)} records")
            
            # Create unique key for deduplication: Ticker + Earning_Date
            existing_df["Earning_Date"] = pd.to_datetime(existing_df["Earning_Date"], errors="coerce")
            existing_keys = set(
                zip(existing_df["Ticker"].astype(str), existing_df["Earning_Date"].dt.date)
            )
        else:
            existing_keys = set()

        # Filter only truly new earnings
        df_new["key"] = list(zip(df_new["Ticker"].astype(str), df_new["Earning_Date"].dt.date))
        df_to_add = df_new[~df_new["key"].isin(existing_keys)].copy()
        df_to_add = df_to_add.drop(columns=["key"])

        print(f"[INFO] New earnings to append: {len(df_to_add)}")

        if len(df_to_add) == 0:
            print("[INFO] No new earnings to add. File is up to date.")
            return

        # === Fill E_Day columns ONLY for new entries ===
        for col in DAY_COLS:
            df_to_add[col] = pd.NA

        print("[INFO] Filling post-earnings prices for new entries...")
        for i in range(1, 7):
            day_prices = []
            for _, row in df_to_add.iterrows():
                earning_date = row["Earning_Date"]
                target_date = earning_date + timedelta(days=i)
                price_map = get_price_map(target_date)
                price = price_map.get(row["Ticker"], pd.NA)
                day_prices.append(price)
            df_to_add[f"E_Day{i}"] = day_prices
            print(f"[DEBUG] Filled E_Day{i}")

        # === Append to existing data ===
        if not existing_df.empty:
            final_df = pd.concat([existing_df, df_to_add], ignore_index=True)
        else:
            final_df = df_to_add.copy()

        # Final cleanup and sort
        final_df = final_df[
            ["Rank", "Ticker", "Price", "Sector", "Industry",
             "RS Percentile", "52WKH", "52WKL", "Earning_Date"] + DAY_COLS
        ].copy()

        final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")

        final_df.to_csv(out_path, index=False)
        print(f"[SUCCESS] Added {len(df_to_add)} new records. Total now: {len(final_df)} → {out_path.name}")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
