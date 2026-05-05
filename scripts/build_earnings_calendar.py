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

# -------------------------------
# GLOBAL CACHE (avoid reloading files)
# -------------------------------
PRICE_CACHE = {}

def normalize_ticker(val):
    return str(val).strip().upper()

def is_missing(val):
    return pd.isna(val) or val == "" or str(val).strip().lower() == "nan"

def get_today_source():
    today_str = datetime.now().strftime("%m%d%Y")
    file_path = ARCHIVE_DIR / f"rs_stocks_{today_str}.csv"

    if not file_path.exists():
        files = sorted(ARCHIVE_DIR.glob("rs_stocks_*.csv"), reverse=True)
        if files:
            file_path = files[0]
            print(f"[WARNING] Today's file not found. Using latest: {file_path.name}")
        else:
            raise FileNotFoundError(f"No rs_stocks files found in {ARCHIVE_DIR}")

    print(f"[INFO] Using source file: {file_path.name}")
    return file_path

def read_source(path: Path):
    print(f"[DEBUG] Reading file: {path.name}")

    df = pd.read_csv(path)

    df.columns = [c.strip().replace(" ", "") for c in df.columns]

    rename_map = {
        "EarningDate": "EarningDate",
        "Earning_Date": "EarningDate",
        "RSPercentile": "RS Percentile",
        "RSPercentileAvg": "RS Percentile",
    }
    df = df.rename(columns=rename_map)

    if "Ticker" in df.columns:
        df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()

    if "EarningDate" in df.columns:
        df["EarningDate"] = pd.to_datetime(df["EarningDate"], errors="coerce")

    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile", "Rank"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def get_price_map(target_date):
    if target_date in PRICE_CACHE:
        return PRICE_CACHE[target_date]

    file_name = f"rs_stocks_{target_date.strftime('%m%d%Y')}.csv"
    file_path = ARCHIVE_DIR / file_name

    print(f"[DEBUG] Loading price file: {file_name}")

    if not file_path.exists():
        print(f"[WARNING] Missing file: {file_name}")
        PRICE_CACHE[target_date] = {}
        return {}

    df = read_source(file_path)

    price_col = "Close" if "Close" in df.columns else "Price"

    if price_col not in df.columns:
        print(f"[ERROR] No price column in {file_name}")
        PRICE_CACHE[target_date] = {}
        return {}

    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()

    price_dict = df.set_index("Ticker")[price_col].to_dict()

    print(f"[DEBUG] Loaded {len(price_dict)} prices for {target_date}")

    PRICE_CACHE[target_date] = price_dict
    return price_dict

def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"

def main():
    try:
        TODAY_SOURCE = get_today_source()
        run_date = datetime.now().date()
        out_path = month_output_path(run_date)

        print(f"[INFO] Run Date: {run_date} | Output: {out_path.name}")

        df_today = read_source(TODAY_SOURCE)
        print(f"[DEBUG] Rows loaded: {len(df_today)}")

        tech_filter = (
            (df_today["Price"].fillna(-1) > df_today.get("SMA200", float("inf")).fillna(float("inf"))) &
            (df_today["Price"].fillna(-1) > df_today.get("SMA30W", float("inf")).fillna(float("inf")))
        )

        df_candidates = df_today[tech_filter].copy()
        print(f"[DEBUG] After filter: {len(df_candidates)}")

        if "EarningDate" in df_candidates.columns:
            df_candidates = df_candidates[df_candidates["EarningDate"].notna()].copy()
            print(f"[DEBUG] With earnings date: {len(df_candidates)}")

        if len(df_candidates) == 0:
            print("[INFO] No earnings candidates today.")
            return

        df_candidates = df_candidates[BASE_COLS].copy()
        df_candidates = df_candidates.rename(columns={"EarningDate": "Earning_Date"})
        df_candidates["Earning_Date"] = pd.to_datetime(df_candidates["Earning_Date"], errors="coerce")
        df_candidates["Ticker"] = df_candidates["Ticker"].apply(normalize_ticker)

        if out_path.exists():
            df_existing = pd.read_csv(out_path)
            df_existing["Earning_Date"] = pd.to_datetime(df_existing["Earning_Date"], errors="coerce")
            df_existing["Ticker"] = df_existing["Ticker"].apply(normalize_ticker)
            print(f"[INFO] Loaded existing file: {len(df_existing)} rows")
        else:
            df_existing = pd.DataFrame(columns=list(df_candidates.columns) + DAY_COLS)
            print("[INFO] Creating new monthly earnings file")

        records_to_add = []
        updated_count = 0

        for _, row in df_candidates.iterrows():
            ticker = normalize_ticker(row["Ticker"])
            earn_date = row["Earning_Date"].date()

            mask = (
                df_existing["Ticker"].eq(ticker) &
                (pd.to_datetime(df_existing["Earning_Date"]).dt.normalize() == pd.to_datetime(earn_date))
            )

            print(f"\n[TRACE] Processing {ticker} | Earn Date: {earn_date} | Matches: {mask.sum()}")

            if mask.any():
                for idx in df_existing[mask].index:
                    for i in range(1, 7):
                        col = f"E_Day{i}"

                        val = df_existing.at[idx, col] if col in df_existing.columns else None

                        if is_missing(val):
                            target_date = earn_date + timedelta(days=i)

                            # 🚫 Skip future dates (CRITICAL FIX)
                            if target_date > run_date:
                                print(f"[SKIP FUTURE] {ticker} {col} -> {target_date}")
                                continue

                            print(f"[CHECK] {ticker} {col} -> {target_date}")

                            price_map = get_price_map(target_date)

                            found = ticker in price_map
                            price = price_map.get(ticker, pd.NA)

                            print(f"[RESULT] Found={found} Price={price}")

                            df_existing.at[idx, col] = price
                            updated_count += 1
            else:
                print(f"[NEW] Adding new record for {ticker}")

                new_row = row.copy()

                for col in DAY_COLS:
                    new_row[col] = pd.NA

                for i in range(1, 7):
                    target_date = earn_date + timedelta(days=i)

                    if target_date > run_date:
                        continue

                    price_map = get_price_map(target_date)
                    new_row[f"E_Day{i}"] = price_map.get(ticker, pd.NA)

                records_to_add.append(new_row)

        if records_to_add:
            df_new = pd.DataFrame(records_to_add)
            final_df = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            final_df = df_existing.copy()

        final_cols = ["Rank", "Ticker", "Price", "Sector", "Industry",
                      "RS Percentile", "52WKH", "52WKL", "Earning_Date"] + DAY_COLS

        final_df = final_df[final_cols].copy()

        final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")

        final_df.to_csv(out_path, index=False)

        print(f"[SUCCESS] Updated {updated_count} fields | Total records: {len(final_df)}")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
