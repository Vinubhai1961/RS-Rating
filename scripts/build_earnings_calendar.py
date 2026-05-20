from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import traceback

BASE_DIR = Path(".")
ARCHIVE_DIR = BASE_DIR / "archive"
OUTPUT_DIR = BASE_DIR / "Earnings"

# ====================== NEW: ATR & ADR CONFIG ======================
MIN_ATR = 3.0
MIN_ADR = 3.0
# ================================================================

BASE_COLS = [
    "Rank", "Ticker", "Price", "Sector", "Industry",
    "RS Percentile", "52WKH", "52WKL", "EarningDate"
]
DAY_COLS = [f"E_Day{i}" for i in range(1, 7)]

PRICE_CACHE = {}

def next_trading_day(start_date, days_ahead: int):
    current = start_date
    trading_days_found = 0
    while trading_days_found < days_ahead:
        current += timedelta(days=1)
        if current.weekday() < 5:
            trading_days_found += 1
    return current

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

    # Numeric conversion
    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile", 
                "Rank", "ATR", "ADR"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def get_price_map(target_date):
    if target_date in PRICE_CACHE:
        return PRICE_CACHE[target_date]

    print(f"[DEBUG] Looking for prices on/near {target_date.strftime('%Y-%m-%d')}")

    current_date = target_date
    max_lookback = 7
    for _ in range(max_lookback + 1):
        file_name = f"rs_stocks_{current_date.strftime('%m%d%Y')}.csv"
        file_path = ARCHIVE_DIR / file_name

        if file_path.exists():
            print(f"[INFO] Using data from {current_date.strftime('%Y-%m-%d')}")
            df = read_source(file_path)
            price_col = "Close" if "Close" in df.columns else "Price"
            df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
            price_dict = df.set_index("Ticker")[price_col].to_dict()
            PRICE_CACHE[target_date] = price_dict
            return price_dict

        current_date = current_date - timedelta(days=1)

    print(f"[WARNING] No price data found for {target_date}")
    PRICE_CACHE[target_date] = {}
    return {}

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

        has_earning = pd.notna(df_today.get("EarningDate", pd.Series([]))) & \
                     (df_today.get("EarningDate", pd.Series([])).astype(str).str.strip() != "")

        price_series = df_today["Price"].fillna(-1)

        def is_above_sma(sma_col):
            if sma_col not in df_today.columns:
                return pd.Series(True, index=df_today.index)
            sma_series = df_today[sma_col].fillna(-999999)
            return price_series > sma_series

        above_sma200 = is_above_sma("SMA200")
        above_sma30w = is_above_sma("SMA30W")
        tech_filter = above_sma200 & above_sma30w

        # ====================== ATR & ADR FILTER ======================
        def passes_atr_adr(row):
            if row.get("Sector") == "ETF" or pd.isna(row.get("Sector")):
                return True
            atr = row.get("ATR")
            adr = row.get("ADR")
            if pd.isna(atr) or pd.isna(adr):
                return False
            return atr >= MIN_ATR and adr >= MIN_ADR

        atr_adr_mask = df_today.apply(passes_atr_adr, axis=1)
        # ============================================================

        final_filter = (has_earning | tech_filter) & atr_adr_mask

        df_candidates = df_today[final_filter].copy()
        print(f"[DEBUG] After filter (earnings or technical + ATR/ADR > 3): {len(df_candidates)}")

        if "EarningDate" in df_candidates.columns:
            df_candidates = df_candidates[df_candidates["EarningDate"].notna()].copy()

        df_candidates = df_candidates[BASE_COLS].copy()
        df_candidates = df_candidates.rename(columns={"EarningDate": "Earning_Date"})
        df_candidates["Earning_Date"] = pd.to_datetime(df_candidates["Earning_Date"], errors="coerce")
        df_candidates["Ticker"] = df_candidates["Ticker"].apply(normalize_ticker)

        # === REST OF YOUR ORIGINAL CODE STARTS HERE (UNCHANGED) ===
        if out_path.exists():
            df_existing = pd.read_csv(out_path)
            df_existing["Earning_Date"] = pd.to_datetime(df_existing["Earning_Date"], errors="coerce")
            df_existing["Ticker"] = df_existing["Ticker"].apply(normalize_ticker)
            print(f"[INFO] Loaded existing file: {len(df_existing)} rows")
        else:
            df_existing = pd.DataFrame(columns=list(df_candidates.columns) + DAY_COLS)
            print("[INFO] Creating new monthly earnings file")

        updated_count = 0

        print("\n[STEP] Updating existing records (Trading Days)...")

        for idx, row in df_existing.iterrows():
            ticker = normalize_ticker(row["Ticker"])
            earn_date = pd.to_datetime(row["Earning_Date"], errors="coerce")
            if pd.isna(earn_date):
                continue
            earn_date = earn_date.date()

            for i in range(1, 7):
                col = f"E_Day{i}"
                if col not in df_existing.columns:
                    continue
                if not is_missing(row[col]):
                    continue

                target_date = next_trading_day(earn_date, i)
                if target_date > run_date:
                    continue

                price_map = get_price_map(target_date)
                price = price_map.get(ticker, pd.NA)
                df_existing.at[idx, col] = price
                updated_count += 1

        records_to_add = []

        for _, row in df_candidates.iterrows():
            ticker = normalize_ticker(row["Ticker"])
            earn_date = row["Earning_Date"].date()

            mask = (
                df_existing["Ticker"].eq(ticker) &
                (pd.to_datetime(df_existing["Earning_Date"]).dt.normalize() == pd.to_datetime(earn_date))
            )

            if mask.any():
                continue

            new_row = row.copy()
            for col in DAY_COLS:
                new_row[col] = pd.NA
                
            for i in range(1, 7):
                target_date = next_trading_day(earn_date, i)
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

        print(f"\n[SUCCESS] Updated {updated_count} fields | Total records: {len(final_df)}")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())
        
if __name__ == "__main__":
    main()
