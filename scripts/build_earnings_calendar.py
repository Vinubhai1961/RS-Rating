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
        # Fallback to latest available file
        files = sorted(ARCHIVE_DIR.glob("rs_stocks_*.csv"), reverse=True)
        if files:
            file_path = files[0]
            print(f"[WARNING] Today's file not found. Using latest: {file_path.name}")
        else:
            raise FileNotFoundError(f"No rs_stocks files found in {ARCHIVE_DIR}")
    
    print(f"[INFO] Using source file: {file_path.name}")
    return file_path


def read_source(path: Path):
    """Read and clean source file"""
    print(f"[DEBUG] Reading file: {path.name}")
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)
    print(f"[DEBUG] Original columns: {list(df.columns)}")
    
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


def get_price_map(target_date):
    """Get price dictionary for a specific date"""
    file_path = ARCHIVE_DIR / f"rs_stocks_{target_date.strftime('%m%d%Y')}.csv"
    if not file_path.exists():
        print(f"[WARNING] Price file missing: {file_path.name}")
        return {}

    try:
        df = read_source(file_path)
        price_col = "Close" if "Close" in df.columns else "Price"
        if price_col in df.columns:
            price_dict = df.set_index("Ticker")[price_col].to_dict()
            print(f"[DEBUG] Loaded {len(price_dict)} prices for {target_date.date()}")
            return price_dict
    except Exception as e:
        print(f"[ERROR] Failed to load prices for {target_date.date()}: {e}")
    
    return {}


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def main():
    try:
        TODAY_SOURCE = get_today_source()
        run_date = datetime.now().date()
        out_path = month_output_path(run_date)

        print(f"[INFO] Run Date: {run_date} | Output File: {out_path.name}")

        # Load and filter today's data
        df_today = read_source(TODAY_SOURCE)
        print(f"[DEBUG] Total rows loaded: {len(df_today)}")

        # Technical filter
        tech_filter = (
            (df_today["Price"].fillna(-1) > df_today.get("SMA200", float("inf")).fillna(float("inf"))) &
            (df_today["Price"].fillna(-1) > df_today.get("SMA30W", float("inf")).fillna(float("inf")))
        )
        df_candidates = df_today[tech_filter].copy()
        print(f"[DEBUG] After technical filter: {len(df_candidates)} rows")

        if "EarningDate" in df_candidates.columns:
            df_candidates = df_candidates[df_candidates["EarningDate"].notna()].copy()
            print(f"[DEBUG] After earnings date filter: {len(df_candidates)} rows")

        if len(df_candidates) == 0:
            print("[INFO] No earnings candidates today.")
            return

        # Prepare candidates
        df_candidates = df_candidates[BASE_COLS].copy()
        df_candidates = df_candidates.rename(columns={"EarningDate": "Earning_Date"})
        df_candidates["Earning_Date"] = pd.to_datetime(df_candidates["Earning_Date"], errors="coerce")

        # Load existing file
        if out_path.exists():
            df_existing = pd.read_csv(out_path)
            df_existing["Earning_Date"] = pd.to_datetime(df_existing["Earning_Date"], errors="coerce")
            print(f"[INFO] Loaded existing file with {len(df_existing)} records")
        else:
            df_existing = pd.DataFrame(columns=list(df_candidates.columns) + DAY_COLS)
            print("[INFO] Creating new monthly earnings file")

        # Process records
        records_to_add = []
        updated_count = 0

        for _, row in df_candidates.iterrows():
            ticker = row["Ticker"]
            earn_date = row["Earning_Date"].date()
            
            # Check if record already exists
            mask = (df_existing["Ticker"] == ticker) & (df_existing["Earning_Date"].dt.date == earn_date)
            
            if mask.any():
                # Update missing E_Day columns
                idx = df_existing[mask].index[0]
                for i in range(1, 7):
                    col = f"E_Day{i}"
                    if col in df_existing.columns and pd.isna(df_existing.at[idx, col]):
                        target_date = earn_date + timedelta(days=i)
                        price_map = get_price_map(target_date)
                        price = price_map.get(ticker, pd.NA)
                        df_existing.at[idx, col] = price
                        updated_count += 1
                        print(f"[UPDATE] Filled {col} for {ticker} (Earning: {earn_date})")
            else:
                # New record
                new_row = row.copy()
                for col in DAY_COLS:
                    new_row[col] = pd.NA
                
                # Fill E_Days for new record
                for i in range(1, 7):
                    target_date = earn_date + timedelta(days=i)
                    price_map = get_price_map(target_date)
                    new_row[f"E_Day{i}"] = price_map.get(ticker, pd.NA)
                
                records_to_add.append(new_row)
                print(f"[NEW] Added new earning: {ticker} on {earn_date}")

        # Combine data
        if records_to_add:
            df_new = pd.DataFrame(records_to_add)
            final_df = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            final_df = df_existing.copy()

        # Final formatting and save
        final_cols = ["Rank", "Ticker", "Price", "Sector", "Industry",
                     "RS Percentile", "52WKH", "52WKL", "Earning_Date"] + DAY_COLS
        final_df = final_df[final_cols].copy()
        
        final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")

        final_df.to_csv(out_path, index=False)
        
        action = f"Added {len(records_to_add)} new | Updated {updated_count} fields"
        print(f"[SUCCESS] {action} | Total records: {len(final_df)} → {out_path.name}")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
