from pathlib import Path
from datetime import datetime, timedelta, date
import re
import pandas as pd
import numpy as np
import traceback

BASE_DIR = Path(".")
ARCHIVE_DIR = BASE_DIR / "archive"
OUTPUT_DIR = BASE_DIR / "Earnings"

# ====================== ATR & ADR CONFIG ======================
MIN_ATR = 2.0
MIN_ADR = 2.0
# ================================================================

BASE_COLS = [
    "Rank", "Ticker", "Price", "Sector", "Industry",
    "RS Percentile", "52WKH", "52WKL", "EarningDate", "ATR"
]
DAY_COLS = [f"E_Day{i}" for i in range(1, 7)]

FINAL_COLS = [
    "Rank",
    "Ticker",
    "Price",
    "Sector",
    "Industry",
    "RS Percentile",
    "ATR",
    "52WKH",
    "52WKL",
    "Earning_Date"
] + DAY_COLS

PRICE_CACHE = {}
TRADING_DATE_CACHE = None


def parse_archive_date(path: Path):
    """Parse rs_stocks_MMDDYYYY.csv or rs_stocks_MM-DD-YYYY.csv."""
    name = path.name

    match = re.fullmatch(r"rs_stocks_(\d{2})(\d{2})(\d{4})\.csv", name)
    if match:
        mm, dd, yyyy = match.groups()
        return date(int(yyyy), int(mm), int(dd))

    match = re.fullmatch(r"rs_stocks_(\d{2})-(\d{2})-(\d{4})\.csv", name)
    if match:
        mm, dd, yyyy = match.groups()
        return date(int(yyyy), int(mm), int(dd))

    return None


def get_archive_path(target_date):
    """Return the exact archive path for a trading date, supporting both filename styles."""
    candidates = [
        ARCHIVE_DIR / f"rs_stocks_{target_date.strftime('%m%d%Y')}.csv",
        ARCHIVE_DIR / f"rs_stocks_{target_date.strftime('%m-%d-%Y')}.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def get_available_trading_dates():
    """Use actual archive snapshots as the authoritative list of completed trading sessions."""
    global TRADING_DATE_CACHE
    if TRADING_DATE_CACHE is None:
        dates = {
            parsed
            for path in ARCHIVE_DIR.glob("rs_stocks_*.csv")
            if (parsed := parse_archive_date(path)) is not None
        }
        TRADING_DATE_CACHE = sorted(dates)
        print(f"[INFO] Trading sessions found in archive: {len(TRADING_DATE_CACHE)}")
    return TRADING_DATE_CACHE


def next_trading_day(start_date, days_ahead: int):
    """Return the Nth actual archived trading session strictly after start_date."""
    sessions = [d for d in get_available_trading_dates() if d > start_date]
    if len(sessions) < days_ahead:
        return None
    return sessions[days_ahead - 1]


def normalize_ticker(val):
    return str(val).strip().upper()


def is_missing(val):
    return pd.isna(val) or val == "" or str(val).strip().lower() == "nan"


def get_today_source():
    today = datetime.now().date()
    file_path = get_archive_path(today)

    if file_path is None:
        dated_files = []
        for path in ARCHIVE_DIR.glob("rs_stocks_*.csv"):
            file_date = parse_archive_date(path)
            if file_date is not None:
                dated_files.append((file_date, path))

        if dated_files:
            dated_files.sort(key=lambda item: item[0])
            file_path = dated_files[-1][1]
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

    for col in ["Price", "SMA200", "SMA30W", "52WKH", "52WKL", "RS Percentile",
                "Rank", "ATR", "ADR"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_price_map(target_date):
    if target_date in PRICE_CACHE:
        return PRICE_CACHE[target_date]

    print(f"[DEBUG] Looking for exact prices on {target_date.strftime('%Y-%m-%d')}")

    file_path = get_archive_path(target_date)

    if file_path is None:
        print(f"[WARNING] Missing archive file for trading session: {target_date:%Y-%m-%d}")
        PRICE_CACHE[target_date] = {}
        return {}

    print(f"[INFO] Using exact archive: {file_path.name}")

    df = read_source(file_path)
    price_col = "Close" if "Close" in df.columns else "Price"
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    price_dict = df.set_index("Ticker")[price_col].to_dict()
    PRICE_CACHE[target_date] = price_dict
    return price_dict


def month_output_path(run_date):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{run_date.strftime('%B_%Y')}_Earnings.csv"


def list_all_earnings_files():
    """Return all monthly earnings CSVs sorted by filename (oldest first)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(OUTPUT_DIR.glob("*_Earnings.csv"))
    # Exclude the validation report if it ever matches the pattern
    files = [f for f in files if f.name != "validation_fixes.csv"]
    return files


def fill_missing_eday_prices(df: pd.DataFrame, run_date: date, label: str = "") -> tuple:
    """
    Fill any blank E_Day1..E_Day6 columns using the archive.
    Returns (updated_df, filled_count).
    """
    if df.empty:
        return df, 0

    df = df.copy()
    # Ensure all day columns exist
    for col in DAY_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    filled = 0

    for idx, row in df.iterrows():
        ticker = normalize_ticker(row["Ticker"])
        earn_date = pd.to_datetime(row["Earning_Date"], errors="coerce")
        if pd.isna(earn_date):
            continue
        earn_date = earn_date.date()

        for i in range(1, 7):
            col = f"E_Day{i}"
            if not is_missing(row.get(col)):
                continue

            target_date = next_trading_day(earn_date, i)
            if target_date is None or target_date > run_date:
                continue

            price_map = get_price_map(target_date)
            price = price_map.get(ticker, pd.NA)
            if not is_missing(price):
                df.at[idx, col] = price
                filled += 1

    if label and filled:
        print(f"[INFO] {label}: filled {filled} missing E_Day prices")
    return df, filled


def validate_and_fix_eday_prices(df: pd.DataFrame, run_date: date) -> tuple:
    """
    Re-check every E_Day against the exact archive and correct mismatches / fill blanks.
    Returns (fixed_df, validation_issues list, fixed_count).
    """
    if df.empty:
        return df, [], 0

    df = df.copy()
    validation_issues = []
    fixed_count = 0

    for idx, row in df.iterrows():
        ticker = normalize_ticker(row["Ticker"])
        earn_date = pd.to_datetime(row["Earning_Date"], errors="coerce")
        if pd.isna(earn_date):
            continue
        earn_date = earn_date.date()

        for i in range(1, 7):
            col = f"E_Day{i}"
            if col not in df.columns:
                continue

            target_date = next_trading_day(earn_date, i)
            if target_date is None or target_date > run_date:
                continue

            file_path = get_archive_path(target_date)

            # exact archive required
            if file_path is None:
                if not is_missing(row.get(col)):
                    validation_issues.append({
                        "Ticker": ticker,
                        "Column": col,
                        "Issue": "Archive Missing",
                        "ExpectedDate": target_date,
                        "OldPrice": row.get(col),
                        "NewPrice": pd.NA
                    })
                    df.at[idx, col] = pd.NA
                    fixed_count += 1
                continue

            df_check = read_source(file_path)
            price_col = "Close" if "Close" in df_check.columns else "Price"
            df_check["Ticker"] = (
                df_check["Ticker"].astype(str).str.strip().str.upper()
            )
            match = df_check[df_check["Ticker"] == ticker]

            if match.empty:
                if not is_missing(row.get(col)):
                    validation_issues.append({
                        "Ticker": ticker,
                        "Column": col,
                        "Issue": "Ticker Missing",
                        "ExpectedDate": target_date,
                        "OldPrice": row.get(col),
                        "NewPrice": pd.NA
                    })
                    df.at[idx, col] = pd.NA
                    fixed_count += 1
                continue

            actual_price = match.iloc[0][price_col]
            if pd.isna(actual_price):
                continue

            recorded_price = row.get(col)

            # blank -> fill correct price
            if is_missing(recorded_price):
                df.at[idx, col] = actual_price
                validation_issues.append({
                    "Ticker": ticker,
                    "Column": col,
                    "Issue": "Filled Missing Price",
                    "ExpectedDate": target_date,
                    "OldPrice": pd.NA,
                    "NewPrice": actual_price
                })
                fixed_count += 1
                continue

            # mismatch -> fix
            if round(float(actual_price), 4) != round(float(recorded_price), 4):
                validation_issues.append({
                    "Ticker": ticker,
                    "Column": col,
                    "Issue": "Price Corrected",
                    "ExpectedDate": target_date,
                    "OldPrice": recorded_price,
                    "NewPrice": actual_price
                })
                df.at[idx, col] = actual_price
                fixed_count += 1

    return df, validation_issues, fixed_count


def process_prior_month_file(path: Path, run_date: date) -> int:
    """
    Open a previous-month earnings file, fill any still-missing E_Day prices,
    run validation, and save it back. Returns number of cells filled/fixed.
    """
    print(f"\n[STEP] Back-filling prior month file: {path.name}")

    df = pd.read_csv(path)
    df["Earning_Date"] = pd.to_datetime(df["Earning_Date"], errors="coerce")
    df["Ticker"] = df["Ticker"].apply(normalize_ticker)

    # Ensure day columns exist
    for col in DAY_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Fast pass: fill blanks from price cache
    df, filled = fill_missing_eday_prices(df, run_date, label=path.name)

    # Thorough pass: validate / correct against archives
    df, issues, fixed = validate_and_fix_eday_prices(df, run_date)

    total = filled + fixed
    if total > 0 or issues:
        # Keep only the standard columns
        available = [c for c in FINAL_COLS if c in df.columns]
        df = df[available].copy()
        df = df.sort_values(["Earning_Date", "Rank"], na_position="last")
        df.to_csv(path, index=False)
        print(f"[SUCCESS] {path.name}: filled/fixed {total} cells, saved.")
    else:
        print(f"[INFO] {path.name}: nothing left to fill.")

    return total


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
        print(f"[DEBUG] After filter (earnings or technical + ATR/ADR): {len(df_candidates)}")

        if "EarningDate" in df_candidates.columns:
            df_candidates = df_candidates[df_candidates["EarningDate"].notna()].copy()

        df_candidates = df_candidates[BASE_COLS].copy()
        df_candidates = df_candidates.rename(columns={"EarningDate": "Earning_Date"})
        df_candidates["Earning_Date"] = pd.to_datetime(df_candidates["Earning_Date"], errors="coerce")
        df_candidates["Ticker"] = df_candidates["Ticker"].apply(normalize_ticker)

        # ---------- CURRENT MONTH ----------
        if out_path.exists():
            df_existing = pd.read_csv(out_path)
            df_existing["Earning_Date"] = pd.to_datetime(df_existing["Earning_Date"], errors="coerce")
            df_existing["Ticker"] = df_existing["Ticker"].apply(normalize_ticker)
            print(f"[INFO] Loaded existing file: {len(df_existing)} rows")
        else:
            df_existing = pd.DataFrame(columns=list(df_candidates.columns) + DAY_COLS)
            print("[INFO] Creating new monthly earnings file")

        # Fill missing days on existing current-month rows
        print("\n[STEP] Updating existing records (Trading Days)...")
        df_existing, updated_count = fill_missing_eday_prices(
            df_existing, run_date, label="current month existing"
        )

        # Add brand-new candidates
        records_to_add = []
        for _, row in df_candidates.iterrows():
            ticker = normalize_ticker(row["Ticker"])
            earn_date = row["Earning_Date"].date()

            mask = (
                df_existing["Ticker"].eq(ticker) &
                (pd.to_datetime(df_existing["Earning_Date"]).dt.normalize() ==
                 pd.to_datetime(earn_date))
            )
            if mask.any():
                continue

            new_row = row.copy()
            for col in DAY_COLS:
                new_row[col] = pd.NA

            for i in range(1, 7):
                target_date = next_trading_day(earn_date, i)
                if target_date is None or target_date > run_date:
                    continue
                price_map = get_price_map(target_date)
                new_row[f"E_Day{i}"] = price_map.get(ticker, pd.NA)

            records_to_add.append(new_row)

        if records_to_add:
            df_new = pd.DataFrame(records_to_add)
            final_df = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            final_df = df_existing.copy()

        # ---------- ATR back-fill for old records ----------
        if "ATR" not in final_df.columns:
            final_df["ATR"] = np.nan
            print("[INFO] Added missing 'ATR' column for backward compatibility")

        if "ATR" in df_today.columns:
            latest_atr = df_today[["Ticker", "ATR"]].drop_duplicates("Ticker")
            latest_atr = latest_atr.set_index("Ticker")["ATR"]
            mask_nan = final_df["ATR"].isna()
            if mask_nan.any():
                final_df.loc[mask_nan, "ATR"] = (
                    final_df.loc[mask_nan, "Ticker"].map(latest_atr)
                )
                filled_count = mask_nan.sum() - final_df["ATR"].isna().sum()
                print(f"[INFO] Backfilled ATR for {filled_count} existing records")

        available_cols = [col for col in FINAL_COLS if col in final_df.columns]
        final_df = final_df[available_cols].copy()

        # ---------- VALIDATE CURRENT MONTH ----------
        print("\n[STEP] VALIDATING & FIXING E_DAY PRICES (current month)...")
        final_df, validation_issues, fixed_count = validate_and_fix_eday_prices(
            final_df, run_date
        )

        if validation_issues:
            df_issues = pd.DataFrame(validation_issues)
            validation_file = OUTPUT_DIR / "validation_fixes.csv"
            df_issues.to_csv(validation_file, index=False)
            print(f"[INFO] Validation report saved: {validation_file}")
            print(f"[INFO] Total fixes applied (current month): {fixed_count}")
        else:
            print("[SUCCESS] No validation issues found (current month)")

        final_df = final_df.sort_values(["Earning_Date", "Rank"], na_position="last")
        final_df.to_csv(out_path, index=False)

        print(
            f"\n[SUCCESS] Current month → Updated {updated_count} fields | "
            f"Auto-fixed {fixed_count} price issues | "
            f"Total records: {len(final_df)}"
        )

        # =========================================================
        # BACK-FILL ALL PRIOR MONTH FILES
        # (so July 7-30 / 7-31 earnings still get E_Day1..6 after we roll into August)
        # =========================================================
        print("\n" + "=" * 60)
        print("[STEP] Back-filling PRIOR month earnings files...")
        print("=" * 60)

        prior_total = 0
        for path in list_all_earnings_files():
            if path.resolve() == out_path.resolve():
                continue  # already handled above
            prior_total += process_prior_month_file(path, run_date)

        if prior_total:
            print(f"\n[SUCCESS] Prior months: filled/fixed {prior_total} cells in total.")
        else:
            print("\n[INFO] Prior months: nothing left to fill.")

    except Exception as e:
        print(f"[ERROR] Script failed: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
