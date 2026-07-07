import pandas as pd
from pandas.errors import EmptyDataError
from pathlib import Path
from datetime import datetime

# ================= CONFIG =================
SOURCE_CSV = "RS_Data/rs_stocks.csv"
IPO_DIR = Path("IPO")
# ==========================================


def read_existing_ipo_file(ipo_file: Path, columns) -> pd.DataFrame:
    if not ipo_file.exists() or ipo_file.stat().st_size == 0:
        return pd.DataFrame(columns=columns)

    try:
        return pd.read_csv(ipo_file)
    except EmptyDataError:
        return pd.DataFrame(columns=columns)


def main():
    IPO_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now()
    year = today.year
    today_str = today.strftime("%m/%d/%Y")

    ipo_file = IPO_DIR / f"IPO_{year}.csv"

    df = pd.read_csv(SOURCE_CSV)

    if "IPO" not in df.columns:
        raise ValueError("Source CSV missing required column: IPO")

    if "Ticker" not in df.columns:
        raise ValueError("Source CSV missing required column: Ticker")

    # Keep all source columns exactly as-is
    ipo_df = df[df["IPO"].astype(str).str.strip().str.upper().eq("YES")].copy()

    if ipo_df.empty:
        print("No IPO rows found today.")
        return

    # Add IPO DATE only for newly discovered IPO tickers
    if "IPO DATE" not in ipo_df.columns:
        ipo_df["IPO DATE"] = today_str

    old_df = read_existing_ipo_file(ipo_file, ipo_df.columns)

    # Make sure old file has any new columns added later in source CSV
    for col in ipo_df.columns:
        if col not in old_df.columns:
            old_df[col] = ""

    for col in old_df.columns:
        if col not in ipo_df.columns:
            ipo_df[col] = ""

    old_df = old_df[ipo_df.columns]

    existing_tickers = set(
        old_df["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Append only brand-new IPO tickers
    new_rows = ipo_df[
        ~ipo_df["Ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
        .isin(existing_tickers)
    ].copy()

    if new_rows.empty:
        print("No new IPO tickers to add.")
        print(f"IPO file unchanged: {ipo_file}")
        print(f"Existing IPO rows: {len(old_df)}")
        return

    combined = pd.concat([old_df, new_rows], ignore_index=True)

    combined.to_csv(ipo_file, index=False)

    print(f"IPO file updated: {ipo_file}")
    print(f"Existing IPO rows: {len(old_df)}")
    print(f"New IPO rows added: {len(new_rows)}")
    print(f"Total IPO rows saved: {len(combined)}")


if __name__ == "__main__":
    main()
