import pandas as pd
from pathlib import Path
from datetime import datetime

# ================= CONFIG =================
SOURCE_CSV = "RS_Data/rs_stocks.csv"
IPO_DIR = Path("IPO")
# ==========================================

def main():
    IPO_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(SOURCE_CSV)

    if "IPO" not in df.columns:
        raise ValueError("Source CSV missing required column: IPO")

    today = datetime.now()
    year = today.year
    ipo_file = IPO_DIR / f"IPO_{year}.csv"

    # Keep rows where IPO column is marked YES
    ipo_df = df[df["IPO"].astype(str).str.strip().str.upper().eq("YES")].copy()

    if ipo_df.empty:
        print("No IPO rows found today.")
        return

    # Add IPO DATE only if not already present
    if "IPO DATE" not in ipo_df.columns:
        ipo_df["IPO DATE"] = today.strftime("%m/%d/%Y")

    # If yearly file exists, append/update without duplicates
    if ipo_file.exists():
        old_df = pd.read_csv(ipo_file)

        combined = pd.concat([old_df, ipo_df], ignore_index=True)

        # Keep latest row per ticker, but preserve all source columns
        if "Ticker" in combined.columns:
            combined = combined.drop_duplicates(subset=["Ticker"], keep="last")
        else:
            combined = combined.drop_duplicates(keep="last")
    else:
        combined = ipo_df

    combined.to_csv(ipo_file, index=False)
    print(f"IPO file updated: {ipo_file}")
    print(f"IPO rows saved: {len(combined)}")

if __name__ == "__main__":
    main()
