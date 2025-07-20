#!/usr/bin/env python3
import json
import os
from collections import Counter

FILE_PATH = "data/ticker_info.json"

def verify_ticker_info():
    if not os.path.exists(FILE_PATH):
        print(f"❌ ERROR: {FILE_PATH} not found.")
        return 1

    with open(FILE_PATH, "r") as f:
        data = json.load(f)

    total = len(data)
    unresolved = [sym for sym, rec in data.items()
                  if rec.get("info", {}).get("sector", "").lower() in ("", "n/a", "unknown")
                  or rec.get("info", {}).get("industry", "").lower() in ("", "n/a", "unknown")]

    unresolved_count = len(unresolved)
    resolved_count = total - unresolved_count

    print("=== Ticker Info Verification ===")
    print(f"Total entries   : {total}")
    print(f"Resolved entries: {resolved_count}")
    print(f"Unresolved      : {unresolved_count}")

    # Optional: show top 10 unresolved
    if unresolved:
        print("\nSample unresolved tickers:")
        print(", ".join(unresolved[:10]))

    # Sector distribution (optional)
    sector_counts = Counter(rec.get("info", {}).get("sector", "n/a") for rec in data.values())
    print("\nSector distribution (top 10):")
    for sector, count in sector_counts.most_common(10):
        print(f"  {sector}: {count}")

    # Return non-zero exit code if too many unresolved
    if unresolved_count > 0.5 * total:
        print("❌ WARNING: More than 50% unresolved.")
        return 1

    return 0

if __name__ == "__main__":
    exit(verify_ticker_info())
