#!/usr/bin/env python3
import json
import glob
import os

OUTPUT_FILE = "data/ticker_info.json"

def quality(info):
    s = info.get("sector", "").lower()
    i = info.get("industry", "").lower()
    return int(s not in ("", "n/a", "unknown") and i not in ("", "n/a", "unknown"))

def merge_ticker_info():
    merged = {}
    files = glob.glob("artifacts/**/ticker_info.json", recursive=True)
    if not files:
        print("No ticker_info.json parts found!")
        return

    for p in files:
        with open(p, "r") as f:
            part_data = json.load(f)
        for sym, rec in part_data.items():
            if sym not in merged or quality(rec["info"]) > quality(merged[sym]["info"]):
                merged[sym] = rec

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(merged, f, indent=2, sort_keys=True)

    print(f"Merged {len(files)} files into {OUTPUT_FILE}")
    print(f"Total entries: {len(merged)}")

if __name__ == "__main__":
    merge_ticker_info()
