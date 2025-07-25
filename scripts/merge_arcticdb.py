#!/usr/bin/env python3
import os
import arcticdb as adb
import argparse
from glob import glob

def merge_arcticdb(source_root, dest_path):
    src_dirs = glob(os.path.join(source_root, "arctic-db-*"))
    if not src_dirs:
        print(f"❌ No source directories found at {source_root}/arctic-db-*")
        return

    dest_uri = f"lmdb://{dest_path}"
    os.makedirs(dest_path, exist_ok=True)

    arctic = adb.Arctic(dest_uri)
    if not arctic.has_library("prices"):
        arctic.create_library("prices")
    dest_lib = arctic.get_library("prices")

    total_merged = 0
    all_symbols = set()
    duplicate_symbols = set()
    failed_symbols = []

    print(f"🔀 Starting merge into: {dest_path}")
    print(f"📦 Found {len(src_dirs)} source shards")

    for src in sorted(src_dirs):
        shard_merged = 0
        try:
            src_uri = f"lmdb://{src}"
            src_arctic = adb.Arctic(src_uri)

            if not src_arctic.has_library("prices"):
                print(f"⚠️ Skipping {src}: no 'prices' library found")
                continue

            src_lib = src_arctic.get_library("prices")
            symbols = src_lib.list_symbols()
            print(f"🔍 {src}: {len(symbols)} symbols found - Sample: {symbols[:5] if symbols else []}")

            for symbol in symbols:
                try:
                    if symbol in all_symbols:
                        duplicate_symbols.add(symbol)
                    all_symbols.add(symbol)
                    df = src_lib.read(symbol).data
                    if not {"close", "datetime"}.issubset(df.columns):
                        print(f"  ❌ {symbol}: Missing required columns {set(['close', 'datetime']) - set(df.columns)}")
                        failed_symbols.append((src, symbol, "Missing required columns"))
                        continue
                    dest_lib.write(symbol, df)
                    print(f"  ✅ {symbol}")
                    total_merged += 1
                    shard_merged += 1
                except Exception as sym_err:
                    print(f"  ❌ Failed to merge {symbol}: {sym_err}")
                    failed_symbols.append((src, symbol, str(sym_err)))

        except Exception as e:
            print(f"❌ Error processing {src}: {e}")
            failed_symbols.append((src, "N/A", str(e)))

        print(f"✅ Done with {src}: {shard_merged} symbols merged\n")

    # Log duplicates and failures
    if duplicate_symbols:
        print(f"⚠️ Found {len(duplicate_symbols)} duplicate symbols across shards: {list(duplicate_symbols)[:10]}")
    if failed_symbols:
        print(f"⚠️ {len(failed_symbols)} symbols failed to merge. Writing to {os.path.join(dest_path, 'merge_failed_symbols.log')}")
        with open(os.path.join(dest_path, "merge_failed_symbols.log"), "w") as f:
            f.write("Source,Symbol,Error\n")
            for src, symbol, err in failed_symbols:
                f.write(f"{src},{symbol},{err}\n")

    print("🧾 Merge summary:")
    print(f"  - Source partitions: {len(src_dirs)}")
    print(f"  - Total unique symbols found: {len(all_symbols)}")
    print(f"  - Total symbols merged: {total_merged}")
    print(f"  - Duplicate symbols: {len(duplicate_symbols)}")
    print(f"  - Failed symbols: {len(failed_symbols)}")
    print(f"  - Merged ArcticDB location: {dest_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge ArcticDB partitioned shards into a single DB")
    parser.add_argument("--source-root", required=True, help="Path containing arctic-db-* folders")
    parser.add_argument("--dest-path", required=True, help="Destination path for merged ArcticDB")

    args = parser.parse_args()
    merge_arcticdb(args.source_root, args.dest_path)
