#!/usr/bin/env python3
import os
import arcticdb as adb
import argparse
from glob import glob

def merge_arcticdb(source_root, dest_path):
    src_dirs = glob(os.path.join(source_root, "arctic-db-*"))
    dest_uri = f"lmdb://{dest_path}"
    os.makedirs(dest_path, exist_ok=True)

    arctic = adb.Arctic(dest_uri)
    if not arctic.has_library("prices"):
        arctic.create_library("prices")
    dest_lib = arctic.get_library("prices")

    for src in src_dirs:
        src_uri = f"lmdb://{src}"
        try:
            src_arctic = adb.Arctic(src_uri)
            if not src_arctic.has_library("prices"):
                print(f"⚠️ No 'prices' library in {src}")
                continue
            src_lib = src_arctic.get_library("prices")
            for symbol in src_lib.list_symbols():
                df = src_lib.read(symbol).data
                dest_lib.write(symbol, df)
                print(f"✅ Merged {symbol} from {src}")
        except Exception as e:
            print(f"❌ Failed merging from {src}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", required=True, help="Path containing partitioned arctic-db-* folders")
    parser.add_argument("--dest-path", required=True, help="Destination path for merged ArcticDB")
    args = parser.parse_args()

    merge_arcticdb(args.source_root, args.dest_path)
