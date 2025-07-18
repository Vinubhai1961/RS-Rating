#!/usr/bin/env python
"""
Author: Dipen Patel
RS-Rating: Fetch NASDAQ symbols -> Sector, Industry, ETF flag, Latest Price.

"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple

import requests
import yfinance as yf
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # Graceful fallback if tqdm not installed
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else range(0)


# --------------------------------------------------------------------------------------
# Constants / Paths
# --------------------------------------------------------------------------------------
NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(REPO_ROOT, "data")
LOG_DIR = os.path.join(REPO_ROOT, "log")
OUTPUT_JSON = os.path.join(DATA_DIR, "ticker_info.json")
ERROR_LOG = os.path.join(LOG_DIR, "error.log")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
def log_error(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG, "a", encoding="utf-8") as fh:
        fh.write(f"{ts} UTC - {msg}\n")


# --------------------------------------------------------------------------------------
# NASDAQ Symbol File Parsing
# --------------------------------------------------------------------------------------
def download_nasdaq_file(url: str = NASDAQ_URL) -> List[str]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text.splitlines()


def parse_nasdaq_lines(
    lines: List[str],
    include_test_issues: bool = False,
    include_financial_distressed: bool = False,
) -> List[Dict[str, str]]:
    """
    Parse nasdaqtraded.txt lines.

    Returns list of dict rows with keys:
      symbol, etf, test_issue, financial_status
    """
    if not lines:
        return []

    header = lines[0].split("|")
    # Map header -> index
    idx = {name: i for i, name in enumerate(header)}

    required_cols = ["Symbol", "ETF", "Test Issue", "Financial Status", "Nasdaq Traded"]
    for col in required_cols:
        if col not in idx:
            raise RuntimeError(f"Missing expected column '{col}' in NASDAQ file header.")

    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("|")
        # Skip trailing footer row: "File Creation Time"
        if parts[0].startswith("File Creation Time"):
            continue

        try:
            nasdaq_traded = parts[idx["Nasdaq Traded"]].strip()
            symbol = parts[idx["Symbol"]].strip()
            etf_flag = parts[idx["ETF"]].strip()
            test_issue = parts[idx["Test Issue"]].strip()
            fin_stat = parts[idx["Financial Status"]].strip()
        except Exception as exc:  # malformed row
            log_error(f"Parse error for line: {line[:80]}... ({exc})")
            continue

        if nasdaq_traded != "Y":
            # Not actively traded on a UTP plan; skip quietly
            continue

        if not include_test_issues and test_issue.upper() == "Y":
            log_error(f"Skip test issue: {symbol}")
            continue

        if not include_financial_distressed and fin_stat.upper() == "D":
            # 'D' = Deficient; 'E' = Delinquent; etc. We only drop D by default.
            log_error(f"Skip financial distressed (D): {symbol}")
            continue

        if not symbol:
            log_error("Skip empty symbol row.")
            continue

        rows.append(
            {
                "symbol": symbol,
                "etf": etf_flag.upper() if etf_flag else "N",
                "test_issue": test_issue.upper(),
                "financial_status": fin_stat.upper(),
            }
        )
    return rows


# --------------------------------------------------------------------------------------
# Symbol Filtering for Yahoo
# --------------------------------------------------------------------------------------
NONSTANDARD_PATTERN = re.compile(r"[^A-Z0-9]")  # anything not alnum uppercase triggers nonstandard

def filter_symbols_for_yahoo(
    rows: List[Dict[str, str]],
    include_nonstandard: bool = False,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Split into (clean_rows, skipped_rows). If include_nonstandard, return all as clean.
    """
    if include_nonstandard:
        return rows, []

    clean = []
    skipped = []
    for r in rows:
        sym = r["symbol"]
        # Symbols on NASDAQ feed are uppercase; if not, normalise
        sym_up = sym.upper()
        r["symbol"] = sym_up

        if NONSTANDARD_PATTERN.search(sym_up):
            # Contains chars Yahoo typically can't resolve directly (., $, ^, -, / etc.)
            skipped.append({**r, "reason": "nonstandard-symbol"})
            continue

        # Some NASDAQ entries include 5+ letters; Yahoo supports many, so we allow length.
        clean.append(r)

    return clean, skipped


# --------------------------------------------------------------------------------------
# Price Fetching (Batch)
# --------------------------------------------------------------------------------------
def chunker(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def fetch_prices_batch(
    symbols: List[str],
    chunk_size: int = 200,
    pause: float = 0.1,
) -> Dict[str, float]:
    """
    Use yf.download in chunks to get last close price. Returns dict symbol->price(float) or None.
    """
    prices: Dict[str, float] = {}
    for chunk in chunker(symbols, chunk_size):
        try:
            data = yf.download(
                tickers=" ".join(chunk),
                period="1d",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            log_error(f"Batch price download error (chunk len {len(chunk)}): {exc}")
            # fall back to per-symbol on this chunk
            for sym in chunk:
                prices[sym] = _fetch_price_single(sym)
            continue

        # yf.download format differs when 1 ticker vs many; handle both
        if isinstance(data.columns, pd.MultiIndex):
            # MultiIndex: top level symbols
            for sym in chunk:
                if sym in data.columns.levels[0]:
                    try:
                        close_series = data[sym]["Close"].dropna()
                        if not close_series.empty:
                            prices[sym] = float(close_series.iloc[-1])
                        else:
                            prices[sym] = _fetch_price_single(sym)
                    except Exception:
                        prices[sym] = _fetch_price_single(sym)
                else:
                    prices[sym] = _fetch_price_single(sym)
        else:
            # Single combined frame (only one ticker)
            try:
                close_series = data["Close"].dropna()
                if not close_series.empty:
                    prices[chunk[0]] = float(close_series.iloc[-1])
                else:
                    prices[chunk[0]] = _fetch_price_single(chunk[0])
            except Exception:
                prices[chunk[0]] = _fetch_price_single(chunk[0])

        time.sleep(pause)

    return prices


def _fetch_price_single(sym: str) -> float:
    """
    Slower fallback. Attempts fast_info then 1d history. Returns None if not found.
    """
    try:
        t = yf.Ticker(sym)
        try:
            p = t.fast_info.last_price
            if p is not None:
                return float(p)
        except Exception:
            pass

        hist = t.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as exc:
        log_error(f"Single price fetch error {sym}: {exc}")
    return None


# --------------------------------------------------------------------------------------
# Sector / Industry Metadata (Per-Symbol w/ retry)
# --------------------------------------------------------------------------------------
def fetch_metadata(sym: str, is_etf: bool, retries: int = 3, pause: float = 0.5) -> Dict[str, str]:
    """
    Fetch sector & industry for a symbol. Force N/A if ETF.
    """
    if is_etf:
        return {"Sector": "N/A", "Industry": "N/A"}

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            t = yf.Ticker(sym)
            info = t.get_info()
            sector = info.get("sector", "N/A")
            industry = info.get("industry", "N/A")
            if not sector:
                sector = "N/A"
            if not industry:
                industry = "N/A"
            return {"Sector": sector, "Industry": industry}
        except Exception as exc:
            last_exc = exc
            time.sleep(pause * attempt)  # simple backoff

    log_error(f"Metadata fetch failed {sym}: {last_exc}")
    return {"Sector": "Error", "Industry": "Error"}


# --------------------------------------------------------------------------------------
# Full Run
# --------------------------------------------------------------------------------------
def run(
    include_test_issues: bool = False,
    include_financial_distressed: bool = False,
    include_nonstandard: bool = False,
    limit: int = 0,
    max_workers: int = 16,
    chunk_size: int = 200,
    no_progress: bool = False,
) -> Dict[str, Dict[str, str]]:
    """
    Orchestrate full pipeline. Returns results dict.
    """
    # --- Download & parse NASDAQ file ---
    lines = download_nasdaq_file()
    all_rows = parse_nasdaq_lines(
        lines,
        include_test_issues=include_test_issues,
        include_financial_distressed=include_financial_distressed,
    )

    clean_rows, skipped_rows = filter_symbols_for_yahoo(
        all_rows,
        include_nonstandard=include_nonstandard,
    )

    for sr in skipped_rows:
        log_error(f"Skip nonstandard symbol: {sr['symbol']} ({sr['reason']})")

    if limit and limit > 0:
        clean_rows = clean_rows[:limit]

    symbols = [r["symbol"] for r in clean_rows]

    print(f"Symbols after filtering: {len(symbols)}")

    # --- Price batch fetch ---
    print("Fetching prices (batched)...")
    prices = fetch_prices_batch(symbols, chunk_size=chunk_size)

    # --- Metadata fetch (parallel) ---
    print("Fetching metadata (sector/industry)...")
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: Dict[str, Dict[str, str]] = {}

    iterator = (
        as_completed(
            {
                executor.submit(fetch_metadata, r["symbol"], (r["etf"] == "Y")): r["symbol"]
                for r in clean_rows
            }
        )
        # We'll populate executor below; hacky placeholder; real code below
    )

    # Correctly structure concurrency:
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_metadata, r["symbol"], (r["etf"] == "Y")): r for r in clean_rows
        }

        iterable = as_completed(future_map)
        if not no_progress:
            iterable = tqdm(iterable, total=len(future_map), desc="Metadata")

        for fut in iterable:
            row = future_map[fut]
            sym = row["symbol"]
            meta = fut.result()
            is_etf = row["etf"] == "Y"
            price = prices.get(sym)
            # convert None to "N/A" for JSON cleanliness
            price_out = round(price, 4) if isinstance(price, (int, float)) else "N/A"
            results[sym] = {
                "Sector": meta["Sector"] if not is_etf else "N/A",
                "Industry": meta["Industry"] if not is_etf else "N/A",
                "ETF": row["etf"],
                "Price": price_out,
            }

    # --- Save ---
    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=4, sort_keys=True)

    # --- Summary ---
    n_etf = sum(1 for r in clean_rows if r["etf"] == "Y")
    n_missing_price = sum(1 for s in symbols if prices.get(s) is None)
    print("----- Summary -----")
    print(f"Total NASDAQ rows parsed: {len(all_rows)}")
    print(f"Skipped nonstandard symbols: {len(skipped_rows)}")
    print(f"Symbols processed: {len(symbols)} (ETFs: {n_etf})")
    print(f"Missing price count: {n_missing_price}")
    print(f"Output: {OUTPUT_JSON}")
    print(f"Errors logged: {ERROR_LOG}")

    return results


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch NASDAQ ticker sector/industry/price data.")
    p.add_argument("--include-test-issues", action="store_true", help="Include NASDAQ test issues.")
    p.add_argument(
        "--include-financial-distressed",
        action="store_true",
        help="Include Financial Status = D (deficient) symbols.",
    )
    p.add_argument(
        "--include-nonstandard",
        action="store_true",
        help="Include symbols with punctuation ($, ., /, etc.) that often fail in Yahoo.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only first N symbols (debug). 0 = all.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Threads for metadata fetch.",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=200,
        help="Batch size for price download.",
    )
    p.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar (useful in some CI logs).",
    )
    return p


def main():
    args = build_arg_parser().parse_args()
    run(
        include_test_issues=args.include_test_issues,
        include_financial_distressed=args.include_financial_distressed,
        include_nonstandard=args.include_nonstandard,
        limit=args.limit,
        max_workers=args.max_workers,
        chunk_size=args.chunk_size,
        no_progress=args.no_progress,
    )


if __name__ == "__main__":
    main()
