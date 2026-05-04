#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import json
import os
import argparse
import logging
from yahooquery import Ticker
from tqdm import tqdm
from datetime import datetime
import time
import random

OUTPUT_DIR = "data"
TICKER_INFO_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
TICKER_PRICE_PART_FILE = os.path.join(OUTPUT_DIR, "ticker_price_part_%d.json")
UNRESOLVED_PRICE_TICKERS = os.path.join(OUTPUT_DIR, "unresolved_price_tickers.txt")
LOG_PATH = "logs/build_ticker_price.log"

BATCH_SIZE = 250
BATCH_DELAY_RANGE = (20, 30)
MAX_BATCH_RETRIES = 3
RETRY_SUBPASS = True
PRICE_THRESHOLD = 5.0

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()]
)

def get_today_earning_date(calendar_events, yahoo_sym):
    try:
        if not isinstance(calendar_events, dict):
            return None
        cal = calendar_events.get(yahoo_sym, {})
        if not isinstance(cal, dict):
            return None
        earnings = cal.get("earnings", {})
        if not isinstance(earnings, dict):
            return None
        ed_list = earnings.get("earningsDate")
        if not ed_list:
            return None

        raw = str(ed_list[0])
        cleaned = raw.replace(":S", "")
        dt = datetime.fromisoformat(cleaned)
        if dt.date() == datetime.now().date():
            return today.strftime("%Y-%m-%d")
    except Exception as e:
        logging.debug(f"{yahoo_sym} earnings parse error: {e}")
    return None


def ensure_dirs():
    os.makedirs("logs", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_ticker_info():
    if not os.path.exists(TICKER_INFO_FILE):
        logging.error(f"{TICKER_INFO_FILE} not found!")
        return {}
    with open(TICKER_INFO_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            logging.error("Invalid JSON in ticker_info.json")
            return {}


def partition_tickers(tickers, part_index, part_total):
    per_part = len(tickers) // part_total
    start = part_index * per_part
    end = start + per_part if part_index < part_total - 1 else len(tickers)
    return tickers[start:end]


def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def process_batch(batch, ticker_info):
    for attempt in range(MAX_BATCH_RETRIES):
        try:
            prices = []
            failure_reasons = {"no_price": 0, "below_threshold": 0, "error": 0, "skipped_type": 0}

            yahoo_symbols = [yahoo_symbol(symbol) for symbol in batch]
            yq = Ticker(yahoo_symbols)

            hist = yq.history(period="1d")
            summary_details = yq.summary_detail
            calendar_events = yq.calendar_events

            for symbol in batch:
                yahoo_sym = yahoo_symbol(symbol)
                try:
                    logging.debug(f"Processing {symbol} (yahoo: {yahoo_sym})")

                    # === PRICE EXTRACTION ===
                    price = None
                    if yahoo_sym in hist.index.get_level_values(0):
                        df = hist.loc[yahoo_sym]
                        if not df.empty:
                            price = df['close'].iloc[-1]
                            logging.debug(f"  → {symbol} price = {price}")
                    else:
                        logging.debug(f"  → {symbol} not found in history index")

                    if price is None or not isinstance(price, (int, float)):
                        failure_reasons["no_price"] += 1
                        logging.warning(f"❌ {symbol} dropped: No valid price")
                        continue

                    if price < PRICE_THRESHOLD:
                        failure_reasons["below_threshold"] += 1
                        logging.warning(f"❌ {symbol} dropped: Price {price} < threshold")
                        continue

                    # === METADATA ===
                    info = ticker_info.get(symbol, {}).get("info", {})
                    ticker_type = info.get("type", "Unknown")

                    if ticker_type != "Stock":
                        earning_date = None
                        logging.debug(f"{symbol} (type={ticker_type}) → skipping earnings")
                    else:
                        earning_date = get_today_earning_date(calendar_events, yahoo_sym)

                    # === BUILD ENTRY ===
                    summary = summary_details.get(yahoo_sym, {}) if isinstance(summary_details, dict) else {}

                    prices.append({
                        "ticker": symbol,
                        "info": {
                            "Price": round(price, 2),
                            "industry": info.get("industry", "n/a"),
                            "sector": info.get("sector", "n/a"),
                            "type": ticker_type,
                            "DVol": summary.get("volume"),
                            "AvgVol": summary.get("averageVolume"),
                            "AvgVol10": summary.get("averageVolume10days"),
                            "52WKL": round(summary.get("fiftyTwoWeekLow", 0), 2) if summary.get("fiftyTwoWeekLow") else None,
                            "52WKH": round(summary.get("fiftyTwoWeekHigh", 0), 2) if summary.get("fiftyTwoWeekHigh") else None,
                            "MCAP": round(summary.get("marketCap", 0), 2) if summary.get("marketCap") else None,
                            "Earning_Date": earning_date
                        }
                    })
                    logging.debug(f"✅ {symbol} successfully added")

                except Exception as e:
                    logging.error(f"Exception processing {symbol}: {e}")
                    failure_reasons["error"] += 1

            failed_tickers = [s for s in batch if s not in [p["ticker"] for p in prices]]
            logging.info(f"Batch summary → Success: {len(prices)}, Failed: {len(failed_tickers)}, Reasons: {failure_reasons}")
            
            return len(prices), failed_tickers, prices

        except Exception as e:
            logging.warning(f"Batch level error (attempt {attempt+1}): {e}")
            time.sleep(random.uniform(5, 10))

    return 0, batch, []


def main(part_index=None, part_total=None, verbose=False):
    start_time = time.time()
    ensure_dirs()

    logging.info(f"Starting price build for part {part_index}")

    ticker_info = load_ticker_info()
    qualified_tickers = list(ticker_info.keys())
    logging.info(f"Total tickers loaded: {len(qualified_tickers)}")

    if "SPY" in qualified_tickers:
        logging.info("✅ SPY found in ticker_info.json")
    else:
        logging.warning("❌ SPY NOT found in ticker_info.json!")

    if part_index is not None and part_total is not None:
        part_tickers = partition_tickers(qualified_tickers, part_index, part_total)
    else:
        part_tickers = qualified_tickers

    batches = [part_tickers[i:i + BATCH_SIZE] for i in range(0, len(part_tickers), BATCH_SIZE)]

    all_prices = []
    all_failed = []

    for idx, batch in enumerate(tqdm(batches, desc="Processing Price Batches"), 1):
        updated, failed_tickers, prices = process_batch(batch, ticker_info)
        all_prices.extend(prices)
        all_failed.extend(failed_tickers)

        if "SPY" in batch:
            logging.info(f"SPY was in batch {idx} → Success: {'YES' if any(p['ticker']=='SPY' for p in prices) else 'NO'}")

    # Final check
    spy_final = any(p.get("ticker") == "SPY" for p in all_prices)
    logging.info(f"FINAL RESULT → SPY in output: {'✅ YES' if spy_final else '❌ NO'}")

    if RETRY_SUBPASS and all_failed:
        logging.info(f"Running retry sub-pass on {len(set(all_failed))} tickers...")
        # ... retry logic (same as before)

    # Save output
    output_file = TICKER_PRICE_PART_FILE % part_index
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_prices, f, indent=2)

    logging.info(f"Completed. Total tickers saved: {len(all_prices)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--part-total", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    main(part_index=args.part_index, part_total=args.part_total, verbose=args.verbose)
