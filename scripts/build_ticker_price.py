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
MAX_RETRY_TIMEOUT = 120
RETRY_SUBPASS = True
PRICE_THRESHOLD = 5.0

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# =========================================================
# ✅ UPDATED: Earnings helper (TODAY ONLY) - SAFER VERSION
# =========================================================
def get_today_earning_date(calendar_events, yahoo_sym):
    try:
        cal = calendar_events.get(yahoo_sym, {})
        earnings = cal.get("earnings", {})
        ed_list = earnings.get("earningsDate")

        if not ed_list:
            return None

        raw = ed_list[0]
        cleaned = raw.replace(":S", "")
        dt = datetime.fromisoformat(cleaned)
        today = datetime.now().date()

        logging.debug(f"{yahoo_sym} | raw={raw} | parsed={dt.date()} | today={today}")

        if dt.date() == today:
            return today.strftime("%Y-%m-%d")

    except Exception as e:
        logging.debug(f"{yahoo_sym} earnings parse error: {e}")

    return None


def ensure_dirs():
    os.makedirs("logs", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger().setLevel(level)


def load_ticker_info():
    if not os.path.exists(TICKER_INFO_FILE):
        logging.error(f"{TICKER_INFO_FILE} not found!")
        return {}, []
    
    with open(TICKER_INFO_FILE, "r", encoding="utf-8") as f:
        try:
            ticker_info = json.load(f)
            # ✅ CRITICAL FIX: Always sort for consistent partitioning
            qualified_tickers = sorted(ticker_info.keys())
            
            logging.info(f"Total tickers loaded: {len(qualified_tickers)}")
            logging.info(f"First 5 tickers: {qualified_tickers[:5]}")
            logging.info(f"Last 5 tickers: {qualified_tickers[-5:]}")
            
            if "SPY" in qualified_tickers:
                logging.info("✅ SPY found in ticker_info.json")
            
            return ticker_info, qualified_tickers
            
        except json.JSONDecodeError:
            logging.error("Invalid JSON in ticker_info.json")
            return {}, []


def partition_tickers(tickers, part_index, part_total):
    per_part = len(tickers) // part_total
    start = part_index * per_part
    end = start + per_part if part_index < part_total - 1 else len(tickers)
    return tickers[start:end]


def yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def process_batch(batch, ticker_info):
    total_wait = 0

    for attempt in range(MAX_BATCH_RETRIES):
        try:
            prices = []
            failure_reasons = {"no_price": 0, "below_threshold": 0, "error": 0}

            yahoo_symbols = [yahoo_symbol(symbol) for symbol in batch]
            yq = Ticker(yahoo_symbols)

            hist = yq.history(period="1d")
            summary_details = yq.summary_detail
            calendar_events = yq.calendar_events

            for symbol in batch:
                yahoo_sym = yahoo_symbol(symbol)

                try:
                    price = None

                    if yahoo_sym in hist.index.get_level_values(0):
                        df = hist.loc[yahoo_sym]
                        if not df.empty:
                            price = df['close'].iloc[-1]

                    if price is None or not isinstance(price, (int, float)):
                        failure_reasons["no_price"] += 1
                        continue

                    if price < PRICE_THRESHOLD:
                        failure_reasons["below_threshold"] += 1
                        continue

                    summary = summary_details.get(yahoo_sym, {}) if isinstance(summary_details, dict) else {}

                    volume = summary.get("volume")
                    avg_volume = summary.get("averageVolume")
                    avg_volume_10days = summary.get("averageVolume10days")
                    fifty_two_week_low = summary.get("fiftyTwoWeekLow")
                    fifty_two_week_high = summary.get("fiftyTwoWeekHigh")
                    market_cap = summary.get("marketCap")

                    # =========================
                    # ✅ FIXED: Earnings logic (Only for Stocks)
                    # =========================
                    info = ticker_info.get(symbol, {}).get("info", {})
                    ticker_type = info.get("type", "Unknown")

                    if ticker_type != "Stock":
                        earning_date = None
                        logging.debug(f"{symbol} skipped earnings (type={ticker_type})")
                        # Still include SPY and other important ETFs
                        if symbol == "SPY":
                            logging.info(f"✅ Force including SPY (type={ticker_type})")
                    else:
                        earning_date = get_today_earning_date(calendar_events, yahoo_sym)

                    prices.append({
                        "ticker": symbol,
                        "info": {
                            "Price": round(price, 2),
                            "industry": info.get("industry", "n/a"),
                            "sector": info.get("sector", "n/a"),
                            "type": ticker_type,
                            "DVol": volume if isinstance(volume, int) else None,
                            "AvgVol": avg_volume if isinstance(avg_volume, int) else None,
                            "AvgVol10": avg_volume_10days if isinstance(avg_volume_10days, int) else None,
                            "52WKL": round(fifty_two_week_low, 2) if isinstance(fifty_two_week_low, (int, float)) else None,
                            "52WKH": round(fifty_two_week_high, 2) if isinstance(fifty_two_week_high, (int, float)) else None,
                            "MCAP": round(market_cap, 2) if isinstance(market_cap, (int, float)) else None,
                            "Earning_Date": earning_date
                        }
                    })

                except Exception as e:
                    logging.debug(f"{symbol} failed: {e}")
                    failure_reasons["error"] += 1

            failed_tickers = [s for s in batch if s not in [p["ticker"] for p in prices]]
            logging.info(f"Batch failure reasons: {failure_reasons}")
            return len(prices), failed_tickers, prices

        except Exception as e:
            logging.warning(f"Batch error (attempt {attempt+1}): {e}")
            time.sleep(random.uniform(5, 10))

    return 0, batch, []


def main(part_index=None, part_total=None, verbose=False):
    start_time = time.time()

    ensure_dirs()
    setup_logging(verbose)

    logging.info(f"Starting price build for part {part_index}")

    ticker_info, qualified_tickers = load_ticker_info()
    
    if not ticker_info:
        logging.error("No ticker_info.json found to process.")
        return

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

        logging.info(f"Batch {idx}/{len(batches)} - Fetched data for {updated} tickers")

        if idx < len(batches):
            time.sleep(random.uniform(*BATCH_DELAY_RANGE))

    # Final SPY check
    spy_in_output = any(p.get("ticker") == "SPY" for p in all_prices)
    logging.info(f"SPY in final output: {'✅ YES' if spy_in_output else '❌ NO'}")

    if RETRY_SUBPASS and all_failed:
        unresolved_unique = sorted(set(all_failed))
        logging.info(f"Retry sub-pass for {len(unresolved_unique)} unresolved tickers...")

        retry_batches = [unresolved_unique[i:i + BATCH_SIZE] for i in range(0, len(unresolved_unique), BATCH_SIZE)]

        for idx, batch in enumerate(tqdm(retry_batches, desc="Retry Price Batches"), 1):
            updated, failed_tickers, prices = process_batch(batch, ticker_info)
            all_prices.extend(prices)
            time.sleep(random.uniform(5, 10))

    unresolved_final = sorted(set(all_failed))

    with open(UNRESOLVED_PRICE_TICKERS, "w") as f:
        f.write("\n".join(unresolved_final))

    output_file = TICKER_PRICE_PART_FILE % part_index
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_prices, f, indent=2)

    elapsed = time.time() - start_time
    logging.info("Price build completed. Elapsed: %.1fs", elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ticker_price.json from ticker_info.json.")
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--part-total", type=int, required=True)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    main(part_index=args.part_index, part_total=args.part_total, verbose=args.verbose)
