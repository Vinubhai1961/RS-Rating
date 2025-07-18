import requests
import yfinance as yf
import json
import os
import time
from tqdm import tqdm
import concurrent.futures

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ticker_info.json")
LOG_DIR = "log"
LOG_FILE = os.path.join(LOG_DIR, "error.log")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def log_error(message):
    with open(LOG_FILE, "a") as log:
        log.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def get_nasdaq_tickers():
    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.get(NASDAQ_URL, timeout=10)
            response.raise_for_status()  # Raise exception for bad status codes
            lines = response.text.splitlines()
            
            tickers = []
            for line in lines[1:]:
                parts = line.split("|")
                if len(parts) > 6 and parts[0] == "Y":  # Active symbol
                    symbol = parts[1].strip()
                    etf_flag = parts[5].strip()
                    if symbol:
                        tickers.append((symbol, etf_flag))
            return tickers
        except requests.RequestException as e:
            log_error(f"Failed to fetch NASDAQ tickers (attempt {attempt + 1}): {e}")
            time.sleep(2)  # Wait before retrying
    log_error("Failed to fetch NASDAQ tickers after retries")
    return []

def fetch_info(symbol, etf_flag):
    for attempt in range(3):  # Retry up to 3 times
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.get_info()

            sector = info.get("sector", "N/A") if etf_flag == "N" else "N/A"
            industry = info.get("industry", "N/A") if etf_flag == "N" else "N/A"

            # Get latest price
            try:
                price = ticker.fast_info.get("lastPrice", None)  # Use correct key
            except Exception:
                price = None
            if price is None:
                try:
                    hist = ticker.history(period="1d", auto_adjust=True)
                    if not hist.empty:
                        price = round(hist["Close"].iloc[-1], 2)
                    else:
                        price = "N/A"
                except Exception:
                    price = "N/A"

            return {
                "Sector": sector,
                "Industry": industry,
                "ETF": etf_flag,
                "Price": price
            }
        except Exception as e:
            log_error(f"Attempt {attempt + 1} failed for {symbol}: {e}")
            time.sleep(1)  # Wait before retrying
            continue
    log_error(f"Failed for {symbol} after retries")
    return {
        "Sector": "Error",
        "Industry": "Error",
        "ETF": etf_flag,
        "Price": "N/A"
    }

def main():
    print("Fetching NASDAQ tickers...")
    tickers = get_nasdaq_tickers()
    if not tickers:
        print("No tickers fetched. Check logs for details.")
        return
    print(f"Total tickers found: {len(tickers)}")

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_info, sym, etf): sym for sym, etf in tickers}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing tickers"):
            sym = futures[future]
            results[sym] = future.result()

    print(f"Saving results to {OUTPUT_FILE}...")
    try:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(results, f, indent=4)
    except Exception as e:
        log_error(f"Failed to save results to {OUTPUT_FILE}: {e}")
        print(f"Failed to save results: {e}")
        return

    print("Done!")

if __name__ == "__main__":
    main()
