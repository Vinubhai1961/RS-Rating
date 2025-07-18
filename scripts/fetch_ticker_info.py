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
    response = requests.get(NASDAQ_URL)
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

def fetch_info(symbol, etf_flag):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.get_info()

        sector = info.get("sector", "N/A") if etf_flag == "N" else "N/A"
        industry = info.get("industry", "N/A") if etf_flag == "N" else "N/A"

        # Get latest price quickly
        try:
            price = ticker.fast_info.get("last_price")
        except Exception:
            price = None
        if price is None:
            try:
                hist = ticker.history(period="1d")
                price = round(hist["Close"].iloc[-1], 2)
            except Exception:
                price = "N/A"

        return {
            "Sector": sector,
            "Industry": industry,
            "ETF": etf_flag,
            "Price": price
        }

    except Exception as e:
        log_error(f"Failed for {symbol}: {e}")
        return {
            "Sector": "Error",
            "Industry": "Error",
            "ETF": etf_flag,
            "Price": "N/A"
        }

def main():
    print("Fetching NASDAQ tickers...")
    tickers = get_nasdaq_tickers()
    print(f"Total tickers found: {len(tickers)}")

    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_info, sym, etf): sym for sym, etf in tickers}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing tickers"):
            sym = futures[future]
            results[sym] = future.result()

    print(f"Saving results to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)

    print("Done!")
