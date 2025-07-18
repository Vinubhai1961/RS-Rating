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


def fetch_metadata(symbol, etf_flag):
    """Fetch sector and industry for non-ETF tickers."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info  # Faster than get_info()
        sector = "N/A"
        industry = "N/A"

        if etf_flag == "N":  # Only for non-ETFs
            # Use Ticker.info as fallback (still slow, but less frequent)
            try:
                details = ticker.get_info()
                sector = details.get("sector", "N/A")
                industry = details.get("industry", "N/A")
            except Exception:
                pass

        return {
            "Sector": sector,
            "Industry": industry,
            "ETF": etf_flag
        }
    except Exception as e:
        log_error(f"Metadata failed for {symbol}: {e}")
        return {"Sector": "Error", "Industry": "Error", "ETF": etf_flag}


def fetch_prices(symbols):
    """Fetch latest prices in bulk."""
    try:
        data = yf.download(tickers=symbols, period="1d", group_by='ticker', threads=True)
        prices = {}

        if isinstance(data.columns, pd.MultiIndex):
            for symbol in symbols:
                try:
                    prices[symbol] = round(data[symbol]['Close'].iloc[-1], 2)
                except:
                    prices[symbol] = "N/A"
        else:
            # Single ticker fallback
            try:
                prices[symbols[0]] = round(data['Close'].iloc[-1], 2)
            except:
                prices[symbols[0]] = "N/A"

        return prices
    except Exception as e:
        log_error(f"Price fetch failed for {symbols}: {e}")
        return {s: "N/A" for s in symbols}


def main():
    print("Fetching NASDAQ tickers...")
    tickers = get_nasdaq_tickers()
    print(f"Total tickers found: {len(tickers)}")

    results = {}

    # Step 1: Fetch metadata concurrently
    print("Fetching metadata...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_metadata, sym, etf): sym for sym, etf in tickers}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Metadata"):
            sym = futures[future]
            results[sym] = future.result()

    # Step 2: Fetch prices in batches
    print("Fetching prices in bulk...")
    symbols = [sym for sym, _ in tickers]
    batch_size = 200
    for i in tqdm(range(0, len(symbols), batch_size), desc="Price batches"):
        batch = symbols[i:i + batch_size]
        prices = fetch_prices(batch)
        for sym, price in prices.items():
            if sym in results:
                results[sym]["Price"] = price

    print(f"Saving results to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=4)

    print("Done!")
