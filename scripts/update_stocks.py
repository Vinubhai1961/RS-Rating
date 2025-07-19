import os
import json
import time
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import yfinance as yf
from tqdm import tqdm

# Configure logging to write to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='update_stocks.log',
    filemode='w'
)

# Constants
INITIAL_DELAY = 0.5  # Increased delay to avoid rate-limiting
RETRY_ATTEMPTS = 3
MAX_WORKERS = 5  # Reduced to avoid overwhelming the API

def fetch_nasdaq_data(url, max_attempts=7, delay=5):
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"Fetching data from {url} (attempt {attempt}/{max_attempts})")
            df = pd.read_csv(url, sep='|')
            return df
        except Exception as e:
            logging.error(f"Failed to fetch data: {e}")
            if attempt < max_attempts:
                time.sleep(delay)
            else:
                raise Exception("Max attempts reached. Could not fetch NASDAQ data.")
    return None

def process_symbol(symbol, etf_status, delay):
    for attempt in range(RETRY_ATTEMPTS):
        try:
            time.sleep(delay * (2 ** attempt))  # Exponential backoff
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                symbol: {
                    'Sector': info.get('sector', 'N/A'),
                    'Industry': info.get('industry', 'N/A'),
                    'Type': 'ETF' if etf_status.get(symbol) == 'Y' else 'Stock',
                    'Price': info.get('regularMarketPrice', None)
                }
            }
        except Exception as e:
            if "Too Many Requests" in str(e):
                if attempt < RETRY_ATTEMPTS - 1:
                    logging.warning(f"Rate limit for {symbol}, retrying after delay (attempt {attempt + 1}/{RETRY_ATTEMPTS})")
                    continue
            logging.error(f"Failed to process {symbol}: {e}")
            return None
    return None

def process_nasdaq_file():
    start_time = time.time()
    logging.info(f"Script started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    df = fetch_nasdaq_data(url)
    symbols = df[df['Test Issue'] == 'N']['Symbol'].tolist()
    logging.info(f"Retrieved {len(symbols)} symbols from NASDAQ")
    
    etf_status = dict(zip(df['Symbol'], df['ETF']))
    
    os.makedirs('data', exist_ok=True)
    
    ticker_info = {}
    try:
        with open(os.path.join('data', 'ticker_info.json'), 'r') as f:
            ticker_info = json.load(f)
        logging.info(f"Loaded existing data with {len(ticker_info)} entries")
    except FileNotFoundError:
        logging.info("No existing ticker_info.json found, starting fresh")
    
    failed_symbols = []
    
    print("Starting symbol processing...", flush=True)  # Debug check for stdout
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(
            executor.map(
                lambda s: process_symbol(s, etf_status, INITIAL_DELAY),
                symbols
            ),
            total=len(symbols),
            file=sys.stdout,  # Ensure progress bar writes to stdout
            ascii=True,  # Use ASCII characters for compatibility
            desc="Processing symbols"
        ))
    
    for result in results:
        if result:
            ticker_info.update(result)
        else:
            failed_symbol = list(result.keys())[0] if result else None
            if failed_symbol:
                failed_symbols.append(failed_symbol)
    
    with open(os.path.join('data', 'ticker_info.json'), 'w') as f:
        json.dump(ticker_info, f, indent=2)
    logging.info(f"Saved {len(ticker_info)} symbols to data/ticker_info.json")
    
    with open(os.path.join('data', 'failed_symbols.json'), 'w') as f:
        json.dump(failed_symbols, f, indent=2)  # Pretty-print with indentation
    logging.info(f"Saved {len(failed_symbols)} failed symbols")
    
    end_time = time.time()
    logging.info(f"Script finished at {time.strftime('%Y-%m-%d %H:%M:%S')}, took {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    process_nasdaq_file()
