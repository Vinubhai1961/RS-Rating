import os
import json
import time
import logging
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Configure logging to write to a file with error handling
log_file = 'update_stocks.log'
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='w'
    )
    logging.info("Logging initialized successfully")
except PermissionError:
    print("Warning: Could not create log file due to permission issues. Logging to console instead.")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
INITIAL_DELAY = 0.5
MAX_WORKERS = 5
BATCH_SIZE = 100

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

def process_symbol(symbol, etf_status):
    try:
        time.sleep(INITIAL_DELAY)
        ticker = yf.Tickers(symbol).tickers[symbol]
        info = ticker.info
        return {
            symbol: {
                'Sector': info.get('sector', 'N/A'),
                'Industry': info.get('industry', 'N/A'),
                'Type': 'ETF' if etf_status.get(symbol) == 'Y' else 'Stock',
                'Price': info.get('currentPrice', None),
                'MarketCap': info.get('marketCap', None)
            }
        }
    except Exception as e:
        logging.error(f"Failed to process {symbol}: {e}")
        return None

def process_nasdaq_file():
    start_time = time.time()
    logging.info(f"Script started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    df = fetch_nasdaq_data(url)
    if df is None:
        logging.error("Failed to retrieve NASDAQ data, exiting.")
        return
    symbols = df[df['Test Issue'] == 'N']['Symbol'].tolist()
    logging.info(f"Retrieved {len(symbols)} symbols from NASDAQ")
    
    etf_status = dict(zip(df['Symbol'], df['ETF']))
    
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    ticker_info = {}
    try:
        with open(os.path.join(data_dir, 'ticker_info.json'), 'r') as f:
            ticker_info = json.load(f)
        logging.info(f"Loaded existing data with {len(ticker_info)} entries")
    except FileNotFoundError:
        logging.info("No existing ticker_info.json found, starting fresh")
    
    failed_symbols = []
    total_symbols = len(symbols)
    processed_count = 0
    
    with tqdm(total=total_symbols, desc="Processing Symbols", unit="symbol") as pbar:
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_symbols = symbols[i:i + BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(
                    lambda s: process_symbol(s, etf_status),
                    batch_symbols
                ))
            
            for result in results:
                processed_count += 1
                if result:
                    ticker_info.update(result)
                else:
                    failed_symbol = next(iter(result.keys())) if result else None
                    if failed_symbol:
                        failed_symbols.append(failed_symbol)
                pbar.update(1)  # Update progress bar for each processed symbol
            
            logging.info(f"Progress: {processed_count}/{total_symbols} ({(processed_count/total_symbols)*100:.1f}%)")
    
    try:
        with open(os.path.join(data_dir, 'ticker_info.json'), 'w') as f:
            json.dump(ticker_info, f, indent=2)
        logging.info(f"Saved {len(ticker_info)} symbols to data/ticker_info.json")
    except Exception as e:
        logging.error(f"Failed to save ticker_info.json: {e}")
    
    try:
        with open(os.path.join(data_dir, 'failed_symbols.json'), 'w') as f:
            json.dump(failed_symbols, f, indent=2)
        logging.info(f"Saved {len(failed_symbols)} failed symbols")
    except Exception as e:
        logging.error(f"Failed to save failed_symbols.json: {e}")
    
    end_time = time.time()
    logging.info(f"Script finished at {time.strftime('%Y-%m-%d %H:%M:%S')}, took {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    process_nasdaq_file()
