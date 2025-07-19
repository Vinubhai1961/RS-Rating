import os
import requests
import pandas as pd
import logging
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import time
from user_agents import get_random_user_agent

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
MAX_WORKERS = 10
NASDAQ_URL = 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt'
RETRY_ATTEMPTS = 7
INITIAL_DELAY = 0.1  # Delay in seconds for rate-limiting

def fetch_nasdaq_data():
    attempt = 1
    while attempt <= RETRY_ATTEMPTS:
        logging.info(f"Fetching data from {NASDAQ_URL} (attempt {attempt}/{RETRY_ATTEMPTS})")
        try:
            headers = {'User-Agent': get_random_user_agent()}
            response = requests.get(NASDAQ_URL, headers=headers)
            response.raise_for_status()
            df = pd.read_csv(NASDAQ_URL, sep='|', skipfooter=1, engine='python')
            logging.info(f"Retrieved {len(df)} symbols from NASDAQ")
            return df
        except Exception as e:
            logging.error(f"Attempt {attempt} failed: {e}")
            attempt += 1
            if attempt > RETRY_ATTEMPTS:
                raise Exception(f"Failed to fetch NASDAQ data after {RETRY_ATTEMPTS} attempts")
    return None

def process_symbol(symbol, etf_status, delay):
    try:
        time.sleep(delay)  # Rate-limiting delay
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
        logging.error(f"Failed to process {symbol}: {e}")
        return None

def process_nasdaq_file():
    df = fetch_nasdaq_data()
    if df is None:
        raise Exception("Failed to fetch NASDAQ data")
    
    existing_data = {}
    ticker_info_path = os.path.join('data', 'ticker_info.json')
    if os.path.exists(ticker_info_path):
        with open(ticker_info_path, 'r') as f:
            existing_data = json.load(f)
        logging.info(f"Loaded existing data with {len(existing_data)} entries")
    
    # Create dictionary mapping symbols to ETF status
    etf_status = dict(zip(df['Symbol'], df['ETF']))
    symbols = df['Symbol'].tolist()
    failed_symbols = []
    
    # Process symbols in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(
            executor.map(
                lambda s: process_symbol(s, etf_status, INITIAL_DELAY),
                symbols
            ),
            total=len(symbols)
        ))
    
    # Collect results
    new_data = {}
    for result in results:
        if result:
            new_data.update(result)
        else:
            # Track failed symbols
            failed_index = results.index(result)
            failed_symbols.append(symbols[failed_index])
    
    # Merge with existing data
    new_data.update(existing_data)
    
    # Save results
    with open(ticker_info_path, 'w') as f:
        json.dump(new_data, f, indent=2)
    logging.info(f"Saved {len(new_data)} symbols to {ticker_info_path}")
    
    # Save failed symbols
    with open(os.path.join('data', 'failed_symbols.json'), 'w') as f:
        json.dump(failed_symbols, f, indent=2)
    logging.info(f"Saved {len(failed_symbols)} failed symbols")

if __name__ == "__main__":
    start_time = time.time()
    logging.info(f"Script started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        process_nasdaq_file()
    except Exception as e:
        logging.error(f"Error processing NASDAQ data: {e}")
        raise
    finally:
        end_time = time.time()
        logging.info(f"Script finished at {time.strftime('%Y-%m-%d %H:%M:%S')}, took {end_time - start_time:.2f} seconds")
