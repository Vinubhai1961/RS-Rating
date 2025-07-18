import json
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import datetime
import re

# Setup logging
os.makedirs('log', exist_ok=True)
logging.basicConfig(filename='log/error.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_ticker_info(symbol, is_etf):
    """Fetch ticker info using yfinance, incorporating ETF status from NASDAQ data."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        price = info.get('regularMarketPrice', info.get('previousClose', info.get('lastPrice')))
        
        if price is None:
            raise ValueError("Price data not available")
        
        return {
            "Sector": info.get('sector', 'N/A'),
            "Industry": info.get('industry', 'N/A'),
            "ETF": is_etf,  # Use NASDAQ file's ETF status
            "Price": round(float(price), 2)
        }
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {str(e)}")
        return None

def process_batch(batch):
    """Process a batch of (symbol, is_etf) tuples."""
    results = {}
    for symbol, is_etf in batch:
        try:
            info = fetch_ticker_info(symbol, is_etf)
            if info:
                results[symbol] = info
        except Exception as e:
            logging.error(f"Error processing {symbol}: {str(e)}")
    return results

def main():
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    try:
        # Download and read the data
        df = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
        
        # Filter valid symbols: non-test issues and NASDAQ Symbol without special characters
        valid_tickers = df[
            (df['Test Issue'] == 'N') & 
            (~df['NASDAQ Symbol'].str.contains(r'[\+\-=/]', na=False, regex=True))
        ][['Symbol', 'ETF']].copy()
        valid_tickers['ETF'] = valid_tickers['ETF'].apply(lambda x: 'Y' if x == 'Y' else 'N')
        symbols = [(row['Symbol'], row['ETF']) for _, row in valid_tickers.iterrows()]
        
        # Batch processing with threading
        batch_size = 100
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        results = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Process batches with progress bar
            for batch_result in tqdm(executor.map(process_batch, batches), total=len(batches), desc="Processing batches"):
                results.update(batch_result)
        
        # Save to JSON
        os.makedirs('data', exist_ok=True)
        with open('data/ticker_info.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logging.info(f"Successfully processed {len(results)} symbols")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()
