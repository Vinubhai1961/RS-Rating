import json
import yfinance as yf
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import datetime
import time
from requests.exceptions import HTTPError

# Setup logging
os.makedirs('log', exist_ok=True)
logging.basicConfig(filename='log/error.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_ticker_info(symbol, is_etf, tickers_object):
    """Fetch ticker info using yf.Tickers object."""
    max_retries = 3
    retry_delay = 5  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            info = tickers_object.tickers[symbol].info
            price = info.get('regularMarketPrice', info.get('previousClose', info.get('lastPrice')))
            
            if price is None:
                raise ValueError("Price data not available")
            
            return {
                "Sector": info.get('sector', 'N/A'),
                "Industry": info.get('industry', 'N/A'),
                "ETF": is_etf,
                "Price": round(float(price), 2)
            }
        except HTTPError as e:
            if attempt < max_retries - 1 and "429" in str(e):  # Too Many Requests
                time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                continue
            logging.error(f"HTTP Error fetching data for {symbol}: {str(e)}")
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
        return None

def fetch_prices_with_download(symbols):
    """Fetch prices for multiple symbols using yf.download."""
    max_retries = 3
    retry_delay = 10  # Adjusted delay for download
    for attempt in range(max_retries):
        try:
            data = yf.download(symbols, period="1d", threads=True)
            if data.empty or data['Close'].isna().all().any():
                raise ValueError("No price data available")
            return data['Close'].iloc[-1]  # Return latest closing prices as Series
        except HTTPError as e:
            if attempt < max_retries - 1 and "429" in str(e):
                time.sleep(retry_delay * (2 ** attempt))
                continue
            logging.error(f"HTTP Error fetching prices for {symbols}: {str(e)}")
        except Exception as e:
            logging.error(f"Error fetching prices for {symbols}: {str(e)}")
    return None

def process_batch(batch):
    """Process a batch of (symbol, is_etf) tuples."""
    results = {}
    symbols = [symbol for symbol, _ in batch]
    is_etf_list = dict(batch)  # Map symbol to is_etf for lookup
    
    # Create yf.Tickers object for the batch
    try:
        tickers = yf.Tickers(' '.join(symbols))
        
        # Fetch initial info
        for symbol in symbols:
            info = fetch_ticker_info(symbol, is_etf_list[symbol], tickers)
            if info:
                results[symbol] = info
        
        # Fetch prices with yf.download
        prices = fetch_prices_with_download(symbols)
        if prices is not None:
            for symbol in symbols:
                if symbol in results and prices.get(symbol, None) is not None:
                    results[symbol]["Price"] = round(float(prices[symbol]), 2)
    except Exception as e:
        logging.error(f"Error processing batch {symbols}: {str(e)}")
    
    return results

def main():
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    try:
        df = pd.read_csv(url, sep='|', skipfooter=1, engine='python')
        
        # Filter for clean tickers
        valid_tickers = df[
            (df['Test Issue'] == 'N') & 
            (~df['NASDAQ Symbol'].str.contains(r'[\.\+\-=/]', na=False, regex=True)) & 
            (df['NASDAQ Symbol'].str.len().between(1, 5))
        ][['NASDAQ Symbol', 'ETF']].copy()
        valid_tickers.columns = ['Symbol', 'ETF']
        valid_tickers['ETF'] = valid_tickers['ETF'].apply(lambda x: 'Y' if x == 'Y' else 'N')
        symbols = [(row['Symbol'], row['ETF']) for _, row in valid_tickers.iterrows()]
        
        batch_size = 20  # Further reduced to minimize rate limit issues
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        results = {}
        
        with ThreadPoolExecutor(max_workers=1) as executor:  # Single worker to control rate
            for batch_result in tqdm(executor.map(process_batch, batches), total=len(batches), desc="Processing batches"):
                results.update(batch_result)
                time.sleep(20)  # Increased delay between batches
        
        os.makedirs('data', exist_ok=True)
        with open('data/ticker_info.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logging.info(f"Successfully processed {len(results)} symbols")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()
