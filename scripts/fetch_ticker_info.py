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

def fetch_ticker_info(symbol, is_etf):
    """Fetch ticker info with retry for rate limits."""
    max_retries = 3
    retry_delay = 5  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
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

def fetch_price_with_download(symbols):
    """Fetch price data using yf.download with rate limit handling."""
    max_retries = 3
    retry_delay = 10  # Adjusted delay for download
    for attempt in range(max_retries):
        try:
            data = yf.download(symbols, period="1d", threads=True)
            if data.empty or data['Close'].isna().all():
                raise ValueError("No price data available")
            return data['Close'].iloc[-1]  # Return latest closing price
        except HTTPError as e:
            if attempt < max_retries - 1 and "429" in str(e):
                time.sleep(retry_delay * (2 ** attempt))
                continue
            logging.error(f"HTTP Error fetching price for {symbols}: {str(e)}")
        except Exception as e:
            logging.error(f"Error fetching price for {symbols}: {str(e)}")
    return None

def process_batch(batch):
    """Process a batch of (symbol, is_etf) tuples using both methods."""
    results = {}
    for symbol, is_etf in batch:
        try:
            # Fetch additional info using fetch_ticker_info
            info = fetch_ticker_info(symbol, is_etf)
            if info:
                # Fetch price using yf.download
                price = fetch_price_with_download(symbol)
                if price is not None:
                    info["Price"] = round(float(price), 2)
                results[symbol] = info
        except Exception as e:
            logging.error(f"Error processing {symbol}: {str(e)}")
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
        
        batch_size = 50  # Reduced batch size to minimize rate limit issues
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        results = {}
        
        with ThreadPoolExecutor(max_workers=2) as executor:  # Further reduced workers
            for batch_result in tqdm(executor.map(process_batch, batches), total=len(batches), desc="Processing batches"):
                results.update(batch_result)
                time.sleep(15)  # Increased delay between batches
        
        os.makedirs('data', exist_ok=True)
        with open('data/ticker_info.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logging.info(f"Successfully processed {len(results)} symbols")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()
