import yfinance as yf
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import os
import time
from requests.exceptions import HTTPError

def fetch_ticker_info(symbol, is_etf):
    """Fetch sector and industry for a given ticker with retry logic."""
    max_retries = 3
    retry_delay = 5  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                "Sector": info.get('sector', 'N/A'),
                "Industry": info.get('industry', 'N/A'),
                "ETF": is_etf
            }
        except HTTPError as e:
            if attempt < max_retries - 1 and "429" in str(e):  # Too Many Requests
                print(f"HTTP Error 429 for {symbol}, retrying in {retry_delay * (2 ** attempt)} seconds...")
                time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                continue
            print(f"HTTP Error fetching data for {symbol}: {str(e)}")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
        return {"Sector": "N/A", "Industry": "N/A", "ETF": is_etf}

def fetch_prices_batch(symbols):
    """Fetch prices for a batch of symbols using yf.download()."""
    try:
        data = yf.download(symbols, period="1d", threads=True)
        prices = {}
        for symbol in symbols:
            price = data['Close'][symbol].iloc[-1] if symbol in data['Close'] and not data['Close'][symbol].empty else None
            prices[symbol] = round(float(price), 2) if price is not None else None
        return prices
    except Exception as e:
        print(f"Error fetching prices for batch {symbols}: {str(e)}")
        return {symbol: None for symbol in symbols}

def process_batch(batch):
    """Process a batch of (symbol, is_etf) tuples."""
    results = {}
    symbols, is_etfs = zip(*batch)
    info_results = {symbol: fetch_ticker_info(symbol, is_etf) for symbol, is_etf in batch}
    price_results = fetch_prices_batch(symbols)
    
    for symbol in symbols:
        info = info_results[symbol]
        price = price_results.get(symbol)
        if price is not None:
            info["Price"] = price
        results[symbol] = info
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
        
        batch_size = 200  # Increased to 200 for bulk price fetching
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        results = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for batch_result in tqdm(executor.map(process_batch, batches), total=len(batches), desc="Processing batches"):
                results.update(batch_result)
                time.sleep(10)  # Delay between batches to manage rate limits
        
        os.makedirs('data', exist_ok=True)
        with open('data/ticker_info.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\nSuccessfully processed {len(results)} symbols")
        
    except Exception as e:
        print(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()
