import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Define date range and paths
start_date = datetime.strptime("08012025", "%m%d%Y")
end_date = datetime.now()
while end_date.weekday() >= 5:  # Skip weekends (Saturday=5, Sunday=6)
    end_date -= timedelta(days=1)
archive_path = "archive"
output_path = "IBD-20"
plot_path = os.path.join(output_path, "plots")

# Ensure output and plot directories exist
os.makedirs(output_path, exist_ok=True)
os.makedirs(plot_path, exist_ok=True)

# Required columns in input CSVs
required_columns = [
    'Ticker', 'Price', 'DVol', 'AvgVol', '52WKH', '52WKL', 
    'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', 
    '6M_RS Percentile', 'AvgVol10', 'MCAP', 'Sector', 'Industry', 'IPO'
]

# Initialize data storage
stock_data = {}

# Read CSV files
dates = []
total_records = 0
for i in range((end_date - start_date).days + 1):
    current_date = start_date + timedelta(days=i)
    # Skip weekends in file processing
    if current_date.weekday() >= 5:
        continue
    file_date = current_date.strftime("%m%d%Y")
    file_path = os.path.join(archive_path, f"rs_stocks_{file_date}.csv")
    
    if os.path.exists(file_path):
        print(f"Processing {file_path}")
        df = pd.read_csv(file_path)
        record_count = len(df)
        total_records += record_count
        print(f"Read {record_count} records from {file_path}")
        
        # Validate record count
        if record_count < 7000 or record_count > 9000:
            print(f"Warning: {file_path} has {record_count} records, expected ~8000")
        
        # Validate required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: {file_path} missing columns: {missing_columns}")
            continue
        
        dates.append(file_date)
        for _, row in df.iterrows():
            ticker = row['Ticker']
            if ticker not in stock_data:
                stock_data[ticker] = {}
            stock_data[ticker][file_date] = row
    else:
        print(f"File {file_path} not found")
print(f"Total records processed: {total_records}")
print(f"Total unique tickers: {len(stock_data)}")

# Function to plot trends for debugging
def plot_trends(ticker, price_series, rs_series, rs_1m_series, rs_3m_series, rs_6m_series, dates):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    date_labels = [datetime.strptime(d, "%m%d%Y").strftime("%m-%d") for d in dates]
    
    # Plot RS metrics
    ax1.plot(date_labels, rs_series, label='RS Percentile', marker='o')
    ax1.plot(date_labels, rs_1m_series, label='1M_RS Percentile', marker='s')
    ax1.plot(date_labels, rs_3m_series, label='3M_RS Percentile', marker='^')
    ax1.plot(date_labels, rs_6m_series, label='6M_RS Percentile', marker='d')
    ax1.set_title(f"{ticker} RS Percentile Trends")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("RS Percentile")
    ax1.legend()
    ax1.grid(True)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot Price
    ax2.plot(date_labels, price_series, label='Price', marker='o', color='purple')
    ax2.set_title(f"{ticker} Price Trend")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Price")
    ax2.legend()
    ax2.grid(True)
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plot_file = os.path.join(plot_path, f"{ticker}_trends.png")
    plt.savefig(plot_file)
    plt.close()
    print(f"Saved trend plot for {ticker} to {plot_file}")

# Calculate momentum metrics
results = []
stocks_to_plot = []
plot_limit = 5  # Plot up to 5 stocks for debugging
for ticker, data in stock_data.items():
    if len(data) < 2:  # Need at least 2 days for trend analysis
        print(f"{ticker}: Skipped, only {len(data)} dates")
        continue
    
    # Extract time series data
    price_series = []
    rs_series = []
    rs_1m_series = []
    rs_3m_series = []
    rs_6m_series = []
    sorted_dates = sorted(data.keys())
    
    for date in sorted_dates:
        row = data[date]
        try:
            price_series.append(float(row['Price']))
            rs_series.append(float(row['RS Percentile']))
            rs_1m_series.append(float(row['1M_RS Percentile']))
            rs_3m_series.append(float(row['3M_RS Percentile']))
            rs_6m_series.append(float(row['6M_RS Percentile']))
        except (ValueError, TypeError):
            print(f"{ticker}: Skipped, invalid data in {date}")
            break
    
    if len(price_series) < 2:
        continue
    
    # Check RS trends and price increase
    price_increasing = price_series[-1] > price_series[0]
    rs_trends_positive = sum([
        rs_series[-1] > rs_series[0],
        rs_1m_series[-1] > rs_1m_series[0],
        rs_3m_series[-1] > rs_3m_series[0],
        rs_6m_series[-1] > rs_6m_series[0]
    ]) >= 3
    
    if price_increasing and rs_trends_positive:
        print(f"{ticker}: Passed trend check (Price increased, at least 3/4 RS trends positive)")
        # Add to plotting list if under limit
        if len(stocks_to_plot) < plot_limit:
            stocks_to_plot.append((ticker, price_series, rs_series, rs_1m_series, rs_3m_series, rs_6m_series, sorted_dates))
        
        # Get data for first and last dates
        first_date = start_date.strftime("%m%d%Y")
        last_date = end_date.strftime("%m%d%Y")
        
        if first_date in data and last_date in data:
            print(f"{ticker}: Has data for first and last dates")
            first_row = data[first_date]
            last_row = data[last_date]
            
            # Extract fields, round numerical values
            try:
                price_start = round(float(first_row['Price']), 2)
                price_end = round(float(last_row['Price']), 2)
                d_vol = round(float(last_row['DVol']), 2)
                avg_vol = round(float(last_row['AvgVol']), 2)
                wkh52 = round(float(last_row['52WKH']), 2)
                wkl52 = round(float(last_row['52WKL']), 2)
                rs = round(float(last_row['RS Percentile']), 2)
                rs_1m = round(float(last_row['1M_RS Percentile']), 2)
                rs_3m = round(float(last_row['3M_RS Percentile']), 2)
                rs_6m = round(float(last_row['6M_RS Percentile']), 2)
                avg_vol10 = round(float(last_row['AvgVol10']), 2)
                mcap = round(float(last_row['MCAP']), 2)
            except (ValueError, TypeError):
                print(f"{ticker}: Skipped, invalid data for {last_date}")
                continue
            
            # Calculate RS averages across all dates
            rs_avg = round(np.mean(rs_series), 2)
            rs_1m_avg = round(np.mean(rs_1m_series), 2)
            rs_3m_avg = round(np.mean(rs_3m_series), 2)
            rs_6m_avg = round(np.mean(rs_6m_series), 2)
            
            # Check individual RS thresholds
            if rs_avg <= 80:
                print(f"{ticker}: Failed RS Percentile threshold ({rs_avg} <= 80)")
                continue
            if rs_1m_avg <= 75:
                print(f"{ticker}: Failed 1M_RS Percentile threshold ({rs_1m_avg} <= 75)")
                continue
            if rs_3m_avg <= 70:
                print(f"{ticker}: Failed 3M_RS Percentile threshold ({rs_3m_avg} <= 70)")
                continue
            if rs_6m_avg <= 70:
                print(f"{ticker}: Failed 6M_RS Percentile threshold ({rs_6m_avg} <= 70)")
                continue
            
            # Calculate CompositeRS
            composite_rs = round((rs_avg + rs_1m_avg + rs_3m_avg + rs_6m_avg) / 4, 2)
            
            # Calculate metrics, round to 2 decimals
            price_momentum = round(((price_end - price_start) / price_start) * 100, 2)
            distance_to_52wkh = round(((wkh52 - price_end) / wkh52) * 100, 2)
            distance_from_52wkl = round(((price_end - wkl52) / wkl52) * 100, 2) if wkl52 > 0 else 0.00
            volume_ratio = round(d_vol / avg_vol, 2) if avg_vol > 0 else 0.00
            
            # Check 52-week high/low and RS/volume conditions
            if distance_to_52wkh <= 25:
                print(f"{ticker}: Passed DistanceTo52WKH filter ({distance_to_52wkh} <= 25)")
                if distance_from_52wkl >= 100:
                    print(f"{ticker}: Passed DistanceFrom52WKL filter ({distance_from_52wkl} >= 100)")
                    if composite_rs >= 80:
                        print(f"{ticker}: Passed CompositeRS filter ({composite_rs} >= 80)")
                        if volume_ratio >= 1.5:
                            print(f"{ticker}: Passed VolumeRatio filter ({volume_ratio} >= 1.5)")
                            # Calculate momentum score, round to 2 decimals
                            momentum_score = round(
                                (0.4 * composite_rs) + 
                                (0.35 * (100 - distance_to_52wkh)) + 
                                (0.2 * volume_ratio) + 
                                (0.05 * price_momentum), 2
                            )
                            
                            # Store results
                            results.append({
                                'Rank': 0,
                                'Ticker': ticker,
                                'Price': price_end,
                                'DVol': d_vol,
                                'Sector': last_row['Sector'],
                                'Industry': last_row['Industry'],
                                'RS Percentile': rs,
                                '1M_RS Percentile': rs_1m,
                                '3M_RS Percentile': rs_3m,
                                '6M_RS Percentile': rs_6m,
                                'AvgVol': avg_vol,
                                'AvgVol10': avg_vol10,
                                '52WKH': wkh52,
                                '52WKL': wkl52,
                                'MCAP': mcap,
                                'IPO': last_row['IPO'],
                                'PriceMomentum': price_momentum,
                                'DistanceTo52WKH': distance_to_52wkh,
                                'DistanceFrom52WKL': distance_from_52wkl,
                                'VolumeRatio': volume_ratio,
                                'CompositeRS': composite_rs,
                                'MomentumScore': momentum_score
                            })
                        else:
                            print(f"{ticker}: Failed VolumeRatio filter ({volume_ratio} < 1.5)")
                    else:
                        print(f"{ticker}: Failed CompositeRS filter ({composite_rs} < 80)")
                else:
                    print(f"{ticker}: Failed DistanceFrom52WKL filter ({distance_from_52wkl} < 100)")
            else:
                print(f"{ticker}: Failed DistanceTo52WKH filter ({distance_to_52wkh} > 25)")

# Plot trends for selected stocks
for ticker, price_series, rs_series, rs_1m_series, rs_3m_series, rs_6m_series, dates in stocks_to_plot:
    plot_trends(ticker, price_series, rs_series, rs_1m_series, rs_3m_series, rs_6m_series, dates)

# Define output columns
output_columns = [
    'Rank', 'Ticker', 'Price', 'DVol', 'Sector', 'Industry', 
    'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
    'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'MCAP', 'IPO',
    'PriceMomentum', 'DistanceTo52WKH', 'DistanceFrom52WKL', 
    'VolumeRatio', 'CompositeRS', 'MomentumScore'
]

# Handle empty results
if not results:
    print("No stocks passed all filters. Creating empty output CSV.")
    results_df = pd.DataFrame(columns=output_columns)
else:
    results_df = pd.DataFrame(results)
    filtered_df = results_df[
        (results_df['CompositeRS'] >= 80) & 
        (results_df['VolumeRatio'] >= 1.5)
    ].sort_values(by='MomentumScore', ascending=False)
    
    # Get top 10 stocks and assign ranks
    top_stocks = filtered_df.head(10).copy()
    top_stocks['Rank'] = range(1, len(top_stocks) + 1)
    results_df = top_stocks[output_columns]
    
# Print results
print(results_df.to_string(index=False))

# Save output CSV with 2 decimal places for numerical values
output_file = os.path.join(output_path, 'TopStockOpportunities.csv')
results_df.to_csv(output_file, index=False, float_format='%.2f')
print(f"Results saved to {output_file}")
