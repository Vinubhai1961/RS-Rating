import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# Define date range and paths
start_date = datetime.strptime("08012025", "%m%d%Y")
end_date = datetime.strptime("08262025", "%m%d%Y")
archive_path = "archive"
output_path = "IBD-20"

# Ensure output directory exists
os.makedirs(output_path, exist_ok=True)

# Initialize data storage
stock_data = {}

# Read CSV files
dates = []
for i in range((end_date - start_date).days + 1):
    current_date = start_date + timedelta(days=i)
    file_date = current_date.strftime("%m%d%Y")
    file_path = os.path.join(archive_path, f"rs_stocks_{file_date}.csv")
    
    if os.path.exists(file_path):
        print(f"Processing {file_path}")
        df = pd.read_csv(file_path)
        dates.append(file_date)
        for _, row in df.iterrows():
            ticker = row['Ticker']
            if ticker not in stock_data:
                stock_data[ticker] = {}
            stock_data[ticker][file_date] = row
    else:
        print(f"File {file_path} not found")

# Calculate momentum metrics
results = []
for ticker, data in stock_data.items():
    if len(data) < 2:  # Need at least 2 days for trend analysis
        continue
    
    # Extract time series data
    price_series = []
    rs_series = []
    rs_1m_series = []
    rs_3m_series = []
    rs_6m_series = []
    date_indices = []
    
    for date in sorted(data.keys()):
        row = data[date]
        price_series.append(float(row['Price']))
        rs_series.append(float(row['RS Percentile']))
        rs_1m_series.append(float(row['1M_RS Percentile']))
        rs_3m_series.append(float(row['3M_RS Percentile']))
        rs_6m_series.append(float(row['6M_RS Percentile']))
        date_indices.append((datetime.strptime(date, "%m%d%Y") - start_date).days)
    
    # Calculate linear regression slopes
    def get_slope(x, y):
        if len(x) < 2:
            return 0
        coeffs = np.polyfit(x, y, 1)
        return coeffs[0]  # Slope
    
    price_slope = get_slope(date_indices, price_series)
    rs_slope = get_slope(date_indices, rs_series)
    rs_1m_slope = get_slope(date_indices, rs_1m_series)
    rs_3m_slope = get_slope(date_indices, rs_3m_series)
    rs_6m_slope = get_slope(date_indices, rs_6m_series)
    
    # Check if all slopes are positive
    slopes_positive = all(slope > 0 for slope in [rs_slope, rs_1m_slope, rs_3m_slope, rs_6m_slope, price_slope])
    
    if slopes_positive:
        # Get data for first and last dates
        first_date = start_date.strftime("%m%d%Y")
        last_date = end_date.strftime("%m%d%Y")
        
        if first_date in data and last_date in data:
            first_row = data[first_date]
            last_row = data[last_date]
            
            # Extract fields
            price_start = float(first_row['Price'])
            price_end = float(last_row['Price'])
            d_vol = float(last_row['DVol'])
            avg_vol = float(last_row['AvgVol'])
            wkh52 = float(last_row['52WKH'])
            wkl52 = float(last_row['52WKL'])
            rs = float(last_row['RS Percentile'])
            rs_1m = float(last_row['1M_RS Percentile'])
            rs_3m = float(last_row['3M_RS Percentile'])
            rs_6m = float(last_row['6M_RS Percentile'])
            
            # Calculate metrics
            price_momentum = ((price_end - price_start) / price_start) * 100
            distance_to_52wkh = ((wkh52 - price_end) / wkh52) * 100
            distance_from_52wkl = ((price_end - wkl52) / wkl52) * 100 if wkl52 > 0 else 0
            volume_ratio = d_vol / avg_vol if avg_vol > 0 else 0
            composite_rs = (rs_1m + rs_3m + rs_6m) / 3
            avg_rs_slope = (rs_slope + rs_1m_slope + rs_3m_slope + rs_6m_slope) / 4
            
            # Check 52-week high/low conditions
            if distance_to_52wkh <= 25 and distance_from_52wkl >= 100:
                # Calculate momentum score
                momentum_score = (0.35 * composite_rs) + (0.3 * (100 - distance_to_52wkh)) + \
                                (0.2 * volume_ratio) + (0.05 * price_momentum) + (0.1 * avg_rs_slope * 10)
                
                # Store results
                results.append({
                    'Rank': 0,
                    'Ticker': ticker,
                    'Price': price_end,
                    'DVol': last_row['DVol'],
                    'Sector': last_row['Sector'],
                    'Industry': last_row['Industry'],
                    'RS Percentile': rs,
                    '1M_RS Percentile': rs_1m,
                    '3M_RS Percentile': rs_3m,
                    '6M_RS Percentile': rs_6m,
                    'AvgVol': last_row['AvgVol'],
                    'AvgVol10': last_row['AvgVol10'],
                    '52WKH': last_row['52WKH'],
                    '52WKL': last_row['52WKL'],
                    'MCAP': last_row['MCAP'],
                    'IPO': last_row['IPO'],
                    'PriceMomentum': price_momentum,
                    'DistanceTo52WKH': distance_to_52wkh,
                    'DistanceFrom52WKL': distance_from_52wkl,
                    'VolumeRatio': volume_ratio,
                    'CompositeRS': composite_rs,
                    'RS_Slope': rs_slope,
                    'RS1M_Slope': rs_1m_slope,
                    'RS3M_Slope': rs_3m_slope,
                    'RS6M_Slope': rs_6m_slope,
                    'Price_Slope': price_slope,
                    'MomentumScore': momentum_score
                })

# Convert results to DataFrame and filter
results_df = pd.DataFrame(results)
filtered_df = results_df[
    (results_df['CompositeRS'] >= 80) & 
    (results_df['VolumeRatio'] >= 1.5)
].sort_values(by='MomentumScore', ascending=False)

# Get top 10 stocks and assign ranks
top_stocks = filtered_df.head(10).copy()
top_stocks['Rank'] = range(1, len(top_stocks) + 1)

# Reorder columns for output
output_columns = [
    'Rank', 'Ticker', 'Price', 'DVol', 'Sector', 'Industry', 
    'RS Percentile', '1M_RS Percentile', '3M_RS Percentile', '6M_RS Percentile',
    'AvgVol', 'AvgVol10', '52WKH', '52WKL', 'MCAP', 'IPO',
    'PriceMomentum', 'DistanceTo52WKH', 'DistanceFrom52WKL', 
    'VolumeRatio', 'CompositeRS', 'RS_Slope', 'RS1M_Slope', 
    'RS3M_Slope', 'RS6M_Slope', 'Price_Slope', 'MomentumScore'
]
top_stocks = top_stocks[output_columns]

# Print results
print(top_stocks.to_string(index=False))

# Save output CSV
output_file = os.path.join(output_path, 'TopStockOpportunities.csv')
top_stocks.to_csv(output_file, index=False)
print(f"Results saved to {output_file}")
