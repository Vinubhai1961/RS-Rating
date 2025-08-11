import pandas as pd
import datetime as dt
import pandas_market_calendars as mcal

# File paths
input_file = "RSRATING.csv"
output_file = "RSRATING_TV.csv"

# NYSE calendar
nyse = mcal.get_calendar('NYSE')

# Generate last 35 trading days from 2025-08-11
end_date = dt.date(2025, 8, 11)
schedule = nyse.schedule(start_date=end_date - pd.Timedelta(days=60), end_date=end_date)
trading_days = mcal.date_range(schedule, frequency='1D').strftime('%Y%m%d').tolist()

# Take last 35 trading days (most recent first)
last_35_days = trading_days[-35:]

# Read CSV
df = pd.read_csv(input_file)

# Replace first column with new dates
df.iloc[:, 0] = last_35_days[::-1]  # reverse to keep earliest first if needed

# Save to CSV
df.to_csv(output_file, index=False)
print(f"Saved updated file to {output_file}")
