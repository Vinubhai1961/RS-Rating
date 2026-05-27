#!/usr/bin/env python3
import sys
import pandas_market_calendars as mcal
from datetime import datetime
import pytz
import os

def is_nyse_trading_day():
    try:
        nyse = mcal.get_calendar('NYSE')
        today = datetime.now(pytz.timezone('America/New_York')).date()
        today_str = today.isoformat()
        
        schedule = nyse.schedule(start_date=today_str, end_date=today_str)
        is_open = len(schedule) > 0
        
        print(f"NYSE trading day check for {today}: {'OPEN' if is_open else 'CLOSED (Holiday or Weekend)'}")
        return is_open
    except Exception as e:
        print(f"Error checking market calendar: {e}")
        today_weekday = datetime.now().weekday()
        is_open = today_weekday < 5
        print(f"Fallback: {'OPEN' if is_open else 'CLOSED'}")
        return is_open

if __name__ == "__main__":
    market_open = is_nyse_trading_day()
    
    # Write to GITHUB_OUTPUT (modern way)
    output_file = os.getenv('GITHUB_OUTPUT')
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"market_open={str(market_open).lower()}\n")
            f.write(f"should_run={str(market_open).lower()}\n")
    else:
        print(f"Manual run - Market Open: {market_open}")
