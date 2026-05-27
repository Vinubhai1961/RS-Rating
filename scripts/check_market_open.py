#!/usr/bin/env python3
import sys
import pandas_market_calendars as mcal
from datetime import datetime
import pytz
import os

def is_nyse_trading_day():
    """Return True if today is a regular NYSE trading day"""
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
        # Fallback: skip weekends
        today_weekday = datetime.now().weekday()
        is_open = today_weekday < 5
        print(f"Fallback check (weekday): {'OPEN' if is_open else 'CLOSED'}")
        return is_open

if __name__ == "__main__":
    should_run = is_nyse_trading_day()
    
    # Modern GitHub Actions output method (no warning)
    output_file = os.getenv('GITHUB_OUTPUT')
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"should_run={str(should_run).lower()}\n")
    else:
        # For manual running
        print(f"Manual run - Should run: {should_run}")
