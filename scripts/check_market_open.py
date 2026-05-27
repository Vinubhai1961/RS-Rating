#!/usr/bin/env python3
import sys
import pandas_market_calendars as mcal
from datetime import datetime
import pytz

def is_nyse_trading_day():
    """Return True if today is a regular NYSE trading day (not holiday/weekend)"""
    try:
        nyse = mcal.get_calendar('NYSE')
        today = datetime.now(pytz.timezone('America/New_York')).date()
        today_str = today.isoformat()
        
        # Get schedule for today
        schedule = nyse.schedule(start_date=today_str, end_date=today_str)
        
        # If there's a schedule entry → market is open today
        is_open = len(schedule) > 0
        print(f"NYSE trading day check for {today}: {'OPEN' if is_open else 'CLOSED (Holiday or Weekend)'}")
        return is_open
    except Exception as e:
        print(f"Error checking market calendar: {e}")
        # Fallback: skip only on weekends if library fails
        today = datetime.now().weekday()
        return today < 5  # 0-4 = Mon-Fri

if __name__ == "__main__":
    if is_nyse_trading_day():
        print("::set-output name=should_run::true")
        sys.exit(0)
    else:
        print("::set-output name=should_run::false")
        sys.exit(0)
