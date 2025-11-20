
import sys
import os
import pandas as pd
from utils import fetch_kline_data, get_tushare_token
import tushare as ts

# Ensure we can import from current directory
sys.path.append(os.getcwd())

def test_fetch():
    ts_code = "600519.SH" # Moutai
    print(f"Testing fetch_kline_data for {ts_code}...")
    
    # Test 1: Daily QFQ
    print("\n--- Test 1: Daily QFQ ---")
    df = fetch_kline_data(ts_code, period='daily', adj='qfq', limit=100)
    if df is not None and not df.empty:
        print(f"Success! Got {len(df)} rows.")
        print(df.tail())
    else:
        print("Failed to get data.")

    # Test 2: Weekly
    print("\n--- Test 2: Weekly ---")
    df = fetch_kline_data(ts_code, period='weekly', adj='qfq', limit=100)
    if df is not None and not df.empty:
        print(f"Success! Got {len(df)} rows.")
        print(df.tail())
    else:
        print("Failed to get data.")

if __name__ == "__main__":
    test_fetch()
