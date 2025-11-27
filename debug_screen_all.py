
import sys
import pandas as pd
from screening import StockScreener
from datetime import datetime

# Initialize Screener
screener = StockScreener()

# Mock progress callback
def progress_callback(msg, percent):
    print(f"[PROGRESS {percent*100:.1f}%] {msg}")

# Mock debug callback
def debug_callback(msg, level):
    print(f"[{level.upper()}] {msg}")

print("🚀 Testing screen_all_stocks with a subset of stocks...")

# Get full list first
try:
    full_list = screener.get_a_stock_list(exclude_st=True)
    print(f"Got {len(full_list)} stocks.")
except Exception as e:
    print(f"Failed to get stock list: {e}")
    sys.exit(1)

# Filter for 000429.SZ and a few others
target_stocks = ['000429.SZ', '600519.SH', '000001.SZ']
subset_list = full_list[full_list['ts_code'].isin(target_stocks)]

if subset_list.empty:
    print("❌ Target stocks not found in list!")
    sys.exit(1)

print(f"Testing with {len(subset_list)} stocks: {subset_list['ts_code'].tolist()}")

# Hack: Temporarily monkeypatch get_a_stock_list to return our subset
# so we can call screen_all_stocks without modifying it to accept a list
original_get_list = screener.get_a_stock_list
screener.get_a_stock_list = lambda exclude_st=True: subset_list

try:
    # Run screening
    # Use default params: years=5 (dynamic), min_roe=10.0, pr_threshold=1.0
    results = screener.screen_all_stocks(
        pr_threshold=1.0,
        min_roe=10.0,
        max_workers=2,
        api_delay=0.0,
        progress_callback=progress_callback,
        debug_callback=debug_callback
    )

    print("\n📊 Screening Results:")
    print(f"Total Passed: {len(results)}")
    for stock in results:
        print(f"✅ {stock['ts_code']} {stock.get('name', '')}")
        print(f"   PR: {stock['valuation_details']['final_pr']}")
        print(f"   ROE: {stock['valuation_details']['roe_waa']}")

    # Verify 000429.SZ passed
    passed_codes = [r['ts_code'] for r in results]
    if '000429.SZ' in passed_codes:
        print("\n✅ SUCCESS: 000429.SZ passed screening!")
    else:
        print("\n❌ FAILURE: 000429.SZ did NOT pass screening!")

except Exception as e:
    print(f"\n❌ Screening failed with error: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Restore original method
    screener.get_a_stock_list = original_get_list
