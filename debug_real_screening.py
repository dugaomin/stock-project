
import pandas as pd
from screening import StockScreener, run_full_market_screening
from utils import calculate_recent_years
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_real_screening():
    print("🚀 Starting debug of REAL environment...")
    
    screener = StockScreener()
    
    # 1. Check Stock List
    print("📋 Fetching stock list...")
    try:
        df = screener.get_a_stock_list(exclude_st=True)
        print(f"✅ Stock List Count: {len(df)}")
        
        target_stock = '000429.SZ'
        if target_stock in df['ts_code'].values:
            print(f"✅ Target stock {target_stock} IS in the list.")
        else:
            print(f"❌ Target stock {target_stock} is NOT in the list!")
            # If not in list, we can't test it. But let's see what is in the list.
            print("First 5 stocks:", df['ts_code'].head().tolist())
            
    except Exception as e:
        print(f"❌ Failed to fetch stock list: {e}")
        return

    # 2. Run Screening on a subset (Target + 9 others)
    print("\n🧪 Running screening on subset...")
    
    # Filter for subset
    if target_stock in df['ts_code'].values:
        subset_df = df[df['ts_code'] == target_stock]
        others = df[df['ts_code'] != target_stock].head(9)
        subset_df = pd.concat([subset_df, others])
    else:
        subset_df = df.head(10)
        
    print(f"Testing on {len(subset_df)} stocks: {subset_df['ts_code'].tolist()}")
    
    # Monkeypatch get_a_stock_list to return this subset
    original_get_list = StockScreener.get_a_stock_list
    StockScreener.get_a_stock_list = lambda self, exclude_st=True: subset_df
    
    # Calculate years
    start_year, end_year = calculate_recent_years(required_years=5)
    print(f"📅 Years: {start_year} - {end_year}")
    
    # Run screening with concurrency
    results, stats = run_full_market_screening(
        pr_threshold=1.0,
        min_roe=10.0,
        start_year=start_year,
        end_year=end_year,
        max_workers=5,     # Test concurrency
        api_delay=0.1,
        debug_callback=lambda msg, type: print(f"[{type.upper()}] {msg}")
    )
    
    print("\n📊 Screening Results:")
    print(f"Total Passed: {len(results)}")
    for stock in results:
        print(f" - {stock['ts_code']} {stock['name']}: PR={stock['valuation_details']['final_pr']}, ROE={stock['valuation_details']['roe_waa']}")

    # Restore
    StockScreener.get_a_stock_list = original_get_list

if __name__ == "__main__":
    test_real_screening()
