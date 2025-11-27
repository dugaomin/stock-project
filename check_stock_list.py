
import sys
import pandas as pd
from screening import StockScreener
from utils import get_pro_client

def check_stock_list():
    print("🔍 Checking Stock List Retrieval...")
    
    try:
        screener = StockScreener()
        df = screener.get_a_stock_list(exclude_st=True)
        
        print(f"✅ Retrieved {len(df)} stocks.")
        

        # Check for 000429.SZ
        target_code = "000429.SZ"
        if target_code in df['ts_code'].values:
            # Find index
            idx = df[df['ts_code'] == target_code].index[0]
            print(f"✅ Target stock {target_code} is in the list at index {idx}.")
            row = df.iloc[idx]
            print(f"   Name: {row['name']}, Industry: {row['industry']}")
            
            if idx > 349:
                print(f"💡 Hypothesis: If user stopped at 349, {target_code} was NOT processed.")
            else:
                print(f"💡 Hypothesis: {target_code} SHOULD have been processed within first 349 stocks.")
        else:
            print(f"❌ Target stock {target_code} is NOT in the list!")

            
        # Check if list is suspiciously small
        if len(df) < 1000:
            print("⚠️ WARNING: Stock list is suspiciously small (< 1000).")
            print("   Sample stocks:")
            print(df.head())
            
    except Exception as e:
        print(f"❌ Error retrieving stock list: {e}")

if __name__ == "__main__":
    check_stock_list()
