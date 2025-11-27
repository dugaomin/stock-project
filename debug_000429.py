
import sys
import os
import pandas as pd
from datetime import datetime
from utils import analyze_fundamentals, fetch_valuation_data
from screening import StockScreener
from cache_manager import data_cache

# Setup
ts_code = "000429.SZ"
start_year = 2019
end_year = 2023
required_years = end_year - start_year + 1
pr_threshold = 1.0
min_roe = 10.0

screener = StockScreener()

print(f"\n{'='*20} DEBUGGING {ts_code} ({start_year}-{end_year}) {'='*20}")

# 1. Check Cache
print(f"\n[1] Checking Cache Status...")
start_date = f"{start_year}0101"
end_date = f"{end_year}1231"
cache_key = f"{ts_code}_{start_date}_{end_date}_{required_years}"
print(f"   Cache Key: {cache_key}")

cached_data = data_cache.get(cache_key)
if cached_data:
    print("   ✅ Cache HIT")
    # Analyze cached data
    if 'metrics_dict' in cached_data:
        metrics_df = pd.DataFrame(cached_data['metrics_dict'])
        print(f"   Cached Years Found: {len(metrics_df)}")
        if not metrics_df.empty:
            print(f"   Years: {metrics_df['end_date'].tolist()}")
    else:
        print("   ⚠️ Cache exists but 'metrics_dict' missing")
else:
    print("   ❌ Cache MISS")

# 2. Run Analysis (Simulate Screening Logic)
print(f"\n[2] Running Analysis (use_cache=True)...")
try:
    result = screener.analyze_single_stock(
        ts_code=ts_code,
        pr_threshold=pr_threshold,
        min_roe=min_roe,
        start_year=start_year,
        end_year=end_year,
        debug_callback=lambda msg, type: print(f"   [LOG] {msg}")
    )

    if result:
        print(f"\n[3] Analysis Result:")
        print(f"   Overall Pass: {result['overall_pass']}")
        
        print(f"   --- Fundamentals ({result['fundamentals_pass']}) ---")
        details = result['fundamentals_details']
        print(f"   Audit Pass: {details.get('audit_pass')} (Details: {details.get('audit_details')})")
        print(f"   Cashflow Pass: {details.get('cashflow_pass')}")
        print(f"   Cashflow >= Profit: {details.get('cashflow_ge_profit')}")
        print(f"   Data Sufficiency: {details.get('data_sufficiency_pass')} ({details.get('data_sufficiency_msg', 'OK')})")
        
        if 'cashflow_details' in details and isinstance(details['cashflow_details'], dict):
             cf_det = details['cashflow_details']
             if 'yearly_cashflow' in cf_det:
                 print("   Cashflow Details per Year:")
                 for y in cf_det['yearly_cashflow']:
                     print(f"     {y['year']}: OCF={y['ocf']}, NetIncome={y['net_income']}, Positive={y['positive']}, Cover={y['cover_profit']}")

        print(f"   --- Valuation ({result['valuation_pass']}) ---")
        val = result['valuation_details']
        print(f"   PR Pass: {val.get('pr_pass')} (PR: {val.get('final_pr')} <= {val.get('pr_threshold')})")
        print(f"   ROE Pass: {val.get('roe_pass')} (ROE: {val.get('roe_waa')} >= {val.get('min_roe')})")
        
    else:
        print("   ❌ Analysis returned None")

except Exception as e:
    print(f"   ❌ Exception during analysis: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*50}")
