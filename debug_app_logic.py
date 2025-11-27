
import sys
import os
import pandas as pd
from datetime import datetime
from utils import analyze_fundamentals, calculate_recent_years
from screening import StockScreener

# Mock Streamlit session state behavior if needed, but here we just test logic
# Parameters from app.py defaults
years = 5
min_roe = 10.0
max_pr = 1.0
ts_code = '000429.SZ'
stock_name = '粤高速A'

print(f"Testing logic for {ts_code} ({stock_name})")
print(f"Parameters: years={years}, min_roe={min_roe}, max_pr={max_pr}")

# 1. Calculate years
analysis_start_year, analysis_end_year = calculate_recent_years(required_years=years)
print(f"Calculated Years: {analysis_start_year} - {analysis_end_year}")

# 2. Analyze Fundamentals (replicating app.py call)
print("Calling analyze_fundamentals...")
result = analyze_fundamentals(
    ts_code=ts_code,
    start_date=f"{analysis_start_year}0101",
    end_date=f"{analysis_end_year}1231",
    years=years,
    use_cache=True,
    api_delay=0.0,
    max_workers=1,
    user_points=None
)

if not result:
    print("❌ analyze_fundamentals returned None")
    sys.exit(1)

audit_records = result.get('audit_records', [])
metrics = result.get('metrics')

print(f"Got {len(audit_records)} audit records")
if metrics is not None:
    print(f"Got metrics DataFrame with {len(metrics)} rows")
    print(metrics[['end_date', 'n_income', 'n_cashflow_act']].head())
else:
    print("❌ metrics is None")

# 3. Check Fundamentals Pass
screener = StockScreener()
print("Calling check_fundamentals_pass...")
fundamentals_pass, fundamentals_details = screener.check_fundamentals_pass(
    audit_records=audit_records,
    metrics=metrics,
    required_years=years
)

print(f"Fundamentals Pass: {fundamentals_pass}")
print(f"Details: {fundamentals_details}")

if fundamentals_pass:
    # 4. Check Valuation Pass
    print("Calling check_valuation_pass...")
    valuation_pass, valuation_details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=max_pr,
        min_roe=min_roe
    )
    print(f"Valuation Pass: {valuation_pass}")
    print(f"Details: {valuation_details}")
    
    if valuation_pass:
        print("✅ STOCK PASSED ALL CHECKS")
    else:
        print("❌ STOCK FAILED VALUATION CHECK")
else:
    print("❌ STOCK FAILED FUNDAMENTALS CHECK")
