
import sys
import os
import pandas as pd
from datetime import datetime
from utils import calculate_recent_years, analyze_fundamentals, get_pro_client

# Mock streamlit to avoid errors if utils/app imports it
import streamlit as st
if not hasattr(st, 'session_state'):
    st.session_state = {}
if 'user_points_info' not in st.session_state:
    st.session_state.user_points_info = {'total_points': 2000}

def debug_screening():
    print(f"🔍 Debugging Screening Logic")
    print(f"--------------------------------")
    
    # 1. Check Current Date
    now = datetime.now()
    print(f"📅 Current System Date: {now}")
    

def run_test(ts_code, start_year, end_year, use_cache, description):
    print(f"\n🧪 {description}")
    print(f"   Range: {start_year}-{end_year}, Cache: {use_cache}")
    
    try:
        result = analyze_fundamentals(
            ts_code=ts_code,
            start_date=f"{start_year}0101",
            end_date=f"{end_year}1231",
            years=5,
            use_cache=use_cache,
            api_delay=0,
            max_workers=1
        )
        
        if result:
            metrics = result.get('metrics')
            audit_records = result.get('audit_records', [])
            
            if metrics is not None and not metrics.empty:
                if 'end_date' in metrics.columns:
                    years_found = [d[:4] for d in metrics['end_date']]
                    print(f"   ✅ Data Found: {years_found}")
                    
                    # Screening Check
                    from screening import StockScreener
                    screener = StockScreener()
                    fund_pass, _ = screener.check_fundamentals_pass(audit_records, metrics, 5)
                    val_pass, _ = screener.check_valuation_pass(ts_code, 1.0, 10.0)
                    
                    print(f"   📊 Screening: Fundamentals={'✅' if fund_pass else '❌'}, Valuation={'✅' if val_pass else '❌'}")
                    if fund_pass and val_pass:
                        print("   🎉 PASSED")
                    else:
                        print("   ❌ FAILED")
                else:
                    print("   ❌ Metrics missing 'end_date' column")
            else:
                print("   ❌ No metrics data")
        else:
            print("   ❌ analyze_fundamentals returned None")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

def debug_screening():
    print(f"🔍 Debugging Screening Logic")
    print(f"--------------------------------")
    
    ts_code = "000429.SZ"
    
    # Test 1: Calculated Range (2020-2024) with Cache
    s_year, e_year = calculate_recent_years(5)
    run_test(ts_code, s_year, e_year, True, "Test 1: Calculated Range (2020-2024) + Cache")
    
    # Test 2: User Screenshot Range (2019-2023) with Cache
    run_test(ts_code, 2019, 2023, True, "Test 2: User Range (2019-2023) + Cache")


if __name__ == "__main__":
    debug_screening()
