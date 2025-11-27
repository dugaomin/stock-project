#!/usr/bin/env python3
"""
Diagnostic script to test the full screening logic and identify why all stocks fail.
This script simulates the exact flow used in the app.py screening process.
"""

import os
os.environ['TOKENIZERS_PARALLELISM'] = 'false'

from utils import analyze_fundamentals, calculate_recent_years
from screening import StockScreener
from datetime import datetime


def test_single_stock(ts_code: str, stock_name: str, years: int = 5, use_cache: bool = True):
    """
    Test a single stock through the full screening process.
    """
    print(f"\n{'='*70}")
    print(f"Testing: {ts_code} - {stock_name}")
    print(f"{'='*70}")
    
    # Step 1: Calculate year range (same as in app.py)
    start_year, end_year = calculate_recent_years(required_years=years)
    print(f"Year range: {start_year}-{end_year} ({years} years)")
    print(f"Date range: {start_year}0101 - {end_year}1231")
    
    # Step 2: Analyze fundamentals (same as in app.py)
    print(f"\n[Step 1] Calling analyze_fundamentals...")
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=years,
        use_cache=use_cache,
        api_delay=0.0,
        max_workers=1,
        user_points=None
    )
    
    if not result:
        print(f"❌ analyze_fundamentals returned None!")
        return False
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"✅ analyze_fundamentals returned result")
    print(f"   Audit records: {len(audit_records)}")
    print(f"   Metrics shape: {metrics.shape if metrics is not None else 'None'}")
    
    if metrics is not None and not metrics.empty:
        years_in_data = sorted([row['end_date'][:4] for _, row in metrics.iterrows()])
        print(f"   Years in metrics: {years_in_data} ({len(years_in_data)} years)")
    
    if metrics is None or metrics.empty:
        print(f"❌ Metrics is None or empty!")
        return False
    
    # Step 3: Check fundamentals (same as in app.py)
    print(f"\n[Step 2] Checking fundamentals...")
    screener = StockScreener()
    fundamentals_pass, fundamentals_details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=years
    )
    
    print(f"Result: {'✅ PASSED' if fundamentals_pass else '❌ FAILED'}")
    print(f"Details:")
    print(f"   data_sufficiency_pass: {fundamentals_details.get('data_sufficiency_pass')}")
    if not fundamentals_details.get('data_sufficiency_pass'):
        print(f"      Reason: {fundamentals_details.get('data_sufficiency_msg', 'Unknown')}")
    print(f"   audit_pass: {fundamentals_details.get('audit_pass')}")
    print(f"   cashflow_pass: {fundamentals_details.get('cashflow_pass')}")
    print(f"   cashflow_ge_profit: {fundamentals_details.get('cashflow_ge_profit')}")
    
    if not fundamentals_pass:
        print(f"\n❌ Failed at fundamental check!")
        return False
    
    # Step 4: Check valuation (same as in app.py)
    print(f"\n[Step 3] Checking valuation...")
    valuation_pass, valuation_details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=1.0,
        min_roe=10.0
    )
    
    print(f"Result: {'✅ PASSED' if valuation_pass else '❌ FAILED'}")
    print(f"Details:")
    print(f"   final_pr: {valuation_details.get('final_pr')}")
    print(f"   roe_waa: {valuation_details.get('roe_waa')}")
    print(f"   pr_pass: {valuation_details.get('pr_pass')}")
    print(f"   roe_pass: {valuation_details.get('roe_pass')}")
    
    overall_pass = fundamentals_pass and valuation_pass
    print(f"\n{'='*70}")
    print(f"OVERALL RESULT: {'✅ PASSED ALL CHECKS' if overall_pass else '❌ FAILED'}")
    print(f"{'='*70}")
    
    return overall_pass


def main():
    """
    Main diagnostic function.
    """
    print(f"\n{'#'*70}")
    print(f"# Screening Diagnostic Tool")
    print(f"# Current Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*70}")
    
    # Test stocks
    test_stocks = [
        ('000429.SZ', '粤高速A'),
        ('600519.SH', '贵州茅台'),
        ('000001.SZ', '平安银行'),
    ]
    
    results = {}
    
    # Test with cache
    print(f"\n\n{'='*70}")
    print(f"TEST 1: Using Cache (use_cache=True) - Same as app.py")
    print(f"{'='*70}")
    
    for ts_code, name in test_stocks:
        passed = test_single_stock(ts_code, name, years=5, use_cache=True)
        results[f"{name} (cached)"] = passed
    
    # Test without cache
    print(f"\n\n{'='*70}")
    print(f"TEST 2: Without Cache (use_cache=False) - Fresh Data")
    print(f"{'='*70}")
    
    for ts_code, name in test_stocks:
        passed = test_single_stock(ts_code, name, years=5, use_cache=False)
        results[f"{name} (fresh)"] = passed
    
    # Summary
    print(f"\n\n{'#'*70}")
    print(f"# SUMMARY")
    print(f"{'#'*70}")
    
    for key, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {key}")
    
    # Analysis
    cached_pass_count = sum(1 for k, v in results.items() if '(cached)' in k and v)
    fresh_pass_count = sum(1 for k, v in results.items() if '(fresh)' in k and v)
    
    print(f"\nCached data: {cached_pass_count}/{len(test_stocks)} passed")
    print(f"Fresh data: {fresh_pass_count}/{len(test_stocks)} passed")
    
    if cached_pass_count < fresh_pass_count:
        print(f"\n⚠️  CACHE ISSUE DETECTED!")
        print(f"Fresh data passes more stocks than cached data.")
        print(f"Recommendation: Clear the cache or fix the cache invalidation logic.")
    elif cached_pass_count == 0 and fresh_pass_count == 0:
        print(f"\n⚠️  LOGIC ISSUE DETECTED!")
        print(f"No stocks pass even with fresh data.")
        print(f"The screening logic or year calculation may be incorrect.")
    else:
        print(f"\n✅ Cache appears to be working correctly.")


if __name__ == '__main__':
    main()
