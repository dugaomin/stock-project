#!/usr/bin/env python3
"""
验证全网筛选修复后600519的筛选效果
"""

import sys
from datetime import datetime
from screening import StockScreener

def test_600519_screening():
    """
    测试使用全网筛选逻辑分析600519
    """
    print("=" * 80)
    print("🔍 测试全网筛选修复效果（600519-贵州茅台）")
    print("=" * 80)
    print(f"当前时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
    
    ts_code = "600519.SH"
    
    print("【策略说明】")
    print("   全网筛选现在使用：获取所有可用数据（1990-2999）")
    print("   让数据源决定返回什么，不再预判年报发布时间")
    print("   然后从返回的数据中取最近5年进行筛选\n")
    
    # 创建筛选器
    screener = StockScreener()
    
    print(f"【步骤1】使用analyze_single_stock分析{ts_code}...")
    print("   (这个方法在screen_all_stocks中被调用)\n")
    
    # 使用analyze_single_stock（全网筛选内部使用的方法）
    result = screener.analyze_single_stock(
        ts_code=ts_code,
        pr_threshold=1.0,
        min_roe=10.0,
        start_year=None,  # 让函数自己决定（会使用1990-2999）
        end_year=None,
        api_delay=0.1,
        max_workers=1
    )
    
    if not result:
        print(f"❌ analyze_single_stock返回None")
        return
    
    print(f"\n{'='*80}")
    print("【筛选结果】")
    print(f"{'='*80}\n")
    
    fundamentals_pass = result.get('fundamentals_pass')
    valuation_pass = result.get('valuation_pass')
    overall_pass = result.get('overall_pass')
    
    print(f"基本面筛选: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"估值筛选: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"综合判断: {'✅ 通过筛选' if overall_pass else '❌ 未通过筛选'}\n")
    
    # 详细信息
    fund_details = result.get('fundamentals_details', {})
    val_details = result.get('valuation_details', {})
    
    print("【基本面详情】")
    print(f"   数据完整性: {'✅' if fund_details.get('data_sufficiency_pass') else '❌'}")
    if 'data_sufficiency_msg' in fund_details:
        print(f"   说明: {fund_details['data_sufficiency_msg']}")
    print(f"   审计意见: {'✅' if fund_details.get('audit_pass') else '❌'}")
    print(f"   现金流≥0: {'✅' if fund_details.get('cashflow_pass') else '❌'}")
    print(f"   现金流覆盖利润: {'✅' if fund_details.get('cashflow_ge_profit') else '❌'}\n")
    
    print("【估值详情】")
    print(f"   PR: {val_details.get('final_pr', 'N/A')}")
    print(f"   ROE: {val_details.get('roe_waa', 'N/A')}%")
    print(f"   PE_TTM: {val_details.get('pe_ttm', 'N/A')}")
    print(f"   PR通过 (≤1.0): {'✅' if val_details.get('pr_pass') else '❌'}")
    print(f"   ROE通过 (≥10%): {'✅' if val_details.get('roe_pass') else '❌'}\n")
    
    print(f"{'='*80}")
    if overall_pass:
        print("🎉 成功！600519现在通过了全网筛选！")
        print("   采用'获取所有数据'策略后，避免了年份判断问题")
    else:
        print("⚠️ 仍未通过筛选")
        if not fundamentals_pass:
            print("   问题：基本面筛选未通过")
            if fund_details.get('data_sufficiency_msg'):
                print(f"   原因：{fund_details['data_sufficiency_msg']}")
        if not valuation_pass:
            print("   问题：估值筛选未通过")
    print(f"{'='*80}")


if __name__ == "__main__":
    test_600519_screening()
