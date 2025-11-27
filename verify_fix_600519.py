#!/usr/bin/env python3
"""
验证年份计算优化后600519的筛选结果
"""

import sys
from datetime import datetime
from utils import calculate_recent_years, analyze_fundamentals
from screening import StockScreener

def test_600519_with_new_logic():
    """
    使用优化后的年份计算逻辑测试600519
    """
    print("=" * 80)
    print("🔍 验证年份计算优化后的效果")
    print("=" * 80)
    print(f"当前时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print()
    
    ts_code = "600519.SH"
    
    # 使用calculate_recent_years函数（已优化）
    print("【步骤1】调用优化后的calculate_recent_years函数...")
    start_year, end_year = calculate_recent_years(required_years=5)
    required_years = end_year - start_year + 1
    
    print(f"\n计算结果:")
    print(f"   开始年份: {start_year}")
    print(f"   结束年份: {end_year}")
    print(f"   需要年数: {required_years}")
    
    # 获取基本面数据
    print(f"\n【步骤2】获取{ts_code}的基本面数据...")
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=required_years,
        use_cache=True,
        api_delay=0.1
    )
    
    if not result:
        print(f"❌ 无法获取数据")
        return
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"\n✅ 数据获取成功")
    print(f"   审计记录数: {len(audit_records)}")
    print(f"   财务指标行数: {len(metrics) if metrics is not None and not metrics.empty else 0}")
    
    if metrics is not None and not metrics.empty:
        years_in_data = [row['end_date'][:4] for _, row in metrics.iterrows()]
        print(f"\n   财务指标年份: {years_in_data}")
        print(f"   共{len(years_in_data)}年，{'✅充足' if len(years_in_data) >= required_years else '❌不足'} (需要{required_years}年)")
    
    # 使用筛选器检查基本面
    print(f"\n【步骤3】基本面筛选...")
    screener = StockScreener()
    fundamentals_pass, fund_details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=required_years
    )
    
    print(f"\n基本面筛选结果: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"\n详细检查:")
    print(f"   - 数据完整性: {'✅' if fund_details.get('data_sufficiency_pass') else '❌'} {fund_details.get('data_sufficiency_msg', '')}")
    print(f"   - 审计意见: {'✅' if fund_details.get('audit_pass') else '❌'}")
    print(f"   - 现金流≥0: {'✅' if fund_details.get('cashflow_pass') else '❌'}")
    print(f"   - 现金流覆盖利润: {'✅' if fund_details.get('cashflow_ge_profit') else '❌'}")
    
    # 估值筛选
    print(f"\n【步骤4】估值筛选...")
    pr_threshold = 1.0
    min_roe = 10.0
    valuation_pass, val_details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=pr_threshold,
        min_roe=min_roe
    )
    
    print(f"\n估值筛选结果: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"   - PR: {val_details.get('final_pr', 'N/A')}")
    print(f"   - ROE: {val_details.get('roe_waa', 'N/A')}%")
    print(f"   - PR通过: {'✅' if val_details.get('pr_pass') else '❌'}")
    print(f"   - ROE通过: {'✅' if val_details.get('roe_pass') else '❌'}")
    
    # 综合判断
    overall_pass = fundamentals_pass and valuation_pass
    print(f"\n{'='*80}")
    print(f"【最终结果】")
    print(f"{'='*80}")
    print(f"基本面筛选: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"估值筛选: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"综合判断: {'✅ 通过筛选' if overall_pass else '❌ 未通过筛选'}")
    print(f"{'='*80}")
    
    if overall_pass:
        print("\n🎉 成功！600519现在通过了筛选！")
        print("   优化年份计算逻辑后，使用了数据源中已有的完整5年数据")
    else:
        print("\n⚠️  仍未通过筛选")
        if not fundamentals_pass:
            print("   问题：基本面筛选未通过")
            if not fund_details.get('data_sufficiency_pass'):
                print(f"   原因：{fund_details.get('data_sufficiency_msg')}")
                print("   建议：可能需要清除缓存后重新获取数据")
        if not valuation_pass:
            print("   问题：估值筛选未通过")


if __name__ == "__main__":
    test_600519_with_new_logic()
