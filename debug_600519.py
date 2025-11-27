#!/usr/bin/env python3
"""
调试600519（贵州茅台）在全网筛选中未通过的问题

目标：
1. 使用单项分析方法测试600519
2. 使用全网筛选方法测试600519  
3. 对比两种方法的差异，找出问题
"""

import sys
from datetime import datetime
from utils import calculate_recent_years, analyze_fundamentals
from screening import StockScreener
import pandas as pd

def test_single_stock_analysis(ts_code="600519.SH"):
    """
    测试单个股票分析方法（类似🔎单项分析）
    """
    print("=" * 80)
    print(f"【方法1】单股分析测试: {ts_code}")
    print("=" * 80)
    
    # 使用相同的年份计算逻辑
    current_year = datetime.now().year
    start_year = current_year - 5  # 2020
    end_year = current_year - 1     # 2024
    required_years = end_year - start_year + 1  # 5
    
    print(f"\n📅 年份范围: {start_year}-{end_year} (需要{required_years}年数据)")
    print(f"   开始日期: {start_year}0101")
    print(f"   结束日期: {end_year}1231")
    
    # 获取基本面数据
    print(f"\n🔍 获取{ts_code}的基本面数据...")
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=required_years,
        use_cache=True,  # 使用缓存
        api_delay=0.1
    )
    
    if not result:
        print(f"❌ 无法获取数据")
        return None
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"\n✅ 数据获取成功")
    print(f"   审计记录数: {len(audit_records)}")
    print(f"   财务指标行数: {len(metrics) if metrics is not None and not metrics.empty else 0}")
    
    if audit_records:
        print(f"\n   审计记录详情:")
        for record in audit_records[:10]:
            print(f"      {record.end_date[:4]}年: {record.audit_result} ({'✅标准' if record.is_standard else '❌非标准'})")
    
    if metrics is not None and not metrics.empty:
        print(f"\n   财务指标年份:")
        years_in_data = [row['end_date'][:4] for _, row in metrics.iterrows()]
        print(f"      {years_in_data}")
        print(f"      共{len(years_in_data)}年，{'✅充足' if len(years_in_data) >= required_years else '❌不足'} (需要{required_years}年)")
    
    # 使用筛选器检查基本面
    screener = StockScreener()
    fundamentals_pass, fund_details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=required_years
    )
    
    print(f"\n📊 基本面筛选结果: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"\n详细检查:")
    print(f"   - 数据完整性: {'✅' if fund_details.get('data_sufficiency_pass') else '❌'} {fund_details.get('data_sufficiency_msg', '')}")
    print(f"   - 审计意见: {'✅' if fund_details.get('audit_pass') else '❌'}")
    print(f"   - 现金流≥0: {'✅' if fund_details.get('cashflow_pass') else '❌'}")
    print(f"   - 现金流覆盖利润: {'✅' if fund_details.get('cashflow_ge_profit') else '❌'}")
    
    # 估值筛选
    print(f"\n💰 估值筛选...")
    pr_threshold = 1.0
    min_roe = 10.0
    valuation_pass, val_details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=pr_threshold,
        min_roe=min_roe
    )
    
    print(f"\n💰 估值筛选结果: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"   - PR: {val_details.get('final_pr', 'N/A')}")
    print(f"   - ROE: {val_details.get('roe_waa', 'N/A')}%")
    print(f"   - PR通过: {'✅' if val_details.get('pr_pass') else '❌'}")
    print(f"   - ROE通过: {'✅' if val_details.get('roe_pass') else '❌'}")
    
    # 综合判断
    overall_pass = fundamentals_pass and valuation_pass
    print(f"\n{'='*80}")
    print(f"【方法1结果】{'✅ 通过筛选' if overall_pass else '❌ 未通过筛选'}")
    print(f"{'='*80}")
    
    return {
        'method': 'single_analysis',
        'fundamentals_pass': fundamentals_pass,
        'valuation_pass': valuation_pass,
        'overall_pass': overall_pass,
        'fund_details': fund_details,
        'val_details': val_details,
        'metrics': metrics,
        'audit_records': audit_records
    }


def test_screening_method(ts_code="600519.SH"):
    """
    测试全网筛选方法（使用analyze_single_stock）
    """
    print("\n\n" + "=" * 80)
    print(f"【方法2】全网筛选方法测试: {ts_code}")
    print("=" * 80)
    
    screener = StockScreener()
    
    # 使用相同的参数
    pr_threshold = 1.0
    min_roe = 10.0
    
    # 这里不传start_year和end_year，让analyze_single_stock自己计算
    # 这模拟了screen_all_stocks的实际行为
    print(f"\n🔍 调用analyze_single_stock（不传start_year/end_year，由函数内部计算）...")
    result = screener.analyze_single_stock(
        ts_code=ts_code,
        pr_threshold=pr_threshold,
        min_roe=min_roe,
        start_year=None,  # 让函数自己计算
        end_year=None,    # 让函数自己计算
        api_delay=0.1,
        max_workers=1
    )
    
    if not result:
        print(f"❌ analyze_single_stock返回None")
        return None
    
    print(f"\n✅ analyze_single_stock执行成功")
    print(f"   基本面通过: {'✅' if result.get('fundamentals_pass') else '❌'}")
    print(f"   估值通过: {'✅' if result.get('valuation_pass') else '❌'}")
    print(f"   综合通过: {'✅' if result.get('overall_pass') else '❌'}")
    
    # 详细信息
    fund_details = result.get('fundamentals_details', {})
    val_details = result.get('valuation_details', {})
    
    print(f"\n📊 基本面详情:")
    print(f"   - 数据完整性: {'✅' if fund_details.get('data_sufficiency_pass') else '❌'} {fund_details.get('data_sufficiency_msg', '')}")
    print(f"   - 审计意见: {'✅' if fund_details.get('audit_pass') else '❌'}")
    print(f"   - 现金流≥0: {'✅' if fund_details.get('cashflow_pass') else '❌'}")
    print(f"   - 现金流覆盖利润: {'✅' if fund_details.get('cashflow_ge_profit') else '❌'}")
    
    print(f"\n💰 估值详情:")
    print(f"   - PR: {val_details.get('final_pr', 'N/A')}")
    print(f"   - ROE: {val_details.get('roe_waa', 'N/A')}%")
    print(f"   - PR通过: {'✅' if val_details.get('pr_pass') else '❌'}")
    print(f"   - ROE通过: {'✅' if val_details.get('roe_pass') else '❌'}")
    
    print(f"\n{'='*80}")
    print(f"【方法2结果】{'✅ 通过筛选' if result.get('overall_pass') else '❌ 未通过筛选'}")
    print(f"{'='*80}")
    
    return {
        'method': 'screening_method',
        'result': result
    }


def compare_results(result1, result2):
    """
    对比两种方法的结果
    """
    print("\n\n" + "=" * 80)
    print("【结果对比】")
    print("=" * 80)
    
    if not result1 or not result2:
        print("⚠️ 某个方法未能返回结果，无法对比")
        return
    
    # 对比基本面通过情况
    fund1 = result1.get('fundamentals_pass')
    fund2 = result2.get('result', {}).get('fundamentals_pass')
    
    print(f"\n基本面筛选:")
    print(f"   方法1（单项分析）: {'✅' if fund1 else '❌'}")
    print(f"   方法2（全网筛选）: {'✅' if fund2 else '❌'}")
    if fund1 != fund2:
        print(f"   ⚠️ 不一致！")
        
        # 详细对比
        fund_details1 = result1.get('fund_details', {})
        fund_details2 = result2.get('result', {}).get('fundamentals_details', {})
        
        print(f"\n   详细对比:")
        print(f"   数据完整性:")
        print(f"      方法1: {'✅' if fund_details1.get('data_sufficiency_pass') else '❌'} {fund_details1.get('data_sufficiency_msg', '')}")
        print(f"      方法2: {'✅' if fund_details2.get('data_sufficiency_pass') else '❌'} {fund_details2.get('data_sufficiency_msg', '')}")
        
        print(f"   审计意见:")
        print(f"      方法1: {'✅' if fund_details1.get('audit_pass') else '❌'}")
        print(f"      方法2: {'✅' if fund_details2.get('audit_pass') else '❌'}")
        
        print(f"   现金流≥0:")
        print(f"      方法1: {'✅' if fund_details1.get('cashflow_pass') else '❌'}")
        print(f"      方法2: {'✅' if fund_details2.get('cashflow_pass') else '❌'}")
        
        print(f"   现金流覆盖利润:")
        print(f"      方法1: {'✅' if fund_details1.get('cashflow_ge_profit') else '❌'}")
        print(f"      方法2: {'✅' if fund_details2.get('cashflow_ge_profit') else '❌'}")
    else:
        print(f"   ✅ 一致")
    
    # 对比估值通过情况
    val1 = result1.get('valuation_pass')
    val2 = result2.get('result', {}).get('valuation_pass')
    
    print(f"\n估值筛选:")
    print(f"   方法1（单项分析）: {'✅' if val1 else '❌'}")
    print(f"   方法2（全网筛选）: {'✅' if val2 else '❌'}")
    if val1 != val2:
        print(f"   ⚠️ 不一致！")
        
        # 详细对比
        val_details1 = result1.get('val_details', {})
        val_details2 = result2.get('result', {}).get('valuation_details', {})
        
        print(f"\n   详细对比:")
        print(f"   PR值:")
        print(f"      方法1: {val_details1.get('final_pr', 'N/A')}")
        print(f"      方法2: {val_details2.get('final_pr', 'N/A')}")
        
        print(f"   ROE值:")
        print(f"      方法1: {val_details1.get('roe_waa', 'N/A')}%")
        print(f"      方法2: {val_details2.get('roe_waa', 'N/A')}%")
    else:
        print(f"   ✅ 一致")
    
    # 对比综合结果
    overall1 = result1.get('overall_pass')
    overall2 = result2.get('result', {}).get('overall_pass')
    
    print(f"\n综合结果:")
    print(f"   方法1（单项分析）: {'✅ 通过' if overall1 else '❌ 未通过'}")
    print(f"   方法2（全网筛选）: {'✅ 通过' if overall2 else '❌ 未通过'}")
    if overall1 != overall2:
        print(f"\n   🚨 发现问题！两种方法结果不一致！")
        print(f"   这就是导致600519在全网筛选中{'未通过' if not overall2 else '通过'}但单项分析{'通过' if overall1 else '未通过'}的原因！")
    else:
        print(f"   ✅ 一致（都{'通过' if overall1 else '未通过'}筛选）")


def main():
    print("=" * 80)
    print("🔍 调试600519（贵州茅台）筛选问题")
    print("=" * 80)
    print(f"当前时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print()
    
    ts_code = "600519.SH"
    
    # 测试方法1：单项分析
    result1 = test_single_stock_analysis(ts_code)
    
    # 测试方法2：全网筛选方法
    result2 = test_screening_method(ts_code)
    
    # 对比结果
    compare_results(result1, result2)
    
    print("\n" + "=" * 80)
    print("调试完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
