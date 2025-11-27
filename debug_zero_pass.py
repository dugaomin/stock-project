#!/usr/bin/env python3
"""
调试全网筛选零通过问题

测试目标:
1. 验证年份范围计算是否正确
2. 检查数据获取是否完整
3. 确认基本面筛选条件
4. 找出导致零通过的具体原因
"""

import sys
from datetime import datetime
from utils import calculate_recent_years, analyze_fundamentals
from screening import StockScreener

def test_year_calculation():
    """测试年份范围计算"""
    print("=" * 80)
    print("1. 测试年份范围计算")
    print("=" * 80)
    
    current_year = datetime.now().year
    current_month = datetime.now().month
    print(f"当前日期: {current_year}年{current_month}月")
    
    for required_years in [3, 5, 7]:
        start_year, end_year = calculate_recent_years(required_years=required_years)
        year_span = end_year - start_year + 1
        print(f"\n需要{required_years}年 -> 计算结果: {start_year}-{end_year} (跨度{year_span}年)")
        
        if year_span != required_years:
            print(f"⚠️ 警告: 年份跨度({year_span})与要求({required_years})不符！")
    
    print("\n")

def test_stock_data_availability(ts_code: str = "000429.SZ"):
    """测试单个股票的数据可用性"""
    print("=" * 80)
    print(f"2. 测试股票数据可用性: {ts_code}")
    print("=" * 80)
    
    # 使用5年作为测试
    required_years = 5
    start_year, end_year = calculate_recent_years(required_years=required_years)
    
    print(f"\n年份范围: {start_year}-{end_year} (需要{required_years}年)")
    print(f"日期范围: {start_year}0101 - {end_year}1231")
    
    # 获取基本面数据
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=required_years,
        use_cache=False,  # 不使用缓存，确保获取最新数据
        api_delay=0.5
    )
    
    if not result:
        print(f"❌ 无法获取 {ts_code} 的数据")
        return None
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"\n✅ 数据获取成功")
    print(f"   - 审计记录数: {len(audit_records)}")
    print(f"   - 财务指标行数: {len(metrics) if metrics is not None and not metrics.empty else 0}")
    
    if audit_records:
        print(f"\n   审计记录详情:")
        for record in audit_records[:10]:
            print(f"      {record.end_date[:4]}年: {record.audit_result} ({'✅标准' if record.is_standard else '❌非标准'})")
    
    if metrics is not None and not metrics.empty:
        print(f"\n   财务指标年份:")
        years_in_data = [row['end_date'][:4] for _, row in metrics.iterrows()]
        print(f"      {years_in_data}")
        print(f"      共{len(years_in_data)}年，{'✅' if len(years_in_data) >= required_years else '❌不足'} (需要{required_years}年)")
        
        # 显示每年的关键指标
        print(f"\n   现金流情况:")
        for _, row in metrics.head(required_years).iterrows():
            year = row['end_date'][:4]
            ocf = row.get('n_cashflow_act', 0)
            profit = row.get('n_income', 0)
            positive = row.get('cashflow_positive', False)
            cover = row.get('cashflow_ge_profit', False)
            print(f"      {year}年: 经营现金流={ocf/10000:.2f}万 净利润={profit/10000:.2f}万 [{'✅' if positive else '❌'}正 {'✅' if cover else '❌'}覆盖]")
    
    return result

def test_fundamental_checks(ts_code: str = "000429.SZ"):
    """测试基本面筛选条件"""
    print("\n" + "=" * 80)
    print(f"3. 测试基本面筛选: {ts_code}")
    print("=" * 80)
    
    required_years = 5
    start_year, end_year = calculate_recent_years(required_years=required_years)
    
    # 获取数据
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=required_years,
        use_cache=False,
        api_delay=0.5
    )
    
    if not result:
        print(f"❌ 无法获取数据")
        return
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    # 使用筛选器检查
    screener = StockScreener()
    fundamentals_pass, details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=required_years
    )
    
    print(f"\n基本面筛选结果: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"\n详细信息:")
    print(f"   - 数据完整性: {'✅' if details.get('data_sufficiency_pass') else '❌'} {details.get('data_sufficiency_msg', '')}")
    print(f"   - 审计意见: {'✅' if details.get('audit_pass') else '❌'}")
    print(f"   - 现金流≥0: {'✅' if details.get('cashflow_pass') else '❌'}")
    print(f"   - 现金流覆盖利润: {'✅' if details.get('cashflow_ge_profit') else '❌'}")
    
    if not details.get('data_sufficiency_pass'):
        print(f"\n⚠️ 数据完整性检查失败！这可能是导致零通过的主要原因。")
        print(f"   原因: {details.get('data_sufficiency_msg')}")
    
    return fundamentals_pass, details

def test_valuation_checks(ts_code: str = "000429.SZ"):
    """测试估值筛选条件"""
    print("\n" + "=" * 80)
    print(f"4. 测试估值筛选: {ts_code}")
    print("=" * 80)
    
    screener = StockScreener()
    
    # 使用常见的筛选参数
    pr_threshold = 1.0
    min_roe = 10.0
    
    print(f"\n筛选参数: PR≤{pr_threshold}, ROE≥{min_roe}%")
    
    valuation_pass, details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=pr_threshold,
        min_roe=min_roe
    )
    
    print(f"\n估值筛选结果: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"\n详细信息:")
    print(f"   - PR: {details.get('final_pr', 'N/A')}")
    print(f"   - ROE: {details.get('roe_waa', 'N/A')}%")
    print(f"   - PE_TTM: {details.get('pe_ttm', 'N/A')}")
    
    return valuation_pass, details

def test_multiple_stocks():
    """测试多只股票找出模式"""
    print("\n" + "=" * 80)
    print("5. 批量测试多只股票")
    print("=" * 80)
    
    # 测试多只股票（包括用户提到的"粤速A"）
    test_stocks = [
        "000429.SZ",  # 粤高速A
        "600036.SH",  # 招商银行
        "000858.SZ",  # 五粮液
        "600519.SH",  # 贵州茅台
        "000001.SZ"   # 平安银行
    ]
    
    required_years = 5
    start_year, end_year = calculate_recent_years(required_years=required_years)
    
    results = []
    
    for ts_code in test_stocks:
        print(f"\n测试 {ts_code}...")
        try:
            result = analyze_fundamentals(
                ts_code=ts_code,
                start_date=f"{start_year}0101",
                end_date=f"{end_year}1231",
                years=required_years,
                use_cache=False,
                api_delay=0.5
            )
            
            if result:
                metrics = result.get('metrics')
                years_found = len(metrics) if metrics is not None and not metrics.empty else 0
                audit_count = len(result.get('audit_records', []))
                
                screener = StockScreener()
                fundamentals_pass, fund_details = screener.check_fundamentals_pass(
                    audit_records=result.get('audit_records', []),
                    metrics=metrics,
                    required_years=required_years
                )
                
                results.append({
                    'ts_code': ts_code,
                    'years_found': years_found,
                    'audit_count': audit_count,
                    'data_sufficient': fund_details.get('data_sufficiency_pass'),
                    'fundamentals_pass': fundamentals_pass
                })
                
                status = "✅" if fundamentals_pass else "❌"
                print(f"   {status} 数据年数: {years_found}, 审计记录: {audit_count}, 数据完整: {fund_details.get('data_sufficiency_pass')}")
            else:
                print(f"   ❌ 无法获取数据")
                results.append({
                    'ts_code': ts_code,
                    'years_found': 0,
                    'audit_count': 0,
                    'data_sufficient': False,
                    'fundamentals_pass': False
                })
        except Exception as e:
            print(f"   ❌ 错误: {e}")
            results.append({
                'ts_code': ts_code,
                'error': str(e)
            })
    
    # 汇总分析
    print("\n" + "=" * 80)
    print("汇总分析")
    print("=" * 80)
    
    data_sufficient_count = sum(1 for r in results if r.get('data_sufficient'))
    fundamentals_pass_count = sum(1 for r in results if r.get('fundamentals_pass'))
    
    print(f"\n测试股票数: {len(test_stocks)}")
    print(f"数据完整的股票: {data_sufficient_count}/{len(test_stocks)}")
    print(f"通过基本面筛选的股票: {fundamentals_pass_count}/{len(test_stocks)}")
    
    if data_sufficient_count == 0:
        print(f"\n⚠️ 所有测试股票的数据都不完整！")
        print(f"   这很可能是导致'零通过'的根本原因。")
        print(f"   原因可能是: 2024年的年报尚未发布，导致只有4年数据而非5年。")

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("🔍 全网筛选零通过问题调试")
    print("=" * 80)
    print(f"当前时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print()
    
    # 测试1: 年份计算
    test_year_calculation()
    
    # 测试2: 单个股票数据
    test_stock_data_availability("000429.SZ")
    
    # 测试3: 基本面检查
    test_fundamental_checks("000429.SZ")
    
    # 测试4: 估值检查
    test_valuation_checks("000429.SZ")
    
    # 测试5: 批量测试
    test_multiple_stocks()
    
    print("\n" + "=" * 80)
    print("调试完成")
    print("=" * 80)
