#!/usr/bin/env python3
"""
简化测试：直接测试几只股票的筛选流程
"""

from utils import calculate_recent_years, analyze_fundamentals
from screening import StockScreener

# 测试参数
required_years = 5
min_roe = 10.0
max_pr = 1.0

print("=" * 80)
print("简化筛选测试")
print("=" * 80)

# 计算年份范围
start_year, end_year = calculate_recent_years(required_years=required_years)
print(f"\n年份范围: {start_year}-{end_year} (需要{required_years}年)")
print(f"筛选参数: ROE≥{min_roe}%, PR≤{max_pr}")

# 测试股票列表
test_stocks = [
    ("000429.SZ", "粤高速A"),
    ("600036.SH", "招商银行"),
    ("600519.SH", "贵州茅台"),
]

screener = StockScreener()
passed_stocks = []
failed_stocks = []

for ts_code, name in test_stocks:
    print(f"\n{'='*80}")
    print(f"测试: {ts_code} ({name})")
    print(f"{'='*80}")
    
    try:
        # 1. 基本面分析
        result = analyze_fundamentals(
            ts_code=ts_code,
            start_date=f"{start_year}0101",
            end_date=f"{end_year}1231",
            years=required_years,
            use_cache=False,
            api_delay=0.5
        )
        
        if not result:
            print(f"❌ 无法获取基本面数据")
            failed_stocks.append((ts_code, name, "无基本面数据"))
            continue
        
        audit_records = result.get('audit_records', [])
        metrics = result.get('metrics')
        
        if metrics is None or metrics.empty:
            print(f"❌ 无财务指标数据")
            failed_stocks.append((ts_code, name, "无财务指标"))
            continue
        
        # 2. 基本面筛选
        fundamentals_pass, fund_details = screener.check_fundamentals_pass(
            audit_records=audit_records,
            metrics=metrics,
            required_years=required_years
        )
        
        print(f"\n基本面筛选: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
        print(f"  - 数据完整性: {'✅' if fund_details.get('data_sufficiency_pass') else '❌'} {fund_details.get('data_sufficiency_msg', '')}")
        print(f"  - 审计意见: {'✅' if fund_details.get('audit_pass') else '❌'}")
        print(f"  - 现金流≥0: {'✅' if fund_details.get('cashflow_pass') else '❌'}")
        print(f"  - 现金流覆盖利润: {'✅' if fund_details.get('cashflow_ge_profit') else '❌'}")
        
        if not fundamentals_pass:
            failed_stocks.append((ts_code, name, f"基本面不通过: {fund_details.get('data_sufficiency_msg', '')}"))
            continue
        
        # 3. 估值筛选
        valuation_pass, val_details = screener.check_valuation_pass(
            ts_code=ts_code,
            pr_threshold=max_pr,
            min_roe=min_roe
        )
        
        pr = val_details.get('final_pr')
        roe = val_details.get('roe_waa')
        
        print(f"\n估值筛选: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
        print(f"  - PR: {pr:.4f if pr is not None else 'N/A'} (要求≤{max_pr})")
        print(f"  - ROE: {roe:.2f if roe is not None else 'N/A'}% (要求≥{min_roe}%)")
        
        if not valuation_pass:
            failed_stocks.append((ts_code, name, f"估值不通过 PR={pr:.2f if pr else 'N/A'} ROE={roe:.2f if roe else 'N/A'}%"))
            continue
        
        # 通过所有检查
        passed_stocks.append({
            'ts_code': ts_code,
            'name': name,
            'pr': pr,
            'roe': roe
        })
        print(f"\n✅ **通过所有筛选**")
        
    except Exception as e:
        print(f"❌ 错误: {e}")
        failed_stocks.append((ts_code, name, f"异常: {str(e)}"))
        import traceback
        traceback.print_exc()

# 汇总结果
print(f"\n\n{'='*80}")
print("筛选结果汇总")
print(f"{'='*80}")

print(f"\n通过股票数: {len(passed_stocks)}/{len(test_stocks)}")
if passed_stocks:
    print("\n✅ 通过的股票:")
    for stock in passed_stocks:
        print(f"  - {stock['ts_code']} ({stock['name']}): PR={stock['pr']:.4f}, ROE={stock['roe']:.2f}%")

print(f"\n失败股票数: {len(failed_stocks)}/{len(test_stocks)}")
if failed_stocks:
    print("\n❌ 未通过的股票:")
    for ts_code, name, reason in failed_stocks:
        print(f"  - {ts_code} ({name}): {reason}")
