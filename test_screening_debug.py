#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试筛选逻辑调试脚本
用于诊断为什么全网筛选返回0通过结果
"""

import sys
from utils import analyze_fundamentals, calculate_recent_years
from screening import StockScreener

def test_single_stock(ts_code="000429.SZ", name="粤高速A"):
    """测试单只股票的筛选过程"""
    print(f"\n{'='*60}")
    print(f"测试股票: {ts_code} ({name})")
    print(f"{'='*60}\n")
    
    # 1. 计算年份范围
    required_years = 5
    start_year, end_year = calculate_recent_years(required_years=required_years)
    print(f"✅ 年份范围: {start_year}-{end_year}")
    
    # 2. 调用基本面分析
    print(f"\n{'─'*60}")
    print("📊 步骤1: 基本面分析")
    print(f"{'─'*60}")
    
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date=f"{start_year}0101",
        end_date=f"{end_year}1231",
        years=required_years,
        use_cache=True,
        api_delay=0.0,
        max_workers=1,
        user_points=None
    )
    
    if not result:
        print("❌ analyze_fundamentals 返回 None")
        return
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"  - 审计记录数: {len(audit_records)}")
    if audit_records:
        # Handle both dict and object types
        years = []
        opinions = []
        for r in audit_records:
            if hasattr(r, 'end_date'):
                years.append(r.end_date[:4])
                opinions.append(getattr(r, 'audit_result', ''))
            else:
                years.append(r.get('end_date', '')[:4])
                opinions.append(r.get('audit_result', ''))
        print(f"  - 审计记录年份: {years}")
        print(f"  - 审计意见: {opinions}")
    
    if metrics is not None and not metrics.empty:
        print(f"  - 财务指标记录数: {len(metrics)}")
        print(f"  - 财务指标年份: {sorted(metrics['end_date'].str[:4].unique().tolist())}")
    else:
        print("❌ metrics 为空或 None")
        return
    
    # 3. 基本面检查
    print(f"\n{'─'*60}")
    print("🔍 步骤2: 基本面检查")
    print(f"{'─'*60}")
    
    screener = StockScreener()
    fundamentals_pass, fundamentals_details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=required_years
    )
    
    print(f"  - 基本面检查结果: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}")
    print(f"  - 详细信息:")
    for key, value in fundamentals_details.items():
        print(f"    • {key}: {value}")
    
    if not fundamentals_pass:
        print("\n⚠️ 基本面检查未通过，无法继续估值检查")
        return
    
    # 4. 估值检查
    print(f"\n{'─'*60}")
    print("💰 步骤3: 估值检查")
    print(f"{'─'*60}")
    
    min_roe = 10.0
    max_pr = 1.0
    
    valuation_pass, valuation_details = screener.check_valuation_pass(
        ts_code=ts_code,
        pr_threshold=max_pr,
        min_roe=min_roe
    )
    
    print(f"  - 估值检查结果: {'✅ 通过' if valuation_pass else '❌ 未通过'}")
    print(f"  - 详细信息:")
    for key, value in valuation_details.items():
        if isinstance(value, float):
            print(f"    • {key}: {value:.4f}")
        else:
            print(f"    • {key}: {value}")
    
    # 5. 最终结论
    print(f"\n{'='*60}")
    if fundamentals_pass and valuation_pass:
        print("🎉 最终结果: ✅ 该股票通过所有筛选")
    else:
        print("❌ 最终结果: 该股票未通过筛选")
        if not fundamentals_pass:
            print("  原因: 基本面检查未通过")
        if not valuation_pass:
            print("  原因: 估值检查未通过")
    print(f"{'='*60}\n")

def test_multiple_stocks():
    """测试多只股票"""
    test_stocks = [
        ("000429.SZ", "粤高速A"),
        ("600519.SH", "贵州茅台"),
        ("000001.SZ", "平安银行"),
    ]
    
    for ts_code, name in test_stocks:
        try:
            test_single_stock(ts_code, name)
        except Exception as e:
            print(f"❌ 测试 {ts_code} ({name}) 时出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # 测试指定股票
        ts_code = sys.argv[1]
        name = sys.argv[2] if len(sys.argv) > 2 else "未知"
        test_single_stock(ts_code, name)
    else:
        # 测试默认股票列表
        print("🧪 多股票筛选测试\n")
        test_multiple_stocks()
