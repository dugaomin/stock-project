#!/usr/bin/env python3
"""
详细诊断600519基本面筛选未通过的原因
"""

from datetime import datetime
from utils import analyze_fundamentals
from screening import StockScreener

def diagnose_600519():
    """
    详细分析600519的基本面数据，找出未通过的具体原因
    """
    print("=" * 80)
    print("🔍 详细诊断600519基本面筛选")
    print("=" * 80)
    print(f"当前时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
    
    ts_code = "600519.SH"
    
    # 获取所有数据
    print("【步骤1】获取600519的财务数据（所有可用数据）...")
    result = analyze_fundamentals(
        ts_code=ts_code,
        start_date="19900101",
        end_date="29991231",
        years=5,
        use_cache=False,  # 禁用缓存，避免增量更新问题
        api_delay=0.1
    )
    
    if not result:
        print("❌ 无法获取数据")
        return
    
    audit_records = result.get('audit_records', [])
    metrics = result.get('metrics')
    
    print(f"\n✅ 数据获取成功")
    print(f"   审计记录数: {len(audit_records)}")
    print(f"   财务指标行数: {len(metrics) if metrics is not None and not metrics.empty else 0}\n")
    
    if metrics is None or metrics.empty:
        print("❌ 无财务指标数据")
        return
    
    # 显示最近的年份数据
    years_in_data = [row['end_date'][:4] for _, row in metrics.iterrows()]
    print(f"📅 财务数据年份: {years_in_data}")
    print(f"   共{len(years_in_data)}年\n")
    
    # 取最近5年进行分析
    recent_5_years = metrics.head(5)
    required_years = 5
    
    print("=" * 80)
    print("【步骤2】基本面筛选 - 逐项检查")
    print("=" * 80)
    
    # 1. 数据完整性
    print(f"\n1️⃣ 数据完整性检查")
    print(f"   需要: {required_years}年")
    print(f"   实际: {len(metrics)}年")
    print(f"   结果: {'✅ 通过' if len(metrics) >= required_years else '❌ 未通过'}\n")
    
    # 2. 审计意见
    print(f"2️⃣ 审计意见检查（最近{required_years}年）")
    recent_audits = audit_records[:required_years]
    for record in recent_audits:
        status = '✅' if record.is_standard else '❌'
        print(f"   {status} {record.end_date[:4]}年: {record.audit_result}")
    all_standard = all(record.is_standard for record in recent_audits)
    print(f"   结果: {'✅ 全部为标准无保留意见' if all_standard else '❌ 存在非标准意见'}\n")
    
    # 3. 现金流≥0检查
    print(f"3️⃣ 现金流≥0检查（最近{required_years}年）")
    cashflow_positive_count = 0
    for _, row in recent_5_years.iterrows():
        year = row['end_date'][:4]
        ocf = row.get('n_cashflow_act', 0)
        is_positive = row.get('cashflow_positive', False)
        status = '✅' if is_positive else '❌'
        print(f"   {status} {year}年: 经营现金流 = {ocf/100000000:.2f}亿")
        if is_positive:
            cashflow_positive_count += 1
    
    all_positive = cashflow_positive_count == len(recent_5_years)
    print(f"   统计: {cashflow_positive_count}/{len(recent_5_years)}年为正")
    print(f"   结果: {'✅ 全部为正' if all_positive else '❌ 存在负值年份'}\n")
    
    # 4. 现金流覆盖利润检查（关键检查）
    print(f"4️⃣ 现金流覆盖利润检查（最近{required_years}年）⭐⭐⭐")
    print(f"   要求: 经营现金流 ≥ 净利润\n")
    
    cover_profit_count = 0
    failed_years = []
    
    for _, row in recent_5_years.iterrows():
        year = row['end_date'][:4]
        ocf = row.get('n_cashflow_act', 0)
        profit = row.get('n_income', 0)
        is_cover = row.get('cashflow_ge_profit', False)
        
        ocf_billion = ocf / 100000000
        profit_billion = profit / 100000000
        diff = ocf_billion - profit_billion
        
        status = '✅' if is_cover else '❌'
        print(f"   {status} {year}年:")
        print(f"      经营现金流: {ocf_billion:>10.2f}亿")
        print(f"      净利润:     {profit_billion:>10.2f}亿")
        print(f"      差额:       {diff:>10.2f}亿 ({'+' if diff >= 0 else '-'}{abs(diff/profit_billion)*100:.1f}%)")
        
        if is_cover:
            cover_profit_count += 1
            print(f"      ✅ 现金流覆盖利润")
        else:
            failed_years.append(year)
            print(f"      ❌ 现金流不足，少收了{abs(diff):.2f}亿")
        print()
    
    all_cover = cover_profit_count == len(recent_5_years)
    print(f"   统计: {cover_profit_count}/{len(recent_5_years)}年覆盖")
    print(f"   结果: {'✅ 全部覆盖' if all_cover else f'❌ 存在{len(failed_years)}年未覆盖'}")
    
    if failed_years:
        print(f"\n   ⚠️  未覆盖年份: {', '.join(failed_years)}")
        print(f"   说明: 这些年份账面利润很高，但收到的现金不够")
        print(f"        可能原因: 应收账款增加、存货积压等")
    
    print("\n" + "=" * 80)
    print("【最终判断】")
    print("=" * 80)
    
    # 使用筛选器进行官方判断
    screener = StockScreener()
    fundamentals_pass, fund_details = screener.check_fundamentals_pass(
        audit_records=audit_records,
        metrics=metrics,
        required_years=required_years
    )
    
    print(f"\n基本面筛选: {'✅ 通过' if fundamentals_pass else '❌ 未通过'}\n")
    
    print("各项检查:")
    print(f"   ✅ 数据完整性: {fund_details.get('data_sufficiency_pass')}")
    print(f"   {'✅' if fund_details.get('audit_pass') else '❌'} 审计意见: {fund_details.get('audit_pass')}")
    print(f"   {'✅' if fund_details.get('cashflow_pass') else '❌'} 现金流≥0: {fund_details.get('cashflow_pass')}")
    print(f"   {'✅' if fund_details.get('cashflow_ge_profit') else '❌'} 现金流覆盖利润: {fund_details.get('cashflow_ge_profit')}")
    
    print("\n" + "=" * 80)
    if not fundamentals_pass:
        print("💡 结论：")
        if not fund_details.get('cashflow_ge_profit'):
            print(f"   600519未通过基本面筛选的原因是：")
            print(f"   最近5年中，有{len(failed_years)}年的经营现金流未能覆盖净利润")
            print(f"   ")
            print(f"   这是筛选条件的严格要求：")
            print(f"   不仅要\"赚钱\"（净利润），还要\"收到钱\"（现金流≥利润）")
            print(f"   ")
            print(f"   虽然600519是优质公司，但某些年份的现金回收效率不够")
            print(f"   导致无法通过这个严格的现金流质量筛选")
    print("=" * 80)

if __name__ == "__main__":
    diagnose_600519()
