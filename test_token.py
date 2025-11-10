# -*- coding: utf-8 -*-
"""测试新Token是否可用"""

import tushare as ts
from settings import DEFAULT_TOKEN

def test_token():
    """测试Token连通性"""
    print("=" * 50)
    print("开始测试 Tushare Token...")
    print(f"Token: {DEFAULT_TOKEN[:20]}...{DEFAULT_TOKEN[-10:]}")
    print("=" * 50)
    
    try:
        # 初始化
        pro = ts.pro_api(DEFAULT_TOKEN)
        print("✅ Token 初始化成功")
        
        # 测试1：获取交易日历
        print("\n测试1：获取交易日历...")
        df = pro.trade_cal(limit=5)
        print(f"✅ 成功获取 {len(df)} 条交易日历记录")
        print(df.head())
        
        # 测试2：获取股票基本信息
        print("\n测试2：获取股票基本信息...")
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        print(f"✅ 成功获取 {len(df)} 只股票基本信息")
        print(df.head())
        
        # 测试3：获取茅台的审计意见（这个接口权限要求较高）
        print("\n测试3：获取审计意见（测试高级接口）...")
        try:
            df = pro.fina_audit(ts_code='600519.SH', limit=3)
            if df.empty:
                print("⚠️  审计意见数据为空（可能需要更高权限）")
            else:
                print(f"✅ 成功获取 {len(df)} 条审计意见")
                print(df[['end_date', 'audit_result', 'audit_agency']])
        except Exception as e:
            print(f"⚠️  审计意见接口失败: {e}")
        
        # 测试4：获取财务数据
        print("\n测试4：获取资产负债表...")
        try:
            df = pro.balancesheet(ts_code='600519.SH', period='20221231', fields='end_date,total_assets,total_liab')
            if df.empty:
                print("⚠️  资产负债表数据为空")
            else:
                print(f"✅ 成功获取资产负债表")
                print(df)
        except Exception as e:
            print(f"⚠️  资产负债表接口失败: {e}")
        
        print("\n" + "=" * 50)
        print("🎉 Token 测试完成！基本功能正常")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Token 测试失败: {e}")
        print("\n可能的原因：")
        print("1. Token 无效或已过期")
        print("2. 网络连接问题")
        print("3. Tushare 服务器问题")
        print("\n请访问 https://tushare.pro/user/token 检查Token状态")
        return False

if __name__ == "__main__":
    test_token()

