# -*- coding: utf-8 -*-
"""
测试获取A股股票列表功能
"""

import sys
import traceback
from screening import StockScreener
from utils import get_pro_client

def test_get_stock_list():
    """测试获取股票列表"""
    print("=" * 60)
    print("测试：获取A股股票列表")
    print("=" * 60)
    
    try:
        # 1. 测试Tushare连接
        print("\n[步骤1] 测试Tushare连接...")
        pro = get_pro_client()
        print("✅ Tushare客户端初始化成功")
        
        # 2. 测试stock_basic接口
        print("\n[步骤2] 测试stock_basic接口...")
        try:
            test_df = pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            print(f"✅ stock_basic接口调用成功")
            print(f"   返回数据行数: {len(test_df)}")
            if not test_df.empty:
                print(f"   示例数据:")
                print(f"   {test_df.head(3).to_string()}")
        except Exception as e:
            print(f"❌ stock_basic接口调用失败: {e}")
            print(f"   错误详情:")
            traceback.print_exc()
            return False
        
        # 3. 测试StockScreener的get_a_stock_list方法
        print("\n[步骤3] 测试StockScreener.get_a_stock_list()...")
        screener = StockScreener()
        
        print("   测试：获取全部股票（包含ST股）...")
        df_all = screener.get_a_stock_list(exclude_st=False)
        print(f"   ✅ 获取成功，共 {len(df_all)} 只股票")
        if not df_all.empty:
            print(f"   前5只股票:")
            for idx, row in df_all.head(5).iterrows():
                print(f"     {row['ts_code']} - {row['name']} ({row['industry']})")
        
        print("\n   测试：获取股票（排除ST股）...")
        df_no_st = screener.get_a_stock_list(exclude_st=True)
        print(f"   ✅ 获取成功，共 {len(df_no_st)} 只股票（已排除ST股）")
        st_count = len(df_all) - len(df_no_st)
        print(f"   排除的ST股数量: {st_count}")
        
        if not df_no_st.empty:
            print(f"   前5只股票:")
            for idx, row in df_no_st.head(5).iterrows():
                print(f"     {row['ts_code']} - {row['name']} ({row['industry']})")
        
        # 4. 检查数据质量
        print("\n[步骤4] 检查数据质量...")
        if df_no_st.empty:
            print("❌ 警告：获取的股票列表为空！")
            return False
        
        # 检查必需字段
        required_fields = ['ts_code', 'name', 'industry', 'area']
        missing_fields = [f for f in required_fields if f not in df_no_st.columns]
        if missing_fields:
            print(f"❌ 缺少必需字段: {missing_fields}")
            return False
        else:
            print(f"✅ 所有必需字段都存在: {required_fields}")
        
        # 检查数据完整性
        null_counts = df_no_st[required_fields].isnull().sum()
        if null_counts.any():
            print(f"⚠️  发现空值:")
            print(null_counts[null_counts > 0])
        else:
            print(f"✅ 所有必需字段都没有空值")
        
        print("\n" + "=" * 60)
        print("✅ 测试通过！股票列表获取功能正常")
        print("=" * 60)
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ 测试失败: {e}")
        print("=" * 60)
        print("\n错误堆栈:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_get_stock_list()
    sys.exit(0 if success else 1)

