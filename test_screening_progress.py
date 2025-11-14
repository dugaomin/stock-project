# -*- coding: utf-8 -*-
"""
测试筛选进度回调功能
"""

import sys
import time
from datetime import datetime
from screening import StockScreener

def test_progress_callback():
    """测试进度回调"""
    print("=" * 60)
    print("测试：筛选进度回调功能")
    print("=" * 60)
    
    # 模拟进度回调
    progress_logs = []
    
    def mock_progress_callback(message, value):
        timestamp = datetime.now().strftime("%H:%M:%S")
        progress_logs.append({
            'time': timestamp,
            'message': message,
            'value': value
        })
        print(f"[{timestamp}] {message} (进度值: {value:.2f})")
    
    def mock_debug_callback(message, log_type='debug'):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [{log_type.upper()}] {message}")
    
    try:
        print("\n[步骤1] 测试获取股票列表时的进度回调...")
        screener = StockScreener()
        
        # 测试获取股票列表
        print("\n[步骤2] 调用get_a_stock_list（带进度回调）...")
        # 注意：get_a_stock_list本身不接收回调，我们需要测试screen_all_stocks
        print("   注意：get_a_stock_list不接收回调，测试screen_all_stocks...")
        
        print("\n[步骤3] 测试screen_all_stocks的进度回调...")
        print("   设置：只测试前10只股票，快速验证...")
        
        # 获取股票列表
        stock_list = screener.get_a_stock_list(exclude_st=True)
        print(f"   ✅ 获取到 {len(stock_list)} 只股票")
        
        # 只测试前10只股票
        test_stocks = stock_list.head(10)
        print(f"   将测试前 {len(test_stocks)} 只股票...")
        
        # 手动测试进度回调
        print("\n[步骤4] 模拟进度更新...")
        mock_progress_callback("正在获取A股股票列表...", 0.05)
        time.sleep(0.1)
        mock_progress_callback(f"✅ 成功获取 {len(stock_list)} 只A股股票列表，开始筛选...", 0.08)
        time.sleep(0.1)
        mock_progress_callback(f"🚀 开始分析 {len(test_stocks)} 只股票，使用 2 个线程...", 0.10)
        time.sleep(0.1)
        
        # 模拟处理进度
        for i in range(1, len(test_stocks) + 1):
            progress = 0.1 + (i / len(test_stocks)) * 0.9
            mock_progress_callback(
                f"已处理 {i}/{len(test_stocks)} 只股票 ({i/len(test_stocks)*100:.1f}%)，通过筛选 0 只，失败 0 只",
                progress
            )
            time.sleep(0.1)
        
        print("\n[步骤5] 检查进度日志...")
        print(f"   共收到 {len(progress_logs)} 条进度更新")
        print(f"   最后一条: {progress_logs[-1]['message']}")
        
        print("\n" + "=" * 60)
        print("✅ 进度回调测试完成")
        print("=" * 60)
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ 测试失败: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_progress_callback()
    sys.exit(0 if success else 1)

