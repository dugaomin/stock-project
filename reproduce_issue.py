
import pandas as pd
from screening import StockScreener
import sys

# Mock get_a_stock_list to return a few stocks including 000429.SZ
def mock_get_a_stock_list(self, exclude_st=True):
    print("Mocking get_a_stock_list...")
    data = {
        'ts_code': ['000429.SZ', '000001.SZ'],
        'symbol': ['000429', '000001'],
        'name': ['粤速A', '平安银行'],
        'area': ['深圳', '深圳'],
        'industry': ['公路', '银行'],
        'list_date': ['19980101', '19910403']
    }
    return pd.DataFrame(data)

# Patch the method
StockScreener.get_a_stock_list = mock_get_a_stock_list

def test_screen_all_stocks():
    print("Initializing StockScreener...")
    screener = StockScreener()
    
    print("Calling screen_all_stocks...")
    try:
        results = screener.screen_all_stocks(
            pr_threshold=1.0,
            min_roe=10.0,
            max_workers=2
        )
        print(f"Screening completed. Passed: {len(results)}")
        for r in results:
            print(f"Passed: {r['ts_code']}")
            
    except Exception as e:
        print(f"Caught exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_screen_all_stocks()
