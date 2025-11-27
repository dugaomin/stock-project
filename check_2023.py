
import tushare as ts
from utils import get_pro_client

print("Checking 2023 Annual Report for 000429.SZ...")
pro = get_pro_client()

# Try fetching by specific period
try:
    df = pro.balancesheet(ts_code='000429.SZ', period='20231231')
    print(f"\n[Query by period='20231231']")
    if not df.empty:
        print("✅ Found 2023 data!")
        print(df[['ts_code', 'end_date', 'ann_date']].head())
    else:
        print("❌ No data found for period='20231231'")

    # Try fetching by date range with wider limits
    print(f"\n[Query by date range 20230101-20240501]")
    df_range = pro.balancesheet(ts_code='000429.SZ', start_date='20230101', end_date='20231231')
    if not df_range.empty:
        print(f"✅ Found {len(df_range)} records in range")
        print(df_range[['ts_code', 'end_date', 'ann_date']].head())
    else:
        print("❌ No data found in range")

except Exception as e:
    print(f"Error: {e}")
