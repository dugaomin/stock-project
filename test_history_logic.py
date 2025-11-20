import json
import os
from datetime import datetime

class WatchlistHistoryManager:
    FILE_PATH = "test_history.json"
    
    @staticmethod
    def load_history():
        if os.path.exists(WatchlistHistoryManager.FILE_PATH):
            try:
                with open(WatchlistHistoryManager.FILE_PATH, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    @staticmethod
    def save_record(record):
        history = WatchlistHistoryManager.load_history()
        
        existing_record = None
        for item in history:
            if (item['date'] == record['date'] and 
                item['code'] == record['code'] and 
                item['signal_type'] == record['signal_type']):
                existing_record = item
                break
        
        if existing_record:
            print(f"Updating existing record. Old price: {existing_record['price']}, New price: {record['price']}")
            existing_record['last_time'] = record['time']
            existing_record['price'] = record['price']
            existing_record['trigger_count'] = existing_record.get('trigger_count', 1) + 1
        else:
            print("Creating new record.")
            record['trigger_count'] = 1
            record['last_time'] = record['time']
            history.append(record)
        
        history.sort(key=lambda x: x['time'], reverse=True)
        
        with open(WatchlistHistoryManager.FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        return True

# Test
if os.path.exists("test_history.json"):
    os.remove("test_history.json")

# 1. First record
rec1 = {
    "time": "10:00:00",
    "date": "20251120",
    "code": "000001.SZ",
    "signal_type": "gold_cross",
    "price": 10.0
}
WatchlistHistoryManager.save_record(rec1)

# Verify
with open("test_history.json", 'r') as f:
    data = json.load(f)
    print(f"After 1st save: Price={data[0]['price']}, Count={data[0]['trigger_count']}")

# 2. Update record (Price changes)
rec2 = {
    "time": "10:05:00",
    "date": "20251120",
    "code": "000001.SZ",
    "signal_type": "gold_cross",
    "price": 10.5
}
WatchlistHistoryManager.save_record(rec2)

# Verify
with open("test_history.json", 'r') as f:
    data = json.load(f)
    print(f"After 2nd save: Price={data[0]['price']}, Count={data[0]['trigger_count']}")

# Cleanup
if os.path.exists("test_history.json"):
    os.remove("test_history.json")
