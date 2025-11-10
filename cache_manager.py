# -*- coding: utf-8 -*-
"""缓存管理模块 - 持久化缓存Tushare数据"""

import os
import json
import time
from typing import Any, Optional
from datetime import datetime, timedelta


class DataCache:
    """数据缓存管理器"""
    
    def __init__(self, cache_dir: str = "data/cache", expire_hours: int = 24):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录
            expire_hours: 缓存过期时间（小时）
        """
        self.cache_dir = cache_dir
        self.expire_seconds = expire_hours * 3600
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """获取缓存文件路径"""
        # 使用安全的文件名（替换特殊字符）
        safe_key = key.replace('/', '_').replace('\\', '_').replace(':', '_')
        return os.path.join(self.cache_dir, f"{safe_key}.json")
    
    def get(self, key: str) -> Optional[Any]:
        """
        从缓存获取数据
        
        Args:
            key: 缓存键
            
        Returns:
            缓存的数据，如果不存在或已过期则返回None
        """
        cache_path = self._get_cache_path(key)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查是否过期
            cached_time = cache_data.get('timestamp', 0)
            if time.time() - cached_time > self.expire_seconds:
                # 缓存已过期，删除文件
                os.remove(cache_path)
                return None
            
            return cache_data.get('data')
            
        except Exception as e:
            print(f"读取缓存失败 {key}: {e}")
            return None
    
    def set(self, key: str, data: Any) -> bool:
        """
        保存数据到缓存
        
        Args:
            key: 缓存键
            data: 要缓存的数据
            
        Returns:
            是否保存成功
        """
        cache_path = self._get_cache_path(key)
        
        try:
            cache_data = {
                'timestamp': time.time(),
                'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': data
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"保存缓存失败 {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """删除指定缓存"""
        cache_path = self._get_cache_path(key)
        
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return True
        except Exception as e:
            print(f"删除缓存失败 {key}: {e}")
            return False
    
    def clear_all(self) -> int:
        """清空所有缓存"""
        count = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.json'):
                    os.remove(os.path.join(self.cache_dir, filename))
                    count += 1
        except Exception as e:
            print(f"清空缓存失败: {e}")
        
        return count
    
    def clear_expired(self) -> int:
        """清理过期缓存"""
        count = 0
        try:
            for filename in os.listdir(self.cache_dir):
                if not filename.endswith('.json'):
                    continue
                
                cache_path = os.path.join(self.cache_dir, filename)
                
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    
                    cached_time = cache_data.get('timestamp', 0)
                    if time.time() - cached_time > self.expire_seconds:
                        os.remove(cache_path)
                        count += 1
                except:
                    # 如果读取失败，也删除
                    os.remove(cache_path)
                    count += 1
                    
        except Exception as e:
            print(f"清理过期缓存失败: {e}")
        
        return count
    
    def get_cache_info(self) -> dict:
        """获取缓存统计信息"""
        total = 0
        valid = 0
        expired = 0
        total_size = 0
        
        try:
            for filename in os.listdir(self.cache_dir):
                if not filename.endswith('.json'):
                    continue
                
                cache_path = os.path.join(self.cache_dir, filename)
                total += 1
                total_size += os.path.getsize(cache_path)
                
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    
                    cached_time = cache_data.get('timestamp', 0)
                    if time.time() - cached_time > self.expire_seconds:
                        expired += 1
                    else:
                        valid += 1
                except:
                    expired += 1
                    
        except Exception as e:
            print(f"获取缓存信息失败: {e}")
        
        return {
            'total': total,
            'valid': valid,
            'expired': expired,
            'size_mb': round(total_size / 1024 / 1024, 2),
            'expire_hours': self.expire_seconds / 3600
        }


# 全局缓存实例
data_cache = DataCache(cache_dir="data/cache", expire_hours=24)

