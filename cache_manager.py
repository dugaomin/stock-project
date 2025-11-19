# -*- coding: utf-8 -*-
"""缓存管理模块 - 持久化缓存Tushare数据"""

import os
import json
import time
from typing import Any, Optional
from datetime import datetime, timedelta


class DataCache:
    """
    数据缓存管理器
    
    功能：
    1. 提供基于文件的键值对缓存
    2. 支持设置过期时间
    3. 自动处理Numpy/Pandas数据类型的序列化
    4. 线程安全的文件写入（原子操作）
    5. 防止路径遍历攻击
    """
    
    def __init__(self, cache_dir: str = "data/cache", expire_hours: int = 365 * 24):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录，默认为 data/cache
            expire_hours: 默认缓存过期时间(小时),默认为365天(8760小时) - 接近永久缓存
        """
        self.cache_dir = cache_dir
        self.expire_seconds = expire_hours * 3600
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        """
        获取缓存文件路径（私有方法）
        
        安全机制：
        1. 验证key非空
        2. 对过长key进行MD5哈希
        3. 替换非法字符
        4. 使用os.path.basename防止路径遍历
        5. 处理Windows保留文件名
        
        参数:
            key: 缓存键
            
        返回:
            安全的缓存文件绝对路径
        """
        import re
        import hashlib
        
        # 1. 验证key不为空
        if not key or not key.strip():
            raise ValueError("缓存键不能为空")
        
        # 2. 处理过长文件名（Windows限制255字符，Linux限制255字节）
        if len(key) > 200:
            # 使用hash避免过长文件名
            key_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
            safe_key = f"{key[:50]}_{key_hash}"
        else:
            # 替换所有非字母数字字符为下划线，保留连字符和下划线
            safe_key = re.sub(r'[^a-zA-Z0-9_-]', '_', key)
        
        # 3. 防止路径遍历攻击（确保只使用文件名，不包含路径分隔符）
        safe_key = os.path.basename(safe_key)
        
        # 4. Windows保留名检查（CON, PRN, AUX等）
        reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 
                          'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
                          'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
        if safe_key.upper() in reserved_names:
            safe_key = f"_{safe_key}"
        
        return os.path.join(self.cache_dir, f"{safe_key}.json")
    
    def get_cache_file_path(self, key: str) -> str:
        """
        获取缓存文件路径（公共方法）
        
        参数:
            key: 缓存键
            
        返回:
            缓存文件路径
        """
        return self._get_cache_path(key)
    
    def get(self, key: str) -> Optional[Any]:
        """
        从缓存获取数据
        
        参数:
            key: 缓存键
            
        返回:
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
        
        特性：
        1. 自动转换numpy/pandas类型为Python原生类型
        2. 使用临时文件+重命名实现原子写入，防止文件损坏
        
        参数:
            key: 缓存键
            data: 要缓存的数据
            
        返回:
            是否保存成功
        """
        cache_path = self._get_cache_path(key)
        
        try:
            # 转换numpy/pandas类型为Python原生类型（修复JSON序列化问题）
            def convert_to_native(obj):
                """递归转换numpy/pandas类型为Python原生类型"""
                import numpy as np
                import pandas as pd
                
                if isinstance(obj, (np.integer, np.int64, np.int32)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64, np.float32)):
                    return float(obj) if not pd.isna(obj) else None
                elif isinstance(obj, np.bool_):
                    return bool(obj)
                elif isinstance(obj, pd.Timestamp):
                    return obj.isoformat()
                elif isinstance(obj, dict):
                    return {k: convert_to_native(v) for k, v in obj.items()}
                elif isinstance(obj, (list, tuple)):
                    return [convert_to_native(item) for item in obj]
                elif pd.isna(obj):
                    return None
                else:
                    return obj
            
            # 转换数据
            converted_data = convert_to_native(data)
            
            cache_data = {
                'timestamp': time.time(),
                'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data': converted_data
            }
            
            # 使用临时文件写入，成功后再替换（避免写入中断导致文件损坏）
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            temp_path = cache_path + '.tmp'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            # 原子替换（避免并发写入问题）
            os.replace(temp_path, cache_path)
            
            return True
            
        except Exception as e:
            print(f"保存缓存失败 {key}: {e}")
            # 清理临时文件（如果存在）
            temp_path = cache_path + '.tmp'
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return False
    
    def delete(self, key: str) -> bool:
        """
        删除指定缓存
        
        参数:
            key: 缓存键
            
        返回:
            是否删除成功
        """
        cache_path = self._get_cache_path(key)
        
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return True
        except Exception as e:
            print(f"删除缓存失败 {key}: {e}")
            return False
    
    def clear_all(self) -> int:
        """
        清空所有缓存文件
        
        返回:
            删除的文件数量
        """
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
        """
        清理所有过期缓存
        
        返回:
            清理的文件数量
        """
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
        """
        获取缓存统计信息（按类型分类）
        
        返回:
            包含总数、有效数、过期数、大小、过期时间配置的字典
        """
        total = 0
        valid = 0
        expired = 0
        total_size = 0
        
        # 按类型分类统计
        financial_valid = 0  # 财务数据缓存（有效）
        financial_expired = 0  # 财务数据缓存（过期）
        company_valid = 0  # 公司信息缓存（有效）
        company_expired = 0  # 公司信息缓存（过期）
        user_valid = 0  # 用户积分缓存（有效）
        user_expired = 0  # 用户积分缓存（过期）
        
        try:
            for filename in os.listdir(self.cache_dir):
                # 只统计.json文件，排除临时文件（.tmp）和隐藏文件
                if not filename.endswith('.json') or filename.endswith('.tmp.json'):
                    continue
                
                # 排除临时文件（原子写入时使用的.tmp文件）
                if filename.endswith('.tmp'):
                    continue
                
                cache_path = os.path.join(self.cache_dir, filename)
                
                # 检查文件是否存在且可读
                if not os.path.exists(cache_path):
                    continue
                
                try:
                    file_size = os.path.getsize(cache_path)
                    total += 1
                    total_size += file_size
                    
                    # 尝试读取缓存文件
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    
                    cached_time = cache_data.get('timestamp', 0)
                    
                    # 判断缓存类型和有效期
                    is_expired = False
                    if cached_time == 0:
                        is_expired = True
                    else:
                        # 根据缓存类型判断有效期
                        if filename.startswith('company_info_'):
                            # 公司信息缓存：30天有效期
                            expire_seconds = 30 * 24 * 3600
                        elif filename.startswith('user_points_'):
                            # 用户积分缓存：24小时有效期
                            expire_seconds = 24 * 3600
                        else:
                            # 财务数据缓存：24小时有效期（默认）
                            expire_seconds = self.expire_seconds
                        
                        if time.time() - cached_time > expire_seconds:
                            is_expired = True
                    
                    # 按类型分类统计
                    if filename.startswith('company_info_'):
                        if is_expired:
                            company_expired += 1
                            expired += 1
                        else:
                            company_valid += 1
                            valid += 1
                    elif filename.startswith('user_points_'):
                        if is_expired:
                            user_expired += 1
                            expired += 1
                        else:
                            user_valid += 1
                            valid += 1
                    else:
                        # 财务数据缓存或其他
                        if is_expired:
                            financial_expired += 1
                            expired += 1
                        else:
                            financial_valid += 1
                            valid += 1
                            
                except (json.JSONDecodeError, IOError, OSError) as e:
                    # 文件损坏、正在写入或读取失败，认为是过期缓存
                    # 根据文件名判断类型
                    if filename.startswith('company_info_'):
                        company_expired += 1
                    elif filename.startswith('user_points_'):
                        user_expired += 1
                    else:
                        financial_expired += 1
                    expired += 1
                except Exception as e:
                    # 其他异常，也认为是过期缓存
                    if filename.startswith('company_info_'):
                        company_expired += 1
                    elif filename.startswith('user_points_'):
                        user_expired += 1
                    else:
                        financial_expired += 1
                    expired += 1
                    
        except Exception as e:
            print(f"获取缓存信息失败: {e}")
        
        return {
            'total': total,
            'valid': valid,
            'expired': expired,
            'size_mb': round(total_size / 1024 / 1024, 2),
            'expire_hours': self.expire_seconds / 3600,
            # 按类型分类统计
            'by_type': {
                'financial': {'valid': financial_valid, 'expired': financial_expired, 'total': financial_valid + financial_expired},
                'company': {'valid': company_valid, 'expired': company_expired, 'total': company_valid + company_expired},
                'user': {'valid': user_valid, 'expired': user_expired, 'total': user_valid + user_expired},
            }
        }


# 全局缓存实例
data_cache = DataCache(cache_dir="data/cache", expire_hours=24)

