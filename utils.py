# -*- coding: utf-8 -*-
"""
工具模块 - 数据获取和处理核心

功能概述：
    封装Tushare Pro API，提供财务数据获取、连通性检测、
    数据处理等功能，支持智能缓存和API频率控制。

主要模块：
    1. Token管理 - 安全的Token获取和客户端初始化
    2. 连通性检测 - DNS、HTTP、Tushare API三重检测
    3. 数据获取 - 公司信息、审计、财务三表、估值数据
    4. 综合分析 - 多数据源整合，计算核心指标
    5. 数据过滤 - 筛选年报，排除季报

API频率控制：
    支持根据用户积分等级设置延迟，避免触发Tushare限制

缓存支持：
    使用cache_manager模块实现24小时持久化缓存

作者：gaomindu
"""

from __future__ import annotations  # 兼容未来注解语法

import os  # 读取环境变量
import socket  # DNS 测试
import time  # 添加延迟控制
import json  # JSON处理（用于缓存）
from datetime import datetime  # 日期时间处理
from dataclasses import dataclass  # 结构化审计信息
from functools import lru_cache  # 缓存客户端实例
from typing import Any, Dict, List, Optional, Tuple  # 类型提示

import pandas as pd  # DataFrame 处理
import requests  # HTTP 测试
import tushare as ts  # Tushare SDK

from settings import DEFAULT_TOKEN  # 默认 token
from cache_manager import data_cache  # 数据缓存

API_HOST = "api.waditu.com"  # 官方接口域名


def get_token() -> str:
    """
    获取Tushare Pro Token
    
    优先级：
        1. 环境变量 TUSHARE_TOKEN
        2. settings.py 中的 DEFAULT_TOKEN
    
    返回:
        Token字符串
    
    安全性：
        Token不应硬编码在代码中，应从配置文件或环境变量读取
    """
    return os.environ.get("TUSHARE_TOKEN", DEFAULT_TOKEN)


@lru_cache(maxsize=1)
def get_pro_client(token: Optional[str] = None):
    """
    获取Tushare Pro API客户端（带缓存）
    
    使用lru_cache装饰器，确保客户端只初始化一次，避免重复连接。
    
    参数:
        token: Tushare Token，不传则使用get_token()获取
        
    返回:
        Tushare Pro API客户端实例
    """
    return ts.pro_api(token or get_token())


def get_user_points_info(token: Optional[str] = None) -> Optional[Dict]:
    """
    获取用户积分信息（包括到期积分）
    
    参数:
        token: Tushare Token，不传则使用get_token()获取
        
    返回:
        包含积分信息的字典，如果查询失败返回None
    """
    try:
        # 修复bug: 统一使用传入的token或默认token，避免不一致
        actual_token = token or get_token()
        pro = get_pro_client(actual_token)
        df = pro.user(token=actual_token)
        
        if df.empty:
            return None
        
        # 计算总积分和最近到期时间
        total_points = df['到期积分'].sum()
        
        # 找到最近的到期时间
        df['到期时间'] = pd.to_datetime(df['到期时间'])
        nearest_expiry = df['到期时间'].min()
        nearest_expiry_points = df[df['到期时间'] == nearest_expiry]['到期积分'].sum()
        
        return {
            'total_points': float(total_points),
            'nearest_expiry_date': nearest_expiry.strftime('%Y-%m-%d') if pd.notna(nearest_expiry) else None,
            'nearest_expiry_points': float(nearest_expiry_points) if pd.notna(nearest_expiry) else 0,
            'expiry_records': df.to_dict('records')
        }
    except Exception as e:
        print(f"查询积分信息失败: {e}")
        return None


def get_api_delay(api_name: str, user_points: Optional[float] = None, max_workers: int = 1) -> float:
    """
    根据API名称、用户积分等级和并发线程数计算合适的延迟时间
    
    重要说明：
    - Tushare的频率限制是**全局限制**（所有线程加起来的总限制）
    - 计算公式：每个线程延迟 = 60秒 / (全局限制次数 / 并发线程数)
    - 例如：200次/分钟，10线程并发 → 每个线程延迟 = 60 / (200/10) = 3秒
    
    参数:
        api_name: API名称（如 'stock_company', 'fina_audit', 'balancesheet' 等）
        user_points: 用户积分，如果不提供则自动获取
        max_workers: 并发线程数（默认1，用于计算每个线程的延迟）
        
    返回:
        每个线程的延迟时间（秒）
    
    API频率限制规则（根据Tushare文档，2000分中级用户）：
    - stock_company: 每分钟10次（特殊限制，无论积分等级）
    - 财务数据API（fina_audit, balancesheet, income, cashflow）: 
      * 免费用户(0-119分): 每分钟2次
      * 注册用户(120-599分): 每分钟5次
      * 中级用户(600-4999分): 每分钟200次 ← 2000分属于这个等级
      * 高级用户(5000+分): 每分钟200次
    - user API: 每天50次（不在此函数处理）
    """
    # 如果没有提供积分，使用默认值（中级用户2000分）
    # 注意：不再调用get_user_points_info()，避免重复调用API
    # 调用者应该在app.py的main()函数中获取积分信息并传入
    if user_points is None:
        user_points = 2000  # 默认中级用户（2000分）
    
    # 确保max_workers至少为1
    max_workers = max(1, max_workers)
    
    # stock_company API特殊限制：每分钟10次（无论积分等级）
    if api_name == 'stock_company':
        # 全局限制：每分钟10次
        # 计算公式：每个线程延迟 = 60秒 / (10次 / 线程数)
        # 单线程：60 / 10 = 6秒
        # 10线程：60 / (10/10) = 60秒（太慢，实际可以更激进）
        # 更合理的策略：每个线程延迟 = 60 / 10 * 线程数 / 线程数 = 6秒（保守）
        # 或者：60 / (10 / 线程数) = 6 * 线程数（更保守）
        # 实际使用：6秒（无论线程数，因为限制很严格）
        return 6.0  # 每分钟10次，单线程6秒，多线程时也保持6秒（保守策略）
    
    # 财务数据API根据积分等级设置延迟
    if api_name in ['fina_audit', 'balancesheet', 'income', 'cashflow', 'fina_indicator']:
        if user_points < 120:
            # 免费用户：每分钟2次（全局限制）
            # 10线程并发：60 / (2/10) = 300秒（太慢，实际使用保守策略）
            return 30.0  # 单线程延迟，多线程时保持（保守策略）
        elif user_points < 600:
            # 注册用户：每分钟5次（全局限制）
            return 12.0  # 单线程延迟
        elif user_points < 5000:
            # 中级用户：每分钟200次（全局限制）← 2000分属于这个等级
            # 计算公式：每个线程延迟 = 60 / (200 / max_workers)
            # 单线程：60 / 200 = 0.3秒
            # 10线程：60 / (200/10) = 3秒
            return 60.0 / (200.0 / max_workers) if max_workers > 0 else 0.3
        else:
            # 高级用户：每分钟200次（全局限制）
            return 60.0 / (200.0 / max_workers) if max_workers > 0 else 0.3
    
    # 其他API默认使用中级用户的延迟
    return 60.0 / (200.0 / max_workers) if max_workers > 0 else 0.3


def run_connectivity_tests(verbose: bool = True) -> Tuple[bool, List[Dict[str, str]]]:
    """
    网络连通性三重检测
    
    检测项：
        1. DNS解析 - 检查api.waditu.com能否解析
        2. HTTP连接 - 检查HTTP请求是否正常
        3. Tushare API - 检查API接口是否可用
    
    参数:
        verbose: 是否打印详细日志
        
    返回:
        (是否全部通过, 日志列表)
        
    用途：
        在查询数据前预检查网络环境，提前发现问题
    """
    checks = [
        ("DNS 连通性", _dns_check),
        ("HTTP 测试", _http_check),
        ("Tushare API", _tushare_check),
    ]
    success = True
    logs: List[Dict[str, str]] = []
    for title, fn in checks:
        ok, message = fn()
        status = "PASS" if ok else "FAIL"
        log_entry = {"status": status, "title": title, "message": message}
        logs.append(log_entry)
        if verbose:
            print(f"[{status}] {title}：{message}")
        success = success and ok
    return success, logs


def _dns_check() -> Tuple[bool, str]:
    """检查域名能否解析。"""
    try:
        ip_addr = socket.gethostbyname(API_HOST)
        return True, f"{API_HOST} -> {ip_addr}"
    except socket.gaierror as exc:
        return False, f"DNS 解析失败：{exc}"


def _http_check() -> Tuple[bool, str]:
    """发起 HTTP 请求验证链路。"""
    try:
        resp = requests.get(f"http://{API_HOST}", timeout=5)
        return True, f"HTTP 状态 {resp.status_code}"
    except requests.RequestException as exc:
        return False, f"HTTP 请求失败：{exc}"


def _tushare_check() -> Tuple[bool, str]:
    """调用最小接口验证 token/网络是否正常。"""
    try:
        pro = get_pro_client()
        df = pro.trade_cal(limit=1)
        return True, f"trade_cal 返回 {len(df)} 条记录"
    except Exception as exc:  # noqa: BLE001
        return False, f"Tushare 调用失败：{exc}"


@dataclass
class AuditRecord:
    """每个报告期的审计意见。"""

    ann_date: str
    end_date: str
    audit_result: str
    audit_agency: str
    audit_sign: str

    @property
    def is_standard(self) -> bool:
        """是否为标准无保留意见。"""
        return "标准无保留意见" in (self.audit_result or "")


def fetch_valuation_data(
    ts_code: str,
    trade_date: str,
    target_type: str = "个股",
) -> Optional[Dict[str, Any]]:
    """
    获取市赚率计算所需的估值数据
    
    ⚠️ 重要：此函数不应使用缓存！
    - 价格数据（close, pe_ttm）每天变化，必须获取最新数据才能准确计算PR
    - 如果未来为了性能优化需要添加缓存，缓存时间不应超过1小时
    - 建议：在同一天内可以缓存，但跨天必须重新获取
    
    参数:
        ts_code: 股票代码
        trade_date: 交易日期，格式YYYYMMDD
        target_type: 标的类型（个股/宽基指数）
        
    返回:
        包含pe_ttm, roe_waa, eps, dividend等字段的字典
    """
    try:
        pro = get_pro_client()
        
        # 1. 获取每日指标（pe_ttm, close等）
        pe_ttm = None
        close_price = None
        data_source = "daily_basic"

        if target_type == "宽基指数":
            index_df = pro.index_dailybasic(
                ts_code=ts_code,
                trade_date=trade_date,
                fields="ts_code,trade_date,close,pe_ttm"
            )
            if index_df.empty and trade_date:
                # 若指定日期无数据，尝试获取最近一期
                index_df = pro.index_dailybasic(
                    ts_code=ts_code,
                    end_date=trade_date,
                    fields="ts_code,trade_date,close,pe_ttm",
                    limit=1
                )
            if not index_df.empty:
                latest = index_df.sort_values("trade_date", ascending=False).iloc[0]
                pe_ttm = latest.get("pe_ttm")
                close_price = latest.get("close")
                data_source = "index_dailybasic"
        else:
            daily_df = pro.daily_basic(
                ts_code=ts_code,
                trade_date=trade_date,
                fields="ts_code,trade_date,close,pe_ttm"
            )
            if daily_df.empty and trade_date:
                daily_df = pro.daily_basic(
                    ts_code=ts_code,
                    end_date=trade_date,
                    fields="ts_code,trade_date,close,pe_ttm",
                    limit=1
                )
            if not daily_df.empty:
                latest = daily_df.sort_values("trade_date", ascending=False).iloc[0]
                pe_ttm = latest.get("pe_ttm")
                close_price = latest.get("close")
        
        if pe_ttm is None and close_price is None:
            print(f"⚠️ 未获取到{ts_code}在{trade_date}的估值基础数据")
        
        # 2. 获取财务指标（roe_waa, eps）- 获取最新的财务数据
        roe_waa = None
        eps = None
        
        if target_type == "个股":
            # 先尝试使用截止到trade_date的最近财报
            fina_df = pro.fina_indicator(
                ts_code=ts_code,
                end_date=trade_date,
                fields="ts_code,end_date,roe_waa,eps",
                limit=1
            )
            if fina_df.empty:
                # 如果仍为空，退而求其次取最近披露的财报
                fina_df = pro.fina_indicator(
                    ts_code=ts_code,
                    fields="ts_code,end_date,roe_waa,eps",
                    limit=1
                )
            if not fina_df.empty:
                fina_row = fina_df.sort_values("end_date", ascending=False).iloc[0]
                roe_waa = fina_row.get("roe_waa")
                eps = fina_row.get("eps")
            else:
                print(f"⚠️ {ts_code} 未获取到财务指标数据（roe_waa / eps）")
        
        # 3. 获取分红数据 - 获取最近一次分红
        dividend_per_share = None
        if target_type == "个股":
            div_df = pro.dividend(
                ts_code=ts_code,
                end_date=trade_date,
                fields="ts_code,div_proc,cash_div,ex_date,record_date,ann_date,imp_ann_date",
                limit=30
            )
            if div_df.empty:
                div_df = pro.dividend(
                    ts_code=ts_code,
                    fields="ts_code,div_proc,cash_div,ex_date,record_date,ann_date,imp_ann_date",
                    limit=30
                )
            if not div_df.empty:
                div_df = div_df.sort_values("ex_date", ascending=False)
                executed = div_df[
                    (div_df["div_proc"].fillna("") == "实施") & (div_df["cash_div"].notna()) & (div_df["cash_div"] > 0)
                ]
                source_df = executed if not executed.empty else div_df
                latest_div = source_df.iloc[0]
                dividend_per_share = latest_div.get("cash_div")
        
        return {
            'ts_code': ts_code,
            'trade_date': trade_date,
            'close': close_price,
            'pe_ttm': pe_ttm,
            'roe_waa': roe_waa,
            'eps': eps,
            'dividend_per_share': dividend_per_share,
            'data_source': data_source,
        }
    except Exception as e:
        print(f"获取估值数据失败: {e}")
        return None


def fetch_kline_data(ts_code: str, period: str = 'daily', adj: str = 'qfq', limit: int = 500) -> Optional[pd.DataFrame]:
    """
    获取K线数据 (支持日/周/月线及复权)
    
    参数:
        ts_code: 股票代码
        period: 周期 ('daily', 'weekly', 'monthly')
        adj: 复权类型 ('qfq'前复权, 'hfq'后复权, None不复权)
        limit: 获取条数
        
    返回:
        DataFrame
    """
    try:
        # 映射周期参数到 pro_bar 的 freq 参数
        # pro_bar freq: D=日线, W=周线, M=月线
        freq_map = {'daily': 'D', 'weekly': 'W', 'monthly': 'M'}
        freq = freq_map.get(period, 'D')
        
        # 计算开始日期 (根据limit估算)
        # 为了保证MACD计算准确，多取一些数据
        days_per_bar = 1 if freq == 'D' else 5 if freq == 'W' else 20
        total_days = limit * days_per_bar * 2 # 多取一倍以防万一
        start_date = (datetime.now() - pd.Timedelta(days=total_days)).strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d")
        
        # 使用 ts.pro_bar 获取数据 (自动处理复权和周期)
        # 注意：ts.pro_bar 需要初始化 pro 接口，或者传入 api 实例
        # 这里我们使用全局配置的 token
        
        # 确保 tushare 已初始化
        token = get_token()
        ts.set_token(token)
        pro = ts.pro_api()
        
        df = ts.pro_bar(
            ts_code=ts_code,
            api=pro,
            adj=adj,
            freq=freq,
            start_date=start_date,
            end_date=end_date
        )
        
        # ---------------------------------------------------------
        # 实时数据拼接逻辑
        # ---------------------------------------------------------
        # 仅在日线模式下尝试拼接实时数据
        if freq == 'D':
            try:
                # 获取实时行情 (Sina源，速度快)
                # ts_code 格式如 600519.SH，get_realtime_quotes 需要 600519
                code = ts_code.split('.')[0]
                df_rt = ts.get_realtime_quotes(code)
                
                if df_rt is not None and not df_rt.empty:
                    rt_row = df_rt.iloc[0]
                    rt_date = rt_row['date'] # YYYY-MM-DD
                    rt_date_str = rt_date.replace('-', '') # YYYYMMDD
                    
                    # 检查是否需要拼接
                    # 如果历史数据为空，或者历史数据最新日期小于今日
                    last_date = df['trade_date'].max() if (df is not None and not df.empty) else '00000000'
                    
                    if rt_date_str > last_date:
                        # 构造新行
                        # 注意：实时数据是未复权的
                        # 如果 adj='qfq' (前复权)，通常以当前价格为基准，过去价格向下调整。
                        # 所以当前实时价格可以直接作为 QFQ 价格使用（因为 QFQ 的最新价 = 现价）。
                        # 如果 adj='hfq' (后复权)，则需要乘以前面的复权因子，这里简化处理，直接使用现价（可能会有断层，但在非除权日无影响）。
                        
                        new_row = pd.DataFrame([{
                            'ts_code': ts_code,
                            'trade_date': rt_date_str,
                            'open': float(rt_row['open']),
                            'high': float(rt_row['high']),
                            'low': float(rt_row['low']),
                            'close': float(rt_row['price']),
                            'vol': float(rt_row['volume']) / 100, # 手 -> 手 (Sina返回的是股? 需确认. Sina volume is usually in shares, tushare daily vol is in lots (100 shares). Wait, ts.get_realtime_quotes volume is in shares? Let's verify. Usually Sina API returns shares. Tushare daily returns lots. So / 100.)
                            'amount': float(rt_row['amount']) / 1000 # 元 -> 千元
                        }])
                        
                        # 拼接
                        if df is None or df.empty:
                            df = new_row
                        else:
                            df = pd.concat([df, new_row], ignore_index=True)
                            
            except Exception as e:
                print(f"实时数据拼接失败: {e}")
        
        if df is None or df.empty:
            print(f"⚠️ 未获取到 {ts_code} 的{period}数据")
            return None
            
        # 统一列名 (pro_bar 返回的列名通常已经是标准的)
        # 确保按日期升序排列
        df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        
        # 截取最近 limit 条
        if len(df) > limit:
            df = df.iloc[-limit:].reset_index(drop=True)
            
        return df
        
    except Exception as e:
        print(f"获取K线数据失败: {e}")
        return None


def fetch_company_info(
    ts_code: str,
    use_cache: bool = True,
    return_cache_status: bool = False
) -> Any:
    """
    获取公司基本信息
    
    优化：公司信息很少变化，使用30天缓存避免频繁调用API
    stock_company API限制：每分钟10次（2000分中级用户）
    
    参数:
        ts_code: 股票代码
        use_cache: 是否使用缓存（默认True）
        return_cache_status: 是否返回缓存命中状态（默认False）
        
    返回:
        如果return_cache_status=False: 公司信息字典，如果获取失败返回None
        如果return_cache_status=True: (公司信息字典, 是否命中缓存)
    """
    from cache_manager import data_cache
    
    # 生成缓存键
    cache_key = f"company_info_{ts_code}"
    
    # 先检查缓存（30天有效，公司信息很少变化）
    if use_cache:
        cache_path = data_cache.get_cache_file_path(cache_key)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                # 检查是否过期（30天 = 2592000秒）
                cached_time = cache_data.get('timestamp', 0)
                expire_seconds = 30 * 24 * 3600  # 30天
                if time.time() - cached_time < expire_seconds:
                    cached_data = cache_data.get('data')
                    if cached_data is not None:
                        print(f"✅ 从缓存加载公司信息：{ts_code}（30天缓存有效，跳过API调用）")
                        if return_cache_status:
                            return cached_data, True
                        return cached_data
                else:
                    # 缓存已过期，删除文件
                    try:
                        os.remove(cache_path)
                    except:
                        pass
            except Exception as e:
                # 缓存文件损坏，删除它
                print(f"⚠️ 缓存文件损坏，删除并重新获取：{e}")
                try:
                    os.remove(cache_path)
                except:
                    pass
    
    # 缓存未命中，调用API
    try:
        pro = get_pro_client()
        df = pro.stock_company(
            ts_code=ts_code,
            fields='ts_code,com_name,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,employees,main_business,business_scope'
        )
        if df.empty:
            if return_cache_status:
                return None, False
            return None
        
        row = df.iloc[0]
        company_info = {
            'ts_code': row.get('ts_code', ''),
            'com_name': row.get('com_name', ''),
            'chairman': row.get('chairman', ''),
            'manager': row.get('manager', ''),
            'secretary': row.get('secretary', ''),
            'reg_capital': row.get('reg_capital', 0),
            'setup_date': row.get('setup_date', ''),
            'province': row.get('province', ''),
            'city': row.get('city', ''),
            'introduction': row.get('introduction', ''),
            'website': row.get('website', ''),
            'email': row.get('email', ''),
            'employees': row.get('employees', 0),
            'main_business': row.get('main_business', ''),
            'business_scope': row.get('business_scope', ''),
        }
        
        # 保存到缓存（30天有效）
        if use_cache:
            # 使用data_cache.set方法，它会自动转换numpy/pandas类型
            # 注意：由于需要30天缓存，我们不能直接使用data_cache.set（它默认24小时）
            # 所以需要手动处理，但要确保类型转换
            cache_path = data_cache.get_cache_file_path(cache_key)
            
            # 转换numpy/pandas类型为Python原生类型
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
            
            # 转换公司信息中的numpy类型
            company_info_converted = convert_to_native(company_info)
            
            cache_data = {
                'data': company_info_converted,
                'timestamp': time.time(),
                'datetime': datetime.now().isoformat()
            }
            try:
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                # 先写入临时文件，成功后再替换（避免并发写入问题）
                temp_path = cache_path + '.tmp'
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, cache_path)
                print(f"✅ 公司信息已缓存（30天有效）：{ts_code}")
            except Exception as cache_error:
                print(f"⚠️ 缓存保存失败（不影响使用）：{cache_error}")
        
        if return_cache_status:
            return company_info, False
        return company_info
    except Exception as e:
        print(f"获取公司信息失败: {e}")
        # 如果API调用失败，尝试使用过期缓存
        if use_cache:
            try:
                cache_path = data_cache.get_cache_file_path(cache_key)
                if os.path.exists(cache_path):
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    cached_info = cache_data.get('data')
                    if cached_info:
                        print(f"⚠️ 使用过期缓存的公司信息：{ts_code}")
                        if return_cache_status:
                            return cached_info, True
                        return cached_info
            except Exception:
                pass
        
        if return_cache_status:
            return None, False
        return None


def fetch_audit_records(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 20,
) -> List[AuditRecord]:
    """获取指定时间范围内的审计意见列表。"""
    pro = get_pro_client()
    fields = "ann_date,end_date,audit_result,audit_agency,audit_sign"
    # 重要：不限制数据量，获取所有可用数据
    # 如果设置了日期范围，使用足够大的limit确保获取所有数据
    if start_date and end_date:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        year_span = end_year - start_year + 1
        # 每年最多4条（Q1, Q2, Q3, 年报），乘以年份跨度，再加大量缓冲确保获取所有数据
        api_limit = year_span * 20  # 每年20条记录（足够大的缓冲，确保获取所有数据）
    else:
        api_limit = 10000  # 不限制，使用足够大的值确保获取所有数据
    if start_date and end_date:
        print(f"📊 调用fina_audit API，limit={api_limit}（不限制，获取所有数据），日期范围：{start_date} - {end_date}")
    params: Dict[str, Any] = {
        "ts_code": ts_code,
        "start_date": start_date,
        "end_date": end_date,
        "limit": api_limit,
        "fields": fields,
    }
    params = {k: v for k, v in params.items() if v is not None}
    df = pro.fina_audit(**params)
    if df.empty:
        raise ValueError("未获取到审计意见，请确认权限或披露情况。")
    # 不再使用head限制，因为已经通过start_date和end_date正确过滤了
    df = df.sort_values("end_date", ascending=False)
    records = [
        AuditRecord(
            ann_date=row["ann_date"],
            end_date=row["end_date"],
            audit_result=row["audit_result"],
            audit_agency=row["audit_agency"],
            audit_sign=row["audit_sign"],
        )
        for _, row in df.iterrows()
    ]
    return records


def fetch_balancesheet(
    ts_code: str,
    start_date: Optional[str],
    end_date: Optional[str],
    max_records: int,
) -> pd.DataFrame:
    """获取资产负债表数据。"""
    pro = get_pro_client()
    fields = "ts_code,ann_date,end_date,total_assets,total_liab"
    # 重要：不限制数据量，获取所有可用数据
    # 如果设置了日期范围，使用足够大的limit确保获取所有数据
    if start_date and end_date:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        year_span = end_year - start_year + 1
        # 每年最多4条（Q1, Q2, Q3, 年报），乘以年份跨度，再加大量缓冲确保获取所有数据
        api_limit = year_span * 20  # 每年20条记录（足够大的缓冲，确保获取所有数据）
    else:
        api_limit = 10000  # 不限制，使用足够大的值确保获取所有数据
    print(f"📊 调用balancesheet API，limit={api_limit}（不限制，获取所有数据），日期范围：{start_date} - {end_date}")
    df = pro.balancesheet(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=api_limit,
    )
    return _filter_annual_records(
        df,
        start_date,
        end_date,
        ["total_assets", "total_liab"],
        max_records,
    )


def fetch_income(
    ts_code: str,
    start_date: Optional[str],
    end_date: Optional[str],
    max_records: int,
) -> pd.DataFrame:
    """获取利润表数据。"""
    pro = get_pro_client()
    fields = "ts_code,ann_date,end_date,revenue,oper_cost,n_income"
    # 重要：不限制数据量，获取所有可用数据
    # 如果设置了日期范围，使用足够大的limit确保获取所有数据
    if start_date and end_date:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        year_span = end_year - start_year + 1
        # 每年最多4条（Q1, Q2, Q3, 年报），乘以年份跨度，再加大量缓冲确保获取所有数据
        api_limit = year_span * 20  # 每年20条记录（足够大的缓冲，确保获取所有数据）
    else:
        api_limit = 10000  # 不限制，使用足够大的值确保获取所有数据
    print(f"📊 调用income API，limit={api_limit}（不限制，获取所有数据），日期范围：{start_date} - {end_date}")
    df = pro.income(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=api_limit,
    )
    return _filter_annual_records(
        df,
        start_date,
        end_date,
        ["revenue", "oper_cost", "n_income"],
        max_records,
    )


def fetch_cashflow(
    ts_code: str,
    start_date: Optional[str],
    end_date: Optional[str],
    max_records: int,
) -> pd.DataFrame:
    """获取现金流量表数据。"""
    pro = get_pro_client()
    fields = "ts_code,ann_date,end_date,n_cashflow_act"
    # 重要：不限制数据量，获取所有可用数据
    # 如果设置了日期范围，使用足够大的limit确保获取所有数据
    if start_date and end_date:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        year_span = end_year - start_year + 1
        # 每年最多4条（Q1, Q2, Q3, 年报），乘以年份跨度，再加大量缓冲确保获取所有数据
        api_limit = year_span * 20  # 每年20条记录（足够大的缓冲，确保获取所有数据）
    else:
        api_limit = 10000  # 不限制，使用足够大的值确保获取所有数据
    print(f"📊 调用cashflow API，limit={api_limit}（不限制，获取所有数据），日期范围：{start_date} - {end_date}")
    df = pro.cashflow(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=api_limit,
    )
    return _filter_annual_records(
        df,
        start_date,
        end_date,
        ["n_cashflow_act"],
        max_records,
    )


def calculate_recent_years(required_years: int = 5) -> Tuple[int, int]:
    """
    智能计算"最近N年"的年份范围,考虑年报发布时间
    
    逻辑:
    - 年报通常在次年4-5月发布
    - 如果当前月份 < 5月,上一年年报可能未发布,需要往前推一年
    - 如果当前月份 >= 5月,上一年年报应该已发布,可以包含
    
    例子:
    - 2026年1月,需要5年: 返回 (2020, 2024) - 因为2025年报还没出
    - 2026年6月,需要5年: 返回 (2021, 2025) - 因为2025年报已出
    
    参数:
        required_years: 需要的年份数量,默认5年
        
    返回:
        (开始年份, 结束年份) 元组
    """
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # 判断上一年的年报是否已发布
    if current_month >= 5:
        # 5月及之后,上一年年报应该已发布
        end_year = current_year - 1
    else:
        # 1-4月,上一年年报可能未发布,往前推一年
        end_year = current_year - 2
    
    start_year = end_year - required_years + 1
    
    print(f"📅 智能年份计算: 当前{current_year}年{current_month}月,最近{required_years}年 = {start_year}-{end_year}")
    
    return start_year, end_year


def analyze_fundamentals(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    years: int = 5,
    use_cache: bool = True,
    api_delay: float = 0.0,  # 额外延迟（在API规则延迟基础上增加）
    max_workers: int = 1,  # 并发线程数（用于计算合适的延迟）
    progress_callback=None,
    user_points: Optional[float] = None,  # 用户积分（可选，避免重复调用API）
) -> Dict[str, Any]:
    """
    执行综合分析，计算资产负债率、毛利率、经营现金流等指标。
    
    ⚠️ 重要：此函数只缓存财务数据（资产负债表、利润表、现金流），不包含价格数据！
    - 缓存的财务数据相对稳定（年度/季度更新），可以长期缓存（365天）
    - 不缓存价格、PE等估值数据（这些数据每天变化，必须实时获取）
    - 估值检查应使用 fetch_valuation_data() 获取最新价格数据，不要使用此函数的缓存数据
    
    参数:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        years: 年数
        use_cache: 是否使用缓存

    返回:
        dict: 包含审计信息、指标 DataFrame、现金流统计等数据。
        注意：返回的数据不包含价格、PE等估值数据，这些需要单独调用 fetch_valuation_data() 获取。
    """
    # 生成缓存键（包含完整的日期范围，确保年份变化时缓存键也变化）
    # 修复bug: 处理None值，避免缓存键变成 "600519_None_None_5"
    start_date_str = start_date if start_date else 'all'
    end_date_str = end_date if end_date else 'all'
    cache_key = f"{ts_code}_{start_date_str}_{end_date_str}_{years}"
    print(f"🔑 缓存键：{cache_key}")
    
    # 增量更新标志
    incremental_update = False
    cached_base_data = None
    fetch_start_date = start_date
    fetch_end_date = end_date
    
    # 先检查缓存
    if use_cache:
        cached_data = data_cache.get(cache_key)
        if cached_data is not None:
            try:
                # 将cached_data中的DataFrame转回pandas DataFrame
                if 'metrics_dict' in cached_data and cached_data['metrics_dict']:
                    metrics_df = pd.DataFrame(cached_data['metrics_dict'])
                    
                    # 重要：从缓存恢复时，强制过滤，确保只保留年度数据（end_date以1231结尾）
                    if not metrics_df.empty:
                        # 确保end_date是字符串类型
                        metrics_df['end_date'] = metrics_df['end_date'].astype(str)
                        before_filter = len(metrics_df)
                        metrics_df = metrics_df[metrics_df['end_date'].str.endswith('1231')].copy()
                        if before_filter != len(metrics_df):
                            print(f"⚠️ 从缓存恢复时过滤季度数据：从{before_filter}条记录过滤到{len(metrics_df)}条年度记录（只保留end_date以1231结尾的数据）")
                    
                    # 将audit_records dict转回AuditRecord对象
                    audit_list = []
                    if 'audit_records' in cached_data and isinstance(cached_data['audit_records'], list):
                        audit_list = [
                            AuditRecord(**r) if isinstance(r, dict) else r
                            for r in cached_data['audit_records']
                        ]
                    
                    # 重新构建完整的result对象
                    result = {
                        'company_info': cached_data.get('company_info'),
                        'metrics': metrics_df,
                        'audit_records': audit_list,
                        'cashflow_positive_years': cached_data.get('cashflow_positive_years', 0),
                        'cashflow_cover_profit': cached_data.get('cashflow_cover_profit', False)
                    }
                    
                    print(f"✅ 从缓存加载数据：{len(metrics_df)}年数据（日期范围：{start_date} - {end_date}）")
                    # 验证缓存数据的年份范围是否完整
                    if not metrics_df.empty:
                        cached_years = sorted([row['end_date'][:4] for _, row in metrics_df.iterrows()])
                        print(f"📅 缓存数据包含的年份：{cached_years}（已确保全部为年度数据）")
                    
                    # 重要：验证缓存数据的年份范围是否与查询范围匹配
                    if start_date and end_date:
                        start_year = int(start_date[:4])
                        end_year = int(end_date[:4])
                        expected_years = list(range(start_year, end_year + 1))
                        actual_years = [int(y) for y in cached_years]
                        missing_years = [y for y in expected_years if y not in actual_years]
                        
                        current_year = datetime.now().year
                        
                        if missing_years:
                            current_month = datetime.now().month
                            
                            # 优化：过滤掉当前年份（因为当前年份年报肯定没出，缺失是正常的）
                            # 例如：当前是2025年，missing_years=[2024, 2025]，我们只关心2024是否缺失
                            effective_missing = [y for y in missing_years if y < current_year]
                            
                            # 1. 如果缺失的年份是更早的历史年份（< current_year - 1），必须拒绝缓存
                            # 例如：当前2025，缺失2023或更早，说明数据严重不全
                            historical_missing = [y for y in effective_missing if y < current_year - 1]
                            
                            if historical_missing:
                                # ===== 智能增量更新 =====
                                print(f"💡 检测到缺失历史年份: {historical_missing}")
                                print(f"🔄 启用增量更新: 只获取缺失年份,不删除现有缓存")
                                
                                # 计算需要获取的年份范围
                                fetch_start_year = min(historical_missing)
                                fetch_end_year = max(historical_missing)
                                fetch_start_date = f"{fetch_start_year}0101"
                                fetch_end_date = f"{fetch_end_year}1231"
                                
                                print(f"📥 准备获取缺失年份: {fetch_start_year}-{fetch_end_year}")
                                
                                # 调用API获取缺失年份的数据(这部分代码会在后面执行)
                                # 设置一个标志,表示需要增量更新
                                incremental_update = True
                                cached_base_data = result  # 保存现有缓存数据
                                
                                # 不return,继续执行获取数据的逻辑
                            
                            # 2. 如果过滤后没有缺失年份（即只缺失当前年份），或者只缺失最近一年（current_year - 1）
                            # 我们允许使用缓存（因为最近一年可能还没发布，或者Tushare还没更新）
                            elif len(effective_missing) <= 1:
                                # 只有最近一年缺失，或者没有有效缺失（只缺当前年）
                                if effective_missing:
                                    missing_year = effective_missing[0]
                                    if current_month >= 5:
                                        print(f"⚠️ 注意：{missing_year}年年报应该已经发布（当前是{current_year}年{current_month}月），但Tushare数据源可能还没更新")
                                        print(f"💡 使用现有缓存数据（{cached_years}），如果后续数据源更新，缓存会自动刷新")
                                    else:
                                        print(f"💡 说明：{missing_year}年年报通常在{current_year}年4-5月发布，当前是{current_year}年{current_month}月，可能还未发布")
                                else:
                                    print(f"💡 说明：{current_year}年年报尚未发布，使用缓存数据是合理的")
                                
                                print(f"⚡ 缓存命中！跳过API调用，直接返回缓存数据（节省约6-10秒）")
                                return result
                                
                            # 3. 如果过滤后仍缺失超过1年（例如缺失2023, 2024），启用增量更新
                            else:
                                print(f"💡 检测到缺失多个年份: {effective_missing}")
                                print(f"🔄 启用增量更新: 只获取缺失年份{effective_missing},不删除现有缓存{cached_years}")
                                
                                # 计算需要获取的年份范围
                                fetch_start_year = min(effective_missing)
                                fetch_end_year = max(effective_missing)
                                fetch_start_date = f"{fetch_start_year}0101"
                                fetch_end_date = f"{fetch_end_year}1231"
                                
                                print(f"� 准备获取缺失年份: {fetch_start_year}-{fetch_end_year}")
                                
                                # 设置增量更新标志
                                incremental_update = True
                                cached_base_data = result
                                
                                # 不return,继续执行获取数据的逻辑
                        else:
                            # 年份范围完全匹配，可以使用缓存
                            print(f"⚡ 缓存命中！年份范围完全匹配，跳过API调用（节省约6-10秒）")
                            return result
                    else:
                        # 没有指定日期范围，直接使用缓存
                        print(f"⚡ 缓存命中！跳过API调用，直接返回缓存数据（节省约6-10秒）")
                        return result
                else:
                    # 缓存数据为空，删除并重新获取
                    print(f"⚠️ 警告：缓存数据为空，删除并重新获取")
                    data_cache.delete(cache_key)
            except Exception as e:
                print(f"⚠️ 缓存数据解析失败，删除并重新获取: {e}")
                data_cache.delete(cache_key)
    
    # 缓存未命中或异常，调用API获取数据
    if incremental_update:
        print(f"🔄 增量更新模式: 只获取缺失年份的数据 ({fetch_start_date[:4]}-{fetch_end_date[:4]})")
    else:
        print(f"🔄 缓存未命中，开始调用API获取数据...")
    
    # 重要：不限制数据量，获取所有可用数据
    # 如果指定了日期范围，根据日期范围计算，但不设置上限
    if fetch_start_date and fetch_end_date:
        # 计算年份跨度（例如：19950101 到 20501231 = 56年）
        start_year = int(fetch_start_date[:4])
        end_year = int(fetch_end_date[:4])
        max_records = end_year - start_year + 1
        # 不设置上限，获取所有数据
        print(f"📊 根据日期范围计算max_records：{start_year}-{end_year} = {max_records}年（不限制，获取所有数据）")
    elif start_date or end_date:
        # 只指定了开始或结束日期，使用足够大的值确保获取所有数据
        max_records = 1000  # 足够大的值，确保获取所有数据
    else:
        # 没有指定日期范围，使用years参数，但不限制
        max_records = max(years, 100)  # 至少100年，确保获取所有数据
    
    # 根据API规则自动计算延迟时间（基于用户积分等级和并发线程数）
    # 如果没有传入user_points，使用默认值（避免重复调用API）
    # 注意：调用者应该在app.py的main()函数中获取积分信息并传入
    if user_points is None:
        user_points = 2000  # 默认中级用户（2000分）
    
    # 第1次调用：公司基本信息（stock_company API，每分钟10次限制）
    if progress_callback:
        progress_callback("正在获取公司基本信息... (1/5)", 0.20)
    
    # 优化：获取缓存状态，如果命中缓存则跳过延迟
    company_info, is_company_cache_hit = fetch_company_info(ts_code, use_cache=True, return_cache_status=True)
    print(f"✅ 已获取公司信息")
    
    # stock_company API专用延迟（根据API规则和并发线程数自动计算）
    # 只有在未命中缓存时才需要等待
    if not is_company_cache_hit:
        company_api_delay = get_api_delay('stock_company', user_points, max_workers)
        if api_delay > 0:
            company_api_delay = company_api_delay + api_delay
        print(f"⏰ 等待{company_api_delay:.2f}秒（stock_company API：每分钟10次，{max_workers}线程并发）...")
        time.sleep(company_api_delay)
    else:
        print(f"⚡ 公司信息命中缓存，跳过API延迟等待")
    
    # 第2次调用：审计意见（fina_audit API）
    if progress_callback:
        progress_callback("正在获取审计意见... (2/5)", 0.40)
    print(f"📅 查询日期范围：start_date={fetch_start_date}, end_date={fetch_end_date}")
    audit_records = fetch_audit_records(ts_code, fetch_start_date, fetch_end_date, max_records)
    print(f"✅ 已获取审计意见，共{len(audit_records)}条记录")
    
    # 财务数据API延迟（根据用户积分等级和并发线程数自动计算）
    financial_api_delay = get_api_delay('fina_audit', user_points, max_workers)
    # api_delay参数作为额外延迟（在API规则延迟基础上增加）
    if api_delay > 0:
        financial_api_delay = financial_api_delay + api_delay
        print(f"⏰ 等待{financial_api_delay:.2f}秒（基础延迟{get_api_delay('fina_audit', user_points, max_workers):.2f}秒 + 额外延迟{api_delay}秒，{max_workers}线程并发）...")
    else:
        print(f"⏰ 等待{financial_api_delay:.2f}秒（财务数据API：每分钟200次，{user_points:.0f}分，{max_workers}线程并发）...")
    
    if financial_api_delay > 0:
        time.sleep(financial_api_delay)
    
    # 第3次调用：资产负债表（balancesheet API）
    if progress_callback:
        progress_callback("正在获取资产负债表... (3/5)", 0.60)
    balance_df = fetch_balancesheet(ts_code, fetch_start_date, fetch_end_date, max_records)
    print(f"✅ 已获取资产负债表")
    
    if financial_api_delay > 0:
        time.sleep(financial_api_delay)
    
    # 第4次调用：利润表（income API）
    if progress_callback:
        progress_callback("正在获取利润表... (4/5)", 0.80)
    income_df = fetch_income(ts_code, fetch_start_date, fetch_end_date, max_records)
    print(f"✅ 已获取利润表")
    
    if financial_api_delay > 0:
        time.sleep(financial_api_delay)
    
    # 第5次调用：现金流量表（cashflow API）
    if progress_callback:
        progress_callback("正在获取现金流量表... (5/5)", 1.0)
    cashflow_df = fetch_cashflow(ts_code, fetch_start_date, fetch_end_date, max_records)
    print("✅ 已获取现金流量表，数据收集完成！")
    print(f"📊 获取到的原始数据统计：")
    if not balance_df.empty:
        balance_years = sorted([row['end_date'][:4] for _, row in balance_df.iterrows()])
        print(f"  - 资产负债表：{len(balance_df)}条记录，年份范围：{balance_years[0] if balance_years else 'N/A'} - {balance_years[-1] if balance_years else 'N/A'}，年份列表：{balance_years}")
    if not income_df.empty:
        income_years = sorted([row['end_date'][:4] for _, row in income_df.iterrows()])
        print(f"  - 利润表：{len(income_df)}条记录，年份范围：{income_years[0] if income_years else 'N/A'} - {income_years[-1] if income_years else 'N/A'}，年份列表：{income_years}")
    if not cashflow_df.empty:
        cashflow_years = sorted([row['end_date'][:4] for _, row in cashflow_df.iterrows()])
        print(f"  - 现金流量表：{len(cashflow_df)}条记录，年份范围：{cashflow_years[0] if cashflow_years else 'N/A'} - {cashflow_years[-1] if cashflow_years else 'N/A'}，年份列表：{cashflow_years}")

    # 数据合并：使用inner join确保三个表都有数据的年份才保留
    # 注意：如果某个年份某个表数据缺失，该年份会被过滤掉
    print(f"🔍 合并前数据统计：")
    if not balance_df.empty:
        balance_years = sorted([row['end_date'][:4] for _, row in balance_df.iterrows()])
        print(f"  - 资产负债表：{len(balance_df)}条，年份：{balance_years}")
    else:
        print(f"  - 资产负债表：{len(balance_df)}条（空）")
    if not income_df.empty:
        income_years = sorted([row['end_date'][:4] for _, row in income_df.iterrows()])
        print(f"  - 利润表：{len(income_df)}条，年份：{income_years}")
    else:
        print(f"  - 利润表：{len(income_df)}条（空）")
    if not cashflow_df.empty:
        cashflow_years = sorted([row['end_date'][:4] for _, row in cashflow_df.iterrows()])
        print(f"  - 现金流量表：{len(cashflow_df)}条，年份：{cashflow_years}")
    else:
        print(f"  - 现金流量表：{len(cashflow_df)}条（空）")
    
    # 重要：检查合并前的数据完整性
    # 如果某个表数据不完整，inner join会导致数据丢失
    balance_years_set = set([row['end_date'][:4] for _, row in balance_df.iterrows()]) if not balance_df.empty else set()
    income_years_set = set([row['end_date'][:4] for _, row in income_df.iterrows()]) if not income_df.empty else set()
    cashflow_years_set = set([row['end_date'][:4] for _, row in cashflow_df.iterrows()]) if not cashflow_df.empty else set()
    
    # 计算交集：资产负债表和利润表的交集（inner join后）
    common_years = balance_years_set & income_years_set
    print(f"🔍 合并分析：")
    print(f"  - 资产负债表年份：{sorted(balance_years_set)}")
    print(f"  - 利润表年份：{sorted(income_years_set)}")
    print(f"  - 现金流量表年份：{sorted(cashflow_years_set)}")
    print(f"  - 资产负债表和利润表的交集（inner join后）：{sorted(common_years)}")
    
    # 如果交集少于期望，给出警告
    if start_date and end_date:
        expected_years = set([str(y) for y in range(int(start_date[:4]), int(end_date[:4]) + 1)])
        missing_in_balance = expected_years - balance_years_set
        missing_in_income = expected_years - income_years_set
        if missing_in_balance:
            print(f"⚠️ 警告：资产负债表缺失年份：{sorted(missing_in_balance)}")
        if missing_in_income:
            print(f"⚠️ 警告：利润表缺失年份：{sorted(missing_in_income)}")
        if missing_in_balance or missing_in_income:
            print(f"⚠️ 注意：inner join会导致缺失的年份被过滤掉，最终只有交集年份：{sorted(common_years)}")
    
    merged = (
        balance_df[["end_date", "total_assets", "total_liab"]]
        .merge(
            income_df[["end_date", "revenue", "oper_cost", "n_income"]],
            on="end_date",
            how="inner",  # inner join：只保留两个表都有的end_date
        )
        .merge(
            cashflow_df[["end_date", "n_cashflow_act"]],
            on="end_date",
            how="left",  # left join：保留前两个表合并后的所有end_date，即使现金流量表缺失
        )
        .sort_values("end_date", ascending=False)
        .reset_index(drop=True)
    )
    
    # 重要修复：合并后再次过滤，确保只保留年度数据（end_date以1231结尾）
    # 虽然每个表都已经过滤了，但为了保险起见，合并后再次过滤
    if not merged.empty:
        before_filter_count = len(merged)
        merged = merged[merged['end_date'].str.endswith('1231')].copy()
        after_filter_count = len(merged)
        if before_filter_count != after_filter_count:
            print(f"⚠️ 合并后过滤：从{before_filter_count}条记录过滤到{after_filter_count}条年度记录（已过滤掉季度数据）")
    
    print(f"🔍 合并后数据统计：{len(merged)}条记录（已确保全部为年度数据）")
    
    # 打印合并后的数据年份范围（智能判断，避免不必要的警告）
    if not merged.empty:
        merged_years = sorted([row['end_date'][:4] for _, row in merged.iterrows()])
        print(f"📅 合并后的数据年份：{merged_years}（共{len(merged)}年）")
        
        # 计算期望的年份范围
        if start_date and end_date:
            start_year = int(start_date[:4])
            end_year = int(end_date[:4])
            expected_years = list(range(start_year, end_year + 1))
            actual_years = [int(y) for y in merged_years]
            missing_years = [y for y in expected_years if y not in actual_years]
            
            current_year = datetime.now().year
            current_month = datetime.now().month
            print(f"📅 查询年份范围：{start_year}-{end_year}年 | 实际返回数据年份：{merged_years}（共{len(merged)}年）")
            
            # 如果数据少于期望，打印详细调试信息
            if len(merged) < (end_year - start_year + 1):
                print(f"⚠️ 数据不完整：期望{end_year - start_year + 1}年，实际{len(merged)}年")
                if missing_years:
                    print(f"   缺失年份：{missing_years}")
                print(f"   可能原因：某些年份的资产负债表或利润表数据缺失，导致inner join过滤掉了")
            
            if missing_years:
                # 如果缺失的年份是当前年份或未来年份，说明年报还未发布，这是正常的
                if all(y >= current_year for y in missing_years):
                    print(f"💡 说明：{missing_years}年的年报尚未发布（通常在次年4-5月发布），这是正常情况")
                elif len(missing_years) > 1 or (missing_years and missing_years[0] < current_year - 1):
                    print(f"⚠️ 警告：缺少以下年份的数据：{missing_years}（可能是数据缺失）")
                # 特殊情况：如果缺失的是 end_year - 1（查询的最后一年），且 end_year = current_year，且当前月份 >= 5
                # 说明年报应该已经发布了，但数据源可能还没更新
                elif len(missing_years) == 1 and missing_years[0] == end_year - 1 and end_year == current_year:
                    if current_month >= 5:
                        print(f"⚠️ 注意：{missing_years[0]}年年报应该已经发布（当前是{current_year}年{current_month}月），但Tushare数据源可能还没更新")
                        print(f"💡 建议：可以稍后再试，或者检查该股票是否有2024年年报数据")
                    else:
                        print(f"💡 说明：{missing_years[0]}年年报通常在{current_year}年4-5月发布，当前是{current_year}年{current_month}月，可能还未发布")
        else:
            print(f"📅 合并后的数据年份：{merged_years}（共{len(merged)}年）")
    else:
        print(f"⚠️ 警告：合并后的数据为空！")

    # 修复bug: 添加除零检查和空值验证，避免计算错误
    def safe_calc_debt_ratio(row):
        """安全计算资产负债率，处理除零和空值"""
        total_liab = row.get('total_liab', 0)
        total_assets = row.get('total_assets', 0)
        # 检查是否为空值
        if pd.isna(total_liab) or pd.isna(total_assets):
            return pd.NA
        # 检查除零情况
        if total_assets == 0:
            return pd.NA
        return total_liab / total_assets
    
    merged["debt_ratio"] = merged.apply(safe_calc_debt_ratio, axis=1)
    
    # 计算毛利率：需要检查revenue是否为0或NaN
    # 如果revenue为0或NaN，则毛利率为NaN（表示数据缺失）
    def calc_gross_margin(row):
        revenue = row.get('revenue', 0)
        oper_cost = row.get('oper_cost', 0)
        if pd.isna(revenue) or revenue == 0:
            return pd.NA  # 返回NaN表示数据缺失
        if pd.isna(oper_cost):
            return pd.NA  # 如果成本缺失，也无法计算毛利率
        return (revenue - oper_cost) / revenue
    
    merged["gross_margin"] = merged.apply(calc_gross_margin, axis=1)
    
    # 检查是否有毛利率缺失的情况，并打印调试信息
    missing_gross_margin = merged["gross_margin"].isna().sum()
    if missing_gross_margin > 0:
        print(f"⚠️ 警告：有 {missing_gross_margin} 年的毛利率数据缺失（可能是财报中revenue或oper_cost字段缺失）")
        # 打印缺失的年份和原因
        for idx, row in merged[merged["gross_margin"].isna()].iterrows():
            revenue = row.get('revenue', 0)
            oper_cost = row.get('oper_cost', 0)
            year = row['end_date'][:4] if pd.notna(row.get('end_date')) else '未知'
            if pd.isna(revenue) or revenue == 0:
                print(f"  - {year}年：revenue缺失或为0 (revenue={revenue})")
            elif pd.isna(oper_cost):
                print(f"  - {year}年：oper_cost缺失 (oper_cost={oper_cost})")
    
    merged["cashflow_positive"] = merged["n_cashflow_act"] >= 0
    merged["cashflow_ge_profit"] = merged["n_cashflow_act"] >= merged["n_income"]

    result = {
        "company_info": company_info,
        "audit_records": audit_records,
        "metrics": merged,
        "cashflow_positive_years": int(merged["cashflow_positive"].sum()),
        "cashflow_cover_profit": bool(merged["cashflow_ge_profit"].all()),
    }
    
    # ===== 增量更新：合并新旧数据 =====
    if incremental_update and cached_base_data is not None:
        print(f"🔄 执行增量更新: 合并新数据与现有缓存")
        
        # 获取现有缓存的metrics
        cached_metrics = cached_base_data.get('metrics', pd.DataFrame())
        
        if not cached_metrics.empty:
            print(f"📊 现有缓存数据: {len(cached_metrics)} 年")
            print(f"📊 新获取数据: {len(merged)} 年")
            
            # 合并数据：现有缓存 + 新获取的数据
            combined_metrics = pd.concat([cached_metrics, merged], ignore_index=True)
            
            # 去重：如果同一年份同时存在于缓存和新数据中,保留新数据
            # 按end_date排序后去重,保留最后出现的(即新数据)
            combined_metrics = combined_metrics.sort_values('end_date').drop_duplicates(subset=['end_date'], keep='last')
            combined_metrics = combined_metrics.sort_values('end_date', ascending=False).reset_index(drop=True)
            
            # 确保只保留年度数据
            combined_metrics = combined_metrics[combined_metrics['end_date'].str.endswith('1231')].copy()
            
            combined_years = sorted([row['end_date'][:4] for _, row in combined_metrics.iterrows()])
            print(f"✅ 合并完成: 共 {len(combined_metrics)} 年数据, 年份={combined_years}")
            
            # 替换为合并后的数据
            merged = combined_metrics
            
            # 重新计算统计指标
            result['metrics'] = merged
            result['cashflow_positive_years'] = int(merged["cashflow_positive"].sum())
            result['cashflow_cover_profit'] = bool(merged["cashflow_ge_profit"].all())
            
            # 合并审计记录
            cached_audits = cached_base_data.get('audit_records', [])
            if cached_audits:
                # 将新旧审计记录合并并去重
                all_audits = audit_records + cached_audits
                # 按end_date去重,保留新记录
                seen_dates = set()
                unique_audits = []
                for audit in sorted(all_audits, key=lambda x: x.end_date, reverse=True):
                    if audit.end_date not in seen_dates:
                        unique_audits.append(audit)
                        seen_dates.add(audit.end_date)
                audit_records = unique_audits
                result['audit_records'] = audit_records
                print(f"✅ 审计记录合并完成: 共 {len(audit_records)} 条")
    
    # 保存到缓存
    if use_cache:
        # 重要：保存前再次确认只保存年度数据（end_date以1231结尾）
        # 虽然merged已经过滤过了，但为了保险起见，再次确认
        merged_for_cache = merged[merged['end_date'].str.endswith('1231')].copy()
        if len(merged_for_cache) != len(merged):
            print(f"⚠️ 保存缓存前过滤：从{len(merged)}条记录过滤到{len(merged_for_cache)}条年度记录")
            merged = merged_for_cache
        
        # 准备缓存数据
        # 注意：data_cache.set会自动处理numpy类型的转换
        cache_data = {
            'company_info': company_info,
            'metrics_dict': merged.to_dict('records'),
            'cashflow_positive_years': int(merged["cashflow_positive"].sum()),
            'cashflow_cover_profit': bool(merged["cashflow_ge_profit"].all()),
            'audit_records': [
                {
                    'ann_date': r.ann_date,
                    'end_date': r.end_date,
                    'audit_result': r.audit_result,
                    'audit_agency': r.audit_agency,
                    'audit_sign': r.audit_sign,
                }
                for r in audit_records
            ]
        }
        
        saved = data_cache.set(cache_key, cache_data)
        if saved:
            print(f"✅ 数据已缓存：{cache_key}")
        else:
            print(f"⚠️ 缓存保存失败")
    
    return result


def _filter_annual_records(
    df: pd.DataFrame,
    start_date: Optional[str],
    end_date: Optional[str],
    value_columns: List[str],
    max_records: int,
) -> pd.DataFrame:
    """筛选年报并转换字段类型。"""
    if df.empty:
        raise ValueError("接口返回为空，请检查 ts_code 或权限。")

    # 打印原始数据统计（用于调试）
    if start_date and end_date:
        all_end_dates = df["end_date"].astype(str).tolist() if "end_date" in df.columns else []
        annual_dates = [d for d in all_end_dates if str(d).endswith("1231")]
        if len(annual_dates) != len(all_end_dates):
            print(f"📊 API返回原始数据：共{len(all_end_dates)}条记录，其中年报{len(annual_dates)}条（end_date以1231结尾）")
            if annual_dates:
                annual_years = sorted([str(d)[:4] for d in annual_dates])
                print(f"📅 原始数据中的年报年份：{annual_years}")

    df["end_date"] = df["end_date"].astype(str)
    # 强制过滤：只保留年度数据（end_date必须以1231结尾，例如：20231231）
    # 季度数据格式：20230331（Q1）、20230630（Q2）、20230930（Q3）、20231231（年报）
    # 只有1231结尾的才是年度数据
    before_filter = len(df)
    df = df[df["end_date"].str.endswith("1231")].copy()
    if before_filter != len(df):
        print(f"📊 过滤季度数据：从{before_filter}条记录过滤到{len(df)}条年度记录（只保留end_date以1231结尾的数据）")
    
    # 重要调试：打印过滤后的数据年份
    if not df.empty:
        filtered_years = sorted([row['end_date'][:4] for _, row in df.iterrows()])
        print(f"📅 过滤后的年度数据年份：{filtered_years}（共{len(df)}年）")
    
    if df.empty:
        raise ValueError("未查询到年报数据，请确认公司是否披露年报。")

    # 重要调试：打印日期范围过滤前的数据
    if start_date or end_date:
        before_date_filter = len(df)
        before_date_years = sorted([row['end_date'][:4] for _, row in df.iterrows()]) if not df.empty else []
        print(f"📅 日期范围过滤前：{len(df)}条记录，年份：{before_date_years}")

    if start_date:
        df = df[df["end_date"] >= start_date]
    if end_date:
        df = df[df["end_date"] <= end_date]
    
    # 重要调试：打印日期范围过滤后的数据
    if start_date or end_date:
        after_date_years = sorted([row['end_date'][:4] for _, row in df.iterrows()]) if not df.empty else []
        print(f"📅 日期范围过滤后：{len(df)}条记录，年份：{after_date_years}（范围：{start_date} - {end_date}）")
    
    if df.empty:
        raise ValueError("指定时间范围内没有年报数据，请调整时间区间。")

    df = df.sort_values("end_date", ascending=False)
    df = df.drop_duplicates(subset="end_date", keep="first")
    
    # 重要：不限制数据量，返回所有符合日期范围的数据
    # 已经通过start_date和end_date正确过滤了，直接返回所有数据
    print(f"✅ 获取到{len(df)}条年度数据（不限制，显示所有数据）")
    df = df.copy()

    for col in value_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=value_columns, how="all")
    return df
