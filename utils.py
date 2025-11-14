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
    
    Returns:
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
    
    Args:
        token: Tushare Token，不传则使用get_token()获取
        
    Returns:
        Tushare Pro API客户端实例
    """
    return ts.pro_api(token or get_token())


def get_user_points_info(token: Optional[str] = None) -> Optional[Dict]:
    """
    获取用户积分信息（包括到期积分）
    
    Args:
        token: Tushare Token，不传则使用get_token()获取
        
    Returns:
        包含积分信息的字典，如果查询失败返回None
    """
    try:
        pro = get_pro_client(token)
        df = pro.user(token=get_token())
        
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


def run_connectivity_tests(verbose: bool = True) -> Tuple[bool, List[Dict[str, str]]]:
    """
    网络连通性三重检测
    
    检测项：
        1. DNS解析 - 检查api.waditu.com能否解析
        2. HTTP连接 - 检查HTTP请求是否正常
        3. Tushare API - 检查API接口是否可用
    
    Args:
        verbose: 是否打印详细日志
        
    Returns:
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
    
    Args:
        ts_code: 股票代码
        trade_date: 交易日期，格式YYYYMMDD
        target_type: 标的类型（个股/宽基指数）
        
    Returns:
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


def fetch_company_info(ts_code: str) -> Optional[Dict[str, Any]]:
    """获取上市公司基本信息"""
    try:
        pro = get_pro_client()
        df = pro.stock_company(
            ts_code=ts_code,
            fields='ts_code,com_name,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,employees,main_business,business_scope'
        )
        if df.empty:
            return None
        
        row = df.iloc[0]
        return {
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
    except Exception as e:
        print(f"获取公司信息失败: {e}")
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
    # 如果设置了日期范围，使用更大的limit确保获取所有数据
    api_limit = max(limit * 3, 200) if (start_date and end_date) else limit
    if start_date and end_date:
        print(f"📊 调用fina_audit API，limit={api_limit}，日期范围：{start_date} - {end_date}")
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
    # 如果设置了日期范围，使用更大的limit确保获取所有数据
    # 对于1995-2024这样的范围，需要足够大的limit
    api_limit = max(max_records * 3, 200)  # 至少200条，或者max_records的3倍
    print(f"📊 调用balancesheet API，limit={api_limit}，日期范围：{start_date} - {end_date}")
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
    # 如果设置了日期范围，使用更大的limit确保获取所有数据
    api_limit = max(max_records * 3, 200)  # 至少200条，或者max_records的3倍
    print(f"📊 调用income API，limit={api_limit}，日期范围：{start_date} - {end_date}")
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
    # 如果设置了日期范围，使用更大的limit确保获取所有数据
    api_limit = max(max_records * 3, 200)  # 至少200条，或者max_records的3倍
    print(f"📊 调用cashflow API，limit={api_limit}，日期范围：{start_date} - {end_date}")
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


def analyze_fundamentals(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    years: int = 5,
    use_cache: bool = True,
    api_delay: int = 31,
    progress_callback=None,
) -> Dict[str, Any]:
    """
    执行综合分析，计算资产负债率、毛利率、经营现金流等指标。
    
    Args:
        ts_code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        years: 年数
        use_cache: 是否使用缓存

    Returns:
        dict: 包含审计信息、指标 DataFrame、现金流统计等数据。
    """
    # 生成缓存键（包含完整的日期范围，确保年份变化时缓存键也变化）
    cache_key = f"{ts_code}_{start_date}_{end_date}_{years}"
    print(f"🔑 缓存键：{cache_key}")
    
    # 先检查缓存
    if use_cache:
        cached_data = data_cache.get(cache_key)
        if cached_data is not None:
            try:
                # 将cached_data中的DataFrame转回pandas DataFrame
                if 'metrics_dict' in cached_data and cached_data['metrics_dict']:
                    metrics_df = pd.DataFrame(cached_data['metrics_dict'])
                    
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
                    # 验证缓存数据的年份范围是否正确
                    if not metrics_df.empty:
                        cached_years = sorted([row['end_date'][:4] for _, row in metrics_df.iterrows()])
                        print(f"📅 缓存数据包含的年份：{cached_years}")
                    return result
                else:
                    print("⚠️ 缓存数据格式异常（无metrics_dict），删除并重新获取")
                    data_cache.delete(cache_key)
            except Exception as e:
                print(f"⚠️ 缓存数据解析失败，删除并重新获取: {e}")
                data_cache.delete(cache_key)
    
    # 缓存未命中或异常，调用API获取数据
    # 如果指定了日期范围，根据日期范围计算需要的记录数
    if start_date and end_date:
        # 计算年份跨度（例如：19950101 到 20501231 = 56年）
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        max_records = end_year - start_year + 1
        # 但也要设置一个合理的上限，避免请求过多数据（比如最多100年）
        max_records = min(max_records, 100)
        print(f"📊 根据日期范围计算max_records：{start_year}-{end_year} = {max_records}年")
    elif start_date or end_date:
        # 只指定了开始或结束日期，使用默认值
        max_records = 20
    else:
        # 没有指定日期范围，使用years参数
        max_records = years
    
    # 为了避免触发频率限制，在每次API调用之间添加延迟
    # 免费用户(0-119分)：每分钟2次 → 需要间隔31秒
    # 注册用户(120-599分)：每分钟5次 → 间隔13秒
    # 中级用户(600-4999分)：每分钟20次 → 间隔4秒
    # 高级用户(5000+分)：每分钟200次 → 无需延迟
    
    # 第1次调用：公司基本信息
    if progress_callback:
        progress_callback("正在获取公司基本信息... (1/5)", 0.20)
    company_info = fetch_company_info(ts_code)
    print(f"✅ 已获取公司信息")
    
    if api_delay > 0:
        print(f"⏰ 等待{api_delay}秒...")
        time.sleep(api_delay)
    
    # 第2次调用：审计意见
    if progress_callback:
        progress_callback("正在获取审计意见... (2/5)", 0.40)
    print(f"📅 查询日期范围：start_date={start_date}, end_date={end_date}")
    audit_records = fetch_audit_records(ts_code, start_date, end_date, max_records)
    print(f"✅ 已获取审计意见，共{len(audit_records)}条记录")
    
    if api_delay > 0:
        time.sleep(api_delay)
    
    # 第3次调用：资产负债表
    if progress_callback:
        progress_callback("正在获取资产负债表... (3/5)", 0.60)
    balance_df = fetch_balancesheet(ts_code, start_date, end_date, max_records)
    print(f"✅ 已获取资产负债表")
    
    if api_delay > 0:
        time.sleep(api_delay)
    
    # 第4次调用：利润表
    if progress_callback:
        progress_callback("正在获取利润表... (4/5)", 0.80)
    income_df = fetch_income(ts_code, start_date, end_date, max_records)
    print(f"✅ 已获取利润表")
    
    if api_delay > 0:
        time.sleep(api_delay)
    
    # 第5次调用：现金流量表
    if progress_callback:
        progress_callback("正在获取现金流量表... (5/5)", 1.0)
    cashflow_df = fetch_cashflow(ts_code, start_date, end_date, max_records)
    print("✅ 已获取现金流量表，数据收集完成！")
    print(f"📊 获取到的原始数据统计：")
    if not balance_df.empty:
        balance_years = sorted([row['end_date'][:4] for _, row in balance_df.iterrows()])
        print(f"  - 资产负债表：{len(balance_df)}条记录，年份范围：{balance_years[0] if balance_years else 'N/A'} - {balance_years[-1] if balance_years else 'N/A'}")
    if not income_df.empty:
        income_years = sorted([row['end_date'][:4] for _, row in income_df.iterrows()])
        print(f"  - 利润表：{len(income_df)}条记录，年份范围：{income_years[0] if income_years else 'N/A'} - {income_years[-1] if income_years else 'N/A'}")
    if not cashflow_df.empty:
        cashflow_years = sorted([row['end_date'][:4] for _, row in cashflow_df.iterrows()])
        print(f"  - 现金流量表：{len(cashflow_df)}条记录，年份范围：{cashflow_years[0] if cashflow_years else 'N/A'} - {cashflow_years[-1] if cashflow_years else 'N/A'}")

    merged = (
        balance_df[["end_date", "total_assets", "total_liab"]]
        .merge(
            income_df[["end_date", "revenue", "oper_cost", "n_income"]],
            on="end_date",
            how="inner",
        )
        .merge(
            cashflow_df[["end_date", "n_cashflow_act"]],
            on="end_date",
            how="left",
        )
        .sort_values("end_date", ascending=False)
        .reset_index(drop=True)
    )
    
    # 打印合并后的数据年份范围
    if not merged.empty:
        merged_years = sorted([row['end_date'][:4] for _, row in merged.iterrows()])
        print(f"📅 合并后的数据年份：{merged_years}（共{len(merged)}年）")
        print(f"📅 数据年份范围：{merged_years[0]} - {merged_years[-1]}")
        print(f"📅 期望的年份范围：{start_date[:4]} - {end_date[:4]}")
        if merged_years[0] != start_date[:4] or merged_years[-1] != end_date[:4]:
            print(f"⚠️ 警告：合并后的数据年份范围与期望范围不一致！")
    else:
        print(f"⚠️ 警告：合并后的数据为空！")

    merged["debt_ratio"] = merged["total_liab"] / merged["total_assets"]
    
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
    
    merged["cashflow_positive"] = merged["n_cashflow_act"] > 0
    merged["cashflow_ge_profit"] = merged["n_cashflow_act"] >= merged["n_income"]

    result = {
        "company_info": company_info,
        "audit_records": audit_records,
        "metrics": merged,
        "cashflow_positive_years": int(merged["cashflow_positive"].sum()),
        "cashflow_cover_profit": bool(merged["cashflow_ge_profit"].all()),
    }
    
    # 保存到缓存
    if use_cache:
        # 准备可序列化的缓存数据
        cache_data = {
            'company_info': company_info,  # 公司信息
            'metrics_dict': merged.to_dict('records'),  # DataFrame转dict
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

    df["end_date"] = df["end_date"].astype(str)
    df = df[df["end_date"].str.endswith("1231")]
    if df.empty:
        raise ValueError("未查询到年报数据，请确认公司是否披露年报。")

    if start_date:
        df = df[df["end_date"] >= start_date]
    if end_date:
        df = df[df["end_date"] <= end_date]
    if df.empty:
        raise ValueError("指定时间范围内没有年报数据，请调整时间区间。")

    df = df.sort_values("end_date", ascending=False)
    df = df.drop_duplicates(subset="end_date", keep="first")
    
    # 重要修复：如果用户设置了日期范围（如1995-2050），应该返回该范围内的所有数据
    # 而不是被max_records限制。但如果数据量超过max_records，说明可能有问题，给出警告
    if max_records and len(df) > max_records:
        print(f"⚠️ 警告：获取到{len(df)}条记录，但max_records={max_records}")
        print(f"   实际年份范围：{df['end_date'].min()} - {df['end_date'].max()}")
        print(f"   将返回所有符合日期范围的数据（{len(df)}条），而不是只返回最近{max_records}条")
    # 不再使用head限制，因为已经通过start_date和end_date正确过滤了
    df = df.copy()

    for col in value_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=value_columns, how="all")
    return df
