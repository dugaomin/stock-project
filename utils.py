# -*- coding: utf-8 -*-
"""工具模块：包含 token 管理、连通性检测与财务分析函数。"""

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
    """优先从环境变量读取 token，缺省使用 settings 中的默认值。"""
    return os.environ.get("TUSHARE_TOKEN", DEFAULT_TOKEN)


@lru_cache(maxsize=1)
def get_pro_client(token: Optional[str] = None):
    """实例化并缓存 Tushare pro 客户端。"""
    return ts.pro_api(token or get_token())


def run_connectivity_tests(verbose: bool = True) -> Tuple[bool, List[Dict[str, str]]]:
    """
    依次执行 DNS、HTTP 与 Tushare API 检查，并打印日志。

    Returns:
        Tuple[bool, List[Dict[str, str]]]: (是否全部通过, 日志列表)。
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
    params: Dict[str, Any] = {
        "ts_code": ts_code,
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "fields": fields,
    }
    params = {k: v for k, v in params.items() if v is not None}
    df = pro.fina_audit(**params)
    if df.empty:
        raise ValueError("未获取到审计意见，请确认权限或披露情况。")
    df = df.sort_values("end_date", ascending=False).head(limit)
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
    df = pro.balancesheet(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=max_records * 2,
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
    df = pro.income(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=max_records * 2,
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
    df = pro.cashflow(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        limit=max_records * 2,
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
    # 生成缓存键
    cache_key = f"{ts_code}_{start_date}_{end_date}_{years}"
    
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
                    
                    print(f"✅ 从缓存加载数据：{len(metrics_df)}年数据")
                    return result
                else:
                    print("⚠️ 缓存数据格式异常（无metrics_dict），删除并重新获取")
                    data_cache.delete(cache_key)
            except Exception as e:
                print(f"⚠️ 缓存数据解析失败，删除并重新获取: {e}")
                data_cache.delete(cache_key)
    
    # 缓存未命中或异常，调用API获取数据
    max_records = years if not (start_date or end_date) else 20
    
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
    audit_records = fetch_audit_records(ts_code, start_date, end_date, max_records)
    print(f"✅ 已获取审计意见")
    
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

    merged["debt_ratio"] = merged["total_liab"] / merged["total_assets"]
    merged["gross_margin"] = (merged["revenue"] - merged["oper_cost"]) / merged["revenue"]
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
    df = df.head(max_records).copy()

    for col in value_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=value_columns, how="all")
    return df
