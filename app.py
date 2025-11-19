# -*- coding: utf-8 -*-
"""
A股财务综合分析系统 - 主应用模块

功能概述：
    基于审计意见与三大核心财务指标（资产负债率、毛利率、经营现金流）
    的智能筛选系统，帮助投资者快速评估公司财务健康状况。

核心功能：
    1. 单项分析 - 深度分析单只股票的财务数据
    2. 市赚率估值 - 基于PE和ROE的估值方法
    3. 历史记录 - 分析历史管理

技术栈：
    - Streamlit：Web UI框架
    - Tushare Pro：数据源API
    - Plotly：交互式图表
    - Pandas：数据处理

作者：gaomindu
版本：2.0.0
更新：2025-11-10
"""

import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Optional
from utils import analyze_fundamentals, run_connectivity_tests, fetch_valuation_data, get_user_points_info, calculate_recent_years
import json
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cache_manager import data_cache
from valuation import PRValuation
from screening import run_full_market_screening, StockScreener
import threading
import time
from constants import SECTOR_RULES

# 页面配置
st.set_page_config(
    page_title="A股财务综合分析系统",
    page_icon="📊",
    layout="wide",
)

# 数据文件路径
HISTORY_FILE = "data/analysis_history.json"
os.makedirs("data", exist_ok=True)

# 指数代码别名映射（常用宽基指数）
INDEX_CODE_ALIASES = {
    "沪深300": "000300.SH",
    "HS300": "000300.SH",
    "000300": "000300.SH",
    "399300": "399300.SZ",
    "深证成指": "399001.SZ",
    "399001": "399001.SZ",
    "恒生指数": "HSI",
    "HSI": "HSI",
    "恒生国企": "HSCEI",
    "HSCEI": "HSCEI",
}


def normalize_ts_code(raw_code: str, target_type: str) -> str:
    """
    规范化股票/指数代码，自动补全交易所后缀
    
    参数:
        raw_code: 用户输入的原始代码
        target_type: 标的类型（个股/宽基指数）
        
    返回:
        带有交易所后缀的标准代码
    """
    if not raw_code:
        return ""
    
    code = raw_code.upper().strip()
    
    if code in INDEX_CODE_ALIASES:
        return INDEX_CODE_ALIASES[code]
    
    if code.endswith((".SH", ".SZ", ".BJ", ".HK")):
        return code
    
    if target_type == "宽基指数":
        if code in INDEX_CODE_ALIASES:
            return INDEX_CODE_ALIASES[code]
        if code.startswith(("000", "001", "002", "003")):
            return f"{code}.SH"
        if code.startswith(("399", "159", "150", "560")):
            return f"{code}.SZ"
        return f"{code}.SH"
    
    if len(code) == 6:
        if code.startswith(("6", "9")):
            return f"{code}.SH"
        if code.startswith("8"):
            return f"{code}.BJ"
        return f"{code}.SZ"
    
    return code


# 初始化会话状态
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False
if 'start_year' not in st.session_state or 'end_year' not in st.session_state:
    # 使用智能年份计算：根据当前月份判断最近5年
    start_year, end_year = calculate_recent_years(required_years=5)
    st.session_state.start_year = start_year
    st.session_state.end_year = end_year
if 'selected_sector' not in st.session_state:
    st.session_state.selected_sector = "消费"
if 'ocf_consecutive_years' not in st.session_state:
    st.session_state.ocf_consecutive_years = 3
if 'api_delay' not in st.session_state:
    st.session_state.api_delay = 0.1  # 默认0.1秒，适配中级用户（2000积分）


class HistoryManager:
    """
    历史记录管理类
    
    功能：
        - 保存分析历史到JSON文件
        - 加载历史记录
        - 限制最多保留100条记录
    
    存储位置：
        data/analysis_history.json
    """
    
    @staticmethod
    def load_history():
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    @staticmethod
    def save_history(records):
        try:
            with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            return True
        except:
            return False
    
    @staticmethod
    def add_record(record):
        history = HistoryManager.load_history()
        history.insert(0, record)
        history = history[:100]
        HistoryManager.save_history(history)


def format_percentage(value: float) -> str:
    """
    将小数格式化为百分比字符串
    
    参数:
        value: 小数值（如0.6表示60%）
        
    返回:
        格式化后的百分比字符串（如"60.00%"），数据缺失返回"-"
    """
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.2f}%"


def format_metric_value(
    value: Optional[float],
    spec: str = ".2f",
    suffix: str = "",
    default: str = "无数据",
) -> str:
    """
    安全格式化数值，避免None或NaN导致的格式化报错
    
    参数:
        value: 数值
        spec: 格式化规格（例如'.2f'）
        suffix: 追加的单位（例如'元'、'%'）
        default: 缺失数据时返回的字符串
        
    返回:
        格式化后的字符串
    """
    if value is None:
        return default
    if isinstance(value, (int, float)) and pd.isna(value):
        return default
    try:
        return f"{format(value, spec)}{suffix}"
    except (TypeError, ValueError):
        return default


def format_number(value: float, unit="万元") -> str:
    """格式化数字"""
    if value is None or pd.isna(value):
        return "-"
    if unit == "亿元":
        return f"{value/100000000:,.2f}亿元"
    return f"{value/10000:,.2f}万元"


def evaluate_year(row, sector_rules) -> dict:
    """
    评估单年财务指标，计算年度得分（0-3分）
    
    评分规则：
        - 资产负债率达标：+1分
        - 毛利率达标：+1分
        - 经营现金流为正：+1分
    
    参数:
        row: DataFrame的一行数据，包含财务指标
        sector_rules: 行业评分标准字典
        
    返回:
        (年度得分, 各项检查结果字典)
    """
    score = 0
    checks = {
        'debt_ratio_pass': False,
        'gross_margin_pass': False,
        'ocf_positive': False,
        'ocf_ge_profit': False
    }
    
    # 资产负债率检查
    if pd.notna(row['debt_ratio']):
        debt_ratio_pct = row['debt_ratio'] * 100
        if debt_ratio_pct <= sector_rules['debt_ratio_max']:
            score += 1
            checks['debt_ratio_pass'] = True
    
    # 毛利率检查
    if pd.notna(row['gross_margin']):
        gross_margin_pct = row['gross_margin'] * 100
        if gross_margin_pct >= sector_rules['gross_margin_min']:
            score += 1
            checks['gross_margin_pass'] = True
    
    # 现金流检查
    if row['cashflow_positive']:
        score += 1
        checks['ocf_positive'] = True
    
    # 现金流≥净利润
    if row['cashflow_ge_profit']:
        checks['ocf_ge_profit'] = True
    
    return score, checks


def check_ocf_consecutive(metrics: pd.DataFrame, k: int) -> tuple:
    """
    检查经营现金流连续性
    
    核心问题：连续k年（通常3-5年）经营现金流为正吗？
    
    参数:
        metrics: 财务指标DataFrame
        k: 要求的连续年数
        
    返回:
        (是否连续k年为正, 为正年数, 现金流≥利润年数, 总年数, 最长连续年数)
    """
    consecutive = 0
    max_consecutive = 0
    positive_count = 0
    ge_profit_count = 0
    
    for _, row in metrics.iterrows():
        if row['cashflow_positive']:
            consecutive += 1
            positive_count += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
        
        if row['cashflow_ge_profit']:
            ge_profit_count += 1
    
    total = len(metrics)
    ocf_consecutive_ok = max_consecutive >= k
    
    return ocf_consecutive_ok, positive_count, ge_profit_count, total, max_consecutive


def evaluate_metrics(metrics: pd.DataFrame, sector_rules: dict, ocf_k: int) -> dict:
    """评估财务指标"""
    if metrics.empty:
        return {
            "avg_score": 0, 
            "latest_score": 0, 
            "red_flags": 0, 
            "scores": [],
            "year_checks": [],
            "ocf_info": {}
        }
    
    scores = []
    year_checks = []
    
    # 评估每年
    for _, row in metrics.iterrows():
        score, checks = evaluate_year(row, sector_rules)
        scores.append(score)
        year_checks.append(checks)
    
    avg_score = sum(scores) / len(scores) if scores else 0
    latest_score = scores[-1] if scores else 0
    red_flags = sum(1 for s in scores if s < 2)
    
    # 检查OCF连续性
    ocf_ok, pos_cnt, ge_cnt, total, max_cons = check_ocf_consecutive(metrics, ocf_k)
    
    return {
        "avg_score": avg_score,
        "latest_score": latest_score,
        "red_flags": red_flags,
        "scores": scores,
        "year_checks": year_checks,
        "ocf_info": {
            "consecutive_ok": ocf_ok,
            "positive_count": pos_cnt,
            "ge_profit_count": ge_cnt,
            "total_years": total,
            "max_consecutive": max_cons
        }
    }


def render_audit_opinion(audit_records):
    """渲染审计意见"""
    st.subheader("1️⃣ 财报审计意见")
    
    if not audit_records:
        st.warning("⚠️ 未获取到审计意见数据")
        return
    
    # 显示最近一年的审计意见
    latest_audit = audit_records[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("报告期", latest_audit.end_date)
    
    with col2:
        is_standard = latest_audit.is_standard
        if is_standard:
            st.success("✅ 标准无保留意见")
        else:
            st.error("❌ 非标准意见")
    
    with col3:
        st.info(f"会计师事务所\n{latest_audit.audit_agency}")
    
    # 详细审计记录
    with st.expander("📋 历年审计意见详情", expanded=False):
        audit_data = [{
            "报告期": r.end_date,
            "公告日期": r.ann_date,
            "审计意见": r.audit_result,
            "是否标准无保留": "✅ 是" if r.is_standard else "❌ 否",
            "会计师事务所": r.audit_agency,
            "签字会计师": r.audit_sign,
        } for r in audit_records]
        st.dataframe(pd.DataFrame(audit_data), use_container_width=True, hide_index=True)


def render_core_indicators(metrics: pd.DataFrame, evaluation: dict, sector_rules: dict):
    """渲染三大核心指标"""
    st.subheader("2️⃣ 三大核心指标分析")
    
    if metrics.empty:
        st.warning("⚠️ 未获取到财务数据")
        return
    
    # 重要：确保只使用年度数据（end_date以1231结尾），过滤掉可能的季度数据
    if not metrics.empty and 'end_date' in metrics.columns:
        before_filter = len(metrics)
        metrics = metrics[metrics['end_date'].astype(str).str.endswith('1231')].copy()
        if before_filter != len(metrics):
            st.warning(f"⚠️ 检测到季度数据：从{before_filter}条记录过滤到{len(metrics)}条年度记录（只保留end_date以1231结尾的数据）")
    
    # 获取最新年份数据
    latest = metrics.iloc[-1]
    
    # 三大指标展示
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("##### 2.1 资产负债率")
        debt_ratio_pct = latest['debt_ratio'] * 100 if pd.notna(latest['debt_ratio']) else None
        if debt_ratio_pct is not None:
            is_pass = debt_ratio_pct <= sector_rules['debt_ratio_max']
            st.metric(
                "最新年份",
                f"{debt_ratio_pct:.2f}%",
                delta=f"标准≤{sector_rules['debt_ratio_max']}%",
                delta_color="normal" if is_pass else "inverse"
            )
            if is_pass:
                st.success("✅ 达标")
            else:
                st.error("❌ 超标")
        else:
            st.warning("数据缺失")
        
        st.caption(f"📝 {sector_rules['description']}")
    
    with col2:
        st.markdown("##### 2.2 毛利率")
        gross_margin_pct = latest['gross_margin'] * 100 if pd.notna(latest['gross_margin']) else None
        if gross_margin_pct is not None:
            is_pass = gross_margin_pct >= sector_rules['gross_margin_min']
            st.metric(
                "最新年份",
                f"{gross_margin_pct:.2f}%",
                delta=f"标准≥{sector_rules['gross_margin_min']}%",
                delta_color="normal" if is_pass else "inverse"
            )
            if is_pass:
                st.success("✅ 达标")
            else:
                st.error("❌ 未达标")
        else:
            st.warning("数据缺失")
            st.caption("💡 说明：Tushare原始数据中该年份的营业收入(revenue)或营业成本(oper_cost)字段缺失，无法计算毛利率。这可能是财报未披露或数据源问题。")
        
        st.caption("📝 产品/服务的基本赚钱能力")
    
    with col3:
        st.markdown("##### 2.3 经营活动现金流量净额")
        ocf_info = evaluation['ocf_info']
        
        st.markdown("**核心两问：**")
        
        # 第一问：连续3-5年为正吗？
        if ocf_info['consecutive_ok']:
            st.success(f"✅ 连续{ocf_info['max_consecutive']}年为正")
        else:
            st.warning(f"⚠️ 最长连续{ocf_info['max_consecutive']}年为正（要求≥{st.session_state.ocf_consecutive_years}年）")
        
        # 第二问：是否持续≥净利润？
        ratio = ocf_info['ge_profit_count'] / ocf_info['total_years'] if ocf_info['total_years'] > 0 else 0
        if ratio >= 0.8:
            st.success(f"✅ 现金流≥净利润：{ocf_info['ge_profit_count']}/{ocf_info['total_years']}年")
        else:
            st.warning(f"⚠️ 现金流≥净利润：{ocf_info['ge_profit_count']}/{ocf_info['total_years']}年")
        
        st.caption('💡 说明"赚到了真金白银"的能力')


def render_year_health_table(metrics: pd.DataFrame, evaluation: dict, sector_rules: dict):
    """渲染年度财务健康度表"""
    st.subheader("🚦 年度财务健康度")
    
    if metrics.empty:
        st.warning("⚠️ 无财务数据")
        return
    
    # 重要：确保只使用年度数据（end_date以1231结尾），过滤掉可能的季度数据
    if not metrics.empty and 'end_date' in metrics.columns:
        before_filter = len(metrics)
        metrics = metrics[metrics['end_date'].astype(str).str.endswith('1231')].copy()
        if before_filter != len(metrics):
            st.warning(f"⚠️ 检测到季度数据：从{before_filter}条记录过滤到{len(metrics)}条年度记录（只保留end_date以1231结尾的数据）")
    
    # 显示年份范围和实际数据年份（显示所有获取到的数据）
    if not metrics.empty:
        actual_years = sorted([row['end_date'][:4] for _, row in metrics.iterrows()])
        st.info(f"📊 共获取到 {len(metrics)} 个年度数据：{', '.join(actual_years)}（最新年份在上方）")
        # 如果数据少于期望的5年，给出提示
        if len(metrics) < 5:
            st.caption(f"💡 提示：默认查询最近5年数据，但该股票只有{len(metrics)}年数据。如需更多数据，可调整年份范围重新查询。")
    else:
        st.caption(f"共分析 {len(metrics)} 个年度数据（最新年份在上方）")
    
    # 按年份倒序显示（最新的在上面）
    # metrics 已经是按 end_date 降序排列的，所以直接正序遍历即可
    for idx in range(len(metrics)):
        row = metrics.iloc[idx]
        year = row['end_date'][:4]
        checks = evaluation['year_checks'][idx]
        score = evaluation['scores'][idx]
        
        # 创建一个容器（优化显示比例）
        with st.container():
            # 年份和得分（优化比例：年份占更大比例，更清晰）
            col_year, col_score = st.columns([2, 3])
            with col_year:
                st.markdown(f"### {year}")
            with col_score:
                score_dots = "🟢" * score + "⚪" * (3 - score)
                st.markdown(f"**年度得分：** {score}/3 {score_dots}")
            
            # 三个指标横向排列（等宽，更美观）
            cols = st.columns(3)
            
            with cols[0]:
                debt_ratio_pct = row['debt_ratio'] * 100 if pd.notna(row['debt_ratio']) else None
                if debt_ratio_pct is not None:
                    icon = "✅" if checks['debt_ratio_pass'] else "❌"
                    st.markdown(f"{icon} **资产负债率** {debt_ratio_pct:.2f}%")
                else:
                    st.markdown("❌ **资产负债率** 数据缺失")
            
            with cols[1]:
                gross_margin_pct = row['gross_margin'] * 100 if pd.notna(row['gross_margin']) else None
                if gross_margin_pct is not None:
                    icon = "✅" if checks['gross_margin_pass'] else "❌"
                    st.markdown(f"{icon} **毛利率** {gross_margin_pct:.2f}%")
                else:
                    st.markdown("❌ **毛利率** 数据缺失")
                    # 检查具体缺失原因
                    revenue = row.get('revenue', 0)
                    oper_cost = row.get('oper_cost', 0)
                    if pd.isna(revenue) or revenue == 0:
                        st.caption(f"💡 原因：营业收入(revenue)缺失或为0")
                    elif pd.isna(oper_cost):
                        st.caption(f"💡 原因：营业成本(oper_cost)缺失")
                    else:
                        st.caption(f"💡 原因：Tushare原始数据缺失，无法计算")
            
            with cols[2]:
                icon = "✅" if checks['ocf_positive'] else "❌"
                ocf_val = row['n_cashflow_act'] / 100000000 if pd.notna(row['n_cashflow_act']) else 0
                profit_val = row['n_income'] / 100000000 if pd.notna(row['n_income']) else 0
                
                if checks['ocf_positive']:
                    st.markdown(f"{icon} **经营净现金流≥0**")
                    # 显示现金流和净利润的对比（单位：亿元）
                    if checks['ocf_ge_profit']:
                        diff = ocf_val - profit_val
                        st.caption(f"✅ 收到现金{ocf_val:.2f}亿 > 账面利润{profit_val:.2f}亿，多{diff:.2f}亿")
                        st.success("💰 结论：赚到了真金白银！")
                    else:
                        diff = profit_val - ocf_val
                        st.caption(f"❌ 收到现金{ocf_val:.2f}亿 < 账面利润{profit_val:.2f}亿，少{diff:.2f}亿")
                        st.warning("⚠️ 结论：账面赚钱，但钱没收回来")
                else:
                    st.markdown(f"{icon} **经营净现金流<0**")
                    st.caption(f"收到现金{ocf_val:.2f}亿，账面利润{profit_val:.2f}亿")
                    st.error("🚨 结论：不仅没赚到钱，还在往外流失！")
            
            st.divider()


def render_health_charts(metrics: pd.DataFrame):
    """渲染年度财务健康度图表"""
    st.subheader("📈 年度财务健康度图表")
    
    if metrics.empty:
        st.warning("无数据")
        return
    
    # 准备数据
    # 重要：确保只使用年度数据（end_date以1231结尾），过滤掉可能的季度数据
    if not metrics.empty and 'end_date' in metrics.columns:
        before_filter = len(metrics)
        # 确保end_date是字符串类型
        metrics['end_date'] = metrics['end_date'].astype(str)
        metrics = metrics[metrics['end_date'].str.endswith('1231')].copy()
        if before_filter != len(metrics):
            st.warning(f"⚠️ 检测到季度数据：从{before_filter}条记录过滤到{len(metrics)}条年度记录（只保留end_date以1231结尾的数据）")
    
    if metrics.empty:
        st.warning("⚠️ 过滤后无年度数据")
        return
    
    # 重要：确保每个年份只有一条记录（按end_date去重，保留最新的）
    # 虽然已经过滤了季度数据，但为了保险起见，再次去重
    if not metrics.empty:
        # 按end_date去重，保留每个年份的最后一条记录（最新的）
        metrics = metrics.drop_duplicates(subset='end_date', keep='last').sort_values('end_date').reset_index(drop=True)
        print(f"🔍 图表生成：使用{len(metrics)}条年度数据，年份：{[row['end_date'][:4] for _, row in metrics.iterrows()]}")
    
    # 使用过滤后的年度数据生成图表
    # 使用整数年份作为X轴，避免Plotly自动处理重复值
    years = [int(row['end_date'][:4]) for _, row in metrics.iterrows()]
    debt_ratios = [row['debt_ratio'] * 100 if pd.notna(row['debt_ratio']) else None for _, row in metrics.iterrows()]
    gross_margins = [row['gross_margin'] * 100 if pd.notna(row['gross_margin']) else None for _, row in metrics.iterrows()]
    ocfs = [row['n_cashflow_act'] / 100000000 if pd.notna(row['n_cashflow_act']) else None for _, row in metrics.iterrows()]
    profits = [row['n_income'] / 100000000 if pd.notna(row['n_income']) else None for _, row in metrics.iterrows()]
    
    # 创建子图
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('资产负债率趋势', '毛利率趋势', '经营现金流趋势', '年度得分趋势')
    )
    
    # 资产负债率
    fig.add_trace(
        go.Scatter(x=years, y=debt_ratios, mode='lines+markers', name='资产负债率(%)',
                   line=dict(color='blue', width=2), marker=dict(size=8)),
        row=1, col=1
    )
    
    # 毛利率
    fig.add_trace(
        go.Scatter(x=years, y=gross_margins, mode='lines+markers', name='毛利率(%)',
                   line=dict(color='orange', width=2), marker=dict(size=8)),
        row=1, col=2
    )
    
    # 经营现金流 vs 净利润
    fig.add_trace(
        go.Scatter(x=years, y=ocfs, mode='lines+markers', name='经营现金流(亿)',
                   line=dict(color='purple', width=2), marker=dict(size=8)),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(x=years, y=profits, mode='lines+markers', name='净利润(亿)',
                   line=dict(color='green', width=2, dash='dash'), marker=dict(size=8)),
        row=2, col=1
    )
    
    # 年度得分
    sector_rules = SECTOR_RULES[st.session_state.selected_sector]
    evaluation = evaluate_metrics(metrics, sector_rules, st.session_state.ocf_consecutive_years)
    
    fig.add_trace(
        go.Scatter(x=years, y=evaluation['scores'], mode='lines+markers', name='年度得分',
                   line=dict(color='red', width=2), marker=dict(size=10)),
        row=2, col=2
    )
    
    # 更新布局
    # 重要：设置X轴为整数年份，避免显示小数（如2023.2, 2023.4）
    # 使用线性类型，每年一个刻度，确保只显示整数年份
    fig.update_xaxes(title_text="年份", type='linear', tickmode='linear', dtick=1, row=1, col=1)
    fig.update_xaxes(title_text="年份", type='linear', tickmode='linear', dtick=1, row=1, col=2)
    fig.update_xaxes(title_text="年份", type='linear', tickmode='linear', dtick=1, row=2, col=1)
    fig.update_xaxes(title_text="年份", type='linear', tickmode='linear', dtick=1, row=2, col=2)
    
    fig.update_yaxes(title_text="资产负债率(%)", row=1, col=1)
    fig.update_yaxes(title_text="毛利率(%)", row=1, col=2)
    fig.update_yaxes(title_text="金额(亿元)", row=2, col=1)
    fig.update_yaxes(title_text="年度得分", row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False)
    
    st.plotly_chart(fig, use_container_width=True)


def render_detailed_table(metrics: pd.DataFrame, evaluation: dict):
    """渲染详细财务数据表"""
    if metrics.empty:
        return
    
    # 重要：确保只使用年度数据（end_date以1231结尾），过滤掉可能的季度数据
    if not metrics.empty and 'end_date' in metrics.columns:
        before_filter = len(metrics)
        metrics = metrics[metrics['end_date'].astype(str).str.endswith('1231')].copy()
        if before_filter != len(metrics):
            st.warning(f"⚠️ 检测到季度数据：从{before_filter}条记录过滤到{len(metrics)}条年度记录（只保留end_date以1231结尾的数据）")
    
    with st.expander("📋 详细财务数据表", expanded=False):
        display_data = []
        for idx, (_, row) in enumerate(metrics.iterrows()):
            checks = evaluation['year_checks'][idx]
            score = evaluation['scores'][idx]
            
            display_data.append({
                "年份": row['end_date'][:4],
                "资产负债率(%)": f"{row['debt_ratio']*100:.2f}" if pd.notna(row['debt_ratio']) else "-",
                "负债率PASS": "✅" if checks['debt_ratio_pass'] else "❌",
                "毛利率(%)": f"{row['gross_margin']*100:.2f}" if pd.notna(row['gross_margin']) else "-",
                "毛利率PASS": "✅" if checks['gross_margin_pass'] else "❌",
                "经营净现金流": f"{row['n_cashflow_act']:,.2f}" if pd.notna(row['n_cashflow_act']) else "-",
                "经营净现金流>0": "✅" if checks['ocf_positive'] else "❌",
                "净利润": f"{row['n_income']:,.2f}" if pd.notna(row['n_income']) else "-",
                "OCF≥净利润": "✅" if checks['ocf_ge_profit'] else "❌",
                "年度得分": score
            })
        
        df_display = pd.DataFrame(display_data)
        st.dataframe(df_display, use_container_width=True, hide_index=True)


def page_single_analysis():
    """
    单项分析页面
    
    功能：
        1. 输入股票代码（自动识别补全后缀）
        2. 获取公司基本信息
        3. 获取财务数据（审计+资产负债+利润+现金流）
        4. 计算三大核心指标
        5. 生成年度健康度报告
        6. 展示趋势图表
        7. 给出投资建议
        8. 保存到历史记录
    
    数据缓存：
        查询结果自动缓存24小时，第二次查询秒开
    """
    st.header("🔎 单项分析")
    
    # 输入区域
    col1, col2 = st.columns([3, 1])
    with col1:
        ts_code = st.text_input(
            "股票代码",
            value="600519",
            placeholder="例如：600519 或 600519.SH",
            help="支持输入6位代码或完整代码"
        ).strip().upper()
        
        # 自动补全后缀
        if len(ts_code) == 6:
            if ts_code.startswith('6'):
                ts_code = f"{ts_code}.SH"
            else:
                ts_code = f"{ts_code}.SZ"
            st.info(f"📌 标准代码：**{ts_code}**")
    
    if st.button("🔍 开始分析", type="primary", use_container_width=True):
        if not ts_code:
            st.error("请填写股票代码")
            return
        
        # 计算日期范围
        start_date = f"{st.session_state.start_year}0101"
        end_date = f"{st.session_state.end_year}1231"
        
        # 显示实际使用的年份范围（调试信息）
        st.info(f"📅 查询年份范围：{st.session_state.start_year}年 - {st.session_state.end_year}年 (开始日期: {start_date}, 结束日期: {end_date})")
        
        # 连通性检测
        if st.session_state.debug_mode:
            with st.spinner("正在进行连通性检测..."):
                success, logs = run_connectivity_tests(verbose=False)
            
            with st.expander("🔌 连通性检测结果", expanded=not success):
                for log in logs:
                    if log["status"] == "PASS":
                        st.success(f"✅ {log['title']}：{log['message']}")
                    else:
                        st.error(f"❌ {log['title']}：{log['message']}")
            
            if not success:
                st.warning("连通性检测未全部通过，但继续尝试分析...")
        
        # 数据分析（带持久化缓存）
        # 显示缓存状态
        if st.session_state.debug_mode:
            cache_info = data_cache.get_cache_info()
            st.info(f"🔍 缓存统计：有效 {cache_info['valid']} 个 | 过期 {cache_info['expired']} 个 | 总大小 {cache_info['size_mb']} MB")
        
        # 显示即将调用的API次数和预计时间
        total_time = st.session_state.api_delay * 4  # 4个延迟间隔（5次调用）
        st.info(f"💡 查询设置：延迟{st.session_state.api_delay}秒/次 | 预计耗时{total_time}秒 | 如有缓存则0秒返回")
        st.caption("💡 包含：公司信息+审计意见+资产负债表+利润表+现金流量表")
        
        # 创建进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(message, value):
            """更新进度"""
            status_text.text(message)
            progress_bar.progress(value)
        
        try:
            # 从session_state获取积分信息（避免重复调用API）
            user_points = None
            if 'user_points_info' in st.session_state and st.session_state.user_points_info:
                user_points = st.session_state.user_points_info.get('total_points', 2000)
            
            result = analyze_fundamentals(
                ts_code, start_date, end_date, 
                use_cache=True,
                api_delay=st.session_state.api_delay,
                progress_callback=update_progress,
                user_points=user_points  # 传入积分信息，避免重复调用API
            )
            
            progress_bar.empty()
            status_text.empty()
            
            # 检查是否使用了缓存（通过判断耗时）
            # 如果result中包含缓存标记更好，但这里简化处理
            st.success("✅ 数据获取成功！")
        except Exception as exc:
            error_msg = str(exc)
            st.error(f"❌ 分析失败：{error_msg}")
            
            # 判断错误类型并给出建议
            if "超限" in error_msg or "超过" in error_msg or "限制" in error_msg:
                st.error("### 🚫 Tushare API 访问限制")
                
                st.markdown(f"""
**错误信息：** `{error_msg}`

**问题分析：**
- 每次查询消耗：**4次** API调用（审计+资产负债表+利润表+现金流量表）
- 限制规则：根据您的积分等级，有不同的频率限制

**Tushare 用户等级：**

| 积分 | 等级 | 每分钟限制 | 每天限制 |
|-----|------|----------|---------|
| 0-119 | 未认证 | 2次 | 200次 |
| 120-599 | 注册用户 | 5次 | 500次 |
| 600-4999 | 中级用户 | 20次 | 2000次 |
| 5000+ | 高级用户 | 200次 | 20000次 |

**请检查您的实际积分：**
1. 访问 https://tushare.pro/user/token
2. 查看您的积分数和权限等级
3. 如果积分充足但仍超限，可能是：
   - 在1分钟内查询了太多股票
   - 今日调用总次数已达上限
   - 某些财务接口有特殊限制

**立即解决：**
- ⏰ **等待1-2分钟后重试**
- 📦 使用缓存：查询过的股票会自动缓存，不消耗API
- 🎯 减少查询频率：先分析完一个，再查下一个

**系统优化：**
- ✅ 已添加智能缓存，重复查询不调用API
- ✅ 会显示API调用次数提醒
- ✅ 建议开启调试模式查看详细信息
                """)
                
                # 添加清除缓存按钮
                col1, col2 = st.columns(2)
                if col1.button("🗑️ 清除所有缓存"):
                    count = data_cache.clear_all()
                    st.success(f"已清除 {count} 个缓存文件")
                    st.rerun()
                if col2.button("🧹 清理过期缓存"):
                    count = data_cache.clear_expired()
                    st.success(f"已清理 {count} 个过期缓存")
                    st.rerun()
            elif "代码" in error_msg or "code" in error_msg.lower():
                st.warning("""
### ⚠️ 股票代码错误

**可能原因：**
- 股票代码输入错误
- 该股票不存在或已退市
- 代码格式不正确

**建议：**
- 检查股票代码是否正确（如：600519.SH）
- 确认该股票是否还在交易
- 尝试其他股票代码
                """)
            else:
                st.warning("""
### ⚠️ 数据获取失败

**可能原因：**
- 网络连接问题
- Tushare 服务器暂时不可用
- 该股票数据缺失

**建议：**
- 检查网络连接
- 稍后重试
- 尝试其他股票或调整年份范围
                """)
            
            if st.session_state.debug_mode:
                with st.expander("🔧 详细错误信息", expanded=False):
                    st.exception(exc)
            return
        
        company_info = result.get("company_info", None)
        audit_records = result.get("audit_records", [])
        metrics = result.get("metrics", None)
        
        # 调试信息：显示实际获取到的数据年份
        if metrics is not None and not metrics.empty:
            actual_years = sorted([row['end_date'][:4] for _, row in metrics.iterrows()])
            if st.session_state.debug_mode:
                st.info(f"🔍 调试信息：实际获取到 {len(metrics)} 年数据，年份：{', '.join(actual_years)}")
        
        # 检查是否有数据
        if metrics is None or metrics.empty:
            st.error("❌ 未获取到财务数据")
            st.warning(f"""
**可能的原因：**
1. 该股票在 {st.session_state.start_year}-{st.session_state.end_year} 期间没有年报数据
2. 股票代码输入错误
3. 该公司尚未上市或已退市

**建议：**
- 检查股票代码是否正确
- 调整查询年份范围
- 尝试其他股票代码
            """)
            return
        
        # 获取行业规则
        sector_rules = SECTOR_RULES[st.session_state.selected_sector]
        
        # 评估得分
        evaluation = evaluate_metrics(metrics, sector_rules, st.session_state.ocf_consecutive_years)
        
        # 显示公司基本信息
        if company_info:
            st.subheader("📌 公司基本信息")
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.markdown(f"**股票代码：** {company_info.get('ts_code', ts_code)}")
                st.markdown(f"**公司全称：** {company_info.get('com_name', '未知')}")
                
                # 董事长信息
                chairman = company_info.get('chairman', '')
                if chairman and chairman.strip():
                    st.markdown(f"**董事长：** {chairman}")
                
                if company_info.get('province') and company_info.get('city'):
                    st.markdown(f"**所在地：** {company_info.get('province', '')}{company_info.get('city', '')}")
                if company_info.get('setup_date'):
                    st.markdown(f"**成立日期：** {company_info.get('setup_date', '')}")
            
            with col2:
                main_business = company_info.get('main_business', '')
                if main_business and main_business.strip():
                    st.markdown(f"**主要业务及产品：**")
                    st.info(main_business[:200] + ('...' if len(main_business) > 200 else ''))
                
                business_scope = company_info.get('business_scope', '')
                if business_scope and business_scope.strip():
                    with st.expander("📋 经营范围详情", expanded=False):
                        st.write(business_scope)
            
            st.divider()
        
        # 显示综合评分
        st.success(f"✅ 分析完成！行业分类：**{sector_rules['name']}** | 获取到 **{len(metrics)}年** 财务数据")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("平均分", f"{evaluation['avg_score']:.2f}/3.00")
        col2.metric("最新年分", f"{evaluation['latest_score']}/3")
        col3.metric("红旗年数", f"{evaluation['red_flags']}年")
        col4.metric("分析年限", f"{len(metrics)}年")
        
        st.divider()
        
        # 1. 审计意见
        render_audit_opinion(audit_records)
        
        st.divider()
        
        # 重要：在显示前统一过滤，确保只使用年度数据（end_date以1231结尾）
        if not metrics.empty and 'end_date' in metrics.columns:
            before_filter = len(metrics)
            metrics['end_date'] = metrics['end_date'].astype(str)
            metrics = metrics[metrics['end_date'].str.endswith('1231')].copy()
            if before_filter != len(metrics):
                st.warning(f"⚠️ 检测到季度数据：从{before_filter}条记录过滤到{len(metrics)}条年度记录（只保留end_date以1231结尾的数据）")
                # 重新评估（因为数据量变了）
                evaluation = evaluate_metrics(metrics, sector_rules, st.session_state.ocf_consecutive_years)
        
        # 2. 三大核心指标
        render_core_indicators(metrics, evaluation, sector_rules)
        
        st.divider()
        
        # 年度明细
        render_year_health_table(metrics, evaluation, sector_rules)
        
        # 图表
        render_health_charts(metrics)
        
        # 详细数据表
        render_detailed_table(metrics, evaluation)
        
        # 综合评价
        st.subheader("💡 年度总结")
        
        ocf_info = evaluation['ocf_info']
        red_flags = evaluation['red_flags']
        total_years = len(metrics)
        good_years = total_years - red_flags
        
        # 财务健康度总结
        st.markdown("#### 🏥 财务健康度")
        
        if evaluation['avg_score'] >= 2.5:
            st.success("**评级：优秀** 🟢")
            st.write(f"✅ 近{total_years}年财务指标稳健，{good_years}年达标，{red_flags}年需改善")
            st.write("💡 **建议：** 可以深入研究，值得关注")
        elif evaluation['avg_score'] >= 2.0:
            st.info("**评级：良好** 🟡")
            st.write(f"✅ 近{total_years}年整体表现不错，{good_years}年达标，{red_flags}年需改善")
            st.write("💡 **建议：** 可以关注，但需要结合行业情况")
        elif evaluation['avg_score'] >= 1.5:
            st.warning("**评级：一般** 🟠")
            st.write(f"⚠️ 近{total_years}年表现波动较大，{red_flags}年未达标")
            st.write("💡 **建议：** 需要谨慎评估，深入分析未达标原因")
        else:
            st.error("**评级：较差** 🔴")
            st.write(f"❌ 近{total_years}年有{red_flags}年财务指标未达标")
            st.write("💡 **建议：** 风险较高，建议回避")
        
        st.divider()
        
        # 现金流总结（最关键）
        st.markdown("#### 💰 最关键：公司到底赚没赚到钱？")
        
        cash_quality = ocf_info['ge_profit_count'] / total_years if total_years > 0 else 0
        
        if ocf_info['consecutive_ok'] and cash_quality >= 0.8:
            st.success("**结论：真金白银，赚到手了！** ✅")
            st.markdown(f"""
**现金流表现：**
- ✅ 连续 **{ocf_info['max_consecutive']}年** 收到真实现金
- ✅ 近{total_years}年中有 **{ocf_info['ge_profit_count']}年** 现金流≥账面利润（占比{cash_quality*100:.0f}%）
- ✅ 赚的是实实在在的钱，不是"账面富翁"

**通俗解读：** 就像做生意，不仅账本上有利润，钱也真的收回来了。这种公司才是真赚钱！
            """)
            
        elif ocf_info['consecutive_ok']:
            st.info("**结论：基本赚钱，但要注意收款** 🟡")
            st.markdown(f"""
**现金流表现：**
- ✅ 连续 **{ocf_info['max_consecutive']}年** 有现金流入
- ⚠️ 但有 **{total_years - ocf_info['ge_profit_count']}年** 收到的钱少于账面利润
- ⚠️ 部分利润可能还在客户那里（应收账款）

**通俗解读：** 生意是赚钱的，但有些钱还没收回来，要注意是否能收回。
            """)
            
        elif ocf_info['max_consecutive'] >= 2:
            st.warning("**结论：收款能力不稳定** ⚠️")
            st.markdown(f"""
**现金流表现：**
- ⚠️ 最长连续 **{ocf_info['max_consecutive']}年** 有现金（标准要求≥{st.session_state.ocf_consecutive_years}年）
- ⚠️ 现金流断断续续，不够稳定
- ⚠️ 有些年份钱收不回来

**通俗解读：** 像做生意时好时坏，今年赚了钱收回来，明年又收不回。这种不稳定性需要警惕。
            """)
            
        else:
            st.error("**结论：可能是纸上富贵** 🚨")
            st.markdown(f"""
**现金流表现：**
- ❌ 最长只连续 **{ocf_info['max_consecutive']}年** 有现金
- ❌ 现金流很不稳定
- ❌ 账面有利润，但钱大多没收回来

**通俗解读：** 就像欠条一大堆，看着账面有钱，实际钱包空空。这种公司风险很高！

**投资建议：** 强烈建议回避
            """)
        
        # 保存历史
        HistoryManager.add_record({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "单项分析",
            "code": ts_code,
            "sector": sector_rules['name'],
            "year_range": f"{st.session_state.start_year}-{st.session_state.end_year}",
            "avg_score": float(evaluation['avg_score']),
            "latest_score": int(evaluation['latest_score']),
            "red_flags": int(evaluation['red_flags']),
        })


def page_pr_valuation():
    """
    市赚率估值分析页面
    
    功能：
        基于PE和ROE的估值方法，判断股票/指数的买卖时机
    
    支持类型：
        - 个股：使用修正市赚率（考虑分红质量）
        - 宽基指数：使用宽基市赚率（分母150）
    
    核心公式：
        - 标准PR = PE / ROE / 100（ROE取小数形式）
        - 修正PR = N × PE / ROE / 100（N根据股息支付率确定）
        - 宽基PR = PE / ROE / 150
    
    输出：
        - 基础数据展示
        - 详细计算过程
        - 交易信号（买入/持有/卖出）
        - 建议仓位
        - 可视化阈值对照
    """
    st.header("💰 市赚率估值分析")
    st.markdown("*基于PE和ROE的估值方法，判断买卖时机*")
    
    st.info(
        """
**速记规则（注意ROE需先换算为小数，如13%→0.13）：**

- **买单个公司**：`PR = PE / ROE / 150` → PR < 1.0 划算，PR > 1.0 偏贵（需结合股息修正）
- **买整个指数**：`PR = PE / ROE / 150` → PR < 1.0 划算，PR > 1.0 偏贵（不同指数有细分阈值）
- **巴菲特卖点**：`PR ≈ 1.5` → 整个市场太贵，建议大幅减仓/清仓
        """
    )
    
    # 输入区域
    col1, col2 = st.columns([2, 1])
    
    with col1:
        raw_code = st.text_input(
            "股票/指数代码",
            value="600519",
            placeholder="例如：600519 或 000300（沪深300）",
            help="支持个股或指数代码",
            key="pr_code"
        ).strip().upper()
    
    with col2:
        # 选择类型
        target_type = st.selectbox(
            "类型",
            options=["个股", "宽基指数"],
            help="个股使用修正市赚率，指数使用宽基市赚率"
        )
    
    # 日期选择
    trade_date = st.date_input(
        "查询日期",
        value=datetime.now(),
        help="查询指定交易日的估值数据"
    )
    
    ts_code = normalize_ts_code(raw_code, target_type)
    if ts_code and ts_code != raw_code:
        st.info(f"📌 标准代码：**{ts_code}**")
    
    if st.button("📊 开始估值分析", type="primary", use_container_width=True, key="btn_pr"):
        if not ts_code:
            st.error("请填写股票代码")
            return
        
        # 转换日期格式
        trade_date_str = trade_date.strftime("%Y%m%d")
        
        # 获取估值数据
        try:
            with st.spinner(f"正在获取 {ts_code} 在 {trade_date_str} 的估值数据..."):
                val_data = fetch_valuation_data(ts_code, trade_date_str, target_type)
            
            if val_data is None:
                st.error("❌ 未获取到估值数据")
                st.warning("""
**可能原因：**
1. 该日期不是交易日
2. 数据尚未更新
3. 股票代码错误

**建议：**
- 选择最近的交易日
- 检查股票代码是否正确
                """)
                return
            
            # 显示原始数据
            st.subheader("📊 基础数据")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("收盘价", format_metric_value(val_data.get('close'), suffix="元"))
            col2.metric("市盈率TTM", format_metric_value(val_data.get('pe_ttm')))
            col3.metric("加权ROE", format_metric_value(val_data.get('roe_waa'), suffix="%"))
            col4.metric("每股收益", format_metric_value(val_data.get('eps'), suffix="元"))
            col5.metric("每股股息", format_metric_value(val_data.get('dividend_per_share'), suffix="元"))
            
            missing_metrics = []
            if val_data.get('pe_ttm') is None:
                missing_metrics.append("市盈率TTM（pe_ttm）")
            if target_type == "个股" and val_data.get('roe_waa') is None:
                missing_metrics.append("加权ROE（roe_waa）")
            if target_type == "个股" and val_data.get('eps') is None:
                missing_metrics.append("每股收益（eps）")
            
            if missing_metrics:
                st.warning("⚠️ 以下核心字段缺失，部分公式可能无法计算：\n- " + "\n- ".join(missing_metrics))
            
            st.divider()
            
            # 计算市赚率
            if target_type == "个股":
                # 个股估值分析
                result = PRValuation.analyze_stock_valuation(val_data)
                
                st.subheader("💎 个股估值分析")
                
                # 显示计算过程
                with st.expander("📐 计算过程详解", expanded=True):
                    st.markdown("### 第1步：计算股息支付率")
                    payout_ratio = result.get('payout_ratio')
                    dividend_val = val_data.get('dividend_per_share')
                    eps_val = val_data.get('eps')
                    if dividend_val is None:
                        st.info("提示：最近披露的分红记录中未找到每股股息，可能未分红或尚未披露。")
                    else:
                        st.caption(f"最近一次每股股息：{format_metric_value(dividend_val, '.2f', '元')}")
                    if payout_ratio is not None:
                        st.latex(r"\text{股息支付率} = \frac{\text{每股股息}}{\text{基本每股收益}} \times 100\%")
                        dividend_text = format_metric_value(dividend_val)
                        eps_text = format_metric_value(eps_val)
                        st.markdown(f"= {dividend_text} / {eps_text} × 100%")
                        st.success(f"**= {format_metric_value(payout_ratio, '.2f', '%')}**")
                    else:
                        st.warning("无法计算（缺少分红或收益数据）")
                    
                    st.markdown("### 第2步：确定修正系数N")
                    payout = result.get('payout_ratio')
                    if payout is None:
                        st.warning("股息支付率缺失，默认采用N = 2.0进行保守评估")
                    elif payout >= 50:
                        st.info(f"股息支付率{format_metric_value(payout, '.2f', '%')} ≥ 50% → **N = 1.0**（分红充足）")
                    elif payout <= 25:
                        st.warning(f"股息支付率{format_metric_value(payout, '.2f', '%')} ≤ 25% → **N = 2.0**（分红不足，需加倍修正）")
                    else:
                        st.info(
                            f"股息支付率{format_metric_value(payout, '.2f', '%')}在25%-50%之间 → "
                            f"**N = 50% / {format_metric_value(payout, '.2f', '%')} = {format_metric_value(result['correction_factor'], '.3f', '')}**"
                        )
                    
                    st.markdown("### 第3步：计算修正市赚率")
                    st.latex(r"\text{修正PR} = N \times \frac{\text{PE}_{\text{TTM}}}{\text{ROE（小数）}} \div 150")
                    pe_text = format_metric_value(val_data.get('pe_ttm'))
                    roe_raw = val_data.get('roe_waa')
                    if roe_raw is None or (isinstance(roe_raw, (int, float)) and pd.isna(roe_raw)):
                        roe_decimal = None
                    else:
                        try:
                            roe_val = float(roe_raw)
                            roe_decimal = roe_val / 100 if abs(roe_val) > 1 else roe_val
                        except (TypeError, ValueError):
                            roe_decimal = None
                    roe_text = format_metric_value(val_data.get('roe_waa'), suffix="%")
                    roe_decimal_text = format_metric_value(roe_decimal, ".4f")
                    st.caption(f"ROE原始值：{roe_text} → 换算为小数：{roe_decimal_text}")
                    st.markdown(
                        f"= {format_metric_value(result.get('correction_factor'), '.3f')} × {pe_text} / {roe_decimal_text} / 150"
                    )
                    if result['corrected_pr'] is not None:
                        st.success(f"**= {format_metric_value(result['corrected_pr'], '.4f', '')}**")
                    else:
                        st.warning("缺少PE或ROE数据，无法计算修正市赚率")
                    
                    st.markdown("### 第4步：汇总公式拆解")
                    standard_pr_val = result.get('standard_pr')
                    corrected_pr_val = result.get('corrected_pr')
                    st.markdown(
                        f"""
**标准市赚率（PR_standard）**  
= {pe_text} / {roe_decimal_text} / 150  
{f"= {format_metric_value(standard_pr_val, '.4f', '')}" if standard_pr_val is not None else "= 无法计算（缺少PE或ROE）"}
                        
**修正市赚率（PR_corrected）**  
= {format_metric_value(result.get('correction_factor'), '.3f')} × {pe_text} / {roe_decimal_text} / 150  
{f"= {format_metric_value(corrected_pr_val, '.4f', '')}" if corrected_pr_val is not None else "= 无法计算（缺少PE或ROE）"}
                        """
                    )
                
                # 显示最终结果
                col1, col2, col3 = st.columns(3)
                col1.metric("标准市赚率", format_metric_value(result.get('standard_pr'), ".4f", "", "N/A"))
                col2.metric("修正市赚率", format_metric_value(result.get('corrected_pr'), ".4f", "", "N/A"))
                col3.metric("股息支付率", format_metric_value(result.get('payout_ratio'), ".2f", "%", "N/A"))
                
                # 计算并显示巴菲特指标
                pe_ttm = val_data.get('pe_ttm')
                roe_waa = val_data.get('roe_waa')
                buffett_sell_pr = PRValuation.calculate_buffett_sell_pr(pe_ttm, roe_waa)
                buffett_buy_pr = PRValuation.calculate_buffett_buy_pr(pe_ttm, roe_waa)
                
                st.divider()
                st.subheader("📊 巴菲特估值指标")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 🏛️ 巴菲特卖标普指标（系数150）")
                    st.markdown("*判断整个市场是否太贵*")
                    buffett_sell_display = format_metric_value(buffett_sell_pr, ".4f", "", "N/A")
                    st.metric("PR值", buffett_sell_display)
                    if buffett_sell_pr is not None:
                        if buffett_sell_pr > 1.5:
                            st.error("⚠️ PR > 1.5，整个市场太贵了，建议清仓")
                        elif buffett_sell_pr > 1.0:
                            st.warning("⚠️ PR > 1.0，市场偏高，考虑减仓")
                        else:
                            st.success("✅ PR ≤ 1.0，市场估值合理")
                    st.caption("公式：PR = PE / ROE / 150")
                
                with col2:
                    st.markdown("### 💰 巴菲特购买股票指标（系数100）")
                    st.markdown("*判断个股是否值得买入*")
                    buffett_buy_display = format_metric_value(buffett_buy_pr, ".4f", "", "N/A")
                    st.metric("PR值", buffett_buy_display)
                    if buffett_buy_pr is not None:
                        if buffett_buy_pr < 0.4:
                            st.success("✅ PR < 0.4，严重低估（用40美分买1美元资产），强烈买入")
                        elif buffett_buy_pr < 0.6:
                            st.info("💡 PR 0.4-0.6，低估（用50-60美分买1美元资产），可买入")
                        elif buffett_buy_pr < 1.0:
                            st.info("💡 PR 0.6-1.0，合理估值，可持有")
                        else:
                            st.warning("⚠️ PR > 1.0，可能高估，建议卖出或持有")
                    st.caption("公式：PR = PE / ROE / 100")
                
            else:
                # 指数估值分析
                index_name = st.selectbox(
                    "选择指数",
                    options=["沪深300", "恒生指数", "恒生国企"],
                    help="不同指数有不同的估值基准"
                )
                
                result = PRValuation.analyze_index_valuation(val_data, index_name)
                
                st.subheader("💎 指数估值分析")
                
                # 显示计算过程
                with st.expander("📐 计算过程详解", expanded=True):
                    st.markdown("### 宽基指数市赚率公式")
                    st.latex(r"\text{宽基PR} = \frac{\text{PE}_{\text{TTM}}}{\text{ROE（小数）}} \div 150")
                    index_roe_raw = val_data.get('roe_waa')
                    if index_roe_raw is None or (isinstance(index_roe_raw, (int, float)) and pd.isna(index_roe_raw)):
                        index_roe_decimal = None
                    else:
                        try:
                            roe_val = float(index_roe_raw)
                            index_roe_decimal = roe_val / 100 if abs(roe_val) > 1 else roe_val
                        except (TypeError, ValueError):
                            index_roe_decimal = None
                    index_roe_percent_text = format_metric_value(index_roe_raw, suffix="%")
                    index_roe_decimal_text = format_metric_value(index_roe_decimal, ".4f")
                    st.caption(f"ROE原始值：{index_roe_percent_text} → 换算为小数：{index_roe_decimal_text}")
                    st.markdown(
                        f"= {format_metric_value(val_data.get('pe_ttm'))} / {index_roe_decimal_text} / 150"
                    )
                    if result['broad_pr'] is not None:
                        st.success(f"**= {format_metric_value(result['broad_pr'], '.4f', '')}**")
                    else:
                        st.warning("缺少PE或ROE数据，无法计算宽基市赚率")
                        st.caption("提示：指数需要通过 `index_dailybasic` 获取PE，通过行业研究机构或手工录入ROE")
                    
                    st.caption("💡 个股与宽基指数当前统一采用150作为分母，指数相当于继续沿用保守口径")
                    
                    st.markdown("### 公式拆解")
                    broad_pr_val = result.get('broad_pr')
                    st.markdown(
                        f"""
**宽基市赚率（PR_broad）**  
= {format_metric_value(val_data.get('pe_ttm'))} / {index_roe_decimal_text} / 150  
{f"= {format_metric_value(broad_pr_val, '.4f', '')}" if broad_pr_val is not None else "= 无法计算（缺少PE或ROE）"}
                        """
                    )
                
                # 显示指数配置
                benchmark = result['benchmark']
                if benchmark:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("合理PR基准", format_metric_value(benchmark.get('reasonable_pr'), ".2f", ""))
                    col2.metric("股息税率", format_metric_value(benchmark.get('tax_rate', 0) * 100, ".0f", "%"))
                    col3.metric("当前PR", format_metric_value(result.get('broad_pr'), ".4f", "", "N/A"))
                
                # 计算并显示巴菲特指标（指数分析也显示）
                index_pe_ttm = val_data.get('pe_ttm')
                index_roe_waa = val_data.get('roe_waa')
                index_buffett_sell_pr = PRValuation.calculate_buffett_sell_pr(index_pe_ttm, index_roe_waa)
                index_buffett_buy_pr = PRValuation.calculate_buffett_buy_pr(index_pe_ttm, index_roe_waa)
                
                st.divider()
                st.subheader("📊 巴菲特估值指标")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 🏛️ 巴菲特卖标普指标（系数150）")
                    st.markdown("*判断整个市场是否太贵*")
                    index_buffett_sell_display = format_metric_value(index_buffett_sell_pr, ".4f", "", "N/A")
                    st.metric("PR值", index_buffett_sell_display)
                    if index_buffett_sell_pr is not None:
                        if index_buffett_sell_pr > 1.5:
                            st.error("⚠️ PR > 1.5，整个市场太贵了，建议清仓")
                        elif index_buffett_sell_pr > 1.0:
                            st.warning("⚠️ PR > 1.0，市场偏高，考虑减仓")
                        else:
                            st.success("✅ PR ≤ 1.0，市场估值合理")
                    st.caption("公式：PR = PE / ROE / 150")
                
                with col2:
                    st.markdown("### 💰 巴菲特购买股票指标（系数100）")
                    st.markdown("*判断个股是否值得买入*")
                    index_buffett_buy_display = format_metric_value(index_buffett_buy_pr, ".4f", "", "N/A")
                    st.metric("PR值", index_buffett_buy_display)
                    if index_buffett_buy_pr is not None:
                        if index_buffett_buy_pr < 0.4:
                            st.success("✅ PR < 0.4，严重低估（用40美分买1美元资产），强烈买入")
                        elif index_buffett_buy_pr < 0.6:
                            st.info("💡 PR 0.4-0.6，低估（用50-60美分买1美元资产），可买入")
                        elif index_buffett_buy_pr < 1.0:
                            st.info("💡 PR 0.6-1.0，合理估值，可持有")
                        else:
                            st.warning("⚠️ PR > 1.0，可能高估，建议卖出或持有")
                    st.caption("公式：PR = PE / ROE / 100")
            
            st.divider()
            
            # 显示交易信号
            if result.get('signal'):
                signal = result['signal']
                
                st.subheader("🚦 交易信号")
                
                # 根据信号类型显示不同颜色
                if signal['signal_type'] == "买入":
                    st.success(f"### 🟢 {signal['signal_type']}")
                elif signal['signal_type'] == "持有":
                    st.info(f"### 🟡 {signal['signal_type']}")
                elif signal['signal_type'] == "部分卖出":
                    st.warning(f"### 🟠 {signal['signal_type']}")
                else:
                    st.error(f"### 🔴 {signal['signal_type']}")
                
                # 详细信号信息
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("估值状态", signal['valuation_status'])
                col2.metric("市赚率", format_metric_value(signal.get('pr_value'), ".4f"))
                col3.metric("建议仓位", format_metric_value(signal.get('suggested_position', 0) * 100, ".1f", "%"))
                col4.metric("卖出比例", format_metric_value(signal.get('sell_ratio', 0) * 100, ".1f", "%"))
                
                # 决策理由
                st.info(f"**决策理由：** {signal['reason']}")
                
                # 阈值对照
                with st.expander("📊 阈值对照表", expanded=False):
                    threshold_df = pd.DataFrame([
                        {"阈值类型": "买入阈值", "PR值": signal['buy_threshold'], "说明": "低于此值建议买入"},
                        {"阈值类型": "开始卖出", "PR值": signal['sell_start'], "说明": "高于此值开始卖出"},
                        {"阈值类型": "完全清仓", "PR值": signal['sell_all'], "说明": "高于此值全部清仓"},
                        {"阈值类型": "当前PR", "PR值": signal['pr_value'], "说明": "当前市赚率水平"}
                    ])
                    st.dataframe(threshold_df, use_container_width=True, hide_index=True)
                
                # 可视化阈值
                fig = go.Figure()
                
                # 添加阈值线
                fig.add_hline(y=signal['buy_threshold'], line_dash="dash", line_color="green", 
                             annotation_text=f"买入阈值 {signal['buy_threshold']:.2f}")
                fig.add_hline(y=signal['sell_start'], line_dash="dash", line_color="orange", 
                             annotation_text=f"卖出阈值 {signal['sell_start']:.2f}")
                fig.add_hline(y=signal['sell_all'], line_dash="dash", line_color="red", 
                             annotation_text=f"清仓阈值 {signal['sell_all']:.2f}")
                
                # 添加当前PR点
                fig.add_scatter(x=["当前PR"], y=[signal['pr_value']], 
                               mode='markers', marker=dict(size=20, color='blue'),
                               name="当前市赚率")
                
                fig.update_layout(
                    title="市赚率水平对照",
                    yaxis_title="PR值",
                    height=400,
                    showlegend=True
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"❌ 估值分析失败：{e}")
            if st.session_state.debug_mode:
                st.exception(e)


def page_full_market_screening():
    """
    全网筛选页面 - 简化版本，一步步实现
    
    业务逻辑：
        1. 获取全部A股股票列表（排除ST股）
        2. 遍历列表，对每只股票执行深度分析
        3. 应用基本面和估值的双重筛选规则
        4. 收集所有通过筛选的股票
        5. 按修正市赚率从低到高进行排序
        6. 输出最终结果列表
    
    筛选规则：
        第一层：基本面判断
            - 审计意见：近5年审计结论必须全部为"标准无保留意见"
            - 现金流质量：经营活动现金流≥0 且 收到的现金≥账面利润
        
        第二层：巴菲特估值判断
            - 市赚率计算：使用修正市赚率（NPR）
            - 估值阈值：PR ≤ 用户设定的上限（默认1.0）
            - ROE要求：≥ 用户设定的下限（默认10.0%）
    """
    # 初始化session_state（简化版）
    if 'screening_results' not in st.session_state:
        st.session_state.screening_results = []
    if 'stock_list' not in st.session_state:
        st.session_state.stock_list = None  # 缓存股票列表
    if 'screening_in_progress' not in st.session_state:
        st.session_state.screening_in_progress = False  # 筛选是否进行中
    if 'screening_progress' not in st.session_state:
        st.session_state.screening_progress = {
            'processed': 0,
            'total': 0,
            'passed': 0,
            'failed': 0,
            'current_index': 0,  # 当前处理的股票索引
            'start_time': datetime.now(),  # 记录开始时间
            'last_update_time': datetime.now(),  # 记录最后更新时间
            'time_records': []  # 记录每只股票的耗时
        }
    if 'screening_history' not in st.session_state:
        st.session_state.screening_history = []  # 筛选历史记录

    # 左侧配置面板
    with st.sidebar:
        st.header("⚙️ 配置面板")
        
        # 分析参数
        st.subheader("📊 分析参数")
        
        years = st.number_input(
            "年数",
            min_value=3,
            max_value=10,
            value=5,
            step=1,
            help="分析历史财务数据的年数"
        )
        
        min_roe = st.number_input(
            "最低ROE (%)",
            min_value=0.0,
            max_value=50.0,
            value=10.0,
            step=0.5,
            help="最低净资产收益率要求"
        )
        
        max_pr = st.number_input(
            "最高PR",
            min_value=0.1,
            max_value=2.0,
            value=1.0,
            step=0.1,
            help="修正市赚率的上限"
        )
        
        # 高级设置
        st.subheader("🔧 高级设置")
        
        # 显示当前用户积分等级和API延迟规则
        try:
            from utils import get_api_delay
            # 从session_state读取积分信息（避免重复调用API）
            points_info = st.session_state.get('user_points_info', None)
            if points_info:
                user_points = points_info.get('total_points', 2000)
                # 显示积分等级
                if user_points < 120:
                    level = "免费用户"
                    financial_delay = get_api_delay('fina_audit', user_points)
                elif user_points < 600:
                    level = "注册用户"
                    financial_delay = get_api_delay('fina_audit', user_points)
                elif user_points < 5000:
                    level = "中级用户"
                    financial_delay = get_api_delay('fina_audit', user_points)
                else:
                    level = "高级用户"
                    financial_delay = get_api_delay('fina_audit', user_points)
                
                # 显示单线程和多线程的延迟差异
                single_thread_delay = get_api_delay('fina_audit', user_points, max_workers=1)
                multi_thread_delay = get_api_delay('fina_audit', user_points, max_workers=max_workers)
                company_delay = get_api_delay('stock_company', user_points, max_workers=max_workers)
                st.info(f"💰 当前积分：{user_points:.0f}分（{level}）| 财务API延迟：{multi_thread_delay:.2f}秒/次（{max_workers}线程）| 公司信息API延迟：{company_delay:.2f}秒/次")
                st.caption(f"💡 说明：财务API单线程延迟={single_thread_delay:.2f}秒/次，{max_workers}线程并发时延迟={multi_thread_delay:.2f}秒/次（全局限制：每分钟200次）")
            else:
                st.info("💰 无法获取积分信息，使用默认延迟设置（中级用户：3秒/次）")
        except:
            st.info("💰 使用默认延迟设置（中级用户：3秒/次）")
        
        api_delay = st.number_input(
            "额外API延迟 (秒)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,  # 默认0秒，使用API规则自动计算的延迟
            step=0.1,
            help="在API规则延迟基础上额外增加的延迟（可选，用于更保守的调用策略）"
        )
        
        max_workers = st.number_input(
            "线程数",
            min_value=1,
            max_value=20,  # 优化：提高最大值到20，支持更高并发
            value=10,  # 优化：默认10线程，加速筛选（从4提升到10）
            step=1,
            help="并发处理线程数（建议：中级用户10，高级用户20）"
        )
        
        # 历史记录
        st.subheader("📜 历史记录")
        
        if st.session_state.screening_history:
            for record in st.session_state.screening_history[-10:]:  # 显示最近10条
                date_str = record.get('date', '')
                count = record.get('count', 0)
                st.caption(f"{date_str} ({count}只)")
        else:
            st.caption("暂无历史记录")
        
        # 帮助按钮
        if st.button("❓ 帮助", use_container_width=True):
            st.info("""
            **筛选规则：**
            1. 审计意见：近5年全部标准无保留
            2. 现金流：≥0 且 ≥净利润
            3. 估值：PR ≤ 设定值
            4. ROE：≥ 设定值
            """)

    # 主显示区
    st.title("🌐 全网智能筛选")
    
    # 第一步：获取股票列表
    st.subheader("📋 第一步：获取股票列表")
    
    if st.session_state.stock_list is None:
        if st.button("🚀 获取全部A股股票列表", type="primary", use_container_width=True):
            with st.spinner("正在获取股票列表..."):
                try:
                    screener = StockScreener()
                    stock_list = screener.get_a_stock_list(exclude_st=True)
                    st.session_state.stock_list = stock_list
                    st.success(f"✅ 成功获取 {len(stock_list)} 只A股股票（已排除ST股）")
                    st.info("💡 股票列表已缓存，可以开始筛选")
                except Exception as e:
                    st.error(f"❌ 获取股票列表失败：{e}")
                    if st.session_state.debug_mode:
                        st.exception(e)
    else:
        stock_list = st.session_state.stock_list
        st.success(f"✅ 已缓存 {len(stock_list)} 只A股股票")
        
        # 显示前10只股票作为预览
        with st.expander("📊 股票列表预览（前10只）", expanded=False):
            preview_df = stock_list.head(10)[['ts_code', 'name', 'area', 'industry']]
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
        
        if st.button("🔄 重新获取股票列表", use_container_width=True):
            st.session_state.stock_list = None
            st.rerun()
    
    st.divider()
    
    # 第二步：开始筛选（如果股票列表已获取）
    if st.session_state.stock_list is not None:
        st.subheader("🔍 第二步：开始筛选")
        
        stock_list = st.session_state.stock_list
        total_stocks = len(stock_list)
        
        # 使用智能年份计算：根据当前月份和年报发布时间判断最近N年
        start_year, end_year = calculate_recent_years(required_years=years)
        
        # 显示筛选参数
        st.info(f"📊 筛选参数：年份范围={start_year}年-{end_year}年（智能计算最近{years}年数据），ROE≥{min_roe}%，PR≤{max_pr}")
        
        # 开始筛选按钮
        if not st.session_state.screening_in_progress:
            if st.button("🚀 开始全网筛选", type="primary", use_container_width=True):
                # 初始化筛选状态
                st.session_state.screening_in_progress = True
                st.session_state.screening_results = []
                start_time = datetime.now()  # 记录开始时间
                st.session_state.screening_progress = {
                    'processed': 0,
                    'total': total_stocks,
                    'passed': 0,
                    'failed': 0,
                    'current_index': 0,
                    'start_time': start_time,  # 记录开始时间
                    'last_update_time': start_time,  # 记录最后更新时间
                    'time_records': []  # 记录每只股票的耗时
                }
                st.rerun()
        else:
            # 筛选进行中，显示进度
            progress = st.session_state.screening_progress
            processed = progress['processed']
            total = progress['total']
            passed = progress['passed']
            failed = progress['failed']
            start_time = progress.get('start_time')
            if start_time is None:
                start_time = datetime.now()
                st.session_state.screening_progress['start_time'] = start_time
            
            last_update_time = progress.get('last_update_time', start_time)
            time_records = progress.get('time_records', [])
            
            # 计算时间信息
            current_time = datetime.now()
            elapsed_time = (current_time - start_time).total_seconds()  # 已用时间（秒）
            
            # 计算平均每只股票耗时
            avg_time_per_stock = 0
            if processed > 0:
                avg_time_per_stock = elapsed_time / processed
            
            # 预估剩余时间
            remaining_stocks = total - processed
            estimated_remaining_time = 0
            if processed > 0 and remaining_stocks > 0:
                estimated_remaining_time = avg_time_per_stock * remaining_stocks
            
            # 格式化时间显示
            def format_time(seconds):
                """格式化时间显示"""
                if seconds < 60:
                    return f"{int(seconds)}秒"
                elif seconds < 3600:
                    minutes = int(seconds // 60)
                    secs = int(seconds % 60)
                    return f"{minutes}分{secs}秒"
                else:
                    hours = int(seconds // 3600)
                    minutes = int((seconds % 3600) // 60)
                    secs = int(seconds % 60)
                    return f"{hours}小时{minutes}分{secs}秒"
            
            # 美化UI：时间信息卡片
            st.markdown("---")
            st.markdown("### ⏱️ 时间统计")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🕐 开始时间", start_time.strftime("%H:%M:%S"))
            with col2:
                st.metric("⏱️ 已用时间", format_time(elapsed_time))
            with col3:
                st.metric("📊 平均耗时", f"{avg_time_per_stock:.2f}秒/只" if avg_time_per_stock > 0 else "计算中...")
            with col4:
                st.metric("⏳ 预估剩余", format_time(estimated_remaining_time) if estimated_remaining_time > 0 else "计算中...")
            
            # 美化UI：进度条和进度信息（使用卡片样式）
            if total > 0:
                progress_value = processed / total
                
                # 进度卡片容器
                st.markdown("---")
                st.markdown("### 📊 筛选进度")
                
                # 使用自定义样式美化进度条区域
                progress_html = f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            padding: 20px; 
                            border-radius: 10px; 
                            margin: 10px 0;
                            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                    <div style="background-color: rgba(255, 255, 255, 0.1); 
                                border-radius: 8px; 
                                padding: 15px; 
                                margin-bottom: 15px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <span style="color: white; font-size: 16px; font-weight: bold;">总体进度</span>
                            <span style="color: white; font-size: 18px; font-weight: bold;">{progress_value*100:.2f}%</span>
                        </div>
                        <div style="background-color: rgba(0, 0, 0, 0.2); 
                                    height: 30px; 
                                    border-radius: 15px; 
                                    overflow: hidden;
                                    position: relative;">
                            <div style="background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%); 
                                        height: 100%; 
                                        width: {progress_value*100}%; 
                                        transition: width 0.3s ease;
                                        border-radius: 15px;
                                        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);">
                            </div>
                        </div>
                    </div>
                    <div style="background-color: rgba(255, 255, 255, 0.95); 
                                border-radius: 8px; 
                                padding: 15px; 
                                margin-top: 10px;">
                        <div style="font-size: 22px; 
                                    font-weight: bold; 
                                    color: #333; 
                                    text-align: center;
                                    line-height: 1.6;">
                            <span style="color: #667eea;">📊 进度：{processed}/{total} ({progress_value*100:.2f}%)</span>
                            <span style="margin: 0 10px;">|</span>
                            <span style="color: #10b981;">✅ 通过：{passed}</span>
                            <span style="margin: 0 10px;">|</span>
                            <span style="color: #ef4444;">❌ 失败：{failed}</span>
                        </div>
                    </div>
                </div>
                """
                st.markdown(progress_html, unsafe_allow_html=True)
                
                # 同时使用Streamlit原生进度条作为备用（确保兼容性）
                st.progress(progress_value)
            
            # 处理股票（每次刷新处理一只）
            screener = StockScreener()  # 使用全局导入的StockScreener
            stock_list = st.session_state.stock_list
            current_index = progress.get('current_index', 0)
            
            # 处理全部股票（不是测试模式）
            if current_index < total_stocks:
                # 还有股票需要处理
                stock_row = stock_list.iloc[current_index]
                ts_code = stock_row['ts_code']
                stock_name = stock_row['name']
                
                st.info(f"🔄 正在处理：{ts_code} ({stock_name}) [{current_index + 1}/{total_stocks}]")
                
                try:
                    # 优化：使用动态获取的年份范围（最近5年数据）
                    current_year = datetime.now().year  # 获取当前年份（如2025年）
                    analysis_end_year = current_year  # 结束年份：当前年份（如2025年），查询end_date<=20251231可获取2024年年报
                    analysis_start_year = current_year - 5  # 开始年份：当前年份-5（如2020年，最近5年）
                    
                    # 从session_state获取积分信息（避免重复调用API）
                    user_points = None
                    if 'user_points_info' in st.session_state and st.session_state.user_points_info:
                        user_points = st.session_state.user_points_info.get('total_points', 2000)
                    
                    # 调用深度分析（传递并发线程数，用于计算合适的延迟）
                    result = analyze_fundamentals(
                        ts_code=ts_code,
                        start_date=f"{analysis_start_year}0101",
                        end_date=f"{analysis_end_year}1231",
                        years=years,  # 这个参数在指定日期范围时会被忽略
                        use_cache=True,
                        api_delay=api_delay,
                        max_workers=1,  # 单线程模式（因为这里是串行处理）
                        user_points=user_points  # 传入积分信息，避免重复调用API
                    )
                    
                    if result:
                        audit_records = result.get('audit_records', [])
                        metrics = result.get('metrics')
                        
                        if metrics is not None and not metrics.empty:
                            # 检查基本面
                            fundamentals_pass, fundamentals_details = screener.check_fundamentals_pass(
                                audit_records, metrics
                            )
                            
                            if fundamentals_pass:
                                # 基本面通过，检查估值
                                valuation_pass, valuation_details = screener.check_valuation_pass(
                                    ts_code=ts_code,
                                    pr_threshold=max_pr,
                                    min_roe=min_roe
                                )
                                
                                if valuation_pass:
                                    # 通过所有筛选，添加到结果
                                    stock_result = {
                                        'ts_code': ts_code,
                                        'name': stock_name,
                                        'fundamentals_details': fundamentals_details,
                                        'valuation_details': valuation_details
                                    }
                                    st.session_state.screening_results.append(stock_result)
                                    st.session_state.screening_progress['passed'] += 1
                                else:
                                    st.session_state.screening_progress['failed'] += 1
                            else:
                                st.session_state.screening_progress['failed'] += 1
                        else:
                            st.session_state.screening_progress['failed'] += 1
                    else:
                        st.session_state.screening_progress['failed'] += 1
                    
                    # 更新进度和时间记录
                    current_time = datetime.now()
                    progress_start_time = st.session_state.screening_progress.get('start_time', current_time)
                    if current_index > 0:
                        # 计算当前股票的耗时（从上次更新到现在）
                        last_time = st.session_state.screening_progress.get('last_update_time', progress_start_time)
                        stock_time = (current_time - last_time).total_seconds()
                        time_records = st.session_state.screening_progress.get('time_records', [])
                        time_records.append(stock_time)
                        st.session_state.screening_progress['time_records'] = time_records
                    
                    st.session_state.screening_progress['processed'] = current_index + 1
                    st.session_state.screening_progress['current_index'] = current_index + 1
                    st.session_state.screening_progress['last_update_time'] = current_time
                    
                    # 继续处理下一只（自动刷新）
                    time.sleep(0.5)  # 短暂延迟，让用户看到进度
                    st.rerun()
                    
                except Exception as e:
                    # 处理失败
                    current_time = datetime.now()
                    progress_start_time = st.session_state.screening_progress.get('start_time', current_time)
                    if current_index > 0:
                        # 计算当前股票的耗时（从上次更新到现在）
                        last_time = st.session_state.screening_progress.get('last_update_time', progress_start_time)
                        stock_time = (current_time - last_time).total_seconds()
                        time_records = st.session_state.screening_progress.get('time_records', [])
                        time_records.append(stock_time)
                        st.session_state.screening_progress['time_records'] = time_records
                    
                    st.session_state.screening_progress['failed'] += 1
                    st.session_state.screening_progress['processed'] = current_index + 1
                    st.session_state.screening_progress['current_index'] = current_index + 1
                    st.session_state.screening_progress['last_update_time'] = current_time
                    if st.session_state.debug_mode:
                        st.warning(f"处理 {ts_code} 失败：{e}")
                    time.sleep(0.5)
                    st.rerun()
            else:
                # 所有股票处理完成
                st.session_state.screening_in_progress = False
                final_passed = st.session_state.screening_progress['passed']
                final_processed = st.session_state.screening_progress['processed']
                st.success(f"✅ 筛选完成！处理了 {final_processed} 只股票，通过 {final_passed} 只")
                
                # 如果有结果，按PR排序（从低到高）
                if st.session_state.screening_results:
                    st.session_state.screening_results.sort(
                        key=lambda x: x.get('valuation_details', {}).get('final_pr', float('inf'))
                    )
                    st.info(f"📊 结果已按修正市赚率（PR）从低到高排序")
            
            # 停止按钮
            if st.button("⏹️ 停止筛选", use_container_width=True):
                st.session_state.screening_in_progress = False
                st.warning("⏸️ 筛选已停止")
                st.rerun()
    
    # 结果展示区域
    if st.session_state.screening_results:
        st.divider()
        st.subheader("📊 筛选结果")
        st.info(f"✅ 共找到 {len(st.session_state.screening_results)} 只符合条件的股票（已按PR从低到高排序）")
        
        # 构建表格数据
        table_data = []
        for i, stock in enumerate(st.session_state.screening_results, 1):
            valuation = stock.get('valuation_details', {})
            fundamentals = stock.get('fundamentals_details', {})
            
            # 获取PR值（修正PR优先，如果没有则用标准PR）
            final_pr = valuation.get('final_pr')
            if final_pr is None:
                final_pr = valuation.get('standard_pr')
            
            table_data.append({
                '排名': i,
                '代码': stock['ts_code'],
                '名称': stock.get('name', '未知'),
                'PR': f"{final_pr:.4f}" if final_pr is not None else "-",
                'ROE(%)': f"{valuation.get('roe_waa', 0):.2f}" if valuation.get('roe_waa') is not None else "-",
                'PE(TTM)': f"{valuation.get('pe_ttm', 0):.2f}" if valuation.get('pe_ttm') is not None else "-",
                '审计通过': "✅" if fundamentals.get('audit_pass', False) else "❌",
                '现金流通过': "✅" if fundamentals.get('cashflow_pass', False) else "❌",
            })
        
        df_results = pd.DataFrame(table_data)
        st.dataframe(df_results, use_container_width=True, hide_index=True)
        
        # 导出按钮
        csv = df_results.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 导出CSV",
            data=csv,
            file_name=f"全网筛选结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime='text/csv'
        )

def page_full_market_screening_old():
    """旧版本（已废弃，保留作为参考）"""
    pass

def page_history():
    """历史记录页面"""
    st.header("🕘 历史记录")

    history = HistoryManager.load_history()

    if not history:
        st.info("📭 暂无历史记录")
        return

    for record in history[:50]:
        time = record.get('time', '')
        code = record.get('code', '')
        sector = record.get('sector', '')
        avg_score = record.get('avg_score', 0)
        icon = "🟢" if avg_score >= 2.5 else "🟡" if avg_score >= 2.0 else "🟠"

        with st.expander(f"{icon} {time} - {code} ({sector}) - 得分: {avg_score:.2f}", expanded=False):
            col1, col2, col3 = st.columns(3)
            col1.write(f"**代码：** {code}")
            col2.write(f"**行业：** {sector}")
            col3.write(f"**年限：** {record.get('year_range', '未知')}")

            col1, col2 = st.columns(2)
            col1.write(f"**平均分：** {avg_score:.2f}/3.00")
            col2.write(f"**红旗数：** {record.get('red_flags', 0)}年")
    
    if st.button("🗑️ 清空历史记录"):
        HistoryManager.save_history([])
        st.success("✅ 历史记录已清空")
        st.rerun()


def main():
    """主函数"""
    # ========== 积分信息获取（页面置顶，只调用一次，缓存到session_state） ==========
    # 使用持久化缓存避免频繁查询（积分信息变化不频繁，每天最多50次查询限制）
    # 缓存键：使用固定键名，确保所有会话共享同一缓存
    cache_key = 'user_points_info_daily'
    
    # 初始化session_state中的积分信息（如果不存在）
    if 'user_points_info' not in st.session_state:
        st.session_state.user_points_info = None
    
    # 先检查session_state中是否已有积分信息（避免重复调用）
    points_info = st.session_state.user_points_info
    
    # 如果session_state中没有，则从持久化缓存读取
    if points_info is None:
        try:
            points_info = data_cache.get(cache_key)
            # 如果从缓存读取成功，保存到session_state
            if points_info:
                st.session_state.user_points_info = points_info
        except Exception as cache_read_error:
            # 缓存读取失败（可能是JSON格式错误），尝试修复
            if st.session_state.debug_mode:
                st.warning(f"⚠️ 缓存读取失败，尝试修复：{cache_read_error}")
            cache_path = data_cache.get_cache_file_path(cache_key)
            if os.path.exists(cache_path):
                try:
                    # 尝试直接读取缓存文件（不检查过期时间）
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    points_info = cache_data.get('data')
                    if points_info:
                        cache_time = cache_data.get('datetime', '未知')
                        if st.session_state.debug_mode:
                            st.info(f"✅ 从损坏的缓存文件中恢复数据（缓存时间：{cache_time}）")
                        # 保存到session_state
                        st.session_state.user_points_info = points_info
                except Exception as repair_error:
                    # 缓存文件完全损坏，删除它
                    if st.session_state.debug_mode:
                        st.warning(f"⚠️ 缓存文件损坏，删除并重新获取：{repair_error}")
                    try:
                        os.remove(cache_path)
                    except:
                        pass
                    points_info = None
    
    # 如果仍然没有积分信息，调用API（只调用一次）
    if points_info is None:
        # 缓存不存在或已过期，需要调用API
        try:
            if st.session_state.debug_mode:
                st.info("🔄 正在获取积分信息（每天最多调用一次）...")
            points_info = get_user_points_info()
            
            # 保存到session_state和持久化缓存（24小时有效）
            if points_info:
                st.session_state.user_points_info = points_info  # 保存到session_state
                
                # 使用临时文件写入，成功后再替换（避免并发写入问题）
                cache_path = data_cache.get_cache_file_path(cache_key)
                try:
                    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                    cache_data = {
                        'data': points_info,
                        'timestamp': time.time(),
                        'datetime': datetime.now().isoformat()
                    }
                    temp_path = cache_path + '.tmp'
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        json.dump(cache_data, f, ensure_ascii=False, indent=2)
                    os.replace(temp_path, cache_path)
                    if st.session_state.debug_mode:
                        st.success("✅ 积分信息已缓存，24小时内不再调用API")
                except Exception as cache_save_error:
                    if st.session_state.debug_mode:
                        st.warning(f"⚠️ 积分信息缓存保存失败：{cache_save_error}")
        except Exception as e:
            # 如果查询失败，尝试使用过期缓存（如果有）
            cache_path = data_cache.get_cache_file_path(cache_key)
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    points_info = cache_data.get('data')
                    if points_info:
                        cache_time = cache_data.get('datetime', '未知')
                        if st.session_state.debug_mode:
                            st.warning(f"⚠️ 获取积分信息失败，使用过期缓存（缓存时间：{cache_time}）: {e}")
                        # 保存到session_state
                        st.session_state.user_points_info = points_info
                except Exception as read_error:
                    if st.session_state.debug_mode:
                        st.error(f"❌ 无法获取积分信息且读取缓存失败: {e}, {read_error}")
            else:
                if st.session_state.debug_mode:
                    st.error(f"❌ 无法获取积分信息且无缓存: {e}")
    else:
        # 使用缓存数据（从session_state或持久化缓存）
        if st.session_state.debug_mode:
            # 显示缓存信息
            cache_path = data_cache.get_cache_file_path(cache_key)
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    cache_time = cache_data.get('datetime', '未知')
                    st.caption(f"💾 使用缓存的积分信息（缓存时间：{cache_time}）")
                except:
                    pass
    
    # 显示积分信息（在页面最顶部）
    if points_info:
        # 显示总积分和到期信息（简洁版，在页面顶部）
        st.markdown("### 💰 Tushare积分信息")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总积分", f"{points_info['total_points']:.0f}")
        
        with col2:
            if points_info.get('nearest_expiry_date'):
                # 计算距离到期的天数
                expiry_date = datetime.strptime(points_info['nearest_expiry_date'], '%Y-%m-%d')
                days_left = (expiry_date - datetime.now()).days
                st.metric(
                    "最近到期时间",
                    points_info['nearest_expiry_date'],
                    delta=f"{days_left}天后到期" if days_left > 0 else "已过期" if days_left < 0 else "今日到期"
                )
            else:
                st.metric("最近到期时间", "未知")
        
        with col3:
            if points_info.get('nearest_expiry_date'):
                st.metric("到期积分", f"{points_info['nearest_expiry_points']:.0f}")
            else:
                st.metric("到期积分", "0")
        
        # 展开显示详细到期记录表格（按照第三张图片的格式）
        with st.expander("📋 查看详细到期记录", expanded=False):
            if points_info.get('expiry_records'):
                st.caption("💡 账户的总积分，用户可以登录tushare pro，在个人主页里可以查看到")
                
                # 创建到期记录表格
                expiry_df = pd.DataFrame(points_info['expiry_records'])
                
                # 确保列名正确（Tushare API返回的字段名）
                required_cols = []
                if '到期时间' in expiry_df.columns:
                    required_cols.append('到期时间')
                elif 'expiry_date' in expiry_df.columns:
                    expiry_df['到期时间'] = expiry_df['expiry_date']
                    required_cols.append('到期时间')
                
                if '到期积分' in expiry_df.columns:
                    required_cols.append('到期积分')
                elif 'expiry_points' in expiry_df.columns:
                    expiry_df['到期积分'] = expiry_df['expiry_points']
                    required_cols.append('到期积分')
                
                if len(required_cols) == 2:
                    # 格式化日期显示
                    expiry_df['到期时间'] = pd.to_datetime(expiry_df['到期时间']).dt.strftime('%Y-%m-%d')
                    expiry_df['到期积分'] = expiry_df['到期积分'].apply(lambda x: f"{float(x):.4f}" if pd.notna(x) else "0.0000")
                    
                    # 只显示需要的列
                    display_df = expiry_df[['到期时间', '到期积分']].copy()
                    
                    # 按到期时间排序（最近的在前）
                    display_df = display_df.sort_values('到期时间').reset_index(drop=True)
                    
                    # 显示表格（按照第三张图片的格式）
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "到期时间": st.column_config.TextColumn("到期时间", width="medium"),
                            "到期积分": st.column_config.TextColumn("到期积分", width="medium")
                        }
                    )
                else:
                    st.warning(f"⚠️ 积分数据格式异常，缺少必需字段。可用字段：{list(expiry_df.columns)}")
            else:
                st.caption("暂无积分到期记录")
        
        st.divider()
    else:
        # 查询失败，显示提示
        if st.session_state.debug_mode:
            st.warning("⚠️ 无法获取积分信息（可能达到API调用限制，每天最多50次）")
    
    st.title("📊 A股财务分析系统")
    st.markdown("*基于审计意见与三大核心指标的智能筛选*")
    
    # 侧边栏 - 系统配置
    with st.sidebar:
        st.header("⚙️ 系统配置")
        
        # 行业选择
        st.markdown("### 🏭 行业分类")
        st.session_state.selected_sector = st.selectbox(
            "选择行业板块",
            options=list(SECTOR_RULES.keys()),
            index=list(SECTOR_RULES.keys()).index(st.session_state.selected_sector),
            help="不同行业有不同的财务健康标准"
        )
        
        sector_info = SECTOR_RULES[st.session_state.selected_sector]
        st.info(f"""
**{sector_info['name']}行业标准：**
- 资产负债率 ≤ {sector_info['debt_ratio_max']}%
- 毛利率 ≥ {sector_info['gross_margin_min']}%
- 经营现金流连续≥{st.session_state.ocf_consecutive_years}年为正

💡 {sector_info['description']}
        """)
        
        st.divider()
        
        # 分析参数
        st.markdown("### 📊 分析参数")
        col1, col2 = st.columns(2)
        with col1:
            new_start_year = st.number_input(
                "开始年",
                min_value=1900,
                max_value=2999,
                value=st.session_state.start_year,
                step=1,
                help="可自由输入任何年份（1900-2999）",
                key="start_year_input"
            )
            # 如果年份发生变化，更新session_state
            if new_start_year != st.session_state.start_year:
                st.session_state.start_year = new_start_year
                st.info(f"✅ 开始年已更新为：{new_start_year}")
        with col2:
            new_end_year = st.number_input(
                "结束年",
                min_value=1900,
                max_value=2999,
                value=st.session_state.end_year,
                step=1,
                help="可自由输入任何年份（1900-2999）",
                key="end_year_input"
            )
            # 如果年份发生变化，更新session_state
            if new_end_year != st.session_state.end_year:
                st.session_state.end_year = new_end_year
                st.info(f"✅ 结束年已更新为：{new_end_year}")
        
        # 显示当前设置的年份范围
        st.caption(f"📅 当前查询年份范围：{st.session_state.start_year}年 - {st.session_state.end_year}年")
        
        st.session_state.ocf_consecutive_years = st.slider(
            "OCF连续为正年数要求",
            min_value=3,
            max_value=5,
            value=st.session_state.ocf_consecutive_years,
            step=1,
            help="经营现金流需要连续多少年为正"
        )
        
        st.divider()
        
        # API设置
        st.markdown("### 🔌 API设置")
        
        st.markdown("**根据您的积分等级选择延迟：**")
        
        delay_options = {
            "未认证用户 (0-119分)": 31,
            "注册用户 (120-599分)": 13,
            "中级用户 (600-4999分)": 0.1,
            "高级用户 (5000+分)": 0
        }
        
        selected_level = st.selectbox(
            "选择您的Tushare等级",
            options=list(delay_options.keys()),
            index=2,  # 默认选择"中级用户"（2000积分对应）
            help="根据您的积分选择对应等级，系统会自动设置延迟时间"
        )
        
        st.session_state.api_delay = delay_options[selected_level]
        
        if st.session_state.api_delay > 0:
            total_time = st.session_state.api_delay * 4
            st.success(f"""
**✅ 中级用户配置（2000+积分）：**
- 延迟时间：{st.session_state.api_delay}秒/次
- 单次查询耗时：约**{total_time}秒**（5次API调用）
- 每分钟可查询：约4个新股票
- 说明：每次API调用后等待{st.session_state.api_delay}秒

💡 使用缓存后，查询速度<1秒！
            """)
        else:
            st.success("✅ 高级用户无需延迟，查询速度最快！")
        
        st.caption("🔗 查看积分：https://tushare.pro/user/token")
        
        st.divider()
        
        # 缓存管理
        st.markdown("### 💾 缓存管理")
        
        # 获取缓存信息
        cache_info = data_cache.get_cache_info()
        
        st.write(f"**缓存统计：**")
        # 使用单列显示，避免侧边栏宽度不足导致文字截断
        st.metric("有效缓存", f"{cache_info['valid']} 个")
        st.metric("缓存大小", f"{cache_info['size_mb']} MB")
        st.metric("过期缓存", f"{cache_info['expired']} 个")
        st.caption(f"有效期: {cache_info['expire_hours']:.0f} 小时")
        
        # 按类型分类显示（如果有分类数据）
        if 'by_type' in cache_info:
            st.markdown("---")
            st.write("**📊 按类型分类：**")
            type_info = cache_info['by_type']
            
            financial = type_info.get('financial', {})
            company = type_info.get('company', {})
            user = type_info.get('user', {})
            
            st.text(f"财务数据: {financial.get('valid', 0)}/{financial.get('total', 0)}")
            st.text(f"公司信息: {company.get('valid', 0)}/{company.get('total', 0)}")
            st.text(f"用户积分: {user.get('valid', 0)}/{user.get('total', 0)}")
        
        st.caption("💡 缓存会自动保存到文件，关闭浏览器后依然有效")
        
        # 缓存操作按钮
        col1, col2 = st.columns(2)
        if col1.button("🧹 清理过期", use_container_width=True):
            count = data_cache.clear_expired()
            st.success(f"已清理 {count} 个")
            st.rerun()
        
        if col2.button("🗑️ 清空全部", use_container_width=True):
            count = data_cache.clear_all()
            st.success(f"已清空 {count} 个")
            st.rerun()
        
        st.divider()
        
        # 调试模式
        st.session_state.debug_mode = st.checkbox(
            "🔧 调试模式",
            value=st.session_state.debug_mode,
            help="开启后显示详细调试信息和缓存统计"
        )
    
    # 主内容区 - 标签页
    tab1, tab2, tab3, tab4 = st.tabs(["🔎 单项分析", "💰 市赚率估值", "🌐 全网筛选", "🕘 历史记录"])

    with tab1:
        page_single_analysis()

    with tab2:
        page_pr_valuation()

    with tab3:
        page_full_market_screening()

    with tab4:
        page_history()


if __name__ == "__main__":
    main()
