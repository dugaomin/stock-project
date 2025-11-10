# -*- coding: utf-8 -*-
"""A股财务综合分析系统 - 基于审计意见与三大核心指标"""

import pandas as pd
import streamlit as st
from datetime import datetime
from utils import analyze_fundamentals, run_connectivity_tests
import json
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cache_manager import data_cache

# 页面配置
st.set_page_config(
    page_title="A股财务综合分析系统",
    page_icon="📊",
    layout="wide",
)

# 数据文件路径
HISTORY_FILE = "data/analysis_history.json"
os.makedirs("data", exist_ok=True)

# 行业评分标准
SECTOR_RULES = {
    "地产": {
        "name": "地产",
        "debt_ratio_max": 60.0,
        "gross_margin_min": 15.0,
        "description": "地产行业资产负债率<60%较健康"
    },
    "科技": {
        "name": "科技",
        "debt_ratio_max": 50.0,
        "gross_margin_min": 30.0,
        "description": "科技行业资产负债率>50%需警惕"
    },
    "消费": {
        "name": "消费",
        "debt_ratio_max": 40.0,
        "gross_margin_min": 40.0,
        "description": "消费行业越低越安全，毛利率<40%需警惕"
    },
    "制造业": {
        "name": "制造业",
        "debt_ratio_max": 60.0,
        "gross_margin_min": 25.0,
        "description": "制造业毛利率25%就可能很优秀"
    },
    "品牌/平台": {
        "name": "品牌/平台",
        "debt_ratio_max": 40.0,
        "gross_margin_min": 60.0,
        "description": "品牌溢价强，通常毛利率更高（60%+）"
    },
    "金融": {
        "name": "金融",
        "debt_ratio_max": 90.0,
        "gross_margin_min": 20.0,
        "description": "金融行业特殊，负债率高属正常"
    },
    "其他": {
        "name": "其他",
        "debt_ratio_max": 60.0,
        "gross_margin_min": 15.0,
        "description": "通用标准"
    }
}

# 初始化会话状态
if 'debug_mode' not in st.session_state:
    st.session_state.debug_mode = False
if 'start_year' not in st.session_state:
    st.session_state.start_year = 2018
if 'end_year' not in st.session_state:
    st.session_state.end_year = 2023  # 当前最新完整年份
if 'selected_sector' not in st.session_state:
    st.session_state.selected_sector = "消费"
if 'ocf_consecutive_years' not in st.session_state:
    st.session_state.ocf_consecutive_years = 3
if 'api_delay' not in st.session_state:
    st.session_state.api_delay = 0.1  # 默认0.1秒，适配中级用户（2000积分）


class HistoryManager:
    """历史记录管理"""
    
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
    """格式化百分比"""
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.2f}%"


def format_number(value: float, unit="万元") -> str:
    """格式化数字"""
    if value is None or pd.isna(value):
        return "-"
    if unit == "亿元":
        return f"{value/100000000:,.2f}亿元"
    return f"{value/10000:,.2f}万元"


def evaluate_year(row, sector_rules) -> dict:
    """评估单年财务指标"""
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
    """检查经营现金流连续性"""
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
    
    st.caption(f"共分析 {len(metrics)} 个年度数据（最新年份在上方）")
    
    # 按年份倒序显示（最新的在上面）
    # metrics 已经是按 end_date 降序排列的，所以直接正序遍历即可
    for idx in range(len(metrics)):
        row = metrics.iloc[idx]
        year = row['end_date'][:4]
        checks = evaluation['year_checks'][idx]
        score = evaluation['scores'][idx]
        
        # 创建一个容器
        with st.container():
            # 年份和得分
            col_year, col_score = st.columns([1, 3])
            with col_year:
                st.markdown(f"### {year}")
            with col_score:
                score_dots = "🟢" * score + "⚪" * (3 - score)
                st.markdown(f"**年度得分：** {score}/3 {score_dots}")
            
            # 三个指标横向排列
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
    years = [row['end_date'][:4] for _, row in metrics.iterrows()]
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
    fig.update_xaxes(title_text="年份", row=1, col=1)
    fig.update_xaxes(title_text="年份", row=1, col=2)
    fig.update_xaxes(title_text="年份", row=2, col=1)
    fig.update_xaxes(title_text="年份", row=2, col=2)
    
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
    """单项分析页面"""
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
            result = analyze_fundamentals(
                ts_code, start_date, end_date, 
                use_cache=True,
                api_delay=st.session_state.api_delay,
                progress_callback=update_progress
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
            st.session_state.start_year = st.number_input(
                "开始年",
                min_value=1990,
                max_value=2100,
                value=st.session_state.start_year,
                step=1,
                help="可自由输入任何年份"
            )
        with col2:
            st.session_state.end_year = st.number_input(
                "结束年",
                min_value=1990,
                max_value=2100,
                value=st.session_state.end_year,
                step=1,
                help="可自由输入任何年份"
            )
        
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
        col1, col2 = st.columns(2)
        col1.metric("有效缓存", f"{cache_info['valid']} 个")
        col2.metric("过期缓存", f"{cache_info['expired']} 个")
        
        col1, col2 = st.columns(2)
        col1.metric("缓存大小", f"{cache_info['size_mb']} MB")
        col2.metric("有效期", f"{cache_info['expire_hours']:.0f} 小时")
        
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
    tab1, tab2 = st.tabs(["🔎 单项分析", "🕘 历史记录"])
    
    with tab1:
        page_single_analysis()
    
    with tab2:
        page_history()


if __name__ == "__main__":
    main()
