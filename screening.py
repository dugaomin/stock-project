# -*- coding: utf-8 -*-
"""
A股全网筛选系统核心模块

功能概述：
    基于财务分析和估值模型，实现对全A股市场的批量扫描与筛选。
    利用现有的单股分析、估值计算和缓存功能，系统性地评估每只股票，
    并按投资价值（修正市赚率PR）排序输出筛选结果。

核心业务流程：
    1. 获取全部A股股票列表（排除ST股）
    2. 遍历列表，对每只股票执行深度分析
    3. 应用基本面和估值的双重筛选规则
    4. 收集所有通过筛选的股票
    5. 按修正市赚率从低到高进行排序
    6. 输出最终结果列表

筛选规则：
    第一层：基本面判断
        - 审计意见：近5年审计结论必须全部为"标准无保留意见"
        - 现金流质量：
            * 经营活动现金流≥0
            * 收到的现金≥账面利润（利润转化为真实现金流入）

    第二层：巴菲特估值判断
        - 市赚率计算：使用修正市赚率（NPR）
        - 估值阈值：PR ≤ 用户设定的上限（默认1.0）

作者：gaomindu
版本：1.0.0
更新：2025-11-13
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import (
    get_pro_client, analyze_fundamentals, fetch_valuation_data,
    AuditRecord, run_connectivity_tests
)
from valuation import PRValuation
from cache_manager import data_cache


class StockScreener:
    """A股全网筛选器"""

    def __init__(self):
        self.pro = get_pro_client()
        self.screening_cache = {}  # 内存缓存，避免重复计算

    def get_a_stock_list(self, exclude_st: bool = True) -> pd.DataFrame:
        """
        获取全部A股股票列表

        参数:
            exclude_st: 是否排除ST股

        返回:
            包含股票基本信息的DataFrame
        """
        try:
            # 获取所有正常上市交易的股票
            df = self.pro.stock_basic(
                exchange='',  # 空字符串表示所有交易所
                list_status='L',  # L=上市
                fields='ts_code,symbol,name,area,industry,list_date'
            )

            if df.empty:
                raise ValueError("未获取到A股股票列表")

            # 排除ST股（如果需要）
            if exclude_st:
                # ST股通常在股票名称中包含"ST"
                st_mask = df['name'].str.contains('ST', na=False)
                df = df[~st_mask]
                print(f"✅ 排除 {st_mask.sum()} 只ST股，剩余 {len(df)} 只股票")

            # 按市值或行业排序（可选）
            df = df.sort_values('ts_code').reset_index(drop=True)

            print(f"✅ 获取到 {len(df)} 只A股股票")
            return df

        except Exception as e:
            raise ValueError(f"获取A股股票列表失败: {e}")
    
    def check_cache_exists(self, ts_code: str, start_year: int, end_year: int) -> bool:
        """
        快速检查指定股票的缓存是否存在
        
        参数:
            ts_code: 股票代码
            start_year: 开始年份
            end_year: 结束年份
            
        返回:
            True: 缓存存在且有效, False: 缓存不存在或已过期
        """
        # 计算缓存键(与 analyze_fundamentals 中的逻辑一致)
        start_date = f"{start_year}0101"
        end_date = f"{end_year}1231"
        required_years = end_year - start_year + 1
        cache_key = f"{ts_code}_{start_date}_{end_date}_{required_years}"
        
        # 使用 data_cache 检查缓存是否存在
        try:
            cached_data = data_cache.get(cache_key)
            if cached_data is not None:
                # 缓存存在且未过期
                return True
        except:
            pass
        
        return False

    def check_fundamentals_pass(self,
                               audit_records: List[AuditRecord],
                               metrics: pd.DataFrame,
                               required_years: int = 5) -> Tuple[bool, Dict]:
        """
        检查基本面筛选条件

        参数:
            audit_records: 审计记录列表
            metrics: 财务指标DataFrame
            required_years: 要求的年份数（数据必须覆盖这么多年的跨度）

        返回:
            (是否通过, 检查结果详情)
        """
        results = {
            'audit_pass': False,
            'cashflow_pass': False,
            'cashflow_ge_profit': False,
            'data_sufficiency_pass': False,  # 新增：数据完整性检查
            'audit_details': [],
            'cashflow_details': {}
        }

        # 0. 数据完整性检查
        # 检查metrics中的年份跨度是否满足要求
        if not metrics.empty:
            # metrics按end_date降序排列
            years_found = len(metrics)
            # 只要数据行数 >= required_years，就认为满足年份要求
            # 注意：这里假设metrics已经过滤了非年度数据，且没有重复年份
            if years_found >= required_years:
                results['data_sufficiency_pass'] = True
            else:
                results['data_sufficiency_pass'] = False
                results['data_sufficiency_msg'] = f"数据不足：需要{required_years}年，实际只有{years_found}年"
        else:
            results['data_sufficiency_pass'] = False
            results['data_sufficiency_msg'] = "无财务数据"

        # 如果数据不足，直接返回不通过
        if not results['data_sufficiency_pass']:
            return False, results

        # 1. 审计意见检查
        if audit_records:
            # 检查最近N年的审计意见
            recent_audits = audit_records[:required_years]  # 取最新的N条记录
            all_standard = all(record.is_standard for record in recent_audits)

            results['audit_pass'] = all_standard
            results['audit_details'] = [
                {
                    'year': record.end_date[:4],
                    'result': record.audit_result,
                    'is_standard': record.is_standard
                }
                for record in recent_audits
            ]
        else:
            results['audit_details'] = "无审计记录"
            # 如果没有审计记录，视为不通过（严格模式）
            results['audit_pass'] = False

        # 2. 现金流质量检查（近N年经营现金流≥0 且 现金流≥净利润）
        if not metrics.empty:
            # 取前N年
            recent_years = metrics.head(required_years)
            
            # 检查近N年经营现金流是否全部≥0
            all_positive = recent_years['cashflow_positive'].all() if len(recent_years) > 0 else False
            results['cashflow_pass'] = all_positive
            
            # 检查近N年现金流是否覆盖净利润
            all_cover_profit = recent_years['cashflow_ge_profit'].all() if len(recent_years) > 0 else False
            results['cashflow_ge_profit'] = all_cover_profit
            
            # 记录现金流详情（用于展示）
            results['cashflow_details'] = {
                'years_checked': len(recent_years),
                'all_positive': all_positive,
                'all_cover_profit': all_cover_profit,
                'yearly_cashflow': [
                    {
                        'year': row['end_date'][:4],
                        'ocf': row.get('n_cashflow_act', 0),
                        'net_income': row.get('n_income', 0),
                        'positive': row.get('cashflow_positive', False),
                        'cover_profit': row.get('cashflow_ge_profit', False)
                    }
                    for _, row in recent_years.iterrows()
                ]
            }
        else:
            results['cashflow_pass'] = False
            results['cashflow_ge_profit'] = False
            results['cashflow_details'] = {'error': '无财务数据'}

        # 通过条件：数据完整 且 审计意见通过 且 现金流≥0 且 现金流覆盖净利润
        return (results['data_sufficiency_pass'] and 
                results['audit_pass'] and 
                results['cashflow_pass'] and 
                results['cashflow_ge_profit']), results

    def check_valuation_pass(self,
                           ts_code: str,
                           pr_threshold: float = 1.0,
                           min_roe: float = 0.0) -> Tuple[bool, Dict]:
        """
        检查估值筛选条件
        
        ⚠️ 重要：此函数必须使用最新的价格数据！
        - 每次调用都会获取最新交易日的收盘价和PE
        - 价格数据不会使用缓存（因为每天变化）
        - 确保筛选结果反映当前市场估值

        参数:
            ts_code: 股票代码
            pr_threshold: 市赚率阈值
            min_roe: 最低ROE要求(%)

        返回:
            (是否通过, 估值结果详情)
        """
        try:
            # 获取最新交易日的估值数据
            # ⚠️ 注意：fetch_valuation_data 不使用缓存，每次都获取最新价格和PE
            # 这是正确的，因为价格每天变化，必须使用最新数据计算PR
            today = datetime.now().strftime("%Y%m%d")

            valuation_data = fetch_valuation_data(ts_code, today, "个股")

            if valuation_data is None:
                return False, {'error': '无法获取估值数据'}

            # 计算修正市赚率（按照市赚率估值分析模块的方式）
            # 使用PRValuation.analyze_stock_valuation()会自动计算：
            # 1. 股息支付率
            # 2. 修正系数N
            # 3. 修正市赚率 = N × PE / ROE / 150
            result = PRValuation.analyze_stock_valuation(valuation_data)

            if result['corrected_pr'] is None and result['standard_pr'] is None:
                return False, {'error': '无法计算市赚率'}

            # 使用修正PR，如果没有则使用标准PR
            final_pr = result['corrected_pr'] if result['corrected_pr'] is not None else result['standard_pr']

            # 获取ROE值
            roe_waa = result.get('roe_waa')
            if roe_waa is None:
                roe_waa = 0.0

            # 检查是否通过估值阈值（直接复用市赚率估值分析，PR值必须≤pr_threshold，默认1.0）
            pr_pass = final_pr <= pr_threshold if final_pr is not None else False
            
            # ROE要求（如果用户设置了min_roe > 0，则检查ROE；否则不检查ROE）
            roe_pass = True
            if min_roe > 0:
                roe_pass = roe_waa >= min_roe if roe_waa is not None else False
            
            valuation_pass = pr_pass and roe_pass

            valuation_details = {
                'pe_ttm': result['pe_ttm'],
                'roe_waa': roe_waa,
                'eps': result['eps'],
                'dividend_per_share': result['dividend_per_share'],
                'payout_ratio': result['payout_ratio'],
                'correction_factor': result['correction_factor'],
                'standard_pr': result['standard_pr'],
                'corrected_pr': result['corrected_pr'],
                'final_pr': final_pr,
                'pr_threshold': pr_threshold,
                'min_roe': min_roe,
                'pr_pass': pr_pass,
                'roe_pass': roe_pass,
                'valuation_pass': valuation_pass
            }

            return valuation_pass, valuation_details

        except Exception as e:
            return False, {'error': str(e)}

    def analyze_single_stock(self,
                           ts_code: str,
                           pr_threshold: float = 1.0,
                           min_roe: float = 0.0,
                           start_year: int = None,  # 优化：动态获取，默认2000年
                           end_year: int = None,  # 优化：动态获取，默认当前年份
                           api_delay: float = 0.0,  # 额外延迟（在API规则延迟基础上增加）
                           max_workers: int = 1,  # 并发线程数（用于计算合适的延迟）
                           debug_callback=None,
                           user_points: Optional[float] = None) -> Optional[Dict]:  # 用户积分（可选，避免重复调用API）
        """
        分析单只股票是否通过筛选

        参数:
            ts_code: 股票代码
            pr_threshold: 市赚率阈值
            min_roe: 最低ROE要求(%)
            start_year: 开始年份（如果为None，则使用2000年）
            end_year: 结束年份（如果为None，则使用当前年份）
            api_delay: API调用延迟

        返回:
            筛选结果字典，如果分析失败返回None
        """
        try:
            # 优化：在执行分析前动态获取当前年份，使用最近5年数据
            current_year = datetime.now().year  # 获取当前年份（如2025年）
            if end_year is None:
                end_year = current_year  # 结束年份：当前年份（如2025年），查询end_date<=20251231可获取2024年年报
            if start_year is None:
                start_year = current_year - 5  # 开始年份：当前年份-5（如2020年，最近5年）
            
            # 计算要求的年份跨度
            required_years = end_year - start_year + 1
            
            # 检查内存缓存
            cache_key = f"{ts_code}_{pr_threshold}_{min_roe}_{start_year}_{end_year}"
            if cache_key in self.screening_cache:
                if debug_callback:
                    debug_callback(f"🔍 {ts_code} 使用内存缓存", 'debug')
                return self.screening_cache[cache_key]

            if debug_callback:
                debug_callback(f"🔍 开始分析 {ts_code}...", 'debug')
            
            # 打印到控制台，确认任务在执行
            print(f"[ANALYZE {datetime.now().strftime('%H:%M:%S')}] 开始分析股票: {ts_code} (年份范围: {start_year}-{end_year}, 需{required_years}年数据)")

            # 执行财务分析
            start_date = f"{start_year}0101"
            end_date = f"{end_year}1231"

            if debug_callback:
                debug_callback(f"📊 {ts_code} 获取财务数据 ({start_year}-{end_year})...", 'debug')

            analysis_result = analyze_fundamentals(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                years=required_years,  # 传入所需的年数
                use_cache=True,
                api_delay=api_delay,
                max_workers=max_workers,  # 传递并发线程数，用于计算合适的延迟
                user_points=user_points  # 传入积分信息，避免重复调用API
            )

            audit_records = analysis_result.get('audit_records', [])
            metrics = analysis_result.get('metrics', pd.DataFrame())

            if debug_callback:
                debug_callback(f"📋 {ts_code} 获取到 {len(audit_records)} 条审计记录，{len(metrics)} 年财务数据", 'debug')

            # 基本面检查
            if debug_callback:
                debug_callback(f"🔍 {ts_code} 检查基本面条件...", 'debug')
            
            fundamentals_pass, fundamentals_details = self.check_fundamentals_pass(
                audit_records, metrics, required_years
            )

            if debug_callback:
                audit_pass = fundamentals_details.get('audit_pass', False)
                cashflow_pass = fundamentals_details.get('cashflow_pass', False)
                cashflow_ge = fundamentals_details.get('cashflow_ge_profit', False)
                debug_callback(
                    f"📊 {ts_code} 基本面检查: 审计意见={'✅' if audit_pass else '❌'}, "
                    f"现金流≥0={'✅' if cashflow_pass else '❌'}, "
                    f"现金流≥利润={'✅' if cashflow_ge else '❌'}",
                    'debug'
                )

            # 估值检查
            # ⚠️ 重要：估值检查必须使用最新价格数据，不使用缓存
            # check_valuation_pass() 内部会调用 fetch_valuation_data() 获取最新收盘价和PE
            # 这确保了筛选结果反映当前市场估值，而不是过时的价格数据
            if debug_callback:
                debug_callback(f"💰 {ts_code} 检查估值条件 (PR≤{pr_threshold}, ROE≥{min_roe}%)...", 'debug')
            
            valuation_pass, valuation_details = self.check_valuation_pass(
                ts_code, pr_threshold, min_roe
            )

            if debug_callback:
                pr = valuation_details.get('final_pr', 'N/A')
                roe = valuation_details.get('roe_waa', 'N/A')
                pr_pass = valuation_details.get('pr_pass', False)
                roe_pass = valuation_details.get('roe_pass', False)
                debug_callback(
                    f"💰 {ts_code} 估值检查: PR={pr:.4f if isinstance(pr, (int, float)) else pr} {'✅' if pr_pass else '❌'}, "
                    f"ROE={roe:.2f if isinstance(roe, (int, float)) else roe}% {'✅' if roe_pass else '❌'}",
                    'debug'
                )

            # 综合判断
            overall_pass = fundamentals_pass and valuation_pass

            if debug_callback:
                debug_callback(
                    f"{'✅' if overall_pass else '❌'} {ts_code} 综合判断: {'通过筛选' if overall_pass else '未通过筛选'}",
                    'info' if overall_pass else 'warning'
                )

            result = {
                'ts_code': ts_code,
                'overall_pass': overall_pass,
                'fundamentals_pass': fundamentals_pass,
                'valuation_pass': valuation_pass,
                'fundamentals_details': fundamentals_details,
                'valuation_details': valuation_details,
                'analysis_result': analysis_result,
                'timestamp': datetime.now().isoformat()
            }

            # 保存到内存缓存
            self.screening_cache[cache_key] = result

            return result

        except Exception as e:
            error_msg = str(e)
            if debug_callback:
                debug_callback(f"❌ {ts_code} 分析异常: {error_msg}", 'error')
            print(f"分析股票 {ts_code} 失败: {e}")
            return None

    def screen_all_stocks(self,
                         pr_threshold: float = 1.0,
                         min_roe: float = 10.0,  # 优化：默认10.0%（保守型ROE筛选）
                         start_year: int = None,  # 优化：动态获取，默认2000年
                         end_year: int = None,  # 优化：动态获取，默认当前年份
                         max_workers: int = 10,  # 优化：默认10线程，加速筛选
                         api_delay: float = 0.1,  # 优化：默认0.1秒，加速筛选
                         progress_callback=None,
                         debug_callback=None,
                         user_points: Optional[float] = None) -> List[Dict]:  # 用户积分（可选，避免重复调用API）
        """
        全网筛选主函数

        参数:
            pr_threshold: 市赚率阈值
            min_roe: 最低ROE要求(%)
            start_year: 开始年份（如果为None，则使用2000年）
            end_year: 结束年份（如果为None，则使用当前年份）
            max_workers: 最大并发数
            api_delay: API调用延迟
            progress_callback: 进度回调函数

        返回:
            通过筛选的股票列表
        """
        # 优化：在执行筛选前动态获取当前年份，使用最近5年数据
        current_year = datetime.now().year  # 获取当前年份（如2025年）
        if end_year is None:
            end_year = current_year  # 结束年份：当前年份（如2025年），查询end_date<=20251231可获取2024年年报
        if start_year is None:
            start_year = current_year - 5  # 开始年份：当前年份-5（如2020年，最近5年）
        
        print(f"🚀 开始A股全网筛选...")
        print(f"📅 筛选年份范围：{start_year}年 - {end_year}年（查询end_date<={end_year}1231可获取{start_year}-{end_year-1}年年报）")

        # 1. 获取股票列表
        print(f"[SCREENING {datetime.now().strftime('%H:%M:%S')}] ========== 开始获取股票列表 ==========")
        if progress_callback:
            progress_callback("📋 正在获取A股股票列表...", 0.05)
            print("[SCREENING] 开始获取股票列表...")

        try:
            stock_list = self.get_a_stock_list(exclude_st=True)
            total_count = len(stock_list)
            print(f"[SCREENING] ✅ 成功获取 {total_count} 只股票列表")
            
            if progress_callback:
                progress_callback(f"✅ 成功获取 {total_count} 只A股股票列表，开始筛选...", 0.08)
                # 立即更新进度，让用户看到总数
                progress_callback(
                    f"已处理 0/{total_count} 只股票 (0.0%)，通过筛选 0 只，失败 0 只",
                    0.08
                )
            print(f"[SCREENING] 开始分析 {total_count} 只股票...")
        except Exception as e:
            print(f"[SCREENING] ❌ 获取股票列表失败: {e}")
            import traceback
            traceback.print_exc()
            if progress_callback:
                progress_callback(f"❌ 获取股票列表失败: {e}", 0.05)
            raise ValueError(f"获取股票列表失败: {e}")

        total_stocks = len(stock_list)
        passed_stocks = []
        failed_count = 0

        print(f"📊 共需筛选 {total_stocks} 只股票")
        
        # ===== 阶段1: 快速预扫描缓存状态 =====
        print(f"[CACHE-SCAN {datetime.now().strftime('%H:%M:%S')}] 🔍 开始扫描缓存状态...")
        if progress_callback:
            progress_callback(f"🔍 正在扫描 {total_stocks} 只股票的缓存状态...", 0.12)
        
        cached_stocks = []  # 有缓存的股票
        uncached_stocks = []  # 无缓存的股票
        
        scan_start = time.time()
        for idx, (_, row) in enumerate(stock_list.iterrows()):
            ts_code = row['ts_code']
            has_cache = self.check_cache_exists(ts_code, start_year, end_year)
            
            if has_cache:
                cached_stocks.append(row)
            else:
                uncached_stocks.append(row)
            
            # 每扫描1000只股票汇报一次
            if (idx + 1) % 1000 == 0:
                print(f"[CACHE-SCAN] 已扫描 {idx + 1}/{total_stocks} 只股票...")
        
        scan_duration = time.time() - scan_start
        print(f"[CACHE-SCAN] ✅ 缓存扫描完成，耗时 {scan_duration:.2f}秒")
        print(f"[CACHE-SCAN] 📊 统计: 有缓存 {len(cached_stocks)} 只 | 无缓存 {len(uncached_stocks)} 只")
        
        if progress_callback:
            progress_callback(
                f"✅ 缓存扫描完成: 有缓存 {len(cached_stocks)} 只, 无缓存 {len(uncached_stocks)} 只",
                0.15
            )

        # ===== 阶段2: 分组并发处理 =====
        
        # 2.1 处理有缓存的股票 (使用高并发)
        if cached_stocks:
            cache_workers = min(50, len(cached_stocks))  # 最多50线程
            print(f"[CACHED {datetime.now().strftime('%H:%M:%S')}] 🚀 开始处理有缓存的股票，使用 {cache_workers} 个线程")
            if progress_callback:
                progress_callback(f"🚀 快速处理 {len(cached_stocks)} 只有缓存的股票 (使用 {cache_workers} 线程)...", 0.18)
            
            cached_passed, cached_failed = self._process_stock_batch(
                cached_stocks,
                stock_list,
                pr_threshold,
                min_roe,
                start_year,
                end_year,
                cache_workers,  # 高并发
                0.0,  # 无需API延迟
                progress_callback,
                debug_callback,
                user_points,
                batch_name="CACHED"
            )
            
            passed_stocks.extend(cached_passed)
            failed_count += cached_failed
            
            print(f"[CACHED] ✅ 有缓存组处理完成: 通过 {len(cached_passed)} 只, 失败 {cached_failed} 只")
        
        # 2.2 处理无缓存的股票 (使用原并发数和API延迟)
        if uncached_stocks:
            print(f"[UNCACHED {datetime.now().strftime('%H:%M:%S')}] 🚀 开始处理无缓存的股票，使用 {max_workers} 个线程")
            if progress_callback:
                progress_callback(f"🔄 处理 {len(uncached_stocks)} 只无缓存的股票 (使用 {max_workers} 线程，需调用API)...", 0.50)
            
            uncached_passed, uncached_failed = self._process_stock_batch(
                uncached_stocks,
                stock_list,
                pr_threshold,
                min_roe,
                start_year,
                end_year,
                max_workers,  # 原并发数
                api_delay,  # 保留API延迟
                progress_callback,
                debug_callback,
                user_points,
                batch_name="UNCACHED"
            )
            
            passed_stocks.extend(uncached_passed)
            failed_count += uncached_failed
            
            print(f"[UNCACHED] ✅ 无缓存组处理完成: 通过 {len(uncached_passed)} 只, 失败 {uncached_failed} 只")

        # 优化：根据用户配置的并发数，不再强制限制为4
        # 用户可以根据自己的积分等级调整并发数（中级用户建议10，高级用户可到20）
        # 注意：如果触发API频率限制，需要降低并发数或增加延迟
        max_workers = max_workers  # 使用用户配置的并发数

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            print(f"[SCREENING {datetime.now().strftime('%H:%M:%S')}] 开始提交 {total_stocks} 个任务到线程池...")
            future_to_code = {}
            submit_count = 0
            for _, row in stock_list.iterrows():
                future = executor.submit(
                    self.analyze_single_stock,
                    row['ts_code'],
                    pr_threshold,
                    min_roe,
                    start_year,
                    end_year,
                    api_delay,
                    max_workers,  # 传递并发线程数
                    debug_callback,
                    user_points  # 传递积分信息，避免重复调用API
                )
                future_to_code[future] = row['ts_code']
                submit_count += 1
                if submit_count % 500 == 0:
                    print(f"[SCREENING {datetime.now().strftime('%H:%M:%S')}] 已提交 {submit_count}/{total_stocks} 个任务")
            
            print(f"[SCREENING {datetime.now().strftime('%H:%M:%S')}] ✅ 所有 {total_stocks} 个任务已提交完成，开始处理...")

            # 处理完成的任务
            completed = 0
            print(f"[SCREENING {datetime.now().strftime('%H:%M:%S')}] 开始等待任务完成，使用 as_completed...")
            for future in as_completed(future_to_code):
                ts_code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        # 获取股票基本信息用于日志
                        stock_info = stock_list[stock_list['ts_code'] == ts_code].iloc[0]
                        stock_name = stock_info.get('name', '未知')
                        
                        if result['overall_pass']:
                            # 添加股票基本信息
                            result.update({
                                'name': stock_info['name'],
                                'industry': stock_info['industry'],
                                'area': stock_info['area']
                            })
                            passed_stocks.append(result)
                            
                            # 详细日志：通过的股票
                            if progress_callback:
                                valuation = result.get('valuation_details', {})
                                pr = valuation.get('final_pr', 'N/A')
                                roe = valuation.get('roe_waa', 'N/A')
                                progress_callback(
                                    f"✅ {ts_code} {stock_name} 通过筛选 | PR={pr:.4f if isinstance(pr, (int, float)) else pr}, ROE={roe:.2f if isinstance(roe, (int, float)) else roe}%",
                                    0
                                )
                        else:
                            # 详细日志：未通过的股票及原因
                            failed_reasons = []
                            if not result.get('fundamentals_pass', False):
                                failed_reasons.append("基本面未通过")
                            if not result.get('valuation_pass', False):
                                valuation = result.get('valuation_details', {})
                                if not valuation.get('pr_pass', True):
                                    failed_reasons.append(f"PR超标({valuation.get('final_pr', 'N/A')} > {valuation.get('pr_threshold', 'N/A')})")
                                if not valuation.get('roe_pass', True):
                                    failed_reasons.append(f"ROE不足({valuation.get('roe_waa', 'N/A')}% < {valuation.get('min_roe', 'N/A')}%)")
                            
                            if progress_callback:
                                reason_str = " | ".join(failed_reasons) if failed_reasons else "未知原因"
                                progress_callback(
                                    f"❌ {ts_code} {stock_name} 未通过筛选: {reason_str}",
                                    0
                                )
                            failed_count += 1
                    else:
                        failed_count += 1
                        if progress_callback:
                            progress_callback(
                                f"⚠️ {ts_code} 分析失败：无法获取数据",
                                0
                            )

                    completed += 1
                    progress = 0.1 + (completed / total_stocks) * 0.9

                    # 实时更新进度（每只股票都更新，确保用户能看到实时进度）
                    if progress_callback:
                        # 每只股票都发送进度更新
                        progress_callback(
                            f"已处理 {completed}/{total_stocks} 只股票 ({completed/total_stocks*100:.1f}%)，通过筛选 {len(passed_stocks)} 只，失败 {failed_count} 只",
                            progress
                        )
                        
                        # 每处理3只股票发送一次心跳日志，确保用户知道任务在运行
                        if completed % 3 == 0:
                            progress_callback(
                                f"💓 任务运行中... 已处理 {completed}/{total_stocks} 只股票，当前进度 {completed/total_stocks*100:.1f}%",
                                progress
                            )
                    
                    # 每处理5只股票打印一次汇总进度到控制台
                    if completed % 5 == 0:
                        print(f"📈 进度: {completed}/{total_stocks} "
                              f"({completed/total_stocks*100:.1f}%)，"
                              f"通过: {len(passed_stocks)}，失败: {failed_count}")

                except Exception as exc:
                    failed_count += 1
                    error_msg = str(exc)
                    if progress_callback:
                        progress_callback(
                            f"❌ {ts_code} 分析异常：{error_msg[:100]}",
                            0
                        )
                    print(f"股票 {ts_code} 分析出错: {exc}")

        # 3. 按修正市赚率排序（从低到高）
        def get_sort_key(stock):
            """获取排序键：修正PR值，越低越优先"""
            valuation = stock.get('valuation_details', {})
            pr = valuation.get('final_pr')
            return pr if pr is not None else float('inf')

        passed_stocks.sort(key=get_sort_key)

        print(f"✅ 全网筛选完成！共通过 {len(passed_stocks)} 只股票，失败 {failed_count} 只")

        return passed_stocks

    def get_screening_stats(self, results: List[Dict]) -> Dict:
        """
        获取筛选统计信息

        参数:
            results: 筛选结果列表

        返回:
            统计信息字典
        """
        if not results:
            return {}

        stats = {
            'total_passed': len(results),
            'industries': {},
            'areas': {},
            'pr_distribution': {
                '<=0.5': 0,
                '0.5-1.0': 0,
                '1.0-1.5': 0,
                '>1.5': 0
            }
        }

        for stock in results:
            # 行业统计
            industry = stock.get('industry', '其他')
            stats['industries'][industry] = stats['industries'].get(industry, 0) + 1

            # 地区统计
            area = stock.get('area', '其他')
            stats['areas'][area] = stats['areas'].get(area, 0) + 1

            # PR分布统计
            valuation = stock.get('valuation_details', {})
            pr = valuation.get('final_pr')
            if pr is not None:
                if pr <= 0.5:
                    stats['pr_distribution']['<=0.5'] += 1
                elif pr <= 1.0:
                    stats['pr_distribution']['0.5-1.0'] += 1
                elif pr <= 1.5:
                    stats['pr_distribution']['1.0-1.5'] += 1
                else:
                    stats['pr_distribution']['>1.5'] += 1

        return stats


# 全局筛选器实例
stock_screener = StockScreener()


def run_full_market_screening(pr_threshold: float = 1.0,
                             min_roe: float = 10.0,  # 优化：默认10.0%（保守型ROE筛选）
                             start_year: int = None,  # 优化：动态获取，默认2000年
                             end_year: int = None,  # 优化：动态获取，默认当前年份
                             max_workers: int = 10,  # 优化：默认10线程，加速筛选
                             api_delay: float = 0.1,  # 优化：默认0.1秒，加速筛选
                             progress_callback=None,
                             debug_callback=None,
                             user_points: Optional[float] = None) -> Tuple[List[Dict], Dict]:  # 用户积分（可选，避免重复调用API）
    """
    执行全网筛选的主函数

    参数:
        pr_threshold: 市赚率阈值
        min_roe: 最低ROE要求(%)
        start_year: 开始年份
        end_year: 结束年份
        max_workers: 最大并发数
        api_delay: API调用延迟
        progress_callback: 进度回调函数

    返回:
        (筛选结果列表, 统计信息)
    """
    try:
        # 优化：在执行筛选前动态获取当前年份，使用最近5年数据
        current_year = datetime.now().year  # 获取当前年份（如2025年）
        if end_year is None:
            end_year = current_year  # 结束年份：当前年份（如2025年），查询end_date<=20251231可获取2024年年报
        if start_year is None:
            start_year = current_year - 5  # 开始年份：当前年份-5（如2020年，最近5年）
        
        # 执行筛选
        results = stock_screener.screen_all_stocks(
            user_points=user_points,  # 传递积分信息，避免重复调用API
            pr_threshold=pr_threshold,
            min_roe=min_roe,
            start_year=start_year,
            end_year=end_year,
            max_workers=max_workers,
            api_delay=api_delay,
            progress_callback=progress_callback,
            debug_callback=debug_callback
        )

        # 生成统计信息
        stats = stock_screener.get_screening_stats(results)

        return results, stats

    except Exception as e:
        raise ValueError(f"全网筛选失败: {e}")


if __name__ == "__main__":
    # 测试连通性
    success, logs = run_connectivity_tests(verbose=True)
    if not success:
        print("❌ 网络连通性检查失败，请检查网络或API配置")
        exit(1)

    # 测试全网筛选（仅前10只股票作为示例）
    print("\n🧪 开始测试全网筛选功能...")

    try:
        # 这里只是测试，实际使用时会筛选全部股票
        screener = StockScreener()
        stock_list = screener.get_a_stock_list(exclude_st=True)

        print(f"获取到 {len(stock_list)} 只股票，测试前10只...")

        test_results = []
        for i, (_, row) in enumerate(stock_list.head(10).iterrows()):
            print(f"测试股票 {i+1}/10: {row['ts_code']} {row['name']}")
            result = screener.analyze_single_stock(row['ts_code'])
            if result:
                test_results.append(result)
                if result['overall_pass']:
                    print(f"  ✅ 通过筛选")
                else:
                    print(f"  ❌ 未通过筛选")
            else:
                print(f"  ⚠️ 分析失败")

        print(f"\n测试完成，{len([r for r in test_results if r['overall_pass']])}/{len(test_results)} 只股票通过筛选")

    except Exception as e:
        print(f"测试失败: {e}")
