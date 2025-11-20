# -*- coding: utf-8 -*-
"""
市赚率(PR)估值模型
基于市盈率和ROE的估值方法，判断买卖时机
"""

from typing import Dict, Optional, Tuple
import pandas as pd


class PRValuation:
    """市赚率估值计算器"""
    
    STOCK_PR_DENOMINATOR = 150  # 个股市赚率分母
    INDEX_PR_DENOMINATOR = 150  # 宽基指数市赚率分母
    
    # 不同指数的估值基准配置
    INDEX_BENCHMARKS = {
        "沪深300": {
            "type": "宽基指数",
            "reasonable_pr": 1.0,  # 合理PR值
            "buy_threshold": 1.0,  # 买入阈值
            "sell_start": 1.0,     # 开始卖出阈值
            "sell_all": 1.4,       # 完全清仓阈值
            "tax_rate": 0.0,       # A股股息税0%
        },
        "恒生指数": {
            "type": "宽基指数",
            "reasonable_pr": 0.85,
            "buy_threshold": 0.85,
            "sell_start": 0.85,
            "sell_all": 1.19,
            "tax_rate": 0.15,  # 平均股息税15%
        },
        "恒生国企": {
            "type": "宽基指数",
            "reasonable_pr": 0.8,
            "buy_threshold": 0.8,
            "sell_start": 0.8,
            "sell_all": 1.12,
            "tax_rate": 0.20,  # 股息税20%
        }
    }
    
    @staticmethod
    def _normalize_roe(roe_waa: Optional[float]) -> Optional[float]:
        """
        将ROE转换为小数形式（Tushare通常返回百分比值）
        13.01 -> 0.1301；若原本已是0.x则保持不变
        """
        if roe_waa is None:
            return None
        try:
            roe_val = float(roe_waa)
        except (TypeError, ValueError):
            return None
        if roe_val == 0:
            return 0
        return roe_val / 100 if abs(roe_val) > 1 else roe_val
    
    @staticmethod
    def calculate_dividend_payout_ratio(dividend_per_share: float, eps: float) -> Optional[float]:
        """
        计算股息支付率
        """
        if eps is None or eps == 0:
            return None
        if dividend_per_share is None:
            return 0.0
        return dividend_per_share / eps

    @staticmethod
    def calculate_buffett_sell_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算巴菲特卖标普指标（宽基指数卖出标准）
        
        公式：PR = 市盈率TTM / 加权净资产收益率 / 150
        注：ROE需转换为小数形式（13.01% → 0.1301）
        
        用途：判断整个市场（标普500等宽基指数）是否太贵，PR>1.5时建议清仓
        
        参数:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            
        返回:
            巴菲特卖标普指标PR值，如果无法计算则返回None
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
        
        # 巴菲特卖标普指标 = PE / ROE / 150（ROE转换为小数形式）
        buffett_sell_pr = pe_ttm / roe_rate / 150
        
        return buffett_sell_pr
    
    @staticmethod
    def calculate_buffett_buy_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算巴菲特购买股票指标（个股买入标准）
        
        公式：PR = 市盈率TTM / 加权净资产收益率 / 100
        注：ROE需转换为小数形式（13.01% → 0.1301）
        
        用途：判断个股是否值得买入，PR<0.4时严重低估（用40美分买1美元资产）
        
        参数:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            
        返回:
            巴菲特购买股票指标PR值，如果无法计算则返回None
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
        
        # 巴菲特购买股票指标 = PE / ROE / 100（ROE转换为小数形式）
        buffett_buy_pr = pe_ttm / roe_rate / 100
        
        return buffett_buy_pr
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 23, signal_period: int = 8) -> pd.DataFrame:
        """
        计算修正版 MACD 指标
        
        参数:
            df: 包含 'close' 列的 DataFrame
            fast_period: 短周期 (默认12)
            slow_period: 长周期 (默认23，修正版推荐值)
            signal_period: 信号平滑周期 (默认8，修正版推荐值)
            
        返回:
            包含 'dif', 'dea', 'macd' 列的 DataFrame
        """
        # 计算 EMA
        ema_fast = df['close'].ewm(span=fast_period, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow_period, adjust=False).mean()
        
        # 计算 DIF
        df['dif'] = ema_fast - ema_slow
        
        # 计算 DEA
        df['dea'] = df['dif'].ewm(span=signal_period, adjust=False).mean()
        
        # 计算 MACD 柱体 (通常乘以2)
        df['macd'] = (df['dif'] - df['dea']) * 2
        
        return df

    @staticmethod
    def calculate_yellow_bar(df: pd.DataFrame) -> pd.DataFrame:
        """
        计算“黄柱”逻辑
        
        黄柱业务含义：
        1. 出现：阶段性机会点提醒（行情可能由弱转强）
        2. 消失：高位风险提醒（上涨动能衰减）
        
        计算逻辑（技术实现）：
        这里需要定义具体的黄柱触发条件。根据业务描述：
        - "在股价相对低位区域，指标首次出现黄柱" -> 可能与 DIF/DEA 的位置或拐点有关
        - "在股价相对高位区域，原本持续出现的黄柱突然消失"
        
        由于业务侧未给出具体的数学公式，我们将基于常见的“转强信号”来模拟黄柱逻辑：
        假设黄柱代表“多头动能增强”或“空头动能衰减”的关键时刻。
        
        拟定逻辑：
        1. 必须满足 DIF > DEA (多头区域) 或者 DIF 上穿 DEA (金叉)
        2. 或者 MACD 绿柱缩短 (空头衰减)
        
        为了简化并符合“黄柱”作为额外标记的特性，我们定义：
        黄柱 = 当 (MACD > 0 且 MACD > 昨日MACD) 或者 (MACD < 0 且 MACD > 昨日MACD)
        即：MACD 值在增长（红柱变长或绿柱变短），代表动能向好。
        
        为了更贴合“低位出现，高位消失”的描述，我们增加限制：
        - 仅在 MACD 柱体数值增加时显示黄柱
        - 黄柱的高度等于 MACD 柱体的高度（覆盖显示）
        """
        # 计算 MACD 的 5日均线
        df['macd_ma5'] = df['macd'].rolling(window=5).mean()
        
        # 定义黄柱逻辑：MACD > MACD_MA5
        # 这里我们只标记是否满足条件，具体的绘制（从MA5画到MACD）在前端处理
        # yellow_bar 列存储 MACD 的值，用于判断是否显示
        
        df['yellow_bar'] = df.apply(lambda x: x['macd'] if x['macd'] > x['macd_ma5'] else 0, axis=1)
        
        # 识别信号点
        # 1. 黄柱首次出现 (昨天无黄柱，今天有)
        df['yellow_bar_prev'] = df['yellow_bar'].shift(1)
        df['yellow_appear'] = (df['yellow_bar_prev'] == 0) & (df['yellow_bar'] != 0)
        
        # 2. 黄柱消失 (昨天有黄柱，今天无)
        df['yellow_disappear'] = (df['yellow_bar_prev'] != 0) & (df['yellow_bar'] == 0)
        
        return df

    @staticmethod
    def calculate_price_for_pr(target_pr: float, roe_waa: float, eps: float, coefficient: int) -> Optional[float]:
        """
        逆向推导：根据目标PR值计算对应的股价
        
        公式推导：
        PR = (Price / EPS) / ROE / Coefficient
        => Price = PR * Coefficient * ROE * EPS
        
        参数:
            target_pr: 目标PR值
            roe_waa: 加权净资产收益率(%)
            eps: 每股收益
            coefficient: 系数 (100 或 150)
            
        返回:
            对应股价，如果无法计算则返回None
        """
        if eps is None or eps <= 0:
            return None
            
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
            
        # 股价 = PR * 系数 * ROE(小数) * EPS
        target_price = target_pr * coefficient * roe_rate * eps
        return target_price
    
    @staticmethod
    def calculate_standard_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算标准市赚率 (不考虑分红修正)
        
        公式：PR = 市盈率TTM / 加权净资产收益率 / 150
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
            
        return pe_ttm / roe_rate / PRValuation.STOCK_PR_DENOMINATOR

    @staticmethod
    def calculate_correction_factor(payout_ratio: float) -> float:
        """
        计算修正系数N
        
        规则：
        - 股息支付率 >= 50% -> N = 1.0
        - 股息支付率 <= 25% -> N = 2.0
        - 25% < 支付率 < 50% -> N = 50% / 支付率
        """
        if payout_ratio >= 0.5:
            return 1.0
        elif payout_ratio <= 0.25:
            return 2.0
        else:
            return 0.5 / payout_ratio

    @staticmethod
    def calculate_corrected_pr(pe_ttm: float, roe_waa: float, dividend_per_share: float, eps: float) -> Tuple[Optional[float], Optional[float], float]:
        """
        计算修正市赚率 (考虑分红修正)
        
        返回:
            (corrected_pr, payout_ratio, correction_factor)
        """
        standard_pr = PRValuation.calculate_standard_pr(pe_ttm, roe_waa)
        if standard_pr is None:
            return None, None, 1.0
            
        payout_ratio = PRValuation.calculate_dividend_payout_ratio(dividend_per_share, eps)
        
        # 如果无法计算支付率（如EPS<=0），则默认不修正(N=1)或按最差情况(N=2)? 
        # 这里假设如果无法计算支付率，则不进行修正，或者视情况而定。
        # 根据文档逻辑，如果无法计算，可能无法得出修正PR。
        # 但为了健壮性，如果无法计算支付率，我们假设N=1 (即不惩罚也不奖励)，或者返回None?
        # 让我们假设N=1如果无法计算支付率
        if payout_ratio is None:
            return standard_pr, None, 1.0
            
        N = PRValuation.calculate_correction_factor(payout_ratio)
        corrected_pr = standard_pr * N
        
        return corrected_pr, payout_ratio * 100, N

    @staticmethod
    def calculate_broad_index_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算宽基指数市赚率
        
        公式：PR = 市盈率TTM / 加权净资产收益率 / 150
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
            
        return pe_ttm / roe_rate / PRValuation.INDEX_PR_DENOMINATOR
    
    @staticmethod
    def generate_trading_signal(pr_value: float, index_name: str) -> Dict:
        """
        生成交易信号
        
        参数:
            pr_value: 当前市赚率值
            index_name: 指数名称（如"沪深300"）
            
        返回:
            交易信号字典，包含信号类型、强度、建议仓位等
        """
        # 获取配置
        config = PRValuation.INDEX_BENCHMARKS.get(index_name, PRValuation.INDEX_BENCHMARKS["沪深300"])
        
        buy_threshold = config["buy_threshold"]
        sell_start = config["sell_start"]
        sell_all = config["sell_all"]
        
        # 判断信号类型
        if pr_value <= buy_threshold:
            # 低估区域 - 买入信号
            signal_type = "买入"
            distance_to_buy = buy_threshold - pr_value
            signal_strength = min(1.0, distance_to_buy / buy_threshold)  # 越低估信号越强
            suggested_position = 1.0  # 满仓
            sell_ratio = 0.0
            reason = f"市赚率{pr_value:.4f}低于买入阈值{buy_threshold:.2f}，当前低估，建议买入"
            
        elif pr_value < sell_start:
            # 合理区域 - 持有信号
            signal_type = "持有"
            signal_strength = 0.5
            suggested_position = 1.0  # 保持满仓
            sell_ratio = 0.0
            reason = f"市赚率{pr_value:.4f}处于合理区间[{buy_threshold:.2f}, {sell_start:.2f}]，建议持有"
            
        elif pr_value < sell_all:
            # 高估区域 - 部分卖出（越涨越卖）
            signal_type = "部分卖出"
            
            # 计算卖出比例（线性增加）
            # 从sell_start到sell_all，卖出比例从0%增加到100%
            sell_progress = (pr_value - sell_start) / (sell_all - sell_start)
            sell_ratio = min(1.0, sell_progress)  # 0到1之间
            
            signal_strength = sell_ratio
            suggested_position = 1.0 - sell_ratio
            
            reason = f"市赚率{pr_value:.4f}高于卖出阈值{sell_start:.2f}，建议卖出{sell_ratio*100:.1f}%仓位"
            
        else:
            # 严重高估 - 全部卖出
            signal_type = "全部卖出"
            signal_strength = 1.0
            suggested_position = 0.0
            sell_ratio = 1.0
            reason = f"市赚率{pr_value:.4f}超过清仓阈值{sell_all:.2f}，严重高估，建议清仓"
        
        return {
            "signal_type": signal_type,
            "signal_strength": round(signal_strength, 2),
            "suggested_position": round(suggested_position, 4),
            "sell_ratio": round(sell_ratio, 4),
            "reason": reason,
            "pr_value": round(pr_value, 4),
            "buy_threshold": buy_threshold,
            "sell_start": sell_start,
            "sell_all": sell_all,
            "valuation_status": "低估" if pr_value <= buy_threshold else "合理" if pr_value < sell_start else "高估"
        }
    
    @staticmethod
    def analyze_stock_valuation(data: Dict) -> Dict:
        """
        综合分析个股估值
        
        参数:
            data: 包含pe_ttm, roe_waa, dividend_per_share, eps等字段的字典
            
        返回:
            完整的估值分析结果
        """
        pe_ttm = data.get('pe_ttm')
        roe_waa = data.get('roe_waa')
        dividend_per_share = data.get('dividend_per_share', 0)
        eps = data.get('eps')
        
        # 计算标准市赚率
        standard_pr = PRValuation.calculate_standard_pr(pe_ttm, roe_waa)
        
        # 计算修正市赚率
        corrected_pr, payout_ratio, N = PRValuation.calculate_corrected_pr(
            pe_ttm, roe_waa, dividend_per_share, eps
        )
        
        # 默认使用修正市赚率，如果无法计算则使用标准市赚率
        final_pr = corrected_pr if corrected_pr is not None else standard_pr
        
        # 生成交易信号（个股使用"沪深300"类似的阈值）
        signal = None
        if final_pr is not None:
            signal = PRValuation.generate_trading_signal(final_pr, "沪深300")
        
        return {
            "pe_ttm": pe_ttm,
            "roe_waa": roe_waa,
            "eps": eps,
            "dividend_per_share": dividend_per_share,
            "payout_ratio": payout_ratio,
            "correction_factor": N,
            "standard_pr": standard_pr,
            "corrected_pr": corrected_pr,
            "final_pr": final_pr,
            "signal": signal
        }
    
    @staticmethod
    def analyze_index_valuation(data: Dict, index_name: str = "沪深300") -> Dict:
        """
        综合分析指数估值（宽基指数）
        
        参数:
            data: 包含pe_ttm, roe_waa等字段的字典
            index_name: 指数名称
            
        返回:
            完整的估值分析结果
        """
        pe_ttm = data.get('pe_ttm')
        roe_waa = data.get('roe_waa')
        
        # 计算宽基指数市赚率（分母150）
        broad_pr = PRValuation.calculate_broad_index_pr(pe_ttm, roe_waa)
        
        # 生成交易信号
        signal = None
        if broad_pr is not None:
            signal = PRValuation.generate_trading_signal(broad_pr, index_name)
        
        return {
            "pe_ttm": pe_ttm,
            "roe_waa": roe_waa,
            "broad_pr": broad_pr,
            "final_pr": broad_pr,
            "signal": signal,
            "index_name": index_name,
            "benchmark": PRValuation.INDEX_BENCHMARKS.get(index_name)
        }


# 使用示例和测试
if __name__ == "__main__":
    # 示例1：个股估值（茅台）
    print("=" * 50)
    print("示例1：个股估值（贵州茅台）")
    print("=" * 50)
    
    stock_data = {
        'pe_ttm': 30.5,      # 市盈率TTM
        'roe_waa': 25.0,     # 加权ROE 25%
        'eps': 45.0,         # 每股收益45元
        'dividend_per_share': 27.0  # 每股分红27元
    }
    
    result = PRValuation.analyze_stock_valuation(stock_data)
    
    print(f"市盈率TTM: {result['pe_ttm']}")
    print(f"加权ROE: {result['roe_waa']}%")
    print(f"每股收益: {result['eps']}元")
    print(f"每股股息: {result['dividend_per_share']}元")
    print(f"股息支付率: {result['payout_ratio']:.2f}%" if result['payout_ratio'] else "无法计算")
    print(f"修正系数N: {result['correction_factor']:.3f}")
    print(f"标准市赚率: {result['standard_pr']:.4f}" if result['standard_pr'] else "无法计算")
    print(f"修正市赚率: {result['corrected_pr']:.4f}" if result['corrected_pr'] else "无法计算")
    print(f"最终市赚率: {result['final_pr']:.4f}" if result['final_pr'] else "无法计算")
    
    if result['signal']:
        print(f"\n交易信号: {result['signal']['signal_type']}")
        print(f"估值状态: {result['signal']['valuation_status']}")
        print(f"建议仓位: {result['signal']['suggested_position']*100:.1f}%")
        print(f"决策理由: {result['signal']['reason']}")
    
    # 示例2：指数估值（沪深300）
    print("\n" + "=" * 50)
    print("示例2：指数估值（沪深300）")
    print("=" * 50)
    
    index_data = {
        'pe_ttm': 13.5,   # 市盈率TTM
        'roe_waa': 11.0,  # 加权ROE 11%
    }
    
    result = PRValuation.analyze_index_valuation(index_data, "沪深300")
    
    print(f"市盈率TTM: {result['pe_ttm']}")
    print(f"加权ROE: {result['roe_waa']}%")
    print(f"宽基市赚率: {result['broad_pr']:.4f}" if result['broad_pr'] else "无法计算")
    
    if result['signal']:
        print(f"\n交易信号: {result['signal']['signal_type']}")
        print(f"估值状态: {result['signal']['valuation_status']}")
        print(f"建议仓位: {result['signal']['suggested_position']*100:.1f}%")
        print(f"决策理由: {result['signal']['reason']}")

