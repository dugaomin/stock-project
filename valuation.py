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
        
        公式：股息支付率 = (每股股息 / 基本每股收益) × 100%
        
        Args:
            dividend_per_share: 每股股息（现金分红）
            eps: 基本每股收益
            
        Returns:
            股息支付率(%)，如果无法计算则返回None
        """
        if eps is None or eps <= 0:
            return None
        
        if dividend_per_share is None or dividend_per_share < 0:
            return None
        
        payout_ratio = (dividend_per_share / eps) * 100
        
        # 股息支付率不应超过100%（超过说明分红超过利润，需要预警）
        if payout_ratio > 100:
            print(f"⚠️ 股息支付率{payout_ratio:.2f}%超过100%，数据可能异常")
        
        return payout_ratio
    
    @staticmethod
    def calculate_correction_factor(payout_ratio: Optional[float]) -> float:
        """
        计算修正系数N（用于修正市赚率）
        
        规则：
        - 股息支付率 ≥ 50% → N = 1.0（分红充足，不需修正）
        - 股息支付率 ≤ 25% → N = 2.0（分红太少，需要加倍修正）
        - 股息支付率在25%-50%之间 → N = 50% / 实际支付率
        
        Args:
            payout_ratio: 股息支付率(%)
            
        Returns:
            修正系数N
        """
        if payout_ratio is None:
            return 2.0  # 无数据时保守处理，使用最大修正系数
        
        if payout_ratio >= 50:
            return 1.0  # 分红充足
        elif payout_ratio <= 25:
            return 2.0  # 分红太少
        else:
            # 25% < 支付率 < 50%，线性插值
            return 50.0 / payout_ratio
    
    @staticmethod
    def calculate_standard_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算标准市赚率（个股使用）
        
        公式：标准PR = 市盈率TTM / 加权净资产收益率 / 150
        注：ROE需转换为小数形式（13.01% → 0.1301）
        
        Args:
            pe_ttm: 市盈率TTM（滚动市盈率）
            roe_waa: 加权净资产收益率(%)
            
        Returns:
            标准市赚率，如果无法计算则返回None
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
        
        # 标准市赚率 = PE / ROE / 150（ROE转换为小数形式）
        standard_pr = pe_ttm / roe_rate / PRValuation.STOCK_PR_DENOMINATOR
        
        return standard_pr
    
    @staticmethod
    def calculate_corrected_pr(pe_ttm: float, roe_waa: float, 
                               dividend_per_share: float, eps: float) -> Tuple[Optional[float], Optional[float], float]:
        """
        计算修正市赚率（考虑股息支付率的个股估值）
        
        公式：修正PR = N × 市盈率TTM / 加权净资产收益率 / 150
        注：ROE需转换为小数形式（13.01% → 0.1301）
        
        Args:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            dividend_per_share: 每股股息
            eps: 基本每股收益
            
        Returns:
            (修正市赚率, 股息支付率, 修正系数N)
        """
        # 先计算股息支付率
        payout_ratio = PRValuation.calculate_dividend_payout_ratio(dividend_per_share, eps)
        
        # 计算修正系数N
        N = PRValuation.calculate_correction_factor(payout_ratio)
        
        # 计算修正市赚率
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if pe_ttm is None or pe_ttm <= 0 or roe_rate is None or roe_rate <= 0:
            return None, payout_ratio, N
        
        corrected_pr = N * pe_ttm / roe_rate / PRValuation.STOCK_PR_DENOMINATOR
        
        return corrected_pr, payout_ratio, N
    
    @staticmethod
    def calculate_broad_index_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算宽基指数市赚率
        
        公式：宽基PR = 市盈率TTM / 加权净资产收益率 / 150
        注：ROE需先转换为小数形式
        
        注：分母使用150而不是100，因为宽基指数估值更保守
        
        Args:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            
        Returns:
            宽基指数市赚率，如果无法计算则返回None
        """
        if pe_ttm is None or pe_ttm <= 0:
            return None
        
        roe_rate = PRValuation._normalize_roe(roe_waa)
        if roe_rate is None or roe_rate <= 0:
            return None
        
        # 宽基指数市赚率 = PE / ROE / 150（ROE转换为小数形式）
        broad_pr = pe_ttm / roe_rate / PRValuation.INDEX_PR_DENOMINATOR
        
        return broad_pr
    
    @staticmethod
    def calculate_buffett_sell_pr(pe_ttm: float, roe_waa: float) -> Optional[float]:
        """
        计算巴菲特卖标普指标（宽基指数卖出标准）
        
        公式：PR = 市盈率TTM / 加权净资产收益率 / 150
        注：ROE需转换为小数形式（13.01% → 0.1301）
        
        用途：判断整个市场（标普500等宽基指数）是否太贵，PR>1.5时建议清仓
        
        Args:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            
        Returns:
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
        
        Args:
            pe_ttm: 市盈率TTM
            roe_waa: 加权净资产收益率(%)
            
        Returns:
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
    def generate_trading_signal(pr_value: float, index_name: str) -> Dict:
        """
        生成交易信号
        
        Args:
            pr_value: 当前市赚率值
            index_name: 指数名称（如"沪深300"）
            
        Returns:
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
        
        Args:
            data: 包含pe_ttm, roe_waa, dividend_per_share, eps等字段的字典
            
        Returns:
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
        
        Args:
            data: 包含pe_ttm, roe_waa等字段的字典
            index_name: 指数名称
            
        Returns:
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

