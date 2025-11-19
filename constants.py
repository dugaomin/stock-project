# -*- coding: utf-8 -*-
"""
常量配置模块
存储项目中使用的静态配置和常量
"""

# 行业评分标准配置字典
# key: 行业名称
# value: 评分规则字典，包含：
#   - name: 行业显示名称
#   - debt_ratio_max: 资产负债率上限（%），超过此值扣分
#   - gross_margin_min: 毛利率下限（%），低于此值扣分
#   - description: 规则描述文本，用于UI展示
SECTOR_RULES = {
    "地产": {
        "name": "地产",
        "debt_ratio_max": 60.0,  # 地产行业杠杆较高，容忍度设为60%
        "gross_margin_min": 15.0, # 地产行业毛利相对较低，设为15%
        "description": "地产行业资产负债率<60%较健康"
    },
    "科技": {
        "name": "科技",
        "debt_ratio_max": 50.0,  # 科技行业风险较高，负债率不宜过高，设为50%
        "gross_margin_min": 30.0, # 科技行业通常有较高毛利，设为30%
        "description": "科技行业资产负债率>50%需警惕"
    },
    "消费": {
        "name": "消费",
        "debt_ratio_max": 40.0,  # 消费行业现金流好，负债率应较低，设为40%
        "gross_margin_min": 40.0, # 消费品通常有品牌溢价，毛利要求较高，设为40%
        "description": "消费行业越低越安全，毛利率<40%需警惕"
    },
    "制造业": {
        "name": "制造业",
        "debt_ratio_max": 60.0,  # 制造业重资产，容忍较高负债，设为60%
        "gross_margin_min": 25.0, # 制造业竞争激烈，25%毛利已算优秀
        "description": "制造业毛利率25%就可能很优秀"
    },
    "品牌/平台": {
        "name": "品牌/平台",
        "debt_ratio_max": 40.0,  # 品牌/平台类轻资产，负债率应低
        "gross_margin_min": 60.0, # 品牌溢价极高，毛利要求60%以上
        "description": "品牌溢价强，通常毛利率更高（60%+）"
    },
    "金融": {
        "name": "金融",
        "debt_ratio_max": 90.0,  # 金融行业高杠杆经营是常态，设为90%
        "gross_margin_min": 20.0, # 金融行业毛利计算特殊，暂设20%
        "description": "金融行业特殊，负债率高属正常"
    },
    "其他": {
        "name": "其他",
        "debt_ratio_max": 60.0,  # 通用标准：负债率60%
        "gross_margin_min": 15.0, # 通用标准：毛利率15%
        "description": "通用标准"
    }
}
