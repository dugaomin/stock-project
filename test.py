# -*- coding: utf-8 -*-
"""简单联通性测试脚本：验证 tushare 模块可用并尝试获取交易日历。"""

from __future__ import annotations

import tushare as ts

from settings import DEFAULT_TOKEN


def main() -> None:
    pro = ts.pro_api(DEFAULT_TOKEN)
    df = pro.trade_cal(limit=5)
    print(df)


if __name__ == "__main__":
    main()
