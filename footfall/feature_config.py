"""
Prophet 外生变量：与 finaldata1 列顺序一致（无 IS_FRIDAY；含 EXCHANGE_RATE、PRICE_INDEX）。
"""

from __future__ import annotations

# 与训练时 prophet_df 中 add_regressor 顺序一致
REGRESSOR_COLUMNS: list[str] = [
    "IS_PH_CN",
    "IS_PH_HK",
    "HAS_Concerts",
    "HAS_Macau_Big_Events",
    "IS_SATURDAY",
    "IS_SUNDAY",
    "IS_TYPHOON8910",
    "EXCHANGE_RATE",
    "PRICE_INDEX",
]

CONTINUOUS_COLUMNS: tuple[str, ...] = ("EXCHANGE_RATE", "PRICE_INDEX")

# DeepSeek 只判这 5 个 0/1；周末两格由日历覆盖
AI_BINARY_KEYS: tuple[str, ...] = (
    "IS_PH_CN",
    "IS_PH_HK",
    "HAS_Concerts",
    "HAS_Macau_Big_Events",
    "IS_TYPHOON8910",
)
