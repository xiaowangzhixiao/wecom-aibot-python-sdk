"""
通用工具方法
"""

from __future__ import annotations

import secrets
import time


def generate_random_string(length: int = 8) -> str:
    """生成随机十六进制字符串"""
    return secrets.token_hex(length // 2 + 1)[:length]


def generate_req_id(prefix: str) -> str:
    """
    生成唯一请求 ID

    格式: ``{prefix}_{timestamp}_{random}``
    """
    timestamp = int(time.time() * 1000)
    random_str = generate_random_string()
    return f"{prefix}_{timestamp}_{random_str}"
