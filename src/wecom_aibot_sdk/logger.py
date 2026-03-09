"""
默认日志实现
带有日志级别和时间戳的控制台日志
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class DefaultLogger:
    """默认日志实现，带有日志级别和时间戳"""

    def __init__(self, prefix: str = "AiBotSDK") -> None:
        self._prefix = prefix

    def _format_time(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def debug(self, message: str, *args: Any) -> None:
        extra = " ".join(str(a) for a in args)
        print(f"[{self._format_time()}] [{self._prefix}] [DEBUG] {message} {extra}".rstrip())

    def info(self, message: str, *args: Any) -> None:
        extra = " ".join(str(a) for a in args)
        print(f"[{self._format_time()}] [{self._prefix}] [INFO] {message} {extra}".rstrip())

    def warn(self, message: str, *args: Any) -> None:
        extra = " ".join(str(a) for a in args)
        print(f"[{self._format_time()}] [{self._prefix}] [WARN] {message} {extra}".rstrip())

    def error(self, message: str, *args: Any) -> None:
        extra = " ".join(str(a) for a in args)
        print(f"[{self._format_time()}] [{self._prefix}] [ERROR] {message} {extra}".rstrip())
