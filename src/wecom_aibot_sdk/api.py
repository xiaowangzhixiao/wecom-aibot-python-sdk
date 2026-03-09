"""
企业微信 API 客户端
仅负责文件下载等 HTTP 辅助功能，消息收发均走 WebSocket 通道
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote

import httpx


class WeComApiClient:
    """HTTP API 客户端（文件下载）"""

    def __init__(self, logger: Any, timeout: int = 10000) -> None:
        self._logger = logger
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout / 1000),
            headers={"Content-Type": "application/json"},
        )

    async def download_file_raw(self, url: str) -> dict[str, Any]:
        """
        下载文件（返回原始 bytes 及文件名）

        :returns: ``{"buffer": bytes, "filename": str | None}``
        """
        self._logger.info("Downloading file...")

        try:
            response = await self._client.get(url)
            response.raise_for_status()

            # 从 Content-Disposition 头中解析文件名
            content_disposition = response.headers.get("content-disposition", "")
            filename: str | None = None
            if content_disposition:
                # 优先匹配 filename*=UTF-8''xxx
                utf8_match = re.search(r"filename\*=UTF-8''([^;\s]+)", content_disposition, re.IGNORECASE)
                if utf8_match:
                    filename = unquote(utf8_match.group(1))
                else:
                    match = re.search(r'filename="?([^";\s]+)"?', content_disposition, re.IGNORECASE)
                    if match:
                        filename = unquote(match.group(1))

            self._logger.info("File downloaded successfully")
            return {"buffer": response.content, "filename": filename}
        except Exception as e:
            self._logger.error("File download failed:", str(e))
            raise

    async def close(self) -> None:
        await self._client.aclose()
