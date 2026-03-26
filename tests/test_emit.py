"""
测试 WSClient.emit() 对同步和异步事件处理器的异常处理行为

覆盖场景：
- 同步 handler 异常被捕获并记录日志
- 异步 handler 异常被捕获并记录日志
- 异步 handler 正常完成不触发错误日志
- 异步 handler 被取消不触发错误日志
- 单个 handler 异常不影响其他 handler 执行
"""

import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock


def _make_client():
    """创建一个不连接 WebSocket 的 WSClient 实例"""
    with patch("wecom_aibot_sdk.client.WsConnectionManager"), \
         patch("wecom_aibot_sdk.client.WeComApiClient"):
        from wecom_aibot_sdk.client import WSClient
        return WSClient(bot_id="test", secret="test")


class TestEmitSyncHandler:
    """同步事件处理器"""

    def test_sync_handler_exception_logged(self):
        """同步 handler 抛异常应被捕获并记录日志"""
        client = _make_client()
        client._logger = MagicMock()

        def bad_handler(data):
            raise ValueError("sync boom")

        client.on("test_event", bad_handler)
        client.emit("test_event", {"foo": "bar"})

        client._logger.error.assert_called_once()
        msg = client._logger.error.call_args[0][0]
        assert "test_event" in msg

    def test_sync_handler_exception_does_not_block_others(self):
        """一个同步 handler 异常不影响后续 handler"""
        client = _make_client()
        client._logger = MagicMock()
        called = []

        def bad_handler(data):
            raise RuntimeError("fail")

        def good_handler(data):
            called.append(data)

        client.on("evt", bad_handler)
        client.on("evt", good_handler)
        client.emit("evt", "hello")

        assert called == ["hello"]


class TestEmitAsyncHandler:
    """异步事件处理器"""

    @pytest.mark.asyncio
    async def test_async_handler_exception_logged(self):
        """异步 handler 抛异常应被捕获并通过 logger.error 记录"""
        client = _make_client()
        client._logger = MagicMock()

        async def bad_async_handler(data):
            raise ValueError("async boom")

        client.on("async_evt", bad_async_handler)
        client.emit("async_evt", "payload")

        # 等待所有 handler tasks 完成
        if client._handler_tasks:
            await asyncio.gather(*client._handler_tasks, return_exceptions=True)

        client._logger.error.assert_called_once()
        msg = client._logger.error.call_args[0][0]
        assert "async_evt" in msg
        assert "async" in msg.lower()

    @pytest.mark.asyncio
    async def test_async_handler_success_no_error_log(self):
        """异步 handler 正常完成不应触发错误日志"""
        client = _make_client()
        client._logger = MagicMock()
        result = []

        async def good_async_handler(data):
            result.append(data)

        client.on("ok_evt", good_async_handler)
        client.emit("ok_evt", "data")

        if client._handler_tasks:
            await asyncio.gather(*client._handler_tasks, return_exceptions=True)

        client._logger.error.assert_not_called()
        assert result == ["data"]

    @pytest.mark.asyncio
    async def test_async_handler_cancelled_no_error_log(self):
        """异步 handler 被取消不应触发错误日志"""
        client = _make_client()
        client._logger = MagicMock()

        async def slow_handler(data):
            await asyncio.sleep(999)

        client.on("cancel_evt", slow_handler)
        client.emit("cancel_evt", "x")

        # 取消所有 handler tasks
        for task in list(client._handler_tasks):
            task.cancel()

        await asyncio.gather(*client._handler_tasks, return_exceptions=True)

        client._logger.error.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_handler_exception_does_not_block_others(self):
        """一个异步 handler 异常不影响其他 handler"""
        client = _make_client()
        client._logger = MagicMock()
        result = []

        async def bad_handler(data):
            raise RuntimeError("fail")

        async def good_handler(data):
            result.append(data)

        client.on("multi", bad_handler)
        client.on("multi", good_handler)
        client.emit("multi", "val")

        if client._handler_tasks:
            await asyncio.gather(*client._handler_tasks, return_exceptions=True)

        assert result == ["val"]

    @pytest.mark.asyncio
    async def test_async_handler_task_cleaned_up_after_done(self):
        """异步 handler 完成后应从 _handler_tasks 中移除"""
        client = _make_client()
        client._logger = MagicMock()

        async def handler(data):
            pass

        client.on("cleanup", handler)
        client.emit("cleanup", None)

        assert len(client._handler_tasks) == 1

        await asyncio.gather(*client._handler_tasks, return_exceptions=True)

        # done_callback 会在 gather 返回后的事件循环迭代中执行
        await asyncio.sleep(0)
        assert len(client._handler_tasks) == 0
