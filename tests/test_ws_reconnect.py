"""
测试 WebSocket 重连策略核心逻辑（重点测试文件）

覆盖：构造参数、认证成功/失败计数器、_schedule_reconnect 分支、
connect/disconnect 取消挂起 task、_receive_loop 中 ws 置 None
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets.exceptions

from wecom_aibot_sdk.ws import WsConnectionManager
from wecom_aibot_sdk.types import WsCmd, WSAuthFailureError, WSReconnectExhaustedError


def _make_manager(**kwargs) -> WsConnectionManager:
    """创建带 mock logger 的 WsConnectionManager"""
    logger = MagicMock()
    return WsConnectionManager(logger=logger, **kwargs)


# ========== 构造函数参数测试 ==========


class TestConstructorDefaults:
    """构造函数默认值测试"""

    def test_max_reply_queue_size_default(self):
        mgr = _make_manager()
        assert mgr._max_reply_queue_size == 500

    def test_max_reply_queue_size_custom(self):
        mgr = _make_manager(max_reply_queue_size=100)
        assert mgr._max_reply_queue_size == 100

    def test_max_auth_failure_attempts_default(self):
        mgr = _make_manager()
        assert mgr._max_auth_failure_attempts == 5

    def test_max_auth_failure_attempts_custom(self):
        mgr = _make_manager(max_auth_failure_attempts=3)
        assert mgr._max_auth_failure_attempts == 3

    def test_initial_counters_are_zero(self):
        mgr = _make_manager()
        assert mgr._reconnect_attempts == 0
        assert mgr._auth_failure_attempts == 0

    def test_last_close_was_auth_failure_initially_false(self):
        mgr = _make_manager()
        assert mgr._last_close_was_auth_failure is False


# ========== 认证成功重置计数器测试 ==========


class TestAuthSuccessResetsCounters:
    """认证成功后重置重连和认证失败计数器"""

    @pytest.fixture
    def mgr(self):
        mgr = _make_manager()
        mgr._ws = AsyncMock()
        # 模拟之前有一些失败计数
        mgr._reconnect_attempts = 3
        mgr._auth_failure_attempts = 2
        return mgr

    async def test_auth_success_resets_reconnect_attempts(self, mgr):
        auth_success_frame = {
            "headers": {"req_id": f"{WsCmd.SUBSCRIBE}_12345"},
            "errcode": 0,
        }
        await mgr._handle_frame(auth_success_frame)
        assert mgr._reconnect_attempts == 0

    async def test_auth_success_resets_auth_failure_attempts(self, mgr):
        auth_success_frame = {
            "headers": {"req_id": f"{WsCmd.SUBSCRIBE}_12345"},
            "errcode": 0,
        }
        await mgr._handle_frame(auth_success_frame)
        assert mgr._auth_failure_attempts == 0


# ========== 认证失败触发重连测试 ==========


class TestAuthFailureTrigger:
    """认证失败设置标记并关闭 ws"""

    @pytest.fixture
    def mgr(self):
        mgr = _make_manager()
        mgr._ws = AsyncMock()
        mgr.on_error = MagicMock()
        return mgr

    async def test_auth_failure_sets_flag(self, mgr):
        auth_fail_frame = {
            "headers": {"req_id": f"{WsCmd.SUBSCRIBE}_12345"},
            "errcode": 40001,
            "errmsg": "invalid secret",
        }
        await mgr._handle_frame(auth_fail_frame)
        assert mgr._last_close_was_auth_failure is True

    async def test_auth_failure_closes_ws(self, mgr):
        auth_fail_frame = {
            "headers": {"req_id": f"{WsCmd.SUBSCRIBE}_12345"},
            "errcode": 40001,
            "errmsg": "invalid secret",
        }
        await mgr._handle_frame(auth_fail_frame)
        mgr._ws.close.assert_awaited_once()

    async def test_auth_failure_calls_on_error(self, mgr):
        auth_fail_frame = {
            "headers": {"req_id": f"{WsCmd.SUBSCRIBE}_12345"},
            "errcode": 40001,
            "errmsg": "invalid secret",
        }
        await mgr._handle_frame(auth_fail_frame)
        mgr.on_error.assert_called_once()
        error_arg = mgr.on_error.call_args[0][0]
        assert isinstance(error_arg, Exception)


# ========== _schedule_reconnect 认证失败分支测试 ==========


class TestScheduleReconnectAuthFailure:
    """_schedule_reconnect 认证失败分支"""

    async def test_auth_failure_increments_counter(self):
        mgr = _make_manager(max_auth_failure_attempts=5)
        mgr._last_close_was_auth_failure = True
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        assert mgr._auth_failure_attempts == 1

    async def test_auth_failure_resets_flag(self):
        mgr = _make_manager(max_auth_failure_attempts=5)
        mgr._last_close_was_auth_failure = True
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        assert mgr._last_close_was_auth_failure is False

    async def test_auth_failure_creates_reconnect_task(self):
        mgr = _make_manager(max_auth_failure_attempts=5)
        mgr._last_close_was_auth_failure = True
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task
            await mgr._schedule_reconnect()

        assert mgr._reconnect_task is mock_task

    async def test_auth_failure_exhausted_calls_on_error(self):
        mgr = _make_manager(max_auth_failure_attempts=2)
        mgr._last_close_was_auth_failure = True
        mgr._auth_failure_attempts = 1  # Next increment will reach 2
        mgr.on_error = MagicMock()

        await mgr._schedule_reconnect()

        mgr.on_error.assert_called_once()
        error_arg = mgr.on_error.call_args[0][0]
        assert isinstance(error_arg, WSAuthFailureError)

    async def test_auth_failure_exhausted_no_reconnect_task(self):
        mgr = _make_manager(max_auth_failure_attempts=2)
        mgr._last_close_was_auth_failure = True
        mgr._auth_failure_attempts = 1
        mgr.on_error = MagicMock()

        await mgr._schedule_reconnect()

        # Should not create a reconnect task when exhausted
        assert mgr._reconnect_task is None

    async def test_auth_failure_unlimited_never_exhausts(self):
        """max_auth_failure_attempts = -1 means unlimited"""
        mgr = _make_manager(max_auth_failure_attempts=-1)
        mgr._last_close_was_auth_failure = True
        mgr._auth_failure_attempts = 20  # Use a reasonable number to avoid float overflow
        mgr.on_reconnecting = MagicMock()
        mgr.on_error = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        mgr.on_error.assert_not_called()
        assert mgr._auth_failure_attempts == 21


# ========== _schedule_reconnect 连接断开分支测试 ==========


class TestScheduleReconnectDisconnect:
    """_schedule_reconnect 连接断开分支"""

    async def test_disconnect_increments_reconnect_attempts(self):
        mgr = _make_manager(max_reconnect_attempts=10)
        mgr._last_close_was_auth_failure = False
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        assert mgr._reconnect_attempts == 1

    async def test_disconnect_creates_reconnect_task(self):
        mgr = _make_manager(max_reconnect_attempts=10)
        mgr._last_close_was_auth_failure = False
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task
            await mgr._schedule_reconnect()

        assert mgr._reconnect_task is mock_task

    async def test_disconnect_exhausted_calls_on_error(self):
        mgr = _make_manager(max_reconnect_attempts=3)
        mgr._last_close_was_auth_failure = False
        mgr._reconnect_attempts = 2  # Next increment will reach 3
        mgr.on_error = MagicMock()

        await mgr._schedule_reconnect()

        mgr.on_error.assert_called_once()
        error_arg = mgr.on_error.call_args[0][0]
        assert isinstance(error_arg, WSReconnectExhaustedError)

    async def test_disconnect_exhausted_no_reconnect_task(self):
        mgr = _make_manager(max_reconnect_attempts=3)
        mgr._last_close_was_auth_failure = False
        mgr._reconnect_attempts = 2
        mgr.on_error = MagicMock()

        await mgr._schedule_reconnect()

        assert mgr._reconnect_task is None

    async def test_disconnect_unlimited_never_exhausts(self):
        """max_reconnect_attempts = -1 means unlimited"""
        mgr = _make_manager(max_reconnect_attempts=-1)
        mgr._last_close_was_auth_failure = False
        mgr._reconnect_attempts = 20  # Use a reasonable number to avoid float overflow
        mgr.on_reconnecting = MagicMock()
        mgr.on_error = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        mgr.on_error.assert_not_called()
        assert mgr._reconnect_attempts == 21


# ========== connect() 取消挂起重连 task 测试 ==========


class TestConnectCancelsReconnect:
    """connect() 应取消挂起的重连 task"""

    async def test_connect_cancels_pending_reconnect_task(self):
        mgr = _make_manager()
        mgr.set_credentials("bot1", "secret1")

        # 设置一个 mock 的 reconnect_task
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mgr._reconnect_task = mock_task

        # mock websockets.connect 抛出异常以避免建立真实连接
        with patch("websockets.asyncio.client.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = ConnectionError("test")
            # mock _schedule_reconnect 以避免再次创建 task
            with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock):
                await mgr.connect()

        mock_task.cancel.assert_called_once()

    async def test_connect_skips_done_reconnect_task(self):
        mgr = _make_manager()
        mgr.set_credentials("bot1", "secret1")

        mock_task = MagicMock()
        mock_task.done.return_value = True  # Already done
        mgr._reconnect_task = mock_task

        with patch("websockets.asyncio.client.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = ConnectionError("test")
            with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock):
                await mgr.connect()

        mock_task.cancel.assert_not_called()


# ========== disconnect() 取消挂起重连 task 测试 ==========


class TestDisconnectCancelsReconnect:
    """disconnect() 应取消挂起的重连 task"""

    async def test_disconnect_cancels_pending_reconnect_task(self):
        mgr = _make_manager()

        mock_task = MagicMock()
        mock_task.done.return_value = False
        mgr._reconnect_task = mock_task

        await mgr.disconnect()

        mock_task.cancel.assert_called_once()

    async def test_disconnect_skips_done_reconnect_task(self):
        mgr = _make_manager()

        mock_task = MagicMock()
        mock_task.done.return_value = True
        mgr._reconnect_task = mock_task

        await mgr.disconnect()

        mock_task.cancel.assert_not_called()

    async def test_disconnect_sets_manual_close(self):
        mgr = _make_manager()
        await mgr.disconnect()
        assert mgr._is_manual_close is True


# ========== _receive_loop 中 ws 置 None 测试 ==========


class TestReceiveLoopSetsWsNone:
    """_receive_loop 在 ConnectionClosed 时将 self._ws 设为 None"""

    async def test_connection_closed_sets_ws_to_none(self):
        mgr = _make_manager()
        mgr.on_disconnected = MagicMock()

        # 创建一个 mock ws，迭代时抛出 ConnectionClosed
        mock_ws = AsyncMock()
        mock_ws.__aiter__ = MagicMock(
            return_value=AsyncMock(
                __anext__=AsyncMock(
                    side_effect=websockets.exceptions.ConnectionClosed(None, None)
                )
            )
        )
        mgr._ws = mock_ws

        # mock _schedule_reconnect to prevent real reconnect logic
        with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock):
            await mgr._receive_loop()

        assert mgr._ws is None

    async def test_connection_closed_calls_on_disconnected(self):
        mgr = _make_manager()
        mgr.on_disconnected = MagicMock()

        mock_ws = AsyncMock()
        mock_ws.__aiter__ = MagicMock(
            return_value=AsyncMock(
                __anext__=AsyncMock(
                    side_effect=websockets.exceptions.ConnectionClosed(None, None)
                )
            )
        )
        mgr._ws = mock_ws

        with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock):
            await mgr._receive_loop()

        mgr.on_disconnected.assert_called_once()

    async def test_connection_closed_calls_schedule_reconnect(self):
        mgr = _make_manager()
        mgr.on_disconnected = MagicMock()
        mgr._is_manual_close = False

        mock_ws = AsyncMock()
        mock_ws.__aiter__ = MagicMock(
            return_value=AsyncMock(
                __anext__=AsyncMock(
                    side_effect=websockets.exceptions.ConnectionClosed(None, None)
                )
            )
        )
        mgr._ws = mock_ws

        with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock) as mock_reconnect:
            await mgr._receive_loop()

        mock_reconnect.assert_awaited_once()

    async def test_connection_closed_no_reconnect_when_manual_close(self):
        mgr = _make_manager()
        mgr.on_disconnected = MagicMock()
        mgr._is_manual_close = True

        mock_ws = AsyncMock()
        mock_ws.__aiter__ = MagicMock(
            return_value=AsyncMock(
                __anext__=AsyncMock(
                    side_effect=websockets.exceptions.ConnectionClosed(None, None)
                )
            )
        )
        mgr._ws = mock_ws

        with patch.object(mgr, "_schedule_reconnect", new_callable=AsyncMock) as mock_reconnect:
            await mgr._receive_loop()

        mock_reconnect.assert_not_awaited()


# ========== on_reconnecting 回调测试 ==========


class TestOnReconnectingCallback:
    """验证 on_reconnecting 回调在重连调度时被调用"""

    async def test_auth_failure_calls_on_reconnecting(self):
        mgr = _make_manager(max_auth_failure_attempts=5)
        mgr._last_close_was_auth_failure = True
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        mgr.on_reconnecting.assert_called_once_with(1)

    async def test_disconnect_calls_on_reconnecting(self):
        mgr = _make_manager(max_reconnect_attempts=10)
        mgr._last_close_was_auth_failure = False
        mgr.on_reconnecting = MagicMock()

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
            await mgr._schedule_reconnect()

        mgr.on_reconnecting.assert_called_once_with(1)
