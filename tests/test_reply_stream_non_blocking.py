"""
测试 _pending_acks 相关辅助方法与 reply_stream_non_blocking 跳过逻辑

覆盖：
  - WsConnectionManager.has_pending_ack
  - WSClient.has_pending_reply_ack
  - WSClient.reply_stream_non_blocking
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wecom_aibot_sdk.ws import WsConnectionManager


def _make_manager(**kwargs) -> WsConnectionManager:
    """创建带 mock logger 的 WsConnectionManager"""
    logger = MagicMock()
    return WsConnectionManager(logger=logger, **kwargs)


def _make_client():
    """创建 WSClient（patch 掉两个外部依赖）"""
    with patch("wecom_aibot_sdk.client.WsConnectionManager"), \
         patch("wecom_aibot_sdk.client.WeComApiClient"):
        from wecom_aibot_sdk.client import WSClient
        return WSClient(bot_id="test", secret="test")


# ========== WsConnectionManager.has_pending_ack ==========


class TestHasPendingAck:
    """WsConnectionManager.has_pending_ack 行为测试"""

    def test_returns_false_for_unknown_req_id(self):
        """未知 req_id 应返回 False"""
        mgr = _make_manager()
        assert mgr.has_pending_ack("does_not_exist") is False

    def test_returns_true_after_entry_inserted(self):
        """插入伪条目后应返回 True"""
        mgr = _make_manager()
        mgr._pending_acks["req_42"] = {"future": MagicMock()}
        assert mgr.has_pending_ack("req_42") is True

    def test_returns_false_after_entry_popped(self):
        """条目被弹出后应再次返回 False"""
        mgr = _make_manager()
        mgr._pending_acks["req_42"] = {"future": MagicMock()}
        assert mgr.has_pending_ack("req_42") is True

        mgr._pending_acks.pop("req_42")
        assert mgr.has_pending_ack("req_42") is False


# ========== WSClient.has_pending_reply_ack ==========


class TestHasPendingReplyAck:
    """WSClient.has_pending_reply_ack 行为测试"""

    @pytest.fixture
    def client(self):
        c = _make_client()
        c._ws_manager = MagicMock()
        return c

    def test_reads_req_id_from_headers(self, client):
        """正确从 frame['headers']['req_id'] 中读取并透传给 ws_manager"""
        client._ws_manager.has_pending_ack.return_value = True
        frame = {"headers": {"req_id": "req_xyz"}}

        result = client.has_pending_reply_ack(frame)

        assert result is True
        client._ws_manager.has_pending_ack.assert_called_once_with("req_xyz")

    def test_returns_value_from_ws_manager_false(self, client):
        """返回 ws_manager.has_pending_ack 的值（False 路径）"""
        client._ws_manager.has_pending_ack.return_value = False
        frame = {"headers": {"req_id": "req_xyz"}}

        result = client.has_pending_reply_ack(frame)

        assert result is False
        client._ws_manager.has_pending_ack.assert_called_once_with("req_xyz")

    def test_missing_headers_returns_false(self, client):
        """缺少 'headers' 键应返回 False，不抛异常"""
        frame = {}  # no 'headers' key

        result = client.has_pending_reply_ack(frame)

        assert result is False
        client._ws_manager.has_pending_ack.assert_not_called()

    def test_missing_req_id_in_headers_returns_false(self, client):
        """headers 中缺少 'req_id' 应返回 False"""
        frame = {"headers": {}}  # has headers, no req_id

        result = client.has_pending_reply_ack(frame)

        assert result is False
        client._ws_manager.has_pending_ack.assert_not_called()


# ========== WSClient.reply_stream_non_blocking ==========


class TestReplyStreamNonBlocking:
    """WSClient.reply_stream_non_blocking 行为测试"""

    @pytest.fixture
    def client(self):
        c = _make_client()
        c._ws_manager = MagicMock()
        return c

    async def test_skips_when_not_finish_and_pending_ack(self, client):
        """finish=False 且仍有 pending ack 时直接返回 'skipped'，不调用 reply_stream"""
        client._ws_manager.has_pending_ack.return_value = True
        client.reply_stream = AsyncMock(return_value=MagicMock(name="WsFrame"))

        frame = {"headers": {"req_id": "req_xyz"}}
        result = await client.reply_stream_non_blocking(
            frame, stream_id="s1", content="hello", finish=False,
        )

        assert result == "skipped"
        assert client.reply_stream.call_count == 0

    async def test_sends_when_not_finish_and_no_pending_ack(self, client):
        """finish=False 且无 pending ack 时调用一次 reply_stream，并返回其结果"""
        client._ws_manager.has_pending_ack.return_value = False
        sentinel = MagicMock(name="WsFrameSentinel")
        client.reply_stream = AsyncMock(return_value=sentinel)

        frame = {"headers": {"req_id": "req_xyz"}}
        result = await client.reply_stream_non_blocking(
            frame, stream_id="s1", content="hello", finish=False,
        )

        assert result is sentinel
        assert client.reply_stream.call_count == 1
        client.reply_stream.assert_awaited_once_with(
            frame, "s1", "hello", False, msg_item=None, feedback=None,
        )

    async def test_finish_true_bypasses_skip_when_pending_ack(self, client):
        """finish=True 时即使存在 pending ack 也仍然调用一次 reply_stream"""
        client._ws_manager.has_pending_ack.return_value = True
        sentinel = MagicMock(name="WsFrameSentinel")
        client.reply_stream = AsyncMock(return_value=sentinel)

        frame = {"headers": {"req_id": "req_xyz"}}
        result = await client.reply_stream_non_blocking(
            frame, stream_id="s1", content="bye", finish=True,
        )

        assert result is sentinel
        assert client.reply_stream.call_count == 1

    async def test_finish_true_no_pending_ack_calls_reply_stream(self, client):
        """finish=True 且无 pending ack 时正常调用一次 reply_stream"""
        client._ws_manager.has_pending_ack.return_value = False
        sentinel = MagicMock(name="WsFrameSentinel")
        client.reply_stream = AsyncMock(return_value=sentinel)

        frame = {"headers": {"req_id": "req_xyz"}}
        result = await client.reply_stream_non_blocking(
            frame, stream_id="s1", content="bye", finish=True,
        )

        assert result is sentinel
        assert client.reply_stream.call_count == 1

    async def test_msg_item_and_feedback_forwarded(self, client):
        """msg_item 和 feedback 参数应正确转发给 reply_stream"""
        client._ws_manager.has_pending_ack.return_value = False
        client.reply_stream = AsyncMock(return_value=MagicMock(name="WsFrame"))

        frame = {"headers": {"req_id": "req_xyz"}}
        msg_item = [{"image": {"base64": "iVBOR..."}}]
        feedback = {"enable": True}

        await client.reply_stream_non_blocking(
            frame,
            "s1",
            "done",
            finish=True,
            msg_item=msg_item,
            feedback=feedback,
        )

        client.reply_stream.assert_awaited_once_with(
            frame, "s1", "done", True, msg_item=msg_item, feedback=feedback,
        )
