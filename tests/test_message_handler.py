"""
测试消息处理器：video 消息路由及已有类型不回归
"""

from unittest.mock import MagicMock, call

from wecom_aibot_sdk.message_handler import MessageHandler
from wecom_aibot_sdk.types import WsCmd


def _make_msg_frame(msgtype: str, cmd: str = WsCmd.CALLBACK) -> dict:
    """构造一个消息推送帧"""
    return {
        "cmd": cmd,
        "headers": {"req_id": "test_req_001"},
        "body": {"msgtype": msgtype, "content": "hello"},
    }


def _make_event_frame(eventtype: str) -> dict:
    """构造一个事件推送帧"""
    return {
        "cmd": WsCmd.EVENT_CALLBACK,
        "headers": {"req_id": "test_req_002"},
        "body": {
            "msgtype": "event",
            "event": {"eventtype": eventtype},
        },
    }


class TestVideoMessageRouting:
    """video 消息类型路由测试"""

    def setup_method(self):
        self.logger = MagicMock()
        self.handler = MessageHandler(self.logger)
        self.emitter = MagicMock()

    def test_video_message_emits_specific_event(self):
        frame = _make_msg_frame("video")
        self.handler.handle_frame(frame, self.emitter)

        # 应触发通用 message 事件和特定 message.video 事件
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.video", frame)

    def test_video_message_emit_call_count(self):
        frame = _make_msg_frame("video")
        self.handler.handle_frame(frame, self.emitter)

        # 应恰好触发 2 次 emit：message + message.video
        assert self.emitter.emit.call_count == 2


class TestExistingMessageTypes:
    """已有消息类型不回归测试"""

    def setup_method(self):
        self.logger = MagicMock()
        self.handler = MessageHandler(self.logger)
        self.emitter = MagicMock()

    def test_text_message(self):
        frame = _make_msg_frame("text")
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.text", frame)

    def test_image_message(self):
        frame = _make_msg_frame("image")
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.image", frame)

    def test_file_message(self):
        frame = _make_msg_frame("file")
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.file", frame)

    def test_voice_message(self):
        frame = _make_msg_frame("voice")
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.voice", frame)

    def test_mixed_message(self):
        frame = _make_msg_frame("mixed")
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_any_call("message", frame)
        self.emitter.emit.assert_any_call("message.mixed", frame)


class TestUnknownMessageType:
    """未知消息类型测试"""

    def setup_method(self):
        self.logger = MagicMock()
        self.handler = MessageHandler(self.logger)
        self.emitter = MagicMock()

    def test_unknown_type_emits_only_generic_message(self):
        frame = _make_msg_frame("unknown_type_xyz")
        self.handler.handle_frame(frame, self.emitter)

        # 应触发通用 message 事件
        self.emitter.emit.assert_any_call("message", frame)
        # 不应触发特定事件（只有 1 次 emit 调用）
        assert self.emitter.emit.call_count == 1

    def test_unknown_type_logs_debug(self):
        frame = _make_msg_frame("unknown_type_xyz")
        self.handler.handle_frame(frame, self.emitter)
        self.logger.debug.assert_called()


class TestEventCallback:
    """事件回调路由测试"""

    def setup_method(self):
        self.logger = MagicMock()
        self.handler = MessageHandler(self.logger)
        self.emitter = MagicMock()

    def test_event_callback_emits_event_and_specific(self):
        frame = _make_event_frame("enter_chat")
        self.handler.handle_frame(frame, self.emitter)

        self.emitter.emit.assert_any_call("event", frame)
        self.emitter.emit.assert_any_call("event.enter_chat", frame)

    def test_event_callback_template_card(self):
        frame = _make_event_frame("template_card_event")
        self.handler.handle_frame(frame, self.emitter)

        self.emitter.emit.assert_any_call("event", frame)
        self.emitter.emit.assert_any_call("event.template_card_event", frame)

    def test_invalid_frame_no_body(self):
        """无 body 的帧不触发任何事件"""
        frame = {"cmd": WsCmd.CALLBACK, "headers": {"req_id": "x"}}
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_not_called()

    def test_invalid_frame_no_msgtype(self):
        """body 中无 msgtype 的帧不触发任何事件"""
        frame = {
            "cmd": WsCmd.CALLBACK,
            "headers": {"req_id": "x"},
            "body": {"content": "hello"},
        }
        self.handler.handle_frame(frame, self.emitter)
        self.emitter.emit.assert_not_called()
