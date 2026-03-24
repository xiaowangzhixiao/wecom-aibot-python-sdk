"""
测试类型定义：新增的 VIDEO 枚举值和错误类
"""

from wecom_aibot_sdk.types import MessageType, WSAuthFailureError, WSReconnectExhaustedError


class TestMessageTypeVideo:
    """MessageType.VIDEO 枚举测试"""

    def test_video_enum_exists(self):
        assert hasattr(MessageType, "VIDEO")

    def test_video_enum_value(self):
        assert MessageType.VIDEO == "video"
        assert MessageType.VIDEO.value == "video"

    def test_video_is_member_of_message_type(self):
        assert isinstance(MessageType.VIDEO, MessageType)


class TestWSAuthFailureError:
    """WSAuthFailureError 错误类测试"""

    def test_inherits_from_exception(self):
        assert issubclass(WSAuthFailureError, Exception)

    def test_code_attribute(self):
        assert WSAuthFailureError.code == "WS_AUTH_FAILURE_EXHAUSTED"

    def test_message_contains_max_attempts(self):
        err = WSAuthFailureError(5)
        assert "5" in str(err)
        assert "Max auth failure attempts exceeded" in str(err)

    def test_instance_code(self):
        err = WSAuthFailureError(3)
        assert err.code == "WS_AUTH_FAILURE_EXHAUSTED"

    def test_can_be_raised_and_caught(self):
        try:
            raise WSAuthFailureError(10)
        except WSAuthFailureError as e:
            assert "10" in str(e)
        except Exception:
            raise AssertionError("WSAuthFailureError should be caught by its own type")


class TestWSReconnectExhaustedError:
    """WSReconnectExhaustedError 错误类测试"""

    def test_inherits_from_exception(self):
        assert issubclass(WSReconnectExhaustedError, Exception)

    def test_code_attribute(self):
        assert WSReconnectExhaustedError.code == "WS_RECONNECT_EXHAUSTED"

    def test_message_contains_max_attempts(self):
        err = WSReconnectExhaustedError(10)
        assert "10" in str(err)
        assert "Max reconnect attempts exceeded" in str(err)

    def test_instance_code(self):
        err = WSReconnectExhaustedError(7)
        assert err.code == "WS_RECONNECT_EXHAUSTED"

    def test_can_be_raised_and_caught(self):
        try:
            raise WSReconnectExhaustedError(10)
        except WSReconnectExhaustedError as e:
            assert "10" in str(e)
        except Exception:
            raise AssertionError("WSReconnectExhaustedError should be caught by its own type")
