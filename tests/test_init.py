"""
测试包导出完整性
验证新增的错误类可以从 wecom_aibot_sdk 包直接导入
"""

import wecom_aibot_sdk


class TestPackageExports:
    """验证包的公开 API 完整性"""

    def test_ws_auth_failure_error_importable(self):
        """WSAuthFailureError 可从包直接导入"""
        assert hasattr(wecom_aibot_sdk, "WSAuthFailureError")
        from wecom_aibot_sdk import WSAuthFailureError

        assert WSAuthFailureError is not None

    def test_ws_reconnect_exhausted_error_importable(self):
        """WSReconnectExhaustedError 可从包直接导入"""
        assert hasattr(wecom_aibot_sdk, "WSReconnectExhaustedError")
        from wecom_aibot_sdk import WSReconnectExhaustedError

        assert WSReconnectExhaustedError is not None

    def test_error_classes_in_all(self):
        """错误类列在 __all__ 中"""
        assert "WSAuthFailureError" in wecom_aibot_sdk.__all__
        assert "WSReconnectExhaustedError" in wecom_aibot_sdk.__all__

    def test_message_type_importable(self):
        """MessageType 可从包直接导入"""
        from wecom_aibot_sdk import MessageType

        assert MessageType is not None

    def test_core_classes_importable(self):
        """核心类仍然可以正常导入"""
        from wecom_aibot_sdk import WSClient, WsConnectionManager, MessageHandler

        assert WSClient is not None
        assert WsConnectionManager is not None
        assert MessageHandler is not None

    def test_all_exports_are_accessible(self):
        """__all__ 中列出的所有名称都可以访问"""
        for name in wecom_aibot_sdk.__all__:
            assert hasattr(wecom_aibot_sdk, name), f"{name} listed in __all__ but not accessible"
