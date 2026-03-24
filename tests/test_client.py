"""
测试 WSClient 新构造参数向 WsConnectionManager 的传递
"""

from unittest.mock import patch, MagicMock


class TestClientParameterPassthrough:
    """验证 WSClient 新参数正确传递给 WsConnectionManager"""

    @patch("wecom_aibot_sdk.client.WsConnectionManager")
    @patch("wecom_aibot_sdk.client.WeComApiClient")
    def test_max_auth_failure_attempts_passthrough(self, mock_api_cls, mock_ws_cls):
        from wecom_aibot_sdk.client import WSClient

        WSClient(
            bot_id="bot1",
            secret="secret1",
            max_auth_failure_attempts=3,
        )

        mock_ws_cls.assert_called_once()
        _, kwargs = mock_ws_cls.call_args
        assert kwargs["max_auth_failure_attempts"] == 3

    @patch("wecom_aibot_sdk.client.WsConnectionManager")
    @patch("wecom_aibot_sdk.client.WeComApiClient")
    def test_max_reply_queue_size_passthrough(self, mock_api_cls, mock_ws_cls):
        from wecom_aibot_sdk.client import WSClient

        WSClient(
            bot_id="bot1",
            secret="secret1",
            max_reply_queue_size=200,
        )

        mock_ws_cls.assert_called_once()
        _, kwargs = mock_ws_cls.call_args
        assert kwargs["max_reply_queue_size"] == 200

    @patch("wecom_aibot_sdk.client.WsConnectionManager")
    @patch("wecom_aibot_sdk.client.WeComApiClient")
    def test_default_values_passthrough(self, mock_api_cls, mock_ws_cls):
        from wecom_aibot_sdk.client import WSClient

        WSClient(bot_id="bot1", secret="secret1")

        mock_ws_cls.assert_called_once()
        _, kwargs = mock_ws_cls.call_args
        assert kwargs["max_auth_failure_attempts"] == 5
        assert kwargs["max_reply_queue_size"] == 500

    @patch("wecom_aibot_sdk.client.WsConnectionManager")
    @patch("wecom_aibot_sdk.client.WeComApiClient")
    def test_all_ws_params_passthrough(self, mock_api_cls, mock_ws_cls):
        from wecom_aibot_sdk.client import WSClient

        WSClient(
            bot_id="bot1",
            secret="secret1",
            reconnect_interval=2000,
            max_reconnect_attempts=20,
            max_auth_failure_attempts=8,
            max_reply_queue_size=1000,
            heartbeat_interval=60000,
            ws_url="wss://custom.url",
        )

        mock_ws_cls.assert_called_once()
        call_args = mock_ws_cls.call_args

        # Positional args
        positional = call_args[0]
        # positional[0] is logger, [1] heartbeat_interval, [2] reconnect_interval,
        # [3] max_reconnect_attempts, [4] ws_url, [5] ws_options
        assert positional[1] == 60000  # heartbeat_interval
        assert positional[2] == 2000   # reconnect_interval
        assert positional[3] == 20     # max_reconnect_attempts
        assert positional[4] == "wss://custom.url"  # ws_url

        # Keyword args
        kwargs = call_args[1]
        assert kwargs["max_reply_queue_size"] == 1000
        assert kwargs["max_auth_failure_attempts"] == 8

    @patch("wecom_aibot_sdk.client.WsConnectionManager")
    @patch("wecom_aibot_sdk.client.WeComApiClient")
    def test_set_credentials_called(self, mock_api_cls, mock_ws_cls):
        """验证 set_credentials 被调用且传入正确的 bot_id 和 secret"""
        from wecom_aibot_sdk.client import WSClient

        mock_mgr_instance = MagicMock()
        mock_ws_cls.return_value = mock_mgr_instance

        WSClient(bot_id="my_bot", secret="my_secret")

        mock_mgr_instance.set_credentials.assert_called_once()
        args = mock_mgr_instance.set_credentials.call_args[0]
        assert args[0] == "my_bot"
        assert args[1] == "my_secret"
