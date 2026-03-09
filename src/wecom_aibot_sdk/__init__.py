"""
企业微信智能机器人 Python SDK

基于 WebSocket 长连接通道，提供消息收发、流式回复、模板卡片、事件回调、文件下载解密等核心能力。
"""

from wecom_aibot_sdk.client import WSClient
from wecom_aibot_sdk.api import WeComApiClient
from wecom_aibot_sdk.ws import WsConnectionManager
from wecom_aibot_sdk.message_handler import MessageHandler
from wecom_aibot_sdk.crypto import decrypt_file
from wecom_aibot_sdk.logger import DefaultLogger
from wecom_aibot_sdk.utils import generate_req_id, generate_random_string
from wecom_aibot_sdk.types import (
    MessageType,
    EventType,
    TemplateCardType,
    WsCmd,
)

__all__ = [
    "WSClient",
    "WeComApiClient",
    "WsConnectionManager",
    "MessageHandler",
    "decrypt_file",
    "DefaultLogger",
    "generate_req_id",
    "generate_random_string",
    "MessageType",
    "EventType",
    "TemplateCardType",
    "WsCmd",
]
