"""
类型定义模块

对应 Node.js SDK 中 src/types/ 目录下的所有类型定义。
Python 中使用 TypedDict、Enum 和 dataclass 代替 TypeScript interface。
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, TypedDict, NotRequired


# ========== 通用基础类型 (common.ts) ==========

class LoggerProtocol:
    """日志接口协议（仅用于类型提示）"""

    def debug(self, message: str, *args: Any) -> None: ...
    def info(self, message: str, *args: Any) -> None: ...
    def warn(self, message: str, *args: Any) -> None: ...
    def error(self, message: str, *args: Any) -> None: ...


# ========== 配置选项类型 (config.ts) ==========

class WSClientOptions(TypedDict, total=False):
    """WSClient 配置选项"""
    bot_id: str                    # 机器人 ID（企业微信后台获取）—— 必填
    secret: str                    # 机器人 Secret（企业微信后台获取）—— 必填
    reconnect_interval: int        # 重连基础延迟（毫秒），默认 1000
    max_reconnect_attempts: int    # 最大重连次数，默认 10，-1 无限重连
    heartbeat_interval: int        # 心跳间隔（毫秒），默认 30000
    request_timeout: int           # 请求超时时间（毫秒），默认 10000
    ws_url: str                    # 自定义 WebSocket 连接地址
    logger: Any                    # 自定义日志实例


# ========== WebSocket 命令常量 (api.ts) ==========

class WsCmd:
    """WebSocket 命令类型常量"""
    # 开发者 → 企业微信
    SUBSCRIBE = "aibot_subscribe"
    HEARTBEAT = "ping"
    RESPONSE = "aibot_respond_msg"
    RESPONSE_WELCOME = "aibot_respond_welcome_msg"
    RESPONSE_UPDATE = "aibot_respond_update_msg"
    SEND_MSG = "aibot_send_msg"

    # 企业微信 → 开发者
    CALLBACK = "aibot_msg_callback"
    EVENT_CALLBACK = "aibot_event_callback"


# ========== WebSocket 帧结构 (api.ts) ==========

class WsFrameHeaders(TypedDict):
    """WebSocket 帧头"""
    req_id: str


class WsFrame(TypedDict, total=False):
    """WebSocket 帧结构"""
    cmd: str
    headers: WsFrameHeaders
    body: dict[str, Any]
    errcode: int
    errmsg: str


# ========== 回复消息子结构 (api.ts) ==========

class ReplyMsgItem(TypedDict):
    """回复消息中的图文混排子项"""
    msgtype: Literal["image"]
    image: dict[str, str]  # {"base64": str, "md5": str}


class ReplyFeedback(TypedDict):
    """回复消息中的反馈信息"""
    id: str


# ========== 流式回复消息体 (api.ts) ==========

class StreamContent(TypedDict, total=False):
    """流式回复消息中的 stream 子结构"""
    id: str
    finish: bool
    content: str
    msg_item: list[ReplyMsgItem]
    feedback: ReplyFeedback


class StreamReplyBody(TypedDict):
    """流式回复消息体"""
    msgtype: Literal["stream"]
    stream: StreamContent


# ========== 欢迎语回复消息体 (api.ts) ==========

class WelcomeTextReplyBody(TypedDict):
    """欢迎语回复消息体（文本类型）"""
    msgtype: Literal["text"]
    text: dict[str, str]  # {"content": str}


class WelcomeTemplateCardReplyBody(TypedDict):
    """欢迎语回复消息体（模板卡片类型）"""
    msgtype: Literal["template_card"]
    template_card: dict[str, Any]


WelcomeReplyBody = WelcomeTextReplyBody | WelcomeTemplateCardReplyBody


# ========== 模板卡片类型 (api.ts) ==========

class TemplateCardType(str, Enum):
    """卡片类型枚举"""
    TEXT_NOTICE = "text_notice"
    NEWS_NOTICE = "news_notice"
    BUTTON_INTERACTION = "button_interaction"
    VOTE_INTERACTION = "vote_interaction"
    MULTIPLE_INTERACTION = "multiple_interaction"


# ========== 模板卡片回复消息体 (api.ts) ==========

class TemplateCardReplyBody(TypedDict):
    """模板卡片回复消息体"""
    msgtype: Literal["template_card"]
    template_card: dict[str, Any]


class StreamWithTemplateCardReplyBody(TypedDict, total=False):
    """流式消息 + 模板卡片组合回复消息体"""
    msgtype: Literal["stream_with_template_card"]
    stream: StreamContent
    template_card: dict[str, Any]


class UpdateTemplateCardBody(TypedDict, total=False):
    """更新模板卡片消息体"""
    response_type: Literal["update_template_card"]
    userids: list[str]
    template_card: dict[str, Any]


# ========== 主动发送消息体 (api.ts) ==========

class SendMarkdownMsgBody(TypedDict):
    """主动发送 Markdown 消息体"""
    msgtype: Literal["markdown"]
    markdown: dict[str, str]  # {"content": str}


class SendTemplateCardMsgBody(TypedDict):
    """主动发送模板卡片消息体"""
    msgtype: Literal["template_card"]
    template_card: dict[str, Any]


SendMsgBody = SendMarkdownMsgBody | SendTemplateCardMsgBody


# ========== 消息类型 (message.ts) ==========

class MessageType(str, Enum):
    """消息类型枚举"""
    TEXT = "text"
    IMAGE = "image"
    MIXED = "mixed"
    VOICE = "voice"
    FILE = "file"


# ========== 事件类型 (event.ts) ==========

class EventType(str, Enum):
    """事件类型枚举"""
    ENTER_CHAT = "enter_chat"
    TEMPLATE_CARD_EVENT = "template_card_event"
    FEEDBACK_EVENT = "feedback_event"
