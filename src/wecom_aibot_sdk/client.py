"""
WSClient 核心客户端
基于 EventEmitter 模式，提供连接管理、消息收发等功能
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from wecom_aibot_sdk.types import (
    WsCmd,
    WsFrame,
    ReplyMsgItem,
    ReplyFeedback,
)
from wecom_aibot_sdk.api import WeComApiClient
from wecom_aibot_sdk.ws import WsConnectionManager
from wecom_aibot_sdk.message_handler import MessageHandler
from wecom_aibot_sdk.crypto import decrypt_file
from wecom_aibot_sdk.logger import DefaultLogger
from wecom_aibot_sdk.utils import generate_req_id


class WSClient:
    """
    企业微信智能机器人 WebSocket 客户端

    核心客户端类，提供连接管理、消息收发等功能。
    使用事件监听模式（``on`` / ``emit``）处理消息和事件回调。
    """

    def __init__(
        self,
        bot_id: str,
        secret: str,
        *,
        reconnect_interval: int = 1000,
        max_reconnect_attempts: int = 10,
        heartbeat_interval: int = 30000,
        request_timeout: int = 10000,
        ws_url: str = "",
        logger: Any | None = None,
    ) -> None:
        self._logger = logger or DefaultLogger()
        self._started = False

        # 初始化 API 客户端（仅用于文件下载）
        self._api_client = WeComApiClient(self._logger, request_timeout)

        # 初始化 WebSocket 管理器
        self._ws_manager = WsConnectionManager(
            self._logger,
            heartbeat_interval,
            reconnect_interval,
            max_reconnect_attempts,
            ws_url or None,
        )
        self._ws_manager.set_credentials(bot_id, secret)

        # 初始化消息处理器
        self._message_handler = MessageHandler(self._logger)

        # 事件监听器
        self._listeners: dict[str, list[Callable]] = {}

        # 追踪所有 async handler 创建的 task，防止 GC 回收
        self._handler_tasks: set[asyncio.Task] = set()

        # 绑定 WebSocket 事件
        self._setup_ws_events()

    def _setup_ws_events(self) -> None:
        """设置 WebSocket 事件处理"""
        self._ws_manager.on_connected = lambda: self.emit("connected")
        self._ws_manager.on_authenticated = lambda: (
            self._logger.info("Authenticated"),
            self.emit("authenticated"),
        )
        self._ws_manager.on_disconnected = lambda reason: self.emit("disconnected", reason)
        self._ws_manager.on_reconnecting = lambda attempt: self.emit("reconnecting", attempt)
        self._ws_manager.on_error = lambda error: self.emit("error", error)
        self._ws_manager.on_message = lambda frame: self._message_handler.handle_frame(frame, self)

    # ========== 事件系统 ==========

    def on(self, event: str, handler: Callable) -> "WSClient":
        """
        注册事件监听器

        :param event: 事件名称
        :param handler: 事件处理函数（支持同步或异步）
        :returns: self，支持链式调用

        支持的事件:
            - ``connected`` — WebSocket 连接建立
            - ``authenticated`` — 认证成功
            - ``disconnected`` — 连接断开，参数 ``reason: str``
            - ``reconnecting`` — 正在重连，参数 ``attempt: int``
            - ``error`` — 发生错误，参数 ``error: Exception``
            - ``message`` — 收到消息（所有类型）
            - ``message.text`` — 收到文本消息
            - ``message.image`` — 收到图片消息
            - ``message.mixed`` — 收到图文混排消息
            - ``message.voice`` — 收到语音消息
            - ``message.file`` — 收到文件消息
            - ``event`` — 收到事件回调（所有事件类型）
            - ``event.enter_chat`` — 收到进入会话事件
            - ``event.template_card_event`` — 收到模板卡片事件
            - ``event.feedback_event`` — 收到用户反馈事件
        """
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(handler)
        return self

    def off(self, event: str, handler: Callable | None = None) -> "WSClient":
        """
        移除事件监听器

        :param event: 事件名称
        :param handler: 要移除的处理函数，为 None 时移除该事件所有监听器
        """
        if handler is None:
            self._listeners.pop(event, None)
        elif event in self._listeners:
            self._listeners[event] = [h for h in self._listeners[event] if h != handler]
        return self

    def emit(self, event: str, *args: Any) -> None:
        """触发事件"""
        handlers = self._listeners.get(event, [])
        for handler in handlers:
            try:
                result = handler(*args)
                # 如果 handler 是协程函数，将其包装为 task 并保存引用
                if asyncio.iscoroutine(result):
                    task = asyncio.create_task(result)
                    self._handler_tasks.add(task)
                    task.add_done_callback(self._handler_tasks.discard)
            except Exception as e:
                self._logger.error(f"Error in event handler for '{event}':", str(e))

    # ========== 连接管理 ==========

    async def connect(self) -> "WSClient":
        """
        建立 WebSocket 长连接

        SDK 使用内置默认地址建立连接，连接成功后自动发送认证帧（botId + secret）。

        :returns: self，支持链式调用
        """
        if self._started:
            self._logger.warn("Client already connected")
            return self

        self._logger.info("Establishing WebSocket connection...")
        self._started = True
        await self._ws_manager.connect()
        return self

    async def disconnect(self) -> None:
        """断开 WebSocket 连接"""
        if not self._started:
            self._logger.warn("Client not connected")
            return

        self._logger.info("Disconnecting...")
        self._started = False

        # 等待运行中的 async handler 完成（最多 30 秒）
        await self._wait_handler_tasks(timeout=30.0)

        await self._ws_manager.disconnect()
        await self._api_client.close()
        self._logger.info("Disconnected")

    async def _wait_handler_tasks(self, timeout: float = 30.0) -> None:
        """等待所有 async handler task 完成，超时后取消"""
        if not self._handler_tasks:
            return

        self._logger.info(
            f"Waiting for {len(self._handler_tasks)} handler task(s) to complete..."
        )
        done, pending = await asyncio.wait(
            self._handler_tasks, timeout=timeout
        )
        if pending:
            self._logger.warn(
                f"{len(pending)} handler task(s) timed out, cancelling..."
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # ========== 消息回复 ==========

    async def reply(
        self,
        frame: WsFrame | dict[str, Any],
        body: dict[str, Any],
        cmd: str | None = None,
    ) -> WsFrame:
        """
        通过 WebSocket 通道发送回复消息（通用方法）

        :param frame: 收到的原始 WebSocket 帧，透传 headers.req_id
        :param body: 回复消息体
        :param cmd: 命令类型，默认使用 ``WsCmd.RESPONSE``
        """
        headers = frame.get("headers", {})
        req_id = headers.get("req_id", "")
        return await self._ws_manager.send_reply(req_id, body, cmd or WsCmd.RESPONSE)

    async def reply_stream(
        self,
        frame: WsFrame | dict[str, Any],
        stream_id: str,
        content: str,
        finish: bool = False,
        msg_item: list[ReplyMsgItem] | None = None,
        feedback: ReplyFeedback | None = None,
    ) -> WsFrame:
        """
        发送流式文本回复（便捷方法）

        :param frame: 收到的原始 WebSocket 帧，透传 headers.req_id
        :param stream_id: 流式消息 ID
        :param content: 回复内容（支持 Markdown）
        :param finish: 是否结束流式消息，默认 False
        :param msg_item: 图文混排项（仅在 finish=True 时有效）
        :param feedback: 反馈信息（仅在首次回复时设置）
        """
        stream: dict[str, Any] = {
            "id": stream_id,
            "finish": finish,
            "content": content,
        }

        if finish and msg_item:
            stream["msg_item"] = msg_item

        if feedback:
            stream["feedback"] = feedback

        return await self.reply(frame, {
            "msgtype": "stream",
            "stream": stream,
        })

    async def reply_welcome(
        self,
        frame: WsFrame | dict[str, Any],
        body: dict[str, Any],
    ) -> WsFrame:
        """
        发送欢迎语回复

        注意：需在收到事件（如 enter_chat）5 秒内调用，超时将无法发送。

        :param frame: 对应事件的 WebSocket 帧（需包含该事件的 req_id）
        :param body: 欢迎语消息体（支持文本或模板卡片格式）
        """
        return await self.reply(frame, body, WsCmd.RESPONSE_WELCOME)

    async def reply_template_card(
        self,
        frame: WsFrame | dict[str, Any],
        template_card: dict[str, Any],
        feedback: ReplyFeedback | None = None,
    ) -> WsFrame:
        """
        回复模板卡片消息

        :param frame: 收到的原始 WebSocket 帧
        :param template_card: 模板卡片内容
        :param feedback: 反馈信息
        """
        card = {**template_card, "feedback": feedback} if feedback else template_card
        body = {
            "msgtype": "template_card",
            "template_card": card,
        }
        return await self.reply(frame, body)

    async def reply_stream_with_card(
        self,
        frame: WsFrame | dict[str, Any],
        stream_id: str,
        content: str,
        finish: bool = False,
        *,
        msg_item: list[ReplyMsgItem] | None = None,
        stream_feedback: ReplyFeedback | None = None,
        template_card: dict[str, Any] | None = None,
        card_feedback: ReplyFeedback | None = None,
    ) -> WsFrame:
        """
        发送流式消息 + 模板卡片组合回复

        :param frame: 收到的原始 WebSocket 帧
        :param stream_id: 流式消息 ID
        :param content: 回复内容（支持 Markdown）
        :param finish: 是否结束流式消息，默认 False
        :param msg_item: 图文混排项（仅在 finish=True 时有效）
        :param stream_feedback: 流式消息反馈信息（首次回复时设置）
        :param template_card: 模板卡片内容（同一消息只能回复一次）
        :param card_feedback: 模板卡片反馈信息
        """
        stream: dict[str, Any] = {
            "id": stream_id,
            "finish": finish,
            "content": content,
        }

        if finish and msg_item:
            stream["msg_item"] = msg_item

        if stream_feedback:
            stream["feedback"] = stream_feedback

        body: dict[str, Any] = {
            "msgtype": "stream_with_template_card",
            "stream": stream,
        }

        if template_card:
            card = {**template_card, "feedback": card_feedback} if card_feedback else template_card
            body["template_card"] = card

        return await self.reply(frame, body)

    async def update_template_card(
        self,
        frame: WsFrame | dict[str, Any],
        template_card: dict[str, Any],
        userids: list[str] | None = None,
    ) -> WsFrame:
        """
        更新模板卡片

        注意：需在收到 template_card_event 事件 5 秒内调用。

        :param frame: 对应事件的 WebSocket 帧
        :param template_card: 模板卡片内容（task_id 需与回调收到的 task_id 一致）
        :param userids: 要替换模版卡片消息的 userid 列表，不填则替换所有用户
        """
        body: dict[str, Any] = {
            "response_type": "update_template_card",
            "template_card": template_card,
        }
        if userids:
            body["userids"] = userids
        return await self.reply(frame, body, WsCmd.RESPONSE_UPDATE)

    async def send_message(
        self,
        chatid: str,
        body: dict[str, Any],
    ) -> WsFrame:
        """
        主动发送消息

        向指定会话主动推送消息，无需依赖收到的回调帧。

        :param chatid: 会话 ID，单聊填用户的 userid，群聊填对应群聊的 chatid
        :param body: 消息体（支持 markdown 或 template_card 格式）
        """
        req_id = generate_req_id(WsCmd.SEND_MSG)
        full_body = {"chatid": chatid, **body}
        return await self._ws_manager.send_reply(req_id, full_body, WsCmd.SEND_MSG)

    # ========== 文件下载 ==========

    async def download_file(
        self,
        url: str,
        aes_key: str | None = None,
    ) -> dict[str, Any]:
        """
        下载文件并使用 AES 密钥解密

        :param url: 文件下载地址
        :param aes_key: AES 解密密钥（Base64 编码），取自消息中 image.aeskey 或 file.aeskey
        :returns: ``{"buffer": bytes, "filename": str | None}``
        """
        self._logger.info("Downloading and decrypting file...")

        try:
            result = await self._api_client.download_file_raw(url)
            encrypted_buffer: bytes = result["buffer"]
            filename: str | None = result.get("filename")

            if not aes_key:
                self._logger.warn("No aes_key provided, returning raw file data")
                return {"buffer": encrypted_buffer, "filename": filename}

            decrypted_buffer = decrypt_file(encrypted_buffer, aes_key)
            self._logger.info("File downloaded and decrypted successfully")
            return {"buffer": decrypted_buffer, "filename": filename}
        except Exception as e:
            self._logger.error("File download/decrypt failed:", str(e))
            raise

    # ========== 属性 ==========

    @property
    def is_connected(self) -> bool:
        """获取当前连接状态"""
        return self._ws_manager.is_connected

    @property
    def api(self) -> WeComApiClient:
        """获取 API 客户端实例"""
        return self._api_client
