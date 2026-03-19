"""
WebSocket 长连接管理器
负责维护与企业微信的 WebSocket 长连接，包括心跳、重连、认证等
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Awaitable

import websockets
import websockets.asyncio.client

from wecom_aibot_sdk.types import WsCmd, WsFrame
from wecom_aibot_sdk.utils import generate_req_id

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"


class WsConnectionManager:
    """WebSocket 长连接管理器"""

    def __init__(
        self,
        logger: Any,
        heartbeat_interval: int = 30000,
        reconnect_base_delay: int = 1000,
        max_reconnect_attempts: int = 10,
        ws_url: str | None = None,
        ws_options: dict[str, Any] | None = None,
    ) -> None:
        self._logger = logger
        self._ws_url = ws_url or DEFAULT_WS_URL
        self._ws_options = ws_options or {}
        self._heartbeat_interval = heartbeat_interval / 1000  # 转为秒
        self._reconnect_base_delay = reconnect_base_delay / 1000  # 转为秒
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_max_delay = 30.0  # 30 秒上限

        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._bot_id = ""
        self._bot_secret = ""
        self._extra_auth_params: dict[str, Any] = {}
        self._reconnect_attempts = 0
        self._is_manual_close = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._receive_task: asyncio.Task[None] | None = None

        # 心跳相关
        self._missed_pong_count = 0
        self._max_missed_pong = 2

        # 回复队列（按 req_id 分组）
        self._reply_queues: dict[str, list[dict[str, Any]]] = {}
        self._pending_acks: dict[str, dict[str, Any]] = {}
        self._reply_ack_timeout = 5.0  # 5 秒
        self._max_reply_queue_size = 100
        # 串行锁：保证同一 req_id 的回复消息串行发送
        self._queue_locks: dict[str, asyncio.Lock] = {}
        # 追踪回复队列处理 task，防止 GC 回收
        self._queue_tasks: set[asyncio.Task] = set()

        # 回调
        self.on_connected: Callable[[], Any] | None = None
        self.on_authenticated: Callable[[], Any] | None = None
        self.on_disconnected: Callable[[str], Any] | None = None
        self.on_message: Callable[[WsFrame], Any] | None = None
        self.on_reconnecting: Callable[[int], Any] | None = None
        self.on_error: Callable[[Exception], Any] | None = None
        self.on_server_disconnect: Callable[[str], Any] | None = None

    def set_credentials(
        self,
        bot_id: str,
        bot_secret: str,
        extra_auth_params: dict[str, Any] | None = None,
    ) -> None:
        """设置认证凭证"""
        self._bot_id = bot_id
        self._bot_secret = bot_secret
        self._extra_auth_params = extra_auth_params or {}

    async def connect(self) -> None:
        """建立 WebSocket 连接"""
        self._is_manual_close = False

        # 清理可能未完全关闭的旧连接
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self._logger.info(f"Connecting to WebSocket: {self._ws_url}...")

        try:
            self._ws = await websockets.asyncio.client.connect(
                self._ws_url,
                ping_interval=None,  # 我们自己管理心跳
                ping_timeout=None,
                close_timeout=5,
                **self._ws_options,
            )
            self._logger.info("WebSocket connection established, sending auth...")
            self._reconnect_attempts = 0
            self._missed_pong_count = 0

            # 连接建立后立即发送认证帧
            await self._send_auth()

            if self.on_connected:
                self.on_connected()

            # 启动接收循环
            self._receive_task = asyncio.create_task(self._receive_loop())
        except Exception as e:
            self._logger.error("Failed to create WebSocket connection:", str(e))
            if self.on_error:
                self.on_error(e)
            await self._schedule_reconnect()

    async def _receive_loop(self) -> None:
        """接收消息循环"""
        try:
            async for raw_message in self._ws:
                try:
                    frame: WsFrame = json.loads(raw_message)
                    await self._handle_frame(frame)
                except json.JSONDecodeError as e:
                    self._logger.error("Failed to parse WebSocket message:", str(e))
        except websockets.exceptions.ConnectionClosed as e:
            reason_str = str(e) or "unknown"
            self._logger.warn(f"WebSocket connection closed: {reason_str}")
            self._stop_heartbeat()
            self._clear_pending_messages(f"WebSocket connection closed ({reason_str})")
            if self.on_disconnected:
                self.on_disconnected(reason_str)
            if not self._is_manual_close:
                await self._schedule_reconnect()
        except Exception as e:
            self._logger.error("WebSocket error:", str(e))
            if self.on_error:
                self.on_error(e)
            if not self._is_manual_close:
                self._stop_heartbeat()
                self._clear_pending_messages(f"WebSocket error ({e})")
                if self.on_disconnected:
                    self.on_disconnected(str(e))
                await self._schedule_reconnect()

    async def _send_auth(self) -> None:
        """发送认证帧"""
        try:
            await self.send({
                "cmd": WsCmd.SUBSCRIBE,
                "headers": {"req_id": generate_req_id(WsCmd.SUBSCRIBE)},
                "body": {
                    "bot_id": self._bot_id,
                    "secret": self._bot_secret,
                    **self._extra_auth_params,
                },
            })
            self._logger.info("Auth frame sent")
        except Exception as e:
            self._logger.error("Failed to send auth frame:", str(e))

    async def _handle_frame(self, frame: WsFrame) -> None:
        """处理收到的帧数据"""
        cmd = frame.get("cmd", "")
        req_id = frame.get("headers", {}).get("req_id", "")

        # 消息推送
        if cmd == WsCmd.CALLBACK:
            self._logger.debug("Received push message:", json.dumps(frame.get("body", {}), ensure_ascii=False)[:200])
            if self.on_message:
                self.on_message(frame)
            return

        # 事件推送
        if cmd == WsCmd.EVENT_CALLBACK:
            self._logger.debug("Received event callback:", json.dumps(frame.get("body", {}), ensure_ascii=False)[:200])

            # 检测 disconnected_event：有新连接建立，服务端通知旧连接即将被断开
            body = frame.get("body", {})
            event = body.get("event", {})
            if event.get("eventtype") == "disconnected_event":
                self._logger.warn(
                    "Received disconnected_event: a new connection has been established, "
                    "this connection will be closed by server"
                )
                # 先分发事件给上层（让用户可以监听 event.disconnected_event）
                if self.on_message:
                    self.on_message(frame)
                # 停止心跳、清理待处理消息
                self._stop_heartbeat()
                self._clear_pending_messages("Server disconnected due to new connection")
                # 标记阻止自动重连（服务端正常行为，重连也会被再次断开）
                self._is_manual_close = True
                # 通知上层服务端主动断开
                if self.on_server_disconnect:
                    self.on_server_disconnect("New connection established, server disconnected this connection")
                return

            if self.on_message:
                self.on_message(frame)
            return

        # 无 cmd 的帧：认证响应、心跳响应或回复消息回执

        # 认证响应（优先于 pendingAcks 检查，避免误判）
        if req_id.startswith(WsCmd.SUBSCRIBE):
            errcode = frame.get("errcode", -1)
            if errcode != 0:
                self._logger.error(
                    f"Authentication failed: errcode={errcode}, errmsg={frame.get('errmsg', '')}"
                )
                if self.on_error:
                    self.on_error(
                        Exception(f"Authentication failed: {frame.get('errmsg', '')} (code: {errcode})")
                    )
                return
            self._logger.info("Authentication successful")
            self._start_heartbeat()
            if self.on_authenticated:
                self.on_authenticated()
            return

        # 心跳响应（优先于 pendingAcks 检查，避免误判）
        if req_id.startswith(WsCmd.HEARTBEAT):
            errcode = frame.get("errcode", -1)
            if errcode != 0:
                self._logger.warn(
                    f"Heartbeat ack error: errcode={errcode}, errmsg={frame.get('errmsg', '')}"
                )
                return
            self._missed_pong_count = 0
            return

        # 回复消息的回执
        if req_id in self._pending_acks:
            self._handle_reply_ack(req_id, frame)
            return

        # 未知帧类型 — 只记录警告，不传给 onMessage（避免 body=undefined 导致下游误处理）
        self._logger.warn("Received unknown frame (ignored):", json.dumps(frame, ensure_ascii=False)[:200])

    def _start_heartbeat(self) -> None:
        """启动心跳定时器"""
        self._stop_heartbeat()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._logger.debug(f"Heartbeat timer started, interval: {self._heartbeat_interval}s")

    def _stop_heartbeat(self) -> None:
        """停止心跳定时器"""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
            self._logger.debug("Heartbeat timer stopped")

    async def _heartbeat_loop(self) -> None:
        """心跳循环"""
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                await self._send_heartbeat()
        except asyncio.CancelledError:
            pass

    async def _send_heartbeat(self) -> None:
        """发送心跳"""
        if self._missed_pong_count >= self._max_missed_pong:
            self._logger.warn(
                f"No heartbeat ack received for {self._missed_pong_count} consecutive pings, "
                "connection considered dead"
            )
            self._stop_heartbeat()
            if self._ws:
                await self._ws.close()
            return

        self._missed_pong_count += 1
        try:
            await self.send({
                "cmd": WsCmd.HEARTBEAT,
                "headers": {"req_id": generate_req_id(WsCmd.HEARTBEAT)},
            })
            extra = f" (awaiting {self._missed_pong_count} pong)" if self._missed_pong_count > 1 else ""
            self._logger.debug(f"Heartbeat sent{extra}")
        except Exception as e:
            self._logger.error("Failed to send heartbeat:", str(e))

    async def _schedule_reconnect(self) -> None:
        """安排重连"""
        if (
            self._max_reconnect_attempts != -1
            and self._reconnect_attempts >= self._max_reconnect_attempts
        ):
            self._logger.error(
                f"Max reconnect attempts reached ({self._max_reconnect_attempts}), giving up"
            )
            if self.on_error:
                self.on_error(Exception("Max reconnect attempts exceeded"))
            return

        self._reconnect_attempts += 1
        delay = min(
            self._reconnect_base_delay * (2 ** (self._reconnect_attempts - 1)),
            self._reconnect_max_delay,
        )

        self._logger.info(f"Reconnecting in {delay}s (attempt {self._reconnect_attempts})...")
        if self.on_reconnecting:
            self.on_reconnecting(self._reconnect_attempts)

        await asyncio.sleep(delay)
        if self._is_manual_close:
            return
        await self.connect()

    async def send(self, frame: WsFrame) -> None:
        """发送数据帧"""
        if self._ws is not None:
            await self._ws.send(json.dumps(frame, ensure_ascii=False))
        else:
            raise ConnectionError("WebSocket not connected, unable to send data")

    async def send_reply(
        self,
        req_id: str,
        body: dict[str, Any],
        cmd: str = WsCmd.RESPONSE,
    ) -> WsFrame:
        """
        通过 WebSocket 通道发送回复消息（串行队列版本）

        同一个 req_id 的消息会被放入队列中串行发送。
        """
        future: asyncio.Future[WsFrame] = asyncio.get_running_loop().create_future()

        frame: WsFrame = {
            "cmd": cmd,
            "headers": {"req_id": req_id},
            "body": body,
        }

        item = {"frame": frame, "future": future}

        if req_id not in self._reply_queues:
            self._reply_queues[req_id] = []
        if req_id not in self._queue_locks:
            self._queue_locks[req_id] = asyncio.Lock()

        queue = self._reply_queues[req_id]

        if len(queue) >= self._max_reply_queue_size:
            self._logger.warn(
                f"Reply queue for reqId {req_id} exceeds max size ({self._max_reply_queue_size}), rejecting"
            )
            future.set_exception(
                Exception(f"Reply queue for reqId {req_id} exceeds max size ({self._max_reply_queue_size})")
            )
            return await future

        queue.append(item)

        # 如果队列中只有这一条，说明当前空闲，立即开始处理
        if len(queue) == 1:
            task = asyncio.create_task(self._process_reply_queue(req_id))
            self._queue_tasks.add(task)
            task.add_done_callback(self._queue_tasks.discard)

        return await future

    async def _process_reply_queue(self, req_id: str) -> None:
        """处理指定 req_id 的回复队列"""
        lock = self._queue_locks.get(req_id)
        if lock is None:
            return

        async with lock:
            while True:
                queue = self._reply_queues.get(req_id)
                if not queue:
                    self._reply_queues.pop(req_id, None)
                    self._queue_locks.pop(req_id, None)
                    return

                item = queue[0]
                future: asyncio.Future[WsFrame] = item["future"]

                try:
                    await self.send(item["frame"])
                    self._logger.debug(
                        f"Reply message sent via WebSocket, reqId: {req_id}, queue length: {len(queue)}"
                    )
                except Exception as e:
                    self._logger.error(f"Failed to send reply for reqId {req_id}:", str(e))
                    queue.pop(0)
                    if not future.done():
                        future.set_exception(e)
                    continue

                # 等待回执（通过 pending_acks 机制）
                ack_future: asyncio.Future[WsFrame] = asyncio.get_running_loop().create_future()
                self._pending_acks[req_id] = {"future": ack_future}

                try:
                    ack_frame = await asyncio.wait_for(ack_future, timeout=self._reply_ack_timeout)
                    queue.pop(0)
                    errcode = ack_frame.get("errcode", -1)
                    if errcode != 0:
                        self._logger.warn(
                            f"Reply ack error: reqId={req_id}, errcode={errcode}, "
                            f"errmsg={ack_frame.get('errmsg', '')}"
                        )
                        if not future.done():
                            future.set_exception(
                                Exception(
                                    f"Reply ack error: errcode={errcode}, "
                                    f"errmsg={ack_frame.get('errmsg', '')}"
                                )
                            )
                    else:
                        self._logger.debug(f"Reply ack received for reqId: {req_id}")
                        if not future.done():
                            future.set_result(ack_frame)
                except asyncio.TimeoutError:
                    self._logger.warn(f"Reply ack timeout ({self._reply_ack_timeout}s) for reqId: {req_id}")
                    self._pending_acks.pop(req_id, None)
                    queue.pop(0)
                    if not future.done():
                        future.set_exception(
                            Exception(f"Reply ack timeout ({self._reply_ack_timeout}s) for reqId: {req_id}")
                        )

    def _handle_reply_ack(self, req_id: str, frame: WsFrame) -> None:
        """处理回复消息的回执"""
        pending = self._pending_acks.pop(req_id, None)
        if pending is None:
            return

        ack_future: asyncio.Future[WsFrame] = pending["future"]
        if not ack_future.done():
            ack_future.set_result(frame)

    def _clear_pending_messages(self, reason: str) -> None:
        """清理所有待处理的消息和回执"""
        # 收集已在 pendingAcks 中被 reject 的 future，用于后续去重
        rejected_futures: set[int] = set()

        # 先清理 pendingAcks
        for req_id, pending in self._pending_acks.items():
            fut: asyncio.Future = pending["future"]
            if not fut.done():
                fut.set_exception(Exception(f"{reason}, reply for reqId: {req_id} cancelled"))
                rejected_futures.add(id(fut))
        self._pending_acks.clear()

        # 再清理 replyQueues，跳过已在 pendingAcks 中被 reject 过的 future
        for req_id, queue in self._reply_queues.items():
            for item in queue:
                fut = item["future"]
                if id(fut) in rejected_futures:
                    continue  # 已在 pendingAcks 中被 reject 过，跳过
                if not fut.done():
                    fut.set_exception(Exception(f"{reason}, reply for reqId: {req_id} cancelled"))
        self._reply_queues.clear()
        self._queue_locks.clear()

    async def disconnect(self) -> None:
        """主动断开连接"""
        self._is_manual_close = True
        self._stop_heartbeat()
        self._clear_pending_messages("Connection manually closed")

        # 等待回复队列 task 完成（最多 5 秒）
        if self._queue_tasks:
            self._logger.debug(
                f"Waiting for {len(self._queue_tasks)} reply queue task(s)..."
            )
            _, pending = await asyncio.wait(self._queue_tasks, timeout=5.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self._queue_tasks.clear()

        if self._receive_task is not None:
            self._receive_task.cancel()
            self._receive_task = None

        if self._ws is not None:
            try:
                await self._ws.close(1000, "Manual disconnect")
            except Exception:
                pass
            self._ws = None

        self._logger.info("WebSocket connection manually closed")

    @property
    def is_connected(self) -> bool:
        """获取当前连接状态"""
        return self._ws is not None and self._ws.state.name == "OPEN"
