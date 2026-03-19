"""
企业微信智能机器人 SDK 集成测试示例

通过发送特定指令触发对应测试：
    /echo <内容>        — 流式回复测试
    /upload             — 上传素材 + 被动回复媒体消息测试
    /send               — 主动发送媒体消息测试（发到当前会话）
    /scene              — 验证 scene/plug_version 参数（查看认证日志）
    (发送图片)           — 图片下载解密测试
    (发送文件)           — 文件下载解密测试
    (其他文本)           — 普通流式回复回归测试

    disconnected_event 测试：启动本程序后，再启动另一个相同 bot 的实例，
    观察本程序是否收到 disconnected_event 并停止重连。

使用方式：
    uv run --extra examples python examples/basic.py
"""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from wecom_aibot_sdk import WSClient, generate_req_id


# ========== 生成测试文件（一个 1x1 红色 PNG） ==========
def make_test_png() -> bytes:
    """生成一个最小的合法 PNG 文件（1x1 红色像素），用于上传测试"""
    import struct, zlib

    width, height = 1, 1
    # IHDR
    ihdr_data = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    ihdr = b"IHDR" + ihdr_data
    ihdr_chunk = struct.pack(">I", len(ihdr_data)) + ihdr + struct.pack(">I", zlib.crc32(ihdr) & 0xFFFFFFFF)
    # IDAT: filter byte 0 + RGB
    raw = b"\x00\xff\x00\x00"
    compressed = zlib.compress(raw)
    idat = b"IDAT" + compressed
    idat_chunk = struct.pack(">I", len(compressed)) + idat + struct.pack(">I", zlib.crc32(idat) & 0xFFFFFFFF)
    # IEND
    iend = b"IEND"
    iend_chunk = struct.pack(">I", 0) + iend + struct.pack(">I", zlib.crc32(iend) & 0xFFFFFFFF)
    # PNG signature
    return b"\x89PNG\r\n\x1a\n" + ihdr_chunk + idat_chunk + iend_chunk


async def main() -> None:
    bot_id = os.getenv("BOT_ID", "")
    bot_secret = os.getenv("BOT_SECRET", "")

    if not bot_id or not bot_secret:
        print("请在 .env 文件中配置 BOT_ID 和 BOT_SECRET")
        sys.exit(1)

    # ====================================================
    # 测试点 3.1: scene / plug_version 参数
    # 观察认证日志确认参数被传递（不影响连接即为通过）
    # ====================================================
    ws_client = WSClient(
        bot_id=bot_id,
        secret=bot_secret,
        scene=1,
        plug_version="1.0.0-test",
    )

    # ---------- 连接事件 ----------

    ws_client.on("connected", lambda: print("\n[连接] WebSocket 已连接"))
    ws_client.on("authenticated", lambda: print(
        "[认证] 认证成功 (scene=1, plug_version=1.0.0-test)\n"
        "\n"
        "========== 可用测试指令 ==========\n"
        "  /echo <内容>   流式回复测试\n"
        "  /upload        上传素材 + 被动回复媒体\n"
        "  /send          主动发送媒体消息\n"
        "  /scene         打印当前连接参数\n"
        "  (发送图片)      图片下载解密测试\n"
        "  (发送文件)      文件下载解密测试\n"
        "  (其他文本)      普通流式回复\n"
        "==================================\n"
    ))
    ws_client.on("disconnected", lambda reason: print(f"\n[断开] 连接已断开: {reason}"))
    ws_client.on("reconnecting", lambda attempt: print(f"[重连] 正在进行第 {attempt} 次重连..."))
    ws_client.on("error", lambda error: print(f"[错误] {error}"))

    # ====================================================
    # 测试点 2.1: disconnected_event
    # 启动另一个相同 bot 的实例后观察此事件
    # ====================================================
    def on_disconnected_event(frame):
        print("\n" + "=" * 50)
        print("[disconnected_event] 服务端因新连接建立主动断开了当前连接！")
        print("[disconnected_event] 预期行为：不会自动重连")
        print("=" * 50 + "\n")

    ws_client.on("event.disconnected_event", on_disconnected_event)

    # ---------- 全局消息日志 ----------

    def on_message(frame):
        body = frame.get("body", {})
        print(f"\n[消息] 收到: {json.dumps(body, ensure_ascii=False)[:300]}")

    ws_client.on("message", on_message)

    # ====================================================
    # 测试点 4.1/4.2: 基本功能回归 — 文本消息 + 流式回复
    # ====================================================
    async def on_text_message(frame):
        body = frame.get("body", {})
        content = body.get("text", {}).get("content", "").strip()
        chatid = body.get("chatid") or body.get("from", {}).get("userid", "")
        print(f"\n[文本] 收到: {content}")

        # ---- /echo 指令：流式回复测试 ----
        if content.startswith("/echo"):
            echo_text = content[5:].strip() or "Hello from Python SDK!"
            print(f"[测试 /echo] 开始流式回复: {echo_text}")
            stream_id = generate_req_id("stream")
            await ws_client.reply_stream(frame, stream_id, "正在处理...", False)
            await asyncio.sleep(1)
            await ws_client.reply_stream(frame, stream_id, echo_text, True)
            print("[测试 /echo] 流式回复完成 ✓")
            return

        # ---- /upload 指令：上传素材 + 被动回复媒体消息 ----
        if content.startswith("/upload"):
            print("[测试 /upload] 开始上传测试图片...")
            try:
                test_png = make_test_png()
                print(f"[测试 /upload] 生成测试 PNG: {len(test_png)} bytes")

                result = await ws_client.upload_media(
                    test_png, type="image", filename="test.png"
                )
                media_id = result["media_id"]
                print(f"[测试 /upload] 上传成功: media_id={media_id}")

                await ws_client.reply_media(frame, "image", media_id)
                print("[测试 /upload] 被动回复媒体消息完成 ✓")
            except Exception as e:
                print(f"[测试 /upload] 失败: {e}")

                # 上传失败时回复文本告知
                stream_id = generate_req_id("stream")
                await ws_client.reply_stream(
                    frame, stream_id, f"上传测试失败: {e}", True
                )
            return

        # ---- /send 指令：主动发送媒体消息 ----
        if content.startswith("/send"):
            print(f"[测试 /send] 开始上传并主动发送到会话: {chatid}")
            try:
                test_png = make_test_png()
                result = await ws_client.upload_media(
                    test_png, type="image", filename="send_test.png"
                )
                media_id = result["media_id"]
                print(f"[测试 /send] 上传成功: media_id={media_id}")

                await ws_client.send_media_message(chatid, "image", media_id)
                print("[测试 /send] 主动发送媒体消息完成 ✓")
            except Exception as e:
                print(f"[测试 /send] 失败: {e}")
            return

        # ---- /scene 指令：打印连接参数 ----
        if content.startswith("/scene"):
            stream_id = generate_req_id("stream")
            info = (
                "**当前连接参数:**\n"
                f"- bot_id: `{bot_id[:10]}...`\n"
                f"- scene: `1`\n"
                f"- plug_version: `1.0.0-test`\n"
                f"- is_connected: `{ws_client.is_connected}`"
            )
            await ws_client.reply_stream(frame, stream_id, info, True)
            print("[测试 /scene] 连接参数已回复 ✓")
            return

        # ---- 默认：普通流式回复（回归测试） ----
        stream_id = generate_req_id("stream")
        await ws_client.reply_stream(frame, stream_id, "正在思考中...", False)
        await asyncio.sleep(1)
        await ws_client.reply_stream(
            frame, stream_id, f'你好！你说的是: "{content}"', True
        )
        print("[回归] 普通流式回复完成 ✓")

    ws_client.on("message.text", on_text_message)

    # ====================================================
    # 测试: 图片下载解密
    # ====================================================
    async def on_image_message(frame):
        body = frame.get("body", {})
        image_url = body.get("image", {}).get("url", "")
        print(f"\n[图片] 收到图片消息")

        if not image_url:
            return

        try:
            aes_key = body.get("image", {}).get("aeskey")
            result = await ws_client.download_file(image_url, aes_key)
            buffer = result["buffer"]
            filename = result.get("filename")
            print(f"[图片] 下载解密成功: {len(buffer)} bytes, filename={filename} ✓")

            save_name = filename or f"image_{int(asyncio.get_running_loop().time())}.png"
            save_path = Path(__file__).parent / save_name
            save_path.write_bytes(buffer)
            print(f"[图片] 已保存到: {save_path}")
        except Exception as e:
            print(f"[图片] 下载失败: {e}")

    ws_client.on("message.image", on_image_message)

    # ====================================================
    # 测试: 文件下载解密
    # ====================================================
    async def on_file_message(frame):
        body = frame.get("body", {})
        file_url = body.get("file", {}).get("url", "")
        print(f"\n[文件] 收到文件消息")

        if not file_url:
            return

        try:
            aes_key = body.get("file", {}).get("aeskey")
            result = await ws_client.download_file(file_url, aes_key)
            buffer = result["buffer"]
            filename = result.get("filename")
            print(f"[文件] 下载解密成功: {len(buffer)} bytes, filename={filename} ✓")

            save_name = filename or f"file_{int(asyncio.get_running_loop().time())}"
            save_path = Path(__file__).parent / save_name
            save_path.write_bytes(buffer)
            print(f"[文件] 已保存到: {save_path}")
        except Exception as e:
            print(f"[文件] 下载失败: {e}")

    ws_client.on("message.file", on_file_message)

    # ---------- 其他事件 ----------

    ws_client.on("message.voice", lambda frame: print(
        f"\n[语音] 转文本: {frame.get('body', {}).get('voice', {}).get('content', '')}"
    ))

    ws_client.on("message.mixed", lambda frame: print(
        f"\n[混排] 包含 {len(frame.get('body', {}).get('mixed', {}).get('msg_item', []))} 个子项"
    ))

    async def on_enter_chat(frame):
        print("\n[事件] 用户进入会话")
        await ws_client.reply_welcome(frame, {
            "msgtype": "text",
            "text": {"content": "您好！发送 /echo /upload /send /scene 进行测试"},
        })

    ws_client.on("event.enter_chat", on_enter_chat)

    ws_client.on("event.template_card_event", lambda frame: print(
        f"\n[事件] 模板卡片: {frame.get('body', {}).get('event', {}).get('event_key', '')}"
    ))

    ws_client.on("event.feedback_event", lambda frame: print(
        f"\n[事件] 用户反馈: {json.dumps(frame.get('body', {}).get('event', {}), ensure_ascii=False)}"
    ))

    # ---------- 建立连接 ----------

    await ws_client.connect()

    # ---------- 保持运行，等待信号退出 ----------

    stop_event = asyncio.Event()

    def _signal_handler():
        print("\n[退出] 正在停止机器人（等待 handler 完成）...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()
    await ws_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
