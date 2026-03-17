"""
企业微信智能机器人 SDK 基本使用示例

使用方式：
    uv run --extra examples python examples/basic.py
"""

import asyncio
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

from wecom_aibot_sdk import WSClient, generate_req_id


async def main() -> None:
    bot_id = os.getenv("BOT_ID", "")
    bot_secret = os.getenv("BOT_SECRET", "")

    if not bot_id or not bot_secret:
        print("请在 .env 文件中配置 BOT_ID 和 BOT_SECRET")
        sys.exit(1)

    # 创建 WSClient 实例
    ws_client = WSClient(
        bot_id=bot_id,
        secret=bot_secret,
    )

    # 监听连接事件
    ws_client.on("connected", lambda: print("WebSocket 已连接"))

    # 监听认证成功事件
    ws_client.on("authenticated", lambda: print("认证成功"))

    # 监听断开事件
    ws_client.on("disconnected", lambda reason: print(f"连接已断开: {reason}"))

    # 监听重连事件
    ws_client.on("reconnecting", lambda attempt: print(f"正在进行第 {attempt} 次重连..."))

    # 监听错误事件
    ws_client.on("error", lambda error: print(f"发生错误: {error}"))

    # 监听所有消息（完整打印 body，方便调试引用消息等）
    def on_message(frame):
        import json
        body = frame.get("body", {})
        print(f"收到消息: {json.dumps(body, ensure_ascii=False, indent=2)}")

    ws_client.on("message", on_message)

    # 监听文本消息，使用流式回复
    async def on_text_message(frame):
        body = frame.get("body", {})
        content = body.get("text", {}).get("content", "")
        print(f"收到文本消息: {content}")

        # 生成一个流式消息 ID
        stream_id = generate_req_id("stream")

        # 发送流式中间内容
        await ws_client.reply_stream(frame, stream_id, "正在思考中...", False)

        # 模拟异步处理
        await asyncio.sleep(1)

        # 发送最终结果
        await ws_client.reply_stream(
            frame, stream_id, f'你好！你说的是: "{content}"', True
        )
        print("流式回复完成")

    ws_client.on("message.text", on_text_message)

    # 监听图片消息，下载并解密
    async def on_image_message(frame):
        body = frame.get("body", {})
        image_url = body.get("image", {}).get("url", "")
        print(f"收到图片消息: {image_url}")

        if not image_url:
            return

        try:
            aes_key = body.get("image", {}).get("aeskey")
            result = await ws_client.download_file(image_url, aes_key)
            buffer = result["buffer"]
            filename = result.get("filename")
            print(f"图片下载成功，大小: {len(buffer)} bytes")

            save_name = filename or f"image_{int(asyncio.get_running_loop().time())}.png"
            save_path = Path(__file__).parent / save_name
            save_path.write_bytes(buffer)
            print(f"图片已保存到: {save_path}")
        except Exception as e:
            print(f"图片下载失败: {e}")

    ws_client.on("message.image", on_image_message)

    # 监听语音消息
    def on_voice_message(frame):
        body = frame.get("body", {})
        content = body.get("voice", {}).get("content", "")
        print(f"收到语音消息（转文本）: {content}")

    ws_client.on("message.voice", on_voice_message)

    # 监听文件消息
    async def on_file_message(frame):
        body = frame.get("body", {})
        file_url = body.get("file", {}).get("url", "")
        print(f"收到文件消息: {file_url}")

        if not file_url:
            return

        try:
            aes_key = body.get("file", {}).get("aeskey")
            result = await ws_client.download_file(file_url, aes_key)
            buffer = result["buffer"]
            filename = result.get("filename")
            print(f"文件下载成功，大小: {len(buffer)} bytes")

            save_name = filename or f"file_{int(asyncio.get_running_loop().time())}"
            save_path = Path(__file__).parent / save_name
            save_path.write_bytes(buffer)
            print(f"文件已保存到: {save_path}")
        except Exception as e:
            print(f"文件下载失败: {e}")

    ws_client.on("message.file", on_file_message)

    # 监听图文混排消息
    def on_mixed_message(frame):
        body = frame.get("body", {})
        items = body.get("mixed", {}).get("msg_item", [])
        print(f"收到图文混排消息，包含 {len(items)} 个子项")
        for i, item in enumerate(items):
            if item.get("msgtype") == "text":
                print(f"  [{i}] 文本: {item.get('text', {}).get('content', '')}")
            elif item.get("msgtype") == "image":
                print(f"  [{i}] 图片: {item.get('image', {}).get('url', '')}")

    ws_client.on("message.mixed", on_mixed_message)

    # 监听进入会话事件（发送欢迎语）
    async def on_enter_chat(frame):
        print("用户进入会话")
        await ws_client.reply_welcome(frame, {
            "msgtype": "text",
            "text": {"content": "您好！我是智能助手，有什么可以帮您的吗？"},
        })

    ws_client.on("event.enter_chat", on_enter_chat)

    # 监听模板卡片事件
    def on_template_card_event(frame):
        body = frame.get("body", {})
        event_key = body.get("event", {}).get("event_key", "")
        print(f"收到模板卡片事件: {event_key}")

    ws_client.on("event.template_card_event", on_template_card_event)

    # 监听用户反馈事件
    def on_feedback_event(frame):
        import json
        body = frame.get("body", {})
        print(f"收到用户反馈事件: {json.dumps(body.get('event', {}), ensure_ascii=False)}")

    ws_client.on("event.feedback_event", on_feedback_event)

    # 建立连接
    await ws_client.connect()

    # 保持运行，等待信号退出
    stop_event = asyncio.Event()

    def _signal_handler():
        print("\n正在停止机器人...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()
    await ws_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
