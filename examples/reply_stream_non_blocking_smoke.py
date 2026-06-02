"""
企业微信智能机器人 SDK 示例 — reply_stream_non_blocking 冒烟测试

目的：
    验证 ``reply_stream_non_blocking`` 在快速连续发送时的行为：
      - 当上一条流式回复尚未收到回执时，中间帧会被跳过（返回 ``"skipped"``）；
      - ``finish=True`` 的结束帧不会被跳过，会强制发送，确保流式消息正常收尾。

环境变量：
    需在 .env 文件或 shell 环境中导出：
        export BOT_ID=...
        export BOT_SECRET=...

成功标志：
    向机器人发送任意一条文本消息后，stdout 应输出 30 条 ``[seq=N]`` 日志，
    其中混杂 ``'skipped'`` 与 ``WsFrame(...)``（前几条因首次发送通常是 WsFrame，
    后续未收到回执的会被跳过）；最后一行 ``[final]`` 必须是 ``WsFrame``，绝不能是
    ``'skipped'`` —— 这证明 finish 帧得到了保留。

使用方式：
    uv run --extra examples python examples/reply_stream_non_blocking_smoke.py
"""

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

from wecom_aibot_sdk import WSClient, generate_req_id


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("smoke.reply_stream_non_blocking")

    bot_id = os.getenv("BOT_ID", "")
    bot_secret = os.getenv("BOT_SECRET", "")

    if not bot_id or not bot_secret:
        print("请在 .env 文件中配置 BOT_ID 和 BOT_SECRET")
        print("    export BOT_ID=...")
        print("    export BOT_SECRET=...")
        sys.exit(1)

    print(
        "Reply-stream non-blocking smoke test — send any message to the bot. "
        "Watch stdout for 'skipped' vs WsFrame entries."
    )

    ws_client = WSClient(
        bot_id=bot_id,
        secret=bot_secret,
        scene=1,
        plug_version="1.0.0-test",
    )

    ws_client.on("connected", lambda: print("\n[连接] WebSocket 已连接"))
    ws_client.on("authenticated", lambda: print("[认证] 认证成功，可向机器人发送任意文本触发冒烟测试"))
    ws_client.on("disconnected", lambda reason: print(f"\n[断开] {reason}"))
    ws_client.on("reconnecting", lambda attempt: print(f"[重连] 正在进行第 {attempt} 次重连..."))
    ws_client.on("error", lambda error: print(f"[错误] {error}"))

    async def on_text_message(frame):
        body = frame.get("body", {})
        content = body.get("text", {}).get("content", "").strip()
        print(f"\n[文本] 收到: {content}")
        logger.info("triggered by text=%r — starting non-blocking stream burst", content)

        stream_id = generate_req_id("stream")
        print(f"[smoke] stream_id={stream_id}")

        # 关键：模拟 LLM token 流的真实场景 ——
        #   1) 用 create_task 把每个中间帧放到后台执行（fire-and-forget），主协程不等 ack
        #   2) 在两次 create_task 之间 sleep 一小段（30ms），让队列 worker 有机会
        #      把第 1 帧真正送出去并登记 _pending_acks，这样后续帧调用
        #      reply_stream_non_blocking 时才会观察到 "上一帧 ack 未回" 的状态
        #
        # 一次 ack RTT 实测 ~200ms，所以每 30ms 投一帧时，约 ~6 帧会在第 1 帧
        # in-flight 期间集中触发 skip；此后大致每 ~6 帧 skip 一次，直到全部投完。
        results: dict[int, object] = {}

        async def fire(seq: int) -> None:
            result = await ws_client.reply_stream_non_blocking(
                frame, stream_id, f"chunk {seq} ", finish=False
            )
            results[seq] = result
            print(f"[seq={seq}] result={result!r}")

        tasks = []
        for seq in range(1, 31):
            tasks.append(asyncio.create_task(fire(seq)))
            await asyncio.sleep(0.03)  # 30ms stagger, 显著快于 ack RTT

        # 等所有中间帧 task 落定（已发出的等 ack 回；被 skip 的立即返回）
        await asyncio.gather(*tasks)

        skipped = sum(1 for v in results.values() if v == "skipped")
        sent = len(results) - skipped
        logger.info("middle frames: sent=%d, skipped=%d (skipped 表示中间帧被跳过)", sent, skipped)

        # finish 帧：在中间帧全部处理完后再发，绝不会被跳过
        final_result = await ws_client.reply_stream_non_blocking(
            frame, stream_id, "[done]", finish=True
        )
        print(f"[final] result={final_result!r}")

        if final_result == "skipped":
            logger.error("FAIL: finish frame was skipped — this should never happen")
        else:
            logger.info("OK: finish frame was sent (not skipped)")

    ws_client.on("message.text", on_text_message)

    await ws_client.connect()

    stop_event = asyncio.Event()

    def _signal_handler():
        print("\n[退出] 正在停止机器人...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()
    await ws_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
