import asyncio
import json
import signal

from asyncnsq import create_reader

TOPIC = "test_async_nsq"
CHANNEL = "graceful"


async def handle_message(message):
    try:
        body = json.loads(message.body)
    except json.JSONDecodeError:
        body = message.body
    print(body)
    await asyncio.sleep(0.5)


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    reader = await create_reader(
        nsqd_tcp_addresses=["127.0.0.1:4150"],
        max_in_flight=200,
    )
    reader.set_message_handler(
        handle_message,
        auto_fin=True,
        auto_requeue=True,
        concurrency=32,
    )
    await reader.subscribe(TOPIC, CHANNEL)

    try:
        await stop.wait()
    finally:
        requeued = await reader.graceful_close(timeout=0)
        print(f"requeued unfinished messages: {requeued}")


if __name__ == "__main__":
    asyncio.run(main())
