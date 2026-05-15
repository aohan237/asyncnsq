import asyncio

from asyncnsq.tcp.connection import create_connection
from asyncnsq.tcp.consts import PUB


async def main():
    conn = await create_connection(host="127.0.0.1", port=4150)
    try:
        await conn.identify(feature_negotiation=True)
        await conn.execute(PUB, "test_async_nsq", data=b"low-level message")
    finally:
        await conn.graceful_close(requeue=False)


if __name__ == "__main__":
    asyncio.run(main())
