import asyncio

from asyncnsq import create_writer

TOPIC = "test_async_nsq"


async def main():
    writer = await create_writer(
        host="127.0.0.1",
        port=4150,
        heartbeat_interval=30000,
        feature_negotiation=True,
        tls_v1=False,
        snappy=False,
        deflate=False,
        deflate_level=6,
    )
    try:
        for i in range(100):
            await writer.pub(TOPIC, f"test_async_nsq:{i}".encode())
            await writer.dpub(TOPIC, i * 1000, f"delay:{i}".encode())
    finally:
        writer.close()


if __name__ == "__main__":
    asyncio.run(main())
