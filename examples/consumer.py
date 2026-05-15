import asyncio

from asyncnsq import create_reader

TOPIC = "test_async_nsq"
CHANNEL = "nsq"


async def main():
    reader = await create_reader(
        nsqd_tcp_addresses=["127.0.0.1:4150"],
        max_in_flight=200,
    )
    await reader.subscribe(TOPIC, CHANNEL)
    try:
        async for message in reader.messages():
            print(message.body)
            await message.fin()
    finally:
        await reader.graceful_close()


if __name__ == "__main__":
    asyncio.run(main())
