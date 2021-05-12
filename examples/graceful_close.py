from asyncnsq import create_reader
import asyncio
import json
import time


loop = asyncio.get_event_loop()


async def go():
    try:
        reader = await create_reader(
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            max_in_flight=200)
        await reader.subscribe('ttt', 'nsq')
        cur_message = None
        async for message in reader.messages():
            a = message.body
            b = json.loads(a)
            print(type(b), b)
            await message.fin()
            cur_message = message
            time.sleep(0.5)
    except KeyboardInterrupt as tmp:
        print("KeyboardInterrupt", tmp)
        if not cur_message._is_processed:
            print("cur_message req", cur_message)
            await cur_message.req()
        await reader.clean_close()
    except SystemExit as tmp:
        print("SystemExit", tmp)
        if not cur_message._is_processed:
            print("cur_message req", cur_message)
            await cur_message.req()
        await reader.clean_close()


loop.run_until_complete(go())
