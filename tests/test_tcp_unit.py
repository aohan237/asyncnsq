import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, patch

from asyncnsq.tcp import consts
from asyncnsq.tcp.exceptions import (
    NSQBadTopic, NSQDPubFailed, NSQErrorCode, NSQFinishFailed, make_error,
)
from asyncnsq.tcp.messages import NsqMessage
from asyncnsq.tcp.reader import Reader, _parse_address, create_reader
from asyncnsq.tcp.reader_rdy import RdyControl
from asyncnsq.tcp.writer import Writer, create_writer
from asyncnsq.utils import (
    MaxRetriesExided, _convert_to_bytes, _convert_to_str, get_host_and_port,
    get_logger, retry_iterator, valid_channel_name, valid_topic_name,
)


class FakeConn:

    def __init__(self, id='tcp://127.0.0.1:4150', identify_resp=None):
        self.id = id
        self.endpoint = id
        self.closed = False
        self._in_flight = 0
        self._in_flight_messages = {}
        self._rdy_count = 0
        self._max_rdy_count = 2500
        self._on_rdy_changed_cb = None
        self._on_message = None
        self.commands = []
        self.identify_resp = identify_resp or b'{"auth_required": false}'

    async def identify(self, **config):
        self.identify_config = config
        return self.identify_resp

    async def auth(self, secret):
        self.auth_secret = secret
        return b'{"identity":"test"}'

    def execute(self, command, *args, data=None):
        self.commands.append((command, args, data))
        if command == b'RDY' and args:
            self._rdy_count = int(args[0])
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(b'OK')
        return fut

    def fin_message(self, message_id):
        self.commands.append((b'FIN', (message_id,), None))
        self._in_flight = max(0, self._in_flight - 1)
        return b'OK'

    def req_message(self, message_id, timeout=0):
        self.commands.append((b'REQ', (message_id, timeout), None))
        self._in_flight = max(0, self._in_flight - 1)
        return b'OK'

    def _message_processed(self, msg):
        self.processed_message = msg

    def close(self):
        self.closed = True

    async def stop_receiving(self):
        return await self.execute(b'RDY', 0)

    async def requeue_in_flight(self, timeout=0):
        requeued = 0
        for message_id, msg in list(self._in_flight_messages.items()):
            if not getattr(msg, 'processed', False):
                await msg.req(timeout)
                requeued += 1
            self._in_flight_messages.pop(message_id, None)
        return requeued


class UtilsTest(unittest.TestCase):

    def test_name_validation_and_converters(self):
        self.assertTrue(valid_topic_name('topic#ephemeral'))
        self.assertTrue(valid_channel_name('channel#ephemeral'))
        self.assertFalse(valid_topic_name('bad/topic'))
        self.assertFalse(valid_channel_name(''))
        self.assertEqual(_convert_to_bytes(bytearray(b'a')), bytearray(b'a'))
        self.assertEqual(_convert_to_bytes(3.14), b'3.14')
        self.assertEqual(_convert_to_str(bytearray(b'a')), 'a')
        self.assertEqual(_convert_to_str(42), '42')
        with self.assertRaises(TypeError):
            _convert_to_bytes(object())
        with self.assertRaises(TypeError):
            _convert_to_str(object())

    def test_address_parsing(self):
        self.assertEqual(get_host_and_port('tcp://host:4150'),
                         ('host', '4150'))
        self.assertEqual(get_host_and_port('http://host:4161'),
                         ('host', '4161'))
        self.assertEqual(get_host_and_port('host'), ('host', None))
        self.assertEqual(get_host_and_port('tcp:host:4150'), ('tcp', None))
        self.assertEqual(_parse_address('host', 4150), ('host', 4150))
        self.assertEqual(_parse_address(('host', '4151'), 4150),
                         ('host', 4151))

    def test_get_logger(self):
        logger = get_logger('debug')
        self.assertEqual(logger.level, logging.DEBUG)
        logger = get_logger()
        self.assertEqual(logger.level, logging.INFO)

    def test_retry_iterator(self):
        retries = retry_iterator(init_delay=1, max_delay=2, jitter=0,
                                 max_retries=3)
        self.assertEqual(next(retries), 0)
        self.assertEqual(next(retries), 2)
        self.assertEqual(next(retries), 2)
        with self.assertRaises(MaxRetriesExided):
            next(retries)


class MessageAndErrorTest(unittest.TestCase):

    def test_message_lifecycle(self):
        async def run():
            conn = FakeConn()
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            self.assertFalse(msg.processed)
            self.assertEqual(await msg.touch(), b'OK')
            self.assertFalse(msg.processed)
            self.assertEqual(await msg.fin(), b'OK')
            self.assertTrue(msg.processed)
            with self.assertRaises(RuntimeWarning):
                await msg.fin()
            with self.assertRaises(RuntimeWarning):
                await msg.req()

        asyncio.run(run())

    def test_message_req(self):
        async def run():
            conn = FakeConn()
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            self.assertEqual(await msg.req(0), b'OK')
            self.assertTrue(msg.processed)
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            self.assertEqual(await msg.touch(), b'OK')
            self.assertFalse(msg.processed)
            self.assertEqual(await msg.fin(), b'OK')
            with self.assertRaises(RuntimeWarning):
                await msg.touch()

        asyncio.run(run())

    def test_message_nowait_lifecycle(self):
        conn = FakeConn()
        msg = NsqMessage(1, 1, b'id', b'body', conn)
        self.assertEqual(msg.fin_nowait(), b'OK')
        self.assertTrue(msg.processed)
        self.assertIs(conn.processed_message, msg)
        with self.assertRaises(RuntimeWarning):
            msg.fin_nowait()

        msg = NsqMessage(1, 1, b'id2', b'body', conn)
        self.assertEqual(msg.req_nowait(0), b'OK')
        self.assertTrue(msg.processed)
        self.assertIs(conn.processed_message, msg)
        with self.assertRaises(RuntimeWarning):
            msg.req_nowait()

        class SlowConn:

            def execute(self, command, *args, data=None):
                fut = asyncio.get_running_loop().create_future()
                fut.set_result(b'OK')
                return fut

        async def slow_paths():
            slow = SlowConn()
            msg = NsqMessage(1, 1, b'id3', b'body', slow)
            self.assertEqual(await msg.fin(), b'OK')
            msg = NsqMessage(1, 1, b'id4', b'body', slow)
            self.assertEqual(await msg.req(0), b'OK')

        asyncio.run(slow_paths())

        slow_conn = object()
        msg = NsqMessage(1, 1, b'id5', b'body', slow_conn)
        with self.assertRaises(RuntimeError):
            msg.fin_nowait()
        with self.assertRaises(RuntimeError):
            msg.req_nowait()

    def test_error_mapping(self):
        self.assertIsInstance(make_error(b'E_BAD_TOPIC', b'bad'), NSQBadTopic)
        self.assertIsInstance(
            make_error(b'E_DPUB_FAILED', b'bad'), NSQDPubFailed)
        finish = make_error(b'E_FIN_FAILED', b'bad')
        self.assertIsInstance(finish, NSQFinishFailed)
        self.assertFalse(finish.fatal)
        self.assertIsInstance(make_error(b'E_UNKNOWN', b'bad'), NSQErrorCode)


class RdyControlTest(unittest.TestCase):

    def test_rdy_redistribution_and_update(self):
        async def run():
            c1 = FakeConn('c1')
            c2 = FakeConn('c2')
            c1._in_flight = 1
            control = RdyControl(idle_timeout=10, max_in_flight=3)
            try:
                control.add_connections({'c1': c1, 'c2': c2})
                await control._redistribute_rdy_state()
                self.assertTrue(c1.commands or c2.commands)
                control.redistribute()
                await asyncio.sleep(0)
                await control._update_rdy('c1')
                self.assertIn((b'RDY', (1,), None), c1.commands)
                c1.closed = True
                await control._update_rdy('c1')
                control.rdy_changed('missing')
                control.redistribute()
                control.remove_connection(c2)
                control.remove_all()
                c1.closed = False
                c1._rdy_count = 0
                c1._in_flight = 0
                c1._max_rdy_count = 2
                control.add_connection(c1)
                await control._update_rdy('c1')
                self.assertIn((b'RDY', (2,), None), c1.commands)
            finally:
                control.close()

        asyncio.run(run())

    def test_rdy_distributor_rejects_unknown_command(self):
        async def run():
            control = RdyControl(idle_timeout=10, max_in_flight=1)
            control._cmd_queue.put_nowait((999, ()))
            with self.assertRaises(RuntimeError):
                await control._distributor_task

        asyncio.run(run())

    def test_rdy_control_window_edge_cases(self):
        async def run():
            conn = FakeConn('c1')
            control = RdyControl(idle_timeout=10, max_in_flight=0)
            try:
                control.add_connection(conn)
                self.assertEqual(control._target_rdy_by_id(), {})
                conn._rdy_count = 1
                await control._update_rdy(conn.id)
                self.assertEqual(conn.commands[-1], (b'RDY', (0,), None))

                conn.closed = True
                command_count = len(conn.commands)
                await control._set_conn_rdy(conn, 1)
                self.assertEqual(len(conn.commands), command_count)
            finally:
                control.close()

            control = RdyControl(idle_timeout=10, max_in_flight=4)
            try:
                conn = FakeConn('c1')
                conn._rdy_count = 4
                control.add_connection(conn)
                await control._set_conn_rdy(conn, 4)
                self.assertEqual(conn.commands, [])

                control.rdy_changed(conn.id)
                control.rdy_changed(conn.id)
                self.assertEqual(control._cmd_queue.qsize(), 1)
                control.redistribute()
                control.redistribute()
                self.assertEqual(control._cmd_queue.qsize(), 2)
                control.close()
                control.rdy_changed('ignored')
                control.redistribute()
                self.assertEqual(control._cmd_queue.qsize(), 2)
            finally:
                control.close()

        asyncio.run(run())


class ReaderUnitTest(unittest.TestCase):

    def test_reader_init_rejects_conflicting_compression(self):
        async def run():
            with self.assertRaises(ValueError):
                Reader(snappy=True, deflate=True)

        asyncio.run(run())

    def test_reader_prepare_conn_requires_auth_secret(self):
        async def run():
            reader = Reader()
            conn = FakeConn(identify_resp=b'{"auth_required": true}')
            with self.assertRaises(Exception):
                await reader.prepare_conn(conn)
            self.assertTrue(conn.closed)
            reader.close()

        asyncio.run(run())

    def test_reader_prepare_conn_uses_auth_secret(self):
        async def run():
            reader = Reader(auth_secret=b'secret')
            conn = FakeConn(
                identify_resp=b'{"auth_required": true,'
                              b'"max_rdy_count": 123}')
            await reader.prepare_conn(conn)
            self.assertEqual(conn.auth_secret, 'secret')
            self.assertEqual(conn._max_rdy_count, 123)
            reader.close()

        asyncio.run(run())

    def test_reader_connect_lookupd_mode_is_deferred(self):
        async def run():
            reader = Reader(lookupd_http_addresses=[('lookupd', 4161)])
            await reader.connect()
            self.assertEqual(reader._connections, {})
            reader.close()

        asyncio.run(run())

    def test_reader_connects_direct_addresses(self):
        async def run():
            conn = FakeConn()
            reader = Reader(nsqd_tcp_addresses=[('host', 4150)])
            with patch('asyncnsq.tcp.reader.create_connection',
                       AsyncMock(return_value=conn)):
                await reader.connect()
            self.assertIn(conn.id, reader._connections)
            self.assertIs(conn._on_message.func.__self__, reader)
            self.assertIs(conn._on_message.func.__func__, Reader._on_message)
            self.assertFalse(any(command == b'RDY'
                                 for command, _, _ in conn.commands))
            reader.close()

        asyncio.run(run())

    def test_reader_lookupd_adds_and_subscribes_new_producer(self):
        async def run():
            class FakeLookupd:

                def __init__(self, host, port):
                    self.endpoint = (host, port)
                    self.closed = False

                async def lookup(self, topic):
                    return {
                        'producers': [{
                            'broadcast_address': '127.0.0.1',
                            'tcp_port': 4150,
                        }]
                    }

                async def close(self):
                    self.closed = True

            conn = FakeConn('tcp://127.0.0.1:4150')
            reader = Reader(lookupd_http_addresses=[('lookupd', 4161)])
            reader.topic = 'topic'
            reader.channel = 'channel'
            reader._is_subscribe = True
            with patch('asyncnsq.tcp.reader.NsqLookupd', FakeLookupd), \
                    patch('asyncnsq.tcp.reader.create_connection',
                          AsyncMock(return_value=conn)):
                await reader._lookupd()
            self.assertIn(conn.id, reader._connections)
            self.assertIn((b'SUB', ('topic', 'channel'), None),
                          conn.commands)
            reader.close()

        asyncio.run(run())

    def test_reader_lookupd_failure_closes_lookupd_connection(self):
        async def run():
            class FailingLookupd:

                closed = False

                def __init__(self, host, port):
                    pass

                async def lookup(self, topic):
                    raise RuntimeError('lookup failed')

                async def close(self):
                    self.__class__.closed = True

            reader = Reader()
            reader.topic = 'topic'
            with patch('asyncnsq.tcp.reader.NsqLookupd', FailingLookupd):
                await reader._poll_lookupd('lookupd', 4161)
            self.assertTrue(FailingLookupd.closed)
            reader.close()

        asyncio.run(run())

    def test_reader_subscribe_and_message_helpers(self):
        async def run():
            reader = Reader()
            conn = FakeConn()
            reader._connections[conn.id] = conn
            reader._rdy_control.add_connection(conn)
            await reader.subscribe('topic', 'channel')
            self.assertIsNone(reader._lookupd_task)
            self.assertEqual(conn.commands[-1], (b'SUB', ('topic', 'channel'),
                                                 None))
            with self.assertRaises(ValueError):
                next(Reader().wait_messages())
            with self.assertRaises(ValueError):
                await Reader().messages().__anext__()
            reader.close()

        asyncio.run(run())

    def test_reader_subscribe_lookupd_and_async_generators(self):
        async def run():
            reader = Reader(lookupd_http_addresses=[('lookupd', 4161)])
            reader._lookupd = AsyncMock()
            await reader.subscribe('topic', 'channel')
            reader._lookupd.assert_awaited_once_with()
            self.assertIsNotNone(reader._lookupd_task)
            reader._lookupd_task.cancel()

            reader._queue.put_nowait('message')
            agen = reader.messages()
            self.assertEqual(await agen.__anext__(), 'message')
            await agen.aclose()
            reader.close()

        asyncio.run(run())

    def test_reader_background_loops_exit(self):
        async def run():
            reader = Reader()
            redistributes = []
            reader._rdy_control.redistribute = lambda: redistributes.append(1)
            reader._redistribute_timeout = 0
            reader._is_subscribe = True
            task = asyncio.create_task(reader._redistribute())
            await asyncio.sleep(0)
            reader._is_subscribe = False
            await task
            self.assertTrue(redistributes)

            reader._is_subscribe = True
            reader.clean_closed = False
            reader._lookupd_poll_time = 0
            reader._lookupd = AsyncMock()
            task = asyncio.create_task(reader._lookupd_loop())
            await asyncio.sleep(0)
            reader.clean_closed = True
            await task
            self.assertTrue(reader._lookupd.await_count >= 1)
            reader.close()

        asyncio.run(run())

    def test_reader_handler_defaults_to_queue_backed(self):
        async def run():
            reader = Reader(nsqd_tcp_addresses=[])
            conn = FakeConn()
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            seen = []

            reader.set_message_handler(lambda item: seen.append(item.body))
            queued = reader._on_message(conn, msg)
            self.assertIs(queued, msg)
            self.assertEqual(seen, [])
            reader._queue.put_nowait(queued)
            await reader.wait_handler_tasks()

            self.assertEqual(seen, [b'body'])
            self.assertTrue(msg.processed)
            self.assertEqual(conn.commands[-1], (b'FIN', (b'id',), None))

            reader.clear_message_handler()
            msg = NsqMessage(1, 1, b'id2', b'body2', conn)
            self.assertIs(reader._on_message(conn, msg), msg)

            with self.assertRaises(TypeError):
                reader.set_message_handler(None)
            reader.close()

        asyncio.run(run())

    def test_reader_queue_handler_worker_lifecycle_edges(self):
        async def run():
            active = Reader(nsqd_tcp_addresses=[])

            async def slow_handler(msg):
                await asyncio.sleep(10)

            conn = FakeConn()
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            active.set_message_handler(slow_handler, concurrency=2)
            self.assertEqual(len(active._handler_workers), 2)
            active._queue.put_nowait(msg)
            await asyncio.sleep(0)
            await active.wait_handler_tasks(timeout=0)
            await active._stop_handler_workers()
            self.assertFalse(active._handler_workers)
            active.close()

        asyncio.run(run())

    def test_reader_direct_handler_fast_path(self):
        async def run():
            reader = Reader(nsqd_tcp_addresses=[])
            conn = FakeConn()
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            seen = []

            reader.set_message_handler(
                lambda item: seen.append(item.body), direct=True)
            self.assertIsNone(reader._on_message(conn, msg))

            self.assertEqual(seen, [b'body'])
            self.assertTrue(msg.processed)
            self.assertEqual(conn.commands[-1], (b'FIN', (b'id',), None))
            reader.close()

        asyncio.run(run())

    def test_reader_direct_handler_async_and_error_paths(self):
        async def run():
            reader = Reader(nsqd_tcp_addresses=[])
            conn = FakeConn()
            seen = []

            async def async_handler(msg):
                await asyncio.sleep(0)
                seen.append(msg.body)

            reader.set_message_handler(async_handler, direct=True)
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            reader._on_message(conn, msg)
            await reader.wait_handler_tasks()
            self.assertEqual(seen, [b'body'])
            self.assertTrue(msg.processed)

            def bad_handler(msg):
                raise RuntimeError('bad handler')

            reader.set_message_handler(bad_handler, direct=True)
            msg = NsqMessage(1, 1, b'id2', b'body2', conn)
            reader._on_message(conn, msg)
            self.assertTrue(msg.processed)
            self.assertEqual(conn.commands[-1], (b'REQ', (b'id2', 0), None))

            reader.set_message_handler(
                bad_handler, auto_requeue=False, direct=True)
            msg = NsqMessage(1, 1, b'id3', b'body3', conn)
            reader._on_message(conn, msg)
            self.assertFalse(msg.processed)

            async def bad_async_handler(msg):
                await asyncio.sleep(0)
                raise RuntimeError('bad async handler')

            reader.set_message_handler(bad_async_handler, direct=True)
            msg = NsqMessage(1, 1, b'id4', b'body4', conn)
            reader._on_message(conn, msg)
            await reader.wait_handler_tasks()
            self.assertTrue(msg.processed)
            self.assertEqual(conn.commands[-1], (b'REQ', (b'id4', 0), None))

            reader.set_message_handler(
                bad_async_handler, concurrency=1, direct=True)
            msg = NsqMessage(1, 1, b'id5', b'body5', conn)
            reader._on_message(conn, msg)
            await reader.wait_handler_tasks()
            self.assertTrue(msg.processed)
            self.assertEqual(conn.commands[-1], (b'REQ', (b'id5', 0), None))

            class NoReqConn:
                pass

            msg = NsqMessage(1, 1, b'id6', b'body6', NoReqConn())
            reader._handler_auto_requeue = True
            reader._requeue_failed_handler_message(msg)
            self.assertFalse(msg.processed)

            await reader.wait_handler_tasks()
            reader.close()

        asyncio.run(run())

    def test_reader_direct_handler_concurrency_and_wait_timeout(self):
        async def run():
            reader = Reader(nsqd_tcp_addresses=[])
            conn = FakeConn()
            seen = []

            async def async_handler(msg):
                await asyncio.sleep(0)
                seen.append(msg.message_id)

            reader.set_message_handler(
                async_handler, concurrency=1, direct=True)
            msg = NsqMessage(1, 1, b'id', b'body', conn)
            reader._on_message(conn, msg)
            await reader.wait_handler_tasks(timeout=1)
            self.assertEqual(seen, [b'id'])
            self.assertTrue(msg.processed)

            never = asyncio.create_task(asyncio.sleep(10))
            reader._handler_tasks.add(never)
            await reader.wait_handler_tasks(timeout=0)
            self.assertTrue(never.cancelled())

            async def explode():
                raise RuntimeError('task failed')

            task = asyncio.create_task(explode())
            await asyncio.sleep(0)
            reader._log_handler_task_error(task)

            cancelled = asyncio.create_task(asyncio.sleep(10))
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelled
            reader._log_handler_task_error(cancelled)

            reader.close()

        asyncio.run(run())

    def test_reader_message_callbacks_and_close_paths(self):
        async def run():
            class FakeMessage:

                def __init__(self):
                    self.requeued = False

                async def req(self, timeout=0):
                    self.timeout = timeout
                    self.requeued = True

            reader = Reader()
            conn = FakeConn()
            changed = []
            conn._on_rdy_changed_cb = changed.append
            msg = object()
            self.assertIs(reader._on_message(conn, msg), msg)
            self.assertEqual(changed, [])

            reader._is_subscribe = True
            reader._queue.put_nowait('queued')
            fut = next(reader.wait_messages())
            self.assertEqual(await fut, 'queued')

            reader.clean_closed = True
            reader._queue.put_nowait('closed')
            fut = next(reader.wait_messages())
            self.assertEqual(await fut, 'closed')

            fake_msg = FakeMessage()
            reader._queue.put_nowait(fake_msg)
            await reader.requeue_msg_closed()
            self.assertTrue(fake_msg.requeued)

            task = asyncio.create_task(asyncio.sleep(10))
            reader._lookupd_task = task
            reader._connections[conn.id] = conn
            await reader.clean_close()
            await asyncio.sleep(0)
            self.assertTrue(task.cancelled())
            self.assertTrue(conn.closed)

        asyncio.run(run())

    def test_reader_graceful_close_stops_and_requeues_in_flight(self):
        async def run():
            class FakeMessage:

                processed = False

                def __init__(self):
                    self.timeout = None

                async def req(self, timeout=0):
                    self.timeout = timeout
                    self.processed = True

            reader = Reader()
            conn = FakeConn()
            msg = FakeMessage()
            conn._in_flight_messages[b'id'] = msg
            reader._connections[conn.id] = conn
            reader._is_subscribe = True

            requeued = await reader.graceful_close(timeout=3)

            self.assertEqual(requeued, 1)
            self.assertEqual(msg.timeout, 3)
            self.assertIn((b'RDY', (0,), None), conn.commands)
            self.assertTrue(conn.closed)
            self.assertFalse(reader._is_subscribe)
            self.assertTrue(reader.clean_closed)

        asyncio.run(run())

    def test_reader_graceful_helper_edge_cases(self):
        async def run():
            class LegacyConn:

                id = 'legacy'
                closed = False

                def __init__(self):
                    self.commands = []

                def execute(self, command, *args, data=None):
                    self.commands.append((command, args, data))
                    fut = asyncio.get_running_loop().create_future()
                    fut.set_result(b'OK')
                    return fut

            class ProcessedMessage:

                processed = True

                async def req(self, timeout=0):
                    raise AssertionError("processed messages are skipped")

            class WarningMessage:

                processed = False

                async def req(self, timeout=0):
                    raise RuntimeWarning("already handled")

            class BadRequeueConn:

                async def requeue_in_flight(self, timeout=0):
                    raise RuntimeError("requeue failed")

            reader = Reader()
            self.assertEqual(await reader.stop_receiving(), [])
            self.assertEqual(await reader.requeue_in_flight(), 0)

            closed = FakeConn('closed')
            closed.closed = True
            reader._connections[closed.id] = closed
            self.assertEqual(await reader.stop_receiving(), [])

            legacy = LegacyConn()
            reader._connections = {legacy.id: legacy}
            self.assertEqual(await reader.stop_receiving(), [b'OK'])
            self.assertEqual(legacy.commands, [(b'RDY', (0,), None)])

            reader._queue.put_nowait(object())
            reader._queue.put_nowait(ProcessedMessage())
            reader._queue.put_nowait(WarningMessage())
            self.assertEqual(await reader.requeue_queued_messages(), 0)

            reader._connections = {'bad': BadRequeueConn()}
            self.assertEqual(await reader.requeue_in_flight(), 0)

            reader._connections = {}
            reader.close()

        asyncio.run(run())

    def test_create_reader_parses_addresses(self):
        async def run():
            fake_reader = Reader()
            fake_reader.connect = AsyncMock()
            with patch('asyncnsq.tcp.reader.Reader', return_value=fake_reader):
                result = await create_reader(
                    nsqd_tcp_addresses=['tcp://host:4150'])
            self.assertIs(result, fake_reader)
            fake_reader.close()

        asyncio.run(run())

    def test_create_reader_lookupd_and_default_paths(self):
        async def run():
            with patch('asyncnsq.tcp.reader.Reader') as reader_cls:
                fake_reader = reader_cls.return_value
                fake_reader.connect = AsyncMock()
                result = await create_reader(
                    lookupd_http_addresses=['http://lookupd:4161'])
            self.assertIs(result, fake_reader)
            kwargs = reader_cls.call_args.kwargs
            self.assertEqual(kwargs['lookupd_http_addresses'],
                             [('lookupd', 4161)])

            with patch('asyncnsq.tcp.reader.Reader') as reader_cls:
                fake_reader = reader_cls.return_value
                fake_reader.connect = AsyncMock()
                await create_reader()
            kwargs = reader_cls.call_args.kwargs
            self.assertEqual(kwargs['nsqd_tcp_addresses'],
                             [('127.0.0.1', 4150)])

        asyncio.run(run())


class WriterUnitTest(unittest.TestCase):

    def test_writer_methods_delegate_to_connection(self):
        async def run():
            writer = Writer()
            writer._conn = FakeConn()
            await writer.pub('topic', 'msg')
            await writer.dpub('topic', None, 'msg')
            await writer.mpub('topic', 'a', 'b')
            await writer.sub('topic', 'channel')
            await writer.auth('secret')
            self.assertEqual(writer._conn.commands[0],
                             (b'PUB', ('topic',), 'msg'))
            self.assertEqual(writer._conn.commands[1],
                             (b'DPUB', ('topic', 0), 'msg'))
            writer.close()

        asyncio.run(run())

    def test_writer_pub_uses_connection_fast_path_when_available(self):
        class FastPubConn(FakeConn):

            def pub(self, topic, message):
                self.commands.append(('fast_pub', (topic,), message))
                fut = asyncio.get_running_loop().create_future()
                fut.set_result(b'OK')
                return fut

        async def run():
            writer = Writer()
            writer._conn = FastPubConn()
            self.assertEqual(await writer.pub('topic', 'msg'), b'OK')
            self.assertEqual(writer._conn.commands,
                             [('fast_pub', ('topic',), 'msg')])
            writer.close()

        asyncio.run(run())

    def test_writer_callbacks_id_repr_and_execute_reconnect(self):
        async def run():
            writer = Writer()
            writer._conn = FakeConn()
            writer.rdy_state = 2
            changed = []
            writer._on_rdy_changed_cb = changed.append
            msg = object()
            self.assertIs(writer._on_message(msg), msg)
            self.assertEqual(writer.rdy_state, 1)
            self.assertEqual(changed, [writer.id])
            self.assertGreater(writer.last_message, 0)
            self.assertIn('Writer', repr(writer))

            async def reconnect():
                writer._conn = FakeConn('tcp://127.0.0.1:4151')

            writer._conn = None
            writer.reconnect = AsyncMock(side_effect=reconnect)
            await writer.execute(b'PUB', 'topic', data='body')
            self.assertEqual(writer._conn.id, 'tcp://127.0.0.1:4151')

            async def reconnect_closed():
                writer._conn = FakeConn('tcp://127.0.0.1:4152')

            writer._conn.closed = True
            writer.reconnect = AsyncMock(side_effect=reconnect_closed)
            await writer.execute(b'PUB', 'topic', data='body')
            self.assertEqual(writer._conn.id, 'tcp://127.0.0.1:4152')
            writer.close()

        asyncio.run(run())

    def test_writer_rejects_conflicting_compression(self):
        async def run():
            with self.assertRaises(ValueError):
                Writer(snappy=True, deflate=True)

        asyncio.run(run())

    def test_writer_connect_requires_auth_secret(self):
        async def run():
            conn = FakeConn(identify_resp=b'{"auth_required": true}')
            with patch('asyncnsq.tcp.writer.create_connection',
                       AsyncMock(return_value=conn)):
                writer = Writer()
                with self.assertRaises(Exception):
                    await writer.connect()

        asyncio.run(run())

    def test_writer_connect_with_auth_secret(self):
        async def run():
            conn = FakeConn(identify_resp=b'{"auth_required": true}')
            with patch('asyncnsq.tcp.writer.create_connection',
                       AsyncMock(return_value=conn)):
                writer = Writer(auth_secret=b'secret')
                await writer.connect()
            self.assertEqual(conn.auth_secret, 'secret')
            self.assertEqual(writer._status, consts.CONNECTED)
            conn._on_close(conn, None)
            self.assertEqual(writer._status, consts.CLOSED)
            writer.close()

        asyncio.run(run())

    def test_writer_reconnect_handles_close_errors(self):
        async def run():
            class BadCloseConn(FakeConn):

                def close(self):
                    raise RuntimeError('close failed')

            writer = Writer()
            writer._conn = FakeConn()
            with patch.object(writer, 'connect', AsyncMock()) as connect:
                await writer.reconnect()
            connect.assert_awaited_once_with()
            self.assertEqual(writer._status, consts.CLOSED)

            writer = Writer()
            writer._conn = BadCloseConn()
            with patch.object(writer, 'connect', AsyncMock()) as connect:
                await writer.reconnect()
            connect.assert_awaited_once_with()

        asyncio.run(run())

    def test_writer_auto_reconnect_retries_and_sets_status(self):
        async def run():
            writer = Writer()
            writer._status = consts.CLOSED
            writer.reconnect = AsyncMock(side_effect=[ConnectionError(), None])
            sleep_calls = 0

            async def fake_sleep(delay):
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls >= 2:
                    raise asyncio.CancelledError()

            with patch('asyncnsq.tcp.writer.asyncio.sleep', fake_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await writer.auto_reconnect()
            self.assertEqual(writer._status, consts.CONNECTED)
            self.assertEqual(writer.reconnect.await_count, 2)

        asyncio.run(run())

    def test_create_writer_connects_and_starts_reconnect_task(self):
        async def run():
            with patch.object(Writer, 'connect', AsyncMock()) as connect, \
                    patch.object(Writer, 'start_reconnect_task') as start:
                writer = await create_writer(host='host', port=4150)
            self.assertIsInstance(writer, Writer)
            connect.assert_awaited_once_with()
            start.assert_called_once_with()
            writer.close()

        asyncio.run(run())

    def test_writer_start_reconnect_task_and_close_cancels_it(self):
        async def run():
            writer = Writer()

            async def wait_forever():
                await asyncio.sleep(10)

            writer.auto_reconnect = wait_forever
            writer.start_reconnect_task()
            self.assertIsNotNone(writer._reconnect_task)
            writer.close()
            await asyncio.sleep(0)
            self.assertIsNone(writer._reconnect_task)

        asyncio.run(run())
