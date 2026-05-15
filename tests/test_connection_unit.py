import asyncio
import struct
import unittest
from unittest.mock import AsyncMock, patch

from asyncnsq.tcp import consts
from asyncnsq.tcp.connection import TcpConnection, create_connection
from asyncnsq.tcp.exceptions import NSQBadTopic
from asyncnsq.tcp.protocol import DeflateReader, SnappyReader


def frame(frame_type, body):
    payload = struct.pack('>l', frame_type) + body
    return struct.pack('>l', len(payload)) + payload


def response(body):
    return frame(consts.FRAME_TYPE_RESPONSE, body)


def error(body):
    return frame(consts.FRAME_TYPE_ERROR, body)


def message(body=b'body', msg_id=b'1234567890abcdef'):
    payload = struct.pack('>qH16s', 1, 2, msg_id) + body
    return frame(consts.FRAME_TYPE_MESSAGE, payload)


class FakeStreamReader:

    def __init__(self):
        self.eof = False
        self.reads = asyncio.Queue()

    def at_eof(self):
        return self.eof

    async def read(self, n):
        data = await self.reads.get()
        if data is None:
            self.eof = True
            return b''
        return data

    async def readexactly(self, n):
        return await self.read(n)


class FakeStreamWriter:

    def __init__(self):
        self.writes = []
        self.closed = False
        self.transport = self

    def write(self, data):
        self.writes.append(data)

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    async def start_tls(self, ssl_context, server_hostname=None):
        self.ssl_context = ssl_context
        self.server_hostname = server_hostname

    def pause_reading(self):
        pass

    def get_extra_info(self, key, default=None):
        return default


class TcpConnectionUnitTest(unittest.TestCase):

    def make_conn(self):
        reader = FakeStreamReader()
        writer = FakeStreamWriter()
        conn = TcpConnection(reader, writer, '127.0.0.1', 4150)
        conn.connect()
        return conn, reader, writer

    def test_connect_execute_response_and_close(self):
        async def run():
            conn, _, writer = self.make_conn()
            self.assertEqual(writer.writes[0], consts.MAGIC_V2)
            fut = conn.execute(b'PUB', 'topic', data='body')
            self.assertEqual(len(conn._cmd_waiters), 1)
            conn._parser.feed(response(b'OK'))
            conn._read_buffer()
            self.assertEqual(await fut, b'OK')

            fut = conn.pub('topic', 'fast-body')
            self.assertEqual(len(conn._cmd_waiters), 1)
            self.assertIn(b'PUB topic', writer.writes[-1])
            conn._parser.feed(response(b'OK'))
            conn._read_buffer()
            self.assertEqual(await fut, b'OK')
            conn.close()
            self.assertTrue(writer.closed)
            self.assertTrue(conn.closed)

        asyncio.run(run())

    def test_create_connection_opens_stream_and_sends_magic(self):
        async def run():
            reader = FakeStreamReader()
            writer = FakeStreamWriter()
            open_connection = AsyncMock(return_value=(reader, writer))
            with patch('asyncnsq.tcp.connection.asyncio.open_connection',
                       open_connection):
                conn = await create_connection('127.0.0.1', 4151)

            open_connection.assert_awaited_once_with('127.0.0.1', 4151)
            self.assertEqual(writer.writes[0], consts.MAGIC_V2)
            conn.close()

        asyncio.run(run())

    def test_close_fails_pending_waiters_and_skips_closing_writer(self):
        async def run():
            conn, _, writer = self.make_conn()
            fut = conn.execute(b'PUB', 'topic', data='body')
            closed = []
            conn._on_close = lambda closed_conn, exc: closed.append(
                (closed_conn, exc))
            exc = RuntimeError('boom')
            conn._do_close(exc)
            with self.assertRaises(RuntimeError):
                await fut
            self.assertEqual(closed, [(conn, exc)])
            conn._do_close(RuntimeError('again'))
            self.assertEqual(len(closed), 1)

            writer.closed = True
            writes = len(writer.writes)
            conn._send_close()
            self.assertEqual(len(writer.writes), writes)

            conn._writer = None
            conn._send_close()

        asyncio.run(run())

    def test_execute_validates_arguments_and_closed_connection(self):
        async def run():
            conn, reader, _ = self.make_conn()
            with self.assertRaises(TypeError):
                conn.execute(None)
            with self.assertRaises(TypeError):
                conn.execute(b'PUB', None)
            reader.eof = True
            with self.assertRaises(ConnectionError):
                conn.execute(b'PUB', 'topic')
            conn.close()

        asyncio.run(run())

    def test_rejects_invalid_queue(self):
        async def run():
            with self.assertRaises(TypeError):
                TcpConnection(FakeStreamReader(), FakeStreamWriter(),
                              '127.0.0.1', 4150, queue=object())

        asyncio.run(run())

    def test_fire_and_forget_commands_return_ok_and_track_inflight(self):
        async def run():
            conn, _, _ = self.make_conn()
            changed = []
            conn._on_rdy_changed_cb = changed.append
            conn._in_flight = 2
            self.assertEqual(await conn.execute(b'FIN', b'id'), b'OK')
            self.assertEqual(conn.in_flight, 1)
            self.assertEqual(changed, [conn.id])
            self.assertEqual(conn.req_message(b'id', 5), b'OK')
            self.assertEqual(conn.in_flight, 0)
            self.assertEqual(conn.touch_message(b'id'), b'OK')
            self.assertEqual(await conn.execute(b'RDY', 1), b'OK')
            self.assertEqual(conn._rdy_count, 1)
            conn.close()

        asyncio.run(run())

    def test_rdy_notification_waits_until_low_water(self):
        async def run():
            conn, _, _ = self.make_conn()
            changed = []
            conn._on_rdy_changed_cb = changed.append
            conn._rdy_target = 10
            conn._rdy_low_water = 2
            conn._rdy_count = 5
            conn._in_flight = 4

            conn._notify_rdy_changed()
            self.assertEqual(changed, [])

            conn._rdy_count = 1
            conn._in_flight = 1
            conn._notify_rdy_changed()
            self.assertEqual(changed, [conn.id])
            conn.close()

        asyncio.run(run())

    def test_heartbeat_message_and_error_frames(self):
        async def run():
            conn, _, writer = self.make_conn()
            conn._parser.feed(response(consts.HEARTBEAT))
            conn._read_buffer()
            self.assertEqual(writer.writes[-1], consts.PULSE)

            conn._parser.feed(message())
            conn._read_buffer()
            msg = conn.queue.get_nowait()
            self.assertEqual(msg.body, b'body')
            self.assertEqual(conn.in_flight, 1)
            self.assertIn(msg.message_id, conn._in_flight_messages)
            self.assertEqual(await msg.touch(), b'OK')
            self.assertEqual(await msg.fin(), b'OK')
            self.assertEqual(conn.in_flight, 0)
            self.assertEqual(conn._in_flight_messages, {})

            errors = []
            fut = conn.execute(b'PUB', 'bad/topic', data='body',
                               cb=errors.append)
            conn._parser.feed(error(b'E_BAD_TOPIC bad topic'))
            conn._read_buffer()
            with self.assertRaises(NSQBadTopic):
                await fut
            self.assertEqual(errors, [(b'E_BAD_TOPIC', b'bad topic')])
            await asyncio.sleep(0)
            self.assertTrue(conn.closed)

        asyncio.run(run())

    def test_graceful_close_requeues_in_flight_messages(self):
        async def run():
            conn, _, writer = self.make_conn()
            conn._parser.feed(message())
            conn._read_buffer()
            msg = conn.queue.get_nowait()

            requeued = await conn.requeue_in_flight(timeout=0)

            self.assertEqual(requeued, 1)
            self.assertTrue(msg.processed)
            self.assertEqual(conn._in_flight_messages, {})
            self.assertTrue(any(write == b'REQ 1234567890abcdef 0\n'
                                for write in writer.writes))

            conn._parser.feed(message(msg_id=b'fedcba0987654321'))
            conn._read_buffer()
            await conn.graceful_close(timeout=2)

            self.assertTrue(writer.closed)
            self.assertTrue(conn.closed)
            self.assertTrue(any(write == b'RDY 0\n' for write in writer.writes))
            self.assertTrue(any(write == b'REQ fedcba0987654321 2\n'
                                for write in writer.writes))

        asyncio.run(run())

    def test_graceful_close_edge_cases(self):
        async def run():
            class RuntimeWarningMessage:

                processed = False
                message_id = b'runtime-warning'

                async def req(self, timeout=0):
                    raise RuntimeWarning("already processed")

            conn, _, _ = self.make_conn()
            conn.close()
            self.assertEqual(await conn.stop_receiving(), b'OK')
            self.assertEqual(await conn.requeue_in_flight(), 0)

            conn, _, _ = self.make_conn()
            msg = RuntimeWarningMessage()
            conn._in_flight_messages[msg.message_id] = msg
            self.assertEqual(await conn.requeue_in_flight(), 0)
            conn.close()

            conn, _, _ = self.make_conn()
            conn._parser.feed(message(msg_id=b'processed-message'))
            conn._read_buffer()
            msg = conn.queue.get_nowait()
            msg._is_processed = True
            self.assertEqual(await conn.requeue_in_flight(), 0)
            self.assertEqual(conn._in_flight_messages, {})
            conn.close()

            conn, _, _ = self.make_conn()
            conn.stop_receiving = AsyncMock(side_effect=RuntimeError('bad rdy'))
            await conn.graceful_close(requeue=False)
            self.assertTrue(conn.closed)

        asyncio.run(run())

    def test_nonfatal_error_does_not_close_connection(self):
        async def run():
            conn, _, _ = self.make_conn()
            conn._parser.feed(error(b'E_FIN_FAILED already timed out'))
            conn._read_buffer()
            await asyncio.sleep(0)
            self.assertFalse(conn.closed)
            conn.close()

        asyncio.run(run())

    def test_protocol_error_and_unmatched_response_paths(self):
        async def run():
            conn, _, _ = self.make_conn()
            payload = struct.pack('>l', 99) + b'bad'
            conn._parser.feed(struct.pack('>l', len(payload)) + payload)
            conn._read_buffer()
            await asyncio.sleep(0)
            self.assertTrue(conn.closed)

            conn, _, _ = self.make_conn()
            conn._parser.feed(response(b'OK'))
            self.assertTrue(conn._read_buffer() is None)
            conn.close()

            fut = asyncio.get_running_loop().create_future()
            fut.set_exception(RuntimeError('reader failed'))
            conn._on_reader_task_stopped(fut)
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            conn._on_reader_task_stopped(fut)

        asyncio.run(run())

    def test_reader_task_handles_read_errors_and_repr(self):
        async def run():
            class ErrorStreamReader(FakeStreamReader):

                async def read(self, n):
                    raise RuntimeError('read failed')

            reader = ErrorStreamReader()
            writer = FakeStreamWriter()
            conn = TcpConnection(reader, writer, '127.0.0.1', 4150)
            conn.connect()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertIn('TcpConnection', repr(conn))
            self.assertTrue(conn.closed)

        asyncio.run(run())

    def test_identify_ok_auth_reader_task_and_close_ok(self):
        async def run():
            conn, reader, _ = self.make_conn()
            task = asyncio.create_task(conn.identify(feature_negotiation=True))
            await asyncio.sleep(0)
            conn._parser.feed(response(b'OK'))
            conn._read_buffer()
            self.assertEqual(await task, b'OK')

            task = asyncio.create_task(conn.auth('secret'))
            await asyncio.sleep(0)
            conn._parser.feed(response(b'{"identity":"client"}'))
            conn._read_buffer()
            self.assertEqual(await task, b'{"identity":"client"}')

            reader.reads.put_nowait(None)
            await asyncio.sleep(0)
            conn.close()

            conn, _, _ = self.make_conn()
            conn._parser.feed(response(consts.CLOSE_OK))
            conn._read_buffer()
            conn.close()

        asyncio.run(run())

    def test_identify_tls_deflate_and_plain_json_paths(self):
        async def run():
            conn, _, _ = self.make_conn()
            task = asyncio.create_task(conn.identify())
            await asyncio.sleep(0)
            conn._parser.feed(response(b'{}'))
            conn._read_buffer()
            self.assertEqual(await task, b'{}')
            conn.close()

            conn, reader, _ = self.make_conn()

            async def send_tls_ack():
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                reader.reads.put_nowait(consts.BIN_OK)

            ack_task = asyncio.create_task(send_tls_ack())
            task = asyncio.create_task(conn.identify(tls_v1=True))
            await asyncio.sleep(0)
            conn._parser.feed(response(b'{"tls_v1": true}'))
            conn._read_buffer()
            self.assertEqual(await task, b'{"tls_v1": true}')
            await ack_task
            conn.close()

            conn, _, _ = self.make_conn()
            task = asyncio.create_task(conn.identify(deflate=True))
            await asyncio.sleep(0)
            conn._parser.feed(response(b'{"deflate": true}'))
            conn._read_buffer()
            await asyncio.sleep(0)

            compressor = DeflateReader()
            conn._parser.feed(compressor.compress(response(b'OK')))
            conn._read_buffer()
            self.assertEqual(await task, b'{"deflate": true}')
            conn.close()

        asyncio.run(run())

    def test_on_message_callback_can_transform_queue_value(self):
        async def run():
            conn, _, _ = self.make_conn()
            conn._on_message = lambda msg: {'id': msg.message_id}
            conn._parser.feed(message())
            conn._read_buffer()

            queued = conn.queue.get_nowait()

            self.assertEqual(queued, {'id': b'1234567890abcdef'})
            self.assertIn(b'1234567890abcdef', conn._in_flight_messages)
            conn.close()

            conn, _, _ = self.make_conn()
            conn._on_message = lambda msg: None
            conn._parser.feed(message())
            conn._read_buffer()
            self.assertTrue(conn.queue.empty())
            self.assertIn(b'1234567890abcdef', conn._in_flight_messages)
            conn.close()

        asyncio.run(run())

    def test_identify_rejects_bad_compression_upgrade_ack(self):
        async def run():
            conn, _, _ = self.make_conn()
            task = asyncio.create_task(conn.identify(snappy=True))
            await asyncio.sleep(0)
            conn._parser.feed(response(b'{"snappy": true}'))
            conn._read_buffer()
            await asyncio.sleep(0)

            compressor = SnappyReader()
            conn._parser.feed(compressor.compress(response(b'BAD')))
            conn._read_buffer()
            with self.assertRaises(RuntimeError):
                await task
            conn.close()

        asyncio.run(run())

    def test_tls_upgrade_success_and_failure(self):
        async def run():
            conn, reader, writer = self.make_conn()
            reader.reads.put_nowait(consts.BIN_OK)
            await conn._upgrade_to_tls()
            self.assertEqual(writer.server_hostname, '127.0.0.1')
            conn.close()

            conn, reader, _ = self.make_conn()
            reader.reads.put_nowait(b'not-ok')
            with self.assertRaises(RuntimeError):
                await conn._upgrade_to_tls()
            conn.close()

        asyncio.run(run())
