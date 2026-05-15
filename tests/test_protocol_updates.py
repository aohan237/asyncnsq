import asyncio
import struct
import unittest
from unittest.mock import patch

from asyncnsq.http.lookupd import NsqLookupd
from asyncnsq.http.writer import NsqdHttpWriter
from asyncnsq.tcp import consts
from asyncnsq.tcp.exceptions import ProtocolError
from asyncnsq.tcp.protocol import DeflateReader, Reader, SnappyReader
from asyncnsq.utils import valid_channel_name, valid_topic_name


class CaptureNsqdHttpWriter(NsqdHttpWriter):

    def __init__(self):
        self.calls = []

    async def perform_request(self, method, url, params=None, body=None,
                              headers=None):
        self.calls.append((method, url, params, body, headers))
        return 'OK'


class CaptureLookupd(NsqLookupd):

    def __init__(self):
        self.calls = []

    async def perform_request(self, method, url, params=None, body=None,
                              headers=None):
        self.calls.append((method, url, params, body, headers))
        return ''


class ProtocolUpdateTest(unittest.TestCase):

    def test_topic_and_channel_names_allow_current_nsq_limits(self):
        self.assertTrue(valid_topic_name('a' * 64))
        self.assertTrue(valid_topic_name('topic#ephemeral'))
        self.assertFalse(valid_topic_name('a' * 65))

        self.assertTrue(valid_channel_name('channel#ephemeral'))
        self.assertTrue(valid_channel_name('a' * 64))
        self.assertFalse(valid_channel_name('a' * 65))

    def test_http_mpub_binary_encoding(self):
        async def run():
            conn = CaptureNsqdHttpWriter()
            result = await conn.mpub('topic', b'a\nb', 'c', binary=True)
            self.assertEqual(result, 'OK')
            method, url, params, body, _ = conn.calls[0]
            expected = b''.join((
                struct.pack('>l', 2),
                struct.pack('>l', 3), b'a\nb',
                struct.pack('>l', 1), b'c',
            ))
            self.assertEqual(method, 'POST')
            self.assertEqual(url, 'mpub')
            self.assertEqual(params, {'topic': 'topic', 'binary': 'true'})
            self.assertEqual(body, expected)

        asyncio.run(run())

    def test_http_large_batch_and_deferred_publish_params(self):
        async def run():
            conn = CaptureNsqdHttpWriter()
            messages = [f'message-{i}'.encode() for i in range(1000)]

            result = await conn.mpub(
                'topic', *messages, binary=True, defer=60000)
            self.assertEqual(result, 'OK')
            method, url, params, body, _ = conn.calls[0]
            self.assertEqual(method, 'POST')
            self.assertEqual(url, 'mpub')
            self.assertEqual(params, {
                'topic': 'topic',
                'defer': 60000,
                'binary': 'true',
            })
            self.assertEqual(struct.unpack('>l', body[:4])[0], 1000)
            self.assertIn(b'message-999', body)

            await conn.dpub('topic', 45000, b'delayed-body')
            self.assertEqual(conn.calls[1],
                             ('POST', 'pub', {'topic': 'topic',
                                              'defer': 45000},
                              b'delayed-body', None))

        asyncio.run(run())

    def test_lookupd_tombstone_endpoint(self):
        async def run():
            conn = CaptureLookupd()
            result = await conn.tombstone_topic_producer(
                'topic', '127.0.0.1:4151')
            self.assertEqual(result, '')
            self.assertEqual(
                conn.calls[0],
                ('POST', 'topic/tombstone',
                 {'topic': 'topic', 'node': '127.0.0.1:4151'}, None, None),
            )

        asyncio.run(run())

    def test_tcp_parser_rejects_bad_frames(self):
        reader = Reader()
        reader.feed(b'')
        self.assertEqual(reader.buffer, bytearray())

        reader = Reader()
        reader.feed(struct.pack('>l', 2) + b'xx')
        with self.assertRaises(ProtocolError):
            reader.gets()

        reader = Reader()
        payload = struct.pack('>l', 99) + b'body'
        reader.feed(struct.pack('>l', len(payload)) + payload)
        with self.assertRaises(ProtocolError):
            reader.gets()

        reader = Reader()
        reader._frame_type = 99
        with self.assertRaises(ProtocolError):
            reader._parse_payload()

        reader = Reader()
        payload = struct.pack('>l', consts.FRAME_TYPE_MESSAGE) + b'too-small'
        reader.feed(struct.pack('>l', len(payload)) + payload)
        with self.assertRaises(ProtocolError):
            reader.gets()

    def test_parser_offsets_keep_unread_frames_without_copying_each_frame(self):
        def response_frame(body):
            payload = struct.pack('>l', consts.FRAME_TYPE_RESPONSE) + body
            return struct.pack('>l', len(payload)) + payload

        first = response_frame(b'first')
        second = response_frame(b'second')
        reader = Reader()
        reader.feed(first + second)

        self.assertEqual(reader.gets(), (consts.FRAME_TYPE_RESPONSE, b'first'))
        self.assertEqual(reader.buffer, bytearray(second))
        self.assertEqual(reader.gets(), (consts.FRAME_TYPE_RESPONSE, b'second'))
        self.assertEqual(reader.buffer, bytearray())

        large_first = response_frame(b'x' * (consts.MAX_CHUNK_SIZE + 1))
        reader.feed(large_first + second)
        self.assertEqual(
            reader.gets(),
            (consts.FRAME_TYPE_RESPONSE, b'x' * (consts.MAX_CHUNK_SIZE + 1)),
        )
        self.assertEqual(reader._offset, 0)
        self.assertEqual(reader.buffer, bytearray(second))

    def test_compressed_readers_round_trip_frames(self):
        payload = struct.pack('>l', consts.FRAME_TYPE_RESPONSE) + b'OK'
        frame = struct.pack('>l', len(payload)) + payload

        writer = DeflateReader()
        reader = DeflateReader()
        reader.feed(b'')
        reader.feed(writer.compress(frame))
        self.assertEqual(reader.gets(), (consts.FRAME_TYPE_RESPONSE, b'OK'))
        self.assertTrue(writer.encode_command(b'PUB', 'topic', data='body'))
        self.assertTrue(writer.encode_pub('topic', 'body'))

        writer = SnappyReader()
        reader = SnappyReader()
        reader.feed(b'')
        reader.feed(writer.compress(frame))
        self.assertEqual(reader.gets(), (consts.FRAME_TYPE_RESPONSE, b'OK'))
        self.assertTrue(writer.encode_command(b'PUB', 'topic', data='body'))
        self.assertTrue(writer.encode_pub('topic', 'body'))

        with patch('asyncnsq.tcp.protocol.snappy', None):
            with self.assertRaises(RuntimeError):
                SnappyReader()

    def test_parser_handles_large_message_body_in_chunks(self):
        body = b'x' * (1024 * 1024)
        msg_id = b'large-message-01'
        payload = struct.pack('>qH16s', 1, 3, msg_id) + body
        frame = struct.pack(
            '>l', len(payload) + 4) + struct.pack(
                '>l', consts.FRAME_TYPE_MESSAGE) + payload
        reader = Reader()

        for offset in range(0, len(frame), 8191):
            reader.feed(frame[offset:offset + 8191])
            if offset + 8191 < len(frame):
                self.assertFalse(reader.gets())

        frame_type, parsed = reader.gets()
        timestamp, attempts, parsed_msg_id, parsed_body = parsed
        self.assertEqual(frame_type, consts.FRAME_TYPE_MESSAGE)
        self.assertEqual(timestamp, 1)
        self.assertEqual(attempts, 3)
        self.assertEqual(parsed_msg_id, msg_id)
        self.assertEqual(parsed_body, body)

    def test_tcp_mpub_encodes_large_batch(self):
        messages = [f'item-{i}'.encode() * 32 for i in range(512)]
        command = Reader().encode_command(b'MPUB', 'topic', data=messages)
        header, body = command.split(b'\n', 1)
        body_size = struct.unpack('>l', body[:4])[0]
        count = struct.unpack('>l', body[4:8])[0]

        self.assertEqual(header, b'MPUB topic')
        self.assertEqual(body_size, len(body) - 4)
        self.assertEqual(count, len(messages))
        self.assertIn(messages[-1], body)
