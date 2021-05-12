import asyncio
import json
import ssl
import logging

from collections import deque

from . import consts
from .messages import NsqMessage
from .exceptions import ProtocolError, make_error
from .protocol import Reader, DeflateReader, SnappyReader

logger = logging.getLogger(__package__)


async def create_connection(host='localhost', port=4150,
                            queue=None, loop=None):
    """create nsq tcp connection
    Args:
        host: host address
        port: host port
        queue: user define asyncio queue
        loop: user define asyncio loop
    Return:
        TcpConnection
    """
    reader, writer = await asyncio.open_connection(
        host, port)
    conn = TcpConnection(reader, writer, host, port, queue=queue, loop=loop)
    conn.connect()
    return conn


class TcpConnection:
    """
    base nsq connection class ,used for manipulate reader/writer content
    """

    def __init__(self, reader, writer, host, port, *, on_message=None,
                 queue=None, loop=None, log_level=None):
        self._reader, self._writer = reader, writer
        self._host, self._port = host, port

        self._loop = loop or asyncio.get_event_loop()

        assert isinstance(queue, asyncio.Queue) or queue is None
        self._queue = queue or asyncio.Queue()

        self._parser = Reader()
        # next queue is used for nsq commands
        self._cmd_waiters = deque()
        self._closing = False
        self._closed = False
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        # mark connection in upgrading state to ssl socket
        self._is_upgrading = False
        self._on_message = on_message
        self._on_close = None

        # number of received but not acked or req messages
        self._in_flight = 0

    def connect(self):
        self._send_magic()

    def execute(self, command, *args, data=None, cb=None):
        """XXX"""
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        if command is None:
            raise TypeError("command must not be None")
        if None in set(args):
            raise TypeError("args must not contain None")
        fut = asyncio.Future(loop=self._loop)

        if command in (b'NOP', b'FIN', b'RDY', b'REQ', b'TOUCH'):
            fut.set_result(b'OK')
        else:
            self._cmd_waiters.append((fut, cb))

        command_raw = self._parser.encode_command(command, *args, data=data)
        logger.debug('execute command %s' % command_raw)
        self._writer.write(command_raw)

        # track all processed and requeued messages
        if command in (b'FIN', b'REQ', 'FIN', 'REQ'):
            self._in_flight = max(0,  self._in_flight - 1)
        return fut

    @property
    def in_flight(self):
        return self._in_flight

    @property
    def endpoint(self):
        return "tcp://{}:{}".format(self._host, self._port)

    @property
    def id(self):
        return self.endpoint

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    @property
    def queue(self):
        return self._queue

    def close(self):
        """Close connection."""
        self._do_close()

    async def identify(self, **config):
        # TODO: add config validator
        data = json.dumps(config)
        resp = await self.execute(
            b'IDENTIFY', data=data, cb=self._start_upgrading)
        if resp in (b'OK', 'OK'):
            self._finish_upgrading()
            return resp
        resp_config = json.loads(resp.decode('utf-8'))
        fut = None
        if resp_config.get('tls_v1'):
            await self._upgrade_to_tls()

        if resp_config.get('snappy'):
            fut = self._upgrade_to_snappy()
        elif resp_config.get('deflate'):
            fut = self._upgrade_to_deflate()
        self._finish_upgrading()
        if fut:
            ok = await fut
            assert ok == b'OK'
        return resp

    async def auth(self, secret):
        return await self.execute(b'AUTH', data=secret)

    def _do_close(self, exc=None):
        if exc:
            logger.error("Connection closed with error: {}".format(exc))
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        self._reader_task.cancel()
        self._send_close()

    def _send_close(self):
        self._writer.write(consts.CLS)

    def _send_magic(self):
        self._writer.write(consts.MAGIC_V2)

    def _pulse(self):
        nop = self._parser.encode_command(b'NOP')
        self._writer.write(nop)

    async def _upgrade_to_tls(self):
        self._reader_task.cancel()
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)

        self._reader, self._writer = await asyncio.open_connection(
            sock=raw_sock, ssl=ssl_context, loop=self._loop,
            server_hostname=self._host)
        bin_ok = await self._reader.readexactly(10)
        if bin_ok != consts.BIN_OK:
            raise RuntimeError('Upgrade to TLS failed, got: {}'.format(bin_ok))
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        self._reader_task.add_done_callback(self._on_reader_task_stopped)

    def _on_reader_task_stopped(self, future):
        exc = future.exception()
        logger.error('DONE: TASK {}'.format(exc))

    def _upgrade_to_snappy(self):
        self._parser = SnappyReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    def _upgrade_to_deflate(self):
        self._parser = DeflateReader(self._parser.buffer)
        fut = asyncio.Future(loop=self._loop)
        self._cmd_waiters.append((fut, None))
        return fut

    async def _read_data(self):
        """Response reader task."""
        is_canceled = False
        while not self._reader.at_eof():
            try:
                data = await self._reader.read(52)
            except asyncio.CancelledError:
                is_canceled = True
                logger.debug('Task is canceled')
                break
            except Exception as exc:
                logger.exception(exc)
                logger.debug("Reader task stopped due to: {}".format(exc))
                break
            self._parser.feed(data)
            not self._is_upgrading and self._read_buffer()

        if is_canceled:
            # useful during update to TLS, task canceled but connection
            # should not be closed
            return
        print(f"{self.id} is read to end ,going to close")
        self._closing = True
        self._loop.call_soon(self._do_close, None)

    def _parse_data(self):
        try:
            obj = self._parser.gets()
        except ProtocolError as exc:
            # ProtocolError is fatal
            # so connection must be closed
            logger.exception(exc)
            self._closing = True
            self._loop.call_soon(self._do_close, exc)
            logger.error('ProtocolError is fatal')
            return
        else:
            if obj is False:
                return False
            logger.debug("got nsq data: %s", obj)
            resp_type, resp = obj
            hb = consts.HEARTBEAT
            # print(resp_type, resp)
            if resp_type == consts.FRAME_TYPE_RESPONSE and resp == hb:
                self._pulse()
            elif resp_type == consts.FRAME_TYPE_RESPONSE:
                if resp == consts.CLOSE_OK:
                    logger.info('receive clean close,closed')
                waiter, cb = self._cmd_waiters.popleft()
                if not waiter.cancelled():
                    waiter.set_result(resp)
                    cb is not None and cb(resp)
            elif resp_type == consts.FRAME_TYPE_ERROR:
                waiter, cb = self._cmd_waiters.popleft()
                error = make_error(*resp)
                if not waiter.cancelled():
                    waiter.set_exception(error)
                    cb is not None and cb(resp)
            elif resp_type == consts.FRAME_TYPE_MESSAGE:

                # track number in flight messages
                self._in_flight += 1

                ts, att, msg_id, body = resp
                self._on_message_hook(ts, att, msg_id, body)
                # self._queue.put_nowait(msg)
            return True

    def _on_message_hook(self, ts, att, msg_id, body):
        msg = NsqMessage(ts, att, msg_id, body, self)
        if self._on_message:
            msg = self._on_message(msg)
        self._queue.put_nowait(msg)

    def _read_buffer(self):
        is_continue = True
        while is_continue:
            is_continue = self._parse_data()

    def _start_upgrading(self, resp=None):
        self._is_upgrading = True

    def _finish_upgrading(self, resp=None):
        self._read_buffer()
        self._is_upgrading = False

    def __repr__(self):
        return '<TcpConnection: {}:{}'.format(self._host, self._port)
