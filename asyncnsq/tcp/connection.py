import asyncio
import json
import ssl
import logging

from collections import deque

from . import consts
from .messages import NsqMessage
from .message_tracker import MessageTracker
from .exceptions import ProtocolError, make_error
from .protocol import Reader, DeflateReader, SnappyReader
from ..utils import _convert_to_bytes

logger = logging.getLogger(__package__)

_NO_RESPONSE_COMMANDS = {
    consts.NOP,
    consts.FIN,
    consts.RDY,
    consts.REQ,
    consts.TOUCH,
}


async def create_connection(host='localhost', port=4150, queue=None):
    """create nsq tcp connection
    Args:
        host: host address
        port: host port
        queue: user define asyncio queue
    Return:
        TcpConnection
    """
    reader, writer = await asyncio.open_connection(host, port)
    conn = TcpConnection(reader, writer, host, port, queue=queue)
    conn.connect()
    return conn


class TcpConnection:
    """
    base nsq connection class ,used for manipulate reader/writer content
    """

    def __init__(self, reader, writer, host, port, *, on_message=None,
                 queue=None, log_level=None):
        self._reader, self._writer = reader, writer
        self._host, self._port = host, port

        if queue is not None and not isinstance(queue, asyncio.Queue):
            raise TypeError("queue must be an asyncio.Queue or None")
        self._queue = queue or asyncio.Queue()

        self._parser = Reader()
        # next queue is used for nsq commands
        self._cmd_waiters = deque()
        self._ok_future = asyncio.get_running_loop().create_future()
        self._ok_future.set_result(b'OK')
        self._closing = False
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_data())
        # mark connection in upgrading state to ssl socket
        self._is_upgrading = False
        self._on_message = on_message
        self._on_close = None
        self._on_rdy_changed_cb = None
        self._close_callback_called = False

        # number of received but not acked or req messages
        self._in_flight = 0
        self._message_tracker = MessageTracker()
        self._in_flight_messages = self._message_tracker.messages
        self._rdy_count = 0
        self._max_rdy_count = 2500
        self._rdy_target = 0
        self._rdy_low_water = 1

    def connect(self):
        self._send_magic()

    def execute(self, command, *args, data=None, cb=None):
        """XXX"""
        self._validate_command(command, args)
        command_bytes = _convert_to_bytes(command).upper().strip()

        if command_bytes in _NO_RESPONSE_COMMANDS:
            self._execute_no_response(command_bytes, *args, data=data)
            return self._ok_future

        fut = asyncio.get_running_loop().create_future()
        self._cmd_waiters.append((fut, cb))

        command_raw = self._parser.encode_command(command, *args, data=data)
        logger.debug('execute command %s', command_raw)
        self._writer.write(command_raw)
        return fut

    def fin_message(self, message_id):
        return self._execute_no_response(consts.FIN, message_id)

    def req_message(self, message_id, timeout=0):
        return self._execute_no_response(consts.REQ, message_id, timeout)

    def touch_message(self, message_id):
        return self._execute_no_response(consts.TOUCH, message_id)

    def pub(self, topic, data):
        self._validate_command(consts.PUB, (topic,))
        fut = asyncio.get_running_loop().create_future()
        self._cmd_waiters.append((fut, None))
        command_raw = self._parser.encode_pub(topic, data)
        logger.debug('execute command %s', command_raw)
        self._writer.write(command_raw)
        return fut

    def _validate_command(self, command, args):
        if self.closed or self._reader is None or self._reader.at_eof():
            raise ConnectionError("Connection closed or corrupted")
        if command is None:
            raise TypeError("command must not be None")
        if any(arg is None for arg in args):
            raise TypeError("args must not contain None")

    def _execute_no_response(self, command, *args, data=None):
        self._validate_command(command, args)
        command_raw = self._parser.encode_command(command, *args, data=data)
        logger.debug('execute command %s', command_raw)
        self._writer.write(command_raw)

        if command == consts.RDY and args:
            self._rdy_count = max(0, int(args[0]))
        elif command in (consts.FIN, consts.REQ):
            self._in_flight = max(0, self._in_flight - 1)
            self._notify_rdy_changed()
        return b'OK'

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
            asyncio.get_running_loop().call_soon(self._do_close, None)
        return closed

    @property
    def queue(self):
        return self._queue

    def close(self):
        """Close connection."""
        self._do_close(send_cls=True)

    async def stop_receiving(self):
        """Stop nsqd from sending more messages on this connection."""
        if self.closed:
            return b'OK'
        return await self.execute(consts.RDY, 0)

    async def requeue_in_flight(self, timeout=0):
        """Requeue all known unprocessed messages for graceful shutdown."""
        if self.closed:
            return 0
        return await self._message_tracker.requeue_unprocessed(timeout)

    async def graceful_close(self, *, requeue=True, timeout=0):
        """Stop receiving, optionally requeue in-flight messages, then close."""
        if not self.closed:
            try:
                await self.stop_receiving()
            except Exception as exc:
                logger.warning("Failed to stop receiving on %s: %s", self.id,
                               exc)
            if requeue:
                await self.requeue_in_flight(timeout)
        self.close()

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
        if fut is not None:
            ok = await fut
            if ok != b'OK':
                raise RuntimeError(
                    "compression upgrade failed, got: {}".format(ok))
        return resp

    async def auth(self, secret):
        return await self.execute(b'AUTH', data=secret)

    def _do_close(self, exc=None, send_cls=False):
        if exc:
            logger.error("Connection closed with error: {}".format(exc))
        if self._closed:
            return
        if send_cls:
            self._send_close()
        self._closed = True
        self._closing = False
        self._rdy_count = 0
        self._message_tracker.clear()
        if self._writer is not None:
            self._writer.close()
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._on_close is not None and not self._close_callback_called:
            self._close_callback_called = True
            self._on_close(self, exc)
        close_exc = exc or ConnectionError("Connection closed")
        while self._cmd_waiters:
            waiter, _ = self._cmd_waiters.popleft()
            if not waiter.done():
                waiter.set_exception(close_exc)

    def _send_close(self):
        if self._writer is None or self._writer.is_closing():
            return
        self._writer.write(self._parser.encode_command(consts.CLS))

    def _send_magic(self):
        self._writer.write(consts.MAGIC_V2)

    def _pulse(self):
        nop = self._parser.encode_command(b'NOP')
        self._writer.write(nop)

    async def _upgrade_to_tls(self):
        self._reader_task.cancel()
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        await self._writer.start_tls(ssl_context, server_hostname=self._host)
        bin_ok = await self._reader.readexactly(10)
        if bin_ok != consts.BIN_OK:
            raise RuntimeError('Upgrade to TLS failed, got: {}'.format(bin_ok))
        self._reader_task = asyncio.create_task(self._read_data())
        self._reader_task.add_done_callback(self._on_reader_task_stopped)

    def _on_reader_task_stopped(self, future):
        if future.cancelled():
            return
        exc = future.exception()
        if exc is None:
            logger.debug('reader task stopped cleanly')
        else:
            logger.error('reader task stopped: {}'.format(exc))

    def _upgrade_to_snappy(self):
        self._parser = SnappyReader(self._parser.buffer)
        fut = asyncio.get_running_loop().create_future()
        self._cmd_waiters.append((fut, None))
        return fut

    def _upgrade_to_deflate(self):
        self._parser = DeflateReader(self._parser.buffer)
        fut = asyncio.get_running_loop().create_future()
        self._cmd_waiters.append((fut, None))
        return fut

    async def _read_data(self):
        """Response reader task."""
        is_canceled = False
        while not self._reader.at_eof():
            try:
                data = await self._reader.read(consts.MAX_CHUNK_SIZE)
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
        logger.debug("%s read to end, going to close", self.id)
        self._closing = True
        asyncio.get_running_loop().call_soon(self._do_close, None)

    def _parse_data(self):
        try:
            obj = self._parser.gets()
        except ProtocolError as exc:
            # ProtocolError is fatal
            # so connection must be closed
            logger.exception(exc)
            self._closing = True
            asyncio.get_running_loop().call_soon(self._do_close, exc)
            logger.error('ProtocolError is fatal')
            return
        else:
            if obj is False:
                return False
            logger.debug("got nsq data: %s", obj)
            resp_type, resp = obj
            hb = consts.HEARTBEAT
            if resp_type == consts.FRAME_TYPE_RESPONSE and resp == hb:
                self._pulse()
            elif resp_type == consts.FRAME_TYPE_RESPONSE:
                if resp == consts.CLOSE_OK:
                    logger.info('receive clean close,closed')
                if not self._cmd_waiters:
                    logger.debug("response without waiter: %s", resp)
                    return True
                waiter, cb = self._cmd_waiters.popleft()
                if not waiter.cancelled():
                    waiter.set_result(resp)
                    if cb is not None:
                        cb(resp)
            elif resp_type == consts.FRAME_TYPE_ERROR:
                error = make_error(*resp)
                waiter, cb = (None, None)
                if self._cmd_waiters:
                    waiter, cb = self._cmd_waiters.popleft()
                if waiter is not None and not waiter.cancelled():
                    waiter.set_exception(error)
                    if cb is not None:
                        cb(resp)
                if getattr(error, 'fatal', True):
                    self._closing = True
                    asyncio.get_running_loop().call_soon(self._do_close, error)
            elif resp_type == consts.FRAME_TYPE_MESSAGE:

                # track number in flight messages
                self._in_flight += 1
                self._rdy_count = max(0, self._rdy_count - 1)

                ts, att, msg_id, body = resp
                self._on_message_hook(ts, att, msg_id, body)
                # self._queue.put_nowait(msg)
            return True

    def _on_message_hook(self, ts, att, msg_id, body):
        msg = NsqMessage(ts, att, msg_id, body, self)
        self._message_tracker.add(msg)
        if self._on_message:
            msg = self._on_message(msg)
            if msg is None:
                return
        self._queue.put_nowait(msg)

    def _message_processed(self, msg):
        self._message_tracker.discard(msg)

    def _notify_rdy_changed(self):
        if self._on_rdy_changed_cb is None or self.closed:
            return
        target = self._rdy_target
        if target > 0:
            allocated = self._rdy_count + self._in_flight
            if allocated > self._rdy_low_water:
                return
        self._on_rdy_changed_cb(self.id)

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
        return '<TcpConnection: {}:{}>'.format(self._host, self._port)
