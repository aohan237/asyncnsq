import asyncio
import contextlib
import inspect
import json
import logging
import time
from asyncnsq.http import NsqLookupd
from asyncnsq.tcp.exceptions import ReaderError
from asyncnsq.tcp.reader_rdy import RdyControl
from functools import partial
from .connection import create_connection
from .consts import RDY, SUB
from ..utils import _convert_to_str, get_host_and_port

logger = logging.getLogger(__package__)


def _parse_address(address, default_port):
    if isinstance(address, (list, tuple)):
        host, port = address
    else:
        host, port = get_host_and_port(address)
    return host, int(port or default_port)


async def create_reader(nsqd_tcp_addresses=None, max_in_flight=42,
                        lookupd_http_addresses=None,
                        **kwargs):
    """"
    initial function to get consumer
    param: nsqd_tcp_addresses: tcp addrs with no protocol.
        such as ['127.0.0.1:4150','182.168.1.1:4150']
    param: max_in_flight: number of messages get but not finish or req
    param: lookupd_http_addresses: first priority.if provided nsqd will neglected
    """
    if lookupd_http_addresses:
        lookupd_http_addresses = [
            _parse_address(address, 4161)
            for address in lookupd_http_addresses
        ]
        reader = Reader(lookupd_http_addresses=lookupd_http_addresses,
                        max_in_flight=max_in_flight, **kwargs)
    else:
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = ['127.0.0.1:4150']
        nsqd_tcp_addresses = [
            _parse_address(address, 4150)
            for address in nsqd_tcp_addresses
        ]
        reader = Reader(nsqd_tcp_addresses=nsqd_tcp_addresses,
                        max_in_flight=max_in_flight, **kwargs)
    await reader.connect()
    return reader


class Reader:
    """
    NSQ tcp reader
    """

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, heartbeat_interval=30000,
                 feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0, consumer=False, log_level=None,
                 auth_secret=None, **kwargs):
        if snappy and deflate:
            raise ValueError("snappy and deflate cannot both be enabled")
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }
        self._config.update({
            key: value for key, value in kwargs.items()
            if value is not None
        })
        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._queue = asyncio.Queue()

        self._connections = {}

        self._idle_timeout = 10

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self.channel = None
        self._lookupd_task = None
        self._auth_secret = auth_secret.decode(
            'utf-8') if isinstance(auth_secret, bytes) else auth_secret
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight)
        self._message_handler = None
        self._handler_auto_fin = True
        self._handler_auto_requeue = True
        self._handler_direct = False
        self._handler_concurrency = 1
        self._handler_semaphore = None
        self._handler_workers = set()
        self._handler_tasks = set()

        self.clean_closed = False

    async def connect(self):
        logging.info('reader connecting')
        if self._nsqd_tcp_addresses:
            for host, port in self._nsqd_tcp_addresses:
                conn = await create_connection(
                    host, port, queue=self._queue)
                await self.prepare_conn(conn)
                self._connections[conn.id] = conn
            self._rdy_control.add_connections(self._connections)
        # RDY is only valid after SUB. subscribe() triggers the first RDY
        # update for every connection.

    async def prepare_conn(self, conn):
        conn._on_message = partial(self._on_message, conn)
        resp = await conn.identify(**self._config)
        resp = json.loads(_convert_to_str(resp))
        if resp.get('max_rdy_count') is not None:
            conn._max_rdy_count = int(resp['max_rdy_count'])
        if resp.get('auth_required') is True:
            if not self._auth_secret:
                conn.close()
                self.close()
                raise ReaderError("Auth secret is required for NSQ connection")
            resp = await conn.auth(self._auth_secret)

    def _on_message(self, conn, msg):
        conn._last_message = time.time()
        if self._message_handler is not None and self._handler_direct:
            self._dispatch_message(msg)
            return None
        if self._message_handler is not None:
            self._ensure_handler_workers()
        return msg

    def set_message_handler(self, handler, *, auto_fin=True,
                            auto_requeue=True, concurrency=None,
                            direct=False):
        """Route queued messages to a handler, or bypass the queue explicitly."""
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._message_handler = handler
        self._handler_auto_fin = auto_fin
        self._handler_auto_requeue = auto_requeue
        self._handler_direct = direct
        self._handler_concurrency = max(1, concurrency or 1)
        self._handler_semaphore = (
            asyncio.Semaphore(concurrency)
            if direct and concurrency is not None and concurrency > 0
            else None
        )
        self._cancel_handler_workers_nowait()
        self._ensure_handler_workers()

    def clear_message_handler(self):
        self._message_handler = None
        self._handler_direct = False
        self._handler_semaphore = None
        self._cancel_handler_workers_nowait()

    def _ensure_handler_workers(self):
        if self._message_handler is None or self._handler_direct:
            return
        active = {task for task in self._handler_workers if not task.done()}
        self._handler_workers = active
        missing = self._handler_concurrency - len(active)
        for _ in range(missing):
            task = asyncio.create_task(self._handler_worker())
            self._handler_workers.add(task)
            task.add_done_callback(self._handler_workers.discard)
            task.add_done_callback(self._log_handler_task_error)

    def _cancel_handler_workers_nowait(self):
        for task in tuple(self._handler_workers):
            task.cancel()

    async def _stop_handler_workers(self):
        tasks = tuple(self._handler_workers)
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._handler_workers.clear()

    async def _handler_worker(self):
        while True:
            msg = await self._queue.get()
            try:
                await self._handle_message_async(msg)
            finally:
                self._queue.task_done()

    def _dispatch_message(self, msg):
        if self._handler_semaphore is not None:
            self._track_handler_task(self._run_handler(msg))
            return
        try:
            result = self._message_handler(msg)
            if inspect.isawaitable(result):
                self._track_handler_task(self._finish_async_handler(msg, result))
                return
            self._finish_handled_message(msg)
        except Exception:
            logger.exception("message handler failed")
            self._requeue_failed_handler_message(msg)

    def _finish_handled_message(self, msg):
        if self._handler_auto_fin and not msg.processed:
            msg.fin_nowait()

    def _requeue_failed_handler_message(self, msg):
        if self._handler_auto_requeue and not msg.processed:
            try:
                msg.req_nowait(0)
            except Exception:
                logger.exception("failed to requeue message after handler error")

    def _track_handler_task(self, coro):
        task = asyncio.create_task(coro)
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)
        task.add_done_callback(self._log_handler_task_error)

    def _log_handler_task_error(self, task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("message handler task failed: %s", exc)

    async def _run_handler(self, msg):
        async with self._handler_semaphore:
            await self._handle_message_async(msg)

    async def _finish_async_handler(self, msg, awaitable):
        try:
            await awaitable
            self._finish_handled_message(msg)
        except Exception:
            logger.exception("message handler failed")
            self._requeue_failed_handler_message(msg)

    async def _handle_message_async(self, msg):
        try:
            result = self._message_handler(msg)
            if inspect.isawaitable(result):
                await result
            self._finish_handled_message(msg)
        except Exception:
            logger.exception("message handler failed")
            self._requeue_failed_handler_message(msg)

    async def wait_handler_tasks(self, timeout=None):
        if self._message_handler is not None and not self._handler_direct:
            self._ensure_handler_workers()
            if timeout is None:
                await self._queue.join()
            else:
                try:
                    async with asyncio.timeout(timeout):
                        await self._queue.join()
                except TimeoutError:
                    pass
        if not self._handler_tasks:
            return
        tasks = tuple(self._handler_tasks)
        if timeout is None:
            await asyncio.gather(*tasks, return_exceptions=True)
            return
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)

    async def _poll_lookupd(self, host, port):
        nsqlookup_conn = NsqLookupd(host, port)
        res = {'producers': []}
        try:
            res = await nsqlookup_conn.lookup(self.topic)
            logger.info('lookupd response')
            logger.info(res)
        except Exception as tmp:
            logger.error(tmp)
            logger.exception(tmp)
        finally:
            await nsqlookup_conn.close()

        for producer in res['producers']:
            host = producer['broadcast_address']
            port = producer['tcp_port']
            tmp_id = "tcp://{}:{}".format(host, port)
            if tmp_id not in self._connections:
                logger.debug(('host, port', host, port))
                conn = await create_connection(
                    host, port, queue=self._queue)
                logger.debug(('conn.id:', conn.id))
                await self.prepare_conn(conn)
                self._connections[conn.id] = conn
                self._rdy_control.add_connection(conn)
                if self._is_subscribe and self.channel is not None:
                    await self.sub(conn, self.topic, self.channel)
                    if conn._on_rdy_changed_cb is not None:
                        conn._on_rdy_changed_cb(conn.id)

    async def subscribe(self, topic, channel):
        self.topic = topic
        self.channel = channel
        self._is_subscribe = True
        if self._lookupd_http_addresses:
            await self._lookupd()
        for conn in self._connections.values():
            await self.sub(conn, topic, channel)
            if conn._on_rdy_changed_cb is not None:
                conn._on_rdy_changed_cb(conn.id)

        if self._lookupd_http_addresses and self._lookupd_task is None:
            self._lookupd_task = asyncio.create_task(self._lookupd_loop())

    async def sub(self, conn, topic, channel):
        await conn.execute(SUB, topic, channel)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe and (not self.clean_closed):
            fut = asyncio.create_task(self._queue.get())
            yield fut

        if self.clean_closed:
            while not self._queue.empty():
                fut = asyncio.create_task(self._queue.get())
                yield fut

    async def messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe and (not self.clean_closed):
            result = await self._queue.get()
            yield result

    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout)

    async def _lookupd(self):
        pollers = [
            self._poll_lookupd(host, port)
            for host, port in self._lookupd_http_addresses
        ]
        if pollers:
            await asyncio.gather(*pollers)

    async def _lookupd_loop(self):
        while self._is_subscribe and not self.clean_closed:
            await asyncio.sleep(self._lookupd_poll_time)
            await self._lookupd()

    async def requeue_msg_closed(self, timeout=0):
        logger.info("requeue_msg_closed")
        return await self.requeue_queued_messages(timeout)

    async def stop_receiving(self):
        """Set RDY 0 on all open connections before shutdown."""
        coros = []
        for conn in self._connections.values():
            if getattr(conn, 'closed', False):
                continue
            stop_receiving = getattr(conn, 'stop_receiving', None)
            if stop_receiving is not None:
                coros.append(stop_receiving())
            else:
                coros.append(conn.execute(RDY, 0))
        if not coros:
            return []
        return await asyncio.gather(*coros, return_exceptions=True)

    async def requeue_queued_messages(self, timeout=0):
        """Requeue messages still waiting in the local consumer queue."""
        requeued = 0
        while not self._queue.empty():
            result = await self._queue.get()
            try:
                req = getattr(result, 'req', None)
                if req is None or getattr(result, 'processed', False):
                    continue
                await req(timeout)
            except RuntimeWarning:
                pass
            else:
                requeued += 1
            finally:
                self._queue.task_done()
        return requeued

    async def requeue_in_flight(self, timeout=0):
        """Requeue messages already delivered to user code but unfinished."""
        coros = []
        for conn in self._connections.values():
            requeue = getattr(conn, 'requeue_in_flight', None)
            if requeue is not None:
                coros.append(requeue(timeout))
        if not coros:
            return 0
        results = await asyncio.gather(*coros, return_exceptions=True)
        requeued = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Failed to requeue in-flight messages: %s",
                               result)
            else:
                requeued += result
        return requeued

    async def _cancel_lookupd_task(self):
        if self._lookupd_task is None:
            return
        self._lookupd_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._lookupd_task
        self._lookupd_task = None

    async def graceful_close(self, *, requeue=True, timeout=0):
        logger.info("graceful_close")
        self._rdy_control.close()
        await self._cancel_lookupd_task()
        await self.stop_receiving()
        self._is_subscribe = False
        self.clean_closed = True
        await self._stop_handler_workers()
        requeued = 0
        if requeue:
            requeued += await self.requeue_queued_messages(timeout)
            requeued += await self.requeue_in_flight(timeout)
        for conn in self._connections.values():
            conn.close()
        return requeued

    async def clean_close(self, *, requeue=True, timeout=0):
        logger.info("clean_close")
        return await self.graceful_close(requeue=requeue, timeout=timeout)

    def close(self, *args):
        logger.info("reader closed")
        self._rdy_control.close()
        self._cancel_handler_workers_nowait()
        if self._lookupd_task is not None:
            self._lookupd_task.cancel()
        self._is_subscribe = False
        self.clean_closed = True
        for conn in self._connections.values():
            conn.close()
