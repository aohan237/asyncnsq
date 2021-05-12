import asyncio
import json
import logging
import random
import time
import signal
from asyncnsq.http import NsqLookupd
from asyncnsq.tcp.exceptions import ReaderError
from asyncnsq.tcp.reader_rdy import RdyControl
from functools import partial
from .connection import create_connection
from .consts import SUB
from ..utils import _convert_to_str

logger = logging.getLogger(__package__)


async def create_reader(nsqd_tcp_addresses=None, loop=None,
                        max_in_flight=42, lookupd_http_addresses=None,
                        **kwargs):
    """"
    initial function to get consumer
    param: nsqd_tcp_addresses: tcp addrs with no protocol.
        such as ['127.0.0.1:4150','182.168.1.1:4150']
    param: max_in_flight: number of messages get but not finish or req
    param: lookupd_http_addresses: first priority.if provided nsqd will neglected
    """
    loop = loop or asyncio.get_event_loop()
    if lookupd_http_addresses:
        reader = Reader(lookupd_http_addresses=lookupd_http_addresses,
                        max_in_flight=max_in_flight, loop=loop)
    else:
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = ['127.0.0.1:4150']
        nsqd_tcp_addresses = [i.split(':') for i in nsqd_tcp_addresses]
        reader = Reader(nsqd_tcp_addresses=nsqd_tcp_addresses,
                        max_in_flight=max_in_flight, loop=loop,
                        **kwargs)
    await reader.connect()
    return reader


class Reader:
    """
    NSQ tcp reader
    """

    def __init__(self, nsqd_tcp_addresses=None, lookupd_http_addresses=None,
                 max_in_flight=42, loop=None, heartbeat_interval=30000,
                 feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0, consumer=False, log_level=None, auth_secret=None):
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }
        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self._lookupd_http_addresses = lookupd_http_addresses or []

        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue()

        self._connections = {}

        self._idle_timeout = 10

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self._auth_secret = auth_secret.decode(
            'utf-8') if isinstance(auth_secret, bytes) else auth_secret
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)

        self.clean_closed = False

    async def connect(self):
        logging.info('reader connecting')
        if self._lookupd_http_addresses:
            """
            because lookupd must find a topic to find nsqds
            so, lookupd connect changed in to subscribe func
            """
            pass
        if self._nsqd_tcp_addresses:
            for host, port in self._nsqd_tcp_addresses:
                conn = await create_connection(
                    host, port, queue=self._queue,
                    loop=self._loop)
                await self.prepare_conn(conn)
                self._connections[conn.id] = conn
            self._rdy_control.add_connections(self._connections)
        # init distribute for conns, init update rdy state for conn
        self._rdy_control.redistribute()

    async def prepare_conn(self, conn):
        conn._on_message = partial(self._on_message, conn)
        resp = await conn.identify(**self._config)
        resp = json.loads(_convert_to_str(resp))
        if resp.get('auth_required') is True:
            if not self._auth_secret:
                conn.close()
                self.close()
                raise ReaderError("Auth secret is required for NSQ connection")
            resp = await conn.auth(self._auth_secret)

    def _on_message(self, conn, msg):
        conn._last_message = time.time()
        if conn._on_rdy_changed_cb is not None:
            conn._on_rdy_changed_cb(conn.id)
        return msg

    async def _poll_lookupd(self, host, port):
        nsqlookup_conn = NsqLookupd(host, port, loop=self._loop)
        try:
            res = await nsqlookup_conn.lookup(self.topic)
            logger.info('lookupd response')
            logger.info(res)
        except Exception as tmp:
            logger.error(tmp)
            logger.exception(tmp)

        for producer in res['producers']:
            host = producer['broadcast_address']
            port = producer['tcp_port']
            tmp_id = "tcp://{}:{}".format(host, port)
            if tmp_id not in self._connections:
                logger.debug(('host, port', host, port))
                conn = await create_connection(
                    host, port, queue=self._queue,
                    loop=self._loop)
                logger.debug(('conn.id:', conn.id))
                self._connections[conn.id] = conn
                self._rdy_control.add_connection(conn)
        await nsqlookup_conn.close()

    async def subscribe(self, topic, channel):
        self.topic = topic
        self._is_subscribe = True
        if self._lookupd_http_addresses:
            await self._lookupd()
        for conn in self._connections.values():
            await self.sub(conn, topic, channel)
            if conn._on_rdy_changed_cb is not None:
                conn._on_rdy_changed_cb(conn.id)

        # redistribute is a task for fail or overload to
        # rebalance tcpconnections
        # consider for further. disable now
        # self._redistribute_task = self._loop.create_task(self._redistribute())

    async def sub(self, conn, topic, channel):
        await conn.execute(SUB, topic, channel)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe and (not self.clean_closed):
            fut = self._loop.create_task(self._queue.get())
            yield fut

        if self.clean_closed:
            while not self._queue.empty():
                fut = self._loop.create_task(self._queue.get())
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
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

    async def _lookupd(self):
        host, port = random.choice(self._lookupd_http_addresses)
        await self._poll_lookupd(host, port)

    async def requeue_msg_closed(self):
        logger.info("requeue_msg_closed")
        if self.clean_closed:
            while not self._queue.empty():
                result = await self._queue.get()
                await result.req()

    async def clean_close(self):
        logger.info("clean_close")
        self._rdy_control.close()
        self.clean_closed = True
        await self.requeue_msg_closed()
        for conn in self._connections.values():
            conn.close()

    def close(self, *args):
        logger.info("reader closed")
        self._rdy_control.close()
        self.clean_closed = True
        for conn in self._connections.values():
            conn.close()
