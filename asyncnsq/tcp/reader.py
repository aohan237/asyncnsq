import asyncio
import random
import logging
import time
from asyncnsq.http import NsqLookupd
from asyncnsq.tcp.reader_rdy import RdyControl
from functools import partial
from ..utils import retry_iterator
from .connection import create_connection
from .consts import SUB,RDY,CLS

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
                 sample_rate=0, consumer=False, log_level=None):
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
        self._queue = asyncio.Queue(loop=self._loop)

        self._connections = {}

        self._idle_timeout = 10

        self._rdy_control = None
        self._max_in_flight = max_in_flight
        self._num_readers = 0

        self._is_subscribe = False
        self._redistribute_timeout = 5  # sec
        self._lookupd_poll_time = 30  # sec
        self.topic = None
        self._rdy_control = RdyControl(idle_timeout=self._idle_timeout,
                                       max_in_flight=self._max_in_flight,
                                       loop=self._loop)

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
        _ = await conn.identify(**self._config)

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
    
    async def set_max_in_flight(self, max_in_flight):
        for conn in self._connections.values():
            conn.execute(RDY, max_in_flight)

    async def send_cls(self):
        for conn in self._connections.values():
            conn.execute(CLS)

    def wait_messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')

        while self._is_subscribe:
            fut = self._loop.create_task(self._queue.get())
            yield fut

    async def messages(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')
        
        try:
            logger.debug("num readers ++ ")
            self._num_readers += 1
            while self._is_subscribe:
                result = await self._queue.get()
                if result is not None:
                    yield result
                else:
                    logger.debug("got NONE, skip")
            logger.debug("leaving message receiving")
        finally:
            logger.debug("num readers -- ")
            self._num_readers -= 1


    async def _redistribute(self):
        while self._is_subscribe:
            self._rdy_control.redistribute()
            await asyncio.sleep(self._redistribute_timeout,
                                loop=self._loop)

    async def _lookupd(self):
        host, port = random.choice(self._lookupd_http_addresses)
        await self._poll_lookupd(host, port)
    
    async def unsubscribe(self):
        if not self._is_subscribe:
            raise ValueError('You must subscribe to the topic first')
        # logger.debug("unsubscribing {}".format(self))
        # mark as disabled
        await self.set_max_in_flight(0)
        # clear is_subscribed flag
        self._is_subscribe = False
        # send None to clear readers
        while self._num_readers > 0:
            for i in range(self._num_readers):
                self._queue.put_nowait(None)
            await asyncio.sleep(0.05)
        # no subscribers now

        # clear & req rest of the messages
        # mostly it is empty
        try:
            # consume all unfinished elems
            while not self._queue.empty():
                try:
                    result = self._queue.get_nowait()
                    if result is not None:
                        logger.debug("req: {}".format(result))
                        await result.req(0)
                except Exception as e:
                    logger.warning("req message failed {}".format(e))
        except Exception as e:
            logger.warning("requeue all messages failed {}".format(e))


    async def cancel(self, timeout = 10):
        if self._is_subscribe:
            await self.unsubscribe()
        try:
            # await self.send_cls()
            # clear rdy_controls
            if self._rdy_control is not None:
                self._rdy_control.stop_working()
            # close all connections
            for conn in self._connections.values():
                conn.close()
        except Exception as e:
            logger.error("close failed: {}".format(e))

    def close(self, timeout = 10):
        time_in = time.time()
        close_task = self._loop.create_task(self.cancel(timeout))
        timeout_generator = retry_iterator(init_delay=0.01, max_delay=1.0)
        while True:
            if close_task.cancelled():
                logger.info("reader closer cancelled..")
                return
            if close_task.done():
                logger.info("reader closer finished..")
                return
            now = time.time()
            if timeout > 0 and now - time_in > timeout:
                logger.warning("writer closer timeout")
                return
            t = next(timeout_generator)
            time.sleep(t)