import asyncio
import time
import logging
from . import consts
from ..utils import retry_iterator
from .connection import create_connection
from .consts import TOUCH, REQ, FIN, RDY, CLS, MPUB, PUB, SUB, AUTH, DPUB

logger = logging.getLogger(__package__)


async def create_writer(
        host='127.0.0.1', port=4150, loop=None, queue=None,
        heartbeat_interval=30000, feature_negotiation=True,
        tls_v1=False, snappy=False, deflate=False, deflate_level=6,
        consumer=False, sample_rate=0, log_level=None):
    """"
    param: host: host addr with no protocol. 127.0.0.1 
    param: port: host port 
    param: queue: queue where all the msg been put from the nsq 
    param: heartbeat_interval: heartbeat interval with nsq, set -1 to disable nsq heartbeat check
    params: snappy: snappy compress
    params: deflate: deflate compress  can't set True both with snappy
    """
    # TODO: add parameters type and value validation
    loop = loop or asyncio.get_event_loop()
    queue = queue or asyncio.Queue(loop=loop)
    writer = Writer(
        host=host, port=port, queue=queue,
        heartbeat_interval=heartbeat_interval,
        feature_negotiation=feature_negotiation,
        tls_v1=tls_v1, snappy=snappy, deflate=deflate,
        deflate_level=deflate_level, log_level=log_level,
        sample_rate=sample_rate, consumer=consumer, loop=loop)
    await writer.connect()
    return writer


class Writer:

    def __init__(self, host='127.0.0.1', port=4150, loop=None, queue=None,
                 heartbeat_interval=30000, feature_negotiation=True,
                 tls_v1=False, snappy=False, deflate=False, deflate_level=6,
                 sample_rate=0, consumer=False, max_in_flight=42,
                 log_level=None):
        # TODO: add parameters type and value validation
        self._config = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            'feature_negotiation': feature_negotiation,
        }

        self._host = host
        self._port = port
        self._conn = None
        self._loop = loop
        self._queue = queue or asyncio.Queue(loop=self._loop)
        self._status = consts.INIT
        self._on_rdy_changed_cb = None
        self._is_working = True
        self._auto_reconnect_task_closed = False
        self._auto_reconnect_task = self._loop.create_task(self.auto_reconnect())

    async def connect(self):
        logger.debug("writer init connect")
        self._conn = await create_connection(self._host, self._port,
                                             self._queue, loop=self._loop)

        self._conn._on_message = self._on_message
        await self._conn.identify(**self._config)
        self._status = consts.CONNECTED

    def _on_message(self, msg):
        # should not be coroutine
        # update connections rdy state
        self.rdy_state = int(self.rdy_state) - 1

        self._last_message = time.time()
        if self._on_rdy_changed_cb is not None:
            self._on_rdy_changed_cb(self.id)
        return msg

    @property
    def last_message(self):
        return self._last_message

    async def reconnect(self):
        logger.debug("writer reconnect")
        logger.debug(self._status)
        try:
            if self._conn:
                self._conn.close()
            self._status = consts.CLOSED
        except Exception as tmp:
            logger.info(
                'conn close failed,maybe its closed already or init')
            logger.exception(tmp)
        await self.connect()

    async def auto_reconnect(self):
        logger.debug("writer autoreconnect")
        timeout_generator = retry_iterator(init_delay=0.1, max_delay=10.0)
        try:
            while self._is_working:
                logger.debug("writer autoreconnect check loop")
                if not (self._status == consts.CONNECTED or self._status == consts.INIT):
                    logger.debug(
                        f"writer close({self._status})detected,reconnect")
                    conn_id = self.id if self._conn else 'init'
                    logger.info('reconnect writer{}'.format(conn_id))
                    try:
                        await self.reconnect()
                    except ConnectionError:
                        logger.error("Can not connect to: {}:{} ".format(
                            self._host, self._port))
                    else:
                        self._status = consts.CONNECTED
                t = next(timeout_generator)
                await asyncio.sleep(t, loop=self._loop)
        except asyncio.CancelledError:
            logger.info("{} auto_reconnect cancelled".format(self))
        finally:
            self._auto_reconnect_task_closed = True
            try:
                if self._conn and not self._conn.closed:
                    self._conn.close()
            except Exception as tmp:
                logger.info(
                    'conn close failed,maybe its closed already or init')
                logger.exception(tmp)

    async def execute(self, command, *args, data=None):
        if self._conn.closed:
            logger.debug(
                f"execute found conn closed, reconnect()")
            await self.reconnect()
        response = self._conn.execute(command, *args, data=data)
        return await response

    async def auth(self, secret):
        """

        :param secret:
        :return:
        """
        return await self.execute(AUTH, data=secret)

    async def sub(self, topic, channel):
        """

        :param topic:
        :param channel:
        :return:
        """
        self._is_subscribe = True

        return await self.execute(SUB, topic, channel)

    async def pub(self, topic, message):
        """

        :param topic:
        :param message:
        :return:
        """
        return await self.execute(PUB, topic, data=message)

    async def dpub(self, topic, delay_time, message):
        """

        :param topic:
        :param message:
        :param delay_time: delayed time in millisecond
        :return:
        """
        if not delay_time or delay_time is None:
            delay_time = 0
        return await self.execute(DPUB, topic, delay_time, data=message)

    async def mpub(self, topic, *messages):
        """

        :param topic:
        :param message:
        :param messages:
        :return:
        """
        msgs = list(messages)
        return await self.execute(MPUB, topic, data=msgs)

    @property
    def id(self):
        return self._conn.endpoint

    async def cancel(self, timeout = 10):
        time_in = time.time()
        self._is_working = False
        self._conn.close()
        while not self._conn.closed:
            await asyncio.sleep(0.1, loop=self._loop)
            now = time.time()
            if timeout > 0 and now - time_in > timeout:
                logger.warning("close failed: timeout")
        logger.info("closing _auto_reconnect_task")
        self._status = consts.CLOSED
        self._auto_reconnect_task.cancel()
        while not self._auto_reconnect_task_closed:
            await asyncio.sleep(0.1, loop=self._loop)
            now = time.time()
            if timeout > 0 and now - time_in > timeout:
                logger.warning("cancel failed: timeout")
        logger.info("_auto_reconnect_task_closed = {}".format(self._auto_reconnect_task_closed))

    def close(self, timeout = 10):
        time_in = time.time()
        close_task = self._loop.create_task(self.cancel(timeout))
        timeout_generator = retry_iterator(init_delay=0.01, max_delay=1.0)
        while True:
            if close_task.cancelled():
                logger.info("writer closer cancelled..")
                return
            if close_task.done():
                logger.info("writer closer finished..")
                return
            now = time.time()
            if timeout > 0 and now - time_in > timeout:
                logger.warning("writer closer timeout")
                return
            t = next(timeout_generator)
            time.sleep(t)

    def __repr__(self):
        return '<Writer{}>'.format(self._conn.__repr__())
