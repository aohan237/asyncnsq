import asyncio
import json

from asyncnsq.tcp.connection import create_connection
from asyncnsq.tcp.exceptions import ReaderError, WriterError
from asyncnsq.tcp.reader import create_reader
from asyncnsq.tcp.writer import create_writer
from asyncnsq.utils import _convert_to_str

from ._testutils import run_until_complete, BaseTest


class NsqTest(BaseTest):
    required_ports = (('127.0.0.1', 4150),)

    def setUp(self):
        self.topic = 'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        self.auth_secret = 'test_secret'
        super().setUp()

    @run_until_complete
    async def test_01_writer(self):
        nsq = await create_writer(host=self.host, port=self.port,
                                  heartbeat_interval=30000,
                                  feature_negotiation=True,
                                  tls_v1=True,
                                  snappy=False,
                                  deflate=False,
                                  deflate_level=0,
                                  auth_secret=self.auth_secret)
        for i in range(10):
            pub_res = await nsq.pub('foo', 'bar')
            self.assertEqual(pub_res, b"OK")
        nsq.close()

    @run_until_complete
    async def test_02_reader(self):
        nsq = await create_reader(nsqd_tcp_addresses=[
            f"{self.host}:{self.port}"],
            heartbeat_interval=30000,
            feature_negotiation=True,
            tls_v1=True,
            snappy=False,
            deflate=False,
            deflate_level=0,
            auth_secret=self.auth_secret)
        await nsq.subscribe('foo', 'bar')
        writer = await create_writer(host=self.host, port=self.port,
                                     heartbeat_interval=30000,
                                     feature_negotiation=True,
                                     tls_v1=True,
                                     snappy=False,
                                     deflate=False,
                                     deflate_level=0,
                                     auth_secret=self.auth_secret)
        for _ in range(10):
            pub_res = await writer.pub('foo', 'bar')
            self.assertEqual(pub_res, b"OK")
        num = 0
        try:
            while num < 10:
                msg = await asyncio.wait_for(nsq._queue.get(), timeout=5)
                num += 1
                fin_res = await msg.fin()
                self.assertEqual(fin_res, b"OK")
        finally:
            writer.close()
        nsq.close()

    async def _is_auth_required(self):
        conn = await create_connection(
            host=self.host,
            port=self.port
        )
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        auth_required = res.get('auth_required') or False
        conn.close()
        return auth_required

    @run_until_complete
    async def test_03_writer_fail_missing_secret(self):
        if await self._is_auth_required():
            with self.assertRaises(WriterError):
                _ = await create_writer(
                    host=self.host,
                    port=self.port,
                    feature_negotiation=True
                )
        else:
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_04_reader_fail_missing_secret(self):
        if await self._is_auth_required():
            with self.assertRaises(ReaderError):
                _ = await create_reader(
                    nsqd_tcp_addresses=[f"{self.host}:{self.port}"],
                    feature_negotiation=True
                )
        else:
            self.skipTest("no auth enabled")
