import asyncio
import json
from ._testutils import run_until_complete, BaseTest
from asyncnsq.tcp.connection import create_connection, TcpConnection
from asyncnsq.tcp.exceptions import NSQAuthFailed, NSQUnauthorized
from asyncnsq.http.writer import NsqdHttpWriter
from asyncnsq.tcp.protocol import Reader, SnappyReader, DeflateReader
from asyncnsq.utils import _convert_to_str


class NsqConnectionTest(BaseTest):

    def setUp(self):
        self.topic = 'foo'
        self.host = '127.0.0.1'
        self.port = 4150
        super().setUp()
        self.http_writer = NsqdHttpWriter(
            self.host, self.port+1, loop=self.loop)
        create_topic_res = self.loop.run_until_complete(
            self.http_writer.create_topic(self.topic))
        print("create_topic_res", create_topic_res)
        self.assertEqual(create_topic_res, "")
        self.auth_secret = 'test_secret'

    def tearDown(self):
        super().tearDown()

    @run_until_complete
    async def test_basic_instance(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        self.assertIsInstance(conn, TcpConnection)
        self.assertTrue('TcpConnection' in conn.__repr__())
        self.assertTrue(host in conn.endpoint)
        self.assertTrue(str(port) in conn.endpoint)
        conn.close()
        self.assertEqual(conn.closed, True)

    @run_until_complete
    async def test_auth_fail_bad_secret(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            with self.assertRaises(NSQAuthFailed):
                await conn.auth('this is the wrong secret')
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_wrong_ip(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            with self.assertRaises(NSQAuthFailed):
                await conn.auth('test_ip')
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_needs_tls(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            with self.assertRaises(NSQAuthFailed):
                await conn.auth('test_tls')
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_bad_secret(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            with self.assertRaises(NSQAuthFailed):
                await conn.auth('this is the wrong secret')
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_no_pub(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            await conn.auth('test_no_pub')
            with self.assertRaises(NSQUnauthorized) as cm:
                await self._pub_sub_rdy_fin(conn)
            self.assertIn('AUTH failed for PUB', repr(cm.exception))
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_no_sub(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            await conn.auth('test_no_sub')
            with self.assertRaises(NSQUnauthorized) as cm:
                await self._pub_sub_rdy_fin(conn)
            self.assertIn('AUTH failed for SUB', repr(cm.exception))
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_wrong_topic(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            await conn.auth('test_topic')
            with self.assertRaises(NSQUnauthorized) as cm:
                await self._pub_sub_rdy_fin(conn)
            self.assertIn('AUTH failed for PUB', repr(cm.exception))
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_auth_fail_wrong_channel(self):
        host, port = '127.0.0.1', 4150
        conn = await create_connection(host=host, port=port,
                                       loop=self.loop)
        res = await conn.identify(feature_negotiation=True)
        res = json.loads(_convert_to_str(res))
        if res.get('auth_required') is True:
            await conn.auth('test_channel')
            with self.assertRaises(NSQUnauthorized) as cm:
                await self._pub_sub_rdy_fin(conn)
            self.assertIn('AUTH failed for PUB', repr(cm.exception))
            conn.close()
        else:
            conn.close()
            self.skipTest("no auth enabled")

    @run_until_complete
    async def test_tls(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        config = {'feature_negotiation': True, 'tls_v1': True,
                  'snappy': False, 'deflate': False
                  }

        res = await conn.identify(**config)
        self.assertTrue(res)
        conn.close()

    @run_until_complete
    async def test_snappy(self):
        print("test_snappy 1")
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)
        print("test_snappy conn")
        config = {'feature_negotiation': True, 'tls_v1': False,
                  'snappy': True, 'deflate': False
                  }
        self.assertIsInstance(conn._parser, Reader)
        config_res = await conn.identify(**config)
        print("test_snappy config", config_res)
        self.assertIsInstance(conn._parser, SnappyReader)

        config_res = json.loads(_convert_to_str(config_res))
        if config_res.get('auth_required') is True:
            await conn.auth(self.auth_secret)

        print("test_snappy")
        await self._pub_sub_rdy_fin(conn)
        conn.close()

    @run_until_complete
    async def test_deflate(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        config = {'feature_negotiation': True, 'tls_v1': False,
                  'snappy': False, 'deflate': True
                  }
        self.assertIsInstance(conn._parser, Reader)

        nego_res = await conn.identify(**config)
        print(nego_res)
        self.assertIsInstance(conn._parser, DeflateReader)

        nego_res = json.loads(_convert_to_str(nego_res))
        if nego_res.get('auth_required') is True:
            await conn.auth(self.auth_secret)

        await self._pub_sub_rdy_fin(conn)
        conn.close()

    @asyncio.coroutine
    async def _pub_sub_rdy_fin(self, conn):
        print("start _pub_sub_rdy_fin")
        print(conn.closed)
        ok = await conn.execute('PUB', 'foo', data=b'msg foo')
        print("_pub_sub_rdy_fin pub data", ok)
        self.assertEqual(ok, b'OK')
        await conn.execute(b'SUB', 'foo', 'bar')
        await conn.execute(b'RDY', 1)
        print("starting to get msg")
        msg = await conn._queue.get()
        print("get message", msg)
        self.assertEqual(msg.processed, False)
        await msg.fin()
        self.assertEqual(msg.processed, True)
        await conn.execute(b'CLS')

    @run_until_complete
    async def test_message(self):
        conn = await create_connection(host=self.host, port=self.port,
                                       loop=self.loop)

        resp = await conn.identify(feature_negotiation=True)
        resp = json.loads(_convert_to_str(resp))
        if resp.get('auth_required') is True:
            await conn.auth(self.auth_secret)

        ok = await conn.execute(b'PUB', self.topic, data=b'boom')
        self.assertEqual(ok, b'OK')
        res = await conn.execute(b'SUB', self.topic,  'boom')
        self.assertEqual(res, b"OK")
        await conn.execute(b'RDY', 1)

        msg = await conn._queue.get()
        self.assertEqual(msg.processed, False)

        await msg.touch()
        self.assertEqual(msg.processed, False)
        await msg.req(1)
        self.assertEqual(msg.processed, True)
        await conn.execute(b'RDY', 1)
        new_msg = await conn._queue.get()
        res = await new_msg.fin()
        self.assertEqual(res, b"OK")
        self.assertEqual(msg.processed, True)

        await conn.execute(b'CLS')
        conn.close()
