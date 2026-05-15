from ._testutils import run_until_complete, BaseTest
from asyncnsq.http.lookupd import NsqLookupd


class NsqLookupdTest(BaseTest):
    """
    :see: http://nsq.io/components/nsqd.html
    """
    required_ports = (('127.0.0.1', 4161),)

    @run_until_complete
    async def test_ok(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.ping()
        self.assertEqual(res, 'OK')
        await conn.close()

    @run_until_complete
    async def test_info(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.info()
        self.assertTrue('version' in res)
        await conn.close()

    @run_until_complete
    async def test_lookup(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.lookup('foo')
        self.assertIn('producers', res)
        await conn.close()

    @run_until_complete
    async def test_topics(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.topics()
        self.assertIn('topics', res)
        await conn.close()

    @run_until_complete
    async def test_channels(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.channels('foo')
        self.assertIn('channels', res)
        await conn.close()

    @run_until_complete
    async def test_nodes(self):
        conn = NsqLookupd('127.0.0.1', 4161)
        res = await conn.nodes()
        self.assertIn('producers', res)
        await conn.close()
