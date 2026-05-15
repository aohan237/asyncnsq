from ._testutils import run_until_complete, BaseTest
from asyncnsq.http.writer import NsqdHttpWriter


class NsqdHttpWriterTest(BaseTest):
    """
    :see: http://nsq.io/components/NsqdHttpWriter.html
    """
    required_ports = (('127.0.0.1', 4151),)

    @run_until_complete
    async def test_ok(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.ping()
        self.assertEqual(res, 'OK')
        await conn.close()

    @run_until_complete
    async def test_info(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.info()
        self.assertIn('version', res)
        await conn.close()

    @run_until_complete
    async def test_stats(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.stats()
        self.assertIn('version', res)
        await conn.close()

    @run_until_complete
    async def test_pub(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.pub('baz', 'baz_msg')
        self.assertEqual('OK', res)
        await conn.close()

    @run_until_complete
    async def test_mpub(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.mpub('baz', 'baz_msg:1', 'baz_msg:1')
        self.assertEqual('OK', res)
        await conn.close()

    @run_until_complete
    async def test_create_topic(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.create_topic('foo2')
        self.assertEqual('', res)
        await conn.close()

    @run_until_complete
    async def test_delete_topic(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.create_topic('foo2')
        self.assertEqual('', res)
        res = await conn.delete_topic('foo2')
        self.assertEqual('', res)
        await conn.close()

    @run_until_complete
    async def test_create_channel(self):
        conn = NsqdHttpWriter('127.0.0.1', 4151)
        res = await conn.create_topic('zap')
        self.assertEqual('', res)
        res = await conn.create_channel('zap', 'bar')
        self.assertEqual('', res)
        await conn.close()
