from ._testutils import run_until_complete, BaseTest
from asyncnsq.http.writer import NsqdHttpWriter


class NsqdHttpWriterTest(BaseTest):

    @run_until_complete
    async def test_http_publish(self):

        http_writer = NsqdHttpWriter(
            "127.0.0.1", 4151, loop=self.loop)
        ok = await http_writer.pub('http_baz', 'producer msg')
        self.assertEqual(ok, 'OK')
        await http_writer.close()

    @run_until_complete
    async def test_http_mpublish(self):

        http_writer = NsqdHttpWriter(
            "127.0.0.1", 4151, loop=self.loop)
        messages = ['baz:1', b'baz:2', 3.14, 42]
        ok = await http_writer.mpub('http_baz', *messages)
        self.assertEqual(ok, 'OK')
        await http_writer.close()
