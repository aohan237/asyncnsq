import asyncio
import json
import threading
import unittest
from unittest.mock import patch

import httpx

from asyncnsq.http import auth as auth_module
from asyncnsq.http.auth import (
    AuthServer, _parse_bool, create_auth_server, create_dev_auth_server,
)
from asyncnsq.http.base import NsqHTTPConnection
from asyncnsq.http.lookupd import NsqLookupd
from asyncnsq.http.writer import NsqdHttpWriter
from asyncnsq.tcp.exceptions import NSQHttpError


class FakeResponse:

    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class FakeAsyncClient:

    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.requests = []
        self.is_closed = False
        self.responses = []
        self.__class__.instances.append(self)

    async def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse('OK')

    async def aclose(self):
        self.is_closed = True


class CaptureNsqdWriter(NsqdHttpWriter):

    def __init__(self):
        self.calls = []

    async def perform_request(self, method, url, params=None, body=None,
                              headers=None):
        self.calls.append((method, url, params, body, headers))
        return 'OK'


class CaptureLookupd(NsqLookupd):

    def __init__(self):
        self.calls = []

    async def perform_request(self, method, url, params=None, body=None,
                              headers=None):
        self.calls.append((method, url, params, body, headers))
        return 'OK'


class HttpConnectionTest(unittest.TestCase):

    def setUp(self):
        FakeAsyncClient.instances = []

    def test_perform_request_uses_httpx_and_decodes_json(self):
        async def run():
            with patch('asyncnsq.http.base.httpx.AsyncClient',
                       FakeAsyncClient):
                conn = NsqHTTPConnection('localhost', 4151)
                self.assertEqual(conn.endpoint, 'http://localhost:4151')
                self.assertEqual(repr(conn), '<NsqHTTPConnection: '
                                            "('localhost', 4151)>")
                client = conn._ensure_session()
                client.responses.append(FakeResponse('{"ok": true}'))
                result = await conn.perform_request(
                    'POST', '/path', {'a': 1}, {'b': 2})
                self.assertEqual(result, {'ok': True})
                self.assertEqual(
                    client.requests[0],
                    ('POST', '/path',
                     {'params': {'a': 1}, 'content': b'{"b": 2}',
                      'headers': None}),
                )
                await conn.close()
                self.assertTrue(client.is_closed)

        asyncio.run(run())

    def test_perform_request_returns_text_and_raises_for_http_errors(self):
        async def run():
            with patch('asyncnsq.http.base.httpx.AsyncClient',
                       FakeAsyncClient):
                conn = NsqHTTPConnection('localhost', 4151)
                client = conn._ensure_session()
                client.responses.append(FakeResponse('OK'))
                self.assertEqual(
                    await conn.perform_request('GET', 'ping'), 'OK')
                client.responses.append(FakeResponse('OK'))
                self.assertEqual(
                    await conn.perform_request(
                        'POST', 'pub', body='body',
                        headers={'Content-Type': 'text/plain'}),
                    'OK')
                self.assertEqual(
                    client.requests[-1],
                    ('POST', '/pub',
                     {'params': None, 'content': b'body',
                      'headers': {'Content-Type': 'text/plain'}}),
                )
                client.responses.append(FakeResponse('bad topic', 400))
                with self.assertRaises(NSQHttpError):
                    await conn.perform_request('POST', 'pub')
                await conn.close()

        asyncio.run(run())

    def test_context_manager_closes_client(self):
        async def run():
            with patch('asyncnsq.http.base.httpx.AsyncClient',
                       FakeAsyncClient):
                async with NsqHTTPConnection('localhost', 4151) as conn:
                    client = conn._session
                    self.assertIsNotNone(client)
                self.assertTrue(client.is_closed)

        asyncio.run(run())


class HttpEndpointTest(unittest.TestCase):

    def test_nsqd_writer_endpoints(self):
        async def run():
            conn = CaptureNsqdWriter()
            await conn.ping()
            await conn.info()
            await conn.stats()
            await conn.pub('topic', b'msg', defer=10)
            await conn.dpub('topic', 20, 'msg')
            await conn.mpub('topic', 'a', 'b', defer=30)
            with self.assertRaises(ValueError):
                await conn.mpub('topic')
            await conn.create_topic('topic')
            await conn.delete_topic('topic')
            await conn.create_channel('topic', 'channel')
            await conn.delete_channel('topic', 'channel')
            await conn.empty_topic('topic')
            await conn.empty_channel('topic', 'channel')
            await conn.channel_empty('topic', 'channel')
            await conn.topic_pause('topic')
            await conn.topic_unpause('topic')
            await conn.pause_channel('channel', 'topic')
            await conn.unpause_channel('channel', 'topic')
            await conn.debug_pprof()
            await conn.debug_pprof_profile()
            await conn.debug_pprof_goroutine()
            await conn.debug_pprof_heap()
            await conn.debug_pprof_block()
            await conn.debug_pprof_threadcreate()
            await conn.nsqlookupd_tcp_addresses()
            await conn.set_nsqlookupd_tcp_addresses(['127.0.0.1:4160'])

            self.assertIn(
                ('POST', 'pub', {'topic': 'topic', 'defer': 10},
                 b'msg', None),
                conn.calls,
            )
            self.assertIn(
                ('PUT', 'config/nsqlookupd_tcp_addresses', None,
                 ['127.0.0.1:4160'], None),
                conn.calls,
            )

        asyncio.run(run())

    def test_lookupd_endpoints(self):
        async def run():
            conn = CaptureLookupd()
            await conn.ping()
            await conn.info()
            await conn.lookup('topic')
            await conn.topics()
            await conn.channels('topic')
            await conn.nodes()
            await conn.create_topic('topic')
            await conn.delete_topic('topic')
            await conn.create_channel('topic', 'channel')
            await conn.delete_channel('topic', 'channel')
            await conn.tombstone_topic_producer('topic', 'node:4151')

            self.assertEqual(
                conn.calls[-1],
                ('POST', 'topic/tombstone',
                 {'topic': 'topic', 'node': 'node:4151'}, None, None),
            )

        asyncio.run(run())


class AuthServerTest(unittest.TestCase):

    def test_authorize_and_reject(self):
        server = AuthServer(auth_ttl=123)
        server.add_client('secret', 'client', remote_ip='127.0.0.1',
                          tls=False)
        auth = server.authorize({
            'auth_secret': 'secret',
            'remote_ip': '127.0.0.1',
            'tls': 'false',
        })
        self.assertEqual(auth['ttl'], 123)
        self.assertEqual(auth['identity'], 'client')
        self.assertIsNone(server.authorize({'auth_secret': 'missing'}))
        self.assertIsNone(server.authorize({
            'auth_secret': 'secret',
            'remote_ip': '127.0.0.2',
            'tls': 'false',
        }))
        self.assertIsNone(server.authorize({
            'auth_secret': 'secret',
            'remote_ip': '127.0.0.1',
            'tls': 'true',
        }))

    def test_create_dev_auth_server(self):
        server = create_dev_auth_server()
        auth = server.authorize({
            'auth_secret': 'test_secret',
            'remote_ip': '127.0.0.1',
            'tls': 'false',
        })
        self.assertEqual(auth['identity'], 'test_client_id')
        self.assertIsNone(server.authorize({
            'auth_secret': 'test_tls',
            'remote_ip': '127.0.0.1',
            'tls': 'false',
        }))
        no_pub = server.authorize({
            'auth_secret': 'test_no_pub',
            'remote_ip': '127.0.0.1',
            'tls': 'false',
        })
        self.assertEqual(no_pub['authorizations'][0]['permissions'],
                         ['subscribe'])

    def test_parse_bool(self):
        self.assertTrue(_parse_bool('true'))
        self.assertTrue(_parse_bool('1'))
        self.assertFalse(_parse_bool('false'))
        self.assertIsNone(_parse_bool(None))

    def test_create_auth_server_helper(self):
        server = create_auth_server('secret')
        self.assertIsNotNone(server.authorize({'auth_secret': 'secret'}))

    def test_http_handler(self):
        server = AuthServer()
        server.add_client('secret', 'client', remote_ip='127.0.0.1',
                          tls=False)
        httpd = server.serve('127.0.0.1', 0)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        host, port = httpd.server_address
        try:
            with httpx.Client() as client:
                resp = client.get(
                    f'http://{host}:{port}/auth',
                    params={'auth_secret': 'secret',
                            'remote_ip': '127.0.0.1',
                            'tls': 'false'})
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()['identity'], 'client')

                resp = client.get(f'http://{host}:{port}/missing')
                self.assertEqual(resp.status_code, 404)

                resp = client.get(
                    f'http://{host}:{port}/auth',
                    params={'auth_secret': 'missing'})
                self.assertEqual(resp.status_code, 401)

                resp = client.post(f'http://{host}:{port}/missing')
                self.assertEqual(resp.status_code, 404)

                resp = client.post(f'http://{host}:{port}/clients',
                                   content=b'{bad json')
                self.assertEqual(resp.status_code, 400)

                resp = client.post(
                    f'http://{host}:{port}/clients',
                    content=json.dumps({
                        'secret': 'new',
                        'client_id': 'new-client',
                    }))
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()['auth']['identity'], 'new-client')
        finally:
            httpd.shutdown()
            thread.join(timeout=2)
            httpd.server_close()

    def test_run_and_main(self):
        class FakeHTTPD:

            def __init__(self):
                self.served = False
                self.closed = False

            def serve_forever(self):
                self.served = True

            def server_close(self):
                self.closed = True

        server = AuthServer()
        fake = FakeHTTPD()
        with patch.object(server, 'serve', return_value=fake):
            server.run('127.0.0.1', 1234)
        self.assertTrue(fake.served)
        self.assertTrue(fake.closed)

        with patch('asyncnsq.http.auth.create_dev_auth_server') as helper:
            auth_module.main()
        helper.return_value.run.assert_called_once_with()
