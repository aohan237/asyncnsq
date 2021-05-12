import aiohttp.web
import asyncio
import distutils.util
import ipaddress
import logging

logger = logging.getLogger(__package__)


async def create_auth_server(auth_secret, loop=None):
    """"
    initial function to get auth server
    """
    loop = loop or asyncio.get_event_loop()
    auth_server = AuthServer(auth_secret, loop=loop)
    return auth_server


def create_dev_auth_server(argv):
    auth_server = AuthServer()

    auth_server.add_client('test_secret', 'test_client_id', remote_ip='127.0.0.1', tls=False)
    auth_server.add_client('test_ip', 'test_client_id', remote_ip='127.0.0.2', tls=False)
    auth_server.add_client('test_tls', 'test_client_id', remote_ip='127.0.0.1', tls=True)

    auths = [auth_server.make_auth(permissions=['subscribe'])]
    auth_server.add_client('test_no_pub', 'test_client_id', remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(permissions=['publish'])]
    auth_server.add_client('test_no_sub', 'test_client_id', remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(topic='test_topic')]
    auth_server.add_client('test_topic', 'test_client_id', remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(channels=['test_channel'])]
    auth_server.add_client('test_channel', 'test_client_id', remote_ip='127.0.0.1', tls=False, auths=auths)

    return auth_server.make_app()


class AuthServer:

    def __init__(self, auth_ttl=3600, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._auth_ttl = auth_ttl
        self._client_auths = {}

    def make_auth(self, permissions=None, topic=None, channels=None):
        return {
            "permissions": permissions or ['subscribe', 'publish'],
            "topic": topic or '.*',
            "channels": channels or [".*"]
        }

    def add_client(self, secret, client_id, client_url=None, remote_ip=None, tls=None, auths=None):
        self._client_auths[secret] = {
            'auth': {
                'identity': client_id,
                'identity_url': client_url,
                'authorizations': auths or [self.make_auth()]
            },
            'remote_ip': ipaddress.ip_address(remote_ip) if remote_ip else None,
            'tls': tls
        }
        return self._client_auths[secret]

    async def _handle_clients(self, request):
        data = await request.json()
        secret = data.pop('secret')
        client_id = data.pop('client_id')
        client = self.add_client(secret, client_id, **data)
        return aiohttp.web.json_response(client)

    async def _handle_auth(self, request):
        params = request.rel_url.query
        secret = params.get('secret')
        remote_ip = params.get('remote_ip')
        remote_ip = ipaddress.ip_address(remote_ip) if remote_ip else None
        tls = params.get('tls')
        tls = bool(distutils.util.strtobool(tls)) if tls else None

        if not secret or secret not in self._client_auths:
            raise aiohttp.web.HTTPUnauthorized

        if remote_ip and remote_ip != self._client_auths[secret]['remote_ip']:
            raise aiohttp.web.HTTPUnauthorized

        if tls is not None and tls != self._client_auths[secret]['tls']:
            raise aiohttp.web.HTTPUnauthorized

        client_auth = self._client_auths[secret]['auth']
        client_auth.update(ttl=self._auth_ttl)
        return aiohttp.web.json_response(client_auth)

    async def make_app(self):
        app = aiohttp.web.Application()
        app.router.add_get('/auth', self._handle_auth)
        app.router.add_post('/clients', self._handle_clients)
        return app

    def run(self):
        aiohttp.web.run_app(self.make_app())
