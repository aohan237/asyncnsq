import json
import ipaddress
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__package__)


def _parse_bool(value):
    if value is None:
        return None
    return str(value).lower() in ('1', 'true', 't', 'yes', 'y', 'on')


def create_auth_server(auth_secret=None):
    """Create a local NSQ auth server helper."""
    return AuthServer(auth_secret=auth_secret)


def create_dev_auth_server(argv=None):
    auth_server = AuthServer()

    auth_server.add_client('test_secret', 'test_client_id',
                           remote_ip='127.0.0.1', tls=False)
    auth_server.add_client('test_ip', 'test_client_id',
                           remote_ip='127.0.0.2', tls=False)
    auth_server.add_client('test_tls', 'test_client_id',
                           remote_ip='127.0.0.1', tls=True)

    auths = [auth_server.make_auth(permissions=['subscribe'])]
    auth_server.add_client('test_no_pub', 'test_client_id',
                           remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(permissions=['publish'])]
    auth_server.add_client('test_no_sub', 'test_client_id',
                           remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(topic='test_topic')]
    auth_server.add_client('test_topic', 'test_client_id',
                           remote_ip='127.0.0.1', tls=False, auths=auths)
    auths = [auth_server.make_auth(channels=['test_channel'])]
    auth_server.add_client('test_channel', 'test_client_id',
                           remote_ip='127.0.0.1', tls=False, auths=auths)

    return auth_server


class AuthServer:

    def __init__(self, auth_ttl=3600, auth_secret=None):
        self._auth_ttl = auth_ttl
        self._client_auths = {}
        if auth_secret:
            self.add_client(auth_secret, 'asyncnsq', remote_ip=None, tls=None)

    def make_auth(self, permissions=None, topic=None, channels=None):
        return {
            "permissions": permissions or ['subscribe', 'publish'],
            "topic": topic or '.*',
            "channels": channels or [".*"]
        }

    def add_client(self, secret, client_id, client_url=None, remote_ip=None,
                   tls=None, auths=None):
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

    def authorize(self, params):
        secret = params.get('auth_secret') or params.get('secret')
        remote_ip = params.get('remote_ip')
        remote_ip = ipaddress.ip_address(remote_ip) if remote_ip else None
        tls = _parse_bool(params.get('tls'))

        if not secret or secret not in self._client_auths:
            return None

        client = self._client_auths[secret]
        if client['remote_ip'] is not None and remote_ip != client['remote_ip']:
            return None

        if client['tls'] is not None and tls != client['tls']:
            return None

        client_auth = dict(client['auth'])
        client_auth['ttl'] = self._auth_ttl
        return client_auth

    def add_client_from_json(self, payload):
        secret = payload.pop('secret')
        client_id = payload.pop('client_id')
        return self.add_client(secret, client_id, **payload)

    def make_handler(self):
        auth_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "AsyncNsqAuth/1.0"

            def log_message(self, fmt, *args):
                logger.debug(fmt, *args)

            def _send_json(self, status, body):
                payload = json.dumps(body).encode('utf-8')
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _send_empty(self, status):
                self.send_response(status)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path != '/auth':
                    self._send_empty(404)
                    return

                query = {
                    key: values[-1]
                    for key, values in parse_qs(parsed.query).items()
                }
                client_auth = auth_server.authorize(query)
                if client_auth is None:
                    self._send_empty(401)
                    return
                self._send_json(200, client_auth)

            def do_POST(self):
                parsed = urlparse(self.path)
                if parsed.path != '/clients':
                    self._send_empty(404)
                    return

                content_length = int(self.headers.get('Content-Length', 0))
                payload = self.rfile.read(content_length)
                try:
                    client = auth_server.add_client_from_json(
                        json.loads(payload.decode('utf-8')))
                except Exception:
                    logger.debug("invalid auth client payload", exc_info=True)
                    self._send_empty(400)
                    return
                self._send_json(200, client)

        return Handler

    def serve(self, host='localhost', port=8080):
        return ThreadingHTTPServer((host, port), self.make_handler())

    def run(self, host='localhost', port=8080):
        server = self.serve(host, port)
        try:
            server.serve_forever()
        finally:
            server.server_close()


def main():
    create_dev_auth_server().run()


if __name__ == '__main__':  # pragma: no cover
    main()
