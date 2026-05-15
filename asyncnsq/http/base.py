import json
import logging
import httpx
from asyncnsq.tcp.exceptions import NSQHttpError
from ..utils import _convert_to_bytes


logger = logging.getLogger(__package__)


class NsqHTTPConnection:
    """XXX"""

    def __init__(self, host='127.0.0.1', port=4150):
        self._endpoint = (host, port)
        self._base_url = 'http://{0}:{1}'.format(*self._endpoint)
        self._session = None

    @property
    def endpoint(self):
        return 'http://{0}:{1}'.format(*self._endpoint)

    async def __aenter__(self):
        self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _ensure_session(self):
        if self._session is None or self._session.is_closed:
            self._session = httpx.AsyncClient(base_url=self._base_url)
        return self._session

    async def close(self):
        if self._session is not None:
            return await self._session.aclose()

    async def perform_request(self, method, url, params=None, body=None,
                              headers=None):
        if body is None:
            request_body = None
        elif isinstance(body, (dict, list, tuple)):
            request_body = json.dumps(body).encode('utf-8')
        else:
            request_body = _convert_to_bytes(body)

        url = '/' + url.lstrip('/')
        session = self._ensure_session()

        resp = await session.request(method, url, params=params,
                                     content=request_body, headers=headers)

        resp_body = resp.text
        logger.debug(
            "resp= > %s, %s, %s, %s, %s, %s",
            resp_body, method, url, params, request_body, type(request_body))
        if resp.status_code >= 400:
            raise NSQHttpError(
                "HTTP {} for {} {}: {}".format(
                    resp.status_code, method, url, resp_body))
        try:
            response = json.loads(resp_body)
        except ValueError:
            return resp_body
        return response

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{}: {}>'.format(cls_name, self._endpoint)
