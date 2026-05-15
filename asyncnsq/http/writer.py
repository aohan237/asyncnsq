import struct

from .base import NsqHTTPConnection
from ..utils import _convert_to_bytes, _convert_to_str


def _encode_http_mpub_body(messages):
    parts = [_convert_to_bytes(message) for message in messages]
    payload = [struct.pack('>l', len(parts))]
    for part in parts:
        payload.append(struct.pack('>l', len(part)))
        payload.append(part)
    return b''.join(payload)


class NsqdHttpWriter(NsqHTTPConnection):
    """
    :see: http://nsq.io/components/nsqd.html
    """

    async def ping(self):
        """Monitoring endpoint.
        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return await self.perform_request('GET', 'ping', None, None)

    async def info(self):
        """Returns version information."""
        resp = await self.perform_request('GET', 'info', None, None)
        return resp

    async def stats(self):
        """Returns version information."""
        resp = await self.perform_request(
            'GET', 'stats', {'format': 'json'}, None)
        return resp

    async def pub(self, topic, message, defer=None):
        """Returns version information."""
        params = {'topic': topic}
        if defer is not None:
            params['defer'] = defer
        resp = await self.perform_request(
            'POST', 'pub', params, message)
        return resp

    async def dpub(self, topic, delay_time, message):
        """Publish a deferred message over HTTP."""
        return await self.pub(topic, message, defer=delay_time)

    async def mpub(self, topic, *messages, binary=False, defer=None):
        """Returns version information."""
        if not messages:
            raise ValueError("Specify one or more messages")
        params = {'topic': topic}
        if defer is not None:
            params['defer'] = defer
        if binary:
            params['binary'] = 'true'
            body = _encode_http_mpub_body(messages)
        else:
            _msgs = [_convert_to_str(m) for m in messages]
            body = '\n'.join(_msgs)
        resp = await self.perform_request(
            'POST', 'mpub', params, body)
        return resp

    async def create_topic(self, topic):
        resp = await self.perform_request(
            'POST', 'topic/create', {'topic': topic}, None)
        return resp

    async def delete_topic(self, topic):
        resp = await self.perform_request(
            'POST', 'topic/delete', {'topic': topic}, None)
        return resp

    async def create_channel(self, topic, channel):
        resp = await self.perform_request(
            'POST', 'channel/create', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def delete_channel(self, topic, channel):
        resp = await self.perform_request(
            'POST', 'channel/delete', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def empty_topic(self, topic):
        resp = await self.perform_request(
            'POST', 'topic/empty', {'topic': topic}, None)
        return resp

    async def empty_channel(self, topic, channel):
        resp = await self.perform_request(
            'POST', 'channel/empty', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def channel_empty(self, topic, channel):
        return await self.empty_channel(topic, channel)

    async def topic_pause(self, topic):
        resp = await self.perform_request(
            'POST', 'topic/pause', {'topic': topic}, None)
        return resp

    async def topic_unpause(self, topic):
        resp = await self.perform_request(
            'POST', 'topic/unpause', {'topic': topic}, None)
        return resp

    async def pause_channel(self, channel, topic):
        resp = await self.perform_request(
            'POST', 'channel/pause', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def unpause_channel(self, channel, topic):
        resp = await self.perform_request(
            'POST', 'channel/unpause', {'topic': topic, 'channel': channel},
            None)
        return resp

    async def debug_pprof(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof', None, None)
        return resp

    async def debug_pprof_profile(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof/profile', None, None)
        return resp

    async def debug_pprof_goroutine(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof/goroutine', None, None)
        return resp

    async def debug_pprof_heap(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof/heap', None, None)
        return resp

    async def debug_pprof_block(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof/block', None, None)
        return resp

    async def debug_pprof_threadcreate(self):
        resp = await self.perform_request(
            'GET', 'debug/pprof/threadcreate', None, None)
        return resp

    async def nsqlookupd_tcp_addresses(self):
        """
        List of nsqlookupd TCP addresses.
        """
        resp = await self.perform_request(
            'GET', 'config/nsqlookupd_tcp_addresses', None, None)
        return resp

    async def set_nsqlookupd_tcp_addresses(self, addresses):
        """
        Update nsqlookupd TCP addresses.
        """
        resp = await self.perform_request(
            'PUT', 'config/nsqlookupd_tcp_addresses', None, list(addresses))
        return resp
