import asyncio
import socket
import unittest
from functools import wraps


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        original = fun

        async def fun(test, *args, **kw):
            return original(test, *args, **kw)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        return asyncio.run(fun(test, *args, **kw))
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    required_ports = ()

    def setUp(self):
        for host, port in self.required_ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.2)
                if sock.connect_ex((host, port)) != 0:
                    self.skipTest(
                        "NSQ service is not available at {}:{}".format(
                            host, port))
