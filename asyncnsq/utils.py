import random
import re
import logging
from urllib.parse import urlparse


TOPIC_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')
CHANNEL_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')


def get_host_and_port(host):
    host_parsed = urlparse(host)
    if host_parsed.scheme and host_parsed.netloc:
        result = host_parsed.netloc
    elif host_parsed.scheme == '':
        result = host_parsed.path
    else:
        result = host
    result = result.split(':')
    if len(result) == 2:
        return result[0], result[-1]
    else:
        return result[0], None


def valid_topic_name(topic):
    if not 0 < len(topic) <= 64:
        return False
    return bool(TOPIC_NAME_RE.match(topic))


def valid_channel_name(channel):
    if not 0 < len(channel) <= 64:
        return False
    return bool(CHANNEL_NAME_RE.match(channel))


_converters_to_bytes_map = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
}


_converters_to_str_map = {
    str: lambda val: val,
    bytearray: lambda val: bytes(val).decode('utf-8'),
    bytes: lambda val: val.decode('utf-8'),
    int: lambda val: str(val),
    float: lambda val: str(val),
}


def _convert_to_bytes(value):
    if type(value) in _converters_to_bytes_map:
        converted_value = _converters_to_bytes_map[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


def _convert_to_str(value):
    if type(value) in _converters_to_str_map:
        converted_value = _converters_to_str_map[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


class MaxRetriesExided(Exception):
    pass


def retry_iterator(init_delay=0.1, max_delay=10.0, factor=2.7182818284590451,
                   jitter=0.11962656472, max_retries=None, now=True):
    """Based on twisted reconnection factory.

    :param init_delay:
    :param max_delay:
    :param factor:
    :param jitter:
    :param max_retries:
    :param now:
    :return:
    """
    retries, delay = 0, init_delay
    if now:
        retries += 1
        yield 0.0
    while not max_retries or retries < max_retries:
        retries += 1
        delay *= factor
        delay = random.normalvariate(
            delay, delay * jitter) if jitter else delay
        delay = min(delay, max_delay) if max_delay else delay
        yield delay
    else:
        raise MaxRetriesExided()


def get_logger(log_level=None):
    logger = logging.getLogger("AsyncNsq")
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - \n%(message)s"
    logging.basicConfig(format=FORMAT)
    if log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger
