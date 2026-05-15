"""NSQ protocol parser.

:see: http://nsq.io/clients/tcp_protocol_spec.html
"""
import abc
import struct
import zlib
try:
    import snappy
except ImportError:  # pragma: no cover - depends on optional runtime package
    snappy = None
import logging

from . import consts
from .exceptions import ProtocolError
from ..utils import _convert_to_bytes

logger = logging.getLogger(__package__)


__all__ = ['Reader', 'DeflateReader', 'SnappyReader']

_INT32 = struct.Struct('>l')
_MESSAGE_HEADER = struct.Struct('>qH16s')


class BaseReader(metaclass=abc.ABCMeta):

    @abc.abstractmethod   # pragma: no cover
    def feed(self, chunk):
        """

        :return:
        """

    @abc.abstractmethod  # pragma: no cover
    def gets(self):
        """

        :return:
        """

    @abc.abstractmethod   # pragma: no cover
    def encode_command(self, cmd, *args, data=None):
        """

        :return:
        """


class BaseCompressReader(BaseReader):

    @abc.abstractmethod  # pragma: no cover
    def compress(self, data):
        """

        :param data:
        :return:
        """

    @abc.abstractmethod  # pragma: no cover
    def decompress(self, chunk):
        """

        :param chunk:
        :return:
        """

    def feed(self, chunk):
        if not chunk:
            return
        uncompressed = self.decompress(chunk)
        uncompressed and self._parser.feed(uncompressed)

    def gets(self):
        return self._parser.gets()

    def encode_command(self, cmd, *args, data=None):
        cmd = self._parser.encode_command(cmd, *args, data=data)
        return self.compress(cmd)

    def encode_pub(self, topic, data):
        cmd = self._parser.encode_pub(topic, data)
        return self.compress(cmd)


class DeflateReader(BaseCompressReader):

    def __init__(self, buffer=None, level=6):
        self._parser = Reader()
        wbits = -zlib.MAX_WBITS
        self._decompressor = zlib.decompressobj(wbits)
        self._compressor = zlib.compressobj(level, zlib.DEFLATED, wbits)
        buffer and self.feed(buffer)

    def compress(self, data):
        chunk = self._compressor.compress(data)
        compressed = chunk + self._compressor.flush(zlib.Z_SYNC_FLUSH)
        return compressed

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


class SnappyReader(BaseCompressReader):

    def __init__(self, buffer=None):
        if snappy is None:
            raise RuntimeError(
                "python-snappy is required when snappy compression is enabled")
        self._parser = Reader()
        self._decompressor = snappy.StreamDecompressor()
        self._compressor = snappy.StreamCompressor()
        buffer and self.feed(buffer)

    def compress(self, data):
        compressed = self._compressor.add_chunk(data, compress=True)
        return compressed

    def decompress(self, chunk):
        return self._decompressor.decompress(chunk)


def _encode_body(data):
    _data = _convert_to_bytes(data)
    result = _INT32.pack(len(_data)) + _data
    return result


class Reader(BaseReader):

    def __init__(self, buffer=None):

        self._buffer = bytearray()
        self._offset = 0
        self._payload_size = None
        self._is_header = False
        self._frame_type = None
        buffer and self.feed(buffer)

    @property
    def buffer(self):
        if self._offset == 0:
            return self._buffer
        return self._buffer[self._offset:]

    def feed(self, chunk):
        """Put raw chunk of data obtained from connection to buffer.
        :param data: ``bytes``, raw input data.
        """
        if not chunk:
            return
        self._buffer.extend(chunk)

    def gets(self):
        buffer_size = len(self._buffer) - self._offset
        if not self._is_header and buffer_size >= consts.DATA_SIZE:
            size = _INT32.unpack_from(self._buffer, self._offset)[0]
            self._payload_size = size
            self._is_header = True

        if (self._is_header and buffer_size >=
                consts.DATA_SIZE + self._payload_size):
            if self._payload_size < consts.FRAME_SIZE:
                raise ProtocolError("invalid frame size")

            start = self._offset + consts.DATA_SIZE

            self._frame_type = _INT32.unpack_from(self._buffer, start)[0]
            if self._frame_type not in (consts.FRAME_TYPE_RESPONSE,
                                        consts.FRAME_TYPE_ERROR,
                                        consts.FRAME_TYPE_MESSAGE):
                raise ProtocolError(
                    "invalid frame type: {}".format(self._frame_type))
            resp = self._parse_payload()
            self._reset()
            return resp
        return False

    def _reset(self):
        start = self._offset + consts.DATA_SIZE + self._payload_size
        self._offset = start
        if self._offset == len(self._buffer):
            self._buffer.clear()
            self._offset = 0
        elif self._offset > consts.MAX_CHUNK_SIZE:
            del self._buffer[:self._offset]
            self._offset = 0
        self._is_header = False
        self._payload_size = None
        self._frame_type = None

    def _parse_payload(self):

        response_type, response = self._frame_type, None
        if response_type == consts.FRAME_TYPE_RESPONSE:
            response = self._unpack_response()
        elif response_type == consts.FRAME_TYPE_ERROR:
            response = self._unpack_error()
        elif response_type == consts.FRAME_TYPE_MESSAGE:
            response = self._unpack_message()
        else:
            raise ProtocolError()
        return response_type, response

    def _unpack_error(self):
        start = self._offset + consts.DATA_SIZE + consts.FRAME_SIZE
        end = self._offset + consts.DATA_SIZE + self._payload_size
        error = bytes(self._buffer[start:end])
        code, _, msg = error.partition(b' ')
        return code, msg

    def _unpack_response(self):
        start = self._offset + consts.DATA_SIZE + consts.FRAME_SIZE
        end = self._offset + consts.DATA_SIZE + self._payload_size
        body = bytes(self._buffer[start:end])
        return body

    def _unpack_message(self):
        start = self._offset + consts.DATA_SIZE + consts.FRAME_SIZE
        end = self._offset + consts.DATA_SIZE + self._payload_size
        if end - start < consts.MSG_HEADER:
            raise ProtocolError("invalid message frame size")
        timestamp, attempts, msg_id = _MESSAGE_HEADER.unpack_from(
            self._buffer, start)
        body = bytes(self._buffer[start + consts.MSG_HEADER:end])
        return timestamp, attempts, msg_id, body

    def encode_pub(self, topic, data):
        topic_data = _convert_to_bytes(topic)
        body_data = _encode_body(data)
        return b''.join((consts.PUB, b' ', topic_data, consts.NL, body_data))

    def encode_command(self, cmd, *args, data=None):
        """XXX"""
        _cmd = _convert_to_bytes(cmd.upper().strip())
        _args = [_convert_to_bytes(a) for a in args]
        body_data, params_data = b'', b''

        if len(_args):
            params_data = b' ' + b' '.join(_args)

        if isinstance(data, (list, tuple)):
            data_encoded = [_encode_body(part) for part in data]
            num_parts = len(data_encoded)
            payload = _INT32.pack(num_parts) + b''.join(data_encoded)
            body_data = _INT32.pack(len(payload)) + payload
        elif data is not None:
            body_data = _encode_body(data)

        return b''.join((_cmd, params_data, consts.NL, body_data))
