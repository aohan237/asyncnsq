
class NSQException(Exception):
    """XXX"""


class ProtocolError(NSQException):
    """XXX"""


class ReaderError(NSQException):
    pass


class WriterError(NSQException):
    pass


class NSQRequeueMessage(NSQException):
    pass


class NSQNoConnections(NSQException):
    pass


class NSQHttpError(NSQException):
    pass


class NSQFrameError(NSQException):
    pass


class NSQErrorCode(NSQException):
    fatal = True


class NSQInvalid(NSQErrorCode):
    """E_INVALID"""
    pass


class NSQBadBody(NSQErrorCode):
    """E_BAD_BODY"""
    pass


class NSQBadTopic(NSQErrorCode):
    """E_BAD_TOPIC"""
    pass


class NSQBadChannel(NSQErrorCode):
    """E_BAD_CHANNEL"""
    pass


class NSQBadMessage(NSQErrorCode):
    """E_BAD_MESSAGE"""
    pass


class NSQPutFailed(NSQErrorCode):
    """E_PUT_FAILED"""
    pass


class NSQPubFailed(NSQErrorCode):
    """E_PUB_FAILED"""


class NSQMPubFailed(NSQErrorCode):
    """E_MPUB_FAILED"""


class NSQDPubFailed(NSQErrorCode):
    """E_DPUB_FAILED"""


class NSQAuthDisabled(NSQErrorCode):
    """E_AUTH_DISABLED"""


class NSQAuthFailed(NSQErrorCode):
    """E_AUTH_FAILED"""


class NSQUnauthorized(NSQErrorCode):
    """E_UNAUTHORIZED"""


class NSQFinishFailed(NSQErrorCode):
    """E_FIN_FAILED"""
    fatal = False


class NSQRequeueFailed(NSQErrorCode):
    """E_REQ_FAILED"""
    fatal = False


class NSQTouchFailed(NSQErrorCode):
    """E_TOUCH_FAILED"""
    fatal = False


ERROR_CODES = {
    b'E_INVALID': NSQInvalid,
    b'E_BAD_BODY': NSQBadBody,
    b'E_BAD_TOPIC': NSQBadTopic,
    b'E_BAD_CHANNEL': NSQBadChannel,
    b'E_BAD_MESSAGE': NSQBadMessage,
    b'E_PUT_FAILED': NSQPutFailed,
    b'E_PUB_FAILED': NSQPubFailed,
    b'E_MPUB_FAILED': NSQMPubFailed,
    b'E_DPUB_FAILED': NSQDPubFailed,
    b'E_FINISH_FAILED': NSQFinishFailed,
    b'E_AUTH_DISABLED': NSQAuthDisabled,
    b'E_AUTH_FAILED': NSQAuthFailed,
    b'E_UNAUTHORIZED': NSQUnauthorized,
    b'E_FIN_FAILED': NSQFinishFailed,
    b'E_REQUEUE_FAILED': NSQRequeueFailed,
    b'E_REQ_FAILED': NSQRequeueFailed,
    b'E_TOUCH_FAILED': NSQTouchFailed
}

# https://groups.google.com/forum/#!msg/nsq-users/VSxdPtw2ZmY/kmQJJhZe4wEJ
# E_TOUCH_FAILED, fatal
# E_REQ_FAILED fatal
# E_FIN_FAILED


def make_error(code, error_message):
    return ERROR_CODES.get(code, NSQErrorCode)(error_message)


