from collections import namedtuple
from .consts import TOUCH, REQ, FIN


__all__ = ['NsqMessage', 'NsqErrorMessage']


NsqErrorMessage = namedtuple('NsqError', ['code', 'msg'])
BaseMessage = namedtuple('NsqMessage',
                         'timestamp attempts message_id body conn')


class NsqMessage(BaseMessage):

    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls, *args, **kwargs)
        self._is_processed = False
        return self

    @property
    def processed(self):
        """True if message has been processed: finished or re-queued."""
        return self._is_processed

    async def fin(self):
        """Finish a message (indicate successful processing)

        :raises RuntimeWarning: in case message was processed earlier.
        """
        if self._is_processed:
            raise RuntimeWarning("Message has already been processed")
        fin_message = getattr(self.conn, 'fin_message', None)
        if fin_message is None:
            resp = await self.conn.execute(FIN, self.message_id)
        else:
            resp = fin_message(self.message_id)
        self._is_processed = True
        on_processed = getattr(self.conn, "_message_processed", None)
        if on_processed is not None:
            on_processed(self)
        return resp

    def fin_nowait(self):
        """Finish a message without creating an awaitable response."""
        if self._is_processed:
            raise RuntimeWarning("Message has already been processed")
        fin_message = getattr(self.conn, 'fin_message', None)
        if fin_message is None:
            raise RuntimeError("Connection does not support synchronous FIN")
        resp = fin_message(self.message_id)
        self._is_processed = True
        on_processed = getattr(self.conn, "_message_processed", None)
        if on_processed is not None:
            on_processed(self)
        return resp

    async def req(self, timeout=10):
        """Re-queue a message (indicate failure to process)

        :param timeout: ``int`` configured max timeout  0 is a special case
            that will not defer re-queueing.
        :raises RuntimeWarning: in case message was processed earlier.
        """
        if self._is_processed:
            raise RuntimeWarning("Message has already been processed")
        req_message = getattr(self.conn, 'req_message', None)
        if req_message is None:
            resp = await self.conn.execute(REQ, self.message_id, timeout)
        else:
            resp = req_message(self.message_id, timeout)
        self._is_processed = True
        on_processed = getattr(self.conn, "_message_processed", None)
        if on_processed is not None:
            on_processed(self)
        return resp

    def req_nowait(self, timeout=10):
        """Re-queue a message without creating an awaitable response."""
        if self._is_processed:
            raise RuntimeWarning("Message has already been processed")
        req_message = getattr(self.conn, 'req_message', None)
        if req_message is None:
            raise RuntimeError("Connection does not support synchronous REQ")
        resp = req_message(self.message_id, timeout)
        self._is_processed = True
        on_processed = getattr(self.conn, "_message_processed", None)
        if on_processed is not None:
            on_processed(self)
        return resp

    async def touch(self):
        """Reset the timeout for an in-flight message.
        :raises RuntimeWarning: in case message was processed earlier.
        """
        if self._is_processed:
            raise RuntimeWarning("Message has already been processed")
        touch_message = getattr(self.conn, 'touch_message', None)
        if touch_message is None:
            return await self.conn.execute(TOUCH, self.message_id)
        return touch_message(self.message_id)
