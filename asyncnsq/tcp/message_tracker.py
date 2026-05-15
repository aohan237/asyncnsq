class MessageTracker:
    """Track messages received from nsqd until they are FIN/REQed."""

    def __init__(self):
        self.messages = {}

    def add(self, msg):
        self.messages[msg.message_id] = msg

    def discard(self, msg):
        self.messages.pop(msg.message_id, None)

    def clear(self):
        self.messages.clear()

    async def requeue_unprocessed(self, timeout=0):
        requeued = 0
        for msg in list(self.messages.values()):
            if msg.processed:
                self.discard(msg)
                continue
            try:
                await msg.req(timeout)
            except RuntimeWarning:
                pass
            else:
                requeued += 1
        return requeued
