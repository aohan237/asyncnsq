import asyncio

from .consts import RDY

REDISTRIBUTE = 0
CHANGE_CONN_RDY = 1
DEFAULT_MAX_RDY_COUNT = 2500


class RdyControl:

    def __init__(self, idle_timeout, max_in_flight):
        self._connections = {}
        self._idle_timeout = idle_timeout
        self._max_in_flight = max_in_flight

        self._cmd_queue = asyncio.Queue()
        self._is_working = True
        self._pending_conn_ids = set()
        self._redistribute_pending = False

        self._distributor_task = asyncio.create_task(self._distributor())

    def add_connections(self, connections):
        self._connections = connections
        for conn in self._connections.values():
            conn._on_rdy_changed_cb = self.rdy_changed

    def add_connection(self, connection):
        connection._on_rdy_changed_cb = self.rdy_changed
        self._connections[connection.id] = connection

    def rdy_changed(self, conn_id):
        if not self._is_working or conn_id in self._pending_conn_ids:
            return
        self._pending_conn_ids.add(conn_id)
        self._cmd_queue.put_nowait((CHANGE_CONN_RDY, (conn_id,)))

    def redistribute(self):
        if not self._is_working or self._redistribute_pending:
            return
        self._redistribute_pending = True
        self._cmd_queue.put_nowait((REDISTRIBUTE, ()))

    async def _distributor(self):
        while self._is_working:
            cmd, args = await self._cmd_queue.get()
            if cmd == REDISTRIBUTE:
                self._redistribute_pending = False
                await self._redistribute_rdy_state()
            elif cmd == CHANGE_CONN_RDY:
                conn_id = args[0]
                self._pending_conn_ids.discard(conn_id)
                await self._update_rdy(conn_id)
            else:
                raise RuntimeError("Should never be here")

    def remove_connection(self, conn):
        self._connections.pop(conn.id, None)
        self._pending_conn_ids.discard(conn.id)

    def remove_all(self):
        self._connections = {}
        self._pending_conn_ids.clear()

    def _active_connections(self):
        return [
            conn for conn in self._connections.values()
            if not getattr(conn, 'closed', False)
        ]

    def _target_rdy_by_id(self):
        connections = sorted(self._active_connections(), key=lambda conn: conn.id)
        if not connections or self._max_in_flight <= 0:
            return {}

        base, remainder = divmod(self._max_in_flight, len(connections))
        targets = {}
        for index, conn in enumerate(connections):
            target = base + (1 if index < remainder else 0)
            max_rdy_count = getattr(
                conn, '_max_rdy_count', DEFAULT_MAX_RDY_COUNT)
            targets[conn.id] = min(target, max_rdy_count)
        return targets

    async def _redistribute_rdy_state(self):
        targets = self._target_rdy_by_id()
        coros = [
            self._set_conn_rdy(conn, targets.get(conn.id, 0))
            for conn in self._active_connections()
        ]
        if coros:
            await asyncio.gather(*coros)

    async def _update_rdy(self, conn_id):
        conn = self._connections.get(conn_id)
        if conn is None or conn.closed:
            return
        target = self._target_rdy_by_id().get(conn_id, 0)
        if target <= 0:
            await self._set_conn_rdy(conn, 0)
            return

        allocated = getattr(conn, '_rdy_count', 0) + conn._in_flight
        low_water = max(1, target // 4)
        if allocated <= low_water:
            await self._set_conn_rdy(conn, target)

    async def _set_conn_rdy(self, conn, target):
        if conn.closed:
            return
        conn._rdy_target = target
        conn._rdy_low_water = max(1, target // 4) if target > 0 else 0
        rdy_count = max(0, target - conn._in_flight)
        max_rdy_count = getattr(conn, '_max_rdy_count', DEFAULT_MAX_RDY_COUNT)
        rdy_count = min(rdy_count, max_rdy_count)
        if getattr(conn, '_rdy_count', 0) == rdy_count:
            return
        await conn.execute(RDY, rdy_count)

    def close(self):
        self._is_working = False
        self._pending_conn_ids.clear()
        self._redistribute_pending = False
        if not self._distributor_task.done():
            self._distributor_task.cancel()
