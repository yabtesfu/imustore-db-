"""An asyncio TCP transport that runs a Raft node as part of a real cluster.

The same deterministic :class:`RaftNode` that the simulator drives is here driven
by wall-clock timers and real sockets: a background task ticks it, inbound
connections deliver peer RPCs, and outbound RPCs are sent to peers as
length-prefixed JSON. Lost or refused connections are simply dropped -- Raft's
heartbeats and retries recover -- so a peer being down is a normal condition, not
an error.

The committed log is applied to an ImmuStore database (`KVStateMachine`), giving
a replicated, crash-safe key-value store.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import struct

from ..interface import connect
from .log import RaftLog
from .node import NotLeaderError, RaftConfig, RaftNode
from .statemachine import KVStateMachine
from .wire import decode, encode

_LEN = struct.Struct(">I")


def _frame(data: bytes) -> bytes:
    return _LEN.pack(len(data)) + data


async def _read_frame(reader: asyncio.StreamReader):
    try:
        header = await reader.readexactly(_LEN.size)
        (length,) = _LEN.unpack(header)
        return await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None


class RaftServer:
    def __init__(self, node_id, peers, *, db_path, log_path=None, host="127.0.0.1", port=0,
                 config=None, tick_ms=20, seed=None):
        self.node_id = node_id
        self.peers = dict(peers)  # {peer_id: (host, port)}
        self._db_path = db_path
        self._log_path = log_path
        self._host = host
        self._port = port
        self._config = config or RaftConfig()
        self._tick_ms = tick_ms
        self._seed = node_id if seed is None else seed
        self._db = None
        self._sm = None
        self.node = None
        self._server = None
        self._tick_task = None
        self._lock = asyncio.Lock()  # node methods are synchronous; this serializes callers

    async def start(self):
        self._db = connect(self._db_path)
        self._sm = KVStateMachine(self._db)
        log = RaftLog(self._log_path)
        self.node = RaftNode(
            self.node_id, list(self.peers), apply=self._sm.apply, log=log,
            rng=random.Random(self._seed), config=self._config,
            last_applied=self._sm.applied_index,
        )
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        self._tick_task = asyncio.ensure_future(self._tick_loop())
        return self._server.sockets[0].getsockname()

    async def stop(self):
        if self._tick_task is not None:
            self._tick_task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._db is not None:
            self._db.close()

    @property
    def is_leader(self) -> bool:
        return self.node is not None and self.node.is_leader

    @property
    def leader_id(self):
        return None if self.node is None else self.node.leader_id

    def read(self, key):
        try:
            return self._db[key]
        except KeyError:
            return None

    async def submit(self, command, timeout=3.0):
        """Replicate a command; returns once it is committed and applied here."""
        async with self._lock:
            if not self.node.is_leader:
                raise NotLeaderError(self.node.leader_id)
            index, outgoing = self.node.propose(command)
        await self._dispatch(outgoing)

        async def _await_apply():
            while self.node.last_applied < index:
                await asyncio.sleep(self._tick_ms / 1000)

        await asyncio.wait_for(_await_apply(), timeout)
        return index

    async def _tick_loop(self):
        try:
            while True:
                await asyncio.sleep(self._tick_ms / 1000)
                async with self._lock:
                    outgoing = self.node.tick()
                await self._dispatch(outgoing)
        except asyncio.CancelledError:
            pass

    async def _handle(self, reader, writer):
        try:
            raw = await _read_frame(reader)
            if raw is None:
                return
            envelope = decode(raw)
            async with self._lock:
                outgoing = self.node.step(envelope)
            await self._dispatch(outgoing)
        except Exception:  # pragma: no cover - defensive: a bad peer must not crash us
            pass
        finally:
            writer.close()

    async def _dispatch(self, envelopes):
        for envelope in envelopes:
            await self._send(envelope)

    async def _send(self, envelope):
        address = self.peers.get(envelope.dst)
        if address is None:
            return
        writer = None
        try:
            _reader, writer = await asyncio.wait_for(asyncio.open_connection(*address), timeout=0.5)
            writer.write(_frame(encode(envelope)))
            await writer.drain()
        except (OSError, asyncio.TimeoutError):
            pass  # peer down/slow: heartbeats will retry
        finally:
            if writer is not None:
                writer.close()


def _parse_peer(spec):
    node_id, host, port = spec.split(":")
    return int(node_id), (host, int(port))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="imustore-raft", description="Run a Raft cluster node.")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True, help="ImmuStore database path for this node")
    parser.add_argument("--raft-log", default=None, help="path for this node's persistent Raft log")
    parser.add_argument("--peer", action="append", default=[], metavar="ID:HOST:PORT",
                        help="a peer node (repeatable)")
    args = parser.parse_args(argv)
    peers = dict(_parse_peer(spec) for spec in args.peer)

    async def _run():
        server = RaftServer(args.id, peers, db_path=args.db, log_path=args.raft_log,
                            host=args.host, port=args.port)
        host, port = await server.start()
        print(f"raft node {args.id} listening on {host}:{port} with peers {sorted(peers)}")
        try:
            await asyncio.Event().wait()
        finally:
            await server.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("\nshutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
