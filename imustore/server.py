"""An asyncio, RESP-speaking network server for ImmuStore DB.

    python -m imustore.server --path data.db --port 6380

Because it speaks Redis's wire protocol, ``redis-cli`` works out of the box::

    redis-cli -p 6380 set greeting hello
    redis-cli -p 6380 get greeting
    redis-cli -p 6380 subscribe user:        # live change stream for user:* keys

The storage engine is single-writer, so every database operation is funnelled
through a single-thread executor: that serializes access safely *and* keeps the
event loop responsive while a commit's fsync is in flight. After each committed
mutation the server publishes a change event to the :class:`ChangeBroker`, which
is what powers the real-time ``SUBSCRIBE`` changefeed.
"""

from __future__ import annotations

import argparse
import asyncio
import fnmatch
import time
from concurrent.futures import ThreadPoolExecutor

from .codec import TextCodec
from .interface import connect
from .metrics import REGISTRY
from .physical import DURABILITY_FULL
from .pubsub import ChangeBroker
from .resp import Error, SimpleString, ProtocolError, encode, read_command

# Commands still permitted once a connection has entered subscribe mode.
_SUBSCRIBE_MODE_COMMANDS = {b"SUBSCRIBE", b"UNSUBSCRIBE", b"PING", b"QUIT"}
_GLOB_METACHARS = set("*?[]")


class ImmuServer:
    def __init__(self, path, *, durability: str = DURABILITY_FULL, registry=None):
        self._path = path
        self._durability = durability
        self._db = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="immustore-db")
        self._broker = ChangeBroker()
        self._server = None
        self._loop = None

        self.registry = registry or REGISTRY
        self._m_commands = self.registry.counter("immustore_commands_total", "RESP commands handled")
        self._m_latency = self.registry.histogram(
            "immustore_command_seconds", "RESP command handling latency (seconds)"
        )
        self._m_connections = self.registry.gauge(
            "immustore_connections", "currently open client connections"
        )
        self._m_changes = self.registry.counter(
            "immustore_change_events_total", "change events published to subscribers"
        )

    # -- lifecycle ----------------------------------------------------------
    async def start(self, host: str = "127.0.0.1", port: int = 6380):
        self._loop = asyncio.get_running_loop()
        self._db = await self._run(
            lambda: connect(self._path, codec=TextCodec(), durability=self._durability)
        )
        self._server = await asyncio.start_server(self._handle, host, port)
        return self._server.sockets[0].getsockname()

    async def serve_forever(self) -> None:
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._db is not None:
            await self._run(self._db.close)
            self._db = None
        self._executor.shutdown(wait=True)

    async def _run(self, fn):
        return await self._loop.run_in_executor(self._executor, fn)

    # -- database operations (run on the single DB thread) ------------------
    def _set(self, key, value):
        self._db[key] = value
        self._db.commit()

    def _get(self, key):
        try:
            return self._db[key]
        except KeyError:
            return None

    def _delete(self, keys):
        removed = []
        for key in keys:
            try:
                del self._db[key]
                removed.append(key)
            except KeyError:
                pass
        if removed:
            self._db.commit()
        return removed

    def _exists(self, keys):
        found = 0
        for key in keys:
            try:
                self._db[key]
                found += 1
            except KeyError:
                pass
        return found

    def _match_keys(self, pattern):
        if pattern == "*":
            return list(self._db.keys())
        prefix = pattern[:-1]
        if pattern.endswith("*") and prefix and not (_GLOB_METACHARS & set(prefix)):
            return list(self._db.keys(prefix=prefix))  # uses the B+ tree range scan
        return [key for key in self._db.keys() if fnmatch.fnmatchcase(key, pattern)]

    async def _handle(self, reader, writer):
        await _Connection(self, reader, writer).serve()


class _Connection:
    def __init__(self, server: ImmuServer, reader, writer):
        self._server = server
        self._reader = reader
        self._writer = writer
        self._broker = server._broker
        self._queue = None
        self._pump_task = None
        self._subscriptions = set()

    async def serve(self) -> None:
        self._server._m_connections.inc()
        try:
            while True:
                try:
                    args = await read_command(self._reader)
                except ProtocolError as exc:
                    await self._send(Error(f"ERR protocol error: {exc}"))
                    break
                if args is None:
                    break
                if not args:
                    continue
                if not await self._dispatch(args):
                    break
        finally:
            self._server._m_connections.dec()
            await self._teardown()

    async def _send(self, value) -> None:
        self._writer.write(encode(value))
        await self._writer.drain()

    async def _dispatch(self, args) -> bool:
        command = args[0].upper()
        name = command.decode("ascii", "ignore").lower()
        if self._subscriptions and command not in _SUBSCRIBE_MODE_COMMANDS:
            await self._send(Error("ERR only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT allowed while subscribed"))
            return True

        handler = getattr(self, f"_cmd_{name}", None)
        if handler is None:
            self._server._m_commands.inc(command="unknown")
            await self._send(Error(f"ERR unknown command '{command.decode('utf-8', 'replace')}'"))
            return True

        started = time.perf_counter()
        try:
            return await handler(args)
        finally:
            self._server._m_commands.inc(command=name)
            self._server._m_latency.observe(time.perf_counter() - started, command=name)

    # -- commands -----------------------------------------------------------
    async def _cmd_ping(self, args) -> bool:
        await self._send(args[1].decode("utf-8") if len(args) > 1 else SimpleString("PONG"))
        return True

    async def _cmd_echo(self, args) -> bool:
        if len(args) != 2:
            return await self._wrong_args("echo")
        await self._send(args[1].decode("utf-8"))
        return True

    async def _cmd_set(self, args) -> bool:
        if len(args) != 3:
            return await self._wrong_args("set")
        key, value = args[1].decode("utf-8"), args[2].decode("utf-8")
        if not key:
            return await self._send_ok_error("ERR key must not be empty")
        await self._server._run(lambda: self._server._set(key, value))
        self._broker.publish(key, "set", value)
        self._server._m_changes.inc()
        await self._send(SimpleString("OK"))
        return True

    async def _cmd_get(self, args) -> bool:
        if len(args) != 2:
            return await self._wrong_args("get")
        value = await self._server._run(lambda: self._server._get(args[1].decode("utf-8")))
        await self._send(value)
        return True

    async def _cmd_del(self, args) -> bool:
        if len(args) < 2:
            return await self._wrong_args("del")
        keys = [arg.decode("utf-8") for arg in args[1:]]
        removed = await self._server._run(lambda: self._server._delete(keys))
        for key in removed:
            self._broker.publish(key, "del", None)
            self._server._m_changes.inc()
        await self._send(len(removed))
        return True

    async def _cmd_exists(self, args) -> bool:
        if len(args) < 2:
            return await self._wrong_args("exists")
        keys = [arg.decode("utf-8") for arg in args[1:]]
        await self._send(await self._server._run(lambda: self._server._exists(keys)))
        return True

    async def _cmd_keys(self, args) -> bool:
        if len(args) != 2:
            return await self._wrong_args("keys")
        pattern = args[1].decode("utf-8")
        await self._send(await self._server._run(lambda: self._server._match_keys(pattern)))
        return True

    async def _cmd_dbsize(self, args) -> bool:
        await self._send(await self._server._run(lambda: len(self._server._db)))
        return True

    async def _cmd_info(self, args) -> bool:
        stats = await self._server._run(lambda: self._server._db.stats())
        # Redis INFO framing: CRLF line endings, sections separated by a blank line.
        report = "\r\n".join([
            "# Server",
            "immustore_version:0.2.0",
            "",
            "# Storage",
            f"keys:{stats.key_count}",
            f"records:{stats.record_count}",
            f"file_size:{stats.file_size}",
            f"txn_id:{stats.txn_id}",
            "",
            "# Clients",
            f"connected_clients:{int(self._server._m_connections.value())}",
            "",
        ])
        await self._send(report)
        return True

    async def _cmd_command(self, args) -> bool:
        await self._send([])  # keeps redis-cli's startup handshake happy
        return True

    async def _cmd_quit(self, args) -> bool:
        await self._send(SimpleString("OK"))
        return False

    async def _cmd_subscribe(self, args) -> bool:
        if len(args) < 2:
            return await self._wrong_args("subscribe")
        if self._queue is None:
            self._queue = self._broker.new_queue()
            self._pump_task = asyncio.ensure_future(self._pump())
        for raw in args[1:]:
            prefix = raw.decode("utf-8")
            self._broker.subscribe(prefix, self._queue)
            self._subscriptions.add(prefix)
            await self._send(["subscribe", prefix, len(self._subscriptions)])
        return True

    async def _cmd_unsubscribe(self, args) -> bool:
        prefixes = [arg.decode("utf-8") for arg in args[1:]] or list(self._subscriptions)
        for prefix in prefixes:
            self._broker.unsubscribe(prefix, self._queue)
            self._subscriptions.discard(prefix)
            await self._send(["unsubscribe", prefix, len(self._subscriptions)])
        return True

    # -- subscribe plumbing -------------------------------------------------
    async def _pump(self) -> None:
        """Forward broker messages to this client as they arrive."""
        try:
            while True:
                message = await self._queue.get()
                self._writer.write(encode(message))
                await self._writer.drain()
        except (asyncio.CancelledError, ConnectionError):
            pass

    async def _teardown(self) -> None:
        if self._queue is not None:
            self._broker.unsubscribe_all(self._queue)
        if self._pump_task is not None:
            self._pump_task.cancel()
        try:
            self._writer.close()
        except Exception:  # pragma: no cover - best-effort close
            pass

    async def _wrong_args(self, name) -> bool:
        await self._send(Error(f"ERR wrong number of arguments for '{name}'"))
        return True

    async def _send_ok_error(self, message) -> bool:
        await self._send(Error(message))
        return True


async def _amain(args) -> None:
    server = ImmuServer(args.path, durability=args.durability)
    host, port = await server.start(args.host, args.port)
    print(f"ImmuStore serving {args.path!r} on {host}:{port} (RESP) — Ctrl-C to stop")

    admin = None
    if args.metrics_port is not None:
        from .admin import AdminServer

        admin = AdminServer(server.registry, host=args.host, port=args.metrics_port)
        admin_host, admin_port = admin.start()
        print(f"metrics on http://{admin_host}:{admin_port}/metrics")
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        if admin is not None:
            admin.stop()
        await server.stop()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="imustore-server", description="Serve an ImmuStore DB over RESP.")
    parser.add_argument("--path", default="immustore.db", help="database file to serve")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--durability", default=DURABILITY_FULL, choices=("full", "none"))
    parser.add_argument("--metrics-port", type=int, default=None,
                        help="serve Prometheus metrics on this HTTP port")
    args = parser.parse_args(argv)
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        print("\nshutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
