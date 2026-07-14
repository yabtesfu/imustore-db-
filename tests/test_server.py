"""Tests for the RESP network server and the real-time changefeed."""

import asyncio
import json

import imustore
from imustore.metrics import Registry
from imustore.resp import Error, encode_command, read_reply
from imustore.server import ImmuServer


class Client:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, host, port):
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def cmd(self, *args):
        self.writer.write(encode_command(list(args)))
        await self.writer.drain()
        return await read_reply(self.reader)

    async def next_message(self, timeout=2.0):
        return await asyncio.wait_for(read_reply(self.reader), timeout)

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


async def _serve(tmp_path, name="server.db"):
    server = ImmuServer(tmp_path / name)
    host, port = await server.start("127.0.0.1", 0)
    return server, host, port


def _run(coro_factory):
    asyncio.run(coro_factory())


def test_ping_set_get_del(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        client = await Client.connect(host, port)
        try:
            assert await client.cmd("PING") == "PONG"
            assert await client.cmd("SET", "greeting", "hello") == "OK"
            assert await client.cmd("GET", "greeting") == "hello"
            assert await client.cmd("EXISTS", "greeting") == 1
            assert await client.cmd("DBSIZE") == 1
            assert await client.cmd("GET", "missing") is None
            assert await client.cmd("DEL", "greeting") == 1
            assert await client.cmd("GET", "greeting") is None
        finally:
            await client.close()
            await server.stop()

    _run(run)


def test_keys_uses_prefix_and_glob(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        client = await Client.connect(host, port)
        try:
            for key in ("user:1", "user:2", "order:1"):
                await client.cmd("SET", key, key.upper())
            assert await client.cmd("KEYS", "user:*") == ["user:1", "user:2"]
            assert await client.cmd("KEYS", "*") == ["order:1", "user:1", "user:2"]
            assert await client.cmd("KEYS", "*:1") == ["order:1", "user:1"]
        finally:
            await client.close()
            await server.stop()

    _run(run)


def test_data_persists_across_server_restart(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        client = await Client.connect(host, port)
        await client.cmd("SET", "durable", "yes")
        await client.close()
        await server.stop()

        server2, host2, port2 = await _serve(tmp_path)
        client2 = await Client.connect(host2, port2)
        try:
            assert await client2.cmd("GET", "durable") == "yes"
        finally:
            await client2.close()
            await server2.stop()

    _run(run)


def test_changefeed_streams_matching_writes(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        subscriber = await Client.connect(host, port)
        writer = await Client.connect(host, port)
        try:
            assert await subscriber.cmd("SUBSCRIBE", "user:") == ["subscribe", "user:", 1]

            await writer.cmd("SET", "user:1", "Ada")
            message = await subscriber.next_message()
            assert message[0] == "message" and message[1] == "user:"
            assert json.loads(message[2]) == {"key": "user:1", "op": "set", "value": "Ada"}

            # A non-matching key must not reach the subscriber; the next message
            # it sees is the delete of user:1, not the write to other:1.
            await writer.cmd("SET", "other:1", "ignored")
            await writer.cmd("DEL", "user:1")
            message = await subscriber.next_message()
            assert json.loads(message[2]) == {"key": "user:1", "op": "del", "value": None}
        finally:
            await subscriber.close()
            await writer.close()
            await server.stop()

    _run(run)


def test_subscribe_mode_rejects_other_commands(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        client = await Client.connect(host, port)
        try:
            await client.cmd("SUBSCRIBE", "x:")
            reply = await client.cmd("GET", "x")
            assert isinstance(reply, Error) and "subscribed" in reply
        finally:
            await client.close()
            await server.stop()

    _run(run)


def test_protocol_errors_are_reported(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        client = await Client.connect(host, port)
        try:
            unknown = await client.cmd("FROBNICATE", "x")
            assert isinstance(unknown, Error) and "unknown command" in unknown
            bad_args = await client.cmd("SET", "only-key")
            assert isinstance(bad_args, Error) and "wrong number of arguments" in bad_args
        finally:
            await client.close()
            await server.stop()

    _run(run)


def test_inline_command_is_accepted(tmp_path):
    async def run():
        server, host, port = await _serve(tmp_path)
        reader, writer = await asyncio.open_connection(host, port)
        try:
            writer.write(b"PING\r\n")  # inline, not multibulk
            await writer.drain()
            assert await read_reply(reader) == "PONG"
        finally:
            writer.close()
            await server.stop()

    _run(run)


def test_info_reports_storage_stats(tmp_path):
    async def run():
        server = ImmuServer(tmp_path / "info.db")
        host, port = await server.start("127.0.0.1", 0)
        client = await Client.connect(host, port)
        try:
            await client.cmd("SET", "a", "1")
            info = await client.cmd("INFO")
            assert "keys:1" in info and "immustore_version" in info
            assert "\r\n" in info  # Redis INFO framing
        finally:
            await client.close()
            await server.stop()

    _run(run)


def test_metrics_count_commands(tmp_path):
    async def run():
        registry = Registry()
        server = ImmuServer(tmp_path / "metrics.db", registry=registry)
        host, port = await server.start("127.0.0.1", 0)
        client = await Client.connect(host, port)
        try:
            await client.cmd("SET", "a", "1")
            await client.cmd("GET", "a")
            await client.cmd("GET", "a")
        finally:
            await client.close()
            await server.stop()
        text = registry.render()
        assert 'immustore_commands_total{command="get"} 2' in text
        assert 'immustore_commands_total{command="set"} 1' in text
        assert "immustore_command_seconds_count" in text

    _run(run)


def test_module_exports_server(tmp_path):
    assert hasattr(imustore, "ImmuServer")
