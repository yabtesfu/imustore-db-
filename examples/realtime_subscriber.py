"""Self-contained demo of the real-time changefeed.

Starts an in-process ImmuStore server, subscribes to the ``user:`` prefix on one
connection, then performs writes on another connection and prints the change
events as they stream in live.

    python examples/realtime_subscriber.py
"""

import asyncio
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from imustore.resp import encode_command, read_reply  # noqa: E402
from imustore.server import ImmuServer  # noqa: E402


async def send(writer, reader, *args):
    writer.write(encode_command(list(args)))
    await writer.drain()
    return await read_reply(reader)


async def main():
    with tempfile.TemporaryDirectory() as tmp:
        server = ImmuServer(Path(tmp) / "demo.db")
        host, port = await server.start("127.0.0.1", 0)
        print(f"server listening on {host}:{port}\n")

        sub_reader, sub_writer = await asyncio.open_connection(host, port)
        confirm = await send(sub_writer, sub_reader, "SUBSCRIBE", "user:")
        print(f"subscriber: {confirm}\n")

        writer_reader, writer_writer = await asyncio.open_connection(host, port)
        writes = [
            ("SET", "user:1", "Ada"),
            ("SET", "user:2", "Grace"),
            ("SET", "other:9", "not-a-user"),  # filtered out of the user: feed
            ("DEL", "user:1"),
        ]

        async def do_writes():
            await asyncio.sleep(0.1)
            for command in writes:
                reply = await send(writer_writer, writer_reader, *command)
                print(f"writer: {' '.join(command):22s} -> {reply}")

        async def read_changes(expected):
            for _ in range(expected):
                message = await asyncio.wait_for(read_reply(sub_reader), timeout=2.0)
                change = json.loads(message[2])
                print(f"  >> live change on '{message[1]}': {change}")

        # Three writes match user:*, so expect three change events.
        await asyncio.gather(do_writes(), read_changes(3))

        sub_writer.close()
        writer_writer.close()
        await server.stop()
        print("\ndone.")


if __name__ == "__main__":
    asyncio.run(main())
