"""RESP (REdis Serialization Protocol) codec.

Implementing RESP means ``redis-cli`` and any off-the-shelf Redis client can
talk to ImmuStore's server unchanged. The protocol is deliberately simple:

- ``+OK\\r\\n``            simple string
- ``-ERR reason\\r\\n``    error
- ``:42\\r\\n``            integer
- ``$3\\r\\nfoo\\r\\n``     bulk string (``$-1\\r\\n`` is nil)
- ``*2\\r\\n...``          array of the above

Requests are arrays of bulk strings (the "multibulk" command form redis-cli
sends). Plain whitespace-separated "inline" commands are also accepted so a
command can be typed over a raw socket for debugging.
"""

from __future__ import annotations

import asyncio

CRLF = b"\r\n"


class ProtocolError(Exception):
    """Raised when bytes on the wire are not valid RESP."""


class SimpleString(str):
    """Marker so ``encode`` emits ``+...`` instead of a bulk string."""


class Error(str):
    """Marker so ``encode`` emits ``-...`` (a RESP error)."""


def encode(value) -> bytes:
    """Serialize a Python value into a RESP reply."""
    if value is None:
        return b"$-1" + CRLF
    if isinstance(value, SimpleString):
        return b"+" + value.encode("utf-8") + CRLF
    if isinstance(value, Error):
        return b"-" + value.encode("utf-8") + CRLF
    if isinstance(value, bool):  # bool is an int subclass; handle it first
        return b":" + (b"1" if value else b"0") + CRLF
    if isinstance(value, int):
        return b":" + str(value).encode("ascii") + CRLF
    if isinstance(value, (bytes, bytearray)):
        return b"$" + str(len(value)).encode("ascii") + CRLF + bytes(value) + CRLF
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return b"$" + str(len(raw)).encode("ascii") + CRLF + raw + CRLF
    if isinstance(value, (list, tuple)):
        out = b"*" + str(len(value)).encode("ascii") + CRLF
        return out + b"".join(encode(item) for item in value)
    raise TypeError(f"cannot RESP-encode {type(value).__name__}")


def encode_command(args) -> bytes:
    """Serialize a command (list of str/bytes) as a multibulk request."""
    parts = [b"*" + str(len(args)).encode("ascii") + CRLF]
    for arg in args:
        raw = arg.encode("utf-8") if isinstance(arg, str) else bytes(arg)
        parts.append(b"$" + str(len(raw)).encode("ascii") + CRLF + raw + CRLF)
    return b"".join(parts)


async def read_command(reader: asyncio.StreamReader):
    """Read one request. Returns a list of ``bytes`` args, or ``None`` at EOF."""
    while True:
        try:
            line = await reader.readuntil(CRLF)
        except (asyncio.IncompleteReadError, ConnectionError):
            return None
        if not line:
            return None
        line = line[:-2]
        if line:
            break  # skip blank lines between commands

    if line[:1] != b"*":
        return line.split()  # inline command

    try:
        count = int(line[1:])
    except ValueError:
        raise ProtocolError(f"bad multibulk count: {line!r}")
    if count <= 0:
        return []

    args = []
    try:
        for _ in range(count):
            header = (await reader.readuntil(CRLF))[:-2]
            if header[:1] != b"$":
                raise ProtocolError(f"expected bulk string, got {header!r}")
            length = int(header[1:])
            data = await reader.readexactly(length)
            await reader.readexactly(len(CRLF))  # trailing CRLF
            args.append(data)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None
    return args


async def read_reply(reader: asyncio.StreamReader):
    """Read one reply, returning it as a Python value (client side)."""
    line = await reader.readuntil(CRLF)
    kind, rest = line[:1], line[1:-2]
    if kind == b"+":
        return SimpleString(rest.decode("utf-8"))
    if kind == b"-":
        return Error(rest.decode("utf-8"))
    if kind == b":":
        return int(rest)
    if kind == b"$":
        length = int(rest)
        if length == -1:
            return None
        data = await reader.readexactly(length)
        await reader.readexactly(len(CRLF))
        return data.decode("utf-8")
    if kind == b"*":
        count = int(rest)
        if count == -1:
            return None
        return [await read_reply(reader) for _ in range(count)]
    raise ProtocolError(f"unknown reply type: {line!r}")
