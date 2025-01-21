from __future__ import annotations

import json
from typing import Any, Protocol


class Codec(Protocol):
    def encode(self, value: Any) -> bytes:
        ...

    def decode(self, payload: bytes) -> Any:
        ...


class JsonCodec:
    def encode(self, value: Any) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def decode(self, payload: bytes) -> Any:
        return json.loads(payload.decode("utf-8"))


class TextCodec:
    def encode(self, value: Any) -> bytes:
        return str(value).encode("utf-8")

    def decode(self, payload: bytes) -> str:
        return payload.decode("utf-8")


class BytesCodec:
    def encode(self, value: Any) -> bytes:
        if not isinstance(value, bytes):
            raise TypeError("BytesCodec only accepts bytes")
        return value

    def decode(self, payload: bytes) -> bytes:
        return payload


def parse_cli_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw
