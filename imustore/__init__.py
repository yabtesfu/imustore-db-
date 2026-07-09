from __future__ import annotations

from .audit import AuditReport
from .codec import BytesCodec, JsonCodec, TextCodec
from .errors import ConflictError
from .interface import DBDB, connect
from .mvcc import Snapshot

__all__ = [
    "AuditReport",
    "BytesCodec",
    "ConflictError",
    "DBDB",
    "ImmuServer",
    "JsonCodec",
    "Snapshot",
    "TextCodec",
    "connect",
]


def __getattr__(name):
    # Loaded lazily so `python -m imustore.server` does not re-import the package
    # mid-startup (which triggers a RuntimeWarning).
    if name == "ImmuServer":
        from .server import ImmuServer

        return ImmuServer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
