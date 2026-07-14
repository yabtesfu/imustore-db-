"""A simulated disk: an in-memory file that models fsync durability.

Real crash testing is awkward because an in-process "crash" (closing and
reopening a file) does not actually lose un-fsynced writes -- the operating
system still has them cached. A ``SimDisk`` fixes that by modeling the durability
contract explicitly:

- ``_live`` is what the process sees (every write, buffered or not).
- ``_durable`` is what has actually reached the platter -- it only advances on
  ``flush()`` (which the storage layer calls after fsync).

To simulate power loss, throw away ``_live`` and reopen on ``durable_bytes()``:
anything written since the last flush is gone, exactly as a real crash would
lose it. ``arm_crash()`` goes further and raises :class:`SimCrash` from a future
``flush()``, modeling the machine dying *in the middle of a commit* -- the case
that actually exercises the double-buffered meta blocks and torn-tail recovery.

This is the FoundationDB / TigerBeetle approach in miniature: run the real
storage engine against a fake disk whose faults are deterministic and seeded, so
any failure replays exactly.
"""

from __future__ import annotations

import io
import os


class SimCrash(Exception):
    """Raised from a ``flush()`` that is scheduled to fail (a mid-commit crash)."""


class SimDisk:
    def __init__(self, *, initial: bytes = b""):
        self._live = bytearray(initial)
        self._durable = bytes(initial)
        self._pos = 0
        self._closed = False
        self._flush_count = 0
        self._crash_at_flush = None

    # -- file interface used by Storage / FileLock --------------------------
    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def name(self) -> str:
        return "<simdisk>"

    def fileno(self):
        # No real descriptor: this signals Storage/FileLock to skip fsync/flock.
        raise io.UnsupportedOperation("simulated disk has no file descriptor")

    def seekable(self) -> bool:
        return True

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self._pos = offset
        elif whence == os.SEEK_END:
            self._pos = len(self._live) + offset
        elif whence == os.SEEK_CUR:
            self._pos += offset
        else:
            raise ValueError(f"invalid whence {whence}")
        return self._pos

    def tell(self):
        return self._pos

    def read(self, size=-1):
        if size is None or size < 0:
            end = len(self._live)
        else:
            end = min(self._pos + size, len(self._live))
        data = bytes(self._live[self._pos:end])
        self._pos = end
        return data

    def write(self, data):
        data = bytes(data)
        end = self._pos + len(data)
        if end > len(self._live):
            self._live.extend(b"\x00" * (end - len(self._live)))
        self._live[self._pos:end] = data
        self._pos = end
        return len(data)

    def truncate(self, size=None):
        if size is None:
            size = self._pos
        if size < len(self._live):
            del self._live[size:]
        elif size > len(self._live):
            self._live.extend(b"\x00" * (size - len(self._live)))
        return size

    def flush(self):
        self._flush_count += 1
        if self._crash_at_flush is not None and self._flush_count >= self._crash_at_flush:
            self._crash_at_flush = None
            raise SimCrash(f"simulated power loss at flush #{self._flush_count}")
        self._durable = bytes(self._live)

    def close(self):
        self._closed = True

    # -- simulation controls ------------------------------------------------
    def arm_crash(self, after_flushes: int):
        """Schedule the next ``after_flushes``-th flush to crash instead of persist."""
        self._crash_at_flush = self._flush_count + after_flushes

    def durable_bytes(self) -> bytes:
        """The bytes that would survive a power loss right now."""
        return self._durable
