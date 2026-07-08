from __future__ import annotations

import os
import struct
from dataclasses import dataclass

from .errors import DatabaseCorruptionError
from .locking import FileLock


SUPERBLOCK_SIZE = 4096
ROOT_FORMAT = ">Q"
COUNT_FORMAT = ">Q"
HEADER_FORMAT = ">QQ"  # root address followed by live key count
RECORD_HEADER_FORMAT = ">I"
ROOT_BYTES = struct.calcsize(ROOT_FORMAT)
COUNT_BYTES = struct.calcsize(COUNT_FORMAT)
RECORD_HEADER_BYTES = struct.calcsize(RECORD_HEADER_FORMAT)


@dataclass(frozen=True)
class StorageStats:
    file_size: int
    root_address: int
    record_count: int
    key_count: int


class Storage:
    def __init__(self, fileobj):
        self._f = fileobj
        self._lock = FileLock(fileobj)
        self._ensure_superblock()

    @property
    def closed(self) -> bool:
        return self._f.closed

    @property
    def locked(self) -> bool:
        return self._lock.locked

    def lock(self) -> bool:
        return self._lock.lock()

    def unlock(self) -> None:
        self._lock.unlock()

    def _ensure_superblock(self) -> None:
        self._f.seek(0, os.SEEK_END)
        size = self._f.tell()
        if size >= SUPERBLOCK_SIZE:
            return

        root_address = 0
        if size >= ROOT_BYTES:
            self._f.seek(0)
            root_address = struct.unpack(ROOT_FORMAT, self._f.read(ROOT_BYTES))[0]

        self._f.seek(0)
        self._f.write(struct.pack(ROOT_FORMAT, root_address))
        self._f.write(b"\x00" * (SUPERBLOCK_SIZE - ROOT_BYTES))
        self._f.truncate(SUPERBLOCK_SIZE)
        self.flush()

    def flush(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())

    def get_root_address(self) -> int:
        self._f.seek(0)
        payload = self._f.read(ROOT_BYTES)
        if len(payload) != ROOT_BYTES:
            raise DatabaseCorruptionError("storage superblock is incomplete")
        return struct.unpack(ROOT_FORMAT, payload)[0]

    def get_key_count(self) -> int:
        self._f.seek(ROOT_BYTES)
        payload = self._f.read(COUNT_BYTES)
        if len(payload) != COUNT_BYTES:
            return 0
        return struct.unpack(COUNT_FORMAT, payload)[0]

    def commit_root_address(self, root_address: int, key_count: int = 0) -> None:
        self.lock()
        self.flush()
        self._f.seek(0)
        # Write the root pointer and key count as a single record so a reader
        # never observes a new root paired with a stale count.
        self._f.write(struct.pack(HEADER_FORMAT, root_address, key_count))
        self.flush()
        self.unlock()

    def write(self, payload: bytes) -> int:
        if not isinstance(payload, bytes):
            raise TypeError("storage payload must be bytes")
        self._f.seek(0, os.SEEK_END)
        address = max(self._f.tell(), SUPERBLOCK_SIZE)
        self._f.seek(address)
        self._f.write(struct.pack(RECORD_HEADER_FORMAT, len(payload)))
        self._f.write(payload)
        return address

    def read(self, address: int) -> bytes:
        if address <= 0:
            raise DatabaseCorruptionError("invalid record address")
        self._f.seek(address)
        header = self._f.read(RECORD_HEADER_BYTES)
        if len(header) != RECORD_HEADER_BYTES:
            raise DatabaseCorruptionError(f"missing record header at {address}")
        length = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
        payload = self._f.read(length)
        if len(payload) != length:
            raise DatabaseCorruptionError(f"truncated record at {address}")
        return payload

    def count_records(self) -> int:
        count = 0
        self._f.seek(0, os.SEEK_END)
        end = self._f.tell()
        address = SUPERBLOCK_SIZE
        while address < end:
            self._f.seek(address)
            header = self._f.read(RECORD_HEADER_BYTES)
            if len(header) != RECORD_HEADER_BYTES:
                raise DatabaseCorruptionError(f"truncated record header at {address}")
            length = struct.unpack(RECORD_HEADER_FORMAT, header)[0]
            address += RECORD_HEADER_BYTES + length
            count += 1
        if address != end:
            raise DatabaseCorruptionError("record table does not end cleanly")
        return count

    def stats(self) -> StorageStats:
        self._f.seek(0, os.SEEK_END)
        return StorageStats(
            file_size=self._f.tell(),
            root_address=self.get_root_address(),
            record_count=self.count_records(),
            key_count=self.get_key_count(),
        )

    def close(self) -> None:
        if self.locked:
            self.unlock()
        self._f.close()
