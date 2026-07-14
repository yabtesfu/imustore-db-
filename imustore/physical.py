"""Append-only record storage with crash-safe, self-healing commits.

ImmuStore uses *shadow paging*: a write never overwrites live data. New tree
nodes and values are appended to the end of the file, and a transaction becomes
visible by atomically publishing a new meta block that points at the new root.
This is the same crash-recovery discipline LMDB uses -- and, like LMDB, it needs
no separate write-ahead log, because the commit itself is the atomic pointer flip.

Three mechanisms make that flip provably crash-safe:

1. **Per-record checksums.** Every record carries a CRC32 of its payload, so a
   torn write or bit-rot is detected on read instead of silently returning junk.

2. **Double-buffered meta blocks.** The file begins with two meta slots, each
   holding a transaction id, the root address, the live key count, the durable
   file length, and a CRC. A commit always writes the *other* slot and fsyncs it
   before it counts as durable, so the last good meta is never in flight. On
   open we pick the slot with the highest id whose checksum verifies -- if the
   newest commit's meta write was torn, we transparently fall back to the one
   before it.

3. **Torn-tail truncation.** Each meta records the file length that was durable
   at that commit. A crash after appending records but before publishing the
   meta leaves orphaned (possibly half-written) records past that length; on
   open we truncate them away, restoring the file to its last committed state.
"""

from __future__ import annotations

import io
import os
import struct
import zlib
from dataclasses import dataclass

from .errors import DatabaseCorruptionError, ImmuStoreError
from .locking import FileLock


# -- meta block layout ------------------------------------------------------
MAGIC = b"IMMU"
FORMAT_VERSION = 2
META_SLOT_SIZE = 4096  # each slot sits in its own page so a write to one cannot tear the other
META_SLOTS = 2
SUPERBLOCK_SIZE = META_SLOT_SIZE * META_SLOTS  # records begin here
META_BODY_FORMAT = ">4sIQQQQ"  # magic, version, txn_id, root_address, key_count, data_length
META_CRC_FORMAT = ">I"
META_BODY_BYTES = struct.calcsize(META_BODY_FORMAT)
META_CRC_BYTES = struct.calcsize(META_CRC_FORMAT)

# -- record layout ----------------------------------------------------------
RECORD_HEADER_FORMAT = ">II"  # payload_length, payload_crc32
RECORD_HEADER_BYTES = struct.calcsize(RECORD_HEADER_FORMAT)

# -- durability policies ----------------------------------------------------
DURABILITY_FULL = "full"  # fsync appended data, then fsync the meta block (crash-safe)
DURABILITY_NONE = "none"  # never fsync (fastest; a crash may lose recent commits)
DURABILITY_MODES = (DURABILITY_FULL, DURABILITY_NONE)


@dataclass(frozen=True)
class StorageStats:
    file_size: int
    root_address: int
    record_count: int
    key_count: int
    txn_id: int


@dataclass(frozen=True)
class _Meta:
    txn_id: int
    root_address: int
    key_count: int
    data_length: int
    slot: int


def _crc(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


class Storage:
    def __init__(self, fileobj, *, durability: str = DURABILITY_FULL, readonly: bool = False):
        if durability not in DURABILITY_MODES:
            raise ValueError(f"unknown durability {durability!r}; choose from {DURABILITY_MODES}")
        self._f = fileobj
        self._lock = FileLock(fileobj)
        self._durability = durability
        self._readonly = readonly
        self._meta: _Meta | None = None
        self._ensure_meta()

    @property
    def closed(self) -> bool:
        return self._f.closed

    @property
    def locked(self) -> bool:
        return self._lock.locked

    @property
    def durability(self) -> str:
        return self._durability

    def lock(self) -> bool:
        return self._lock.lock()

    def unlock(self) -> None:
        self._lock.unlock()

    def flush(self) -> None:
        self._f.flush()
        try:
            os.fsync(self._f.fileno())
        except io.UnsupportedOperation:
            # A simulated in-memory disk has no descriptor; its flush() already
            # models durability. Real files always have a descriptor and fsync.
            pass

    def _file_size(self) -> int:
        self._f.seek(0, os.SEEK_END)
        return self._f.tell()

    # -- meta block handling ------------------------------------------------
    def _read_slot(self, index: int) -> _Meta | None:
        self._f.seek(index * META_SLOT_SIZE)
        raw = self._f.read(META_BODY_BYTES + META_CRC_BYTES)
        if len(raw) < META_BODY_BYTES + META_CRC_BYTES:
            return None
        body = raw[:META_BODY_BYTES]
        stored_crc = struct.unpack(META_CRC_FORMAT, raw[META_BODY_BYTES:])[0]
        if _crc(body) != stored_crc:
            return None
        magic, version, txn_id, root, count, data_length = struct.unpack(META_BODY_FORMAT, body)
        if magic != MAGIC:
            return None
        if version != FORMAT_VERSION:
            raise DatabaseCorruptionError(
                f"unsupported storage format version {version}; expected {FORMAT_VERSION}"
            )
        return _Meta(txn_id, root, count, data_length, index)

    def _read_best_meta(self) -> _Meta:
        candidates = [self._read_slot(i) for i in range(META_SLOTS)]
        valid = [meta for meta in candidates if meta is not None]
        if not valid:
            raise DatabaseCorruptionError("no valid superblock meta slot")
        return max(valid, key=lambda meta: meta.txn_id)

    def _write_slot(self, index: int, meta: _Meta) -> None:
        body = struct.pack(
            META_BODY_FORMAT,
            MAGIC,
            FORMAT_VERSION,
            meta.txn_id,
            meta.root_address,
            meta.key_count,
            meta.data_length,
        )
        block = body + struct.pack(META_CRC_FORMAT, _crc(body))
        block += b"\x00" * (META_SLOT_SIZE - len(block))
        self._f.seek(index * META_SLOT_SIZE)
        self._f.write(block)

    def _init_meta(self) -> None:
        for slot in range(META_SLOTS):
            self._write_slot(slot, _Meta(0, 0, 0, SUPERBLOCK_SIZE, slot))
        self._f.truncate(SUPERBLOCK_SIZE)
        self.flush()
        self._meta = _Meta(0, 0, 0, SUPERBLOCK_SIZE, 0)

    def _ensure_meta(self) -> None:
        if self._file_size() < SUPERBLOCK_SIZE:
            if self._readonly:
                self._meta = _Meta(0, 0, 0, SUPERBLOCK_SIZE, 0)
                return
            self._init_meta()
            return
        if self._readonly:
            # A read-only view (e.g. a snapshot) never recovers or truncates; it
            # only reads committed records, so it cannot disturb a live writer.
            self._meta = self._read_best_meta()
            return
        # Hold the write lock while recovering so we cannot race a writer that is
        # mid-commit (it holds the lock until its meta block is durable).
        self.lock()
        try:
            self._meta = self._read_best_meta()
            if self._file_size() > self._meta.data_length:
                self._f.truncate(self._meta.data_length)
                if self._durability == DURABILITY_FULL:
                    self.flush()
        finally:
            self.unlock()

    # -- committed pointers -------------------------------------------------
    def get_root_address(self) -> int:
        self._meta = self._read_best_meta()
        return self._meta.root_address

    def get_key_count(self) -> int:
        self._meta = self._read_best_meta()
        return self._meta.key_count

    def get_txn_id(self) -> int:
        self._meta = self._read_best_meta()
        return self._meta.txn_id

    def commit_root_address(self, root_address: int, key_count: int = 0) -> None:
        if self._readonly:
            raise ImmuStoreError("cannot commit on a read-only storage")
        self.lock()
        # Data records must be durable before the meta block that references them.
        if self._durability == DURABILITY_FULL:
            self.flush()
        new_meta = _Meta(
            txn_id=self._meta.txn_id + 1,
            root_address=root_address,
            key_count=key_count,
            data_length=self._file_size(),
            slot=1 - self._meta.slot,
        )
        self._write_slot(new_meta.slot, new_meta)
        if self._durability == DURABILITY_FULL:
            self.flush()
        self._meta = new_meta
        self.unlock()

    # -- record I/O ---------------------------------------------------------
    def write(self, payload: bytes) -> int:
        if self._readonly:
            raise ImmuStoreError("cannot write to a read-only storage")
        if not isinstance(payload, bytes):
            raise TypeError("storage payload must be bytes")
        address = max(self._file_size(), SUPERBLOCK_SIZE)
        self._f.seek(address)
        self._f.write(struct.pack(RECORD_HEADER_FORMAT, len(payload), _crc(payload)))
        self._f.write(payload)
        return address

    def read(self, address: int) -> bytes:
        if address <= 0:
            raise DatabaseCorruptionError("invalid record address")
        self._f.seek(address)
        header = self._f.read(RECORD_HEADER_BYTES)
        if len(header) != RECORD_HEADER_BYTES:
            raise DatabaseCorruptionError(f"missing record header at {address}")
        length, stored_crc = struct.unpack(RECORD_HEADER_FORMAT, header)
        payload = self._f.read(length)
        if len(payload) != length:
            raise DatabaseCorruptionError(f"truncated record at {address}")
        if _crc(payload) != stored_crc:
            raise DatabaseCorruptionError(f"record checksum mismatch at {address}")
        return payload

    def count_records(self) -> int:
        count = 0
        end = self._file_size()
        address = SUPERBLOCK_SIZE
        while address < end:
            self._f.seek(address)
            header = self._f.read(RECORD_HEADER_BYTES)
            if len(header) != RECORD_HEADER_BYTES:
                raise DatabaseCorruptionError(f"truncated record header at {address}")
            length, _stored_crc = struct.unpack(RECORD_HEADER_FORMAT, header)
            address += RECORD_HEADER_BYTES + length
            count += 1
        if address != end:
            raise DatabaseCorruptionError("record table does not end cleanly")
        return count

    def stats(self) -> StorageStats:
        meta = self._read_best_meta()
        self._meta = meta
        return StorageStats(
            file_size=self._file_size(),
            root_address=meta.root_address,
            record_count=self.count_records(),
            key_count=meta.key_count,
            txn_id=meta.txn_id,
        )

    def close(self) -> None:
        if self.locked:
            self.unlock()
        self._f.close()
