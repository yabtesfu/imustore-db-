from __future__ import annotations

import os
import threading
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Callable

from .audit import AuditReport
from .binary_tree import BinaryTree
from .bplus_tree import BPlusTree
from .codec import Codec, JsonCodec
from .errors import ConflictError, DatabaseClosedError, ImmuStoreError, KeyEncodingError
from .mvcc import Snapshot
from .physical import DURABILITY_FULL, Storage, StorageStats

# Available index engines. The copy-on-write B+ tree is the default because it
# stays balanced and shallow; the binary tree is kept as a simple reference.
INDEX_CLASSES = {"bplus": BPlusTree, "binary": BinaryTree}
DEFAULT_INDEX = "bplus"


class DBDB(MutableMapping):
    def __init__(
        self,
        fileobj,
        *,
        path: str | os.PathLike[str] | None = None,
        codec: Codec | None = None,
        index: str = DEFAULT_INDEX,
        durability: str = DURABILITY_FULL,
    ):
        if index not in INDEX_CLASSES:
            raise ValueError(f"unknown index {index!r}; choose from {sorted(INDEX_CLASSES)}")
        self._path = Path(path) if path is not None else None
        self._codec = codec or JsonCodec()
        self._index_name = index
        self._durability = durability
        self._storage = Storage(fileobj, durability=durability)
        self._tree = INDEX_CLASSES[index](self._storage)
        self._write_mutex = threading.Lock()  # serializes commit critical sections in-process
        self._open_snapshots = 0

    def _assert_open(self) -> None:
        if self._storage.closed:
            raise DatabaseClosedError("database is closed")

    def _validate_key(self, key: str) -> None:
        if not isinstance(key, str) or not key:
            raise KeyEncodingError("keys must be non-empty strings")

    def __getitem__(self, key: str) -> Any:
        self._assert_open()
        self._validate_key(key)
        return self._codec.decode(self._tree.get(key))

    def __setitem__(self, key: str, value: Any) -> None:
        self._assert_open()
        self._validate_key(key)
        self._tree.set(key, self._codec.encode(value))

    def __delitem__(self, key: str) -> None:
        self._assert_open()
        self._validate_key(key)
        self._tree.delete(key)

    def __iter__(self):
        self._assert_open()
        for key in self.keys():
            yield key

    def __len__(self) -> int:
        self._assert_open()
        return self._tree.length()

    def items(self):
        self._assert_open()
        for key, payload in self._tree.items():
            yield key, self._codec.decode(payload)

    def keys(self, *, prefix: str | None = None, start: str | None = None, stop: str | None = None):
        for key, _ in self.scan(prefix=prefix, start=start, stop=stop):
            yield key

    def scan(self, *, prefix: str | None = None, start: str | None = None, stop: str | None = None):
        self._assert_open()
        if prefix is not None:
            self._validate_key(prefix)
        if start is not None:
            self._validate_key(start)
        if stop is not None:
            self._validate_key(stop)
        for key, payload in self._tree.range_items(start, stop):
            if prefix is None or key.startswith(prefix):
                yield key, self._codec.decode(payload)

    def commit(self) -> None:
        self._assert_open()
        self._tree.commit()

    def close(self) -> None:
        self._storage.close()

    def stats(self) -> StorageStats:
        self._assert_open()
        return self._storage.stats()

    def audit(self) -> AuditReport:
        self._assert_open()
        return self._tree.audit()

    def update_value(self, key: str, updater: Callable[[Any], Any], *, default: Any = None) -> Any:
        self._assert_open()
        self._validate_key(key)
        if self._storage.lock():
            self._tree._refresh_tree_ref()
        try:
            current = self[key]
        except KeyError:
            current = default
        updated = updater(current)
        self[key] = updated
        return updated

    def snapshot(self) -> Snapshot:
        """Open a consistent, read-only, point-in-time view (see mvcc.Snapshot).

        The snapshot pins the current committed version and reads from its own
        file handle, so it never blocks (or is blocked by) writers and will not
        observe commits made after it was taken.
        """
        self._assert_open()
        if self._path is None:
            raise ValueError("snapshots require a path-backed database")
        # Read the committed root/count from the shared main storage under the
        # write mutex, so opening a snapshot can never race a concurrent
        # committer mutating that same file object.
        with self._write_mutex:
            root = self._storage.get_root_address()
            count = self._storage.get_key_count()
            self._open_snapshots += 1
        try:
            reader = os.fdopen(os.open(self._path, os.O_RDONLY), "rb")
        except Exception:
            with self._write_mutex:
                self._open_snapshots -= 1
            raise
        return Snapshot(reader, root, count, codec=self._codec, on_close=self._snapshot_closed)

    def _snapshot_closed(self) -> None:
        with self._write_mutex:
            self._open_snapshots = max(0, self._open_snapshots - 1)

    def transaction(self) -> "Transaction":
        """Begin an optimistic, snapshot-isolated transaction (use with ``with``)."""
        return Transaction(self)

    def begin(self) -> "Transaction":
        """Begin a transaction eagerly for manual ``commit()``/``abort()`` control."""
        transaction = Transaction(self)
        transaction._begin()
        return transaction

    def compact(self) -> StorageStats:
        self._assert_open()
        if self._path is None:
            raise ValueError("compaction requires a path-backed database")

        # Hold the write mutex for the whole rewrite so no commit or snapshot can
        # touch the file while it is being replaced (the snapshot count is checked
        # under the same mutex snapshot() increments it under).
        with self._write_mutex:
            if self._open_snapshots > 0:
                raise ImmuStoreError("cannot compact while snapshots or transactions are open")

            snapshot = list(self.items())
            temp_path = self._path.with_suffix(self._path.suffix + ".compact.tmp")
            compacted = connect(
                temp_path, codec=self._codec, index=self._index_name, durability=self._durability
            )
            try:
                for key, value in snapshot:
                    compacted[key] = value
                compacted.commit()
                stats = compacted.stats()
            finally:
                compacted.close()

            self.close()
            os.replace(temp_path, self._path)
            reopened = _open_database_file(self._path)
            self._storage = Storage(reopened, durability=self._durability)
            self._tree = INDEX_CLASSES[self._index_name](self._storage)
            return stats

    def __enter__(self):
        self._assert_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


_DELETED = object()  # tombstone marker in a transaction's write overlay


class Transaction:
    """An optimistic, snapshot-isolated write transaction.

    Reads see a private snapshot taken when the transaction began, overlaid with
    the transaction's own uncommitted writes (read-your-writes). Writes are
    buffered in memory, so the transaction body never holds the write lock. At
    commit the transaction briefly locks, checks that no key it wrote was changed
    by a concurrent commit, and either applies all writes atomically or raises
    :class:`ConflictError` so the caller can retry.
    """

    def __init__(self, db: DBDB):
        self._db = db
        self._base: Snapshot | None = None
        self._overlay: dict[str, Any] = {}

    def _begin(self) -> None:
        if self._base is None:
            self._db._assert_open()
            self._base = self._db.snapshot()

    def __enter__(self) -> "Transaction":
        self._begin()
        return self

    def get(self, key: str) -> Any:
        self._db._validate_key(key)
        if key in self._overlay:
            payload = self._overlay[key]
            if payload is _DELETED:
                raise KeyError(key)
            return self._db._codec.decode(payload)
        return self._base[key]

    def get_or_none(self, key: str, default: Any = None) -> Any:
        try:
            return self.get(key)
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        if key in self._overlay:
            return self._overlay[key] is not _DELETED
        return key in self._base

    def set(self, key: str, value: Any) -> None:
        self._db._validate_key(key)
        self._overlay[key] = self._db._codec.encode(value)

    def delete(self, key: str) -> None:
        self._db._validate_key(key)
        if key not in self:
            raise KeyError(key)
        self._overlay[key] = _DELETED

    def update(self, key: str, updater: Callable[[Any], Any], *, default: Any = None) -> Any:
        updated = updater(self.get_or_none(key, default))
        self.set(key, updated)
        return updated

    def commit(self) -> None:
        try:
            if not self._overlay:
                return
            db = self._db
            with db._write_mutex:
                db._storage.lock()
                try:
                    db._tree._refresh_tree_ref()
                    self._check_conflicts()
                    for key, payload in self._overlay.items():
                        if payload is _DELETED:
                            try:
                                db._tree.delete(key)
                            except KeyError:
                                pass
                        else:
                            db._tree.set(key, payload)
                    db._tree.commit()  # publishes the new root and releases the lock
                finally:
                    if db._storage.locked:
                        db._storage.unlock()
        finally:
            self._close_base()

    def _check_conflicts(self) -> None:
        for key in self._overlay:
            base_value = self._base._raw(key)
            try:
                current_value = self._db._tree.get(key)
            except KeyError:
                current_value = None
            if base_value != current_value:
                raise ConflictError(f"write conflict on {key!r}: changed since the transaction began")

    def abort(self) -> None:
        self._overlay.clear()
        self._close_base()

    def _close_base(self) -> None:
        if self._base is not None:
            self._base.close()
            self._base = None

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc_type is not None:
            self.abort()
            return False
        self.commit()
        return False


def _open_database_file(path: str | os.PathLike[str]):
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    fd = os.open(path, flags, 0o666)
    return os.fdopen(fd, "r+b")


def connect(
    path: str | os.PathLike[str],
    *,
    codec: Codec | None = None,
    index: str = DEFAULT_INDEX,
    durability: str = DURABILITY_FULL,
) -> DBDB:
    db_path = Path(path)
    if db_path.parent and str(db_path.parent) != ".":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return DBDB(
        _open_database_file(db_path),
        path=db_path,
        codec=codec,
        index=index,
        durability=durability,
    )
