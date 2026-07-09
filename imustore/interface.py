from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Callable

from .audit import AuditReport
from .binary_tree import BinaryTree
from .bplus_tree import BPlusTree
from .codec import Codec, JsonCodec
from .errors import DatabaseClosedError, KeyEncodingError
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

    def transaction(self) -> "Transaction":
        return Transaction(self)

    def compact(self) -> StorageStats:
        self._assert_open()
        if self._path is None:
            raise ValueError("compaction requires a path-backed database")

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


class Transaction:
    def __init__(self, db: DBDB):
        self._db = db
        self._snapshot_ref = None

    def __enter__(self) -> "Transaction":
        self._db._assert_open()
        if self._db._storage.lock():
            self._db._tree._refresh_tree_ref()
        self._snapshot_ref = self._db._tree._tree_ref
        return self

    def set(self, key: str, value: Any) -> None:
        self._db[key] = value

    def delete(self, key: str) -> None:
        del self._db[key]

    def update(self, key: str, updater: Callable[[Any], Any], *, default: Any = None) -> Any:
        return self._db.update_value(key, updater, default=default)

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc_type is not None:
            if self._snapshot_ref is not None:
                self._db._tree._tree_ref = self._snapshot_ref
            if self._db._storage.locked:
                self._db._storage.unlock()
            return False
        self._db.commit()
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
