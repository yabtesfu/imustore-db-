"""Multi-version concurrency control: point-in-time read snapshots.

A :class:`Snapshot` is a consistent, read-only view of the database frozen at
the moment it was taken. It works because the storage engine is copy-on-write:
old tree nodes are never overwritten, so pinning the root address of a committed
transaction pins the entire version behind it. Later writes append new nodes and
publish a new root, but the snapshot keeps reading the old one.

Two consequences make this real MVCC, in the same spirit as LMDB:

- **Readers never block writers, and writers never block readers.** A snapshot
  opens its own read-only file handle and never takes the write lock, so it can
  read while another connection is mid-commit. It simply won't see that commit.
- **Consistent reads.** Every read through one snapshot sees the same version,
  even if dozens of commits land in between -- ideal for a long report or a
  consistent multi-key scan.
"""

from __future__ import annotations

from .audit import AuditReport
from .bplus_tree import BPlusNodeRef, BPlusTree
from .physical import Storage


class Snapshot:
    def __init__(self, fileobj, root_address, key_count, *, codec, on_close=None):
        self._storage = Storage(fileobj, readonly=True)
        self._tree = BPlusTree(self._storage)
        self._root_ref = BPlusNodeRef(address=root_address)
        self._count = key_count
        self._codec = codec
        self._on_close = on_close
        self._closed = False

    def _root(self):
        return self._tree._follow(self._root_ref)

    def _raw(self, key):
        """Return the stored bytes for ``key`` at this version, or ``None``."""
        try:
            return self._tree._get(self._root(), key)
        except KeyError:
            return None

    def __getitem__(self, key):
        payload = self._raw(key)
        if payload is None:
            raise KeyError(key)
        return self._codec.decode(payload)

    def get(self, key, default=None):
        payload = self._raw(key)
        return default if payload is None else self._codec.decode(payload)

    def __contains__(self, key):
        return self._raw(key) is not None

    def __len__(self):
        return self._count

    def items(self):
        for key, payload in self._tree._items(self._root()):
            yield key, self._codec.decode(payload)

    def scan(self, *, prefix=None, start=None, stop=None):
        for key, payload in self._tree._range_items(self._root(), start, stop):
            if prefix is None or key.startswith(prefix):
                yield key, self._codec.decode(payload)

    def keys(self, *, prefix=None, start=None, stop=None):
        for key, _ in self.scan(prefix=prefix, start=start, stop=stop):
            yield key

    def audit(self) -> AuditReport:
        return self._tree.audit(self._root_ref, self._count)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._storage.close()
        if self._on_close is not None:
            self._on_close()

    def __enter__(self) -> "Snapshot":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
