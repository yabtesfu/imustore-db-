from __future__ import annotations

from .physical import Storage


class ValueRef:
    def __init__(self, referent: bytes | None = None, address: int = 0):
        self._referent = referent
        self._address = address

    @property
    def address(self) -> int:
        return self._address

    @property
    def referent(self) -> bytes | None:
        return self._referent

    def get(self, storage: Storage):
        if self._referent is None and self._address:
            self._referent = self.bytes_to_referent(storage.read(self._address))
        return self._referent

    def store(self, storage: Storage) -> None:
        if self._referent is not None and not self._address:
            self.prepare_to_store(storage)
            self._address = storage.write(self.referent_to_bytes(self._referent))

    def prepare_to_store(self, storage: Storage) -> None:
        return None

    @staticmethod
    def referent_to_bytes(referent: bytes) -> bytes:
        return referent

    @staticmethod
    def bytes_to_referent(payload: bytes) -> bytes:
        return payload


class LogicalBase:
    node_ref_class = None
    value_ref_class = ValueRef

    def __init__(self, storage: Storage):
        self._storage = storage
        self._refresh_tree_ref()

    def _refresh_tree_ref(self) -> None:
        self._tree_ref = self.node_ref_class(address=self._storage.get_root_address())

    def _follow(self, ref):
        if ref is None:
            return None
        return ref.get(self._storage)

    def get(self, key: str) -> bytes:
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def set(self, key: str, value: bytes) -> None:
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(self._follow(self._tree_ref), key, self.value_ref_class(value))

    def delete(self, key: str) -> None:
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._delete(self._follow(self._tree_ref), key)

    def commit(self) -> None:
        self._tree_ref.store(self._storage)
        self._storage.commit_root_address(self._tree_ref.address)
        self._refresh_tree_ref()

    def items(self):
        if not self._storage.locked:
            self._refresh_tree_ref()
        yield from self._items(self._follow(self._tree_ref))

    def range_items(self, start: str | None = None, stop: str | None = None):
        if not self._storage.locked:
            self._refresh_tree_ref()
        yield from self._range_items(self._follow(self._tree_ref), start, stop)

    def length(self) -> int:
        if not self._storage.locked:
            self._refresh_tree_ref()
        node = self._follow(self._tree_ref)
        return node.length if node else 0
