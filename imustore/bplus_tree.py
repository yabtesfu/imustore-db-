"""A copy-on-write B+ tree index.

This is the default ImmuStore index. Unlike the reference binary tree in
``binary_tree.py`` (which degrades to O(n) on sorted inserts), a B+ tree keeps
every leaf at the same depth and packs many keys per node, so lookups, inserts,
deletes, and range scans are O(log n) with a very high fan-out.

The tree is *immutable / copy-on-write*, exactly like the rest of the engine:
an update never edits a node in place. Instead it rebuilds the path from the
touched leaf up to a brand-new root, sharing every untouched subtree by
address. Committing swaps a single root pointer in the superblock, so a reader
always sees a fully-formed old or new tree -- never a half-written one. This is
the same design LMDB uses, and it makes cheap snapshots (and future MVCC) fall
out for free.

Structure:
- Leaf nodes hold sorted ``keys`` and aligned ``values`` (value references).
- Internal nodes hold ``keys`` (separators/routers) and ``children`` references,
  where ``len(children) == len(keys) + 1``.
- All keys live in the leaves; internal keys only route searches. A separator
  key ``s`` sends searches for ``k >= s`` into the right child.

The total live-key count is tracked in the storage superblock rather than in
every node, which keeps ``len(db)`` O(1) and node metadata small.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass

import pickle

from .audit import AuditReport
from .logical import LogicalBase, ValueRef

# Maximum number of keys stored in a single node before it splits. A larger
# order means a shallower, wider tree (fewer disk reads per lookup); the default
# keeps nodes comfortably within a single storage page for typical keys.
DEFAULT_ORDER = 64


@dataclass(frozen=True)
class LeafNode:
    keys: tuple
    values: tuple  # tuple[ValueRef], aligned with ``keys``

    def store_refs(self, storage) -> None:
        for value_ref in self.values:
            value_ref.store(storage)


@dataclass(frozen=True)
class InternalNode:
    keys: tuple
    children: tuple  # tuple[BPlusNodeRef], len == len(keys) + 1

    def store_refs(self, storage) -> None:
        for child_ref in self.children:
            child_ref.store(storage)


class BPlusNodeRef(ValueRef):
    """A lazy, address-backed reference to a leaf or internal node."""

    def prepare_to_store(self, storage) -> None:
        if self._referent is not None:
            self._referent.store_refs(storage)

    @staticmethod
    def referent_to_bytes(referent) -> bytes:
        if isinstance(referent, LeafNode):
            payload = {
                "leaf": True,
                "keys": list(referent.keys),
                "values": [value_ref.address for value_ref in referent.values],
            }
        else:
            payload = {
                "leaf": False,
                "keys": list(referent.keys),
                "children": [child_ref.address for child_ref in referent.children],
            }
        return pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def bytes_to_referent(payload: bytes):
        data = pickle.loads(payload)
        if data["leaf"]:
            return LeafNode(
                keys=tuple(data["keys"]),
                values=tuple(ValueRef(address=address) for address in data["values"]),
            )
        return InternalNode(
            keys=tuple(data["keys"]),
            children=tuple(BPlusNodeRef(address=address) for address in data["children"]),
        )


class BPlusTree(LogicalBase):
    node_ref_class = BPlusNodeRef

    def __init__(self, storage, order: int = DEFAULT_ORDER):
        if order < 3:
            raise ValueError("B+ tree order must be at least 3")
        self._order = order
        self._min_keys = order // 2
        self._count = 0
        super().__init__(storage)

    # -- root / count bookkeeping ------------------------------------------
    def _refresh_tree_ref(self) -> None:
        self._tree_ref = self.node_ref_class(address=self._storage.get_root_address())
        self._count = self._storage.get_key_count()

    def commit(self) -> None:
        self._tree_ref.store(self._storage)
        self._storage.commit_root_address(self._tree_ref.address, self._count)
        self._refresh_tree_ref()

    def length(self) -> int:
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._count

    # -- node constructors --------------------------------------------------
    def _leaf_ref(self, keys, values) -> BPlusNodeRef:
        return BPlusNodeRef(referent=LeafNode(keys=tuple(keys), values=tuple(values)))

    def _internal_ref(self, keys, children) -> BPlusNodeRef:
        return BPlusNodeRef(referent=InternalNode(keys=tuple(keys), children=tuple(children)))

    @staticmethod
    def _num_keys(node) -> int:
        return len(node.keys)

    # -- reads --------------------------------------------------------------
    def _get(self, node, key: str) -> bytes:
        while node is not None:
            if isinstance(node, LeafNode):
                index = bisect_left(node.keys, key)
                if index < len(node.keys) and node.keys[index] == key:
                    return self._follow(node.values[index])
                break
            node = self._follow(node.children[bisect_right(node.keys, key)])
        raise KeyError(key)

    def _items(self, node):
        if node is None:
            return
        if isinstance(node, LeafNode):
            for key, value_ref in zip(node.keys, node.values):
                yield key, self._follow(value_ref)
            return
        for child_ref in node.children:
            yield from self._items(self._follow(child_ref))

    def _range_items(self, node, start, stop):
        if node is None:
            return
        if isinstance(node, LeafNode):
            for key, value_ref in zip(node.keys, node.values):
                if start is not None and key < start:
                    continue
                if stop is not None and key >= stop:
                    break
                yield key, self._follow(value_ref)
            return
        for index, child_ref in enumerate(node.children):
            left_bound = node.keys[index - 1] if index > 0 else None
            right_bound = node.keys[index] if index < len(node.keys) else None
            if stop is not None and left_bound is not None and left_bound >= stop:
                break
            if start is not None and right_bound is not None and right_bound <= start:
                continue
            yield from self._range_items(self._follow(child_ref), start, stop)

    # -- insert -------------------------------------------------------------
    def _insert(self, node, key: str, value_ref: ValueRef) -> BPlusNodeRef:
        if node is None:
            self._count += 1
            return self._leaf_ref([key], [value_ref])
        new_ref, split = self._insert_rec(node, key, value_ref)
        if split is None:
            return new_ref
        separator, right_ref = split
        return self._internal_ref([separator], [new_ref, right_ref])

    def _insert_rec(self, node, key, value_ref):
        if isinstance(node, LeafNode):
            keys = list(node.keys)
            values = list(node.values)
            index = bisect_left(keys, key)
            if index < len(keys) and keys[index] == key:
                values[index] = value_ref  # overwrite: live-key count unchanged
            else:
                keys.insert(index, key)
                values.insert(index, value_ref)
                self._count += 1
            if len(keys) <= self._order:
                return self._leaf_ref(keys, values), None
            mid = len(keys) // 2
            left = self._leaf_ref(keys[:mid], values[:mid])
            right = self._leaf_ref(keys[mid:], values[mid:])
            return left, (keys[mid], right)  # separator copied up from the right leaf

        child_index = bisect_right(node.keys, key)
        child = self._follow(node.children[child_index])
        new_child_ref, split = self._insert_rec(child, key, value_ref)

        keys = list(node.keys)
        children = list(node.children)
        children[child_index] = new_child_ref
        if split is not None:
            separator, right_ref = split
            keys.insert(child_index, separator)
            children.insert(child_index + 1, right_ref)
        if len(keys) <= self._order:
            return self._internal_ref(keys, children), None
        mid = len(keys) // 2
        promoted = keys[mid]  # internal split promotes the middle key upward
        left = self._internal_ref(keys[:mid], children[: mid + 1])
        right = self._internal_ref(keys[mid + 1:], children[mid + 1:])
        return left, (promoted, right)

    # -- delete -------------------------------------------------------------
    def _delete(self, node, key: str) -> BPlusNodeRef:
        if node is None:
            raise KeyError(key)
        new_ref = self._delete_rec(node, key)
        new_node = self._follow(new_ref)
        if isinstance(new_node, InternalNode) and len(new_node.keys) == 0:
            return new_node.children[0]  # collapse a root that lost its last key
        if isinstance(new_node, LeafNode) and len(new_node.keys) == 0:
            return self.node_ref_class(address=0)  # empty tree
        return new_ref

    def _delete_rec(self, node, key) -> BPlusNodeRef:
        if isinstance(node, LeafNode):
            keys = list(node.keys)
            values = list(node.values)
            index = bisect_left(keys, key)
            if index >= len(keys) or keys[index] != key:
                raise KeyError(key)
            del keys[index]
            del values[index]
            self._count -= 1
            return self._leaf_ref(keys, values)

        child_index = bisect_right(node.keys, key)
        new_child_ref = self._delete_rec(self._follow(node.children[child_index]), key)

        keys = list(node.keys)
        children = list(node.children)
        children[child_index] = new_child_ref
        if self._num_keys(self._follow(new_child_ref)) >= self._min_keys:
            return self._internal_ref(keys, children)
        return self._rebalance(keys, children, child_index)

    def _rebalance(self, keys, children, index) -> BPlusNodeRef:
        """Fix an under-full child at ``children[index]`` by borrowing or merging."""
        child = self._follow(children[index])
        left = self._follow(children[index - 1]) if index > 0 else None
        right = self._follow(children[index + 1]) if index + 1 < len(children) else None

        if left is not None and self._num_keys(left) > self._min_keys:
            self._borrow_from_left(keys, children, index, left, child)
        elif right is not None and self._num_keys(right) > self._min_keys:
            self._borrow_from_right(keys, children, index, right, child)
        elif left is not None:
            self._merge(keys, children, index - 1, left, child)
        else:
            self._merge(keys, children, index, child, right)
        return self._internal_ref(keys, children)

    def _borrow_from_left(self, keys, children, index, left, child):
        if isinstance(child, LeafNode):
            children[index - 1] = self._leaf_ref(left.keys[:-1], left.values[:-1])
            children[index] = self._leaf_ref(
                (left.keys[-1],) + child.keys, (left.values[-1],) + child.values
            )
            keys[index - 1] = left.keys[-1]
        else:
            children[index - 1] = self._internal_ref(left.keys[:-1], left.children[:-1])
            children[index] = self._internal_ref(
                (keys[index - 1],) + child.keys, (left.children[-1],) + child.children
            )
            keys[index - 1] = left.keys[-1]

    def _borrow_from_right(self, keys, children, index, right, child):
        if isinstance(child, LeafNode):
            children[index] = self._leaf_ref(
                child.keys + (right.keys[0],), child.values + (right.values[0],)
            )
            children[index + 1] = self._leaf_ref(right.keys[1:], right.values[1:])
            keys[index] = right.keys[1]
        else:
            children[index] = self._internal_ref(
                child.keys + (keys[index],), child.children + (right.children[0],)
            )
            children[index + 1] = self._internal_ref(right.keys[1:], right.children[1:])
            keys[index] = right.keys[0]

    def _merge(self, keys, children, left_index, left, right):
        """Merge ``right`` into ``left`` and drop the separator at ``left_index``."""
        if isinstance(left, LeafNode):
            merged = self._leaf_ref(left.keys + right.keys, left.values + right.values)
        else:
            merged = self._internal_ref(
                left.keys + (keys[left_index],) + right.keys,
                left.children + right.children,
            )
        children[left_index] = merged
        del children[left_index + 1]
        del keys[left_index]

    # -- diagnostics --------------------------------------------------------
    def height(self) -> int:
        if not self._storage.locked:
            self._refresh_tree_ref()
        node = self._follow(self._tree_ref)
        height = 0
        while isinstance(node, InternalNode):
            height += 1
            node = self._follow(node.children[0])
        return height + (1 if isinstance(node, LeafNode) else 0)

    def audit(self, root_ref=None, count=None) -> AuditReport:
        # With no arguments this audits the current committed tree; a Snapshot
        # passes its pinned root so it can audit a specific version.
        if root_ref is None:
            if not self._storage.locked:
                self._refresh_tree_ref()
            root_ref = self._tree_ref
            count = self._count
        errors: list = []
        counters = {"nodes": 0, "values": 0, "leaf_keys": 0}

        def check(node, lower, upper, is_root) -> int:
            counters["nodes"] += 1
            keys = node.keys
            for smaller, larger in zip(keys, keys[1:]):
                if not smaller < larger:
                    errors.append(f"keys out of order: {smaller!r} !< {larger!r}")
            for key in keys:
                if lower is not None and key < lower:
                    errors.append(f"{key!r} below lower bound {lower!r}")
                if upper is not None and key >= upper:
                    errors.append(f"{key!r} not below upper bound {upper!r}")

            if len(keys) > self._order:
                errors.append(f"node overflow: {len(keys)} keys exceeds order {self._order}")
            if not is_root and len(keys) < self._min_keys:
                errors.append(f"node underflow: {len(keys)} keys below minimum {self._min_keys}")

            if isinstance(node, LeafNode):
                if len(node.values) != len(keys):
                    errors.append("leaf key/value count mismatch")
                for key, value_ref in zip(keys, node.values):
                    try:
                        self._follow(value_ref)
                        counters["values"] += 1
                    except Exception as exc:  # pragma: no cover - corruption path
                        errors.append(f"{key!r} value could not be loaded: {exc}")
                counters["leaf_keys"] += len(keys)
                return 1

            if len(node.children) != len(keys) + 1:
                errors.append("internal child/key count mismatch")
            if is_root and len(keys) < 1:
                errors.append("root internal node has no separator keys")
            heights = set()
            for child_index, child_ref in enumerate(node.children):
                lo = keys[child_index - 1] if child_index > 0 else lower
                hi = keys[child_index] if child_index < len(keys) else upper
                heights.add(check(self._follow(child_ref), lo, hi, False))
            if len(heights) > 1:
                errors.append("tree is not height-balanced across children")
            return 1 + (max(heights) if heights else 0)

        root = self._follow(root_ref)
        height = 0 if root is None else check(root, None, None, True)
        if counters["leaf_keys"] != count:
            errors.append(
                f"stored key count {count} != leaf key count {counters['leaf_keys']}"
            )
        return AuditReport(
            keys=counters["leaf_keys"],
            nodes=counters["nodes"],
            values=counters["values"],
            height=height,
            errors=tuple(errors),
        )
