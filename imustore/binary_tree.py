from __future__ import annotations

import pickle
from dataclasses import dataclass

from .audit import AuditReport
from .logical import LogicalBase, ValueRef


@dataclass(frozen=True)
class BinaryNode:
    left_ref: "BinaryNodeRef"
    key: str
    value_ref: ValueRef
    right_ref: "BinaryNodeRef"
    length: int

    @classmethod
    def from_node(cls, node: "BinaryNode", **updates):
        data = {
            "left_ref": node.left_ref,
            "key": node.key,
            "value_ref": node.value_ref,
            "right_ref": node.right_ref,
            "length": node.length,
        }
        data.update(updates)
        return cls(**data)

    def store_refs(self, storage) -> None:
        self.value_ref.store(storage)
        self.left_ref.store(storage)
        self.right_ref.store(storage)


class BinaryNodeRef(ValueRef):
    def prepare_to_store(self, storage) -> None:
        if self._referent is not None:
            self._referent.store_refs(storage)

    @staticmethod
    def referent_to_bytes(referent: BinaryNode) -> bytes:
        return pickle.dumps(
            {
                "left": referent.left_ref.address,
                "key": referent.key,
                "value": referent.value_ref.address,
                "right": referent.right_ref.address,
                "length": referent.length,
            },
            protocol=pickle.HIGHEST_PROTOCOL,
        )

    @staticmethod
    def bytes_to_referent(payload: bytes) -> BinaryNode:
        data = pickle.loads(payload)
        return BinaryNode(
            left_ref=BinaryNodeRef(address=data["left"]),
            key=data["key"],
            value_ref=ValueRef(address=data["value"]),
            right_ref=BinaryNodeRef(address=data["right"]),
            length=data["length"],
        )


class BinaryTree(LogicalBase):
    node_ref_class = BinaryNodeRef

    def _ref_length(self, ref: BinaryNodeRef) -> int:
        node = self._follow(ref)
        return node.length if node else 0

    def _node_ref(self, left_ref: BinaryNodeRef, key: str, value_ref: ValueRef, right_ref: BinaryNodeRef):
        return BinaryNodeRef(
            referent=BinaryNode(
                left_ref=left_ref,
                key=key,
                value_ref=value_ref,
                right_ref=right_ref,
                length=1 + self._ref_length(left_ref) + self._ref_length(right_ref),
            )
        )

    def _get(self, node: BinaryNode | None, key: str) -> bytes:
        while node is not None:
            if key < node.key:
                node = self._follow(node.left_ref)
            elif node.key < key:
                node = self._follow(node.right_ref)
            else:
                return self._follow(node.value_ref)
        raise KeyError(key)

    def _insert(self, node: BinaryNode | None, key: str, value_ref: ValueRef) -> BinaryNodeRef:
        if node is None:
            return self._node_ref(BinaryNodeRef(), key, value_ref, BinaryNodeRef())
        if key < node.key:
            left_ref = self._insert(self._follow(node.left_ref), key, value_ref)
            return self._node_ref(left_ref, node.key, node.value_ref, node.right_ref)
        if node.key < key:
            right_ref = self._insert(self._follow(node.right_ref), key, value_ref)
            return self._node_ref(node.left_ref, node.key, node.value_ref, right_ref)
        return self._node_ref(node.left_ref, key, value_ref, node.right_ref)

    def _delete(self, node: BinaryNode | None, key: str) -> BinaryNodeRef:
        if node is None:
            raise KeyError(key)
        if key < node.key:
            left_ref = self._delete(self._follow(node.left_ref), key)
            return self._node_ref(left_ref, node.key, node.value_ref, node.right_ref)
        if node.key < key:
            right_ref = self._delete(self._follow(node.right_ref), key)
            return self._node_ref(node.left_ref, node.key, node.value_ref, right_ref)

        left = self._follow(node.left_ref)
        right = self._follow(node.right_ref)
        if left is None:
            return node.right_ref
        if right is None:
            return node.left_ref
        successor, new_right_ref = self._pop_min(right)
        return self._node_ref(node.left_ref, successor.key, successor.value_ref, new_right_ref)

    def _pop_min(self, node: BinaryNode) -> tuple[BinaryNode, BinaryNodeRef]:
        left = self._follow(node.left_ref)
        if left is None:
            return node, node.right_ref
        successor, new_left_ref = self._pop_min(left)
        return successor, self._node_ref(new_left_ref, node.key, node.value_ref, node.right_ref)

    def _items(self, node: BinaryNode | None):
        if node is None:
            return
        yield from self._items(self._follow(node.left_ref))
        yield node.key, self._follow(node.value_ref)
        yield from self._items(self._follow(node.right_ref))

    def _range_items(self, node: BinaryNode | None, start: str | None, stop: str | None):
        if node is None:
            return
        if start is None or start <= node.key:
            yield from self._range_items(self._follow(node.left_ref), start, stop)
        if (start is None or start <= node.key) and (stop is None or node.key < stop):
            yield node.key, self._follow(node.value_ref)
        if stop is None or node.key < stop:
            yield from self._range_items(self._follow(node.right_ref), start, stop)

    def range_items(self, start: str | None = None, stop: str | None = None):
        if not self._storage.locked:
            self._refresh_tree_ref()
        yield from self._range_items(self._follow(self._tree_ref), start, stop)

    def height(self) -> int:
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._height(self._follow(self._tree_ref))

    def _height(self, node: BinaryNode | None) -> int:
        if node is None:
            return 0
        return 1 + max(self._height(self._follow(node.left_ref)), self._height(self._follow(node.right_ref)))

    def audit(self) -> AuditReport:
        if not self._storage.locked:
            self._refresh_tree_ref()
        errors: list[str] = []
        values = 0

        def walk(node: BinaryNode | None, lower: str | None, upper: str | None, depth: int) -> tuple[int, int]:
            nonlocal values
            if node is None:
                return 0, depth - 1

            if lower is not None and node.key <= lower:
                errors.append(f"{node.key!r} is not greater than lower bound {lower!r}")
            if upper is not None and node.key >= upper:
                errors.append(f"{node.key!r} is not less than upper bound {upper!r}")

            try:
                self._follow(node.value_ref)
                values += 1
            except Exception as exc:  # pragma: no cover - defensive corruption path
                errors.append(f"{node.key!r} value could not be loaded: {exc}")

            left_count, left_height = walk(self._follow(node.left_ref), lower, node.key, depth + 1)
            right_count, right_height = walk(self._follow(node.right_ref), node.key, upper, depth + 1)
            expected_length = 1 + left_count + right_count
            if node.length != expected_length:
                errors.append(f"{node.key!r} length is {node.length}, expected {expected_length}")
            return expected_length, max(depth, left_height, right_height)

        root = self._follow(self._tree_ref)
        nodes, height = walk(root, None, None, 1)
        if root is None:
            height = 0
        return AuditReport(keys=nodes, nodes=nodes, values=values, height=height, errors=tuple(errors))
