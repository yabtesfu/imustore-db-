# Query Operations

ImmuStore keeps keys in sorted order, so scans can reuse the same traversal that powers iteration.

## Prefix Scans

Prefix scans are useful when callers model keys as namespaces.

```python
for key, value in db.scan(prefix="user:"):
    print(key, value)
```

The database still validates keys before scanning. Values are decoded through the configured codec only when the iterator reaches them.

## Range Scans

Range scans use inclusive `start` and exclusive `stop` bounds.

```python
for key, value in db.scan(start="order:", stop="user:"):
    print(key, value)
```

This keeps the public API close to Python slicing while preserving sorted key order.

## Audit Checks

`db.audit()` walks the tree, checks ordering bounds, verifies stored subtree lengths, and confirms values can be reached from each node.
