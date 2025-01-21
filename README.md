# ImmuStore DB

ImmuStore DB is an educational key-value database engine built in Python. It explores append-only storage, immutable indexing, lazy references, atomic commits, compaction, and command-line tooling without hiding the storage mechanics behind a large framework.

The project is inspired by DBDB-style database internals: updates create new tree paths, records are appended to disk, and a commit becomes visible by swapping a single root address.

## Features

- Persistent key-value storage with a dictionary-style API
- Append-only file records with a reserved superblock
- Immutable binary tree index with lazy node/value loading
- Atomic root commits with explicit disk flushes
- Cross-platform file locking around writes
- JSON, text, and bytes codecs
- Transaction-like `update_value` helper
- Atomic transaction context for grouped writes
- Prefix and range scans over sorted keys
- Integrity audit reports for tree metadata and value reachability
- Database compaction for reclaiming stale append-only records
- CLI commands for `get`, `set`, `delete`, `keys`, `scan`, `audit`, `stats`, and `compact`
- Storage format and architecture documentation
- Unit tests for API, tree behavior, storage, compaction, and CLI flows

## Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## Python API

```python
import imustore

with imustore.connect("example.db") as db:
    db["name"] = "Ada"
    db["profile"] = {"language": "Python", "role": "systems"}
    db.commit()

    print(db["profile"])

    with db.transaction() as tx:
        tx.set("profile:ada", {"language": "Python"})
        tx.update("visits", lambda value: value + 1, default=0)

    print(list(db.scan(prefix="profile:")))
    print(db.audit().as_dict())
```

## CLI

```bash
python -m imustore.tool set example.db name Ada
python -m imustore.tool get example.db name
python -m imustore.tool keys example.db --prefix profile:
python -m imustore.tool scan example.db --start a --stop n
python -m imustore.tool audit example.db
python -m imustore.tool stats example.db
python -m imustore.tool compact example.db
```

## Project Layout

```txt
imustore/
  codec.py        Value codecs and CLI parsing
  locking.py      Cross-platform file lock wrapper
  physical.py     Append-only record storage
  logical.py      Lazy references and commit orchestration
  binary_tree.py  Immutable binary tree index
  interface.py    Public database mapping API
  tool.py         Command-line interface
  audit.py        Integrity report model
docs/
  architecture.md
  storage-format.md
tests/
  test_interface.py
  test_storage.py
  test_binary_tree.py
  test_compaction.py
  test_cli.py
```

## Durability Model

New values and tree nodes are appended first. After those records are flushed, the database writes the new root node address into the superblock. A reader can therefore observe the old tree or the new tree, but not a half-committed tree.

## Testing

```bash
python -m pytest
```

## Status

This is a learning project, not a production database. The code favors readability and explicit storage concepts so database internals can be studied directly.
