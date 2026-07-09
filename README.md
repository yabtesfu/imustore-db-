# ImmuStore DB

[![CI](https://github.com/yabtesfu/imustore-db-/actions/workflows/ci.yml/badge.svg)](https://github.com/yabtesfu/imustore-db-/actions/workflows/ci.yml)

ImmuStore DB is key-value database engine built in Python. It explores append-only storage, immutable indexing, lazy references, atomic commits, compaction, and command-line tooling without hiding the storage mechanics behind a large framework.

The project is inspired by DBDB-style database internals: updates create new tree paths, records are appended to disk, and a commit becomes visible by swapping a single root address. The default index is a **copy-on-write B+ tree** — the same immutable-root design LMDB uses — so every leaf stays at the same depth and lookups remain shallow even after millions of ordered inserts.

## Features

- Persistent key-value storage with a dictionary-style API
- Append-only file records with a reserved superblock
- Copy-on-write **B+ tree** index (default): balanced, high fan-out, O(log n) reads/writes/scans
- Pluggable index engine (`index="bplus"` default, `index="binary"` reference tree)
- Lazy node/value loading so scans never touch unrelated values
- O(1) `len()` via a live key count stored in the superblock
- Crash-safe commits: per-record CRC32 checksums, double-buffered meta blocks, and torn-tail recovery on open
- Configurable durability (`durability="full"` default, or `"none"` for speed)
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
  bplus_tree.py   Copy-on-write B+ tree index (default engine)
  binary_tree.py  Immutable binary tree index (reference engine)
  interface.py    Public database mapping API
  tool.py         Command-line interface
  audit.py        Integrity report model
docs/
  architecture.md
  storage-format.md
tests/
  test_interface.py
  test_storage.py
  test_bplus_tree.py
  test_binary_tree.py
  test_recovery.py
  test_compaction.py
  test_cli.py
```

## Durability Model

ImmuStore uses shadow paging (the same crash-recovery approach as LMDB), so no write-ahead log is needed. New values and tree nodes are appended and fsynced first; then the commit publishes a new **meta block** pointing at the new root. A reader always observes the old tree or the new tree, never a half-committed one.

Crashes are handled by three mechanisms:

- **Per-record CRC32 checksums** detect torn writes and bit-rot on read.
- **Double-buffered meta blocks** (two slots, each with a transaction id and checksum) mean a torn meta write during a commit transparently falls back to the previous committed transaction.
- **Torn-tail truncation** on open rolls back orphaned records left by a crash mid-append.

`durability="none"` skips fsyncs for throughput at the cost of losing recent commits on power loss. See [docs/architecture.md](docs/architecture.md) and [docs/storage-format.md](docs/storage-format.md) for details.

## Testing

```bash
python -m pytest
```

The suite includes a randomized fuzz test for the B+ tree (checked against a reference `dict`) and a crash-injection matrix that simulates power loss at the byte level and asserts the database always recovers a committed state.

## Benchmarks

```bash
python -m bench.benchmark
```

Representative results (pure-Python engine; numbers are relative and machine-dependent):

**Why the balanced index matters** — inserting *sorted* keys, the worst case for an unbalanced tree:

| Sorted keys | B+ tree height | B+ tree time | Binary tree height | Binary tree time |
| --- | --- | --- | --- | --- |
| 1,000 | 2 | 4 ms | 1,000 | 1,103 ms |
| 5,000 | 3 | 26 ms | 5,000 | 30,451 ms |

The B+ tree stays shallow and fast; the naive binary tree degrades to a linked list (height == N, ~1000× slower) before it overflows the recursion stack entirely. See [bench/](bench/) for throughput and durability numbers.

## Status

This started as a learning project. It has since grown a balanced copy-on-write B+ tree index, crash-safe shadow-paging commits with checksums and recovery, benchmarks, and CI — the building blocks of a real storage engine, kept small enough to read end to end.
