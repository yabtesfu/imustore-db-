# ImmuStore DB

[![CI](https://github.com/yabtesfu/imustore-db-/actions/workflows/ci.yml/badge.svg)](https://github.com/yabtesfu/imustore-db-/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Dependencies](https://img.shields.io/badge/runtime%20dependencies-zero-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

**A distributed database engine written from scratch in pure Python** — a copy-on-write B+ tree storage core with crash-safe durability, MVCC, a Redis-compatible network server with a real-time changefeed, Raft replication, a document/query layer, observability, and FoundationDB-style deterministic simulation testing. No third-party runtime dependencies, and small enough to read end to end.

It began as a DBDB-style learning project and grew, one deliberate phase at a time, into the building blocks of a real database. Writes never overwrite data: they append new immutable B+ tree nodes and publish a new root — the same shadow-paging design LMDB uses — so a reader always sees a consistent version and a commit is a single atomic pointer swap.

## What's inside

**Storage & indexing**
- Copy-on-write **B+ tree** index — balanced, high fan-out, O(log n) reads/writes/scans (pluggable; a simpler binary tree ships as a reference engine)
- Append-only records with lazy node/value loading, O(1) `len()`, sorted prefix/range scans, and compaction
- JSON / text / bytes codecs and structured integrity `audit()` reports

**Durability & crash recovery**
- Shadow-paging commits with per-record CRC32 checksums and **double-buffered meta blocks**
- Torn-tail truncation and torn-meta fallback on open; configurable fsync policy (`durability="full"`/`"none"`)

**Concurrency (MVCC)**
- Lock-free, consistent, point-in-time **read snapshots** (one writer, many readers — the LMDB model)
- Optimistic, snapshot-isolated **transactions** with write-conflict detection

**Distribution**
- **Raft consensus** — leader election, log replication, and automatic failover across a cluster

**Data model & queries**
- Document **collections** with secondary indexes, a **query planner** (index-or-scan), and **TTL** expiry

**Networking & real-time**
- Async server speaking Redis's **RESP protocol** (`redis-cli` and Redis clients work unchanged)
- **Change data capture**: `SUBSCRIBE` to a key prefix and stream live set/delete events

**Operability**
- **Prometheus** metrics + an admin HTTP endpoint; a Dockerfile and compose (single node or a 3-node Raft cluster)

**Correctness**
- **Deterministic simulation testing**: seed-reproducible fault injection against a simulated disk, plus property/fuzz tests and green CI on Python 3.10–3.12

## Built in seven phases

Each phase is a self-contained, tested, and reviewed step from an educational toy to a real engine:

| Phase | Theme | What landed |
| --- | --- | --- |
| 0 | Credibility | benchmark suite, CI, record checksums |
| 1 | A real index | copy-on-write B+ tree (replacing an unbalanced BST) |
| 2 | Durability | checksums, double-buffered meta blocks, crash recovery |
| 3 | Concurrency | MVCC snapshots + optimistic transactions |
| 4 | Networking | RESP server + real-time changefeed |
| 5 | Distribution | Raft replication |
| 6 | Query & ops | secondary indexes, query planner, TTL, metrics, Docker |
| 7 | Correctness | deterministic simulation testing |

> **Status:** a learning-grade engine, not a production database — it is pure Python and single-writer per node. The point is to implement real database internals directly, and to test them the way real databases are tested.

## Install

Requires Python 3.10+ and has no runtime dependencies.

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"          # dev extras: pytest, pytest-cov, ruff
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
  mvcc.py         MVCC read snapshots
  collection.py   Document collection: secondary indexes, queries, TTL
  server.py       Async RESP network server
  resp.py         RESP protocol codec
  pubsub.py       Change-data-capture broker
  metrics.py      Prometheus metrics registry
  admin.py        Admin HTTP endpoint (/metrics, /healthz, /stats)
  audit.py        Integrity report model
  simdisk.py      Simulated disk modeling fsync durability
  simulation.py   Deterministic simulation test driver
  raft/           Raft consensus (log, node, simulator, TCP transport)
docs/
  architecture.md
  storage-format.md
tests/
  test_interface.py
  test_storage.py
  test_bplus_tree.py
  test_binary_tree.py
  test_recovery.py
  test_mvcc.py
  test_raft.py
  test_collection.py
  test_metrics.py
  test_server.py
  test_simulation.py
  test_compaction.py
  test_cli.py
```

## Concurrency (MVCC)

Because the B+ tree is copy-on-write, ImmuStore gets multi-version concurrency control almost for free — the same model LMDB uses: **one writer at a time, many lock-free concurrent readers.**

A **snapshot** pins the committed version at the moment it is taken and reads from its own file handle, so it never blocks (or is blocked by) a writer and never sees later commits:

```python
snap = db.snapshot()          # frozen, consistent, point-in-time view
db["price"] = 130
db.commit()                   # snap is unaffected
print(snap["price"])          # still the old value
snap.close()
```

**Transactions are optimistic and snapshot-isolated.** The body reads a private snapshot plus its own buffered writes (read-your-writes) and holds no lock; at commit the transaction verifies that nothing it wrote changed underneath it, else raises `ConflictError` so you can retry:

```python
from imustore import ConflictError

tx = db.begin()
tx.update("stock", lambda n: n - 1)
try:
    tx.commit()
except ConflictError:
    ...                       # a concurrent commit touched "stock"; retry
```

`with db.transaction() as tx:` commits on success and rolls back on exception. See `examples/mvcc_snapshot.py`.

## Documents, indexes & queries

A `Collection` stores JSON documents and maintains **secondary indexes** so you can query by field without scanning everything. Index entries are written in the *same transaction* as the document, so they never drift out of sync.

```python
from imustore import connect, Collection

db = connect("people.db")
people = Collection(db, indexes=["team", "status"])
people.set("u1", {"name": "Ada", "team": "core", "status": "active", "age": 36}, ttl=3600)

q = people.query().where("team", "core").where("status", "active").filter(lambda d: d["age"] > 30)
q.explain()   # {'plan': 'index', 'indexed_fields': ['team', 'status'], ...}
q.all()       # [("u1", {...})]
```

The query engine does real query planning: equality predicates on indexed fields are answered by B+ tree range scans over the indexes (and intersected); the rest is applied as a residual filter; with no usable index it falls back to a full scan. Keys can carry a **TTL** — expired documents are hidden from reads and reclaimed by `sweep()`. See `examples/query_demo.py`.

## Network server & real-time changefeed

ImmuStore ships a networked server that speaks Redis's RESP protocol, so `redis-cli` and any Redis client library work against it unchanged:

```bash
python -m imustore.server --path data.db --port 6380
```

```bash
redis-cli -p 6380 set greeting hello
redis-cli -p 6380 get greeting          # "hello"
redis-cli -p 6380 keys 'user:*'
redis-cli -p 6380 subscribe user:       # live change stream for user:* keys
```

The headline feature is **change data capture**: `SUBSCRIBE <prefix>` turns the database into a live stream. Every time a key under that prefix is set or deleted, subscribers are pushed a change event the instant it commits — the building block for cache invalidation, materialized views, or real-time UIs.

```
$ python examples/realtime_subscriber.py
writer: SET user:1 Ada         -> OK
  >> live change on 'user:': {'key': 'user:1', 'op': 'set', 'value': 'Ada'}
writer: SET other:9 not-a-user -> OK
writer: DEL user:1             -> 1
  >> live change on 'user:': {'key': 'user:1', 'op': 'del', 'value': None}
```

The server is built on stdlib `asyncio`. Database access is funnelled through a single-thread executor, which serializes writes safely against the single-writer engine while keeping the event loop responsive during a commit's fsync. Supported commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `KEYS`, `DBSIZE`, `SUBSCRIBE`, `UNSUBSCRIBE`, `QUIT`.

## Distribution & consensus (Raft)

ImmuStore can replicate across a cluster using a from-scratch implementation of the **Raft consensus algorithm** (`imustore/raft/`) — leader election, log replication, and automatic failover — with the database as the replicated state machine. Committed log entries are applied to each node's ImmuStore engine, and the applied index rides in the same commit as the data, so apply is crash-safe and exactly-once.

The consensus core reads no clock and does no I/O: it is driven purely by `tick()` and `step(message)` and returns the messages it wants sent. That determinism means the **same code** runs two ways:

- **A simulated cluster** for reproducible, fault-injecting tests — crash nodes, restart them, partition the network — with zero flakiness. The suite asserts Raft's safety properties (at most one leader per term; committed logs never diverge) across 120 rounds of randomized chaos.
- **A real TCP cluster** over `asyncio`. Launch nodes with the CLI and watch a leader get elected and survive being killed:

```bash
python -m imustore.raft.server --id 0 --port 7400 --db node0.db --peer 1:127.0.0.1:7401 --peer 2:127.0.0.1:7402
# ...start nodes 1 and 2 similarly...
```

```bash
python examples/raft_cluster.py   # 3-node cluster in one process: elect, replicate, kill leader, re-elect
```

See [docs/raft.md](docs/raft.md).

## Observability & deployment

The server exposes **Prometheus metrics** (command counts, latency histograms, connection gauge, change-event counter) over a plain HTTP endpoint — no client library needed:

```bash
python -m imustore.server --port 6380 --metrics-port 9100
curl localhost:9100/metrics     # Prometheus text exposition
curl localhost:9100/healthz     # liveness probe
redis-cli -p 6380 info          # storage stats over RESP
```

Container images ship too:

```bash
docker compose up immustore              # a single RESP server + metrics
docker compose up raft0 raft1 raft2      # a 3-node Raft cluster
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

## Deterministic simulation testing

The storage engine is tested the way FoundationDB and TigerBeetle test theirs: run it against a **simulated disk** (`imustore/simdisk.py`) that models fsync durability, and drive it with a workload and faults drawn entirely from one seeded RNG. Faults include clean crashes, crashes *mid-commit*, torn trailing writes, and corrupted meta blocks. After every recovery two invariants are checked against a reference model: the database reopens with a clean `audit()`, and its committed state is the last committed version or the one before it.

Because everything comes from the seed, a run is perfectly reproducible — if a seed finds a bug, it replays byte for byte:

```bash
python -m imustore.simulation --runs 1000 --steps 300   # sweep 1000 seeds
python -m imustore.simulation --seed 4712 --steps 400   # replay one seed
```

A 1000-seed sweep exercises tens of thousands of crash/recover cycles. (The distributed layer gets the same treatment: `SimCluster` runs the Raft cluster deterministically through crashes, partitions, and restarts while checking Raft's safety properties.)

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

## Documentation

- [docs/architecture.md](docs/architecture.md) — how the layers fit together
- [docs/storage-format.md](docs/storage-format.md) — the on-disk format
- [docs/raft.md](docs/raft.md) — the consensus design
- [docs/query-operations.md](docs/query-operations.md) — query and scan operations
