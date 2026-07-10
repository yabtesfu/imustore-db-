# Architecture

ImmuStore DB splits the storage engine into layers so each piece has one job.

- `interface.py` exposes a dictionary-like API, handles encoding values, and selects the index engine.
- `logical.py` manages logical updates, commits, and lazy value references.
- `bplus_tree.py` implements the default copy-on-write B+ tree index.
- `binary_tree.py` implements a simpler immutable binary search tree, kept as a reference engine.
- `physical.py` owns append-only records, checksums, crash recovery, transactional meta blocks, and file statistics.
- `audit.py` provides structured integrity reports for tree metadata and value reachability.
- `tool.py` provides terminal access for reads, writes, deletes, compaction, and inspection.
- `server.py` / `resp.py` / `pubsub.py` expose the database over the network with a real-time changefeed.

Updates are staged in memory until `commit()` is called. A commit stores dirty values and tree nodes first, then publishes a new meta block that points at the new root. Readers see either the previous root or the new root.

## Durability and crash recovery

ImmuStore uses **shadow paging**: writes only ever append: an update never
overwrites live data, and the commit is an atomic pointer flip to a new root.
This is the same crash-recovery paradigm as LMDB, and like LMDB it needs no
separate write-ahead log: a redo log would be redundant because the tree is
already immutable and the root swap is already atomic. Instead, three
mechanisms in `physical.py` make that swap provably crash-safe:

- **Per-record checksums.** Every record carries a CRC32 of its payload, so a
  torn write or bit-rot is caught on read rather than returned as valid data.
- **Double-buffered meta blocks.** Two meta slots each carry a transaction id,
  the root address, the key count, the durable file length, and a CRC. A commit
  writes the *other* slot and fsyncs it before it is durable, so the last good
  meta is never in flight. On open the engine adopts the highest-id slot whose
  CRC verifies; a torn newest commit transparently falls back to the one before.
- **Torn-tail truncation.** A crash after appending records but before
  publishing the meta leaves orphaned records past the last durable length; open
  truncates them, restoring the file to its last committed transaction.

The `durability` setting (`full` by default, or `none`) trades fsync cost for
speed. See [storage-format.md](storage-format.md) for the on-disk layout.

## Network server and real-time changefeed

`server.py` is an `asyncio` TCP server speaking Redis's RESP protocol
(`resp.py`), so `redis-cli` and standard Redis clients connect unchanged. Each
client is one coroutine reading commands and writing replies.

The storage engine is single-writer, so the server funnels every database
operation through a single-thread executor. That serializes access safely
against the engine while keeping the event loop responsive during a commit's
fsync, and it is the seam where Phase 3 (MVCC / snapshot isolation) will let
reads run concurrently with writes.

`pubsub.py` is the change-data-capture broker. Because the server is the only
gateway for writes, it publishes a `(key, op, value)` event to the broker after
each committed mutation. A client that issued `SUBSCRIBE <prefix>` holds a
bounded queue; a background "pump" task drains it to the socket, so matching
changes stream to the client live. Bounded queues mean a slow subscriber sheds
messages rather than growing memory without limit.

## Index engine

The default index is a **copy-on-write B+ tree**. All keys live in leaf nodes; internal nodes hold only separator keys that route searches. Because the tree is immutable, an insert or delete rebuilds only the nodes along the path from the touched leaf up to a new root, sharing every untouched subtree by address — the same shadow-paging discipline as the storage layer.

Key properties:

- **Balanced by construction.** Every leaf sits at the same depth, so lookups, inserts, deletes, and range scans are O(log n) with a high fan-out (default order 64). A sequence of sorted inserts stays shallow, where a plain binary search tree would degrade to a linked list.
- **Splits and merges.** When a node exceeds `order` keys it splits and pushes a separator to its parent; when a delete leaves a node under half-full it borrows from a sibling or merges, keeping occupancy invariants that `audit()` verifies.
- **O(1) size.** The live key count is tracked in the superblock and updated atomically with the root pointer, so `len(db)` never walks the tree.
- **Pluggable.** `connect(path, index="binary")` selects the reference binary tree instead; both share the `LogicalBase` commit machinery.

Range scans reuse the sorted tree traversal and prune whole subtrees that fall outside the requested bounds, without loading unrelated values into user code. The transaction context holds the write lock, keeps a root snapshot, and restores that snapshot if the block exits with an exception.
