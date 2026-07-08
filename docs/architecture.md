# Architecture

ImmuStore DB splits the storage engine into layers so each piece has one job.

- `interface.py` exposes a dictionary-like API, handles encoding values, and selects the index engine.
- `logical.py` manages logical updates, commits, and lazy value references.
- `bplus_tree.py` implements the default copy-on-write B+ tree index.
- `binary_tree.py` implements a simpler immutable binary search tree, kept as a reference engine.
- `physical.py` owns append-only records, root-address commits, the live key count, flushing, and file statistics.
- `audit.py` provides structured integrity reports for tree metadata and value reachability.
- `tool.py` provides terminal access for reads, writes, deletes, compaction, and inspection.

Updates are staged in memory until `commit()` is called. A commit stores dirty values and tree nodes first, then writes a single root pointer in the superblock. Readers see either the previous root or the new root.

## Index engine

The default index is a **copy-on-write B+ tree**. All keys live in leaf nodes; internal nodes hold only separator keys that route searches. Because the tree is immutable, an insert or delete rebuilds only the nodes along the path from the touched leaf up to a new root, sharing every untouched subtree by address — the same shadow-paging discipline as the storage layer.

Key properties:

- **Balanced by construction.** Every leaf sits at the same depth, so lookups, inserts, deletes, and range scans are O(log n) with a high fan-out (default order 64). A sequence of sorted inserts stays shallow, where a plain binary search tree would degrade to a linked list.
- **Splits and merges.** When a node exceeds `order` keys it splits and pushes a separator to its parent; when a delete leaves a node under half-full it borrows from a sibling or merges, keeping occupancy invariants that `audit()` verifies.
- **O(1) size.** The live key count is tracked in the superblock and updated atomically with the root pointer, so `len(db)` never walks the tree.
- **Pluggable.** `connect(path, index="binary")` selects the reference binary tree instead; both share the `LogicalBase` commit machinery.

Range scans reuse the sorted tree traversal and prune whole subtrees that fall outside the requested bounds, without loading unrelated values into user code. The transaction context holds the write lock, keeps a root snapshot, and restores that snapshot if the block exits with an exception.
