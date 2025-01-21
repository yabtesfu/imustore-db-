# Architecture

ImmuStore DB splits the storage engine into layers so each piece has one job.

- `interface.py` exposes a dictionary-like API and handles encoding values.
- `logical.py` manages logical updates, commits, and lazy value references.
- `binary_tree.py` implements an immutable binary search tree index.
- `physical.py` owns append-only records, root-address commits, flushing, and file statistics.
- `audit.py` provides structured integrity reports for tree metadata and value reachability.
- `tool.py` provides terminal access for reads, writes, deletes, compaction, and inspection.

Updates are staged in memory until `commit()` is called. A commit stores dirty values and tree nodes first, then writes a single root pointer in the superblock. Readers see either the previous root or the new root.

Range scans reuse the sorted tree traversal and filter optional prefixes without loading unrelated values into user code. The transaction context holds the write lock, keeps a root snapshot, and restores that snapshot if the block exits with an exception.
