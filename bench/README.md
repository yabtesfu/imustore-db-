# Benchmarks

```bash
python -m bench.benchmark            # default: 50,000 keys
python -m bench.benchmark --keys 200000 --commits 5000
```

The suite reports three things:

1. **Throughput** — bulk load, random point reads, and full scan for the B+ tree
   engine, with SQLite as a reference baseline. ImmuStore is pure Python, so it
   is naturally slower than SQLite's C engine; the goal is honest measurement and
   showing the operations scale, not claiming parity.

2. **Why the balanced index matters** — inserting *sorted* keys, the worst case
   for an unbalanced tree. The B+ tree stays shallow (height ~3 for thousands of
   keys); the naive binary tree degrades to a linked list (height == N) and gets
   thousands of times slower before it overflows the recursion stack entirely.

3. **Durability cost** — commit throughput with fsync on (`full`) vs off (`none`).

Numbers are machine- and Python-version-dependent; treat them as relative, not
absolute. Representative results are quoted in the top-level [README](../README.md).
