#!/usr/bin/env python3
"""Benchmark suite for ImmuStore DB.

Run:  python -m bench.benchmark [--keys N]

Reports three things:

1. Throughput -- bulk load, random point reads, and full scan, for the B+ tree
   engine, with SQLite (a mature C engine) as a reference baseline. ImmuStore is
   pure Python, so it is naturally slower than SQLite; the point is to measure
   honestly and to show the operations scale, not to claim parity.

2. Why the index matters -- inserting *sorted* keys into the balanced B+ tree
   versus the naive binary tree. The binary tree degrades to a linked list
   (height == N, and it overflows Python's recursion stack past ~1000 keys),
   which is exactly the "toy" behaviour the B+ tree fixes.

3. Durability cost -- commit throughput with fsync on (``full``) versus off
   (``none``).
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import imustore  # noqa: E402
from imustore.bplus_tree import BPlusTree  # noqa: E402
from imustore.binary_tree import BinaryTree  # noqa: E402
from imustore.physical import Storage  # noqa: E402


def _timed(fn):
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _rate(count, seconds):
    return f"{count / seconds:,.0f}/s" if seconds > 0 else "n/a"


def bench_throughput(tmp: Path, n: int) -> list[str]:
    keys = [f"key:{i:08d}" for i in range(n)]
    random.Random(0).shuffle(keys)
    read_keys = keys[: n // 2]

    # -- ImmuStore (B+ tree) --------------------------------------------------
    db = imustore.connect(tmp / "immustore.db")

    def load_immu():
        for k in keys:
            db[k] = k
        db.commit()

    immu_write = _timed(load_immu)

    def read_immu():
        for k in read_keys:
            assert db[k] == k

    immu_read = _timed(read_immu)
    immu_scan = _timed(lambda: sum(1 for _ in db.items()))
    db.close()

    # -- SQLite reference (C engine) -----------------------------------------
    conn = sqlite3.connect(str(tmp / "reference.sqlite"))
    conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")

    def load_sqlite():
        conn.executemany("INSERT INTO kv VALUES (?, ?)", ((k, k) for k in keys))
        conn.commit()

    sqlite_write = _timed(load_sqlite)

    def read_sqlite():
        cur = conn.cursor()
        for k in read_keys:
            cur.execute("SELECT v FROM kv WHERE k = ?", (k,))
            assert cur.fetchone()[0] == k

    sqlite_read = _timed(read_sqlite)
    sqlite_scan = _timed(lambda: conn.execute("SELECT k, v FROM kv").fetchall() and None)
    conn.close()

    rows = [
        f"Benchmarked on Python {sys.version.split()[0]} with {n:,} keys.",
        "",
        "| Operation | ImmuStore (B+ tree, pure Python) | SQLite (reference, C) |",
        "| --- | --- | --- |",
        f"| Bulk load | {_rate(n, immu_write)} | {_rate(n, sqlite_write)} |",
        f"| Random point reads | {_rate(len(read_keys), immu_read)} | {_rate(len(read_keys), sqlite_read)} |",
        f"| Full scan | {_rate(n, immu_scan)} | {_rate(n, sqlite_scan)} |",
    ]
    return rows


def bench_index_matters(tmp: Path) -> list[str]:
    rows = [
        "Inserting **sorted** keys -- the worst case for an unbalanced tree.",
        "",
        "| Sorted keys | B+ tree height | B+ tree time | Binary tree height | Binary tree time |",
        "| --- | --- | --- | --- | --- |",
    ]
    for n in (500, 1000, 5000):
        with (tmp / f"bp{n}.db").open("w+b") as fileobj:
            tree = BPlusTree(Storage(fileobj))
            bp_time = _timed(lambda: [tree.set(f"k{i:07d}", b"v") for i in range(n)])
            tree.commit()
            bp_height = tree.height()

        with (tmp / f"bt{n}.db").open("w+b") as fileobj:
            btree = BinaryTree(Storage(fileobj))
            try:
                bt_time = _timed(lambda: [btree.set(f"k{i:07d}", b"v") for i in range(n)])
                btree.commit()
                bt_height = str(btree.height())
                bt_time_str = f"{bt_time * 1000:.0f} ms"
            except RecursionError:
                bt_height = "stack overflow"
                bt_time_str = "crashed"
        rows.append(
            f"| {n:,} | {bp_height} | {bp_time * 1000:.0f} ms | {bt_height} | {bt_time_str} |"
        )
    return rows


def bench_durability(tmp: Path, n: int) -> list[str]:
    rows = [
        f"Commit throughput with one fsync per commit ({n:,} single-key commits).",
        "",
        "| Durability | Commits/sec |",
        "| --- | --- |",
    ]
    for mode in ("full", "none"):
        path = tmp / f"dur_{mode}.db"
        db = imustore.connect(path, durability=mode)

        def run():
            for i in range(n):
                db[f"k{i}"] = i
                db.commit()

        rows.append(f"| `{mode}` | {_rate(n, _timed(run))} |")
        db.close()
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark ImmuStore DB.")
    parser.add_argument("--keys", type=int, default=50_000, help="keys for the throughput benchmark")
    parser.add_argument("--commits", type=int, default=2_000, help="commits for the durability benchmark")
    args = parser.parse_args(argv)

    sys.setrecursionlimit(20_000)  # give the binary tree a fair chance before it overflows

    with tempfile.TemporaryDirectory(prefix="immustore-bench-") as raw:
        tmp = Path(raw)
        sections = [
            ("## 1. Throughput", bench_throughput(tmp, args.keys)),
            ("## 2. Why the balanced index matters", bench_index_matters(tmp)),
            ("## 3. Durability cost", bench_durability(tmp, args.commits)),
        ]

    print("# ImmuStore DB benchmarks\n")
    for title, rows in sections:
        print(title)
        print("\n".join(rows))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
