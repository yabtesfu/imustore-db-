"""Tests for MVCC snapshots and optimistic transactions."""

import threading

import pytest

import imustore
from imustore.errors import ConflictError, ImmuStoreError


def test_snapshot_is_frozen_while_writes_continue(tmp_path):
    db = imustore.connect(tmp_path / "iso.db")
    for i in range(50):
        db[f"k{i}"] = 0
    db.commit()

    snap = db.snapshot()
    for i in range(50):
        db[f"k{i}"] = 1
    db.commit()

    assert all(snap[f"k{i}"] == 0 for i in range(50))  # snapshot unaffected by later commits
    assert len(snap) == 50

    newer = db.snapshot()
    assert all(newer[f"k{i}"] == 1 for i in range(50))  # a fresh snapshot sees the new version

    snap.close()
    newer.close()
    db.close()


def test_snapshot_reads_while_a_writer_holds_the_lock(tmp_path):
    db = imustore.connect(tmp_path / "nonblock.db")
    db["a"] = 1
    db.commit()

    db["a"] = 2  # acquires the write lock; not yet committed
    assert db._storage.locked

    with db.snapshot() as snap:
        assert snap["a"] == 1  # the reader is not blocked and sees committed data

    db.commit()
    assert db["a"] == 2
    db.close()


def test_snapshot_supports_scan_contains_and_audit(tmp_path):
    db = imustore.connect(tmp_path / "views.db")
    for key in ("user:1", "user:2", "order:1"):
        db[key] = key.upper()
    db.commit()

    with db.snapshot() as snap:
        assert list(snap.keys(prefix="user:")) == ["user:1", "user:2"]
        assert dict(snap.items()) == {
            "order:1": "ORDER:1",
            "user:1": "USER:1",
            "user:2": "USER:2",
        }
        assert "user:1" in snap and "missing" not in snap
        assert snap.get("missing") is None
        assert len(snap) == 3
        assert snap.audit().ok
    db.close()


def test_transaction_read_your_writes_and_commit(tmp_path):
    db = imustore.connect(tmp_path / "ryw.db")
    db["balance"] = 100
    db.commit()

    with db.transaction() as tx:
        assert tx.get("balance") == 100
        tx.set("balance", tx.get("balance") - 30)
        assert tx.get("balance") == 70  # read-your-writes inside the transaction
        assert db["balance"] == 100  # not visible outside until commit
    assert db["balance"] == 70
    db.close()


def test_write_write_conflict_is_detected_and_retry_succeeds(tmp_path):
    db = imustore.connect(tmp_path / "conflict.db")
    db["counter"] = 0
    db.commit()

    tx1 = db.begin()
    tx2 = db.begin()  # both start from counter == 0
    tx1.update("counter", lambda v: v + 5)
    tx2.update("counter", lambda v: v + 1)

    tx1.commit()  # commits counter == 5
    with pytest.raises(ConflictError):
        tx2.commit()  # counter changed since tx2 began -> conflict

    retry = db.begin()  # retry from the fresh version (counter == 5)
    retry.update("counter", lambda v: v + 1)
    retry.commit()
    assert db["counter"] == 6
    db.close()


def test_disjoint_transactions_do_not_conflict(tmp_path):
    db = imustore.connect(tmp_path / "disjoint.db")
    db["a"] = 0
    db["b"] = 0
    db.commit()

    tx1 = db.begin()
    tx2 = db.begin()
    tx1.set("a", 1)
    tx2.set("b", 2)
    tx1.commit()
    tx2.commit()  # different keys -> no false conflict

    assert db["a"] == 1 and db["b"] == 2
    db.close()


def test_transaction_abort_and_rollback_discard_writes(tmp_path):
    db = imustore.connect(tmp_path / "abort.db")
    db["x"] = 1
    db.commit()

    tx = db.begin()
    tx.set("x", 99)
    tx.abort()
    assert db["x"] == 1

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.set("x", 42)
            raise RuntimeError("boom")
    assert db["x"] == 1
    db.close()


def test_compaction_is_blocked_while_a_snapshot_is_open(tmp_path):
    db = imustore.connect(tmp_path / "compact.db")
    db["k"] = 1
    db.commit()

    snap = db.snapshot()
    with pytest.raises(ImmuStoreError):
        db.compact()
    snap.close()

    db.compact()  # allowed once the snapshot is closed
    assert db["k"] == 1
    db.close()


def test_concurrent_readers_are_isolated_from_a_concurrent_writer(tmp_path):
    db = imustore.connect(tmp_path / "concurrent.db")
    for i in range(200):
        db[f"k{i}"] = 0
    db.commit()

    snapshots = [db.snapshot() for _ in range(6)]
    errors = []
    sample = range(0, 200, 20)

    def reader(snap):
        try:
            for _ in range(40):
                assert all(snap[f"k{i}"] == 0 for i in sample)
        except Exception as exc:  # pragma: no cover - surfaced via errors list
            errors.append(exc)

    def writer():
        try:
            for round_no in range(1, 6):
                for i in range(200):
                    db[f"k{i}"] = round_no
                db.commit()
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=reader, args=(s,)) for s in snapshots]
    threads.append(threading.Thread(target=writer))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    # Every snapshot still reads the version it pinned, despite five commits.
    for snap in snapshots:
        assert all(snap[f"k{i}"] == 0 for i in sample)
        snap.close()
    assert db["k0"] == 5  # the writer's commits did land
    db.close()
