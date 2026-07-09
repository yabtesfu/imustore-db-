"""Durability and crash-recovery tests.

Real power loss is simulated deterministically at the byte level: we drive the
engine to a known on-disk state, then reproduce exactly what a crash would leave
behind (orphaned tail records, a torn record, or a torn meta block) and assert
that reopening the database always recovers a consistent, committed state.
"""

import os
import random

import pytest

import imustore
from imustore.errors import DatabaseCorruptionError
from imustore.physical import (
    META_BODY_BYTES,
    META_CRC_BYTES,
    META_SLOT_SIZE,
    RECORD_HEADER_BYTES,
    Storage,
)


def _corrupt_slot_crc(path, slot):
    """Flip the checksum of a meta slot so it fails validation on read."""
    with open(path, "r+b") as fileobj:
        offset = slot * META_SLOT_SIZE + META_BODY_BYTES
        fileobj.seek(offset)
        crc = fileobj.read(META_CRC_BYTES)
        fileobj.seek(offset)
        fileobj.write(bytes(byte ^ 0xFF for byte in crc))


def test_records_carry_checksums_that_detect_bitrot(tmp_path):
    path = tmp_path / "crc.db"
    with path.open("w+b") as fileobj:
        storage = Storage(fileobj)
        address = storage.write(b"hello world")
        storage.commit_root_address(address)
        assert storage.read(address) == b"hello world"

        # Flip a byte inside the payload, mimicking bit-rot / a torn write.
        fileobj.seek(address + RECORD_HEADER_BYTES)
        original = fileobj.read(1)
        fileobj.seek(address + RECORD_HEADER_BYTES)
        fileobj.write(bytes([original[0] ^ 0xFF]))
        fileobj.flush()

        with pytest.raises(DatabaseCorruptionError):
            storage.read(address)
        storage.close()


def test_recovers_from_orphaned_append(tmp_path):
    path = tmp_path / "orphan.db"
    db = imustore.connect(path)
    db["a"] = 1
    db.commit()

    # Stage a change and append its records to disk, but "crash" before the
    # commit publishes a new meta block.
    db["b"] = 2
    db._tree._tree_ref.store(db._storage)
    db._storage.flush()
    db._storage._f.close()

    reopened = imustore.connect(path)
    assert reopened["a"] == 1
    assert "b" not in reopened
    assert reopened.stats().record_count >= 1  # the record table walks cleanly again
    assert reopened.audit().ok
    reopened["c"] = 3  # the file is writable after recovery
    reopened.commit()
    assert reopened["c"] == 3
    reopened.close()


def test_recovers_from_torn_tail_record(tmp_path):
    path = tmp_path / "torn.db"
    db = imustore.connect(path)
    db["a"] = 1
    db["b"] = 2
    db.commit()
    db.close()

    # A partially written trailing record: fewer bytes than a record header.
    with open(path, "r+b") as fileobj:
        fileobj.seek(0, os.SEEK_END)
        fileobj.write(b"\x00\x00\x05")

    reopened = imustore.connect(path)
    assert reopened["a"] == 1 and reopened["b"] == 2
    assert reopened.stats().record_count >= 1
    assert reopened.audit().ok
    reopened.close()


def test_torn_latest_meta_falls_back_to_previous_transaction(tmp_path):
    path = tmp_path / "meta.db"
    db = imustore.connect(path)
    db["a"] = 1
    db.commit()  # transaction 1
    db["b"] = 2
    db.commit()  # transaction 2
    db.close()

    peek = Storage(open(path, "r+b"))
    latest_slot, latest_txn = peek._meta.slot, peek._meta.txn_id
    peek.close()
    assert latest_txn == 2

    _corrupt_slot_crc(path, latest_slot)  # torn write of the newest commit's meta

    reopened = imustore.connect(path)
    assert reopened["a"] == 1
    assert "b" not in reopened  # rolled back to the last intact transaction
    assert reopened.stats().txn_id == 1
    assert reopened.audit().ok
    reopened.close()


def test_corrupting_older_meta_slot_keeps_latest(tmp_path):
    path = tmp_path / "older.db"
    db = imustore.connect(path)
    db["a"] = 1
    db.commit()
    db["b"] = 2
    db.commit()
    db.close()

    peek = Storage(open(path, "r+b"))
    older_slot = 1 - peek._meta.slot
    peek.close()

    _corrupt_slot_crc(path, older_slot)

    reopened = imustore.connect(path)
    assert reopened["a"] == 1 and reopened["b"] == 2  # newest commit still wins
    assert reopened.stats().txn_id == 2
    assert reopened.audit().ok
    reopened.close()


def test_durability_none_is_functional(tmp_path):
    path = tmp_path / "fast.db"
    db = imustore.connect(path, durability="none")
    for index in range(50):
        db[f"k{index:02d}"] = index
    db.commit()
    db.close()

    reopened = imustore.connect(path, durability="none")
    assert len(reopened) == 50
    assert reopened["k07"] == 7
    assert reopened.audit().ok
    reopened.close()


def test_unknown_durability_rejected(tmp_path):
    with pytest.raises(ValueError):
        imustore.connect(tmp_path / "bad.db", durability="sometimes")


def test_crash_fuzz_always_recovers_a_committed_state(tmp_path):
    rng = random.Random(7)
    for trial in range(25):
        path = tmp_path / f"fuzz{trial}.db"
        db = imustore.connect(path)

        committed = {}
        for _ in range(rng.randrange(1, 40)):
            key = f"k{rng.randrange(50):02d}"
            value = rng.randrange(1000)
            db[key] = value
            committed[key] = value
        db.commit()
        snapshot = dict(committed)

        # Stage more work and flush its records without publishing a meta block:
        # exactly the on-disk state a crash mid-commit produces.
        for _ in range(rng.randrange(1, 20)):
            db[f"k{rng.randrange(50):02d}"] = rng.randrange(1000)
        db._tree._tree_ref.store(db._storage)
        if rng.random() < 0.5:
            db._storage.flush()
        db._storage._f.close()

        reopened = imustore.connect(path)
        assert dict(reopened.items()) == snapshot, f"trial {trial}"
        assert len(reopened) == len(snapshot)
        report = reopened.audit()
        assert report.ok, report.errors
        reopened.close()
