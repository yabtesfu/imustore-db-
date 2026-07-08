import random

import pytest

import imustore
from imustore.bplus_tree import BPlusTree, InternalNode
from imustore.physical import Storage


def _tree(tmp_path, name="bplus.db", order=4):
    fileobj = (tmp_path / name).open("w+b")
    return BPlusTree(Storage(fileobj), order=order), fileobj


def test_rejects_tiny_order(tmp_path):
    fileobj = (tmp_path / "bad.db").open("w+b")
    with pytest.raises(ValueError):
        BPlusTree(Storage(fileobj), order=2)
    fileobj.close()


def test_inserts_stay_sorted_and_readable_after_splits(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    keys = [f"k{n:03d}" for n in range(200)]
    random.Random(1).shuffle(keys)
    for key in keys:
        tree.set(key, key.encode())
    tree.commit()

    assert [k for k, _ in tree.items()] == sorted(keys)
    assert tree.length() == len(keys)
    for key in keys:
        assert tree.get(key) == key.encode()
    fileobj.close()


def test_sequential_insert_stays_balanced_unlike_a_bst(tmp_path):
    # A naive BST degrades to height == n on sorted input; a B+ tree must not.
    tree, fileobj = _tree(tmp_path, order=8)
    for n in range(500):
        tree.set(f"k{n:04d}", b"v")
    tree.commit()

    assert tree.length() == 500
    assert tree.height() <= 5  # log_5(500) ~= 4; comfortably shallow
    assert tree.audit().ok
    fileobj.close()


def test_overwrite_does_not_change_count(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    for _ in range(10):
        tree.set("same", b"value")
    tree.commit()

    assert tree.length() == 1
    assert tree.get("same") == b"value"
    assert tree.audit().ok
    fileobj.close()


def test_delete_triggers_borrow_and_merge(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    keys = [f"k{n:02d}" for n in range(40)]
    for key in keys:
        tree.set(key, key.encode())
    tree.commit()

    # Delete over half the keys; this forces underflow, borrows, and merges.
    for key in keys[::2]:
        tree.delete(key)
    tree.commit()

    survivors = keys[1::2]
    assert [k for k, _ in tree.items()] == survivors
    assert tree.length() == len(survivors)
    report = tree.audit()
    assert report.ok, report.errors
    for key in survivors:
        assert tree.get(key) == key.encode()
    fileobj.close()


def test_delete_missing_key_raises_and_leaves_tree_intact(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    for key in ["a", "b", "c"]:
        tree.set(key, key.encode())
    with pytest.raises(KeyError):
        tree.delete("zzz")
    assert tree.length() == 3
    assert [k for k, _ in tree.items()] == ["a", "b", "c"]
    fileobj.close()


def test_emptying_tree_returns_to_empty_root(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    for key in ["a", "b", "c", "d", "e"]:
        tree.set(key, key.encode())
    for key in ["a", "b", "c", "d", "e"]:
        tree.delete(key)
    tree.commit()

    assert tree.length() == 0
    assert list(tree.items()) == []
    assert tree.audit().ok
    with pytest.raises(KeyError):
        tree.get("a")
    fileobj.close()


def test_range_scan_prunes_and_respects_bounds(tmp_path):
    tree, fileobj = _tree(tmp_path, order=4)
    for n in range(100):
        tree.set(f"k{n:03d}", str(n).encode())
    tree.commit()

    got = [k for k, _ in tree.range_items("k010", "k020")]
    assert got == [f"k{n:03d}" for n in range(10, 20)]  # stop is exclusive
    assert [k for k, _ in tree.range_items(None, "k003")] == ["k000", "k001", "k002"]
    assert [k for k, _ in tree.range_items("k097", None)] == ["k097", "k098", "k099"]
    fileobj.close()


def test_tree_persists_across_reconnect(tmp_path):
    path = tmp_path / "persist.db"
    db = imustore.connect(path)
    for n in range(300):
        db[f"user:{n:04d}"] = {"n": n}
    db.commit()
    db.close()

    reopened = imustore.connect(path)
    assert len(reopened) == 300
    assert reopened["user:0150"] == {"n": 150}
    assert list(reopened.keys(prefix="user:0299")) == ["user:0299"]
    assert reopened.audit().ok
    reopened.close()


def test_default_engine_is_bplus_and_root_is_shallow(tmp_path):
    db = imustore.connect(tmp_path / "engine.db")
    assert isinstance(db._tree, BPlusTree)
    for n in range(1000):
        db[f"k{n:05d}"] = n
    db.commit()
    root = db._tree._follow(db._tree._tree_ref)
    assert isinstance(root, InternalNode)  # grew past a single leaf
    assert db.audit().height <= 5
    db.close()


def test_binary_index_still_selectable(tmp_path):
    db = imustore.connect(tmp_path / "legacy.db", index="binary")
    db["x"] = 1
    db.commit()
    assert db["x"] == 1
    db.close()


def test_fuzz_matches_reference_dict(tmp_path):
    tree, fileobj = _tree(tmp_path, order=5)
    reference = {}
    rng = random.Random(42)
    for step in range(4000):
        key = f"k{rng.randrange(150):03d}"
        if rng.random() < 0.65:
            value = str(rng.randrange(10_000)).encode()
            tree.set(key, value)
            reference[key] = value
        elif key in reference:
            tree.delete(key)
            del reference[key]
        if step % 500 == 0:
            tree.commit()
            report = tree.audit()
            assert report.ok, report.errors

    tree.commit()
    assert tree.length() == len(reference)
    assert dict(tree.items()) == reference
    assert tree.audit().ok
    fileobj.close()
