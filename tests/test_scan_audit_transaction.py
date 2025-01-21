import pytest

import imustore


def test_prefix_and_range_scan_return_sorted_items(tmp_path):
    db = imustore.connect(tmp_path / "store.db")
    for key in ["user:003", "order:001", "user:001", "user:002", "z-last"]:
        db[key] = key.upper()
    db.commit()

    assert list(db.keys(prefix="user:")) == ["user:001", "user:002", "user:003"]
    assert list(db.scan(start="order:", stop="z")) == [
        ("order:001", "ORDER:001"),
        ("user:001", "USER:001"),
        ("user:002", "USER:002"),
        ("user:003", "USER:003"),
    ]
    db.close()


def test_audit_reports_valid_tree_metadata(tmp_path):
    db = imustore.connect(tmp_path / "store.db")
    for index in range(5):
        db[f"k{index}"] = {"index": index}
    db.commit()

    report = db.audit()
    assert report.ok
    assert report.keys == 5
    assert report.values == 5
    assert report.height >= 1
    db.close()


def test_transaction_commits_success_and_rolls_back_failure(tmp_path):
    db = imustore.connect(tmp_path / "store.db")
    with db.transaction() as tx:
        tx.set("counter", 1)
        tx.update("counter", lambda value: value + 1)

    assert db["counter"] == 2

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.set("counter", 99)
            raise RuntimeError("rollback")

    assert db["counter"] == 2
    db.close()
