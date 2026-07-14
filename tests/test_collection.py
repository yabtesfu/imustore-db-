"""Tests for the document collection: secondary indexes, queries, and TTL."""

import pytest

import imustore
from imustore.collection import Collection


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def _users(tmp_path, **kwargs):
    db = imustore.connect(tmp_path / "coll.db")
    return Collection(db, **kwargs), db


def test_get_set_delete_and_count(tmp_path):
    users, db = _users(tmp_path)
    users.set("u1", {"name": "Ada", "status": "active"})
    users.set("u2", {"name": "Grace", "status": "inactive"})
    assert users.get("u1") == {"name": "Ada", "status": "active"}
    assert users.count() == 2
    assert set(users.keys()) == {"u1", "u2"}
    users.delete("u1")
    assert users.get("u1") is None
    assert users.count() == 1
    db.close()


def test_query_uses_index_when_available(tmp_path):
    users, db = _users(tmp_path, indexes=["status"])
    users.set("u1", {"name": "Ada", "status": "active"})
    users.set("u2", {"name": "Grace", "status": "active"})
    users.set("u3", {"name": "Alan", "status": "inactive"})

    query = users.query().where("status", "active")
    assert query.explain()["plan"] == "index"
    assert sorted(k for k, _ in query.all()) == ["u1", "u2"]
    db.close()


def test_query_falls_back_to_scan_without_index(tmp_path):
    users, db = _users(tmp_path)  # no indexes
    users.set("u1", {"name": "Ada", "age": 30})
    users.set("u2", {"name": "Grace", "age": 40})

    query = users.query().where("age", 40)
    assert query.explain()["plan"] == "full_scan"
    assert [k for k, _ in query.all()] == ["u2"]
    db.close()


def test_query_intersects_indexes_and_applies_residual_filter(tmp_path):
    users, db = _users(tmp_path, indexes=["status", "team"])
    users.set("u1", {"status": "active", "team": "core", "age": 25})
    users.set("u2", {"status": "active", "team": "core", "age": 41})
    users.set("u3", {"status": "active", "team": "ops", "age": 50})
    users.set("u4", {"status": "inactive", "team": "core", "age": 60})

    result = (
        users.query()
        .where("status", "active")
        .where("team", "core")
        .filter(lambda d: d["age"] > 30)
        .all()
    )
    assert [k for k, _ in result] == ["u2"]
    db.close()


def test_index_is_maintained_on_update_and_delete(tmp_path):
    users, db = _users(tmp_path, indexes=["status"])
    users.set("u1", {"status": "active"})
    assert [k for k, _ in users.query().where("status", "active").all()] == ["u1"]

    users.set("u1", {"status": "inactive"})  # value changed -> old index entry removed
    assert users.query().where("status", "active").all() == []
    assert [k for k, _ in users.query().where("status", "inactive").all()] == ["u1"]

    users.delete("u1")
    assert users.query().where("status", "inactive").all() == []
    db.close()


def test_create_index_backfills_existing_documents(tmp_path):
    users, db = _users(tmp_path)
    users.set("u1", {"status": "active"})
    users.set("u2", {"status": "active"})
    users.set("u3", {"status": "inactive"})

    users.create_index("status")
    query = users.query().where("status", "active")
    assert query.explain()["plan"] == "index"
    assert sorted(k for k, _ in query.all()) == ["u1", "u2"]
    db.close()


def test_ttl_expires_lazily_and_via_sweep(tmp_path):
    clock = FakeClock(1000.0)
    users, db = _users(tmp_path, indexes=["status"], clock=clock)
    users.set("session1", {"status": "active"}, ttl=60)
    users.set("permanent", {"status": "active"})

    assert users.get("session1") == {"status": "active"}
    clock.now = 1061.0  # 61s later: session1 has expired
    assert users.get("session1") is None  # lazily hidden
    assert users.count() == 1  # only the permanent doc
    assert [k for k, _ in users.query().where("status", "active").all()] == ["permanent"]

    removed = users.sweep()
    assert removed == 1
    # the expired doc and its index entry are physically gone now
    assert "session1" not in db
    db.close()


def test_reserved_index_keys_are_hidden_from_the_collection(tmp_path):
    users, db = _users(tmp_path, indexes=["status"])
    users.set("u1", {"status": "active"})
    # The underlying db holds internal index keys, but the collection hides them.
    assert any(k.startswith("\x00") for k in db.keys())
    assert list(users.keys()) == ["u1"]
    db.close()


def test_index_and_full_scan_agree_on_cross_type_equality(tmp_path):
    # Querying 25.0 must match a stored int 25 identically whether or not the
    # field is indexed (the index is a transparent optimization, not a filter).
    indexed_db = imustore.connect(tmp_path / "indexed.db")
    plain_db = imustore.connect(tmp_path / "plain.db")
    indexed = Collection(indexed_db, indexes=["age"])
    plain = Collection(plain_db)
    for coll in (indexed, plain):
        coll.set("u1", {"age": 25})

    assert indexed.query().where("age", 25.0).explain()["plan"] == "index"
    assert [k for k, _ in indexed.query().where("age", 25.0).all()] == ["u1"]
    assert [k for k, _ in plain.query().where("age", 25.0).all()] == ["u1"]
    assert indexed.query().where("age", 26).all() == []
    assert plain.query().where("age", 26).all() == []
    indexed_db.close()
    plain_db.close()


def test_reserved_and_empty_keys_are_rejected(tmp_path):
    users, db = _users(tmp_path, indexes=["status"])
    users.set("u1", {"status": "active"}, ttl=100)
    with pytest.raises(ValueError):
        users.set("\x00idx\x00hack", {"x": 1})  # cannot corrupt the internal namespace
    with pytest.raises(ValueError):
        users.get("\x00exp\x00u1")  # cannot read an internal expiry record either
    with pytest.raises(ValueError):
        users.get("")
    db.close()


class _FailingTransaction:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        raise RuntimeError("simulated commit conflict")

    def set(self, *args, **kwargs):
        pass

    def get_or_none(self, *args, **kwargs):
        return None

    def __contains__(self, key):
        return False


def test_create_index_does_not_declare_field_when_backfill_fails(tmp_path):
    users, db = _users(tmp_path)
    users.set("u1", {"email": "a@b.com"})
    db.transaction = lambda: _FailingTransaction()  # force the backfill to abort
    with pytest.raises(RuntimeError):
        users.create_index("email")
    assert "email" not in users.indexes  # not left declared with an empty index
    db.close()
