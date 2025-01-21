import pytest

import imustore
from imustore.errors import DatabaseClosedError, KeyEncodingError


def test_set_get_delete_persist_across_connections(tmp_path):
    path = tmp_path / "store.db"
    db = imustore.connect(path)
    db["name"] = "Alice"
    db["count"] = 3
    db["meta"] = {"ok": True}
    db.commit()
    db.close()

    reopened = imustore.connect(path)
    assert reopened["name"] == "Alice"
    assert reopened["count"] == 3
    assert reopened["meta"] == {"ok": True}

    del reopened["name"]
    reopened.commit()
    with pytest.raises(KeyError):
        _ = reopened["name"]
    reopened.close()


def test_update_value_uses_default_and_returns_new_value(tmp_path):
    db = imustore.connect(tmp_path / "store.db")
    updated = db.update_value("visits", lambda value: value + 1, default=0)
    db.commit()

    assert updated == 1
    assert db["visits"] == 1
    db.close()


def test_invalid_key_and_closed_database_raise_clear_errors(tmp_path):
    db = imustore.connect(tmp_path / "store.db")
    with pytest.raises(KeyEncodingError):
        db[""] = "nope"
    db.close()
    with pytest.raises(DatabaseClosedError):
        len(db)
