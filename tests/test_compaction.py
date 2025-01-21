import imustore


def test_compaction_preserves_live_values(tmp_path):
    path = tmp_path / "store.db"
    db = imustore.connect(path)
    for index in range(12):
        db["key"] = {"version": index}
        db[f"k{index}"] = index
        db.commit()

    before = db.stats().file_size
    compacted = db.compact()

    assert db["key"] == {"version": 11}
    assert db["k3"] == 3
    assert compacted.file_size <= before
    db.close()
