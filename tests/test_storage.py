from imustore.physical import SUPERBLOCK_SIZE, Storage


def test_storage_writes_records_after_superblock(tmp_path):
    path = tmp_path / "store.db"
    with path.open("w+b") as fileobj:
        storage = Storage(fileobj)
        address = storage.write(b"hello")
        storage.commit_root_address(address)

        assert address >= SUPERBLOCK_SIZE
        assert storage.get_root_address() == address
        assert storage.read(address) == b"hello"
        assert storage.stats().record_count == 1
        storage.close()
