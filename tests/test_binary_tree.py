from imustore.binary_tree import BinaryTree
from imustore.physical import Storage


def test_binary_tree_keeps_keys_sorted(tmp_path):
    with (tmp_path / "tree.db").open("w+b") as fileobj:
        storage = Storage(fileobj)
        tree = BinaryTree(storage)
        for key in ["delta", "alpha", "charlie", "bravo"]:
            tree.set(key, key.encode())
        tree.commit()

        assert [key for key, _ in tree.items()] == ["alpha", "bravo", "charlie", "delta"]
        assert tree.get("charlie") == b"charlie"
        storage.close()


def test_binary_tree_delete_rebuilds_path_without_losing_children(tmp_path):
    with (tmp_path / "tree.db").open("w+b") as fileobj:
        storage = Storage(fileobj)
        tree = BinaryTree(storage)
        for key in ["m", "f", "t", "a", "h", "p", "z"]:
            tree.set(key, key.encode())
        tree.delete("m")
        tree.commit()

        assert [key for key, _ in tree.items()] == ["a", "f", "h", "p", "t", "z"]
        storage.close()
