"""Demonstrates MVCC snapshots and optimistic transactions.

    python examples/mvcc_snapshot.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import imustore  # noqa: E402
from imustore.errors import ConflictError  # noqa: E402


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db = imustore.connect(Path(tmp) / "mvcc.db")
        db["price"] = 100
        db.commit()

        print("== Snapshot isolation ==")
        snap = db.snapshot()  # pins the current version (price == 100)
        for new_price in (110, 125, 130):
            db["price"] = new_price
            db.commit()
        print(f"snapshot still sees price = {snap['price']}")  # 100, frozen
        print(f"live database now sees price = {db['price']}")  # 130
        snap.close()

        print("\n== Optimistic transactions with conflict detection ==")
        db["stock"] = 5
        db.commit()

        buyer = db.begin()
        admin = db.begin()  # both read stock == 5
        buyer.update("stock", lambda n: n - 1)
        admin.update("stock", lambda n: n + 100)  # restock

        buyer.commit()
        print(f"buyer committed: stock = {db['stock']}")  # 4
        try:
            admin.commit()  # stock changed since admin began
        except ConflictError as exc:
            print(f"admin conflict detected: {exc}")
            retry = db.begin()  # retry from the fresh version (stock == 4)
            retry.update("stock", lambda n: n + 100)
            retry.commit()
            print(f"admin retried: stock = {db['stock']}")  # 104

        db.close()


if __name__ == "__main__":
    main()
