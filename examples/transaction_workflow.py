from __future__ import annotations

import imustore


def main() -> None:
    with imustore.connect("orders.db") as db:
        with db.transaction() as tx:
            tx.set("order:1001", {"status": "created", "total": 42.5})
            tx.update("metrics:orders", lambda value: value + 1, default=0)

        for key, value in db.scan(prefix="order:"):
            print(key, value)

        print(db.audit().as_dict())


if __name__ == "__main__":
    main()
