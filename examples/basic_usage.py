from __future__ import annotations

import imustore


with imustore.connect("example.db") as db:
    db["project"] = {"name": "ImmuStore DB", "kind": "append-only"}
    db["language"] = "Python"
    db.commit()

    print(db["project"])
    print(list(db.items()))
