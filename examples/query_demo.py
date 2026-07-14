"""Secondary indexes, query planning, and TTL on a document collection.

    python examples/query_demo.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import imustore  # noqa: E402
from imustore.collection import Collection  # noqa: E402


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db = imustore.connect(Path(tmp) / "people.db")
        people = Collection(db, indexes=["team", "status"])

        people.set("u1", {"name": "Ada", "team": "core", "status": "active", "age": 36})
        people.set("u2", {"name": "Grace", "team": "core", "status": "active", "age": 45})
        people.set("u3", {"name": "Alan", "team": "ops", "status": "active", "age": 41})
        people.set("u4", {"name": "Edsger", "team": "core", "status": "inactive", "age": 60})
        people.set("session", {"name": "temp"}, ttl=0)  # already expired

        query = (
            people.query()
            .where("team", "core")
            .where("status", "active")
            .filter(lambda d: d["age"] > 40)
        )
        print("plan:", query.explain())
        print("active core people over 40:")
        for key, doc in query.all():
            print(f"  {key}: {doc['name']} (age {doc['age']})")

        print("\nfull count (expired 'session' excluded):", people.count())
        print("swept expired documents:", people.sweep())
        print("count after sweep:", people.count())

        db.close()


if __name__ == "__main__":
    main()
