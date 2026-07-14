"""A document collection with secondary indexes, a query engine, and TTL.

A :class:`Collection` stores JSON documents (dicts) in an ImmuStore database and
maintains **secondary indexes** so you can find documents by a field value
without scanning everything. Index entries live in the same database under a
reserved ``\\x00`` prefix, so they are updated *in the same transaction* as the
document -- the index can never drift out of sync with the data.

The query engine does real (if small) **query planning**: an equality predicate
on an indexed field narrows the candidate set with a B+ tree range scan over that
index (and several are intersected); the equality is then re-checked against the
fetched document, so the indexed and unindexed paths return identical results.
With no usable index it falls back to a full scan.

Keys can also carry a **TTL**: an expiry timestamp is stored alongside the
document (clock injected for testability), reads skip expired documents, and
:meth:`sweep` reclaims them.
"""

from __future__ import annotations

import json
import time

_RESERVED = "\x00"
_INDEX_PREFIX = "\x00idx\x00"
_EXPIRY_PREFIX = "\x00exp\x00"


def _value_token(value):
    """Canonical string for a value so equal values share one index slot.

    An int-valued float is normalized to an int (so 25 and 25.0 match, exactly as
    Python's ``==`` treats them). Equality everywhere -- index keys, index lookups,
    and residual filtering -- is defined as "same token", which keeps the indexed
    and full-scan paths consistent for cross-type values like 1 vs 1.0.
    """
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return json.dumps(value, sort_keys=True)


def _index_key(field, value, primary_key):
    return f"{_INDEX_PREFIX}{field}\x00{_value_token(value)}\x00{primary_key}"


def _index_scan_prefix(field, value):
    return f"{_INDEX_PREFIX}{field}\x00{_value_token(value)}\x00"


def _expiry_key(primary_key):
    return f"{_EXPIRY_PREFIX}{primary_key}"


def _check_key(key):
    if not isinstance(key, str) or not key:
        raise ValueError("collection keys must be non-empty strings")
    if key.startswith(_RESERVED):
        raise ValueError("collection keys must not start with the reserved \\x00 byte")


class Collection:
    def __init__(self, db, *, indexes=(), clock=time.time):
        self._db = db
        self._indexes = set(indexes)
        self._clock = clock

    @property
    def indexes(self):
        return frozenset(self._indexes)

    # -- reads --------------------------------------------------------------
    def get(self, key):
        _check_key(key)
        doc = self._db.get(key)
        if doc is None or self._is_expired(key):
            return None
        return doc

    def __contains__(self, key):
        return self.get(key) is not None

    def keys(self):
        for key in self._db.keys():
            if not key.startswith(_RESERVED) and not self._is_expired(key):
                yield key

    def documents(self):
        for key in self.keys():
            yield key, self._db[key]

    def count(self):
        return sum(1 for _ in self.keys())

    def _is_expired(self, key):
        expiry = self._db.get(_expiry_key(key))
        return expiry is not None and self._clock() >= expiry

    # -- writes -------------------------------------------------------------
    def set(self, key, document, *, ttl=None):
        _check_key(key)
        if not isinstance(document, dict):
            raise TypeError("collection documents must be dicts")
        with self._db.transaction() as tx:
            old = tx.get_or_none(key)
            self._unindex(tx, key, old)
            tx.set(key, document)
            for field in self._indexes:
                if field in document:
                    tx.set(_index_key(field, document[field], key), key)
            expiry_key = _expiry_key(key)
            if ttl is not None:
                tx.set(expiry_key, self._clock() + ttl)
            elif expiry_key in tx:
                tx.delete(expiry_key)

    def delete(self, key):
        _check_key(key)
        doc = self._db.get(key)
        if doc is None:
            raise KeyError(key)
        with self._db.transaction() as tx:
            self._unindex(tx, key, doc)
            tx.delete(key)
            expiry_key = _expiry_key(key)
            if expiry_key in tx:
                tx.delete(expiry_key)

    def _unindex(self, tx, key, document):
        if not isinstance(document, dict):
            return
        for field in self._indexes:
            if field in document:
                index_key = _index_key(field, document[field], key)
                if index_key in tx:
                    tx.delete(index_key)

    def create_index(self, field):
        """Add an index and backfill it from the documents already stored.

        The field is only marked as indexed once the backfill has committed, so a
        rolled-back backfill can never leave a declared-but-empty index that would
        silently return no results.
        """
        with self._db.transaction() as tx:
            for key in list(self.keys()):
                document = self._db[key]
                if isinstance(document, dict) and field in document:
                    tx.set(_index_key(field, document[field], key), key)
        self._indexes.add(field)

    def sweep(self):
        """Delete every document whose TTL has elapsed; returns the count."""
        now = self._clock()
        expired = [
            expiry_key[len(_EXPIRY_PREFIX):]
            for expiry_key, expiry in self._db.scan(prefix=_EXPIRY_PREFIX)
            if now >= expiry
        ]
        for key in expired:
            try:
                self.delete(key)
            except KeyError:
                pass
        return len(expired)

    # -- queries ------------------------------------------------------------
    def query(self):
        return Query(self)


class Query:
    def __init__(self, collection: Collection):
        self._collection = collection
        self._equals = []
        self._predicates = []

    def where(self, field, value) -> "Query":
        """Match documents where ``field == value`` (uses an index if one exists)."""
        self._equals.append((field, value))
        return self

    def filter(self, predicate) -> "Query":
        """Match documents for which ``predicate(document)`` is true (scan-only)."""
        self._predicates.append(predicate)
        return self

    def explain(self) -> dict:
        """Report how the query will run -- useful for showing the planner works."""
        indexed = [f for f, _ in self._equals if f in self._collection._indexes]
        return {
            "plan": "index" if indexed else "full_scan",
            "indexed_fields": indexed,
            "equals": [f for f, _ in self._equals],
            "predicates": len(self._predicates),
        }

    def _candidate_keys(self):
        """Narrow to candidate keys via indexes, or None to signal a full scan."""
        collection = self._collection
        indexed = [(f, v) for f, v in self._equals if f in collection._indexes]
        if not indexed:
            return None
        key_sets = [
            {pk for _, pk in collection._db.scan(prefix=_index_scan_prefix(field, value))}
            for field, value in indexed
        ]
        return set.intersection(*key_sets)

    def _matches(self, document):
        # Every equality is re-checked here (via canonical tokens) for both the
        # index and full-scan paths, so an index is a pure optimization that can
        # only narrow candidates -- never change the result.
        for field, value in self._equals:
            if _value_token(document.get(field)) != _value_token(value):
                return False
        return all(predicate(document) for predicate in self._predicates)

    def all(self):
        collection = self._collection
        candidates = self._candidate_keys()
        if candidates is None:
            items = ((k, collection._db[k]) for k in collection.keys())
        else:
            items = ((k, collection._db.get(k)) for k in sorted(candidates))

        results = []
        for key, document in items:
            if document is None or collection._is_expired(key):
                continue
            if self._matches(document):
                results.append((key, document))
        return results

    def __iter__(self):
        return iter(self.all())
