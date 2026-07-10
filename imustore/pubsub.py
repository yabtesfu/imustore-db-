"""Change-data-capture broker for the real-time changefeed.

A subscriber registers interest in a key *prefix* and gets an ``asyncio.Queue``
onto which every matching write is pushed as it commits. This turns the database
into a live stream: ``SUBSCRIBE user:`` and you receive a message the instant any
``user:*`` key is set or deleted.

The broker is intentionally decoupled from the storage engine -- the server is
the single gateway for mutations, so it simply calls :meth:`publish` after each
committed write. Queues are bounded, so a subscriber that cannot keep up sheds
messages (and increments a drop counter) instead of growing memory without bound.
"""

from __future__ import annotations

import asyncio
import json

DEFAULT_QUEUE_MAXSIZE = 1024


class ChangeBroker:
    def __init__(self, queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE):
        self._subscribers: dict[str, set[asyncio.Queue]] = {}
        self._queue_maxsize = queue_maxsize
        self.dropped = 0

    def new_queue(self) -> asyncio.Queue:
        return asyncio.Queue(maxsize=self._queue_maxsize)

    def subscribe(self, prefix: str, queue: asyncio.Queue) -> None:
        self._subscribers.setdefault(prefix, set()).add(queue)

    def unsubscribe(self, prefix: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(prefix)
        if subscribers:
            subscribers.discard(queue)
            if not subscribers:
                del self._subscribers[prefix]

    def unsubscribe_all(self, queue: asyncio.Queue) -> None:
        for prefix in list(self._subscribers):
            self.unsubscribe(prefix, queue)

    @property
    def prefixes(self):
        return set(self._subscribers)

    def publish(self, key: str, op: str, value: str | None) -> None:
        """Fan a committed change out to every subscriber whose prefix matches."""
        if not self._subscribers:
            return
        for prefix, queues in self._subscribers.items():
            if not key.startswith(prefix):
                continue
            payload = json.dumps({"key": key, "op": op, "value": value}, separators=(",", ":"))
            message = ["message", prefix, payload]
            for queue in queues:
                try:
                    queue.put_nowait(message)
                except asyncio.QueueFull:
                    self.dropped += 1  # slow subscriber: shed rather than grow unbounded
