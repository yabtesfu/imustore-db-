"""The Raft persistent state: the replicated log plus ``currentTerm`` / ``votedFor``.

Raft requires these three to survive a crash, because safety depends on a server
never forgetting a vote it granted or a log entry it acknowledged. The log is
1-indexed to match the paper (index 0 is the empty "before the first entry"
position). Entries are held in memory; when a path is supplied they are also
persisted to disk (rewritten atomically via a temp file + ``os.replace``, with an
fsync) so a restarted node recovers exactly what it had promised.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .messages import LogEntry


class RaftLog:
    def __init__(self, path=None):
        self._path = Path(path) if path is not None else None
        self.entries: list[LogEntry] = []
        self.current_term = 0
        self.voted_for = None
        if self._path is not None and self._path.exists():
            self._load()

    # -- read helpers -------------------------------------------------------
    def last_index(self) -> int:
        return len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else 0

    def term_at(self, index: int) -> int:
        if index <= 0:
            return 0
        if index > len(self.entries):
            return -1  # no such entry; forces an AppendEntries consistency failure
        return self.entries[index - 1].term

    def get(self, index: int) -> LogEntry:
        return self.entries[index - 1]

    def entries_from(self, index: int) -> tuple:
        return tuple(self.entries[max(index - 1, 0):])

    # -- mutation -----------------------------------------------------------
    def append_new(self, entry: LogEntry) -> int:
        self.entries.append(entry)
        return len(self.entries)

    def append_from(self, prev_index: int, new_entries) -> None:
        """Splice ``new_entries`` in after ``prev_index``, truncating conflicts.

        Matching prefixes are left untouched (idempotent retries); the first
        entry whose term differs from ours truncates everything after it.
        """
        index = prev_index
        for entry in new_entries:
            index += 1
            if index <= len(self.entries):
                if self.entries[index - 1].term != entry.term:
                    del self.entries[index - 1:]
                    self.entries.append(entry)
            else:
                self.entries.append(entry)

    # -- persistence --------------------------------------------------------
    def save(self, current_term: int, voted_for) -> None:
        self.current_term = current_term
        self.voted_for = voted_for
        if self._path is None:
            return
        payload = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "entries": [{"term": e.term, "command": e.command} for e in self.entries],
        }
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fileobj:
            json.dump(payload, fileobj)
            fileobj.flush()
            os.fsync(fileobj.fileno())
        os.replace(tmp, self._path)

    def _load(self) -> None:
        data = json.loads(self._path.read_text(encoding="utf-8"))
        self.current_term = data["current_term"]
        self.voted_for = data["voted_for"]
        self.entries = [LogEntry(term=e["term"], command=e["command"]) for e in data["entries"]]
