"""Replicated state machines that consume committed Raft log entries.

A command is a dict: ``{"op": "set", "key": k, "value": v}``, ``{"op": "del",
"key": k}``, or ``{"op": "noop"}`` (leader placeholders). Raft guarantees every
node applies the same commands in the same order, so every state machine ends up
identical.

:class:`KVStateMachine` applies onto a real ImmuStore database. It records the
last applied log index *inside the same commit* as the data, so the apply step is
crash-safe and exactly-once: after a restart it resumes from the stored index
instead of replaying committed entries it already applied.

:class:`Recorder` is an in-memory state machine used by the simulator and tests.
"""

from __future__ import annotations

RAFT_APPLIED_KEY = "__raft_applied_index__"


class KVStateMachine:
    def __init__(self, db):
        self._db = db

    @property
    def applied_index(self) -> int:
        try:
            return int(self._db[RAFT_APPLIED_KEY])
        except KeyError:
            return 0

    def apply(self, index, command) -> None:
        op = command["op"]
        if op == "set":
            self._db[command["key"]] = command["value"]
        elif op == "del":
            try:
                del self._db[command["key"]]
            except KeyError:
                pass
        # The applied index rides in the same commit as the data, so the two can
        # never disagree after a crash.
        self._db[RAFT_APPLIED_KEY] = index
        self._db.commit()


class Recorder:
    def __init__(self):
        self.applied = []  # list of (index, command)
        self.applied_index = 0

    def apply(self, index, command) -> None:
        self.applied.append((index, command))
        self.applied_index = index

    def kv(self) -> dict:
        """Materialize the key-value state implied by the applied commands."""
        state = {}
        for _index, command in self.applied:
            if command["op"] == "set":
                state[command["key"]] = command["value"]
            elif command["op"] == "del":
                state.pop(command["key"], None)
        return state
