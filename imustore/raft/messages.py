"""Raft RPC message types.

These mirror the RPCs in the Raft paper (Ongaro & Ousterhout, 2014). Every
message is wrapped in an :class:`Envelope` carrying its source and destination so
a transport (real or simulated) can route it and route replies back.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: dict  # {"op": "set"|"del"|"noop", ...}


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: Any
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: Any
    prev_log_index: int
    prev_log_term: int
    entries: tuple = field(default_factory=tuple)  # tuple[LogEntry]
    leader_commit: int = 0


@dataclass(frozen=True)
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int  # follower's last log index, used to advance/back off nextIndex


@dataclass(frozen=True)
class Envelope:
    src: Any
    dst: Any
    body: Any
