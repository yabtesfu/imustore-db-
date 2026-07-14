"""JSON wire encoding for Raft envelopes.

Messages cross the network as length-prefixed JSON rather than pickle, so the
transport never executes arbitrary deserialized objects -- a small, explicit
schema instead of an implicit one.
"""

from __future__ import annotations

import json

from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Envelope,
    LogEntry,
    RequestVote,
    RequestVoteReply,
)


def _body_to_dict(body) -> dict:
    if isinstance(body, RequestVote):
        return {
            "term": body.term,
            "candidate_id": body.candidate_id,
            "last_log_index": body.last_log_index,
            "last_log_term": body.last_log_term,
        }
    if isinstance(body, RequestVoteReply):
        return {"term": body.term, "vote_granted": body.vote_granted}
    if isinstance(body, AppendEntries):
        return {
            "term": body.term,
            "leader_id": body.leader_id,
            "prev_log_index": body.prev_log_index,
            "prev_log_term": body.prev_log_term,
            "entries": [[e.term, e.command] for e in body.entries],
            "leader_commit": body.leader_commit,
        }
    if isinstance(body, AppendEntriesReply):
        return {"term": body.term, "success": body.success, "match_index": body.match_index}
    raise TypeError(f"cannot encode {type(body).__name__}")


def _dict_to_body(kind: str, data: dict):
    if kind == "RequestVote":
        return RequestVote(data["term"], data["candidate_id"], data["last_log_index"], data["last_log_term"])
    if kind == "RequestVoteReply":
        return RequestVoteReply(data["term"], data["vote_granted"])
    if kind == "AppendEntries":
        entries = tuple(LogEntry(term, command) for term, command in data["entries"])
        return AppendEntries(
            data["term"], data["leader_id"], data["prev_log_index"], data["prev_log_term"],
            entries, data["leader_commit"],
        )
    if kind == "AppendEntriesReply":
        return AppendEntriesReply(data["term"], data["success"], data["match_index"])
    raise TypeError(f"cannot decode {kind!r}")


def encode(envelope: Envelope) -> bytes:
    return json.dumps({
        "src": envelope.src,
        "dst": envelope.dst,
        "kind": type(envelope.body).__name__,
        "body": _body_to_dict(envelope.body),
    }).encode("utf-8")


def decode(raw: bytes) -> Envelope:
    data = json.loads(raw.decode("utf-8"))
    return Envelope(data["src"], data["dst"], _dict_to_body(data["kind"], data["body"]))
