"""A deterministic Raft consensus node.

The node performs **no I/O and reads no clock**. It is driven entirely by two
methods -- :meth:`tick` (one logical time step) and :meth:`step` (one delivered
message) -- and it returns the messages it wants sent as plain data. All
randomness (election timeouts) comes from an injected RNG. This determinism is
what lets the same code run under a simulated network with fault injection *and*
over a real socket transport, and it is why the tests are reproducible rather
than timing-dependent.

Implements the core of the Raft paper: leader election with the up-to-date
voting restriction, log replication with the log-matching / conflict-truncation
rules, and the current-term commit rule.
"""

from __future__ import annotations

from dataclasses import dataclass

from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Envelope,
    LogEntry,
    RequestVote,
    RequestVoteReply,
)

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class NotLeaderError(Exception):
    def __init__(self, leader_id):
        super().__init__(f"not the leader; current leader is {leader_id!r}")
        self.leader_id = leader_id


@dataclass
class RaftConfig:
    election_timeout_min: int = 10
    election_timeout_max: int = 20
    heartbeat_interval: int = 3


class RaftNode:
    def __init__(self, node_id, peers, apply, log, rng, config=None, last_applied=0):
        self.node_id = node_id
        self.peers = list(peers)
        self.cluster_size = len(self.peers) + 1
        self._apply_fn = apply
        self.log = log
        self._rng = rng
        self.config = config or RaftConfig()

        # Persistent state (restored from the log's saved term/vote).
        self.current_term = log.current_term
        self.voted_for = log.voted_for

        # Volatile state.
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = last_applied
        self.votes = set()

        # Leader-only volatile state.
        self.next_index = {}
        self.match_index = {}

        # Timers (in logical ticks).
        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.election_timeout = 0
        self._reset_election_timer()

        self._outbox = []

    # -- public driving API -------------------------------------------------
    def tick(self):
        self._outbox = []
        if self.role == LEADER:
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self.config.heartbeat_interval:
                self.heartbeat_elapsed = 0
                self._broadcast_append()
        else:
            self.election_elapsed += 1
            if self.election_elapsed >= self.election_timeout:
                self._become_candidate()
        return self._drain()

    def step(self, envelope):
        self._outbox = []
        body, src = envelope.body, envelope.src
        if isinstance(body, RequestVote):
            self._handle_request_vote(src, body)
        elif isinstance(body, RequestVoteReply):
            self._handle_request_vote_reply(src, body)
        elif isinstance(body, AppendEntries):
            self._handle_append_entries(src, body)
        elif isinstance(body, AppendEntriesReply):
            self._handle_append_entries_reply(src, body)
        return self._drain()

    def propose(self, command):
        """Append a client command (leader only) and start replicating it."""
        if self.role != LEADER:
            raise NotLeaderError(self.leader_id)
        self._outbox = []
        index = self.log.append_new(LogEntry(self.current_term, command))
        self._persist()
        self._broadcast_append()
        self._maybe_advance_commit()  # single-node clusters commit immediately
        return index, self._drain()

    @property
    def is_leader(self) -> bool:
        return self.role == LEADER

    # -- elections ----------------------------------------------------------
    def _become_candidate(self):
        self.role = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self.votes = {self.node_id}
        self._reset_election_timer()
        self._persist()
        if self._has_majority(len(self.votes)):
            self._become_leader()
            return
        for peer in self.peers:
            self._send(peer, RequestVote(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=self.log.last_index(),
                last_log_term=self.log.last_term(),
            ))

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: self.log.last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.heartbeat_elapsed = 0
        # A no-op entry in the new term lets the leader commit entries carried
        # over from previous terms (Raft's current-term commit rule).
        self.log.append_new(LogEntry(self.current_term, {"op": "noop"}))
        self._persist()
        self._broadcast_append()
        self._maybe_advance_commit()

    def _become_follower(self, term):
        self.role = FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.leader_id = None
        self._reset_election_timer()
        self._persist()

    def _handle_request_vote(self, src, rv):
        if rv.term < self.current_term:
            self._send(src, RequestVoteReply(self.current_term, False))
            return
        if rv.term > self.current_term:
            self._become_follower(rv.term)

        log_ok = (rv.last_log_term > self.log.last_term()) or (
            rv.last_log_term == self.log.last_term()
            and rv.last_log_index >= self.log.last_index()
        )
        if self.voted_for in (None, rv.candidate_id) and log_ok:
            self.voted_for = rv.candidate_id
            self._reset_election_timer()
            self._persist()
            self._send(src, RequestVoteReply(self.current_term, True))
        else:
            self._send(src, RequestVoteReply(self.current_term, False))

    def _handle_request_vote_reply(self, src, reply):
        if reply.term > self.current_term:
            self._become_follower(reply.term)
            return
        if self.role != CANDIDATE or reply.term < self.current_term:
            return
        if reply.vote_granted:
            self.votes.add(src)
            if self._has_majority(len(self.votes)):
                self._become_leader()

    # -- log replication ----------------------------------------------------
    def _handle_append_entries(self, src, ae):
        if ae.term < self.current_term:
            self._send(src, AppendEntriesReply(self.current_term, False, self.log.last_index()))
            return
        if ae.term > self.current_term:
            self._become_follower(ae.term)
        # A valid leader for this term: (re)become follower and defer to it.
        self.role = FOLLOWER
        self.leader_id = ae.leader_id
        self._reset_election_timer()

        if ae.prev_log_index > self.log.last_index() or (
            self.log.term_at(ae.prev_log_index) != ae.prev_log_term
        ):
            self._send(src, AppendEntriesReply(self.current_term, False, self.log.last_index()))
            return

        self.log.append_from(ae.prev_log_index, ae.entries)
        new_last = ae.prev_log_index + len(ae.entries)
        if ae.leader_commit > self.commit_index:
            self.commit_index = min(ae.leader_commit, new_last)
            self._apply()
        self._persist()
        self._send(src, AppendEntriesReply(self.current_term, True, new_last))

    def _handle_append_entries_reply(self, src, reply):
        if reply.term > self.current_term:
            self._become_follower(reply.term)
            return
        if self.role != LEADER or reply.term < self.current_term:
            return
        if reply.success:
            self.match_index[src] = reply.match_index
            self.next_index[src] = reply.match_index + 1
            self._maybe_advance_commit()
        else:
            # Roll nextIndex back toward the follower's reported last index.
            self.next_index[src] = max(1, min(self.next_index[src] - 1, reply.match_index + 1))
            self._send_append_to(src)

    def _broadcast_append(self):
        for peer in self.peers:
            self._send_append_to(peer)

    def _send_append_to(self, peer):
        prev_index = self.next_index[peer] - 1
        self._send(peer, AppendEntries(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_index,
            prev_log_term=self.log.term_at(prev_index),
            entries=self.log.entries_from(prev_index + 1),
            leader_commit=self.commit_index,
        ))

    def _maybe_advance_commit(self):
        for index in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(index) != self.current_term:
                continue  # never commit a prior-term entry by counting replicas
            replicas = 1 + sum(1 for peer in self.peers if self.match_index.get(peer, 0) >= index)
            if self._has_majority(replicas):
                self.commit_index = index
                self._apply()
                return

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            self._apply_fn(self.last_applied, entry.command)

    # -- helpers ------------------------------------------------------------
    def _has_majority(self, count) -> bool:
        return count >= (self.cluster_size // 2) + 1

    def _reset_election_timer(self):
        self.election_elapsed = 0
        self.election_timeout = self._rng.randint(
            self.config.election_timeout_min, self.config.election_timeout_max
        )

    def _persist(self):
        self.log.save(self.current_term, self.voted_for)

    def _send(self, dst, body):
        self._outbox.append(Envelope(self.node_id, dst, body))

    def _drain(self):
        out, self._outbox = self._outbox, []
        return out
