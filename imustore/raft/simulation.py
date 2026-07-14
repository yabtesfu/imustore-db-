"""A deterministic, in-memory Raft cluster for testing and demos.

Time is a logical tick counter and the "network" is a list of in-flight
envelopes, so a whole cluster -- elections, replication, failures -- runs
reproducibly with no threads, sockets, or wall clock. Faults are first-class:
crash a node, restart it (its persisted log survives), partition the cluster into
groups, or heal it. This is the harness that proves Raft's safety and liveness
properties, and the foundation for Phase 7's simulation testing.
"""

from __future__ import annotations

import random

from .log import RaftLog
from .node import RaftNode
from .statemachine import Recorder


class NoLeaderError(Exception):
    pass


class SimCluster:
    def __init__(self, size, *, seed=1, config=None, message_latency=1):
        self.ids = list(range(size))
        self.config = config
        self._seed = seed
        self._latency = message_latency
        self.state_machines = {i: Recorder() for i in self.ids}
        self.logs = {i: RaftLog() for i in self.ids}
        self.nodes = {}
        for i in self.ids:
            self._create_node(i)
        self.down = set()
        self._partition = None  # list[set] of node-id groups, or None
        self.inflight = []  # list of (deliver_at_tick, Envelope)
        self.tick_no = 0

    def _create_node(self, node_id, last_applied=0):
        peers = [j for j in self.ids if j != node_id]
        node = RaftNode(
            node_id,
            peers,
            apply=self.state_machines[node_id].apply,
            log=self.logs[node_id],
            rng=random.Random(1000 + node_id + 7 * self._seed),
            config=self.config,
            last_applied=last_applied,
        )
        self.nodes[node_id] = node

    # -- driving ------------------------------------------------------------
    def step(self, times=1):
        for _ in range(times):
            self.tick_no += 1
            produced = []
            ready = [env for at, env in self.inflight if at <= self.tick_no]
            self.inflight = [(at, env) for at, env in self.inflight if at > self.tick_no]
            for env in ready:
                if self._deliverable(env):
                    produced += self.nodes[env.dst].step(env)
            for node_id in self.ids:
                if node_id not in self.down:
                    produced += self.nodes[node_id].tick()
            self._enqueue(produced)
        return self

    def run_until(self, predicate, max_steps=500):
        for _ in range(max_steps):
            if predicate():
                return True
            self.step()
        return predicate()

    def _enqueue(self, envelopes):
        for env in envelopes:
            self.inflight.append((self.tick_no + self._latency, env))

    def _deliverable(self, env) -> bool:
        if env.src in self.down or env.dst in self.down:
            return False
        if self._partition is not None:
            group = next((g for g in self._partition if env.src in g), set())
            if env.dst not in group:
                return False
        return True

    # -- fault injection ----------------------------------------------------
    def crash(self, node_id):
        self.down.add(node_id)

    def restart(self, node_id):
        self.down.discard(node_id)
        # Rebuild volatile state from the persisted log and durable apply index.
        self._create_node(node_id, last_applied=self.state_machines[node_id].applied_index)

    def partition(self, *groups):
        self._partition = [set(group) for group in groups]

    def heal(self):
        self._partition = None

    # -- queries / client ---------------------------------------------------
    def leaders(self):
        return [n for n in self.nodes.values() if n.is_leader and n.node_id not in self.down]

    def leader(self):
        current = [n for n in self.leaders()]
        current.sort(key=lambda n: n.current_term, reverse=True)
        return current[0] if current else None

    def client_write(self, command):
        leader = self.leader()
        if leader is None:
            raise NoLeaderError("no leader is currently elected")
        index, produced = leader.propose(command)
        self._enqueue(produced)
        return index

    def committed_commands(self, node_id):
        node = self.nodes[node_id]
        return [node.log.get(i).command for i in range(1, node.commit_index + 1)]
