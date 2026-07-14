"""Deterministic Raft tests driven by the in-memory cluster simulator.

Because the consensus core takes no wall clock and does no real I/O, every
scenario -- elections, failover, partitions, restarts, and randomized chaos --
runs reproducibly and checks Raft's safety properties directly.
"""

import random

import pytest

import imustore
from imustore.raft import KVStateMachine, NoLeaderError, NotLeaderError, SimCluster


def _committed(cluster, node_id):
    node = cluster.nodes[node_id]
    return [node.log.get(i).command for i in range(1, node.commit_index + 1)]


def _is_prefix(short, long):
    return short == long[: len(short)]


def _assert_logs_consistent(cluster):
    """State Machine Safety: committed logs never diverge across nodes."""
    logs = [_committed(cluster, i) for i in cluster.ids]
    for a in logs:
        for b in logs:
            assert _is_prefix(a, b) or _is_prefix(b, a), (a, b)


def test_elects_exactly_one_leader(tmp_path):
    cluster = SimCluster(3, seed=1)
    assert cluster.run_until(lambda: cluster.leader() is not None)
    assert len(cluster.leaders()) == 1


def test_replicates_writes_to_every_node(tmp_path):
    cluster = SimCluster(3, seed=2)
    cluster.run_until(lambda: cluster.leader() is not None)
    cluster.client_write({"op": "set", "key": "a", "value": "1"})
    cluster.client_write({"op": "set", "key": "b", "value": "2"})
    cluster.run_until(lambda: all(sm.kv() == {"a": "1", "b": "2"} for sm in cluster.state_machines.values()))
    for sm in cluster.state_machines.values():
        assert sm.kv() == {"a": "1", "b": "2"}
    _assert_logs_consistent(cluster)


def test_proposing_to_a_follower_is_rejected(tmp_path):
    cluster = SimCluster(3, seed=2)
    cluster.run_until(lambda: cluster.leader() is not None)
    follower = next(n for n in cluster.nodes.values() if not n.is_leader)
    with pytest.raises(NotLeaderError):
        follower.propose({"op": "set", "key": "x", "value": "1"})


def test_leader_failure_reelects_and_preserves_committed_data(tmp_path):
    cluster = SimCluster(5, seed=3)
    cluster.run_until(lambda: cluster.leader() is not None)
    old = cluster.leader().node_id
    cluster.client_write({"op": "set", "key": "x", "value": "1"})
    cluster.run_until(lambda: cluster.state_machines[old].kv() == {"x": "1"})

    cluster.crash(old)
    assert cluster.run_until(
        lambda: cluster.leader() is not None and cluster.leader().node_id != old, max_steps=400
    )
    cluster.client_write({"op": "set", "key": "y", "value": "2"})
    survivors = [i for i in cluster.ids if i != old]
    cluster.run_until(
        lambda: all(cluster.state_machines[i].kv() == {"x": "1", "y": "2"} for i in survivors),
        max_steps=400,
    )
    for i in survivors:
        assert cluster.state_machines[i].kv() == {"x": "1", "y": "2"}


def test_minority_partition_cannot_commit_then_heals(tmp_path):
    cluster = SimCluster(5, seed=9)
    cluster.run_until(lambda: cluster.leader() is not None)
    leader = cluster.leader().node_id
    majority = [leader] + [j for j in cluster.ids if j != leader][:2]
    minority = [j for j in cluster.ids if j not in majority]

    cluster.partition(set(majority), set(minority))
    cluster.client_write({"op": "set", "key": "p", "value": "v"})
    cluster.run_until(lambda: all(cluster.state_machines[i].kv() == {"p": "v"} for i in majority), max_steps=400)

    assert all(cluster.state_machines[i].kv() == {} for i in minority)  # minority is stalled

    cluster.heal()
    assert cluster.run_until(
        lambda: all(cluster.state_machines[i].kv() == {"p": "v"} for i in cluster.ids), max_steps=500
    )


def test_stale_leader_log_is_overwritten_after_partition(tmp_path):
    cluster = SimCluster(3, seed=4)
    cluster.run_until(lambda: cluster.leader() is not None)
    stale = cluster.leader().node_id
    others = [i for i in cluster.ids if i != stale]

    # Isolate the leader; it appends an entry it can never commit.
    cluster.partition({stale}, set(others))
    stale_node = cluster.nodes[stale]
    stale_node.propose({"op": "set", "key": "ghost", "value": "lost"})

    # The majority elects a new leader and commits a real write.
    assert cluster.run_until(
        lambda: any(cluster.nodes[i].is_leader for i in others), max_steps=400
    )
    cluster.heal()
    cluster.run_until(lambda: cluster.leader() is not None and cluster.leader().node_id in others)
    cluster.client_write({"op": "set", "key": "real", "value": "kept"})

    assert cluster.run_until(
        lambda: all(cluster.state_machines[i].kv() == {"real": "kept"} for i in cluster.ids),
        max_steps=600,
    )
    for sm in cluster.state_machines.values():
        assert "ghost" not in sm.kv()  # the stale leader's uncommitted entry was discarded
    _assert_logs_consistent(cluster)


def test_crashed_node_recovers_and_catches_up(tmp_path):
    cluster = SimCluster(3, seed=6)
    cluster.run_until(lambda: cluster.leader() is not None)
    cluster.client_write({"op": "set", "key": "a", "value": "1"})
    cluster.run_until(lambda: all(sm.kv() == {"a": "1"} for sm in cluster.state_machines.values()))

    victim = next(i for i in cluster.ids if not cluster.nodes[i].is_leader)
    cluster.crash(victim)
    cluster.client_write({"op": "set", "key": "b", "value": "2"})
    alive = [i for i in cluster.ids if i != victim]
    cluster.run_until(lambda: all(cluster.state_machines[i].kv() == {"a": "1", "b": "2"} for i in alive))

    cluster.restart(victim)
    assert cluster.run_until(
        lambda: cluster.state_machines[victim].kv() == {"a": "1", "b": "2"}, max_steps=400
    )


def test_chaos_preserves_safety_invariants(tmp_path):
    cluster = SimCluster(5, seed=20)
    rng = random.Random(20)
    leaders_by_term = {}
    letters = "abcde"

    for round_no in range(120):
        # Record and check: at most one leader per term (Election Safety).
        for node in cluster.nodes.values():
            if node.is_leader and node.node_id not in cluster.down:
                assert leaders_by_term.setdefault(node.current_term, node.node_id) == node.node_id

        action = rng.random()
        if action < 0.20 and len(cluster.down) < 2:
            cluster.crash(rng.choice(cluster.ids))
        elif action < 0.35 and cluster.down:
            cluster.restart(rng.choice(list(cluster.down)))
        elif action < 0.45:
            half = rng.sample(cluster.ids, 2)
            cluster.partition(set(half), set(i for i in cluster.ids if i not in half))
        elif action < 0.55:
            cluster.heal()
        elif cluster.leader() is not None:
            key = rng.choice(letters)
            try:
                cluster.client_write({"op": "set", "key": key, "value": str(round_no)})
            except NoLeaderError:
                pass

        cluster.step(rng.randint(1, 6))
        _assert_logs_consistent(cluster)

    # Heal everything and let the cluster converge; all nodes must agree.
    cluster.heal()
    for node_id in list(cluster.down):
        cluster.restart(node_id)
    assert cluster.run_until(lambda: cluster.leader() is not None, max_steps=500)
    cluster.client_write({"op": "set", "key": "final", "value": "sync"})
    reference = cluster.leader().node_id
    assert cluster.run_until(
        lambda: all(
            cluster.state_machines[i].kv() == cluster.state_machines[reference].kv()
            for i in cluster.ids
        ),
        max_steps=800,
    )
    _assert_logs_consistent(cluster)


def test_wire_encoding_round_trips():
    from imustore.raft.messages import (
        AppendEntries,
        AppendEntriesReply,
        Envelope,
        LogEntry,
        RequestVote,
        RequestVoteReply,
    )
    from imustore.raft.wire import decode, encode

    bodies = [
        RequestVote(3, 1, 5, 2),
        RequestVoteReply(3, True),
        AppendEntries(
            4, 2, 3, 2,
            (LogEntry(4, {"op": "set", "key": "a", "value": "1"}), LogEntry(4, {"op": "noop"})),
            3,
        ),
        AppendEntriesReply(4, True, 5),
    ]
    for body in bodies:
        envelope = Envelope(1, 2, body)
        assert decode(encode(envelope)) == envelope


def test_kv_state_machine_is_durable_and_exactly_once(tmp_path):
    path = tmp_path / "sm.db"
    db = imustore.connect(path)
    machine = KVStateMachine(db)
    assert machine.applied_index == 0

    machine.apply(1, {"op": "set", "key": "a", "value": "1"})
    machine.apply(2, {"op": "noop"})
    machine.apply(3, {"op": "set", "key": "b", "value": "2"})
    machine.apply(4, {"op": "del", "key": "a"})
    assert machine.applied_index == 4
    assert db["b"] == "2" and "a" not in db
    db.close()

    reopened = imustore.connect(path)
    assert KVStateMachine(reopened).applied_index == 4  # survived the restart
    reopened.close()
