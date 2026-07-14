# Raft consensus

`imustore/raft/` replicates an ImmuStore database across a cluster so it survives
node failures. It implements the core of the Raft algorithm (Ongaro & Ousterhout,
2014): leader election, log replication, and the safety rules that make them
correct.

## A deterministic core

The consensus logic in `node.py` reads no clock and performs no I/O. A
`RaftNode` is advanced only by:

- `tick()` — one logical time step (drives election and heartbeat timeouts), and
- `step(envelope)` — one delivered message.

Both return the list of messages the node wants to send. All randomness (the
election timeout) comes from an injected RNG. This is deliberate: the exact same
node code is driven by a simulated network in tests and by real sockets in
production, and it makes every test reproducible instead of timing-dependent.

## Layers

- `messages.py` — the RPC types (`RequestVote`, `AppendEntries`, replies) and the
  `Envelope` (src, dst, body) used for routing.
- `log.py` — the persistent state Raft requires to survive a crash: the replicated
  log plus `currentTerm` and `votedFor`. The log is 1-indexed and resolves
  conflicts by truncating a divergent suffix. When given a path it is persisted
  atomically (temp file + `os.replace` + fsync).
- `node.py` — the deterministic consensus core.
- `statemachine.py` — applies committed commands. `KVStateMachine` writes to an
  ImmuStore database and stores the last applied log index *in the same commit*
  as the data, so apply is crash-safe and exactly-once across restarts.
- `simulation.py` — an in-memory cluster with a logical clock and a message queue,
  plus fault injection (`crash`, `restart`, `partition`, `heal`).
- `wire.py` / `server.py` — JSON message encoding and an `asyncio` TCP transport
  that runs the node as part of a real cluster, with a CLI (`python -m
  imustore.raft.server`).

## Safety

The implementation enforces the properties that make Raft correct, and the tests
check them directly under randomized chaos:

- **Election Safety** — at most one leader per term (a candidate needs a majority,
  and each server grants at most one vote per term).
- **Leader Completeness / up-to-date voting** — a server only votes for a candidate
  whose log is at least as up-to-date as its own, so a new leader always has every
  committed entry.
- **Log Matching** — `AppendEntries` carries the previous entry's index and term;
  a mismatch is rejected and the leader backs up, and conflicting suffixes are
  truncated.
- **State Machine Safety** — the leader only advances the commit index for an entry
  from its *current* term (counting replicas), which prevents a committed entry
  from ever being overwritten. Committed logs therefore never diverge across nodes.

## Scope

This is a single Raft group (one replicated log). Log compaction / snapshotting
and multi-group sharding (splitting the keyspace across several Raft groups for
horizontal scale) are natural extensions that are not implemented here.
