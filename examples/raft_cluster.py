"""Spin up a real 3-node Raft cluster over TCP, then kill the leader.

    python examples/raft_cluster.py

The nodes talk to each other over localhost sockets, elect a leader, replicate a
write to a majority, then survive the leader being killed by electing a new one
and serving the same data.
"""

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from imustore.raft.node import RaftConfig, NotLeaderError  # noqa: E402
from imustore.raft.server import RaftServer  # noqa: E402


async def wait_for_leader(servers, timeout=5.0):
    for _ in range(int(timeout / 0.05)):
        leaders = [i for i, s in servers.items() if s.is_leader]
        if leaders:
            return leaders[0]
        await asyncio.sleep(0.05)
    return None


async def submit_to_leader(servers, command):
    for _ in range(40):
        leader = await wait_for_leader(servers)
        try:
            await servers[leader].submit(command)
            return leader
        except (NotLeaderError, asyncio.TimeoutError):
            await asyncio.sleep(0.1)
    raise RuntimeError("could not reach a leader")


async def main():
    addresses = {0: ("127.0.0.1", 7400), 1: ("127.0.0.1", 7401), 2: ("127.0.0.1", 7402)}
    config = RaftConfig(election_timeout_min=8, election_timeout_max=16, heartbeat_interval=2)

    with tempfile.TemporaryDirectory() as tmp:
        servers = {}
        for node_id, (host, port) in addresses.items():
            peers = {j: addr for j, addr in addresses.items() if j != node_id}
            servers[node_id] = RaftServer(
                node_id, peers, db_path=f"{tmp}/node{node_id}.db",
                host=host, port=port, tick_ms=20, seed=node_id, config=config,
            )
            await servers[node_id].start()

        leader = await wait_for_leader(servers)
        print(f"cluster is up; elected leader = node {leader}")

        await submit_to_leader(servers, {"op": "set", "key": "service", "value": "immustore"})
        await submit_to_leader(servers, {"op": "set", "key": "region", "value": "us-east"})
        await asyncio.sleep(0.3)  # let the followers apply
        print("replicated writes; each node's view of 'service':")
        for node_id, server in servers.items():
            print(f"  node {node_id}: service = {server.read('service')!r}")

        print(f"\nkilling the leader (node {leader})...")
        await servers[leader].stop()
        del servers[leader]

        new_leader = await wait_for_leader(servers)
        print(f"cluster re-elected leader = node {new_leader}")
        await submit_to_leader(servers, {"op": "set", "key": "status", "value": "recovered"})
        await asyncio.sleep(0.3)
        for node_id, server in servers.items():
            print(f"  node {node_id}: service={server.read('service')!r} status={server.read('status')!r}")

        for server in servers.values():
            await server.stop()
        print("\ndone.")


if __name__ == "__main__":
    asyncio.run(main())
