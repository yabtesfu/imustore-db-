"""Deterministic simulation testing for the storage engine.

Everything a run does -- which key is written, when a commit happens, when the
machine "loses power" and how -- is drawn from a single seeded RNG, so a run is
perfectly reproducible: if seed 4712 finds a bug, ``--seed 4712`` reproduces it
byte for byte. The engine runs against a :class:`SimDisk` (an in-memory disk that
models fsync durability), which lets faults be injected precisely and cheaply.

Against a **reference model** (the committed key/value history), every recovery
must satisfy two invariants, no matter where or how the crash landed:

1. the database reopens and ``audit()`` passes -- a crash never corrupts it;
2. the recovered committed state equals the last committed version, or the one
   before it (the most a torn meta-block write can cost, thanks to the
   double-buffered meta blocks).

Run one:      python -m imustore.simulation --seed 42 --steps 400
Run a sweep:  python -m imustore.simulation --runs 200 --steps 400
"""

from __future__ import annotations

import argparse
import random
import struct

from .codec import JsonCodec
from .interface import DBDB
from .physical import DURABILITY_FULL, META_BODY_BYTES, META_CRC_BYTES, META_SLOT_SIZE, META_SLOTS
from .simdisk import SimCrash, SimDisk


class InvariantError(AssertionError):
    """A simulation invariant was violated (the message carries the seed)."""


class StorageSimulation:
    FAULTS = ("clean", "mid_commit", "torn_tail", "meta_corruption")

    def __init__(self, seed, *, key_space=12):
        self.seed = seed
        self.rng = random.Random(seed)
        self.key_space = key_space
        self.disk = SimDisk()
        self.db = self._open(self.disk)
        self.model = {}                 # keys written since the last commit (uncommitted)
        self.committed = [{}]           # history of committed snapshots
        self.stats = {"steps": 0, "commits": 0, "crashes": 0}

    def _open(self, disk):
        return DBDB(disk, codec=JsonCodec(), durability=DURABILITY_FULL)

    def _key(self):
        return f"k{self.rng.randrange(self.key_space)}"

    # -- one step of the workload ------------------------------------------
    def step(self):
        self.stats["steps"] += 1
        roll = self.rng.random()
        if roll < 0.45:
            key, value = self._key(), str(self.rng.randrange(1_000_000))
            self.db[key] = value
            self.model[key] = value
        elif roll < 0.60:
            key = self._key()
            try:
                del self.db[key]
            except KeyError:
                pass
            self.model.pop(key, None)
        elif roll < 0.82:
            self._commit()
        else:
            self._crash()

    def _commit(self):
        self.db.commit()
        self.committed.append(dict(self.model))
        self.stats["commits"] += 1

    # -- fault injection + recovery ----------------------------------------
    def _crash(self):
        fault = self.rng.choice(self.FAULTS)
        last = self.committed[-1]
        prev = self.committed[-2] if len(self.committed) >= 2 else None

        if fault == "mid_commit":
            self.disk.arm_crash(self.rng.choice([1, 2]))
            try:
                self._commit()  # this commit's flush will raise SimCrash
                return          # (armed beyond its flushes: it actually survived)
            except SimCrash:
                pass            # crashed part-way through the commit

        durable = bytearray(self.disk.durable_bytes())
        if fault == "torn_tail":
            durable += bytes(self.rng.randrange(256) for _ in range(self.rng.randint(1, 7)))
        elif fault == "meta_corruption" and prev is not None:
            self._corrupt_a_meta_slot(durable)

        # "Power on": reopen on exactly the bytes that survived.
        self.disk = SimDisk(initial=bytes(durable))
        self.db = self._open(self.disk)
        self.stats["crashes"] += 1

        report = self.db.audit()
        if not report.ok:
            raise InvariantError(f"seed={self.seed}: audit failed after {fault}: {report.errors}")
        recovered = {key: value for key, value in self.db.items()}
        allowed = [last] + ([prev] if prev is not None else [])
        if recovered not in allowed:
            raise InvariantError(
                f"seed={self.seed}: {fault} recovered {recovered}, expected last={last} or prev={prev}"
            )

        # The recovered state is now ground truth; rebase the model onto it.
        self.model = dict(recovered)
        self.committed = [dict(recovered)]

    def _corrupt_a_meta_slot(self, durable):
        # Flip one meta slot's checksum. Whichever slot it is, recovery must fall
        # back to the other valid slot -- so recovery lands on last or prev.
        slot = self.rng.randrange(META_SLOTS)
        offset = slot * META_SLOT_SIZE + META_BODY_BYTES
        if offset + META_CRC_BYTES <= len(durable):
            (crc,) = struct.unpack(">I", bytes(durable[offset:offset + META_CRC_BYTES]))
            durable[offset:offset + META_CRC_BYTES] = struct.pack(">I", crc ^ 0xFFFFFFFF)

    # -- run ----------------------------------------------------------------
    def run(self, steps):
        for _ in range(steps):
            self.step()
        # Final settle: commit and verify one last clean recovery.
        self._commit()
        durable = self.disk.durable_bytes()
        self.db = self._open(SimDisk(initial=durable))
        assert self.db.audit().ok, f"seed={self.seed}: final audit failed"
        final = {key: value for key, value in self.db.items()}
        if final != self.committed[-1]:
            raise InvariantError(
                f"seed={self.seed}: final state {final} != committed {self.committed[-1]}"
            )
        return self.stats


def run_seed(seed, steps=400):
    return StorageSimulation(seed).run(steps)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="imustore.simulation",
        description="Deterministic simulation testing of the storage engine.",
    )
    parser.add_argument("--seed", type=int, default=None, help="run/replay a single seed")
    parser.add_argument("--runs", type=int, default=100, help="number of seeds to sweep")
    parser.add_argument("--steps", type=int, default=400, help="operations per run")
    args = parser.parse_args(argv)

    seeds = [args.seed] if args.seed is not None else range(args.runs)
    total = {"steps": 0, "commits": 0, "crashes": 0}
    for seed in seeds:
        try:
            stats = run_seed(seed, args.steps)
        except InvariantError as exc:
            print(f"FAIL: {exc}")
            print(f"replay with: python -m imustore.simulation --seed {seed} --steps {args.steps}")
            return 1
        for key in total:
            total[key] += stats[key]

    count = len(list(seeds)) if args.seed is None else 1
    print(
        f"ok: {count} seed(s), {total['steps']} steps, "
        f"{total['commits']} commits, {total['crashes']} crash/recover cycles — all invariants held"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
