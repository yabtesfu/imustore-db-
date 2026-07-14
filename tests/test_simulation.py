"""Deterministic simulation tests for the storage engine (Phase 7).

The engine runs against a simulated disk under seeded, reproducible fault
injection, and every recovery is checked against a reference model.
"""

import os

import pytest

from imustore.simdisk import SimCrash, SimDisk
from imustore.simulation import StorageSimulation, run_seed


def test_simdisk_models_fsync_durability():
    disk = SimDisk()
    disk.write(b"hello")
    assert disk.durable_bytes() == b""  # nothing is durable until a flush
    disk.flush()
    assert disk.durable_bytes() == b"hello"

    disk.seek(0, os.SEEK_END)
    disk.write(b" world")  # written but not flushed
    assert disk.durable_bytes() == b"hello"  # a crash right now would lose " world"
    disk.seek(0)
    assert disk.read() == b"hello world"  # ...but the live process still sees it


def test_simdisk_armed_crash_does_not_persist():
    disk = SimDisk()
    disk.write(b"data")
    disk.arm_crash(1)  # the next flush "loses power"
    with pytest.raises(SimCrash):
        disk.flush()
    assert disk.durable_bytes() == b""  # the interrupted flush persisted nothing


def test_simulation_survives_fault_injection_across_seeds():
    total_crashes = 0
    for seed in range(40):
        stats = run_seed(seed, steps=150)  # raises InvariantError on any violation
        total_crashes += stats["crashes"]
    assert total_crashes > 0  # recovery was genuinely exercised, not skipped


def test_simulation_is_reproducible_from_a_seed():
    first = StorageSimulation(4712).run(300)
    second = StorageSimulation(4712).run(300)
    assert first == second  # same seed -> identical run, so any failure replays
