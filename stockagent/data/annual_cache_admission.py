"""Resource admission for derived cache misses, never financial/source policy.

The gateway and its children share a finite memory cgroup. Immutable annual
seeds are normally prewarmed; rebuilding them needs a larger owner than a warm
read. Probe capacity only before the canonical cache's actual miss callback.
Warm reads retain its existing hash/readback proof and do no resource probes.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

from stockagent.data.annual_event_cache import PreparedObservationCache
from stockagent.remote_build import observe_node

CONTRACT = "derived_annual_cache_miss_memory_admission_v1"
SEED_WORKING_SET_BYTES = 24 << 30
MEMORY_RESERVE_BYTES = 2 << 30


class AnnualCacheBuildDeferred(RuntimeError):
    """Retry through a sufficiently provisioned canonical source owner."""

    def __init__(self, details: dict):
        self.details = details
        super().__init__(f"annual cache warming deferred: {details['reason']}")


class MemoryAdmittedObservationCache(PreparedObservationCache):
    """Compose resource checks with the one canonical content-addressed cache.

    24 GiB is a conservative cold-seed estimate for the accepted 58.3M-event
    local workload, not a mathematical bound for every future provider table.
    The small delta estimate deliberately includes extra scratch and a 2 GiB
    reserve. Kernel/service limits and interrupted-build recovery still apply.
    No cached event/key/coordinate logic is duplicated in this adapter.
    """

    def __init__(self, root: Path, *, seed_sources: Iterable[Path],
                 capacity_observer: Callable = observe_node,
                 seed_working_set_bytes: int = SEED_WORKING_SET_BYTES,
                 memory_reserve_bytes: int = MEMORY_RESERVE_BYTES):
        super().__init__(root)
        if (type(seed_working_set_bytes) is not int or seed_working_set_bytes <= 0
                or type(memory_reserve_bytes) is not int or memory_reserve_bytes < 0):
            raise ValueError("positive cold-seed budget and nonnegative memory reserve required")
        self._seed_sources = {Path(path).resolve(strict=True) for path in seed_sources}
        self._capacity_observer = capacity_observer
        self._seed_working_set_bytes = seed_working_set_bytes
        self._memory_reserve_bytes = memory_reserve_bytes

    def _admit_miss(self, source: Path) -> None:
        seed = source in self._seed_sources
        working_set = (max(self._seed_working_set_bytes, source.stat().st_size * 128)
                       if seed else max(512 << 20, source.stat().st_size * 128))
        required = working_set + self._memory_reserve_bytes
        details = {"contract": CONTRACT, "source": str(source), "immutable_seed": seed,
                   "working_set_estimate_bytes": working_set,
                   "memory_reserve_bytes": self._memory_reserve_bytes,
                   "required_headroom_bytes": required, "provider_calls": 0,
                   "model_executed": False, "last_good_generation_retained": True}
        try:
            profile = self._capacity_observer(source, self.root)
            headroom = profile["limits"]["memory_headroom_bytes"]
            if type(headroom) is not int or headroom < 0:
                raise ValueError("invalid observed memory headroom")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise AnnualCacheBuildDeferred({**details, "reason": "capacity_unverified",
                                             "error_type": type(exc).__name__}) from exc
        if headroom < required:
            raise AnnualCacheBuildDeferred({**details, "reason": "insufficient_memory_headroom",
                                             "observed_headroom_bytes": headroom})

    def prepare(self, source: Path, features: list[str], symbols: list[str],
                prepare_coordinates: Callable, *, prepare_identity: str):
        def admitted_prepare(path, names, universe):
            self._admit_miss(path)
            return prepare_coordinates(path, names, universe)

        return super().prepare(source, features, symbols, admitted_prepare,
                               prepare_identity=prepare_identity)
