"""Small, revision-bound monitor view of the large paper-trading state.

The paper engine owns publication. This optional view is only an acceleration
for read-only monitors; a missing or mismatched view requires the full state.
It is a metadata invalidation hint, not a cryptographic integrity receipt.
"""

from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
from typing import Any


PROJECTION_VERSION = 1


def _identity(stat: os.stat_result) -> list[int]:
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def build_shioaji_monitor_projection(
    state: Mapping[str, Any], state_path: Path, *, state_revision: int,
) -> dict[str, Any] | None:
    """Bind only the Shioaji monitor's required counters to the committed state."""

    if type(state_revision) is not int or state.get("state_revision") != state_revision:
        return None
    try:
        signature = _identity(state_path.stat())
    except OSError:
        return None
    benchmarks = state.get("benchmarks")
    modes = state.get("modes")
    sources: set[str] = set()
    quote_times: list[str] = []
    if isinstance(benchmarks, Mapping):
        for row in benchmarks.values():
            if not isinstance(row, Mapping):
                continue
            source = str(row.get("source") or "")
            if not source.startswith("shioaji:"):
                continue
            sources.add(source)
            if row.get("last_quote_at") is not None:
                quote_times.append(str(row["last_quote_at"]))
    return {
        "schema_version": PROJECTION_VERSION,
        "state_revision": state_revision,
        "state_signature": signature,
        "mode_count": len(modes) if isinstance(modes, Mapping) else 0,
        "benchmark_count": len(benchmarks) if isinstance(benchmarks, Mapping) else 0,
        "source_count": len(sources),
        "quote_times": sorted(quote_times),
    }


def validated_shioaji_monitor_projection(
    status: Mapping[str, Any] | None, state_path: Path,
) -> dict[str, Any] | None:
    """Accept a same-revision view only while the state file identity matches."""

    if not isinstance(status, Mapping):
        return None
    projection = status.get("shioaji_monitor_projection")
    if not isinstance(projection, dict):
        return None
    revision = status.get("state_revision")
    if (
        status.get("simulation_only") is not True
        or status.get("production_order_possible") is not False
        or projection.get("schema_version") != PROJECTION_VERSION
        or type(revision) is not int
        or projection.get("state_revision") != revision
        or type(projection.get("mode_count")) is not int
        or type(projection.get("benchmark_count")) is not int
        or type(projection.get("source_count")) is not int
        or projection["mode_count"] < 0
        or projection["benchmark_count"] < 0
        or projection["source_count"] < 0
        or not isinstance(projection.get("quote_times"), list)
        or not all(isinstance(item, str) for item in projection["quote_times"])
        or not isinstance(projection.get("state_signature"), list)
        or len(projection["state_signature"]) != 5
        or any(type(item) is not int for item in projection["state_signature"])
    ):
        return None
    try:
        if projection["state_signature"] != _identity(state_path.stat()):
            return None
    except OSError:
        return None
    return projection


__all__ = [
    "build_shioaji_monitor_projection",
    "validated_shioaji_monitor_projection",
]
