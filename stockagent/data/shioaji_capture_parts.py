"""Shared selection contract for immutable Shioaji capture parts.

The selector lives in the package because both dataset builders and operator
scripts consume it.  ``downloader.shioaji_capture_parts`` remains a compatibility
import for existing scripts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable


_PART_RE = re.compile(
    r"^(?:capture=(?P<capture_id>[A-Za-z0-9_.-]+)-)?"
    r"worker=(?P<worker>\d+)-part=(?P<part>\d+)-(?P<stamp_ns>\d+)\.parquet$"
)
_PART_COUNT_KEYS = {
    "ticks": "tick_parts",
    "book_events": "book_parts",
    "book_1s": "book_1s_parts",
}


def read_capture_manifests(
    capture_root: Path, trade_date: str
) -> list[dict[str, Any]]:
    manifest_dir = capture_root / "manifests" / f"trade_date={trade_date}"
    paths = sorted(manifest_dir.glob("worker=*.json"))
    manifests: list[dict[str, Any]] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"JSON root is not an object: {path}")
        manifests.append(payload)
    return manifests


def shared_capture_id(manifests: Iterable[dict[str, Any]]) -> str | None:
    manifest_list = list(manifests)
    schema_versions = {int(item.get("schema_version", -1)) for item in manifest_list}
    if schema_versions and min(schema_versions) >= 3:
        capture_ids = {str(item.get("capture_id", "")) for item in manifest_list}
        if len(capture_ids) != 1 or "" in capture_ids:
            raise RuntimeError(
                f"worker manifests do not share one capture_id: {sorted(capture_ids)}"
            )
        return next(iter(capture_ids))
    if any(version >= 3 for version in schema_versions):
        raise RuntimeError(
            f"worker manifests mix capture schemas: {sorted(schema_versions)}"
        )
    return None


def _legacy_write_window_ns(manifest: dict[str, Any]) -> tuple[int, int]:
    try:
        started = datetime.fromisoformat(str(manifest["started_at_utc"]))
        finished = datetime.fromisoformat(str(manifest["finished_at_utc"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "legacy capture manifest lacks a valid started/finished interval"
        ) from exc
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=timezone.utc)
    # Schema-2 manifests store whole seconds. The lower bound was rounded down;
    # keep the upper bound exclusive at the following second so the final flush
    # in the recorded finish second is retained.
    return (
        int(started.timestamp() * 1_000_000_000),
        int((finished + timedelta(seconds=1)).timestamp() * 1_000_000_000),
    )


def select_capture_part_paths(
    *,
    capture_root: Path,
    kind: str,
    trade_date: str,
    manifests: Iterable[dict[str, Any]],
    verify_part_counts: bool = True,
) -> list[Path]:
    if kind not in _PART_COUNT_KEYS:
        raise ValueError(f"unsupported capture kind: {kind}")
    manifest_list = list(manifests)
    capture_id = shared_capture_id(manifest_list)
    by_worker = {int(item["worker_index"]): item for item in manifest_list}
    if len(by_worker) != len(manifest_list):
        raise RuntimeError("capture manifests contain duplicate worker indices")
    legacy_windows = (
        {
            worker: _legacy_write_window_ns(manifest)
            for worker, manifest in by_worker.items()
        }
        if capture_id is None
        else {}
    )
    selected: list[Path] = []
    counts = {worker: 0 for worker in by_worker}
    partition = capture_root / kind / f"trade_date={trade_date}"
    for path in sorted(partition.rglob("*.parquet")):
        match = _PART_RE.fullmatch(path.name)
        if match is None:
            continue
        worker = int(match.group("worker"))
        if worker not in by_worker:
            continue
        file_capture_id = match.group("capture_id")
        if capture_id is not None:
            belongs = file_capture_id == capture_id
        else:
            start_ns, end_ns = legacy_windows[worker]
            stamp_ns = int(match.group("stamp_ns"))
            belongs = file_capture_id is None and start_ns <= stamp_ns < end_ns
        if belongs:
            selected.append(path)
            counts[worker] += 1
    if verify_part_counts:
        part_key = _PART_COUNT_KEYS[kind]
        for worker, manifest in by_worker.items():
            expected = int(manifest.get(part_key, -1))
            if counts[worker] != expected:
                raise RuntimeError(
                    f"worker {worker} {kind} part count mismatch "
                    f"manifest={expected} actual={counts[worker]}"
                )
    if not selected:
        raise RuntimeError(f"no {kind} parquet files found for selected capture")
    return selected


__all__ = [
    "read_capture_manifests",
    "select_capture_part_paths",
    "shared_capture_id",
]
