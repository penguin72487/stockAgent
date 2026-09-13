"""Immutable, session-grained projection for dashboard benchmark history.

The canonical benchmark builder still owns financial semantics.  This module
only changes their storage grain: immutable content-addressed session shards,
an atomic current head, and an append-only change ledger.  Readers verify every
selected shard before using it and can always fall back to the canonical
``benchmark_history.json`` when this companion projection is absent or stale.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import threading
from typing import Any, Final


PROJECTION_SCHEMA_VERSION: Final[int] = 1
PROJECTION_DIRECTORY: Final[str] = "benchmark_history_projection/v1"
PROJECTION_HEAD_FILENAME: Final[str] = "head.json"
PROJECTION_DELTA_FILENAME: Final[str] = "delta.jsonl"
MAX_HEAD_BYTES: Final[int] = 4 * 1024 * 1024
MAX_SHARD_COMPRESSED_BYTES: Final[int] = 16 * 1024 * 1024

# Interior minute rows need only fields consumed by chart projection and
# historical valuation/status checks.  The last row for each benchmark/session
# remains complete so endpoint audit details are not discarded.
BENCHMARK_HISTORY_INTERIOR_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "benchmark_id",
        "benchmark_origin_rebased",
        "contract_code",
        "fresh_trade_notional_coverage_ratio",
        "fresh_trade_position_count",
        "historical_minute_replay",
        "initial_capital_twd",
        "last_mark_price",
        "last_trade_carried_position_count",
        "minute",
        "minute_valuation_contract",
        "missing_price_position_count",
        "recorded_at",
        "return_fraction",
        "return_pct",
        "session_date",
        "total_equity_twd",
        "valuation_executable",
        "valuation_source",
        "valuation_stale",
    }
)


@dataclass(frozen=True, slots=True)
class BenchmarkProjection:
    source_size: int
    source_modified_ns: int
    source_sha256: str
    origins: dict[str, Mapping[str, Any]]
    session_entries: dict[str, Mapping[str, Any]]
    marks_by_session: dict[str, tuple[Mapping[str, Any], ...]]

    @property
    def marks(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(
            row
            for session_date in sorted(self.marks_by_session)
            for row in self.marks_by_session[session_date]
        )


def projection_head_path(state_dir: Path) -> Path:
    return Path(state_dir) / PROJECTION_DIRECTORY / PROJECTION_HEAD_FILENAME


def projection_delta_path(state_dir: Path) -> Path:
    return Path(state_dir) / PROJECTION_DIRECTORY / PROJECTION_DELTA_FILENAME


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _atomic_private_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _valid_session_date(value: object) -> str:
    text = str(value or "")[:10]
    if (
        len(text) != 10
        or text[4] != "-"
        or text[7] != "-"
        or not (text[:4] + text[5:7] + text[8:]).isdigit()
    ):
        return ""
    return text


def _compact_session_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    retained = [dict(row) for row in rows]
    latest: dict[str, int] = {}
    for index, row in enumerate(retained):
        latest[str(row.get("benchmark_id") or "")] = index
    return [
        row
        if latest.get(str(row.get("benchmark_id") or "")) == index
        else {
            key: value
            for key, value in row.items()
            if key in BENCHMARK_HISTORY_INTERIOR_FIELDS
        }
        for index, row in enumerate(retained)
    ]


def _load_head(state_dir: Path) -> dict[str, Any] | None:
    path = projection_head_path(state_dir)
    try:
        if not path.is_file() or path.stat().st_size > MAX_HEAD_BYTES:
            return None
        payload = json.loads(path.read_bytes())
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if (
        not isinstance(payload, dict)
        or int(payload.get("schema_version") or 0) != PROJECTION_SCHEMA_VERSION
        or not isinstance(payload.get("sessions"), Mapping)
        or not isinstance(payload.get("origins"), Mapping)
        or not isinstance(payload.get("source"), Mapping)
    ):
        return None
    return payload


def write_benchmark_projection(
    *,
    state_dir: Path,
    source_path: Path,
    source_sha256: str,
    origins: Mapping[str, Any],
    marks: Iterable[Mapping[str, Any]],
    created_at: str,
) -> dict[str, Any]:
    """Publish immutable session shards, then append deltas and switch head."""

    root = Path(state_dir) / PROJECTION_DIRECTORY
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in marks:
        session_date = _valid_session_date(row.get("session_date"))
        if session_date:
            grouped.setdefault(session_date, []).append(row)
    previous = _load_head(state_dir) or {}
    previous_sessions = previous.get("sessions")
    previous_sessions = (
        previous_sessions if isinstance(previous_sessions, Mapping) else {}
    )
    session_entries: dict[str, dict[str, Any]] = {}
    changed: list[dict[str, Any]] = []
    for session_date in sorted(grouped):
        shard_payload = {
            "schema_version": PROJECTION_SCHEMA_VERSION,
            "session_date": session_date,
            "marks": _compact_session_rows(grouped[session_date]),
        }
        encoded = _canonical_json(shard_payload)
        digest = _sha256_bytes(encoded)
        relative = PurePosixPath("shards", session_date, f"{digest}.json.gz")
        target = root.joinpath(*relative.parts)
        if not target.is_file():
            compressed = gzip.compress(encoded, compresslevel=1, mtime=0)
            if len(compressed) > MAX_SHARD_COMPRESSED_BYTES:
                raise ValueError(f"benchmark projection shard too large: {session_date}")
            _atomic_private_write(target, compressed)
        entry = {
            "sha256": digest,
            "path": str(relative),
            "mark_count": len(shard_payload["marks"]),
        }
        session_entries[session_date] = entry
        prior = previous_sessions.get(session_date)
        if not isinstance(prior, Mapping) or prior.get("sha256") != digest:
            changed.append(
                {
                    "schema_version": PROJECTION_SCHEMA_VERSION,
                    "created_at": created_at,
                    "session_date": session_date,
                    "previous_sha256": (
                        str(prior.get("sha256"))
                        if isinstance(prior, Mapping) and prior.get("sha256")
                        else None
                    ),
                    "sha256": digest,
                    "path": str(relative),
                    "mark_count": len(shard_payload["marks"]),
                }
            )
    for removed in sorted(set(previous_sessions) - set(session_entries)):
        prior = previous_sessions[removed]
        changed.append(
            {
                "schema_version": PROJECTION_SCHEMA_VERSION,
                "created_at": created_at,
                "session_date": removed,
                "previous_sha256": (
                    str(prior.get("sha256"))
                    if isinstance(prior, Mapping) and prior.get("sha256")
                    else None
                ),
                "sha256": None,
                "path": None,
                "mark_count": 0,
            }
        )

    source_stat = Path(source_path).stat()
    head = {
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "created_at": created_at,
        "source": {
            "name": Path(source_path).name,
            "size": source_stat.st_size,
            "modified_ns": source_stat.st_mtime_ns,
            "sha256": str(source_sha256),
        },
        "origins": dict(origins),
        "sessions": session_entries,
        "mark_count": sum(int(item["mark_count"]) for item in session_entries.values()),
    }
    if previous and {
        key: value for key, value in previous.items() if key != "created_at"
    } == {key: value for key, value in head.items() if key != "created_at"}:
        return {
            "projection_head": str(projection_head_path(state_dir)),
            "projection_sessions": len(session_entries),
            "projection_changed_sessions": [],
            "projection_mark_count": head["mark_count"],
        }
    # Deltas precede the head switch. A crash can leave harmless unreferenced
    # immutable rows, while readers never observe a head naming missing shards.
    if changed:
        delta_path = projection_delta_path(state_dir)
        delta_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            delta_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC,
            0o600,
        )
        try:
            os.chmod(delta_path, 0o600)
            for row in changed:
                os.write(descriptor, _canonical_json(row) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    _atomic_private_write(projection_head_path(state_dir), _canonical_json(head) + b"\n")
    return {
        "projection_head": str(projection_head_path(state_dir)),
        "projection_sessions": len(session_entries),
        "projection_changed_sessions": [row["session_date"] for row in changed],
        "projection_mark_count": head["mark_count"],
    }


def load_benchmark_projection(
    *,
    state_dir: Path,
    source_path: Path,
    selected_sessions: Iterable[str] | None = None,
) -> BenchmarkProjection | None:
    """Load verified selected shards when the head matches current source."""

    head = _load_head(state_dir)
    if head is None:
        return None
    try:
        stat = Path(source_path).stat()
    except OSError:
        return None
    source = head["source"]
    if (
        source.get("name") != Path(source_path).name
        or int(source.get("size") or -1) != stat.st_size
        or int(source.get("modified_ns") or -1) != stat.st_mtime_ns
        or len(str(source.get("sha256") or "")) != 64
    ):
        return None
    entries = head["sessions"]
    wanted = (
        sorted({_valid_session_date(value) for value in selected_sessions} - {""})
        if selected_sessions is not None
        else sorted(entries)
    )
    root = Path(state_dir) / PROJECTION_DIRECTORY
    marks_by_session: dict[str, tuple[Mapping[str, Any], ...]] = {}
    selected_entries: dict[str, Mapping[str, Any]] = {}
    for session_date in wanted:
        entry = entries.get(session_date)
        if not isinstance(entry, Mapping):
            continue
        digest = str(entry.get("sha256") or "")
        relative = PurePosixPath(str(entry.get("path") or ""))
        if (
            len(digest) != 64
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[:2] != ("shards", session_date)
        ):
            return None
        path = root.joinpath(*relative.parts)
        try:
            if path.stat().st_size > MAX_SHARD_COMPRESSED_BYTES:
                return None
            encoded = gzip.decompress(path.read_bytes())
            if _sha256_bytes(encoded) != digest:
                return None
            payload = json.loads(encoded)
        except (EOFError, OSError, TypeError, ValueError, json.JSONDecodeError, gzip.BadGzipFile):
            return None
        rows = payload.get("marks") if isinstance(payload, Mapping) else None
        if (
            int(payload.get("schema_version") or 0) != PROJECTION_SCHEMA_VERSION
            or payload.get("session_date") != session_date
            or not isinstance(rows, list)
            or int(entry.get("mark_count") or -1) != len(rows)
            or any(not isinstance(row, Mapping) for row in rows)
        ):
            return None
        marks_by_session[session_date] = tuple(rows)
        selected_entries[session_date] = dict(entry)
    origins = {
        str(key): value
        for key, value in head["origins"].items()
        if isinstance(value, Mapping)
    }
    return BenchmarkProjection(
        source_size=stat.st_size,
        source_modified_ns=stat.st_mtime_ns,
        source_sha256=str(source["sha256"]),
        origins=origins,
        # The compact head is also the date/fingerprint catalog.  Return every
        # entry even when only selected shard bodies were requested.
        session_entries={
            str(key): dict(value)
            for key, value in entries.items()
            if isinstance(value, Mapping)
        },
        marks_by_session=marks_by_session,
    )
