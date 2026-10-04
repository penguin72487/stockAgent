#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
import fcntl
import json
import math
import os
import re
import shlex
import stat
import sys
from pathlib import Path, PurePosixPath
import time
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    atomic_write_json,
    sha256_file,
)
from scripts.bybit_refresh_inputs import materialization_input_signature  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
)

DEFAULT_CATALOG = REPO_ROOT / "configs" / "data_sync" / "packed_datasets.json"
SOURCE_LOCK_WAIT_SECONDS = 180.0


class SourceCoordinationLockTimeout(SnapshotError):
    """The bounded wait expired while another source owner held the lock."""


def _expected_bybit_signature(value: object) -> dict[str, object]:
    if (
        not isinstance(value, dict)
        or set(value) != {"files", "metadata_sha256"}
        or type(value.get("files")) is not int
        or value["files"] < 0
        or not isinstance(value.get("metadata_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", value["metadata_sha256"]) is None
    ):
        raise SnapshotError("invalid expected Bybit input signature")
    return dict(value)


def _parse_expected_bybit_signature(text: str) -> dict[str, object]:
    try:
        return _expected_bybit_signature(json.loads(text))
    except (ValueError, SnapshotError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


@contextmanager
def _source_coordination_lock(
    entry: Mapping[str, Any],
    *,
    inherited_fd: int | None = None,
    wait_seconds: float | None = None,
):
    """Serialize a catalogued publisher with cooperative source writers.

    The packed publisher's stat/hash/fingerprint checks remain authoritative;
    this lock only closes the scheduled writer's check-then-start race. A
    subprocess may borrow the owner's open file description through pass_fds;
    it must neither unlock nor close that descriptor on context exit.
    """
    configured = entry.get("source_coordination_lock")
    if not configured:
        if inherited_fd is not None:
            raise SnapshotError("inherited source lock requires a catalogued lock")
        yield None
        return
    relative = Path(str(configured))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotError(f"invalid source coordination lock: {configured!r}")
    path = (REPO_ROOT / relative).resolve()
    if not path.is_relative_to(REPO_ROOT.resolve()):
        raise SnapshotError(f"source coordination lock leaves repo: {configured!r}")
    if inherited_fd is not None:
        if type(inherited_fd) is not int or inherited_fd < 3:
            raise SnapshotError("inherited source lock fd must be an integer >= 3")
        try:
            held = os.fstat(inherited_fd)
            expected = path.stat()
            if not stat.S_ISREG(held.st_mode) or not stat.S_ISREG(expected.st_mode):
                raise SnapshotError("inherited source lock must be a regular file")
            if (held.st_dev, held.st_ino) != (expected.st_dev, expected.st_ino):
                raise SnapshotError("inherited source lock does not match the catalog lock")
            # A distinct open file description held by another process must
            # fail immediately; the owner's inherited description is reentrant.
            fcntl.flock(inherited_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            current = path.stat()
            if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
                raise SnapshotError("catalog source lock changed while borrowing it")
        except OSError as exc:
            raise SnapshotError(f"cannot borrow source coordination lock: {exc}") from exc
        yield inherited_fd
        return
    duration = SOURCE_LOCK_WAIT_SECONDS if wait_seconds is None else wait_seconds
    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise SnapshotError("source coordination lock wait must be finite and nonnegative")
    path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + duration
    with path.open("a+b") as handle:
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise SourceCoordinationLockTimeout(
                        f"timed out waiting for source coordination lock: {path}"
                    ) from None
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        print(
            f"source_coordination_lock dataset={entry['dataset']} "
            f"wait_seconds={time.monotonic() - started:.3f}",
            file=sys.stderr,
            flush=True,
        )
        # Close only our descriptor. Explicit LOCK_UN would also release a
        # surviving child's inherited open file description after owner failure.
        yield handle.fileno()


def _load_catalog(path: Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read data release catalog {path}: {exc}") from exc
    if not isinstance(value, dict) or int(value.get("schema_version", -1)) != 1:
        raise SnapshotError(f"unsupported data release catalog: {path}")
    datasets = value.get("datasets")
    if not isinstance(datasets, list):
        raise SnapshotError("catalog datasets must be a list")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in datasets:
        if not isinstance(raw, dict):
            raise SnapshotError("catalog dataset entry must be an object")
        dataset = str(raw.get("dataset", ""))
        if not dataset or dataset in seen:
            raise SnapshotError(f"duplicate or empty catalog dataset: {dataset!r}")
        seen.add(dataset)
        result.append(raw)
    return result


def _running_commands() -> list[tuple[int, str]]:
    commands: list[tuple[int, str]] = []
    own_pid = os.getpid()
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            pid = int(path.parent.name)
            if pid == own_pid:
                continue
            command = shlex.join([value.decode("utf-8", errors="replace")
                                  for value in path.read_bytes().split(b"\0") if value])
        except (OSError, ValueError):
            continue
        if command:
            commands.append((pid, command))
    return commands


def _blockers(entry: Mapping[str, Any], commands: list[tuple[int, str]]) -> list[dict[str, Any]]:
    patterns = [str(item) for item in entry.get("active_process_substrings", [])]
    def matches(command: str, pattern: str) -> bool:
        # A waiting shell wrapper may mention an already-finished downloader
        # inside its -c program. Match actual script argv, not quoted programs.
        if pattern.endswith(".py") and "/" not in pattern:
            try:
                return any(Path(arg).name == pattern and not any(c.isspace() for c in arg)
                           for arg in shlex.split(command))
            except ValueError:
                return pattern in command  # malformed evidence fails closed
        return pattern in command
    return [
        {"pid": pid, "pattern": pattern, "command": command}
        for pid, command in commands
        for pattern in patterns
        if matches(command, pattern)
    ]


def _source_path(entry: Mapping[str, Any]) -> Path:
    source = Path(str(entry["source"]))
    return source if source.is_absolute() else REPO_ROOT / source


def _source_freshness(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    config = entry.get("freshness")
    if config is None:
        return None
    if not isinstance(config, Mapping):
        raise SnapshotError(f"dataset {entry['dataset']} freshness must be an object")
    relative = PurePosixPath(str(config.get("receipt", "")))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotError(f"invalid freshness receipt path: {relative}")
    receipt_path = _source_path(entry).joinpath(*relative.parts)
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(
            f"cannot read freshness receipt {receipt_path}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise SnapshotError(f"freshness receipt must be an object: {receipt_path}")
    field = str(config.get("field", ""))
    value = str(payload.get(field, ""))
    if config.get("format") != "iso-date":
        raise SnapshotError(f"unsupported freshness format for {entry['dataset']}")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SnapshotError(
            f"freshness field {field} is not an ISO date: {value!r}"
        ) from exc
    required = config.get("required_values", {})
    if not isinstance(required, Mapping):
        raise SnapshotError("freshness required_values must be an object")
    mismatches = {
        str(key): {"expected": expected, "actual": payload.get(key)}
        for key, expected in required.items()
        if payload.get(key) != expected
    }
    if mismatches:
        raise SnapshotError(
            f"freshness receipt completion gate failed: {mismatches}"
        )
    if "max_receipt_age_hours" in config:
        try:
            max_hours = float(config["max_receipt_age_hours"])
            checked = datetime.fromisoformat(str(payload[config["timestamp_field"]]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotError("freshness receipt timestamp is missing or invalid") from exc
        if (not 0 < max_hours <= 8760 or checked.tzinfo is None
                or not timedelta(0) <= datetime.now(UTC) - checked.astimezone(UTC) <= timedelta(hours=max_hours)):
            raise SnapshotError("freshness receipt is outside its configured age window")
    return {
        "field": field,
        "format": "iso-date",
        "receipt": relative.as_posix(),
        "receipt_sha256": sha256_file(receipt_path),
        "value": value,
    }


def _latest_cold_freshness(
    sync_root: Path | None, dataset: str
) -> dict[str, Any] | None:
    if sync_root is None:
        return None
    try:
        # Freshness non-regression is metadata-backed even while an object's
        # payload is missing. Missing bytes cannot erase a newer date gate.
        resolved = resolve_latest_packed(sync_root, dataset, require_objects=False)
    except SnapshotError:
        return None
    metadata = resolved.manifest.get("metadata", {})
    value = metadata.get("freshness_value") if isinstance(metadata, Mapping) else None
    if not value:
        return None
    return {
        "snapshot_id": resolved.manifest["snapshot_id"],
        "field": metadata.get("freshness_field"),
        "format": metadata.get("freshness_format"),
        "value": str(value),
    }


def _status(
    entry: Mapping[str, Any],
    commands: list[tuple[int, str]],
    *,
    sync_root: Path | None = None,
) -> dict[str, Any]:
    source = _source_path(entry)
    blockers = _blockers(entry, commands)
    freshness_error: str | None = None
    try:
        source_freshness = _source_freshness(entry)
    except SnapshotError as exc:
        source_freshness = None
        freshness_error = str(exc)
    cold_freshness = _latest_cold_freshness(
        sync_root, str(entry["dataset"])
    )
    non_regression = (
        source_freshness is None
        or cold_freshness is None
        or str(source_freshness["value"]) >= str(cold_freshness["value"])
    )
    return {
        "dataset": entry["dataset"],
        "role": entry["role"],
        "source": str(source),
        "source_exists": source.exists(),
        "source_resolved": str(source.resolve()) if source.exists() else None,
        "publish": bool(entry["publish"]),
        "publish_ready": bool(entry["publish"])
        and source.is_dir()
        and not blockers
        and freshness_error is None
        and non_regression,
        "active_blockers": blockers,
        "source_freshness": source_freshness,
        "latest_cold_freshness": cold_freshness,
        "freshness_non_regression": non_regression,
        "freshness_error": freshness_error,
        "note": entry["note"],
    }


def _publish_entry(
    entry: Mapping[str, Any], args: argparse.Namespace, sync_root: Path
) -> tuple[dict[str, Any], Any | None]:
    expected_inputs = getattr(args, "expected_bybit_input_signature", None)
    source_guard = None
    if expected_inputs is not None:
        expected_inputs = _expected_bybit_signature(expected_inputs)
        if entry["dataset"] != "bybit" or getattr(args, "source_lock_fd", None) is None:
            raise SnapshotError("expected Bybit inputs require Bybit publication under an inherited lock")
        if _source_path(entry).resolve() != (REPO_ROOT / "data_bybit").resolve():
            raise SnapshotError("Bybit input proof does not match the catalog source")

        def source_guard() -> None:
            try:
                current = materialization_input_signature(REPO_ROOT)
            except (OSError, RuntimeError) as exc:
                raise SnapshotError(f"cannot verify Bybit build inputs: {exc}") from exc
            if current != expected_inputs:
                raise SnapshotError("Bybit materialization inputs changed before publication commit")

    # Acquire before the final process/receipt check. A writer that starts
    # between an earlier status call and publication must not escape the gate.
    with _source_coordination_lock(
        entry, inherited_fd=getattr(args, "source_lock_fd", None)
    ):
        status = _status(entry, _running_commands(), sync_root=sync_root)
        if not status["publish_ready"]:
            return status, None
        resolved = publish_packed_snapshot(
            sync_root,
            str(entry["dataset"]),
            _source_path(entry),
            node_id=args.node_id,
            loose_file_threshold_bytes=int(entry["loose_threshold_mib"])
            * 1024 * 1024,
            pack_buckets=int(entry["pack_buckets"]),
            excluded_subtrees=[
                str(value) for value in entry.get("excluded_subtrees", [])
            ],
            metadata={
                "role": str(entry["role"]),
                "catalog_schema": "1",
                **({
                    "bybit_materialization_input_files": str(expected_inputs["files"]),
                    "bybit_materialization_input_metadata_sha256": str(expected_inputs["metadata_sha256"]),
                } if expected_inputs is not None else {}),
                **(
                    {
                        "freshness_field": str(status["source_freshness"]["field"]),
                        "freshness_format": str(status["source_freshness"]["format"]),
                        "freshness_receipt": str(status["source_freshness"]["receipt"]),
                        "freshness_receipt_sha256": str(
                            status["source_freshness"]["receipt_sha256"]
                        ),
                        "freshness_value": str(status["source_freshness"]["value"]),
                    }
                    if status["source_freshness"] is not None
                    else {}
                ),
            },
            repo_root=REPO_ROOT,
            **({"source_guard": source_guard} if source_guard is not None else {}),
            **({"defer_scan": True} if getattr(args, "defer_scan", False) else {}),
            **({"recover_missing_base_objects": True} if args.recover_missing_base_objects else {}),
        )
        return status, resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish catalogued canonical datasets through the packed Syncthing "
            "release layer. Downloader work shards are never selected implicitly."
        )
    )
    parser.add_argument("command", choices=("status", "publish"))
    parser.add_argument("dataset", nargs="?")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--sync-root", type=Path)
    parser.add_argument("--node-id")
    parser.add_argument(
        "--defer-scan", action="store_true",
        help="Commit and durably queue scans; the source-lock owner must immediately drain outside its lease.",
    )
    parser.add_argument(
        "--source-lock-fd", type=int,
        help="Borrow a catalog-matched source lock descriptor for one dataset publication.",
    )
    parser.add_argument(
        "--expected-bybit-input-signature", type=_parse_expected_bybit_signature,
        help="Bind Bybit publication to the refresh input metadata proof (JSON).",
    )
    parser.add_argument(
        "--result-receipt", type=Path,
        help="Atomically write the successful single-dataset result to a new local path.",
    )
    parser.add_argument("--recover-missing-base-objects", action="store_true",
        help="Explicit repair: retain damaged history; repack current source members into verified objects before updating the head.")
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="Publish every catalog entry that is present, publishable and inactive.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.defer_scan and (
            args.command != "publish" or not args.dataset or args.all_ready
            or args.source_lock_fd is None or args.result_receipt is None
        ):
            raise SnapshotError("--defer-scan requires one publication, inherited source lock, and unique result receipt")
        if args.expected_bybit_input_signature is not None:
            args.expected_bybit_input_signature = _expected_bybit_signature(
                args.expected_bybit_input_signature,
            )
            if (
                args.command != "publish" or args.dataset != "bybit"
                or args.all_ready or args.source_lock_fd is None
            ):
                raise SnapshotError(
                    "--expected-bybit-input-signature requires explicit Bybit publish with --source-lock-fd"
                )
        if args.result_receipt is not None:
            if args.command != "publish" or not args.dataset or args.all_ready:
                raise SnapshotError("--result-receipt requires one explicit publish dataset")
            if args.result_receipt.exists() or args.result_receipt.is_symlink():
                raise SnapshotError("publication result receipt must be a new path")
        if args.source_lock_fd is not None and (
            args.command != "publish" or not args.dataset or args.all_ready
            or args.source_lock_fd < 3
        ):
            raise SnapshotError(
                "--source-lock-fd requires one explicit publish dataset and an fd >= 3"
            )
        entries = _load_catalog(args.catalog.resolve())
        commands = _running_commands()
        status_sync_root = args.sync_root.resolve() if args.sync_root else None
        if args.command == "status":
            selected = entries
            if args.dataset:
                selected = [item for item in entries if item["dataset"] == args.dataset]
                if not selected:
                    raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "datasets": [
                            _status(item, commands, sync_root=status_sync_root)
                            for item in selected
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.sync_root is None:
            raise SnapshotError("publish requires --sync-root")
        if args.recover_missing_base_objects and args.all_ready:
            raise SnapshotError("missing-object recovery requires one explicit dataset")
        if bool(args.dataset) == bool(args.all_ready):
            raise SnapshotError("select exactly one dataset or --all-ready")
        selected = (
            [item for item in entries if item["dataset"] == args.dataset]
            if args.dataset
            else entries
        )
        if not selected:
            raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
        sync_root = args.sync_root.resolve()
        initialize_packed_layout(sync_root, node_id=args.node_id)
        results: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for entry in selected:
            status, resolved = _publish_entry(entry, args, sync_root)
            if resolved is None:
                if args.all_ready:
                    skipped.append(status)
                    continue
                raise SnapshotError(
                    f"dataset {entry['dataset']} is not publish-ready: "
                    f"publish={entry['publish']} source_exists={status['source_exists']} "
                    f"active_blockers={status['active_blockers']}"
                )
            results.append(
                {
                    "dataset": entry["dataset"],
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "scan_policy": "durably_queued" if args.defer_scan else "immediate",
                    "inventory_sha256": resolved.manifest["archive"]["inventory"]["sha256"],
                    "source_files": resolved.manifest["source"]["files"],
                    "source_bytes": resolved.manifest["source"]["logical_bytes"],
                    "sync_objects": resolved.manifest["archive"]["object_count"] + 1,
                    "sync_bytes": resolved.manifest["archive"]["stored_bytes"],
                    "base_snapshot_id": resolved.manifest["archive"].get(
                        "base_snapshot_id"
                    ),
                    "reused_files": resolved.manifest["archive"].get(
                        "reused_files", 0
                    ),
                    "changed_files": resolved.manifest["archive"].get(
                        "changed_files"
                    ),
                    "new_sync_objects": resolved.manifest["archive"].get(
                        "new_object_count"
                    ),
                    "new_sync_bytes": resolved.manifest["archive"].get(
                        "new_stored_bytes"
                    ),
                }
            )
        result_payload = {"published": results, "skipped": skipped}
        if args.result_receipt is not None:
            # A failure here can follow a committed head. Callers must treat a
            # missing receipt/nonzero exit as outcome unknown, not unpublished.
            if args.result_receipt.exists() or args.result_receipt.is_symlink():
                raise SnapshotError("publication result receipt must be a new path")
            atomic_write_json(args.result_receipt, result_payload)
        print(
            json.dumps(
                result_payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
