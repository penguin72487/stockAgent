#!/usr/bin/env python3
"""Opt-in ABBA notification benchmark for one pinned, already-published release.

Default is dry-run. Execution only repeats canonical Syncthing scan requests;
it never publishes, queues, clears pending work, or verifies payload bytes.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import stat
import statistics
import sys
import time
from unittest.mock import patch
import urllib.parse


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync import desync_snapshots as snapshots  # noqa: E402
from stockagent.data_sync import packed_snapshots as packed  # noqa: E402
from stockagent.data_sync import syncthing_scan as scan  # noqa: E402


SYNC_ROOT = Path("/srv/stockagent-packed")
DATASET = "bybit"
HEAD_PATH = "heads/bybit/penguin.json"
SEQUENCE = ("single", "batch", "batch", "single")


class Contaminated(snapshots.SnapshotError):
    """A competing change invalidates the frozen benchmark comparison."""


class BenchmarkBudgetExceeded(BaseException):
    """Outer deadline must bypass trial failure/revalidation handlers."""


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _safe_path(root: Path, relative: str) -> Path:
    parsed = snapshots._safe_relative_path(relative, "benchmark path")
    if parsed.as_posix() != relative:
        raise snapshots.SnapshotError("noncanonical benchmark path")
    current = root
    for part in parsed.parts:
        current /= part
        if current.is_symlink():
            raise snapshots.SnapshotError("redirected benchmark path")
    return current


def _signature(root: Path, relative: str) -> list[int]:
    info = _safe_path(root, relative).stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise snapshots.SnapshotError("benchmark input is not a regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _read_metadata(root: Path, relative: str) -> tuple[bytes, list[int]]:
    before = _signature(root, relative)
    if before[2] > 16 * 1024 * 1024:
        raise snapshots.SnapshotError("benchmark metadata exceeds bounded read size")
    content = _safe_path(root, relative).read_bytes()
    if _signature(root, relative) != before:
        raise Contaminated("metadata changed during observation")
    return content, before


def _code_hashes() -> dict[str, str]:
    paths = (Path(__file__), Path(scan.__file__), Path(packed.__file__), Path(snapshots.__file__))
    return {str(path.relative_to(REPO_ROOT)): _digest(path.read_bytes()) for path in paths}


def _no_pending(root: Path) -> None:
    if root.resolve() != SYNC_ROOT or root.is_symlink():
        raise snapshots.SnapshotError("benchmark requires the canonical cold root")
    pending = scan._pending_path(root, DATASET)
    if pending is None:
        raise snapshots.SnapshotError("canonical D-primary identity is unavailable")
    if pending.exists():
        raise Contaminated("pending notification belongs to another operation")


def _freeze_release(root: Path, head_sha256: str, object_limit: int) -> dict[str, object]:
    _no_pending(root)
    head_bytes, head_stat = _read_metadata(root, HEAD_PATH)
    if _digest(head_bytes) != head_sha256:
        raise snapshots.SnapshotError("pinned head SHA256 mismatch")
    head = json.loads(head_bytes)
    if not isinstance(head, dict) or head.get("dataset") != DATASET or head.get("node_id") != "penguin":
        raise snapshots.SnapshotError("unexpected head identity")
    snapshot_id = snapshots.validate_slug(str(head.get("snapshot_id", "")), "snapshot")
    manifest_relative = f"manifests/{DATASET}/{snapshot_id}.json"
    if head.get("schema_version") != 1 or head.get("manifest_relpath") != manifest_relative:
        raise snapshots.SnapshotError("unexpected head manifest path")
    manifest_bytes, manifest_stat = _read_metadata(root, manifest_relative)
    manifest_sha = _digest(manifest_bytes)
    if manifest_sha != head.get("manifest_sha256"):
        raise snapshots.SnapshotError("manifest SHA256 does not match the pinned head")
    manifest = json.loads(manifest_bytes)
    if not isinstance(manifest, dict):
        raise snapshots.SnapshotError("manifest is not an object")
    packed._validate_manifest(manifest)  # Metadata contract only, no payload hashing.
    if (
        manifest["dataset"] != DATASET or manifest["snapshot_id"] != snapshot_id
        or manifest["publisher"]["node_id"] != "penguin"
        or manifest["hlc"] != head.get("hlc")
    ):
        raise snapshots.SnapshotError("manifest and head identities disagree")
    archive = manifest["archive"]
    selected = list(archive["objects"][:object_limit]) + [archive["inventory"]]
    inputs = []
    for index, item in enumerate(selected):
        digest = item["sha256"]
        kind = "inventories" if index == len(selected) - 1 else {"pack": "packs", "blob": "blobs"}[item["kind"]]
        suffix = {"inventories": ".jsonl.gz", "packs": ".zip", "blobs": ".blob"}[kind]
        relative = f"objects/{kind}/{digest[:2]}/{digest}{suffix}"
        if item["relpath"] != relative or type(item.get("bytes")) is not int or item["bytes"] < 0:
            raise snapshots.SnapshotError("selected object path/size contract is invalid")
        signature = _signature(root, relative)
        if signature[2] != item["bytes"]:
            raise snapshots.SnapshotError("selected object size differs from manifest")
        inputs.append({"path": relative, "bytes": item["bytes"], "stat": signature})
    paths = [row["path"] for row in inputs]
    if not paths or len(paths) > 64 or len(set(paths)) != len(paths):
        raise snapshots.SnapshotError("benchmark selection is not bounded and unique")
    return {
        "snapshot_id": snapshot_id, "head_sha256": head_sha256, "head_stat": head_stat,
        "manifest_path": manifest_relative, "manifest_sha256": manifest_sha,
        "manifest_stat": manifest_stat, "inputs": inputs, "paths": paths,
    }


def _assert_frozen(root: Path, frozen: dict[str, object], code_hashes: dict[str, str]) -> None:
    _no_pending(root)
    for relative, expected_hash, expected_stat in (
        (HEAD_PATH, frozen["head_sha256"], frozen["head_stat"]),
        (frozen["manifest_path"], frozen["manifest_sha256"], frozen["manifest_stat"]),
    ):
        content, signature = _read_metadata(root, relative)
        if _digest(content) != expected_hash or signature != expected_stat:
            raise Contaminated("pinned head or manifest changed")
    for row in frozen["inputs"]:
        if _signature(root, row["path"]) != row["stat"]:
            raise Contaminated("selected object metadata changed")
    if _code_hashes() != code_hashes:
        raise Contaminated("benchmark or canonical implementation changed")


@contextmanager
def _scan_lock(root: Path):
    path = _safe_path(root, ".local-state/locks/scan-bybit.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        current = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise snapshots.SnapshotError("scan execution lock was redirected")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise snapshots.SnapshotError("canonical scan execution lock is busy") from None
        yield
    finally:
        os.close(fd)


@contextmanager
def _hard_deadline(seconds: float, *, terminal: bool = False):
    # This standalone Linux/WSL CLI uses the main thread. The alarm also bounds
    # a stalled mount check or a slow-drip HTTP response beyond socket timeout.
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    previous = signal.getsignal(signal.SIGALRM)
    started = time.monotonic()

    def raise_previous(signum, frame):
        if callable(previous):
            previous(signum, frame)
        # A returning foreign handler cannot disable this benchmark's bound.
        raise BenchmarkBudgetExceeded("enclosing deadline exceeded")

    def expired(signum, frame):
        if previous_delay > 0 and previous_delay <= seconds:
            raise_previous(signum, frame)
        if terminal:
            raise BenchmarkBudgetExceeded("benchmark total deadline exceeded")
        raise TimeoutError("trial deadline exceeded")

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, min(seconds, previous_delay) if previous_delay > 0 else seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        if previous_delay > 0:
            remaining = previous_delay - (time.monotonic() - started)
            if remaining > 0:
                signal.setitimer(signal.ITIMER_REAL, remaining, previous_interval)
            elif not isinstance(sys.exception(), BenchmarkBudgetExceeded):
                # An ordinary inner error must not swallow an outer deadline
                # that elapsed while the inner context was unwinding.
                raise_previous(signal.SIGALRM, None)


def _run_trial(
    root: Path, frozen: dict[str, object], code_hashes: dict[str, str],
    *, policy: str, timeout_seconds: float,
) -> dict[str, object]:
    started = time.monotonic()
    deadline = started + timeout_seconds
    groups = scan._scan_request_groups(
        DATASET, tuple(frozen["paths"]), full_objects_scan=False, batch_object_paths=policy == "batch",
    )
    requests = []
    result: dict[str, object] = {"policy": policy, "state": "running", "requests": requests,
                                "expected_post_count": len(groups)}
    original_urlopen = scan.urllib.request.urlopen

    def observed_urlopen(request, *, timeout):
        # Abort before another request if a live publisher queues or moves head.
        _no_pending(root)
        if _digest(_safe_path(root, HEAD_PATH).read_bytes()) != frozen["head_sha256"]:
            raise Contaminated("head changed during notification")
        parsed = urllib.parse.urlsplit(request.full_url)
        query = urllib.parse.parse_qs(parsed.query)
        index = len(requests)
        if (
            request.get_method() != "POST" or parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1" or parsed.path != "/rest/db/scan"
            or parsed.username is not None or parsed.password is not None or parsed.fragment
            or set(query) != {"folder", "sub"} or query["folder"] != [scan.FOLDER_ID]
            or index >= len(groups) or tuple(query["sub"]) != groups[index]
        ):
            raise snapshots.SnapshotError("canonical request differs from the frozen ordered plan")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("trial deadline exceeded")
        row = {"index": index + 1, "phase": "objects" if index < len(groups) - 2 else
               "manifest" if index == len(groups) - 2 else "head", "paths": list(groups[index]),
               "status": None, "outcome": "failed"}
        requests.append(row)
        request_started = time.monotonic()
        try:
            response = original_urlopen(request, timeout=min(timeout, remaining))
            row["status"] = response.status if type(response.status) is int else None
            row["outcome"] = "acknowledged" if row["status"] == 200 else "failed"
            return response
        except Exception as exc:
            code = getattr(exc, "code", None)
            row["status"] = code if type(code) is int else None
            row["error_type"] = type(exc).__name__  # Never serialize headers or exception text.
            raise
        finally:
            row["elapsed_seconds"] = round(time.monotonic() - request_started, 6)

    try:
        with _hard_deadline(timeout_seconds):
            _assert_frozen(root, frozen, code_hashes)
            with patch.object(scan.urllib.request, "urlopen", observed_urlopen):
                scan._scan_pending(root, DATASET, tuple(frozen["paths"]), full_objects_scan=False,
                                   batch_object_paths=policy == "batch")
            _assert_frozen(root, frozen, code_hashes)
            if len(requests) != len(groups) or any(row["outcome"] != "acknowledged" for row in requests):
                raise snapshots.SnapshotError("incomplete request evidence")
        result["state"] = "completed"
        result["input_metadata_unchanged"] = True
    except Exception as exc:
        result.update(state="contaminated" if isinstance(exc, Contaminated) else "failed",
                      error_type=type(exc).__name__)
        # A failed request does not excuse source/code validation. Do not clear
        # a concurrent producer's pending receipt under any outcome.
        try:
            _assert_frozen(root, frozen, code_hashes)
        except Exception as changed:
            result.update(state="contaminated", contamination_type=type(changed).__name__)
    result.update(
        elapsed_seconds=round(time.monotonic() - started, 6), observed_post_count=len(requests),
        measured_request_seconds=round(sum(row["elapsed_seconds"] for row in requests), 6),
    )
    return result


def _total_budget_seconds(trial_seconds: float) -> float:
    return len(SEQUENCE) * trial_seconds + 30.0


def benchmark(
    *, head_sha256: str, object_limit: int = 18, execute: bool = False,
    timeout_seconds: float = 90, receipt_path: Path,
) -> dict[str, object]:
    if not re.fullmatch(r"[0-9a-f]{64}", head_sha256):
        raise ValueError("head_sha256 must be a lowercase SHA256 digest")
    if type(execute) is not bool:
        raise ValueError("execute must be an explicit boolean")
    if type(object_limit) is not int or not 1 <= object_limit <= 18:
        raise ValueError("object_limit must be 1..18")
    if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or not 1 <= timeout_seconds <= 120:
        raise ValueError("timeout_seconds must be finite and within 1..120")
    output = receipt_path.resolve()
    if receipt_path.is_symlink() or output.exists() or output.is_relative_to(SYNC_ROOT):
        raise ValueError("receipt must be a new path outside the cold store")
    if output.is_relative_to((REPO_ROOT / "data_bybit").resolve()):
        raise ValueError("receipt must not enter the mutable source")
    receipt: dict[str, object] = {
        "schema_version": 1, "started_at_utc": datetime.now(UTC).isoformat(),
        "dataset": DATASET, "head_sha256": head_sha256, "execute_requested": execute,
        "scope": "warm_repeated_existing_object_notification_only",
        "trial_elapsed_scope": "canonical_scan_plus_safety_checks_and_request_observer",
        "request_elapsed_scope": "urlopen_until_response_headers",
        "publication_performed": False, "queue_overhead_measured": False,
        "payload_byte_verification": "not_performed", "release_verification": "not_performed",
        "peer_convergence": "not_checked", "sequence": list(SEQUENCE),
        "trial_timeout_seconds": timeout_seconds, "trials": [], "state": "planning",
        "global_budget_seconds": _total_budget_seconds(timeout_seconds),
    }

    def save():
        snapshots.atomic_write_json(output, receipt)

    try:
        with _hard_deadline(receipt["global_budget_seconds"], terminal=True):
            _benchmark_with_deadline(receipt, head_sha256, object_limit, execute, timeout_seconds, save)
    except (Exception, BenchmarkBudgetExceeded) as exc:
        receipt.update(state="timed_out" if isinstance(exc, BenchmarkBudgetExceeded) else
                       "contaminated" if isinstance(exc, Contaminated) else "refused",
                       error_type=type(exc).__name__)
        receipt.pop("median_trial_seconds", None)
        receipt.pop("median_request_ack_seconds", None)
    receipt["finished_at_utc"] = datetime.now(UTC).isoformat()
    save()
    return receipt


def _benchmark_with_deadline(receipt, head_sha256, object_limit, execute, timeout_seconds, save) -> None:
    """Every cold-store operation, lock and in-run receipt write is budgeted."""

    code_hashes = _code_hashes()
    frozen = _freeze_release(SYNC_ROOT, head_sha256, object_limit)
    receipt.update(release=frozen, code_sha256_before=code_hashes,
                   expected_post_counts={policy: len(scan._scan_request_groups(
                       DATASET, tuple(frozen["paths"]), full_objects_scan=False,
                       batch_object_paths=policy == "batch")) for policy in ("single", "batch")})
    _assert_frozen(SYNC_ROOT, frozen, code_hashes)
    if not execute:
        receipt["state"] = "dry_run"
    else:
        with _scan_lock(SYNC_ROOT):
            _assert_frozen(SYNC_ROOT, frozen, code_hashes)
            receipt["state"] = "running"
            save()
            for policy in SEQUENCE:
                trial = _run_trial(SYNC_ROOT, frozen, code_hashes, policy=policy, timeout_seconds=timeout_seconds)
                receipt["trials"].append(trial)
                save()
                if trial["state"] != "completed":
                    receipt["state"] = trial["state"]
                    break
            else:
                _assert_frozen(SYNC_ROOT, frozen, code_hashes)
                receipt["state"] = "completed"
                receipt["median_trial_seconds"] = {
                    policy: statistics.median(row["elapsed_seconds"] for row in receipt["trials"] if row["policy"] == policy)
                    for policy in ("single", "batch")
                }
                receipt["median_request_ack_seconds"] = {
                    policy: statistics.median(row["measured_request_seconds"] for row in receipt["trials"] if row["policy"] == policy)
                    for policy in ("single", "batch")
                }
    receipt["code_sha256_after"] = _code_hashes()
    if receipt["code_sha256_after"] != code_hashes:
        raise Contaminated("implementation changed at final observation")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--head-sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True, help="New audit receipt outside source/cold storage.")
    parser.add_argument("--objects", type=int, default=18, help="First 1..18 manifest objects plus inventory.")
    parser.add_argument("--trial-timeout", type=float, default=90)
    parser.add_argument("--execute", action="store_true", help="Actually send four sequential ABBA scan trials.")
    args = parser.parse_args(argv)
    try:
        result = benchmark(head_sha256=args.head_sha256, object_limit=args.objects, execute=args.execute,
                           timeout_seconds=args.trial_timeout, receipt_path=args.receipt)
    except (OSError, ValueError) as exc:
        print(json.dumps({"state": "refused", "error_type": type(exc).__name__}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["state"] in {"dry_run", "completed"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
