#!/usr/bin/env python3
"""Prune only old rebuildable compiler caches when disk usage is too high."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import socket
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import atomic_write_bytes, atomic_write_json  # noqa: E402
from stockagent.data_sync.storage_pressure import (  # noqa: E402
    DEFAULT_PROTECTED_PROCESS_SUBSTRINGS,
    maintain_rebuildable_caches,
    validate_cache_roots,
)

TMP_COMPILER_CACHE_ROOT = Path("/tmp/torchinductor_root")
LOCAL_POLICY_PATH = Path("/etc/stockagent/storage-pressure.json")
DEFAULT_OPTIONS = {"min_age_days": 14.0, "high_watermark_percent": 89.0, "target_percent": 88.0}


def _cache_home() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))


def _default_roots(cache_home: Path) -> list[Path]:
    return [
        Path(os.environ.get("TORCHINDUCTOR_CACHE_DIR", cache_home / "torchinductor")),
        Path(os.environ.get("TRITON_CACHE_DIR", cache_home / "triton")),
        Path(os.environ.get("CUDA_CACHE_PATH", cache_home / "nv_cuda")),
    ]


def _tmp_compiler_scope() -> tuple[Path, list[Path]]:
    if os.geteuid() != 0:
        raise ValueError("tmp compiler scope requires root")
    root = TMP_COMPILER_CACHE_ROOT
    parent = root.parent
    if parent.is_symlink() or parent.resolve() != parent:
        raise ValueError("temporary compiler parent is redirected")
    if root.is_symlink() or root.resolve(strict=False) != root:
        raise ValueError("temporary compiler cache is redirected")
    if root.exists():
        info = root.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != 0
                or info.st_dev != parent.stat().st_dev or os.path.ismount(root)):
            raise ValueError("temporary compiler cache is not a root-owned local directory")
    return parent, [root]


def maintenance_scope(args) -> tuple[Path, list[Path]]:
    """Keep the default scope, or opt in to one root-owned compiler cache.

    This does not allow arbitrary /tmp paths. The manual option does not itself
    enroll a timer; automatic enrollment requires a separate local policy.
    """
    if args.tmp_torchinductor_only:
        if args.cache_root or os.geteuid() != 0:
            raise ValueError("tmp compiler scope requires root and no custom cache roots")
        return _tmp_compiler_scope()
    cache_home = _cache_home().expanduser().resolve()
    return cache_home, args.cache_root or _default_roots(cache_home)


def load_local_policy(path: Path) -> tuple[dict, str]:
    """A locally enrolled fixed compiler scope, never synced deletion rules."""
    if os.geteuid() != 0 or path.is_symlink() or path.resolve() != path:
        raise ValueError("compiler policy requires root and a non-redirected absolute path")
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_uid != 0
            or before.st_mode & 0o022 or before.st_size > 4096):
        raise ValueError("compiler policy must be a bounded root-owned non-writable-by-others file")
    raw = path.read_bytes()
    after = path.lstat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError("compiler policy changed during read")
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate compiler policy field")
            result[key] = value
        return result
    policy = json.loads(raw, object_pairs_hook=unique_object)
    if (not isinstance(policy, dict) or set(policy) != {"schema_version", "scope", *DEFAULT_OPTIONS}
            or type(policy["schema_version"]) is not int or policy["schema_version"] != 1
            or policy["scope"] != "compiler-home-and-root-tmp"):
        raise ValueError("unsupported compiler policy schema or scope")
    if any(type(policy[name]) not in (int, float) or not math.isfinite(policy[name])
           for name in DEFAULT_OPTIONS):
        raise ValueError("compiler policy values must be finite numbers")
    if policy["min_age_days"] < 14 or not (
            0 < policy["target_percent"] < policy["high_watermark_percent"] < 100):
        raise ValueError("automatic compiler policy requires at least 14 days and valid watermarks")
    return policy, hashlib.sha256(raw).hexdigest()


def prepare_scopes(args) -> tuple[list[tuple[str, Path, list[Path]]], str | None]:
    digest = None
    if args.policy:
        if args.force or args.cache_root or args.tmp_torchinductor_only or args.protected_process_substring:
            raise ValueError("automatic compiler policy forbids manual roots, force and process overrides")
        policy, digest = load_local_policy(args.policy)
        for name in DEFAULT_OPTIONS:
            requested = getattr(args, name)
            if requested is not None and requested != policy[name]:
                raise ValueError("compiler policy cannot be weakened by command-line overrides")
            setattr(args, name, policy[name])
        home, roots = maintenance_scope(args)
        raw_home = _cache_home().expanduser()
        if raw_home.resolve() != raw_home or raw_home.is_symlink():
            raise ValueError("automatic compiler cache home must not be redirected")
        fixed_roots = {home / name for name in ("torchinductor", "triton", "nv_cuda")}
        if any(root.expanduser() not in fixed_roots for root in roots):
            raise ValueError("automatic compiler policy forbids redirected environment cache roots")
        if home.exists():
            info = home.stat()
            if (info.st_uid != 0 or info.st_mode & 0o002
                    or info.st_mode & 0o020 and info.st_gid != 0):
                raise ValueError("automatic compiler cache home must be root-controlled")
        parent, temporary = _tmp_compiler_scope()
        if not home.is_dir() or home.stat().st_dev != parent.stat().st_dev:
            raise ValueError("combined compiler policy requires one local filesystem")
        scopes = [("cache-home", home, roots), ("tmp-torchinductor-only", parent, temporary)]
    else:
        for name, value in DEFAULT_OPTIONS.items():
            if getattr(args, name) is None:
                setattr(args, name, value)
        home, roots = maintenance_scope(args)
        scopes = [("tmp-torchinductor-only" if args.tmp_torchinductor_only else "cache-home", home, roots)]
    # Validate every scope before cleaning the first; never use '/' as a broad
    # allowlist simply to combine two separately bounded compiler directories.
    seen = []
    for _label, allowed, roots in scopes:
        for root in validate_cache_roots(roots, allowed_root=allowed):
            if args.policy and root.exists() and (
                    root.stat().st_dev != home.stat().st_dev or os.path.ismount(root)):
                raise ValueError("automatic compiler roots must remain on one local filesystem")
            if any(root == other or root.is_relative_to(other) or other.is_relative_to(root) for other in seen):
                raise ValueError("compiler policy scopes overlap")
            seen.append(root)
    return scopes, digest


def enroll_policy(source: Path, expected_digest: str) -> dict:
    _policy, digest = load_local_policy(source)
    if digest != expected_digest:
        raise ValueError("compiler policy changed before local enrollment")
    target = LOCAL_POLICY_PATH
    if target.is_symlink() or target.resolve(strict=False) != target:
        raise ValueError("local compiler policy destination is redirected")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = target.parent.stat()
    if info.st_uid != 0 or info.st_mode & 0o022:
        raise ValueError("local compiler policy directory is not root-controlled")
    created = not target.exists()
    if not created:
        _current, current_digest = load_local_policy(target)
        if current_digest != digest:
            raise ValueError("preserve a different existing local compiler policy")
    else:
        raw = source.read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("compiler policy changed before enrollment commit")
        atomic_write_bytes(target, raw, mode=0o600)
    _current, committed = load_local_policy(target)
    if committed != digest:
        raise ValueError("committed compiler policy differs from enrollment")
    return {"state": "local_compiler_policy_enrolled", "policy_path": str(target),
            "policy_sha256": digest, "created": created, "second_scheduler_created": False,
            "cache_files_deleted_by_enrollment": 0}


def combine_results(results: list[dict]) -> dict:
    if len(results) == 1:
        return results[0]
    result = {"schema_version": 1, "apply": results[0]["apply"],
              "policy": {"scope": "compiler-home-and-root-tmp", "groups": [item["policy"] for item in results]},
              "scope_results": results, "filesystem_before": results[0]["filesystem_before"],
              "filesystem_after": results[-1]["filesystem_after"],
              "under_pressure": any(item["under_pressure"] for item in results),
              "inventory_complete": all(item["inventory_complete"] for item in results),
              "scan_skipped_reason": next((item["scan_skipped_reason"] for item in results if item["scan_skipped_reason"]), None),
              "deferred_reason": next((item["deferred_reason"] for item in results if item["deferred_reason"]), None),
              "protected_processes": [process for item in results for process in item["protected_processes"]],
              "errors": [error for item in results for error in item["errors"]],
              "per_root": {root: values for item in results for root, values in item["per_root"].items()},
              "selected_fingerprint_sha256": hashlib.sha256("\n".join(item["selected_fingerprint_sha256"] for item in results).encode()).hexdigest()}
    for name in ("eligible_files", "eligible_allocated_bytes", "selected_files", "selected_allocated_bytes",
                 "deleted_files", "deleted_allocated_bytes", "skipped_changed", "skipped_open", "skipped_protected_start"):
        result[name] = sum(item[name] for item in results)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", action="append", type=Path)
    parser.add_argument(
        "--tmp-torchinductor-only", action="store_true",
        help="manually audit/prune only root-owned /tmp/torchinductor_root, never all /tmp",
    )
    parser.add_argument("--policy", type=Path, help="explicit locally enrolled automatic compiler scope policy")
    enrollment = parser.add_mutually_exclusive_group()
    enrollment.add_argument("--check-policy", action="store_true", help="validate the fixed local policy/scope without scanning or deleting cache")
    enrollment.add_argument("--enroll-policy", action="store_true", help="enroll a validated fixed policy for the existing timer/cron, without deleting cache")
    parser.add_argument("--min-age-days", type=float)
    parser.add_argument("--high-watermark-percent", type=float)
    parser.add_argument("--target-percent", type=float)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--protected-process-substring",
        action="append",
        help=(
            "defer automatic cleanup while a matching command is active; "
            "repeat to replace the built-in training/compiler patterns"
        ),
    )
    parser.add_argument(
        "--receipt-dir",
        type=Path,
        default=Path("/var/lib/stockagent-storage-pressure/receipts"),
    )
    parser.add_argument(
        "--lock-path",
        type=Path,
        default=Path("/run/lock/stockagent-storage-pressure.lock"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if (args.check_policy or args.enroll_policy) and (not args.policy or args.apply):
            raise ValueError("policy check/enrollment requires --policy and cannot apply cleanup")
        scopes, policy_sha256 = prepare_scopes(args)
        if args.check_policy:
            print(json.dumps({"state": "policy-valid", "policy_sha256": policy_sha256,
                              "scopes": [{"name": label, "allowed_root": str(allowed),
                                          "cache_roots": [str(root) for root in roots]}
                                         for label, allowed, roots in scopes]}))
            return 0
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    args.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with args.lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Storage-pressure maintenance is already running.")
            return 75 if args.enroll_policy else 0
        try:
            if args.enroll_policy:
                enrolled = enroll_policy(args.policy, policy_sha256)
                args.receipt_dir.mkdir(parents=True, exist_ok=True)
                receipt = args.receipt_dir / f"storage-pressure-enrollment-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S.%fZ')}.json"
                atomic_write_json(receipt, enrolled)
                print(json.dumps({**enrolled, "receipt": str(receipt)}))
                return 0
            results = [maintain_rebuildable_caches(
                roots,
                allowed_root=allowed,
                min_age_days=args.min_age_days,
                high_watermark_percent=args.high_watermark_percent,
                target_percent=args.target_percent,
                apply=args.apply,
                force=args.force,
                protected_process_substrings=(
                    args.protected_process_substring
                    or DEFAULT_PROTECTED_PROCESS_SUBSTRINGS
                ),
            ) for _label, allowed, roots in scopes]
            result = combine_results(results)
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
    completed = datetime.now(UTC)
    result.update(
        {
            "host": socket.gethostname(),
            "completed_at": completed.isoformat(),
            "cache_scope": "compiler-home-and-root-tmp" if args.policy else scopes[0][0],
            "enrolled_policy_sha256": policy_sha256,
        }
    )
    args.receipt_dir.mkdir(parents=True, exist_ok=True)
    mode = "apply" if args.apply else "audit"
    receipt = args.receipt_dir / (
        f"storage-pressure-{mode}-{completed.strftime('%Y%m%dT%H%M%S.%fZ')}.json"
    )
    atomic_write_json(receipt, result)
    print(
        json.dumps(
            {
                "receipt": str(receipt),
                "apply": result["apply"],
                "under_pressure": result["under_pressure"],
                "inventory_complete": result["inventory_complete"],
                "scan_skipped_reason": result["scan_skipped_reason"],
                "deferred_reason": result["deferred_reason"],
                "protected_processes": result["protected_processes"],
                "used_percent_before": result["filesystem_before"]["used_percent"],
                "used_percent_after": result["filesystem_after"]["used_percent"],
                "eligible_files": result["eligible_files"],
                "selected_files": result["selected_files"],
                "selected_allocated_bytes": result["selected_allocated_bytes"],
                "deleted_files": result["deleted_files"],
                "deleted_allocated_bytes": result["deleted_allocated_bytes"],
                "skipped_changed": result["skipped_changed"],
                "skipped_open": result["skipped_open"],
                "skipped_protected_start": result["skipped_protected_start"],
                "errors": result["errors"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not result["errors"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
