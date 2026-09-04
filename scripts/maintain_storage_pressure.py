#!/usr/bin/env python3
"""Prune only old rebuildable compiler caches when disk usage is too high."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import socket
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import atomic_write_json  # noqa: E402
from stockagent.data_sync.storage_pressure import (  # noqa: E402
    DEFAULT_PROTECTED_PROCESS_SUBSTRINGS,
    maintain_rebuildable_caches,
)


def _cache_home() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))


def _default_roots(cache_home: Path) -> list[Path]:
    return [
        Path(os.environ.get("TORCHINDUCTOR_CACHE_DIR", cache_home / "torchinductor")),
        Path(os.environ.get("TRITON_CACHE_DIR", cache_home / "triton")),
        Path(os.environ.get("CUDA_CACHE_PATH", cache_home / "nv_cuda")),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", action="append", type=Path)
    parser.add_argument("--min-age-days", type=float, default=14.0)
    parser.add_argument("--high-watermark-percent", type=float, default=95.0)
    parser.add_argument("--target-percent", type=float, default=92.0)
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
    cache_home = _cache_home().expanduser().resolve()
    roots = args.cache_root or _default_roots(cache_home)
    args.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with args.lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Storage-pressure maintenance is already running.")
            return 0
        try:
            result = maintain_rebuildable_caches(
                roots,
                allowed_root=cache_home,
                min_age_days=args.min_age_days,
                high_watermark_percent=args.high_watermark_percent,
                target_percent=args.target_percent,
                apply=args.apply,
                force=args.force,
                protected_process_substrings=(
                    args.protected_process_substring
                    or DEFAULT_PROTECTED_PROCESS_SUBSTRINGS
                ),
            )
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
    completed = datetime.now(UTC)
    result.update(
        {
            "host": socket.gethostname(),
            "completed_at": completed.isoformat(),
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
