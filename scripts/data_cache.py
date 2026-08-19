#!/usr/bin/env python3
"""Self-service cold storage and seven-day materialized dataset cache."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402
from stockagent.data_sync.materialized_cache import (  # noqa: E402
    DEFAULT_CACHE_TTL_DAYS,
    evict_materialized_snapshots,
    materialized_cache_status,
    use_materialized_snapshot,
)


def _env_path(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default)).expanduser()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sync-root",
        type=Path,
        default=_env_path("STOCKAGENT_PACKED_SYNC_ROOT", "/srv/stockagent-packed"),
    )
    parser.add_argument(
        "--materialized-root",
        type=Path,
        default=_env_path(
            "STOCKAGENT_MATERIALIZED_ROOT",
            "/srv/stockagent-packed-materialized",
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="show cold and hot state")
    status.add_argument("dataset", nargs="?")

    use = subparsers.add_parser("use", help="materialize and renew a hot lease")
    use.add_argument("dataset")
    use.add_argument("--snapshot-id")
    use.add_argument(
        "--ttl-days",
        type=float,
        default=float(
            os.environ.get("STOCKAGENT_DATA_CACHE_TTL_DAYS", DEFAULT_CACHE_TTL_DAYS)
        ),
    )
    use.add_argument(
        "--link",
        type=Path,
        action="append",
        default=[],
        help="also atomically point this symlink at the verified hot tree",
    )
    use.add_argument(
        "--verify",
        action="store_true",
        help="rehash an already-ready hot tree instead of trusting its ready proof",
    )
    use.add_argument(
        "--path-only",
        action="store_true",
        help="print only the materialized path for shell command substitution",
    )

    gc = subparsers.add_parser("gc", help="evict every expired safe hot lease")
    gc.add_argument("--dry-run", action="store_true")

    evict = subparsers.add_parser(
        "evict", help="immediately evict one safe, unpinned hot dataset"
    )
    evict.add_argument("dataset")
    evict.add_argument("--snapshot-id")
    evict.add_argument("--dry-run", action="store_true")
    return parser


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "status":
            _print(
                materialized_cache_status(
                    args.sync_root,
                    args.materialized_root,
                    dataset=args.dataset,
                )
            )
            return 0
        if args.command == "use":
            lease = use_materialized_snapshot(
                args.sync_root,
                args.materialized_root,
                args.dataset,
                snapshot_id=args.snapshot_id,
                ttl_days=args.ttl_days,
                links=args.link,
                verify_existing=args.verify,
            )
            if args.path_only:
                print(lease["target"])
            else:
                _print(lease)
            return 0
        if args.command == "gc":
            _print(
                evict_materialized_snapshots(
                    args.sync_root,
                    args.materialized_root,
                    dry_run=args.dry_run,
                )
            )
            return 0
        if args.command == "evict":
            _print(
                evict_materialized_snapshots(
                    args.sync_root,
                    args.materialized_root,
                    dataset=args.dataset,
                    snapshot_id=args.snapshot_id,
                    force=True,
                    dry_run=args.dry_run,
                )
            )
            return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
