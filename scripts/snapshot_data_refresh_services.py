#!/usr/bin/env python3
"""Publish a sanitized systemd snapshot for the hardened public dashboard."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.data_monitor_dashboard import (  # noqa: E402
    _refresh_service_states,
    build_data_monitor_feature_inventory,
    build_data_monitor_public_status,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/live/data_monitor/refresh_services.json"),
    )
    parser.add_argument(
        "--public-status-output",
        type=Path,
        default=Path("artifacts/live/data_monitor/public_status.json"),
    )
    parser.add_argument(
        "--feature-inventory-output",
        type=Path,
        default=Path("artifacts/live/data_monitor/feature_inventory.json"),
    )
    return parser.parse_args()


def _atomic_json(
    path: Path, payload: dict[str, object], *, compact: bool = False
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=None if compact else 2,
                separators=(",", ":") if compact else None,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _current_feature_snapshot(path: Path) -> dict[str, object] | None:
    """Reuse field statistics until a physical footer or its definition changes."""

    dependencies = (
        REPO_ROOT / "artifacts/live/data_monitor/record_inventory_cache.json",
        REPO_ROOT / "stockagent/live/data_monitor_inventory.py",
        REPO_ROOT / "stockagent/live/data_monitor_dashboard.py",
        REPO_ROOT / "configs/data_sync/packed_datasets.json",
        REPO_ROOT / "data_tw_public/dataset_manifest.json",
        Path(__file__),
    )
    try:
        snapshot_mtime = path.stat().st_mtime_ns
        if any(dependency.stat().st_mtime_ns > snapshot_mtime for dependency in dependencies if dependency.exists()):
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            isinstance(payload, dict)
            and payload.get("schema_version") == 1
            and payload.get("read_only") is True
            and payload.get("production_control_possible") is False
            and isinstance(payload.get("rows"), list)
        ):
            return payload
    except (OSError, ValueError, UnicodeError):
        pass
    return None


def main() -> int:
    args = parse_args()
    output = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    public_status_output = (
        args.public_status_output
        if args.public_status_output.is_absolute()
        else REPO_ROOT / args.public_status_output
    )
    feature_inventory_output = (
        args.feature_inventory_output
        if args.feature_inventory_output.is_absolute()
        else REPO_ROOT / args.feature_inventory_output
    )
    observed = datetime.now(UTC)
    services = _refresh_service_states(now=observed)
    if not any(
        state.get("evidence_source") == "systemd_live"
        for state in services.values()
    ):
        print("[data-refresh-status] systemd properties unavailable", file=sys.stderr)
        return 1
    _atomic_json(
        output,
        {
            "schema_version": 1,
            "generated_at_utc": observed.isoformat(),
            "services": services,
        },
    )
    public_status = build_data_monitor_public_status(
        REPO_ROOT,
        now=observed,
        refresh_services=services,
        refresh_inventory=True,
    )
    # This snapshot is rebuilt and read every 30 seconds.  Compact encoding
    # lowers both atomic-write traffic and the request-path read without
    # changing any public fields; the small service-state receipt remains
    # indented for operator inspection.
    _atomic_json(public_status_output, public_status, compact=True)
    feature_inventory = _current_feature_snapshot(feature_inventory_output)
    if feature_inventory is None:
        feature_inventory = build_data_monitor_feature_inventory(
            REPO_ROOT, monitor_status=public_status
        )
        _atomic_json(feature_inventory_output, feature_inventory, compact=True)
    print(
        f"[data-refresh-status] services={len(services)} output={output} "
        f"public_status={public_status_output} "
        f"sources={len(public_status.get('sources') or ())}",
        f"features={len(feature_inventory.get('rows') or ())}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
