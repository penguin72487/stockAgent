#!/usr/bin/env python3
"""Publish a sanitized systemd snapshot for the hardened public dashboard."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.data_monitor_dashboard import (  # noqa: E402
    _refresh_service_states,
    build_data_monitor_feature_inventory,
    build_data_monitor_public_status,
    project_data_monitor_summary,
)
from stockagent.live.dashboard_updates import metadata_signature  # noqa: E402
from stockagent.live.data_monitor_inventory import (  # noqa: E402
    InventorySnapshot,
    build_feature_inventory,
    build_record_inventory,
)
from stockagent.live.data_monitor_feature_receipt import (  # noqa: E402
    feature_reuse_checksum as _feature_reuse_checksum,
    feature_reuse_receipt_path as _feature_reuse_receipt_path,
    feature_source_signature as _feature_source_signature,
)
from stockagent.live.shioaji_api_dashboard import build_shioaji_public_status  # noqa: E402


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
    path: Path, payload: dict[str, object], *, compact: bool = False,
    strict_json: bool = False,
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
                allow_nan=not strict_json,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_public_summary_snapshot(
    status_path: Path, payload: dict[str, object],
) -> Path:
    """Bind the small first-paint payload to exactly one full status generation."""

    signature = metadata_signature(status_path.stat())
    digest = hashlib.sha256()
    with status_path.open("rb") as stream:
        if metadata_signature(os.fstat(stream.fileno())) != signature:
            raise ValueError("data-monitor status changed before summary read")
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
        if metadata_signature(os.fstat(stream.fileno())) != signature:
            raise ValueError("data-monitor status changed during summary read")
    if metadata_signature(status_path.stat()) != signature:
        raise ValueError("data-monitor status changed after summary read")
    output = status_path.with_name("public_summary.json")
    _atomic_json(
        output,
        {
            "schema_version": 1,
            "read_only": True,
            "production_control_possible": False,
            "source_signature": list(signature),
            "source_sha256": digest.hexdigest(),
            "projection": project_data_monitor_summary(payload),
        },
        compact=True,
        strict_json=True,
    )
    return output


def _stable_feature_digest(path: Path, expected: list[int]) -> str | None:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            if _feature_source_signature(os.fstat(stream.fileno())) != expected:
                return None
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            if _feature_source_signature(os.fstat(stream.fileno())) != expected:
                return None
        if _feature_source_signature(path.stat()) != expected:
            return None
        return digest.hexdigest()
    except OSError:
        return None


def _write_feature_reuse_receipt(
    path: Path, fields: int, *, validated_contract: bool = False,
    feature_revision: str | None = None,
) -> None:
    """Publish a compact proof only after the complete snapshot is stable."""
    try:
        if not validated_contract:
            def reject_nonfinite(value: str) -> None:
                raise ValueError(f"non-finite feature JSON: {value}")

            payload = json.loads(path.read_bytes(), parse_constant=reject_nonfinite)
            if (
                not isinstance(payload, dict)
                or payload.get("schema_version") != 1
                or payload.get("read_only") is not True
                or payload.get("production_control_possible") is not False
                or not isinstance(payload.get("rows"), list)
                or len(payload["rows"]) != fields
            ):
                return
        signature = _feature_source_signature(path.stat())
        digest = _stable_feature_digest(path, signature)
        if digest is not None:
            revision_binding = (
                hashlib.sha256(json.dumps(
                    [signature, digest, fields, feature_revision],
                    separators=(",", ":"),
                ).encode("ascii")).hexdigest()
                if isinstance(feature_revision, str) and len(feature_revision) == 32
                else None
            )
            _atomic_json(
                _feature_reuse_receipt_path(path),
                {
                    "schema_version": 3,
                    "read_only": True,
                    "production_control_possible": False,
                    "source_signature": signature,
                    "source_sha256": digest,
                    "fields": fields,
                    "receipt_sha256": _feature_reuse_checksum(
                        signature, digest, fields
                    ),
                    "feature_revision": feature_revision if revision_binding else None,
                    "feature_revision_sha256": revision_binding,
                },
                compact=True,
            )
    except (OSError, ValueError, UnicodeError):
        # This is an optional acceleration receipt. The next run will validate
        # and parse the full source rather than weaken the publication gate.
        pass


def _current_feature_snapshot(
    path: Path, *, feature_revision: str | None = None,
) -> int | None:
    """Reuse field count only after source and dependency proof is current."""

    dependencies = (
        REPO_ROOT / "artifacts/live/data_monitor/record_inventory_cache.json",
        REPO_ROOT / "stockagent/live/data_monitor_inventory.py",
        REPO_ROOT / "stockagent/live/data_monitor_dashboard.py",
        REPO_ROOT / "stockagent/live/data_monitor_feature_receipt.py",
        REPO_ROOT / "configs/data_sync/packed_datasets.json",
        REPO_ROOT / "data_tw_public/dataset_manifest.json",
        Path(__file__),
    )

    def dependencies_current(snapshot_mtime: int, *, revision_bound: bool) -> bool:
        return not any(
            dependency.stat().st_mtime_ns > snapshot_mtime
            for dependency in (dependencies[1:] if revision_bound else dependencies)
            if dependency.exists()
        )

    try:
        signature = _feature_source_signature(path.stat())
        snapshot_mtime = signature[3]
        receipt = json.loads(_feature_reuse_receipt_path(path).read_bytes())
        revision_bound = (
            isinstance(receipt, dict)
            and isinstance(feature_revision, str)
            and len(feature_revision) == 32
            and receipt.get("feature_revision") == feature_revision
            and receipt.get("feature_revision_sha256") == hashlib.sha256(
                json.dumps(
                    [signature, receipt.get("source_sha256"), receipt.get("fields"), feature_revision],
                    separators=(",", ":"),
                ).encode("ascii")
            ).hexdigest()
        )
        if (
            isinstance(receipt, dict)
            and isinstance(feature_revision, str)
            and receipt.get("feature_revision") is not None
            and not revision_bound
        ):
            return None
        if not dependencies_current(snapshot_mtime, revision_bound=revision_bound):
            return None
        if (
            isinstance(receipt, dict)
            and receipt.get("schema_version") == 3
            and receipt.get("read_only") is True
            and receipt.get("production_control_possible") is False
            and receipt.get("source_signature") == signature
            and isinstance(receipt.get("fields"), int)
            and not isinstance(receipt["fields"], bool)
            and 0 <= receipt["fields"] <= 10_000_000
            and isinstance(receipt.get("source_sha256"), str)
            and len(receipt["source_sha256"]) == 64
            and receipt.get("receipt_sha256") == _feature_reuse_checksum(
                signature, receipt["source_sha256"], receipt["fields"]
            )
            and _stable_feature_digest(path, signature) == receipt["source_sha256"]
            and dependencies_current(snapshot_mtime, revision_bound=revision_bound)
        ):
            return receipt["fields"]
    except (OSError, ValueError, UnicodeError):
        pass
    try:
        signature = _feature_source_signature(path.stat())
        with path.open("rb") as stream:
            if _feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
            encoded = stream.read()
            if _feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
        if _feature_source_signature(path.stat()) != signature:
            return None
        if not dependencies_current(signature[3], revision_bound=False):
            return None
        def reject_nonfinite(value: str) -> None:
            raise ValueError(f"non-finite feature JSON: {value}")

        payload = json.loads(encoded, parse_constant=reject_nonfinite)
        if (
            isinstance(payload, dict)
            and payload.get("schema_version") == 1
            and payload.get("read_only") is True
            and payload.get("production_control_possible") is False
            and isinstance(payload.get("rows"), list)
        ):
            fields = len(payload["rows"])
            _write_feature_reuse_receipt(
                path, fields, validated_contract=True,
                feature_revision=feature_revision,
            )
            return fields
    except (OSError, ValueError, UnicodeError):
        pass
    return None


def main() -> int:
    started = time.perf_counter()
    cpu_started = time.process_time()
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
    services_elapsed = time.perf_counter() - started
    inventory_snapshot = InventorySnapshot(REPO_ROOT)
    record_inventory = build_record_inventory(
        REPO_ROOT, refresh=True, snapshot=inventory_snapshot
    )
    inventory_elapsed = time.perf_counter() - started - services_elapsed
    public_status_timing_ms: dict[str, float] = {}
    shioaji_started = time.perf_counter()
    shioaji_timing_ms: dict[str, float] = {}
    shioaji_status = build_shioaji_public_status(
        REPO_ROOT, now=observed, timing_ms=shioaji_timing_ms,
    )
    public_status_timing_ms["shioaji_build"] = round(
        (time.perf_counter() - shioaji_started) * 1_000, 3
    )
    public_status_timing_ms.update({
        f"shioaji.{key}": value for key, value in shioaji_timing_ms.items()
    })
    public_status = build_data_monitor_public_status(
        REPO_ROOT,
        now=observed,
        refresh_services=services,
        shioaji_status=shioaji_status,
        record_inventory=record_inventory,
        timing_ms=public_status_timing_ms,
    )
    # This snapshot is rebuilt and read every 30 seconds.  Compact encoding
    # lowers both atomic-write traffic and the request-path read without
    # changing any public fields; the small service-state receipt remains
    # indented for operator inspection.
    _atomic_json(public_status_output, public_status, compact=True)
    try:
        if (
            shioaji_status.get("read_only") is not True
            or shioaji_status.get("simulation_only") is not True
            or shioaji_status.get("production_order_possible") is not False
        ):
            raise ValueError("Shioaji status violates read-only public contract")
        _atomic_json(
            public_status_output.with_name("shioaji_status.json"),
            shioaji_status, compact=True, strict_json=True,
        )
    except (OSError, ValueError) as exc:
        # The gateway retains its bounded direct-read fallback if projection
        # publication fails. Do not turn this optional projection into a
        # false claim that the source acquisition itself failed.
        print(json.dumps({
            "event": "shioaji_status_projection_failed",
            "error": type(exc).__name__,
        }), flush=True)
    try:
        _write_public_summary_snapshot(public_status_output, public_status)
    except (OSError, ValueError) as exc:
        # The full status remains authoritative. The gateway falls back to it
        # if this optional latency projection is missing or mismatched.
        print(json.dumps({
            "event": "data_monitor_summary_projection_failed",
            "error": type(exc).__name__,
        }), flush=True)
    public_elapsed = time.perf_counter() - started - services_elapsed
    feature_revision = record_inventory.get("feature_revision")
    feature_count = _current_feature_snapshot(
        feature_inventory_output, feature_revision=feature_revision,
    )
    feature_reused = feature_count is not None
    feature_inventory_timing_ms: dict[str, float] = {}
    if feature_count is None:
        feature_inventory = build_data_monitor_feature_inventory(
            REPO_ROOT, monitor_status=public_status,
            inventory=build_feature_inventory(
                REPO_ROOT, snapshot=inventory_snapshot,
                timing_ms=feature_inventory_timing_ms,
            ),
        )
        if (
            feature_inventory.get("schema_version") != 1
            or feature_inventory.get("read_only") is not True
            or feature_inventory.get("production_control_possible") is not False
            or not isinstance(feature_inventory.get("rows"), list)
        ):
            raise ValueError("data-monitor feature projection violates public contract")
        _atomic_json(
            feature_inventory_output, feature_inventory,
            compact=True, strict_json=True,
        )
        feature_count = len(feature_inventory.get("rows") or ())
        _write_feature_reuse_receipt(
            feature_inventory_output, feature_count, validated_contract=True,
            feature_revision=feature_revision,
        )
    feature_elapsed = time.perf_counter() - started - services_elapsed - public_elapsed
    # Reuse this supervised, every-30-second worker instead of installing one
    # more polling daemon. The tracker checks its durable due time before doing
    # any systemd query, and measurement failure cannot invalidate data status.
    sample = None
    try:
        from scripts.track_service_runtime_trends import sample_service_runtime

        sample = sample_service_runtime(
            REPO_ROOT / "artifacts/live/data_monitor/all_service_runtime_baseline.json"
        )
    except Exception as exc:
        print(
            json.dumps({
                "event": "all_service_runtime_sample_failed",
                "error_type": type(exc).__name__,
            }),
            flush=True,
        )
    trend_elapsed = time.perf_counter() - started - services_elapsed - public_elapsed - feature_elapsed
    total_elapsed = time.perf_counter() - started
    # Journal persistence/rotation owns retention; no new unbounded log store.
    # These stages are disjoint. public_s below retains its older inclusive
    # meaning for existing operators, while this receipt supports comparisons.
    print(json.dumps({
        "event": "data_monitor_timing", "schema_version": 1,
        "observed_at_utc": observed.isoformat(),
        "total_ms": round(total_elapsed * 1_000, 3),
        "cpu_ms": round((time.process_time() - cpu_started) * 1_000, 3),
        "cpu_clock": "process_time_excludes_subprocesses",
        "stages_ms": {
            "services": round(services_elapsed * 1_000, 3),
            "inventory": round(inventory_elapsed * 1_000, 3),
            "public_projection": round((public_elapsed - inventory_elapsed) * 1_000, 3),
            "feature_projection": round(feature_elapsed * 1_000, 3),
            "service_trend": round(trend_elapsed * 1_000, 3),
        },
        "inventory_stages_ms": record_inventory.get("timing_ms", {}),
        "inventory_fast_index_hit": bool(record_inventory.get("fast_index_hit")),
        "public_projection_stages_ms": public_status_timing_ms,
        "feature_inventory_stages_ms": feature_inventory_timing_ms,
        "feature_reused": feature_reused,
        "sources": len(public_status.get("sources") or ()),
        "features": feature_count,
        "cached_files": record_inventory["cached_files"],
        "refreshed_files": record_inventory["refreshed_files"],
        "identity_rechecked_files": record_inventory["identity_rechecked_files"],
        "identity_unbound_files": record_inventory["identity_unbound_files"],
    }, separators=(",", ":")), flush=True)
    print(
        f"[data-refresh-status] services={len(services)} output={output} "
        f"public_status={public_status_output} "
        f"sources={len(public_status.get('sources') or ())}",
        f"features={feature_count}",
        f"elapsed_s={total_elapsed:.3f} services_s={services_elapsed:.3f} "
        f"public_s={public_elapsed:.3f} "
        f"inventory_s={inventory_elapsed:.3f} "
        f"feature_s={feature_elapsed:.3f} trend_s={trend_elapsed:.3f}",
        flush=True,
    )
    if sample is not None:
        print(json.dumps(sample, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
