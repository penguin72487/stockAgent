#!/usr/bin/env python3
"""Publish a sanitized systemd snapshot for the hardened public dashboard."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
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

_SHARED_IMPORT_STARTED = time.perf_counter()

from stockagent.live.data_monitor_dashboard import (  # noqa: E402
    _refresh_service_states,
    build_data_monitor_feature_inventory,
    build_data_monitor_public_status,
    feature_source_metadata_sha256,
    project_data_monitor_summary,
)
from stockagent.live.dashboard_updates import metadata_signature  # noqa: E402
from stockagent.live.data_monitor_inventory import (  # noqa: E402
    InventorySnapshot,
    build_feature_inventory,
    build_record_inventory,
)
from stockagent.live.data_monitor_feature_receipt import (  # noqa: E402
    FEATURE_SOURCE_PAGES_MAX_BYTES,
    feature_observation_binding as _feature_observation_binding,
    feature_preview_checksum as _feature_preview_checksum,
    feature_revision_binding as _feature_revision_binding,
    feature_reuse_checksum as _feature_reuse_checksum,
    feature_reuse_receipt_path as _feature_reuse_receipt_path,
    feature_source_pages_path as _feature_source_pages_path,
    feature_source_signature as _feature_source_signature,
)
from stockagent.live.data_monitor_feature_pages import (  # noqa: E402
    feature_page_projections,
    valid_feature_page_preview,
    valid_feature_source_pages,
)
from stockagent.live.shioaji_api_dashboard import build_shioaji_public_status  # noqa: E402

_SHARED_IMPORT_OBSERVATION = {
    "schema_version": 1,
    "wall_ms": round((time.perf_counter() - _SHARED_IMPORT_STARTED) * 1_000, 3),
    "scope": "shared_module_imports_only_excludes_interpreter_and_stdlib_startup",
    "worker_modules_loaded_at_import_completion": {
        name: name in sys.modules for name in (
            "scripts.download_finlab_history",
            "downloader.download_finmind_complement",
            "downloader.download_finmind_sponsor",
        )
    },
}


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
    strict_json: bool = False, sort_keys: bool = True,
    max_bytes: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        encoded = (
            json.dumps(
                payload, ensure_ascii=False, indent=None if compact else 2,
                separators=(",", ":") if compact else None,
                sort_keys=sort_keys, allow_nan=not strict_json,
            ) + "\n"
        )
        if max_bytes is not None and len(encoded.encode("utf-8")) > max_bytes:
            raise ValueError("atomic JSON exceeds bounded projection size")
        temporary.write_text(encoded, encoding="utf-8")
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
    source_metadata_sha256: str | None = None,
    source_observation_root: str | None = None,
    preview: dict[str, object] | None = None,
    source_pages: dict[str, list[dict[str, object]]] | None = None,
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
            revision_binding = _feature_revision_binding(
                signature, digest, fields, feature_revision,
                source_metadata_sha256,
            )
            receipt: dict[str, object] = {
                "schema_version": 3,
                "read_only": True,
                "production_control_possible": False,
                "source_signature": signature,
                "source_sha256": digest,
                "fields": fields,
                "receipt_sha256": _feature_reuse_checksum(signature, digest, fields),
                "feature_revision": feature_revision if revision_binding else None,
                "feature_revision_sha256": revision_binding,
                "source_metadata_sha256": source_metadata_sha256,
            }
            observation_binding = _feature_observation_binding(
                signature, digest, fields, source_observation_root,
            )
            if observation_binding is not None:
                receipt["source_observation_root"] = source_observation_root
                receipt["source_observation_sha256"] = observation_binding
            if preview is not None and valid_feature_page_preview(preview, fields=fields):
                receipt["first_page_preview"] = preview
                receipt["first_page_preview_sha256"] = _feature_preview_checksum(
                    digest, preview,
                )
                if source_pages is not None and valid_feature_source_pages(
                    source_pages, fields=fields,
                ):
                    try:
                        pages_path = _feature_source_pages_path(path)
                        _atomic_json(
                            pages_path,
                            {
                                "schema_version": 1,
                                "read_only": True,
                                "production_control_possible": False,
                                "source_signature": signature,
                                "source_sha256": digest,
                                "fields": fields,
                                "pages": source_pages,
                            },
                            compact=True, strict_json=True, sort_keys=False,
                            max_bytes=FEATURE_SOURCE_PAGES_MAX_BYTES,
                        )
                        pages_signature = _feature_source_signature(pages_path.stat())
                        pages_digest = _stable_feature_digest(pages_path, pages_signature)
                        if pages_digest is not None:
                            receipt["source_pages_sha256"] = pages_digest
                    except (OSError, ValueError, UnicodeError, TypeError) as exc:
                        print(json.dumps({
                            "event": "feature_source_pages_projection_failed",
                            "error_type": type(exc).__name__,
                        }), flush=True)
            _atomic_json(_feature_reuse_receipt_path(path), receipt, compact=True)
    except (OSError, ValueError, UnicodeError, TypeError):
        # This is an optional acceleration receipt. The next run will validate
        # and parse the full source rather than weaken the publication gate.
        pass


def _current_feature_snapshot(
    path: Path, *, feature_revision: str | None = None,
    source_metadata_sha256: str | None = None,
    source_observation_root: str | None = None,
) -> int | None:
    """Reuse field count only after source and dependency proof is current."""

    dependencies = (
        REPO_ROOT / "artifacts/live/data_monitor/record_inventory_cache.json",
        REPO_ROOT / "stockagent/live/data_monitor_inventory.py",
        REPO_ROOT / "stockagent/live/data_monitor_dashboard.py",
        REPO_ROOT / "stockagent/live/data_monitor_feature_receipt.py",
        REPO_ROOT / "stockagent/live/data_monitor_feature_pages.py",
        REPO_ROOT / "stockagent/live/data_monitor_feature_shards.py",
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
        if source_observation_root is not None:
            observation_binding = _feature_observation_binding(
                signature, receipt.get("source_sha256"), receipt.get("fields"), source_observation_root,
            ) if isinstance(receipt, dict) else None
            if (
                not isinstance(receipt, dict)
                or observation_binding is None
                or receipt.get("source_observation_root") != source_observation_root
                or receipt.get("source_observation_sha256") != observation_binding
            ):
                return None
        revision_bound = (
            isinstance(receipt, dict)
            and isinstance(feature_revision, str)
            and len(feature_revision) == 32
            and receipt.get("feature_revision") == feature_revision
            and (
                source_metadata_sha256 is None
                or receipt.get("source_metadata_sha256") == source_metadata_sha256
            )
            and receipt.get("feature_revision_sha256") == _feature_revision_binding(
                signature, receipt.get("source_sha256"), receipt.get("fields"),
                feature_revision, source_metadata_sha256,
            )
        )
        requires_binding = (
            source_metadata_sha256 is not None
            or (
                isinstance(receipt, dict)
                and isinstance(feature_revision, str)
                and receipt.get("feature_revision") is not None
            )
        )
        if requires_binding and not revision_bound:
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
    except (OSError, ValueError, UnicodeError, TypeError):
        pass
    if source_metadata_sha256 is not None or source_observation_root is not None:
        # A full JSON parse proves the file contract, not that its labels were
        # projected from today's public metadata and observed source identities.
        # Rebuild on missing/damaged binding; parsing cannot prove provenance.
        return None
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


def _publish_feature_inventory_snapshot(
    root: Path, output: Path, *, snapshot: InventorySnapshot,
    public_status: dict[str, object], feature_revision: str | None,
    source_metadata_sha256: str, prefer_shards: bool = True,
) -> dict[str, object]:
    """One canonical complete publisher, with an optional bounded shard cache.

    Cache faults retain the full-build path and truthful source root. Receipt
    publication is identical for both paths; neither skips complete field rows.
    """
    stages: dict[str, float] = {}
    inventory_timing: dict[str, float] = {}
    projection_cache: dict[str, object] = {"state": "full_build"}
    result = None
    if prefer_shards:
        from stockagent.live.data_monitor_feature_shards import publish_feature_shards

        started = time.perf_counter()
        try:
            result = publish_feature_shards(root, output, snapshot=snapshot, monitor_status=public_status)
            projection_cache = {
                "state": "published", "schema_version": 1,
                "reused_datasets": result.reused_datasets,
                "rebuilt_datasets": result.rebuilt_datasets,
                "object_recovery": "object_recovery_total" in result.timing_ms,
            }
            inventory_timing.update(result.timing_ms)
        except (OSError, ValueError, TypeError, UnicodeError) as exc:
            projection_cache = {"state": "full_fallback", "error_type": type(exc).__name__}
            print(json.dumps({"event": "feature_projection_cache_fallback", **projection_cache}), flush=True)
        stages["shard_attempt"] = round((time.perf_counter() - started) * 1_000, 3)
    if result is not None:
        fields = result.fields
        source_root = result.source_observation_root
        preview, source_pages = result.preview, result.source_pages
        snapshot.payload = None
        snapshot.selected = None
    else:
        started = time.perf_counter()
        raw = build_feature_inventory(root, snapshot=snapshot, timing_ms=inventory_timing)
        source_root = raw.get("source_observation_root")
        snapshot.payload = None
        snapshot.selected = None
        stages["footer_projection"] = round((time.perf_counter() - started) * 1_000, 3)
        started = time.perf_counter()
        public = build_data_monitor_feature_inventory(root, monitor_status=public_status, inventory=raw)
        del raw
        stages["public_projection"] = round((time.perf_counter() - started) * 1_000, 3)
        if (
            public.get("schema_version") != 1 or public.get("read_only") is not True
            or public.get("production_control_possible") is not False
            or not isinstance(public.get("rows"), list)
        ):
            raise ValueError("data-monitor feature projection violates public contract")
        started = time.perf_counter()
        preview, source_pages = feature_page_projections(public)
        stages["preview"] = round((time.perf_counter() - started) * 1_000, 3)
        started = time.perf_counter()
        _atomic_json(output, public, compact=True, strict_json=True, sort_keys=False)
        fields = len(public["rows"])
        stages["atomic_write"] = round((time.perf_counter() - started) * 1_000, 3)
        del public
    started = time.perf_counter()
    _write_feature_reuse_receipt(
        output, fields, validated_contract=True, feature_revision=feature_revision,
        source_metadata_sha256=source_metadata_sha256, source_observation_root=source_root,
        preview=preview, source_pages=source_pages,
    )
    stages["receipt"] = round((time.perf_counter() - started) * 1_000, 3)
    return {"fields": fields, "source_observation_root": source_root, "stages_ms": stages,
            "inventory_timing_ms": inventory_timing, "projection_cache": projection_cache}


def publish_feature_inventory_snapshot(
    root: Path, output: Path, *, snapshot: InventorySnapshot,
    public_status: dict[str, object], feature_revision: str | None,
    source_metadata_sha256: str, prefer_shards: bool = True,
) -> dict[str, object]:
    """Serialize source+receipt publication, not HTTP visitors or providers.

    Another local publisher must not replace JSON between our rename and
    receipt digest/binding. Both cache and full fallback share this one lock.
    Kernel lock ownership ends on process death; no stale PID lock survives.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.with_name(f".{output.name}.publication.lock")
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a+b") as handle:
        source_stat = os.fstat(handle.fileno())
        import stat
        if (
            not stat.S_ISREG(source_stat.st_mode) or source_stat.st_uid != os.geteuid()
            or source_stat.st_mode & 0o022 or source_stat.st_nlink != 1
        ):
            raise ValueError("untrusted feature publication lock")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            return _publish_feature_inventory_snapshot(
                root, output, snapshot=snapshot, public_status=public_status,
                feature_revision=feature_revision, source_metadata_sha256=source_metadata_sha256,
                prefer_shards=prefer_shards,
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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
    source_metadata_sha256 = feature_source_metadata_sha256(
        public_status, record_inventory["datasets"],
    )
    feature_stage_started = time.perf_counter()
    inventory_source_root = record_inventory.get("source_observation_root")
    feature_count = _current_feature_snapshot(
        feature_inventory_output, feature_revision=feature_revision,
        source_metadata_sha256=source_metadata_sha256,
        source_observation_root=inventory_source_root,
    ) if isinstance(inventory_source_root, str) else None
    feature_stages_ms = {
        "reuse_check": round((time.perf_counter() - feature_stage_started) * 1_000, 3),
    }
    feature_reused = feature_count is not None
    feature_source_root = inventory_source_root if feature_reused else None
    feature_inventory_timing_ms: dict[str, float] = {}
    feature_projection_cache: dict[str, object] = {"state": "whole_snapshot_reused"}
    if feature_count is None:
        publication = publish_feature_inventory_snapshot(
            REPO_ROOT, feature_inventory_output, snapshot=inventory_snapshot,
            public_status=public_status, feature_revision=feature_revision,
            source_metadata_sha256=source_metadata_sha256,
        )
        feature_count = publication["fields"]
        feature_source_root = publication["source_observation_root"]
        feature_stages_ms.update(publication["stages_ms"])
        feature_inventory_timing_ms.update(publication["inventory_timing_ms"])
        feature_projection_cache = publication["projection_cache"]
    feature_elapsed = time.perf_counter() - started - services_elapsed - public_elapsed
    # Reuse this supervised, every-30-second worker instead of installing one
    # more polling daemon. The tracker checks its durable due time before doing
    # any systemd query, and measurement failure cannot invalidate data status.
    sample_lines = []
    try:
        from scripts.track_service_runtime_trends import (
            runtime_sample_journal_lines, sample_service_runtime,
        )

        sample = sample_service_runtime(
            REPO_ROOT / "artifacts/live/data_monitor/all_service_runtime_baseline.json"
        )
        if sample is not None:
            sample_lines = runtime_sample_journal_lines(sample)
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
        "dependency_import_observation": _SHARED_IMPORT_OBSERVATION,
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
        "inventory_preflight": record_inventory.get("preflight_observation"),
        "public_projection_stages_ms": public_status_timing_ms,
        "feature_inventory_stages_ms": feature_inventory_timing_ms,
        "feature_stages_ms": feature_stages_ms,
        "feature_reused": feature_reused,
        "feature_projection_cache": feature_projection_cache,
        "feature_source_observation": {
            "schema_version": 1,
            "matches_record": isinstance(inventory_source_root, str)
                and feature_source_root == inventory_source_root,
            "inventory_matches_cache": record_inventory.get("source_observation_matches_cache") is True,
        },
        "sources": len(public_status.get("sources") or ()),
        "features": feature_count,
        "cached_files": record_inventory["cached_files"],
        "refreshed_files": record_inventory["refreshed_files"],
        "changed_dataset_ids": record_inventory.get("changed_dataset_ids"),
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
    for line in sample_lines:
        print(line, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
