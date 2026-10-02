#!/usr/bin/env python3
"""Read-only shadow proof for dataset-scoped feature-inventory rebuilding.

Observe two completed producer generations. Never write the producer cache,
feature snapshot, receipt, or public response; a source race is inconclusive.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.dashboard_updates import metadata_signature  # noqa: E402
from stockagent.live.data_monitor_dashboard import _MARKET_CATEGORY_LABELS  # noqa: E402
from stockagent.live.data_monitor_feature_receipt import (  # noqa: E402
    feature_observation_binding,
    feature_revision_binding,
    feature_reuse_receipt_path,
    trusted_feature_snapshot,
)
from stockagent.live.data_monitor_inventory import (  # noqa: E402
    InventorySnapshot,
    _bound_dataset_memberships,
    _membership_fingerprint,
    _selected_files,
    build_feature_inventory,
    inventory_dataset_delta as changed_dataset_ids,
)


def _stable_bytes(path: Path) -> tuple[bytes, os.stat_result]:
    before = path.stat()
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        body = stream.read()
        closed = os.fstat(stream.fileno())
    if not (
        metadata_signature(before)
        == metadata_signature(opened)
        == metadata_signature(closed)
        == metadata_signature(path.stat())
    ):
        raise ValueError(f"source changed during read: {path.name}")
    return body, before


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non-finite snapshot JSON: {value}")


def _read_generation(root: Path) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Pin a completed generation, not the producer's moving filename head.

    _stable_bytes fences each opened file while reading. The feature digest,
    revision and receipt must then agree. A newer cache may legitimately be
    published while these already-frozen bytes are decoded; it does not erase
    this completed generation. This is a shadow parity observation, NEVER a
    permission for production readers to skip a current-source freshness gate.
    """
    directory = root / "artifacts/live/data_monitor"
    cache_path = directory / "record_inventory_cache.json"
    feature_path = directory / "feature_inventory.json"
    cache_body, _ = _stable_bytes(cache_path)
    feature_body, feature_stat = _stable_bytes(feature_path)
    if not trusted_feature_snapshot(feature_path, source_stat=feature_stat, body=feature_body):
        raise ValueError("feature producer receipt does not validate source bytes")
    receipt_body, _ = _stable_bytes(feature_reuse_receipt_path(feature_path))
    cache = json.loads(cache_body, parse_constant=_reject_nonfinite)
    feature = json.loads(feature_body, parse_constant=_reject_nonfinite)
    receipt = json.loads(receipt_body, parse_constant=_reject_nonfinite)
    revision = cache.get("feature_revision") if isinstance(cache, Mapping) else None
    signature = list(metadata_signature(feature_stat))
    source_metadata_sha256 = receipt.get("source_metadata_sha256") if isinstance(receipt, Mapping) else None
    revision_binding = (
        feature_revision_binding(
            signature, receipt.get("source_sha256"), receipt.get("fields"),
            revision, source_metadata_sha256,
        )
        if isinstance(receipt, Mapping)
        and (source_metadata_sha256 is None or (
            isinstance(source_metadata_sha256, str)
            and len(source_metadata_sha256) == 64
            and all(char in "0123456789abcdef" for char in source_metadata_sha256)
        ))
        else None
    )
    source_root = cache.get("source_observation_root") if isinstance(cache, Mapping) else None
    observation_binding = (
        feature_observation_binding(signature, receipt.get("source_sha256"), receipt.get("fields"), source_root)
        if isinstance(receipt, Mapping) else None
    )
    if (
        not isinstance(cache, dict)
        or not isinstance(cache.get("files"), dict)
        or not isinstance(cache.get("datasets"), dict)
        or not isinstance(feature, dict)
        or feature.get("schema_version") != 1
        or feature.get("read_only") is not True
        or feature.get("production_control_possible") is not False
        or not isinstance(feature.get("rows"), list)
        or not isinstance(receipt, dict)
        or receipt.get("feature_revision") != revision
        or receipt.get("feature_revision_sha256") != revision_binding
        or receipt.get("fields") != len(feature["rows"])
        or receipt.get("source_sha256") != hashlib.sha256(feature_body).hexdigest()
        or revision_binding is None
        or cache.get("source_observation_matches_cache") is not True
        or observation_binding is None
        or receipt.get("source_observation_root") != source_root
        or receipt.get("source_observation_sha256") != observation_binding
    ):
        raise ValueError("cache, feature, and receipt are not one completed generation")
    return cache, feature, revision


def project_rows(
    old_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]],
    changed: set[str],
) -> list[dict[str, Any]]:
    """Recompose rows without using the new generation's unaffected rows."""

    category_order = {category: index for index, category in enumerate(_MARKET_CATEGORY_LABELS)}
    rows = [row for row in old_rows if row["dataset_id"] not in changed]
    rows.extend(row for row in new_rows if row["dataset_id"] in changed)
    rows.sort(key=lambda row: (
        category_order.get(row["market_category"], 99),
        str(row["provider"]), str(row["source_title"]),
        str(row["dataset_id"]), str(row["field"]),
    ))
    return rows


def _path_signatures(paths: list[Path]) -> dict[str, tuple[int, ...] | None]:
    signatures = {}
    for path in paths:
        try:
            signatures[str(path)] = metadata_signature(path.stat())
        except OSError:
            signatures[str(path)] = None
    return signatures


def _generation_selection(
    cache: Mapping[str, Any], observed: Mapping[str, list[Path]],
) -> tuple[dict[str, list[Path]], str] | None:
    """Select a CLOSED generation, not a mutable current directory listing.

    New partitions may appear immediately after the producer publishes. Remove
    only those absent from its file map, and require the entire per-owner tree
    to match its bound root. Missing paths or reassignment cannot be guessed.
    This proves membership only; source identities still fence restricted rows.
    """

    members = _bound_dataset_memberships(cache)
    files = cache.get("files")
    if members is None or not isinstance(files, Mapping):
        return None
    if cache.get("selection_membership") == _membership_fingerprint(observed):
        return dict(observed), "current_selection_matches_completed_generation"
    candidate = {
        dataset: [path for path in observed.get(dataset, []) if str(path) in files]
        for dataset in members
    }
    if cache.get("selection_membership") != _membership_fingerprint(candidate):
        return None
    return candidate, "completed_generation_bound_membership"


def _different_generation(old_cache_revision: str, old_feature: Mapping[str, Any],
                          new_cache_revision: str, new_feature: Mapping[str, Any]) -> bool:
    # Labels/code can require republishing without changing raw footer inputs.
    # Do not silently discard that real public generation; parity may expose
    # missing public-metadata invalidation. Separately report raw-input changes.
    old_at, new_at = old_feature.get("generated_at_utc"), new_feature.get("generated_at_utc")
    return old_cache_revision != new_cache_revision or (
        isinstance(old_at, str) and isinstance(new_at, str) and old_at != new_at
    )


def _row_value_changes(old_rows: list[dict], new_rows: list[dict]) -> dict[str, dict[str, int]]:
    """Diagnose all changed columns by owner, without logging source values."""

    old = {(row["dataset_id"], row["field"]): row for row in old_rows}
    new = {(row["dataset_id"], row["field"]): row for row in new_rows}
    changes: dict[str, dict[str, int]] = {}
    missing = object()
    for identity in old.keys() | new.keys():
        before, after = old.get(identity, {}), new.get(identity, {})
        columns = [key for key in before.keys() | after.keys()
                   if before.get(key, missing) != after.get(key, missing)]
        if columns:
            counts = changes.setdefault(identity[0], {})
            for key in columns:
                counts[key] = counts.get(key, 0) + 1
    return changes


def run(root: Path, *, wait_seconds: float) -> dict[str, Any]:
    # Decoding a real completed generation already takes around two seconds.
    # Permit bounded retries across cache -> feature -> receipt publication;
    # never mistake a torn generation for a parity success or a service outage.
    initial_deadline = time.monotonic() + min(wait_seconds, 10.0)
    while True:
        try:
            read_started = time.perf_counter()
            old_cache, old_feature, old_revision = _read_generation(root)
            old_generation_read_ms = round((time.perf_counter() - read_started) * 1_000, 3)
            break
        except (OSError, ValueError, UnicodeError) as exc:
            if time.monotonic() >= initial_deadline:
                return {"state": "inconclusive_initial_generation", "error_type": type(exc).__name__,
                        "last_error": str(exc)}
            time.sleep(0.2)
    directory = root / "artifacts/live/data_monitor"
    feature_path = directory / "feature_inventory.json"
    try:
        initial_signature = metadata_signature(feature_path.stat())
    except OSError as exc:
        return {"state": "inconclusive_initial_generation", "error_type": type(exc).__name__}
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        try:
            current_signature = metadata_signature(feature_path.stat())
        except OSError:
            time.sleep(0.2)
            continue
        if current_signature != initial_signature:
            try:
                read_started = time.perf_counter()
                new_cache, new_feature, new_revision = _read_generation(root)
                new_generation_read_ms = round((time.perf_counter() - read_started) * 1_000, 3)
            except (OSError, ValueError, UnicodeError):
                # The producer atomically publishes cache, feature, then proof.
                # A mid-publication read is not a parity failure.
                time.sleep(0.2)
                continue
            if _different_generation(old_revision, old_feature, new_revision, new_feature):
                break
        time.sleep(0.5)
    else:
        return {"state": "no_new_generation", "old_revision": old_revision}

    selected_started = time.perf_counter()
    observed_selected = _selected_files(root)
    selected_discovery_ms = round((time.perf_counter() - selected_started) * 1_000, 3)
    binding_started = time.perf_counter()
    generation_selection = _generation_selection(new_cache, observed_selected)
    generation_membership_binding_ms = round((time.perf_counter() - binding_started) * 1_000, 3)
    if generation_selection is None:
        return {"state": "inconclusive_membership_moved"}
    selected, membership_source = generation_selection
    changed = changed_dataset_ids(old_cache, new_cache, selected)
    if changed is None:
        return {"state": "full_rebuild_required", "old_revision": old_revision,
                "new_revision": new_revision,
                "version_changed": old_cache.get("version") != new_cache.get("version"),
                "membership_changed": old_cache.get("selection_membership") != new_cache.get("selection_membership"),
                "file_keys_changed": old_cache["files"].keys() != new_cache["files"].keys()}
    old_rows = old_feature["rows"]
    new_rows = new_feature["rows"]
    started = time.perf_counter()
    candidate = project_rows(old_rows, new_rows, changed)
    compose_ms = round((time.perf_counter() - started) * 1_000, 3)
    candidate_equal = candidate == new_rows

    changed_paths = [path for dataset in sorted(changed) for path in selected.get(dataset, [])]
    signatures_started = time.perf_counter()
    before_signatures = _path_signatures(changed_paths)
    started = time.perf_counter()
    snapshot = InventorySnapshot(
        root, payload=new_cache,
        selected={dataset: selected.get(dataset, []) for dataset in changed},
    )
    restricted = build_feature_inventory(root, snapshot=snapshot)
    restricted_ms = round((time.perf_counter() - started) * 1_000, 3)
    after_signatures = _path_signatures(changed_paths)
    changed_file_signature_ms = round((time.perf_counter() - signatures_started) * 1_000, 3) - restricted_ms
    moved_paths = {
        key for key in before_signatures
        if before_signatures[key] != after_signatures[key]
    }
    cache_mismatch = {
        key for key, signature in before_signatures.items()
        if (list(signature) if signature is not None else None)
        != (new_cache["files"].get(key) or {}).get("file_identity")
    }
    source_moved = bool(moved_paths or cache_mismatch)
    expected_rows = [
        row for row in new_rows if row["dataset_id"] in changed
    ]
    expected = {
        (row["dataset_id"], row["field"]): row
        for row in expected_rows
    }
    restricted_keys = [
        (row["dataset_id"], row["field"]) for row in restricted["rows"]
    ]
    restricted_equal = (
        not source_moved
        and len(expected_rows) == len(expected)
        and len(restricted_keys) == len(set(restricted_keys))
        and set(restricted_keys) == set(expected)
        and all(
        all(key in expected.get((row["dataset_id"], row["field"]), {})
            and expected[(row["dataset_id"], row["field"])][key] == value
            for key, value in row.items())
        for row in restricted["rows"]
        )
    )
    return {
        "state": "inconclusive_source_signature" if source_moved else "verified" if candidate_equal and restricted_equal else "mismatch",
        "old_revision": old_revision,
        "new_revision": new_revision,
        "raw_feature_revision_changed": new_revision != old_revision,
        "old_feature_generated_at_utc": old_feature.get("generated_at_utc"),
        "new_feature_generated_at_utc": new_feature.get("generated_at_utc"),
        "full_rows": len(new_rows),
        "affected_datasets": sorted(changed),
        "affected_rows": len(expected),
        "membership_source": membership_source,
        "generation_membership_binding_ms": generation_membership_binding_ms,
        "candidate_matches_full_rows": candidate_equal,
        "changed_row_values_by_dataset": _row_value_changes(old_rows, new_rows),
        "restricted_footer_matches_full_rows": restricted_equal,
        "moved_paths": len(moved_paths),
        "cache_identity_mismatch_paths": len(cache_mismatch),
        "compose_ms": compose_ms,
        "old_generation_read_ms": old_generation_read_ms,
        "new_generation_read_ms": new_generation_read_ms,
        "selected_discovery_ms": selected_discovery_ms,
        "changed_file_signature_ms": round(changed_file_signature_ms, 3),
        "restricted_footer_ms": restricted_ms,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--wait-seconds", type=float, default=50.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 0 < args.wait_seconds <= 60:
        parser.error("--wait-seconds must be in (0, 60]")
    result = run(args.repo_root.resolve(), wait_seconds=args.wait_seconds)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return 0 if result["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
