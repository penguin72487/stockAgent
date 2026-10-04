from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts import snapshot_data_refresh_services as snapshot_service
from scripts.benchmark_data_monitor_feature_delta import (
    _different_generation,
    _row_value_changes,
    _generation_selection,
    _read_generation,
    changed_dataset_ids,
    project_rows,
    run,
)
from stockagent.live.dashboard_updates import metadata_signature
from stockagent.live.data_monitor_feature_receipt import feature_observation_binding, feature_reuse_checksum
from stockagent.live.data_monitor_inventory import (
    DATASET_MEMBERSHIP_VERSION,
    _dataset_memberships,
    _membership_root,
    inventory_dataset_delta,
)


def _cache(value: int) -> dict[str, object]:
    memberships = _dataset_memberships({
        "dataset:a": [Path("/tmp/a.parquet")],
        "dataset:b": [Path("/tmp/b.parquet")],
    })
    return {
        "version": 10,
        "selection_membership": _membership_root(memberships),
        "dataset_membership_version": DATASET_MEMBERSHIP_VERSION,
        "dataset_memberships": memberships,
        "schemas": {"schema": [["close", "double"]]},
        "files": {
            "/tmp/a.parquet": {"stats": {"count": value, "schema_id": "schema"}, "file_identity": [1, 2, 3, 4, 5]},
            "/tmp/b.parquet": {"stats": {"count": 2, "schema_id": "schema"}, "file_identity": [1, 2, 3, 4, 5]},
        },
        "datasets": {"dataset:a": {"count": value}, "dataset:b": {"count": 2}},
    }


def _row(dataset: str, field: str, count: int) -> dict[str, object]:
    return {
        "dataset_id": dataset, "field": field, "non_null_count": count,
        "market_category": "crypto", "provider": "provider",
        "source_title": dataset,
    }


def test_changed_dataset_ids_narrows_exact_footer_and_aggregate_changes() -> None:
    selected = {
        "dataset:a": [Path("/tmp/a.parquet")],
        "dataset:b": [Path("/tmp/b.parquet")],
    }
    old = _cache(1)
    new = _cache(3)
    assert changed_dataset_ids(old, new, selected) == {"dataset:a"}
    assert changed_dataset_ids(new, new, selected) == set()

    new["selection_membership"] = "new-membership"
    assert changed_dataset_ids(old, new, selected) is None
    new["selection_membership"] = old["selection_membership"]
    new["schemas"] = {"other": [["close", "double"]]}
    assert changed_dataset_ids(old, new, selected) == {"dataset:a", "dataset:b"}


def test_changed_dataset_ids_keeps_non_footer_contracts_fail_closed() -> None:
    selected = {
        "dataset:a": [Path("/tmp/a.parquet")],
        "dataset:b": [Path("/tmp/b.parquet")],
    }
    old = _cache(1)
    new = _cache(1)
    new["datasets"]["dataset:b"]["last"] = "2026-09-26"
    assert changed_dataset_ids(old, new, selected) == {"dataset:b"}

    new = _cache(1)
    new["files"]["/tmp/b.parquet"]["error"] = "invalid_parquet"
    assert changed_dataset_ids(old, new, selected) == {"dataset:b"}

    new = _cache(1)
    del new["files"]["/tmp/b.parquet"]
    assert changed_dataset_ids(old, new, selected) == {"dataset:b"}

    new = _cache(1)
    new["version"] += 1
    assert changed_dataset_ids(old, new, selected) is None


def test_projection_reuses_unchanged_rows_and_detects_metadata_drift() -> None:
    old = [_row("dataset:a", "close", 1), _row("dataset:b", "close", 2)]
    new = [_row("dataset:a", "close", 3), _row("dataset:b", "close", 2)]
    assert project_rows(old, new, {"dataset:a"}) == new

    new[1] = {**new[1], "source_title": "renamed source"}
    assert project_rows(old, new, {"dataset:a"}) != new


def test_shadow_uses_the_canonical_delta_contract() -> None:
    assert changed_dataset_ids is inventory_dataset_delta


def test_public_republication_is_not_confused_with_a_raw_footer_revision() -> None:
    before = {"generated_at_utc": "2026-10-01T00:00:00+00:00"}
    after = {"generated_at_utc": "2026-10-01T00:00:30+00:00"}
    assert _different_generation("a", before, "a", after) is True
    assert _different_generation("a", before, "a", before) is False
    assert _different_generation("a", {}, "a", {}) is False
    assert _different_generation("a", {}, "b", {}) is True


def test_row_diagnostics_separate_field_values_and_public_labels_without_logging_values() -> None:
    old = [_row("a", "x", 1), _row("b", "y", 3)]
    new = [{**old[0], "non_null_count": None, "schema_state": "partial"},
           {**old[1], "source_title": "renamed"}]
    assert _row_value_changes(old, new) == {
        "a": {"non_null_count": 1, "schema_state": 1},
        "b": {"source_title": 1},
    }


def test_shadow_fences_completed_generation_despite_new_partitions() -> None:
    cache = _cache(1)
    selected = {
        "dataset:a": [Path("/tmp/a.parquet")],
        "dataset:b": [Path("/tmp/b.parquet")],
    }
    assert _generation_selection(cache, selected) == (
        selected, "current_selection_matches_completed_generation",
    )
    observed = {**selected, "dataset:a": [Path("/tmp/a.parquet"), Path("/tmp/new.parquet")]}
    assert _generation_selection(cache, observed) == (
        selected, "completed_generation_bound_membership",
    )
    # A file map alone never proves old ownership. Even if the same union of
    # paths survives, the complete per-owner root must match before reuse.
    reassigned = {"dataset:a": selected["dataset:b"], "dataset:b": selected["dataset:a"]}
    assert _generation_selection(cache, reassigned) is None
    assert _generation_selection(cache, {**selected, "dataset:a": []}) is None
    cache["dataset_memberships"]["dataset:a"] = "0" * 32
    assert _generation_selection(cache, selected) is None


def test_projection_removes_retired_dataset_and_adds_new_dataset() -> None:
    old = [_row("dataset:retired", "x", 3), _row("dataset:b", "close", 2)]
    new = [_row("dataset:b", "close", 2), _row("dataset:new", "x", 3)]
    assert project_rows(old, new, {"dataset:retired", "dataset:new"}) == new


def test_projection_replaces_all_rows_of_changed_dataset() -> None:
    old = [
        _row("dataset:a", "close", 1),
        _row("dataset:a", "retired", 1),
        _row("dataset:b", "close", 2),
    ]
    new = [_row("dataset:a", "close", 3), _row("dataset:b", "close", 2)]
    assert project_rows(old, new, {"dataset:a"}) == new


@pytest.mark.parametrize("source_metadata_sha256", [None, "a" * 64])
def test_generation_requires_feature_revision_binding(
    tmp_path: Path, source_metadata_sha256: str | None,
) -> None:
    directory = tmp_path / "artifacts/live/data_monitor"
    directory.mkdir(parents=True)
    revision = "a" * 32
    cache = {"feature_revision": revision, "files": {}, "datasets": {},
             "source_observation_root": "d" * 32, "source_observation_matches_cache": True}
    (directory / "record_inventory_cache.json").write_text(json.dumps(cache))
    feature_path = directory / "feature_inventory.json"
    feature_path.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    signature = list(metadata_signature(feature_path.stat()))
    digest = hashlib.sha256(feature_path.read_bytes()).hexdigest()
    revision_values = [signature, digest, 0, revision]
    if source_metadata_sha256 is not None:
        revision_values.append(source_metadata_sha256)
    receipt = {
        "schema_version": 3, "read_only": True,
        "production_control_possible": False,
        "source_signature": signature, "source_sha256": digest,
        "fields": 0, "receipt_sha256": feature_reuse_checksum(signature, digest, 0),
        "feature_revision": revision,
        "feature_revision_sha256": hashlib.sha256(json.dumps(
            revision_values, separators=(",", ":"),
        ).encode("ascii")).hexdigest(),
        "source_metadata_sha256": source_metadata_sha256,
        "source_observation_root": "d" * 32,
        "source_observation_sha256": feature_observation_binding(signature, digest, 0, "d" * 32),
    }
    receipt_path = directory / "feature_inventory.json.reuse.json"
    receipt_path.write_text(json.dumps(receipt))
    assert _read_generation(tmp_path)[2] == revision

    if source_metadata_sha256 is not None:
        receipt["source_metadata_sha256"] = "not-a-digest"
        receipt_path.write_text(json.dumps(receipt))
        with pytest.raises(ValueError, match="not one completed generation"):
            _read_generation(tmp_path)
        receipt["source_metadata_sha256"] = source_metadata_sha256

    receipt["feature_revision_sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="not one completed generation"):
        _read_generation(tmp_path)


def test_shadow_accepts_metadata_bound_producer_receipt(tmp_path: Path) -> None:
    directory = tmp_path / "artifacts/live/data_monitor"
    directory.mkdir(parents=True)
    revision = "b" * 32
    metadata_digest = "c" * 64
    (directory / "record_inventory_cache.json").write_text(json.dumps({
        "feature_revision": revision, "files": {}, "datasets": {},
        "source_observation_root": "d" * 32, "source_observation_matches_cache": True,
    }))
    feature_path = directory / "feature_inventory.json"
    feature_path.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    snapshot_service._write_feature_reuse_receipt(
        feature_path, 0, feature_revision=revision,
        source_metadata_sha256=metadata_digest,
        source_observation_root="d" * 32,
    )
    assert _read_generation(tmp_path)[2] == revision


def test_closed_generation_survives_next_cache_head_without_accepting_mixed_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import benchmark_data_monitor_feature_delta as benchmark_module

    directory = tmp_path / "artifacts/live/data_monitor"
    directory.mkdir(parents=True)
    revision = "b" * 32
    cache_path = directory / "record_inventory_cache.json"
    old_cache = {"feature_revision": revision, "files": {}, "datasets": {},
                 "source_observation_root": "d" * 32, "source_observation_matches_cache": True}
    cache_path.write_text(json.dumps(old_cache))
    feature_path = directory / "feature_inventory.json"
    feature_path.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    snapshot_service._write_feature_reuse_receipt(
        feature_path, 0, feature_revision=revision, source_observation_root="d" * 32,
    )
    stable_read = benchmark_module._stable_bytes
    next_revision = "c" * 32

    def next_cache_head(path: Path):
        body, source_stat = stable_read(path)
        if path == feature_path:
            next_cache = cache_path.with_suffix(".next")
            next_cache.write_text(json.dumps({**old_cache, "feature_revision": next_revision}))
            os.replace(next_cache, cache_path)
        return body, source_stat

    monkeypatch.setattr(benchmark_module, "_stable_bytes", next_cache_head)
    assert _read_generation(tmp_path)[2] == revision
    assert json.loads(cache_path.read_text())["feature_revision"] == next_revision

    # The pinned old body was valid; actually reading the new cache with the
    # old feature/receipt is still a torn generation, never a parity pass.
    monkeypatch.setattr(benchmark_module, "_stable_bytes", stable_read)
    with pytest.raises(ValueError, match="not one completed generation"):
        _read_generation(tmp_path)


@pytest.mark.parametrize("tamper", ["legacy", "mismatched_root", "bad_binding", "unverified_cache"])
def test_closed_generation_requires_source_observation_coherence(
    tmp_path: Path, tamper: str,
) -> None:
    directory = tmp_path / "artifacts/live/data_monitor"
    directory.mkdir(parents=True)
    revision = "a" * 32
    cache = {"feature_revision": revision, "files": {}, "datasets": {},
             "source_observation_root": "d" * 32, "source_observation_matches_cache": True}
    cache_path = directory / "record_inventory_cache.json"
    cache_path.write_text(json.dumps(cache))
    feature = directory / "feature_inventory.json"
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    snapshot_service._write_feature_reuse_receipt(
        feature, 0, feature_revision=revision, source_observation_root="d" * 32,
    )
    assert _read_generation(tmp_path)[2] == revision
    receipt_path = directory / "feature_inventory.json.reuse.json"
    receipt = json.loads(receipt_path.read_text())
    if tamper == "legacy":
        cache.pop("source_observation_root")
        cache.pop("source_observation_matches_cache")
        receipt.pop("source_observation_root")
        receipt.pop("source_observation_sha256")
    elif tamper == "unverified_cache":
        cache["source_observation_matches_cache"] = False
    elif tamper == "mismatched_root":
        receipt["source_observation_root"] = "e" * 32
    else:
        receipt["source_observation_sha256"] = "0" * 64
    cache_path.write_text(json.dumps(cache))
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="not one completed generation"):
        _read_generation(tmp_path)


def test_initial_publication_race_is_inconclusive_not_a_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def publishing(_root: Path) -> None:
        raise ValueError("producer is between atomic publishes")

    monkeypatch.setattr(
        "scripts.benchmark_data_monitor_feature_delta._read_generation", publishing,
    )
    assert run(tmp_path, wait_seconds=0.01) == {
        "state": "inconclusive_initial_generation", "error_type": "ValueError",
        "last_error": "producer is between atomic publishes",
    }
