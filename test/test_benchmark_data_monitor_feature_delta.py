from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import snapshot_data_refresh_services as snapshot_service
from scripts.benchmark_data_monitor_feature_delta import (
    _read_generation,
    changed_dataset_ids,
    project_rows,
    run,
)
from stockagent.live.dashboard_updates import metadata_signature
from stockagent.live.data_monitor_feature_receipt import feature_reuse_checksum


def _cache(value: int) -> dict[str, object]:
    return {
        "version": 10,
        "selection_membership": "same-membership",
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
    new["selection_membership"] = "same-membership"
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
    assert changed_dataset_ids(old, new, selected) is None

    new = _cache(1)
    new["version"] += 1
    assert changed_dataset_ids(old, new, selected) is None


def test_projection_reuses_unchanged_rows_and_detects_metadata_drift() -> None:
    old = [_row("dataset:a", "close", 1), _row("dataset:b", "close", 2)]
    new = [_row("dataset:a", "close", 3), _row("dataset:b", "close", 2)]
    assert project_rows(old, new, {"dataset:a"}) == new

    new[1] = {**new[1], "source_title": "renamed source"}
    assert project_rows(old, new, {"dataset:a"}) != new


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
    cache = {"feature_revision": revision, "files": {}, "datasets": {}}
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
    }))
    feature_path = directory / "feature_inventory.json"
    feature_path.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    snapshot_service._write_feature_reuse_receipt(
        feature_path, 0, feature_revision=revision,
        source_metadata_sha256=metadata_digest,
    )
    assert _read_generation(tmp_path)[2] == revision


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
    }
