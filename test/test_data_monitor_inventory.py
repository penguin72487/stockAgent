from __future__ import annotations

from datetime import UTC, date, datetime
import json
import os
from pathlib import Path

import pytest
from scripts import snapshot_data_refresh_services as snapshot_service

from stockagent.live.data_monitor_dashboard import (
    _apply_verified_storage_freshness,
    _market_category,
    _record_stats_for_row,
    build_data_monitor_feature_inventory,
    feature_source_metadata_sha256,
)
from stockagent.live.data_monitor_inventory import build_feature_inventory, build_record_inventory, parquet_footer_stats
from stockagent.live.data_monitor_inventory import PHYSICAL_FAMILIES
from stockagent.live import data_monitor_inventory as inventory_module
from stockagent.live import data_monitor_dashboard as dashboard_module


def test_flat_feature_discovery_matches_path_glob_including_hidden_and_directory(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data_yahoo/crypto"
    root.mkdir(parents=True)
    (root / "A_features.parquet").write_bytes(b"")
    (root / ".hidden_features.parquet").write_bytes(b"")
    (root / "B_features.parquet").mkdir()
    (root / "unrelated.parquet").write_bytes(b"")
    assert inventory_module._feature_paths(root) == inventory_module._sorted_paths(
        root.glob("*_features.parquet")
    )
    assert inventory_module._feature_paths(root, files_only=True) == inventory_module._sorted_paths(
        path for path in root.glob("*_features.parquet") if path.is_file()
    )


def test_feature_projection_resolves_repeated_dataset_metadata_once(tmp_path, monkeypatch):
    calls = 0

    def category(source):
        nonlocal calls
        calls += 1
        return "cross_market"

    monkeypatch.setattr(dashboard_module, "_market_category", category)
    inventory = {
        "rows": [{"dataset_id": "test:daily", "field": f"feature_{i}"} for i in range(1000)],
        "datasets_with_schema": 1, "datasets_total": 1,
        "files_with_schema": 1, "files_total": 1,
        "state": "complete", "basis": "test",
    }
    status = {"sources": [], "generated_at_utc": "2026-09-25T00:00:00+00:00"}
    result = build_data_monitor_feature_inventory(
        tmp_path, monitor_status=status, inventory=inventory
    )
    assert len(result["rows"]) == 1000
    assert calls == 1
    assert {row["market_category"] for row in result["rows"]} == {"cross_market"}


def test_feature_metadata_revision_tracks_labels_not_runtime_health(tmp_path: Path) -> None:
    source = {
        "id": "source:example", "record_inventory_key": "dataset:example",
        "title": "原名稱", "provider": "提供者", "market_category": "crypto",
        "status": "current",
    }
    status = {"sources": [source]}
    original = feature_source_metadata_sha256(status, ["dataset:example"])
    source["status"] = "degraded"
    assert feature_source_metadata_sha256(status, ["dataset:example"]) == original
    source["provider"] = "新提供者"
    assert feature_source_metadata_sha256(status, ["dataset:example"]) != original
    source["provider"] = "提供者"
    source["market_category"] = "forex"
    assert feature_source_metadata_sha256(status, ["dataset:example"]) != original
    source["market_category"] = "crypto"
    source["title"] = "新名稱"
    assert feature_source_metadata_sha256(status, ["dataset:example"]) != original
    inventory = {
        "rows": [{"dataset_id": "dataset:example", "field": "close"}],
        "datasets_with_schema": 1, "datasets_total": 1,
        "files_with_schema": 1, "files_total": 1,
        "state": "complete", "basis": "test",
    }
    projected = build_data_monitor_feature_inventory(
        tmp_path, monitor_status=status, inventory=inventory,
    )
    assert projected["rows"][0]["source_title"] == "新名稱"


def test_shared_inventory_reuses_discovery_but_rechecks_changed_and_deleted_files(tmp_path, monkeypatch):
    first = tmp_path / "data_yahoo/crypto/AAA_features.parquet"
    second = tmp_path / "data_yahoo/crypto/BBB_features.parquet"
    _write_parquet(first, [date(2025, 1, 1)])
    _write_parquet(second, [date(2025, 1, 1)])
    snapshot = inventory_module.InventorySnapshot(tmp_path)
    record = build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    assert set(record["timing_ms"]) == {
        "quick_index_probe", "cache_decode", "discover", "signature_scan",
        "aggregate_and_persist",
    }
    assert all(value >= 0 for value in record["timing_ms"].values())
    original = build_feature_inventory(tmp_path)
    assert original["source_observation_root"] == record["source_observation_root"]
    def unexpected(*args):
        pytest.fail("same-build discovery/JSON must not run twice")
    monkeypatch.setattr(inventory_module, "_selected_files", unexpected)
    monkeypatch.setattr(inventory_module, "_read_json", unexpected)
    feature_timing: dict[str, float] = {}
    assert build_feature_inventory(
        tmp_path, snapshot=snapshot, timing_ms=feature_timing
    ) == original
    assert set(feature_timing) == {
        "prepare", "verify_and_aggregate", "verify_file_signatures",
        "aggregate_fields", "emit_rows",
    }
    assert all(value >= 0 for value in feature_timing.values())
    _write_parquet(second, [date(2025, 1, 1), date(2025, 1, 2)])
    changed = build_feature_inventory(tmp_path, snapshot=snapshot)
    assert changed["source_observation_root"] != record["source_observation_root"]
    assert changed["state"] == "partial"
    assert changed["files_with_schema"] == 1
    assert all(row["schema_state"] == "partial" and row["non_null_count"] is None for row in changed["rows"])
    second.unlink()
    removed = build_feature_inventory(tmp_path, snapshot=snapshot)
    assert removed["source_observation_root"] != changed["source_observation_root"]
    assert {key: value for key, value in removed.items() if key != "source_observation_root"} == {
        key: value for key, value in changed.items() if key != "source_observation_root"
    }
    with pytest.raises(ValueError, match="another repository"):
        build_feature_inventory(tmp_path / "other", snapshot=snapshot)


def test_schema_batch_preserves_mixed_types_missing_fields_and_null_statistics(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    root = tmp_path / "data_yahoo/crypto"
    root.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1, None], "only_first": [2, 3]}), root / "A_features.parquet")
    pq.write_table(pa.table({"x": [3, 4], "only_first": [None, 5]}), root / "B_features.parquet")
    pq.write_table(pa.table({"x": [1.5], "late": [6]}), root / "C_features.parquet", write_statistics=False)
    snapshot = inventory_module.InventorySnapshot(tmp_path)
    build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    rows = {row["field"]: row for row in build_feature_inventory(tmp_path, snapshot=snapshot)["rows"]}
    assert rows["x"]["types"] == ["double", "int64"]
    assert rows["x"]["rows_with_field"] == 5
    assert rows["x"]["non_null_count"] is None
    assert rows["x"]["verified_partial_non_null"] == 3
    assert rows["x"]["non_null_known_files"] == 2
    assert rows["only_first"]["rows_with_field"] == 4
    assert rows["only_first"]["non_null_count"] == 3
    assert rows["late"]["verified_partial_non_null"] is None


def test_feature_non_null_fast_sum_preserves_unknown_malformed_and_large_integer(
    tmp_path: Path,
) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    directory = tmp_path / "data_yahoo/crypto"
    directory.mkdir(parents=True)
    paths = [directory / "A_features.parquet", directory / "B_features.parquet"]
    for path in paths:
        pq.write_table(pa.table({"value": [1]}), path)
    snapshot = inventory_module.InventorySnapshot(tmp_path)
    build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    assert snapshot.payload is not None
    entries = [snapshot.payload["files"][str(path)]["stats"] for path in paths]

    def value_row():
        return next(
            row for row in build_feature_inventory(tmp_path, snapshot=snapshot)["rows"]
            if row["dataset_id"] == "yahoo:crypto" and row["field"] == "value"
        )

    assert value_row()["non_null_count"] == 2
    entries[1]["non_null"][0] = None
    assert value_row()["non_null_count"] is None
    assert value_row()["verified_partial_non_null"] == 1
    entries[1]["non_null"][0] = 1.5
    assert value_row()["non_null_known_files"] == 1
    assert value_row()["verified_partial_non_null"] == 1
    entries[1]["non_null"][0] = 10**30
    assert value_row()["non_null_count"] == 10**30 + 1


def test_snapshot_is_updated_even_when_record_inventory_is_unchanged(tmp_path):
    first = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(first, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    snapshot = inventory_module.InventorySnapshot(tmp_path)
    assert build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)["refreshed_files"] == 0
    assert snapshot.selected is not None
    assert build_feature_inventory(tmp_path, snapshot=snapshot) == build_feature_inventory(tmp_path)


def test_unchanged_inventory_uses_small_index_without_full_cache_decode(
    tmp_path, monkeypatch,
):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    assert first["fast_index_hit"] is False
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    assert index_path.is_file()

    real_read_json = inventory_module._read_json

    def read_without_full_cache(candidate: Path):
        if candidate.name == "record_inventory_cache.json":
            pytest.fail("unchanged refresh must not decode the full file cache")
        return real_read_json(candidate)

    monkeypatch.setattr(inventory_module, "_read_json", read_without_full_cache)
    snapshot = inventory_module.InventorySnapshot(tmp_path)
    second = build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    assert second["fast_index_hit"] is True
    assert second["changed_dataset_ids"] == []
    assert second["datasets"] == first["datasets"]
    assert second["feature_revision"] == first["feature_revision"]
    assert snapshot.selected is not None
    assert second["preflight_observation"]["outcome"] == "unchanged_full_probe"
    assert second["preflight_observation"]["full_signature_probe"] is True


def test_fast_index_rejects_group_reassignment_with_same_unique_files(
    tmp_path, monkeypatch,
):
    crypto = tmp_path / "data_yahoo/crypto/A_features.parquet"
    forex = tmp_path / "data_yahoo/forex/B_features.parquet"
    _write_parquet(crypto, [date(2025, 1, 1)])
    _write_parquet(forex, [date(2025, 1, 2)])
    first = build_record_inventory(tmp_path, refresh=True)
    assert first["datasets"]["yahoo:crypto"]["first"] == "2025-01-01"
    original_selected = inventory_module._selected_files

    def reassigned(root: Path):
        selected = original_selected(root)
        selected["yahoo:crypto"], selected["yahoo:forex"] = (
            selected["yahoo:forex"], selected["yahoo:crypto"]
        )
        return selected

    monkeypatch.setattr(inventory_module, "_selected_files", reassigned)
    second = build_record_inventory(tmp_path, refresh=True)
    assert second["fast_index_hit"] is False
    assert second["datasets"]["yahoo:crypto"]["first"] == "2025-01-02"
    assert second["feature_revision"] != first["feature_revision"]
    assert second["changed_dataset_ids"] == ["yahoo:crypto", "yahoo:forex"]


def test_changed_file_reuses_quick_index_discovery_on_slow_path(tmp_path, monkeypatch):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    original_selected = inventory_module._selected_files
    discoveries = 0

    def counted_selected(root: Path):
        nonlocal discoveries
        discoveries += 1
        return original_selected(root)

    monkeypatch.setattr(inventory_module, "_selected_files", counted_selected)
    _write_parquet(path, [date(2025, 1, 1), date(2025, 1, 2)])
    current = build_record_inventory(tmp_path, refresh=True)
    assert discoveries == 1
    assert current["fast_index_hit"] is False
    assert current["refreshed_files"] == 1
    assert current["datasets"]["yahoo:crypto"]["count"] == 2
    assert current["timing_ms"]["quick_index_probe"] >= 0


def test_preflight_observation_distinguishes_early_hint_reject_from_full_probe(tmp_path, monkeypatch):
    monkeypatch.setattr(inventory_module, "FAST_INDEX_PREFLIGHT_HINTS", 1)
    paths = [tmp_path / f"data_yahoo/crypto/{name}_features.parquet" for name in ("A", "B")]
    for path in paths:
        _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    assert first["preflight_observation"]["outcome"] == "index_unavailable"
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index = json.loads(index_path.read_text())
    sampled = Path(index["preflight_file_hints"][0][0])
    unhinted = next(path for path in paths if path != sampled)
    _write_parquet(unhinted, [date(2025, 1, 1), date(2025, 1, 2)])
    full = build_record_inventory(tmp_path, refresh=True)
    assert full["preflight_observation"]["full_signature_probe"] is True
    assert full["preflight_observation"]["outcome"] == "full_signature_changed"
    _write_parquet(unhinted, [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)])
    early = build_record_inventory(tmp_path, refresh=True)
    assert early["preflight_observation"]["policy"] == "refreshed-priority-uniform-v1"
    assert early["preflight_observation"]["full_signature_probe"] is False
    assert early["preflight_observation"]["outcome"] == "source_hint_changed"
    assert early["preflight_observation"]["hints_checked"] == 1
    assert early["preflight_observation"]["wall_ms"] >= 0
    assert early["datasets"]["yahoo:crypto"]["count"] == 4


def test_preflight_file_hint_skips_redundant_full_probe_on_change(tmp_path, monkeypatch):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert [str(path), inventory_module._file_identity(path.stat())] in index["preflight_file_hints"]

    original_fingerprint = inventory_module._selection_fingerprint

    def no_full_probe_on_changed_hint(selected, *, cache=None):
        if cache is None:
            pytest.fail("changed hint must take the full slow path before a redundant probe scan")
        return original_fingerprint(selected, cache=cache)

    monkeypatch.setattr(inventory_module, "_selection_fingerprint", no_full_probe_on_changed_hint)
    _write_parquet(path, [date(2025, 1, 1), date(2025, 1, 2)])
    current = build_record_inventory(tmp_path, refresh=True)
    assert current["fast_index_hit"] is False
    assert current["refreshed_files"] == 1
    assert current["datasets"]["yahoo:crypto"]["count"] == 2


def test_preflight_priority_is_bounded_deterministic_and_uses_actual_refreshes(monkeypatch):
    monkeypatch.setattr(inventory_module, "FAST_INDEX_PREFLIGHT_HINTS", 4)
    cache = {f"/{i}": {"file_identity": [1, 2, 3, 999, 10 - i]} for i in range(10)}
    arguments = {"recently_refreshed": ["/9", "/8", "/7", "/6", "/missing", "/9"], "seed": "fixture"}
    priority = inventory_module._preflight_keys(cache, **arguments)
    assert len(priority) == len(set(priority)) == 4
    assert set(priority[:3]) <= {"/9", "/8", "/7", "/6"}
    assert set(priority) <= cache.keys()
    assert inventory_module._preflight_keys(dict(reversed(list(cache.items()))), **arguments) == priority
    assert inventory_module._preflight_keys(cache) == inventory_module._preflight_keys(cache, recently_refreshed=[])
    assert inventory_module._preflight_keys({}) == []


def test_refreshed_unsampled_path_is_promoted_to_next_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(inventory_module, "FAST_INDEX_PREFLIGHT_HINTS", 1)
    paths = [tmp_path / f"data_yahoo/crypto/{name}_features.parquet" for name in ("A", "B", "C")]
    for path in paths:
        _write_parquet(path, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    original = json.loads(index_path.read_text(encoding="utf-8"))
    assert original["preflight_policy"] == "refreshed-priority-uniform-v1"
    sampled = original["preflight_file_hints"][0][0]
    changed = next(path for path in paths if str(path) != sampled)
    _write_parquet(changed, [date(2025, 1, 1), date(2025, 1, 2)])
    first_refresh = build_record_inventory(tmp_path, refresh=True)
    assert first_refresh["refreshed_files"] == 1  # unsampled still fully checked
    promoted = json.loads(index_path.read_text(encoding="utf-8"))
    assert promoted["preflight_file_hints"][0][0] == str(changed)
    assert promoted["preflight_file_hints"][0][1] == inventory_module._file_identity(changed.stat())

    _write_parquet(changed, [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)])
    real_fingerprint = inventory_module._selection_fingerprint

    def no_redundant_full_probe(selected, *, cache=None):
        if cache is None:
            pytest.fail("recently changed source must be rejected by its hint before probing all history")
        return real_fingerprint(selected, cache=cache)

    monkeypatch.setattr(inventory_module, "_selection_fingerprint", no_redundant_full_probe)
    refreshed = build_record_inventory(tmp_path, refresh=True)
    assert refreshed["refreshed_files"] == 1
    assert refreshed["datasets"]["yahoo:crypto"]["count"] == 5  # 3 changed rows + two unchanged files


def test_invalid_preflight_hint_falls_back_to_full_inventory(tmp_path):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    original = build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["preflight_file_hints"] = [[str(path), [1, 2, 3]]]
    index["checksum"] = inventory_module._quick_index_checksum(index)
    index_path.write_text(json.dumps(index), encoding="utf-8")

    rebuilt = build_record_inventory(tmp_path, refresh=True)
    assert rebuilt["fast_index_hit"] is False
    assert rebuilt["datasets"] == original["datasets"]
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_unsampled_file_change_is_still_caught_by_full_fingerprint(tmp_path, monkeypatch):
    monkeypatch.setattr(inventory_module, "FAST_INDEX_PREFLIGHT_HINTS", 1)
    paths = [
        tmp_path / "data_yahoo/crypto/A_features.parquet",
        tmp_path / "data_yahoo/crypto/B_features.parquet",
    ]
    for path in paths:
        _write_parquet(path, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert len(index["preflight_file_hints"]) == 1
    sampled = index["preflight_file_hints"][0][0]
    unsampled = next(path for path in paths if str(path) != sampled)

    _write_parquet(unsampled, [date(2025, 1, 1), date(2025, 1, 2)])
    refreshed = build_record_inventory(tmp_path, refresh=True)
    assert refreshed["fast_index_hit"] is False
    assert refreshed["refreshed_files"] == 1
    assert refreshed["datasets"]["yahoo:crypto"]["count"] == 3


def test_same_aggregate_group_swap_still_changes_feature_revision(
    tmp_path, monkeypatch,
):
    crypto = tmp_path / "data_yahoo/crypto/A_features.parquet"
    forex = tmp_path / "data_yahoo/forex/B_features.parquet"
    _write_parquet(crypto, [date(2025, 1, 1)])
    _write_parquet(forex, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    original_selected = inventory_module._selected_files

    def reassigned(root: Path):
        selected = original_selected(root)
        selected["yahoo:crypto"], selected["yahoo:forex"] = (
            selected["yahoo:forex"], selected["yahoo:crypto"]
        )
        return selected

    monkeypatch.setattr(inventory_module, "_selected_files", reassigned)
    second = build_record_inventory(tmp_path, refresh=True)
    assert second["fast_index_hit"] is False
    assert second["datasets"] == first["datasets"]
    assert second["feature_revision"] != first["feature_revision"]


def test_corrupt_fast_index_falls_back_and_repairs(tmp_path):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index_path.write_text("{broken", encoding="utf-8")

    repaired = build_record_inventory(tmp_path, refresh=True)
    assert repaired["fast_index_hit"] is False
    assert repaired["datasets"] == first["datasets"]
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_valid_json_fast_index_aggregate_tampering_falls_back(tmp_path):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    index_path = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["datasets"]["yahoo:crypto"]["count"] = 1_000_000
    index_path.write_text(json.dumps(index), encoding="utf-8")

    repaired = build_record_inventory(tmp_path, refresh=True)
    assert repaired["fast_index_hit"] is False
    assert repaired["datasets"] == first["datasets"]
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_new_file_with_zero_footer_budget_never_reuses_old_aggregate(tmp_path):
    first = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(first, [date(2025, 1, 1)])
    initial = build_record_inventory(tmp_path, refresh=True)
    assert initial["datasets"]["yahoo:crypto"]["files_total"] == 1

    second = tmp_path / "data_yahoo/crypto/B_features.parquet"
    _write_parquet(second, [date(2025, 1, 2)])
    current = build_record_inventory(tmp_path, refresh=True, max_refresh_files=0)
    assert current["refreshed_files"] == 0
    assert current["datasets"]["yahoo:crypto"]["files_total"] == 2
    assert current["datasets"]["yahoo:crypto"]["count"] is None


def test_changed_file_with_zero_footer_budget_never_reuses_old_aggregate(tmp_path):
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    initial = build_record_inventory(tmp_path, refresh=True)
    assert initial["datasets"]["yahoo:crypto"]["count"] == 1

    _write_parquet(path, [date(2025, 1, 1), date(2025, 1, 2)])
    current = build_record_inventory(tmp_path, refresh=True, max_refresh_files=0)
    assert current["refreshed_files"] == 0
    assert current["datasets"]["yahoo:crypto"]["files_total"] == 1
    assert current["datasets"]["yahoo:crypto"]["count"] is None
    assert current["datasets"]["yahoo:crypto"]["state"] == "scanning"


def test_file_disappearing_after_discovery_cannot_reuse_verified_counts(
    tmp_path, monkeypatch
):
    path = tmp_path / "data_yahoo/crypto/AAA_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    initial = build_record_inventory(tmp_path, refresh=True)
    assert initial["datasets"]["yahoo:crypto"]["state"] == "verified"
    selected = inventory_module._selected_files(tmp_path)
    path.unlink()
    monkeypatch.setattr(
        inventory_module,
        "_selected_files",
        lambda _root: selected,
    )

    current = build_record_inventory(tmp_path, refresh=True)
    assert current["refreshed_files"] == 0
    assert current["datasets"]["yahoo:crypto"]["state"] == "scanning"
    assert current["datasets"]["yahoo:crypto"]["count"] is None


def test_transient_directory_move_cannot_pin_partial_counts_in_fast_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    index = tmp_path / "artifacts/live/data_monitor/record_inventory_fast_index.json"
    index.unlink()
    original_aggregate = inventory_module._aggregate
    hidden = path.parent.with_name("temporarily-hidden")

    def moved_during_aggregation(paths, cache, **kwargs):
        if kwargs.get("dataset") != "yahoo:crypto":
            return original_aggregate(paths, cache, **kwargs)
        path.parent.rename(hidden)
        try:
            return original_aggregate(paths, cache, **kwargs)
        finally:
            hidden.rename(path.parent)

    monkeypatch.setattr(inventory_module, "_aggregate", moved_during_aggregation)
    partial = build_record_inventory(tmp_path, refresh=True)
    assert partial["datasets"]["yahoo:crypto"]["count"] is None
    assert partial["source_observation_root"] != first["source_observation_root"]
    assert not index.exists(), "a transient absence must not create a reusable partial index"

    monkeypatch.setattr(inventory_module, "_aggregate", original_aggregate)
    recovered = build_record_inventory(tmp_path, refresh=True)
    assert recovered["fast_index_hit"] is False
    assert recovered["datasets"]["yahoo:crypto"]["count"] == 1
    assert recovered["source_observation_root"] == first["source_observation_root"]
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_same_footer_replacement_changes_source_observation_and_revision(tmp_path: Path) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    replacement = tmp_path / "replacement.parquet"
    replacement.write_bytes(path.read_bytes())
    os.replace(replacement, path)
    current = build_record_inventory(tmp_path, refresh=True)
    assert current["datasets"] == first["datasets"]
    assert current["source_observation_root"] != first["source_observation_root"]
    assert current["feature_revision"] != first["feature_revision"]
    assert current["changed_dataset_ids"] == ["yahoo:crypto"]
    assert current["source_observation_matches_cache"] is True
    assert build_feature_inventory(tmp_path)["source_observation_root"] == current["source_observation_root"]


def test_bybit_venue_training_table_is_registered_without_group_double_count() -> None:
    group, _, path, primary = PHYSICAL_FAMILIES["bybit:venue-training-features"]
    assert group == "bybit"
    assert path == "data_bybit/public_features/bybit_venue_daily.parquet"
    assert primary is False
    mixed_group, _, _, mixed_primary = PHYSICAL_FAMILIES[
        "crypto-public:mixed-research-features"
    ]
    assert mixed_group == "crypto-historical-public"
    assert mixed_primary is False


def _write_parquet(path: Path, dates: list[date]) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"date": pa.array(dates, type=pa.date32()), "value": list(range(len(dates)))}),
        path,
        row_group_size=2,
    )


@pytest.mark.parametrize("prior_version", [10, 11])
def test_membership_contract_migration_retains_footer_rows_with_zero_read_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prior_version: int,
) -> None:
    for name in ("A", "B"):
        _write_parquet(tmp_path / f"data_yahoo/crypto/{name}_features.parquet", [date(2025, 1, 1)])
    original = build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    prior = json.loads(cache_path.read_text(encoding="utf-8"))
    prior["version"] = prior_version
    if prior_version == 10:
        prior.pop("dataset_memberships")
        prior.pop("dataset_membership_version")
        prior["selection_membership"] = "legacy-global-fingerprint"
    cache_path.write_text(json.dumps(prior), encoding="utf-8")

    def no_footer(*args):
        pytest.fail("identical footer semantics must not discard all cached observations")

    monkeypatch.setattr(inventory_module, "parquet_footer_stats", no_footer)
    restored = build_record_inventory(tmp_path, refresh=True, max_refresh_files=0)
    current = json.loads(cache_path.read_text(encoding="utf-8"))
    assert current["version"] == 10
    assert current["dataset_membership_version"] == 1
    assert inventory_module._bound_dataset_memberships(current) is not None
    assert restored["refreshed_files"] == 0
    assert restored["changed_dataset_ids"] is None
    assert restored["datasets"] == original["datasets"]
    assert current["files"] == prior["files"]
    assert current["schemas"] == prior["schemas"]
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_corrupt_membership_binding_is_repaired_before_safe_delta_reuse(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data_yahoo/crypto/A_features.parquet", [date(2025, 1, 1)])
    original = build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    corrupted = json.loads(cache_path.read_text(encoding="utf-8"))
    corrupted["dataset_memberships"]["yahoo:crypto"] = "0" * 32
    cache_path.write_text(json.dumps(corrupted), encoding="utf-8")
    repaired = build_record_inventory(tmp_path, refresh=True)
    assert repaired["fast_index_hit"] is False
    assert repaired["changed_dataset_ids"] is None
    assert repaired["datasets"] == original["datasets"]
    restored = json.loads(cache_path.read_text(encoding="utf-8"))
    assert inventory_module._bound_dataset_memberships(restored) is not None
    assert build_record_inventory(tmp_path, refresh=True)["fast_index_hit"] is True


def test_parquet_footer_proves_actual_bounds_not_requested_start(tmp_path: Path) -> None:
    path = tmp_path / "test.parquet"
    _write_parquet(path, [date(2020, 5, 3), date(2020, 5, 1), date(2020, 5, 4)])
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["count"] == 3
    assert stats["first"] == "2020-05-01"
    assert stats["last"] == "2020-05-04"
    assert stats["time_column"] == "date"


def test_microstructure_unix_nanoseconds_are_reported_in_utc(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "microstructure.parquet"
    pq.write_table(pa.table({"snapshot_ts_ns": [1_785_200_400_000_000_000]}), path)
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["count"] == 1
    assert stats["first"] == "2026-07-28T01:00:00+00:00"
    assert stats["time_column"] == "snapshot_ts_ns"


def test_inventory_is_bounded_and_never_reports_partial_sum_as_total(tmp_path: Path) -> None:
    base = tmp_path / "data_tw_public"
    base.mkdir()
    (base / "dataset_manifest.json").write_text(
        json.dumps([{"name": "twse_one", "start_date": "1900-01-01"}, {"name": "twse_two"}]),
        encoding="utf-8",
    )
    _write_parquet(base / "twse_one.parquet", [date(2023, 1, 2), date(2023, 1, 3)])
    _write_parquet(base / "twse_two.parquet", [date(2024, 2, 1)])
    first = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert first["datasets"]["tw-public:twse_one"]["count"] == 2
    assert first["datasets"]["tw-public:twse_one"]["first"] == "2023-01-02"
    assert first["datasets"]["tw-public:twse_two"]["count"] is None
    assert first["datasets"]["group:tw-public"]["count"] is None
    second = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert second["datasets"]["group:tw-public"]["count"] is None  # absent archive files
    assert second["datasets"]["tw-public:twse_two"]["count"] == 1
    cached = build_record_inventory(tmp_path)
    assert cached["refreshed_files"] == 0
    assert cached["datasets"]["tw-public:twse_two"]["last"] == "2024-02-01"
    _write_parquet(base / "twse_one.parquet", [date(2025, 1, 1)])
    stale = build_record_inventory(tmp_path)
    assert stale["datasets"]["tw-public:twse_one"]["count"] == 2  # snapshot is read-only
    fresh = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert fresh["datasets"]["tw-public:twse_one"]["count"] == 1


def test_inventory_groups_crypto_and_yahoo_by_market(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data_okx/1m/BTC_features.parquet", [date(2020, 1, 1)])
    _write_parquet(tmp_path / "data_yahoo/tw_stocks/2330_features.parquet", [date(2021, 1, 1)])
    _write_parquet(tmp_path / "data_yahoo/crypto/BTC_features.parquet", [date(2022, 1, 1)])
    _write_parquet(tmp_path / "data_forex_frankfurter/USDTWD_features.parquet", [date(2023, 1, 1)])
    _write_parquet(tmp_path / "data_coinmetrics_community/assets/btc_features.parquet", [date(2024, 1, 1)])
    _write_parquet(tmp_path / "data_tw_microstructure/hft_dataset/trade_date=2025-01-01/data.parquet", [date(2025, 1, 1)])
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert inventory["group:okx"]["count"] == 1
    assert inventory["yahoo:tw_stocks"]["first"] == "2021-01-01"
    assert inventory["yahoo:crypto"]["last"] == "2022-01-01"
    assert inventory["group:yahoo-market"]["count"] is None  # absent other partitions
    assert inventory["group:forex-frankfurter"]["count"] == 1
    assert inventory["group:coinmetrics-community"]["count"] == 1
    assert inventory["group:tw-microstructure-train"]["count"] == 1
    assert _market_category({"id": "tw-public:twse_daily_ohlcv"}) == "taiwan_equity"
    assert _market_category({"id": "tw-public:dgbas_cpi_basic"}) == "macro"
    assert _market_category({"id": "inventory:yahoo:crypto"}) == "crypto"
    assert _market_category({"id": "group:okx"}) == "crypto"
    assert _record_stats_for_row({"id": "product:okx_perpetual_swaps:1m", "granularity": "1m"}, inventory)["count"] == 1
    assert _record_stats_for_row({"id": "unknown"}, inventory)["count"] is None
    assert _record_stats_for_row({"id": "credential:test", "scope": "credential_gate"}, inventory)["state"] == "not_applicable"
    assert "重疊" in _record_stats_for_row({"id": "group:tw-minute-source-cold"}, inventory)["basis"]


def test_crypto_hot_tail_is_listed_separately_without_double_counting(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data_okx/1m/BTC_features.parquet", [date(2026, 9, 1)])
    _write_parquet(
        tmp_path / "data_okx/1m/_hot_tail/BTC_features.parquet",
        [date(2026, 9, 1), date(2026, 9, 2)],
    )
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert inventory["group:okx"]["count"] == 1
    assert inventory["physical:okx:hot-tail"]["count"] == 2
    assert inventory["physical:okx:hot-tail"]["last"] == "2026-09-02"


def test_physical_inventory_keeps_derived_views_out_of_group_total(tmp_path: Path) -> None:
    _write_parquet(
        tmp_path / "data_tw_index_futures/all_futures_daily_sessions.parquet",
        [date(2020, 1, 1), date(2020, 1, 2)],
    )
    _write_parquet(
        tmp_path / "data_tw_index_futures/day_session_front_month.parquet",
        [date(2020, 1, 2)],
    )
    _write_parquet(
        tmp_path / "data_free_public/observations.parquet",
        [date(2021, 1, 1)],
    )
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=8)["datasets"]
    assert inventory["physical:tw-futures:all-contracts"]["count"] == 2
    assert inventory["physical:tw-futures:front-month"]["count"] == 1
    assert inventory["group:tw-index-futures"]["count"] == 2
    assert inventory["group:free-public-context"]["count"] == 1
    assert _record_stats_for_row({"id": "product:tw_index_futures:daily"}, inventory)["count"] == 2
    assert _record_stats_for_row(
        {"id": "inventory:tw-futures:front-month", "record_inventory_key": "physical:tw-futures:front-month"},
        inventory,
    )["count"] == 1


def test_supported_event_date_alias_proves_bounds(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "observations.parquet"
    pq.write_table(pa.table({"event_ts_utc": ["2024-01-03T00:00:00Z", "2024-01-01T00:00:00Z"]}), path)
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["first"] == "2024-01-01T00:00:00+00:00"
    assert stats["last"] == "2024-01-03T00:00:00+00:00"


def test_macro_subject_and_publication_dates_have_footer_bounds(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    daily = tmp_path / "daily.parquet"
    pq.write_table(pa.table({"subject_date": ["2002-05-03", "2002-05-02"]}), daily)
    assert parquet_footer_stats(daily)["first"] == "2002-05-02"
    provisional = tmp_path / "provisional.parquet"
    pq.write_table(pa.table({"published_at_taipei": [
        "2024-05-10T16:00:00", "2024-05-09T16:00:00"
    ]}), provisional)
    assert parquet_footer_stats(provisional)["last"] == "2024-05-10T16:00:00"


def test_invalid_parquet_is_visible_and_does_not_trigger_endless_retries(tmp_path: Path) -> None:
    directory = tmp_path / "data_yahoo/crypto"
    directory.mkdir(parents=True)
    (directory / "bad_features.parquet").write_bytes(b"not a parquet footer")
    first = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    stats = first["datasets"]["yahoo:crypto"]
    assert stats["state"] == "invalid"
    assert stats["count"] is None
    assert stats["verified_partial_count"] is None
    assert stats["invalid_files"] == 1
    second = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    assert second["refreshed_files"] == 0
    _write_parquet(directory / "bad_features.parquet", [date(2025, 1, 1)])
    repaired = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    assert repaired["datasets"]["yahoo:crypto"]["count"] == 1
    assert repaired["datasets"]["yahoo:crypto"]["invalid_files"] == 0


def test_same_size_mtime_replacement_rechecks_a_newly_cached_footer(tmp_path: Path) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    assert first["datasets"]["yahoo:crypto"]["first"] == "2025-01-01"
    original = path.stat()
    replacement = path.with_name("replacement.parquet")
    _write_parquet(replacement, [date(2025, 1, 2)])
    assert replacement.stat().st_size == original.st_size
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    os.replace(replacement, path)
    assert path.stat().st_mtime_ns == original.st_mtime_ns

    current = build_record_inventory(tmp_path, refresh=True)

    assert current["refreshed_files"] == 1
    assert current["datasets"]["yahoo:crypto"]["first"] == "2025-01-02"


def test_footer_replaced_during_read_is_not_published(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    real_footer = inventory_module.parquet_footer_stats

    def replace_after_read(target: Path):
        stats = real_footer(target)
        replacement = target.with_name("replacement.parquet")
        _write_parquet(replacement, [date(2025, 1, 2)])
        os.replace(replacement, target)
        return stats

    with monkeypatch.context() as patch:
        patch.setattr(inventory_module, "parquet_footer_stats", replace_after_read)
        raced = build_record_inventory(tmp_path, refresh=True)

    assert raced["datasets"]["yahoo:crypto"]["state"] == "scanning"
    assert raced["datasets"]["yahoo:crypto"]["count"] is None
    repaired = build_record_inventory(tmp_path, refresh=True)
    assert repaired["datasets"]["yahoo:crypto"]["first"] == "2025-01-02"


def test_legacy_footer_identity_recheck_keeps_semantic_revision_if_unchanged(
    tmp_path: Path,
) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    cache["files"][str(path)].pop("file_identity")
    cache_path.write_text(json.dumps(cache), encoding="utf-8")

    rechecked = build_record_inventory(tmp_path, refresh=True)

    assert rechecked["identity_rechecked_files"] == 1
    assert rechecked["refreshed_files"] == 0
    assert rechecked["feature_revision"] == first["feature_revision"]
    assert rechecked["datasets"] == first["datasets"]
    assert json.loads(cache_path.read_text(encoding="utf-8"))["files"][str(path)]["file_identity"]


def test_legacy_footer_identity_recheck_changes_revision_for_new_statistics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    first = build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    cache["files"][str(path)].pop("file_identity")
    cache_path.write_text(json.dumps(cache), encoding="utf-8")
    original = path.stat()
    replacement = path.with_name("replacement.parquet")
    _write_parquet(replacement, [date(2025, 1, 2)])
    assert replacement.stat().st_size == original.st_size
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    os.replace(replacement, path)

    rechecked = build_record_inventory(tmp_path, refresh=True)

    assert rechecked["identity_rechecked_files"] == 1
    assert rechecked["feature_revision"] != first["feature_revision"]
    assert rechecked["datasets"]["yahoo:crypto"]["first"] == "2025-01-02"


def test_identity_recheck_budget_does_not_starve_new_source(tmp_path: Path) -> None:
    directory = tmp_path / "data_yahoo/crypto"
    for name in ("A", "B", "C"):
        _write_parquet(directory / f"{name}_features.parquet", [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    for entry in cache["files"].values():
        entry.pop("file_identity")
    cache_path.write_text(json.dumps(cache), encoding="utf-8")
    _write_parquet(directory / "D_features.parquet", [date(2025, 1, 2)])

    result = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)

    assert result["identity_rechecked_files"] == 1
    assert result["refreshed_files"] == 1
    assert result["identity_unbound_files"] == 2
    assert result["datasets"]["yahoo:crypto"]["count"] == 4


def test_corrupt_cached_file_map_cannot_reuse_verified_dataset_rollup(tmp_path: Path) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    verified = build_record_inventory(tmp_path, refresh=True)
    assert verified["datasets"]["yahoo:crypto"]["state"] == "verified"
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    cache["files"] = []
    cache_path.write_text(json.dumps(cache), encoding="utf-8")

    current = build_record_inventory(tmp_path)

    assert current["cached_files"] == 0
    assert current["datasets"]["yahoo:crypto"]["state"] == "scanning"
    assert current["datasets"]["yahoo:crypto"]["count"] is None


def test_feature_revision_changes_when_null_statistics_change(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2025, 1, 1)], "value": [1.0]}), path)
    first = build_record_inventory(tmp_path, refresh=True)
    pq.write_table(pa.table({"date": [date(2025, 1, 1)], "value": pa.array([None], type=pa.float64())}), path)

    changed = build_record_inventory(tmp_path, refresh=True)

    assert changed["datasets"]["yahoo:crypto"]["count"] == first["datasets"]["yahoo:crypto"]["count"]
    assert changed["feature_revision"] != first["feature_revision"]
    assert changed["changed_dataset_ids"] == ["yahoo:crypto"]
    field = next(row for row in build_feature_inventory(tmp_path)["rows"] if row["field"] == "value")
    assert field["non_null_count"] == 0


def test_invalid_cached_feature_revision_is_rebuilt_not_reused(tmp_path: Path) -> None:
    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write_parquet(path, [date(2025, 1, 1)])
    build_record_inventory(tmp_path, refresh=True)
    cache_path = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    cache["feature_revision"] = "broken"
    cache_path.write_text(json.dumps(cache), encoding="utf-8")

    repaired = build_record_inventory(tmp_path, refresh=True)

    assert repaired["fast_index_hit"] is False
    assert len(repaired["feature_revision"]) == 32
    assert repaired["feature_revision"] != "broken"
    assert repaired["changed_dataset_ids"] is None


def test_feature_inventory_lists_every_field_by_source_without_doubling_rollups(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    tw = tmp_path / "data_tw_public/features/tw_public_stock_daily.parquet"
    tw.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "date": pa.array([date(2024, 1, 2), date(2024, 1, 3)], type=pa.date32()),
        "twpub_value": pa.array([1.0, None]),
    }), tw)
    crypto = tmp_path / "data_okx/1m/BTC_features.parquet"
    crypto.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "date": pa.array([date(2024, 1, 4)], type=pa.date32()),
        "close": [42.0],
    }), crypto)
    stored = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert stored["physical:tw-public:training-features"]["count"] == 2
    assert stored["group:tw-public"]["count"] is None  # Derived feature table is not a raw-group total.
    raw = build_feature_inventory(tmp_path)
    assert raw["files_with_schema"] == raw["files_total"] == 2
    field = next(row for row in raw["rows"] if row["field"] == "twpub_value")
    assert field["non_null_count"] == 1
    assert field["rows_with_field"] == 2
    assert field["dataset_first"] == "2024-01-02"  # Dataset bound, not field bound.
    status = {"generated_at_utc": "2026-09-17T00:00:00+00:00", "sources": []}
    projected = build_data_monitor_feature_inventory(tmp_path, monitor_status=status)
    assert projected["read_only"] is True
    assert projected["production_control_possible"] is False
    assert projected["rows"][0]["market_category"] == "taiwan_equity"
    assert projected["rows"][-1]["market_category"] == "crypto"
    assert {row["field"] for row in projected["rows"]} == {"date", "twpub_value", "close"}


def test_feature_non_null_count_fails_closed_without_footer_null_statistics(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    first = tmp_path / "data_yahoo/tw_stocks/1111_features.parquet"
    second = tmp_path / "data_yahoo/tw_stocks/2222_features.parquet"
    first.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2024, 1, 2)], "feature": [1.0]}), first)
    pq.write_table(pa.table({"date": [date(2024, 1, 3)], "feature": [None]}), second,
                   write_statistics=False)
    build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    field = next(row for row in build_feature_inventory(tmp_path)["rows"] if row["field"] == "feature")
    assert field["rows_with_field"] == 2
    assert field["non_null_count"] is None
    assert field["verified_partial_non_null"] == 1


def test_unchanged_feature_snapshot_is_reused_until_footer_cache_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    cache = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    feature = tmp_path / "artifacts/live/data_monitor/feature_inventory.json"
    cache.parent.mkdir(parents=True)
    cache.write_text("{}", encoding="utf-8")
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False,
        "rows": [{"field": "close"}],
    }), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 1
    receipt = snapshot_service._feature_reuse_receipt_path(feature)
    assert receipt.is_file()
    assert json.loads(receipt.read_text(encoding="utf-8"))["schema_version"] == 3
    receipt_mtime = receipt.stat().st_mtime_ns
    assert snapshot_service._current_feature_snapshot(feature) == 1
    assert receipt.stat().st_mtime_ns == receipt_mtime
    newer = feature.stat().st_mtime_ns + 1_000_000_000
    os.utime(cache, ns=(newer, newer))
    assert snapshot_service._current_feature_snapshot(feature) is None


def test_feature_snapshot_preserves_contract_without_sorting_json_keys(
    tmp_path: Path,
) -> None:
    feature = tmp_path / "feature_inventory.json"
    payload = {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "rows": [{"dataset_id": "physical:example", "field": "收盤價", "non_null_count": None}],
    }
    snapshot_service._atomic_json(
        feature, payload, compact=True, strict_json=True, sort_keys=False,
    )
    assert json.loads(feature.read_bytes()) == payload
    assert feature.read_text(encoding="utf-8").startswith('{"schema_version":1')
    snapshot_service._write_feature_reuse_receipt(feature, 1)
    assert snapshot_service._current_feature_snapshot(feature) == 1


def test_retained_feature_generation_keeps_count_and_bytes_when_dependencies_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    root = tmp_path / "artifacts/live/data_monitor"
    root.mkdir(parents=True)
    cache = root / "record_inventory_cache.json"
    cache.write_text("{}", encoding="utf-8")
    feature = root / "feature_inventory.json"
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False,
        "generated_at_utc": "2026-10-01T00:00:00+00:00",
        "rows": [{"field": "close"}],
    }), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 1
    receipt = snapshot_service._feature_reuse_receipt_path(feature)
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (feature, receipt)}
    newer = feature.stat().st_mtime_ns + 1_000_000_000
    os.utime(cache, ns=(newer, newer))
    assert snapshot_service._current_feature_snapshot(feature) is None
    assert snapshot_service._current_feature_snapshot(feature, allow_stale_dependencies=True) == 1
    assert {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in before} == before
    feature.write_text(feature.read_text().replace('"close"', '"open"'), encoding="utf-8")
    damaged = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in before}
    assert snapshot_service._current_feature_snapshot(feature, allow_stale_dependencies=True) is None
    assert {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in damaged} == damaged


def test_matching_inventory_revision_reuses_feature_bytes_without_touching_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    root = tmp_path / "artifacts/live/data_monitor"
    root.mkdir(parents=True)
    cache = root / "record_inventory_cache.json"
    feature = root / "feature_inventory.json"
    revision = "a" * 32
    cache.write_text(json.dumps({"feature_revision": revision}), encoding="utf-8")
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False,
        "rows": [{"field": "close"}],
    }), encoding="utf-8")
    snapshot_service._write_feature_reuse_receipt(
        feature, 1, feature_revision=revision,
    )
    initial = feature.stat()
    newer = initial.st_mtime_ns + 1_000_000_000
    os.utime(cache, ns=(newer, newer))

    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
    ) == 1
    assert feature.stat().st_ino == initial.st_ino
    assert feature.stat().st_mtime_ns == initial.st_mtime_ns
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision="b" * 32,
    ) is None

    receipt = snapshot_service._feature_reuse_receipt_path(feature)
    tampered = json.loads(receipt.read_text(encoding="utf-8"))
    tampered["feature_revision_sha256"] = "0" * 64
    receipt.write_text(json.dumps(tampered), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
    ) is None


def test_feature_reuse_refuses_changed_or_unbound_source_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    feature = tmp_path / "artifacts/live/data_monitor/feature_inventory.json"
    feature.parent.mkdir(parents=True)
    snapshot_service._atomic_json(feature, {
        "schema_version": 1, "read_only": True,
        "production_control_possible": False,
        "rows": [{"field": "close", "source_title": "原名稱"}],
    }, compact=True, strict_json=True)
    revision = "a" * 32
    old_metadata = "b" * 64
    new_metadata = "c" * 64
    snapshot_service._write_feature_reuse_receipt(
        feature, 1, feature_revision=revision,
        source_metadata_sha256=old_metadata,
    )
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
        source_metadata_sha256=old_metadata,
    ) == 1
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
        source_metadata_sha256=new_metadata,
    ) is None
    snapshot_service._write_feature_reuse_receipt(
        feature, 1, feature_revision=revision,
    )
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
        source_metadata_sha256=old_metadata,
    ) is None
    snapshot_service._feature_reuse_receipt_path(feature).unlink()
    assert snapshot_service._current_feature_snapshot(
        feature, feature_revision=revision,
        source_metadata_sha256=old_metadata,
    ) is None
    assert not snapshot_service._feature_reuse_receipt_path(feature).exists()


@pytest.mark.parametrize("tamper", ["root", "digest", "missing_root", "missing_receipt", "invalid_json"])
def test_feature_reuse_requires_exact_source_observation_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str,
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    feature = tmp_path / "feature_inventory.json"
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [{"field": "close"}],
    }))
    root = "a" * 32
    snapshot_service._write_feature_reuse_receipt(feature, 1, source_observation_root=root)
    assert snapshot_service._current_feature_snapshot(feature, source_observation_root=root) == 1
    assert snapshot_service._current_feature_snapshot(feature, source_observation_root="b" * 32) is None
    receipt_path = snapshot_service._feature_reuse_receipt_path(feature)
    receipt = json.loads(receipt_path.read_text())
    if tamper == "missing_receipt":
        receipt_path.unlink()
    elif tamper == "invalid_json":
        receipt_path.write_text("{invalid")
    else:
        if tamper == "missing_root":
            receipt.pop("source_observation_root")
        elif tamper == "root":
            receipt["source_observation_root"] = "b" * 32
        else:
            receipt["source_observation_sha256"] = "0" * 64
        receipt_path.write_text(json.dumps(receipt))
    assert snapshot_service._current_feature_snapshot(feature, source_observation_root=root) is None


@pytest.mark.parametrize("root", ["", "A" * 32, "a" * 31, "a" * 33, [], {"invalid": True}])
def test_malformed_source_observation_never_becomes_a_reuse_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, root,
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    feature = tmp_path / "feature_inventory.json"
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False, "rows": [],
    }))
    snapshot_service._write_feature_reuse_receipt(feature, 0, source_observation_root=root)
    assert snapshot_service._current_feature_snapshot(feature, source_observation_root=root) is None


def test_feature_reuse_receipt_rechecks_digest_and_falls_back_to_full_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    feature = tmp_path / "artifacts/live/data_monitor/feature_inventory.json"
    feature.parent.mkdir(parents=True)
    original = {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "rows": [{"field": "close"}],
    }
    feature.write_text(json.dumps(original), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 1
    receipt_path = snapshot_service._feature_reuse_receipt_path(feature)

    corrupted_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    corrupted_receipt["source_sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(corrupted_receipt), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 1
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["source_sha256"] != "0" * 64

    corrupted_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    corrupted_receipt["fields"] = 99
    receipt_path.write_text(json.dumps(corrupted_receipt), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 1

    changed = {**original, "rows": [{"field": "open"}, {"field": "close"}]}
    feature.write_text(json.dumps(changed), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) == 2
    feature.write_text("not json", encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) is None
    feature.write_text(json.dumps(original).replace('"close"', "NaN"), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) is None


def test_crypto_completion_cannot_use_requested_end_date_as_stored_tail() -> None:
    now = datetime(2026, 9, 17, 2, 0, tzinfo=UTC)
    rows = [
        {"id": "group:bybit", "status": "current", "data_through": "2026-09-17", "warnings": [], "eta": {}},
        {"id": "product:bybit_perpetuals:1m", "status": "current", "data_through": "2026-09-17", "warnings": [], "eta": {}},
        {"id": "group:okx", "status": "blocked", "data_through": "2026-09-17", "warnings": [], "eta": {}},
    ]
    inventory = {
        "group:bybit": {"state": "verified", "last": "2026-09-14T09:07:00"},
        "group:okx": {"state": "verified", "last": "2026-09-12T21:54:00"},
    }
    _apply_verified_storage_freshness(rows, inventory, now=now)
    assert rows[0]["status"] == "stale"
    assert rows[0]["data_through"] == "2026-09-14T09:07:00"
    assert rows[0]["requested_data_through"] == "2026-09-17"
    assert rows[1]["status"] == "stale"
    assert rows[2]["status"] == "blocked"  # stronger failure state is preserved
