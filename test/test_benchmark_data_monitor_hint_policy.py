from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.benchmark_data_monitor_hint_policy import benchmark
from stockagent.live import data_monitor_inventory as inventory


def _cache(path: Path, count: int) -> None:
    path.write_text(json.dumps({
        "version": inventory.INVENTORY_VERSION, "feature_revision": "a" * 32,
        "files": {f"/private/frozen/{i}.parquet": {"file_identity": [1, i, 20, 30, i]}
                  for i in range(count)},
        "recently_refreshed_files": [f"/private/frozen/{i}.parquet" for i in range(max(0, count - 10), count)],
    }))


def test_full_namespace_abba_keeps_unsampled_changes_and_output_parity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(inventory, "FAST_INDEX_PREFLIGHT_HINTS", 2)
    source = tmp_path / "cache.json"
    _cache(source, 100)
    before = source.read_bytes()
    result = benchmark(source)
    assert result["state"] == "verified"
    assert result["full_cache_paths"] == 100
    assert result["production_writes"] is False
    assert result["complete_producer_wall_claim"] is False
    assert source.read_bytes() == before
    assert len(result["trials"]) == 12
    for scenario in result["summary"]:
        trials = [row for row in result["trials"] if row["scenario"] == scenario]
        assert len({row["result_sha256"] for row in trials}) == 1
        assert [row["variant"] for row in trials] == ["uniform", "refreshed", "refreshed", "uniform"]
    recent = result["summary"]["injected_refreshed_path_change"]
    assert recent["uniform"]["metadata_source_calls"] == 102
    assert recent["refreshed"]["metadata_source_calls"] < 102
    unhinted = result["summary"]["injected_unhinted_path_change"]
    assert unhinted["uniform"]["metadata_source_calls"] == unhinted["refreshed"]["metadata_source_calls"] == 102


def test_saturated_hint_set_does_not_invent_a_recent_only_advantage(tmp_path: Path) -> None:
    source = tmp_path / "cache.json"
    _cache(source, 1)
    result = benchmark(source)
    assert set(result["summary"]) == {"unchanged"}
    assert len(result["trials"]) == 4


def test_invalid_namespace_is_rejected_not_measured_as_empty_success(tmp_path: Path) -> None:
    source = tmp_path / "cache.json"
    _cache(source, 1)
    payload = json.loads(source.read_text())
    payload["files"]["/private/frozen/0.parquet"]["file_identity"][0] = True
    source.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="five-part"):
        benchmark(source)
