from __future__ import annotations

from datetime import date
import json
from pathlib import Path

from scripts import benchmark_data_monitor_projection_pipeline as benchmark
from stockagent.live.data_monitor_inventory import build_record_inventory


def _write(path: Path, value: float) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"date": [date(2026, 10, 1)], "close": [value]}), path)


def test_full_namespace_delta_replay_keeps_all_rows_and_source_checks(tmp_path, monkeypatch):
    source = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write(source, 1.0)
    build_record_inventory(tmp_path, refresh=True)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"generated_at_utc": "2026-10-02T00:00:00+00:00", "sources": []}))
    first = benchmark.capture(tmp_path)
    _write(source, 2.0)
    build_record_inventory(tmp_path, refresh=True)
    second = benchmark.capture(tmp_path)
    generations = iter((first, second))
    monkeypatch.setattr(benchmark, "capture", lambda root: next(generations))
    result = benchmark.benchmark(tmp_path, timeout_seconds=1.0)
    assert result["state"] == "verified"
    assert result["production_writes"] is False and result["promotion"] is False
    assert result["all_source_identities_checked"] is True
    assert result["complete_bytes_parity"] is True
    assert result["changed_dataset_ids"] == ["yahoo:crypto"]
    assert result["unique_selected_paths"] == result["selected_file_references"] == 1
    assert len(result["cold_seed_trials"]) == 2
    assert [trial["variant"] for trial in result["trials"]] == ["full", "sharded", "sharded", "full"]
    assert {trial["fields"] for trial in result["trials"]} == {2}


def test_no_new_generation_is_not_a_success(tmp_path, monkeypatch):
    source = tmp_path / "data_yahoo/crypto/A_features.parquet"
    _write(source, 1.0)
    build_record_inventory(tmp_path, refresh=True)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"generated_at_utc": "2026-10-02T00:00:00+00:00", "sources": []}))
    result = benchmark.benchmark(tmp_path, timeout_seconds=0.0)
    assert result["state"] == "no_new_generation"
    assert result["production_writes"] is False
