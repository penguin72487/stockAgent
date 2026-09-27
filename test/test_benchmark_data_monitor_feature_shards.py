from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.benchmark_data_monitor_feature_shards import _row_blocks, benchmark
from scripts.snapshot_data_refresh_services import _write_feature_reuse_receipt


def _source(tmp_path: Path) -> Path:
    path = tmp_path / "feature_inventory.json"
    payload = {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "summary": {"note": "台股", "nullable": None},
        "rows": [
            {"dataset_id": "a", "field": "開盤", "count": 1.0},
            {"dataset_id": "a", "field": "收盤", "count": 2.5},
            {"dataset_id": "b", "field": "volume", "count": 3},
        ],
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n",
        encoding="utf-8",
    )
    _write_feature_reuse_receipt(path, 3, validated_contract=True)
    return path


def test_shard_proof_is_byte_identical_and_does_not_modify_source(tmp_path: Path) -> None:
    source = _source(tmp_path)
    before = source.read_bytes()

    result = benchmark(source, temp_root=tmp_path / "disposable")

    assert result["state"] == "verified"
    assert result["rows"] == 3
    assert result["dataset_shards"] == 2
    assert [trial["variant"] for trial in result["trials"]] == [
        "full", "sharded", "sharded", "full",
    ]
    assert all(trial["exact_source_bytes"] for trial in result["trials"])
    assert source.read_bytes() == before
    assert not list((tmp_path / "disposable").iterdir())


def test_shard_proof_rejects_tampered_receipt(tmp_path: Path) -> None:
    source = _source(tmp_path)
    source.write_bytes(source.read_bytes().replace(b'"volume"', b'"amount"'))

    with pytest.raises(ValueError, match="not trusted"):
        benchmark(source, temp_root=tmp_path / "disposable")


def test_shard_proof_rejects_noncontiguous_dataset_rows() -> None:
    with pytest.raises(ValueError, match="not contiguous"):
        _row_blocks([
            {"dataset_id": "a"}, {"dataset_id": "b"}, {"dataset_id": "a"},
        ])
