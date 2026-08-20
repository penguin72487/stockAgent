from __future__ import annotations

import json
from pathlib import Path


def test_columnar_storage_contract_is_safe_and_has_unique_datasets() -> None:
    payload = json.loads(Path("configs/columnar_storage.json").read_text())
    assert payload["schema_version"] == 1
    assert payload["canonical_fact_format"] == "parquet"
    assert payload["canonical_compression"]["codec"] == "zstd"
    assert payload["canonical_compression"]["level"] == 3
    assert payload["safety"] == {
        "delete_l0_after_compaction": False,
        "atomic_publish_required": True,
        "pyarrow_polars_duckdb_row_parity_required": True,
        "schema_fingerprint_required": True,
        "source_manifest_required": True,
    }
    datasets = payload["datasets"]
    ids = [item["id"] for item in datasets]
    assert len(ids) == len(set(ids))
    assert {"openbb", "tw_minute_research", "tw_microstructure"}.issubset(ids)
