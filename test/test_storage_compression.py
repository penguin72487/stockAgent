from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import benchmark_storage_compression as trial


def test_codec_trial_preserves_negative_raw_values_nulls_and_ratios(tmp_path, monkeypatch):
    monkeypatch.setattr(trial.shutil, "disk_usage", lambda _path: SimpleNamespace(free=100 * 1024**3))
    path = tmp_path / "values.parquet"
    table = pa.table({"raw": [-100.5, None, 0., float("nan"), 0.25], "ratio": [-.5, 1., 0., None, 9.5]})
    pq.write_table(table, path, compression="zstd")
    before = path.read_bytes()
    result = trial.benchmark([path], tmp_path / "receipt.json")
    assert path.read_bytes() == before
    assert result["source_files_modified"] == 0
    assert all(r["full_value_roundtrip_verified"] for r in result["files"][0]["candidates"])
    assert len(result["files"][0]["candidates"]) == 4
    assert not list(tmp_path.glob("storage-codec-*"))


def test_codec_input_limit_precedes_hash_or_parquet_payload_reads(tmp_path, monkeypatch):
    monkeypatch.setattr(trial.shutil, "disk_usage", lambda _path: SimpleNamespace(free=100 * 1024**3))
    path = tmp_path / "huge.parquet"
    with path.open("wb") as stream:
        stream.truncate(2 * 1024**2)
    monkeypatch.setattr(trial, "digest", lambda _path: (_ for _ in ()).throw(AssertionError("payload hashed")))
    result = trial.benchmark([path], tmp_path / "receipt.json", max_input_mib=1)
    assert result["files"][0]["state"] == "deferred_input_limit_no_payload_read"
    assert result["files"][0]["source_sha256"] is None
