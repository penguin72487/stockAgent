"""The private native metadata writer cannot coerce or discard legacy values."""

from copy import deepcopy
from datetime import date
import hashlib
import json
from pathlib import Path
import struct

import pytest

from stockagent.live import data_monitor_record_codec as codec
from stockagent.live.data_monitor_inventory import InventorySnapshot, build_record_inventory


def _payload(tmp_path: Path) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2026, 10, 1)], "股數": [2**53 + 1]}), source)
    snapshot = InventorySnapshot(tmp_path)
    build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    return snapshot.payload


def _stdlib(payload: object) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def test_native_metadata_is_lossless_and_does_not_mutate_source(tmp_path):
    pytest.importorskip("msgspec")
    payload = _payload(tmp_path)
    original = _stdlib(payload)
    payload["schemas"]["unicode"] = [["台灣𝄞\n\0\"\\", "int64"]]
    expected = json.loads(_stdlib(payload))
    observation = {}
    observed = codec.encode_record_cache(payload, observation=observation)
    assert observation == {"backend": "msgspec_validated_metadata", "fallback_reason": None}
    assert json.loads(observed) == expected
    assert list(json.loads(observed)) == list(payload)
    assert codec.encode_record_cache(json.loads(original)) != b""
    assert json.loads(_stdlib(payload)) == expected


@pytest.mark.parametrize("value", [2**53 + 1, 2**64, 2**128, -(2**128)])
def test_native_counts_retain_arbitrary_integer_type_and_value(tmp_path, value):
    pytest.importorskip("msgspec")
    payload = _payload(tmp_path)
    entry = next(iter(payload["files"].values()))
    entry["stats"]["count"] = value
    entry["stats"]["non_null"] = [None, value]
    observation = {}
    result = json.loads(codec.encode_record_cache(payload, observation=observation))
    assert observation["backend"] == "msgspec_validated_metadata"
    count = next(iter(result["files"].values()))["stats"]["count"]
    assert count == value and type(count) is int


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"),
                                  -0.0, 5e-324, 1.2345678901234567, True])
def test_unexpected_numeric_type_uses_original_writer_without_coercion(tmp_path, value):
    payload = _payload(tmp_path)
    next(iter(payload["files"].values()))["stats"]["non_null"][0] = value
    observation = {}
    body = codec.encode_record_cache(payload, observation=observation)
    assert observation["backend"] == "stdlib"
    assert body == _stdlib(payload)
    observed = next(iter(json.loads(body)["files"].values()))["stats"]["non_null"][0]
    if isinstance(value, float):
        assert struct.pack("!d", observed) == struct.pack("!d", value)
    else:
        assert type(observed) is bool


@pytest.mark.parametrize("boundary", ["root", "entry", "stats", "dataset"])
def test_unknown_fields_are_preserved_at_every_boundary(tmp_path, boundary):
    payload = _payload(tmp_path)
    entry = next(iter(payload["files"].values()))
    target = {"root": payload, "entry": entry, "stats": entry["stats"],
              "dataset": next(iter(payload["datasets"].values()))}[boundary]
    target["future_extension"] = {"raw": [float("inf"), 2**128, "x"]}
    observation = {}
    assert codec.encode_record_cache(payload, observation=observation) == _stdlib(payload)
    assert observation["backend"] == "stdlib"


def test_dependency_absence_and_legacy_shape_keep_original_writer(tmp_path, monkeypatch):
    payload = _payload(tmp_path)
    monkeypatch.setattr(codec, "_native", None)
    assert codec.encode_record_cache(payload) == _stdlib(payload)
    for legacy in (None, {"version": 7, "files": {}}, [None, 2**128, -0.0]):
        assert codec.encode_record_cache(legacy) == _stdlib(legacy)


def test_surrogates_and_unsupported_json_values_keep_stdlib_behavior(tmp_path):
    payload = _payload(tmp_path)
    payload["schemas"]["surrogate"] = [["\ud800", "string"]]
    assert codec.encode_record_cache(payload) == _stdlib(payload)
    for unsupported in (b"not a JSON string", date(2026, 10, 14), {1, 2}):
        variant = deepcopy(payload)
        variant["unknown"] = unsupported
        with pytest.raises(TypeError):
            codec.encode_record_cache(variant)
    payload["cycle"] = payload
    with pytest.raises(ValueError):
        codec.encode_record_cache(payload)


def test_complete_benchmark_keeps_values_and_reports_changed_private_bytes(tmp_path):
    pytest.importorskip("msgspec")
    from scripts import benchmark_data_monitor_record_codec as benchmark

    payload = _payload(tmp_path)
    source = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    captured_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    trial_seed = _stdlib(payload)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"sources": []}))
    result = benchmark.benchmark(tmp_path, repeats=1, mode="encode")
    assert result["state"] == "verified"
    assert result["parity"] is True
    assert result["native_encoder_used"] is True
    assert result["cache_bytes_parity"] is False
    assert len({row["source_stat_calls"] for row in result["trials"]}) == 1
    assert result["production_source_writes"] == 0
    assert result["captured_source_sha256"] == captured_sha
    assert result["input_sha256"] == hashlib.sha256(trial_seed).hexdigest()
    assert result["input_bytes"] == len(trial_seed)
