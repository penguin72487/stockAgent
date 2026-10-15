"""Deferred footer statistics must retain complete, exact legacy semantics."""

from copy import deepcopy
from datetime import date
import json
import struct

import pytest

from scripts import benchmark_data_monitor_record_codec as benchmark
from stockagent import private_json
from scripts import data_monitor_deferred_counts_prototype as deferred
from stockagent.live import data_monitor_inventory as inventory
from stockagent.live import data_monitor_record_codec as codec


pytestmark = pytest.mark.skipif(deferred._native is None, reason="native prototype is unavailable")


def _payload(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2026, 10, 1)], "股數": [2**53 + 1]}), source)
    snapshot = inventory.InventorySnapshot(tmp_path)
    inventory.build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    return snapshot.payload


def plain(value):
    return json.loads(deferred.encode_record_cache(value))


def test_complete_schema_order_omissions_and_native_bytes_are_identical(tmp_path):
    payload = _payload(tmp_path)
    for path, entry in list(payload["files"].items()):
        entry["stats"] = dict(reversed(list(entry["stats"].items())))
        payload["files"][path] = dict(reversed(list(entry.items())))
    payload = dict(reversed(list(payload.items())))
    body = codec.encode_record_cache(payload)
    observation = {}
    decoded = deferred.decode_record_cache(body, observation=observation)
    assert observation["backend"] == "msgspec_deferred_counts"
    assert type(next(iter(decoded["files"].values()))["stats"]["non_null"]) is deferred.DeferredCounts
    assert plain(decoded) == payload
    assert deferred.encode_record_cache(decoded) == body
    assert list(decoded) == list(payload)
    assert len(decoded["files"]) == len(payload["files"])


@pytest.mark.parametrize("value", [None, 0, -1, 2**53 + 1, 2**64, 2**128, -(2**128)])
def test_exact_integer_null_arrays_and_one_expansion_per_iterator(value, monkeypatch):
    counts = deferred.DeferredCounts(json.dumps([value, 0]).encode())
    calls = []
    original = deferred.DeferredCounts.to_list
    monkeypatch.setattr(deferred.DeferredCounts, "to_list", lambda self: (calls.append(1), original(self))[1])
    assert len(counts) == 2
    assert calls == []
    assert list(zip(counts, [1, 2])) == [(value, 1), (0, 2)]
    assert calls == [1]
    assert counts == [value, 0] and [value, 0] == counts
    assert counts == deferred.DeferredCounts(json.dumps([value, 0]).encode())
    assert (counts == (value, 0)) is False
    with pytest.raises(AttributeError):
        counts._encoded = b"[9]"
    with pytest.raises(AttributeError):
        del counts._encoded
    with pytest.raises(TypeError):
        hash(counts)


def test_integer_negative_zero_and_whitespace_have_canonical_bytes():
    left, right = deferred.DeferredCounts(b'[ -0, null, 3 ]'), deferred.DeferredCounts(b'[0,null,3]')
    assert left == right
    assert left._encoded == b'[0,null,3]'


@pytest.mark.parametrize("value", [True, -0.0, 1.5, float("inf"), -float("inf"), float("nan"), "1", {}])
def test_legacy_array_types_fall_back_without_numeric_coercion(tmp_path, value):
    payload = _payload(tmp_path)
    next(iter(payload["files"].values()))["stats"]["non_null"][0] = value
    body = json.dumps(payload, separators=(",", ":")).encode()
    observation = {}
    decoded = deferred.decode_record_cache(body, observation=observation)
    assert observation["backend"] == "private_json"
    raw = next(iter(decoded["files"].values()))["stats"]["non_null"][0]
    assert type(raw) is type(value)
    if isinstance(value, float):
        assert struct.pack("!d", raw) == struct.pack("!d", value)
    else:
        assert raw == value
    assert deferred.encode_record_cache(decoded) == codec.encode_record_cache(payload)


@pytest.mark.parametrize("boundary", ["root", "entry", "stats", "dataset"])
def test_unknown_fields_are_not_dropped_in_any_object(tmp_path, boundary):
    payload = _payload(tmp_path)
    entry = next(iter(payload["files"].values()))
    target = {"root": payload, "entry": entry, "stats": entry["stats"],
              "dataset": next(iter(payload["datasets"].values()))}[boundary]
    target["future_extension"] = {"n": [2**128, None, "台灣𝄞"]}
    observation = {}
    body = codec.encode_record_cache(payload)
    decoded = deferred.decode_record_cache(body, observation=observation)
    assert decoded == payload
    assert observation["backend"] == "private_json"
    assert deferred.encode_record_cache(decoded) == body


def test_future_writer_fallback_expands_only_known_vectors_and_preserves_errors(tmp_path, monkeypatch):
    payload = _payload(tmp_path)
    decoded = deferred.decode_record_cache(codec.encode_record_cache(payload))
    payload["future"] = decoded["future"] = {"n": [2**128, -0.0, float("inf")]}
    assert deferred.encode_record_cache(decoded) == codec.encode_record_cache(payload)
    for value in (date(2026, 10, 15), b"unsupported", {1, 2}):
        decoded["future"] = value
        with pytest.raises(TypeError):
            deferred.encode_record_cache(decoded)
    decoded["future"] = decoded
    with pytest.raises(ValueError, match="Circular"):
        deferred.encode_record_cache(decoded)
    del decoded["future"]
    del payload["future"]
    monkeypatch.setattr(deferred, "_native", None)
    assert plain(decoded) == payload
    assert deferred.decode_record_cache(json.dumps(payload).encode()) == payload


@pytest.mark.parametrize("body", [b"{", b"null", b"[]", b'{"version":7}',
                                  b'{"x": NaN}', b'{"x":"\\ud800"}',
                                  '{"x": "台灣"}'.encode("utf-16")])
def test_legacy_shape_and_encoding_match_the_original_decoder(body):
    try:
        expected = private_json.loads(body)
    except (ValueError, UnicodeError) as exc:
        with pytest.raises(type(exc)):
            deferred.decode_record_cache(body)
    else:
        assert json.dumps(deferred.decode_record_cache(body)) == json.dumps(expected)


def test_full_feature_math_matches_and_expands_once_per_member(tmp_path, monkeypatch):
    payload = _payload(tmp_path)
    cache = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    expected = inventory.build_feature_inventory(tmp_path)
    decoded = deferred.decode_record_cache(cache.read_bytes())
    snapshot = inventory.InventorySnapshot(tmp_path, payload=decoded)
    calls = []
    original = deferred.DeferredCounts.to_list
    monkeypatch.setattr(deferred.DeferredCounts, "to_list", lambda self: (calls.append(1), original(self))[1])
    monkeypatch.setattr(inventory, "_COUNT_VECTOR_TYPE", (list, deferred.DeferredCounts))
    actual = inventory.build_feature_inventory(tmp_path, snapshot=snapshot)
    assert actual == expected
    assert len(calls) == 1
    assert plain(decoded) == payload


def test_complete_replay_parity_includes_all_fields_and_atomic_writes(tmp_path):
    _payload(tmp_path)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"sources": []}))
    result = benchmark.benchmark(tmp_path, repeats=1, mode="deferred")
    assert result["state"] == "verified"
    assert result["parity"] and result["cache_bytes_parity"] and result["feature_bytes_parity"]
    assert result["all_source_identities_checked"] and result["deferred_decoder_used"]
    assert result["interpreter_gc_unchanged"] and result["code_stable"]
    assert {row["features"] for row in result["trials"]} == {2}


def test_list_inputs_are_not_mutated_even_on_legacy_fallback(tmp_path):
    payload = _payload(tmp_path)
    prior = deepcopy(payload)
    assert deferred.encode_record_cache(payload) == codec.encode_record_cache(payload)
    assert payload == prior


def test_dict_first_mapping_full_replay_keeps_abc_and_every_output(tmp_path):
    _payload(tmp_path)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"sources": []}))
    mapping_type = inventory.Mapping
    result = benchmark.benchmark(tmp_path, repeats=1, mode="mapping")
    assert inventory.Mapping is mapping_type
    assert result["state"] == "verified"
    assert result["parity"] and result["cache_bytes_parity"] and result["feature_bytes_parity"]
    assert result["all_source_identities_checked"] and result["code_stable"]
