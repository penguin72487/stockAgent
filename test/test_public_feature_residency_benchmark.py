import json
import random
import struct
from types import SimpleNamespace

import pytest

from scripts import benchmark_public_feature_residency as benchmark


@pytest.mark.parametrize("native", [True, False])
def test_shared_decoder_keeps_unknown_metadata_integer_float_unicode_and_order(monkeypatch, native):
    if not native:
        monkeypatch.setattr(benchmark.pages, "_native", None)
    body = ('{"rows":[{"provider":"台灣來源","source_title":"同一來源","dataset_id":"tw:股",'
            '"market_category":"taiwan_equity","market_category_label":"台股",'
            '"field":"x","extra":[null,true,9007199254740993,18446744073709551616,-0.0,1e-300]},'
            '{"provider":"台灣來源","source_title":"同一來源","dataset_id":"tw:股","field":"y",'
            '"market_category":"taiwan_equity","market_category_label":"台股"}],'
            '"schema_version":1,"read_only":true,"production_control_possible":false,"summary":{"fields":2},'
            '"unknown":{"provider":["not a string"],"flag":false}}').encode()
    baseline, shared = (benchmark.decode(body, variant) for variant in ("baseline", "shared"))
    assert json.dumps(baseline, ensure_ascii=False) == json.dumps(shared, ensure_ascii=False)
    assert list(baseline) == list(shared)
    assert shared["rows"][0]["provider"] is shared["rows"][1]["provider"]
    assert shared["rows"][0]["dataset_id"] is shared["rows"][1]["dataset_id"]
    assert shared["rows"][0]["extra"][2] == 9007199254740993
    assert struct.pack("d", shared["rows"][0]["extra"][-2]) == struct.pack("d", -0.0)
    assert type(shared["rows"][0]["extra"][1]) is bool


@pytest.mark.parametrize("variant", ["baseline", "shared"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_both_feature_decoders_reject_nonfinite_observations(variant, value):
    with pytest.raises(ValueError, match="non.?finite.*JSON"):
        benchmark.decode(f'{{"unknown":{value}}}', variant)


def test_label_pool_is_bounded_and_never_mutates_values_or_drops_rows(monkeypatch):
    monkeypatch.setattr(benchmark.pages, "FEATURE_LABEL_POOL_LIMIT", 2)
    payload = {"schema_version": 1, "read_only": True, "production_control_possible": False,
               "summary": {"fields": 7}, "rows": [{"provider": f"source {index}",
               "source_title": "source", "dataset_id": "a", "field": "value", "market_category": "macro",
               "market_category_label": "總經"} for index in (0, 1, 2, 3, 2, 1, 0)]}
    shared = benchmark.decode(json.dumps(payload), "shared")
    assert shared == payload
    assert shared["rows"][0]["provider"] is shared["rows"][-1]["provider"]
    assert shared["rows"][2]["provider"] == shared["rows"][4]["provider"]
    # The bounded per-decode table is not sys.intern/global process state.
    other = benchmark.decode(json.dumps(payload), "shared")
    assert other == shared and other is not shared


def test_native_unsupported_json_uses_strict_legacy_fallback(monkeypatch):
    def unsupported(body):
        raise ValueError("legacy unsupported representation")
    monkeypatch.setattr(benchmark.pages, "_native", SimpleNamespace(json=SimpleNamespace(decode=unsupported)))
    payload = {"rows": [{"provider": "x", "field": "unpaired-surrogate-\ud800"}], "unknown": 9007199254740993}
    assert benchmark.decode(json.dumps(payload), "shared") == payload
    with pytest.raises(ValueError, match="non-finite JSON"):
        benchmark.decode('{"bad":NaN}', "shared")


def test_native_metadata_decoder_preserves_many_exact_floating_bits():
    if benchmark.pages._native is None:
        pytest.skip("optional native decoder unavailable")
    generator = random.Random(6147)
    values = [-0.0, 0.0, 5e-324, -5e-324, 1.7976931348623157e308, 2.2250738585072014e-308]
    while len(values) < 4096:
        value = struct.unpack("d", generator.randbytes(8))[0]
        if value == value and abs(value) != float("inf"):
            values.append(value)
    body = json.dumps({"schema_version": 1, "read_only": True, "production_control_possible": False,
                       "summary": {"fields": 0}, "rows": [], "unknown_floats": values})
    actual = benchmark.decode(body, "shared")["unknown_floats"]
    assert [struct.pack("d", value) for value in actual] == [struct.pack("d", value) for value in values]
