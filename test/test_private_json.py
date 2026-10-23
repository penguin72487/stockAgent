"""Protect exact counts, non-finite values and unusual legacy JSON inputs."""

import json
import math

import pytest

from stockagent import private_json


@pytest.mark.parametrize("value", [2**53 + 1, 2**64 - 1, 2**64, 2**128,
                                  -(2**63), -(2**63)-1, -(2**128)])
def test_private_json_preserves_integer_value_and_type(value):
    for document in (value, {"rows": value}, [value], {"nested": [{"rows": value}]}):
        body = json.dumps(document).encode()
        result = private_json.loads(body)
        assert result == document
        assert private_json.loads(private_json.dumps(document)) == document
    assert type(private_json.loads(str(value).encode())) is int


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_private_json_keeps_nonfinite_legacy_values(value):
    observed = private_json.loads(private_json.dumps({"value": value}))["value"]
    if math.isnan(value):
        assert math.isnan(observed)
    else:
        assert observed == value


def test_private_json_unicode_and_numeric_strings_are_preserved():
    value = {"名稱": "台灣", "id": "184467440737095516160000", "nested": [None, True, 1.25],
             "legacy_surrogate": "\ud800"}
    assert private_json.loads(private_json.dumps(value)) == value


def test_private_json_keeps_stdlib_error_and_fallback_behavior(monkeypatch):
    monkeypatch.setattr(private_json, "_native", None)
    assert private_json.loads(private_json.dumps({"x": 2**128})) == {"x": 2**128}
    with pytest.raises(json.JSONDecodeError):
        private_json.loads(b'{"rows":')


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-32"])
def test_private_json_preserves_legacy_input_encoding(encoding):
    body = json.dumps({"名稱": "台灣", "count": 2**128}).encode(encoding)
    assert private_json.loads(body) == json.loads(body)


def test_private_json_rejects_bytes_and_cycles_like_stdlib():
    with pytest.raises(TypeError):
        private_json.dumps({"value": b"encoded bytes are not JSON strings"})
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError):
        private_json.dumps(cycle)


@pytest.mark.parametrize("value", [-0.0, 5e-324, 1.2345678901234567, 1e300])
def test_private_json_preserves_finite_float_bits(value):
    import struct
    observed = private_json.loads(private_json.dumps(value))
    assert struct.pack("!d", observed) == struct.pack("!d", value)


@pytest.mark.parametrize("native", [True, False])
def test_object_projection_preserves_exact_values_and_missing_fields(monkeypatch, native):
    import struct
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    fields = ("count", "signed_zero", "nested", "missing", "legacy")
    source = {"count": 2**128, "signed_zero": -0.0,
              "nested": [True, None, {"value": 5e-324}], "legacy": "\ud800", "unused": [1, 2]}
    for body in (json.dumps(source), json.dumps(source).encode("utf-16")):
        result = private_json.projected_object_values(body, fields)
        assert result == tuple(source.get(key) for key in fields)
        assert struct.pack("!d", result[1]) == struct.pack("!d", -0.0)


def test_object_projection_duplicates_nonobjects_and_invalid_documents():
    assert private_json.projected_object_values('{"x":1,"x":2}', ("x",)) == (2,)
    for value in (None, True, 123, [1, 2], "text"):
        assert private_json.projected_object_values(json.dumps(value), ("x",)) is None
    assert math.isnan(private_json.projected_object_values('{"x":NaN}', ("x",))[0])
    with pytest.raises(json.JSONDecodeError):
        private_json.projected_object_values('{"x":', ("x",))


@pytest.mark.skipif(not private_json.native_available(), reason="optional native decoder unavailable")
@pytest.mark.parametrize("value", [-0.0, 5e-324, 1.2345678901234567, 2**53 + 1])
def test_projected_numeric_fast_path_preserves_values_without_legacy_fallback(monkeypatch, value):
    import struct

    def unexpected_fallback(body):
        raise AssertionError("ordinary financial values should use the native projection")

    monkeypatch.setattr(private_json, "loads", unexpected_fallback)
    result = private_json.projected_object_values(json.dumps({"price": value, "unused": [1, 2]}), ("price",))
    assert result == (value,)
    assert type(result[0]) is type(value)
    if isinstance(value, float):
        assert struct.pack("!d", result[0]) == struct.pack("!d", value)


@pytest.mark.parametrize("unused", ['"a\x00b"', '"\\q"', '01', '[1,]'])
def test_projection_rejects_malformed_unused_fields(unused):
    with pytest.raises(json.JSONDecodeError):
        private_json.projected_object_values('{"price":1,"unused":' + unused + '}', ("price",))


@pytest.mark.parametrize("native", [True, False])
def test_grouped_projection_preserves_exact_values_order_and_legacy_rows(monkeypatch, native):
    import struct
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    lines = ["\n", '{"x":18446744073709551617,"y":-0.0}\n',
             '{"x":1,"x":2,"y":5e-324}\n', '{}\n',
             'null\n', '{"x":"legacy","y":"\\ud800"}\n']
    observed = list(private_json.projected_object_rows(lines, ("x", "y")))
    expected = [tuple(row.get(k) for k in ("x", "y"))
                for line in lines if line.strip() and isinstance(row := json.loads(line), dict)]
    assert observed == expected
    assert type(observed[0][0]) is int
    assert struct.pack("!d", observed[0][1]) == struct.pack("!d", -0.0)
    assert struct.pack("!d", observed[1][1]) == struct.pack("!d", 5e-324)


@pytest.mark.parametrize("lines", [
    ['{} {}\n'], ['{\n', '"x":1\n', '}\n'],
    ['{} {}\n', '{"x":\n', '1}\n'],
    ['{"x":1,"unused":[1,]}\n'],
    ['{"x":1}\n', '{"x":'],
])
def test_grouped_projection_keeps_strict_one_document_per_line(lines):
    with pytest.raises(json.JSONDecodeError):
        list(private_json.projected_object_rows(lines, ("x",)))


@pytest.mark.parametrize("native", [True, False])
@pytest.mark.parametrize("whitespace", ["\v", "\f", "\u00a0", "\u2003"])
@pytest.mark.parametrize("placement", ["prefix", "suffix"])
def test_grouped_projection_rejects_non_json_record_whitespace(monkeypatch, native, whitespace, placement):
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    invalid = whitespace + '{"x":1}' if placement == "prefix" else '{"x":1}' + whitespace
    rows = private_json.projected_object_rows(['{"x":7}\n', invalid + "\n"], ("x",))
    assert next(rows) == (7,)
    with pytest.raises(json.JSONDecodeError):
        next(rows)


@pytest.mark.parametrize("native", [True, False])
def test_grouped_projection_retains_legacy_blank_line_skipping(monkeypatch, native):
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    assert list(private_json.projected_object_rows(
        ['\v\f\u00a0\u2003\n', '{"x":1}\n', '\t \n'], ("x",)
    )) == [(1,)]


@pytest.mark.skipif(not private_json.native_available(), reason="optional native decoder unavailable")
def test_grouped_projection_uses_native_batch_without_single_row_fallback(monkeypatch):
    def unexpected_single_row(*_args):
        raise AssertionError("ordinary complete object lines should use one native decode")
    monkeypatch.setattr(private_json, "projected_object_values", unexpected_single_row)
    assert list(private_json.projected_object_rows(
        [' {"x":1}\r\n', '\t{"x":2,"unused":{"nested":[1,2]}}\n', ' \n'], ("x",)
    )) == [(1,), (2,)]


@pytest.mark.parametrize("native", [True, False])
@pytest.mark.parametrize("value", [
    {}, [], None, True, 0, 2**128, -(2**128), 10**100,
    {"名稱": "台灣😀\u2028\u2029", "controls": "".join(map(chr, range(32))),
     "escape": '"\\/', "nested": [{"order": [3, 1, None, False]}, {}]},
    {"floats": [-0.0, 5e-324, 1.2345678901234567, 1e300, float("nan"), float("inf")]},
    {1: "int key", None: "null key", "tuple": (1, True)},
])
def test_pretty_metadata_keeps_exact_stdlib_wire_bytes(monkeypatch, native, value):
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    expected = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
    assert private_json.pretty_metadata_bytes(value) == expected


@pytest.mark.skipif(not private_json.native_available(), reason="optional native formatter unavailable")
@pytest.mark.parametrize("integer", [-(2**63), 2**63-1, 2**64-1])
def test_pretty_metadata_uses_guarded_native_integer_encoding(monkeypatch, integer):
    from types import SimpleNamespace
    formatter = private_json._native.json.format
    encoder = private_json._native.json.encode
    calls = []

    def observe(body, *, indent):
        calls.append(body)
        return formatter(body, indent=indent)

    monkeypatch.setattr(private_json, "_native", SimpleNamespace(json=SimpleNamespace(format=observe, encode=encoder)))
    value = {"count": integer, "files": [{"path": "測試\n", "bytes": 7, "kept": True}]}
    assert private_json.pretty_metadata_bytes(value) == json.dumps(value, ensure_ascii=False, indent=2).encode()
    assert calls == [json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()]


def test_pretty_metadata_custom_containers_and_deep_trees_keep_stdlib_behavior():
    class LegacyList(list):
        def __iter__(self):
            return iter(["overridden", 2**128])

    value = LegacyList([1, 2])
    assert private_json.pretty_metadata_bytes(value) == json.dumps(value, ensure_ascii=False, indent=2).encode()
    value = {"leaf": 1}
    for _ in range(70):
        value = {"nested": value}
    assert private_json.pretty_metadata_bytes(value) == json.dumps(value, ensure_ascii=False, indent=2).encode()


@pytest.mark.parametrize("native", [True, False])
def test_pretty_metadata_rejects_cycles_bytes_and_unpaired_surrogates(monkeypatch, native):
    if not native:
        monkeypatch.setattr(private_json, "_native", None)
    cycle = []
    cycle.append(cycle)
    for value, error in ((cycle, ValueError), ({"raw": b"bytes"}, TypeError),
                         ({"legacy": "\ud800"}, UnicodeEncodeError)):
        with pytest.raises(error) as expected:
            json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        with pytest.raises(error) as observed:
            private_json.pretty_metadata_bytes(value)
        assert str(observed.value) == str(expected.value)


def test_pretty_metadata_formatter_failure_keeps_legacy_encoding(monkeypatch):
    from types import SimpleNamespace

    def unavailable(*_args, **_kwargs):
        raise ValueError("formatter cannot handle document")

    encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    monkeypatch.setattr(private_json, "_native", SimpleNamespace(json=SimpleNamespace(format=unavailable, encode=encode)))
    value = {"count": 2**63-1, "empty": []}
    assert private_json.pretty_metadata_bytes(value) == json.dumps(value, ensure_ascii=False, indent=2).encode()


@pytest.mark.parametrize("value", [2**64, -(2**63)-1, 2**128, 10**100, 1.0, -0.0,
                                  float("nan"), float("inf"), (1, 2)])
def test_pretty_metadata_native_guard_keeps_legacy_numeric_and_container_encoder(monkeypatch, value):
    from types import SimpleNamespace

    def unexpected(*_args, **_kwargs):
        raise AssertionError("legacy values must retain the stdlib encoder")

    monkeypatch.setattr(private_json, "_native", SimpleNamespace(json=SimpleNamespace(format=unexpected, encode=unexpected)))
    document = {"value": value, "normal": [1, "台灣", None]}
    assert private_json.pretty_metadata_bytes(document) == json.dumps(document, ensure_ascii=False, indent=2).encode()


def test_pretty_metadata_deterministic_nested_unicode_wire_oracle():
    import random

    rng = random.Random(20261023)
    alphabet = ''.join(map(chr, range(128))) + "台灣😀\u2028\u2029"

    def node(depth):
        kind = rng.randrange(6 if depth < 5 else 4)
        if kind == 0:
            return ''.join(rng.choices(alphabet, k=rng.randrange(20)))
        if kind == 1:
            return rng.choice([-(2**63), 2**64-1, rng.randrange(-(2**63), 2**64)])
        if kind == 2:
            return rng.choice([None, True, False])
        if kind == 3:
            return []
        if kind == 4:
            return [node(depth+1) for _ in range(rng.randrange(5))]
        return {''.join(rng.choices(alphabet, k=rng.randrange(10))): node(depth+1)
                for _ in range(rng.randrange(5))}

    for _ in range(300):
        value = node(0)
        assert private_json.pretty_metadata_bytes(value) == json.dumps(value, ensure_ascii=False, indent=2).encode()
