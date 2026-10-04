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
