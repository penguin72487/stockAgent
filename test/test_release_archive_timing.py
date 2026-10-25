"""Release-call telemetry keeps artifacts, returns and native failures intact."""
from concurrent.futures import ThreadPoolExecutor
import importlib
import inspect
import io
import json

import pytest

from downloader import release_archive_timing as timing


def event_lines(stream):
    return [json.loads(line.removeprefix(timing.PREFIX)) for line in stream.getvalue().splitlines()]


@pytest.mark.parametrize("dataset,filename", timing.SOURCES.items())
def test_canonical_collectors_use_shared_clock(dataset, filename):
    module = importlib.import_module("downloader." + filename.removesuffix(".py"))
    assert hasattr(module.collect, "__wrapped__")
    assert inspect.signature(module.collect) == inspect.signature(module.collect.__wrapped__)
    assert module.collect.__wrapped__.__name__ == "collect"


def test_complete_call_clock_includes_body_write_and_preserves_object(monkeypatch, capsys, tmp_path):
    output = io.StringIO()
    monkeypatch.setattr(timing.sys, "stderr", output)
    moments = iter((100, 900))
    monkeypatch.setattr("downloader.operation_timing.time.perf_counter_ns", lambda: next(moments))
    result = {"dataset": "mof_macro_release_dates", "status": "degraded", "value": -0.0}
    artifact = tmp_path / "state.json"
    def collect(path, *, flag):
        assert flag is False
        path.write_bytes(b"original state bytes")
        return result
    # Declare the real native owner filename without substituting its business body.
    collect.__code__ = collect.__code__.replace(co_filename=timing.SOURCES[result["dataset"]])
    observed = timing.timed_release_archive(result["dataset"])(collect)
    assert observed(artifact, flag=False) is result
    assert artifact.read_bytes() == b"original state bytes"
    assert capsys.readouterr().out == ""
    record, = event_lines(output)
    assert record["status"] == "degraded" and record["outcome"] == "returned"
    assert record["source_binding_eligible"] is True
    assert record["operation_timing"]["seconds"] == 800 / 1e9
    assert record["operation_timing"]["scope"] == timing.SCOPE
    assert record["operation_timing"]["started_monotonic_ns"] == 100
    assert record["work_units_denominator"] is None
    assert record["acquisition_completeness_verified"] is False
    assert "original state bytes" not in output.getvalue()


@pytest.mark.parametrize("error", [ValueError("native failure"), KeyboardInterrupt(), SystemExit(7)])
def test_native_failure_and_interrupt_propagate_once(monkeypatch, error):
    output, calls = io.StringIO(), []
    monkeypatch.setattr(timing.sys, "stderr", output)
    def collect():
        calls.append(1)
        raise error
    observed = timing.timed_release_archive("mof_macro_release_dates")(collect)
    with pytest.raises(type(error)) as caught:
        observed()
    assert caught.value is error and calls == [1]
    record, = event_lines(output)
    assert record["outcome"] == "raised" and record["status"] is None
    assert record["source_binding_eligible"] is False
    assert "native failure" not in output.getvalue()


@pytest.mark.parametrize("status", ["complete", "partial", "degraded", "unsupported"])
def test_real_owner_status_is_preserved_and_unsupported_does_not_bind(monkeypatch, status):
    output = io.StringIO()
    monkeypatch.setattr(timing.sys, "stderr", output)
    result = {"dataset": "mof_macro_release_dates", "status": status}
    def collect():
        return result
    collect.__code__ = collect.__code__.replace(co_filename=timing.SOURCES[result["dataset"]])
    assert timing.timed_release_archive(result["dataset"])(collect)() is result
    record, = event_lines(output)
    assert record["status"] == status
    assert record["source_binding_eligible"] is (status != "unsupported")


@pytest.mark.parametrize("fault", ["init", "snapshot", "write", "flush"])
@pytest.mark.parametrize("failed", [False, True])
def test_observation_fault_does_not_replace_native_outcome(monkeypatch, fault, failed):
    native_error = ValueError("native error")
    result, calls = object(), []
    def collect():
        calls.append(1)
        if failed:
            raise native_error
        return result
    class FaultClock:
        started_ns = 1
        def __init__(self):
            if fault == "init":
                raise OSError("clock unavailable")
        def snapshot(self, scope):
            if fault == "snapshot":
                raise OSError("clock unavailable")
            return {"invalid_clock": False, "seconds": 1, "scope": scope}
    class FaultStream(io.StringIO):
        def write(self, value):
            if fault == "write":
                raise OSError("log unavailable")
            return super().write(value)
        def flush(self):
            if fault == "flush":
                raise OSError("log unavailable")
            return super().flush()
    monkeypatch.setattr(timing, "OperationTiming", FaultClock)
    monkeypatch.setattr(timing.sys, "stderr", FaultStream())
    observed = timing.timed_release_archive("mof_macro_release_dates")(collect)
    if failed:
        with pytest.raises(ValueError) as caught:
            observed()
        assert caught.value is native_error
    else:
        assert observed() is result
    assert calls == [1]


@pytest.mark.parametrize("result", [None, {"dataset": "wrong", "status": "complete"},
                                   {"dataset": "mof_macro_release_dates", "status": 1}])
def test_mismatched_or_unknown_result_is_not_binding(monkeypatch, result):
    output = io.StringIO()
    monkeypatch.setattr(timing.sys, "stderr", output)
    def collect():
        return result
    assert timing.timed_release_archive("mof_macro_release_dates")(collect)() is result
    record, = event_lines(output)
    assert record["outcome"] == "returned" and record["source_binding_eligible"] is False


def test_reversed_clock_is_retained_as_invalid(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(timing.sys, "stderr", output)
    moments = iter((900, 100))
    monkeypatch.setattr("downloader.operation_timing.time.perf_counter_ns", lambda: next(moments))
    observed = timing.timed_release_archive("mof_macro_release_dates")(
        lambda: {"dataset": "mof_macro_release_dates", "status": "complete"})
    observed()
    record, = event_lines(output)
    assert record["operation_timing"]["seconds"] is None
    assert record["operation_timing"]["invalid_clock"] is True
    assert record["source_binding_eligible"] is False


def test_parallel_calls_emit_complete_individual_json_lines(monkeypatch):
    output = io.StringIO()
    monkeypatch.setattr(timing.sys, "stderr", output)
    calls = timing.timed_release_archive("mof_macro_release_dates")(lambda: object())
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert len(list(pool.map(lambda _: calls(), range(32)))) == 32
    records = event_lines(output)
    assert len(records) == 32
    assert all(record["contract"] == timing.CONTRACT for record in records)
    assert all(record["producer_pid"] > 0 and record["producer_thread_id"] > 0 for record in records)
