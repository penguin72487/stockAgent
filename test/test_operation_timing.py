from contextlib import contextmanager

import pytest

from downloader.operation_timing import CONTRACT, OperationTiming


def test_stage_errors_and_context_suppression_preserve_control_flow(monkeypatch):
    from downloader import operation_timing
    clock = [0]
    monkeypatch.setattr(operation_timing.time, "perf_counter_ns", lambda: clock[0])
    timer = OperationTiming()
    released = []

    @contextmanager
    def slot():
        clock[0] += 10
        try:
            yield "slot"
        except ValueError:
            released.append("suppressed")
        finally:
            released.append("released")

    with timer.enter("slot_wait", slot(), provider="p") as value:
        assert value == "slot"
        with timer.measure("adapter", provider="p"):
            clock[0] += 20
            raise ValueError("private error must not be stored")
    assert released == ["suppressed", "released"]
    snapshot = timer.snapshot("return")
    assert snapshot["contract"] == CONTRACT
    assert snapshot["seconds"] == 30e-9
    assert [(row["stage"], row["seconds"], row["outcome"]) for row in snapshot["stages"]] == [
        ("slot_wait", 10e-9, "returned"), ("adapter", 20e-9, "raised")]
    assert "private" not in str(snapshot)
    with pytest.raises(KeyboardInterrupt):
        timer.call("interrupt", lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
    assert snapshot["stages"][-1]["stage"] == "adapter"  # independent snapshot


def test_invalid_clock_is_unknown_and_never_zero(monkeypatch):
    from downloader import operation_timing
    clock = iter([30, 30, 10, 10])
    monkeypatch.setattr(operation_timing.time, "perf_counter_ns", lambda: next(clock))
    timer = OperationTiming()
    with timer.measure("regressed"):
        pass
    result = timer.snapshot("return")
    assert result["seconds"] is None
    assert result["invalid_clock"] is True
    assert result["stages"] == []
