import pytest

from benchmark_day_trade_source_cache import benchmark


@pytest.mark.parametrize("rounds, passes", [(0, 1), (4, 1), (1, 0), (1, 6)])
def test_benchmark_rejects_unbounded_work(rounds, passes):
    with pytest.raises(ValueError, match="bounded synthetic"):
        benchmark(rounds=rounds, passes=passes)


def test_benchmark_proves_budget_and_preserves_exact_source_and_fifo_account():
    report = benchmark(rounds=1, passes=2)
    assert report["exact_source_and_account_parity"]
    assert report["immutable_files_unchanged"]
    assert report["input_unchanged"]
    assert len(report["input_sha256"]) == 64
    assert report["orders"] == report["formal_artifact_writes"] == 0
    assert len(report["samples"]) == 12
    assert len(report["source_session_sha256"]) == 6
    for setting, summaries in report["summaries"].items():
        current = summaries["shared_bounded"]
        assert current["retained_tensor_bytes"] <= int(float(setting) * 1024**3)
    ample = report["summaries"]["1"]
    assert ample["shared_bounded"]["retained_tensor_bytes"] < ample["baseline"]["retained_tensor_bytes"]
    assert ample["shared_bounded"]["session_npz_loads"] < ample["baseline"]["session_npz_loads"]
    disabled = report["summaries"]["0"]
    assert disabled["baseline"]["retained_tensor_bytes"] > 0
    assert disabled["shared_bounded"]["retained_tensor_bytes"] == 0
    assert {"model_inference", "plots", "cold_filesystem"} <= set(report["excluded_from_accessor_samples"])
