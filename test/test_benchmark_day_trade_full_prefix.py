import pytest

from benchmark_day_trade_full_prefix import benchmark


@pytest.mark.parametrize("rows,rounds", [(0, 1), (41, 1), (1, 0), (1, 6)])
def test_benchmark_rejects_unbounded_work(rows, rounds):
    with pytest.raises(ValueError, match="bounded synthetic"):
        benchmark(rows=rows, rounds=rounds)


def test_benchmark_retains_same_account_and_explicit_scope():
    report = benchmark(rows=2, rounds=1)
    assert report["same_account_parity"]
    assert report["input_unchanged"]
    assert len(report["input_sha256"]) == 64
    assert report["orders"] == report["formal_artifact_writes"] == 0
    assert report["minute_points_per_row"] == 270
    assert [sample["variant"] for sample in report["samples"]] == [
        "baseline_replay", "recorded_endpoint", "recorded_endpoint", "baseline_replay",
    ]
    assert {"model_inference", "plots", "cold_filesystem"} <= set(report["excluded_from_samples"])
