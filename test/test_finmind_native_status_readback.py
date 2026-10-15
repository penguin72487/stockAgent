from copy import deepcopy
from datetime import datetime

import pytest

from scripts import verify_finmind_group_cache_runtime as verifier


def example():
    return {"observed_at_utc": "2026-10-15T00:24:47.693332+00:00", "state": "protected_opening",
            "historical_frontiers": {"A": {"estimate_as_of_utc": "2026-10-15T00:24:47.693368+00:00",
                                             "unseeded_partition_candidates": 12}},
            "series": {"A": {"complete": 2}}}


def test_replay_distinct_native_clocks_without_ignoring_fields(monkeypatch, tmp_path):
    status = example()

    def publish(*args, **kwargs):
        verifier.worker._now()  # task selector's independent decision clock
        result = deepcopy(status)
        result["observed_at_utc"] = verifier.worker._now().isoformat()
        result["historical_frontiers"]["A"]["estimate_as_of_utc"] = verifier.worker._now().isoformat()
        return result

    monkeypatch.setattr(verifier.worker, "_status", publish)
    result, clocks = verifier.replay_status(None, tmp_path, status)
    assert result == status
    assert clocks["observed_at_utc"] != clocks["estimate_as_of_utc"]
    # Data/count/source differences are never normalized or excluded.
    result["historical_frontiers"]["A"]["unseeded_partition_candidates"] += 1
    assert result != status
    result = deepcopy(status)
    result["series"]["A"]["complete"] += 1
    assert result != status


@pytest.mark.parametrize("clock", ["2026-10-15T00:24:46+00:00", "2026-10-15T00:24:48"])
def test_reject_invalid_recorded_clock_order(clock, tmp_path):
    status = example()
    status["historical_frontiers"]["A"]["estimate_as_of_utc"] = clock
    with pytest.raises(ValueError, match="clock_order_invalid"):
        verifier.replay_status(None, tmp_path, status)


def test_reject_divergent_frontier_clocks(tmp_path):
    status = example()
    status["historical_frontiers"]["B"] = {"estimate_as_of_utc": "2026-10-15T00:24:48+00:00"}
    with pytest.raises(ValueError, match="frontier_clocks_inconsistent"):
        verifier.replay_status(None, tmp_path, status)


def test_new_clock_contract_is_not_silently_accepted(monkeypatch, tmp_path):
    def publish(*args, **kwargs):
        for _ in range(4):
            verifier.worker._now()
        return {}

    monkeypatch.setattr(verifier.worker, "_status", publish)
    with pytest.raises(StopIteration):
        verifier.replay_status(None, tmp_path, example())
    assert datetime.fromisoformat(example()["observed_at_utc"]).utcoffset() is not None
