from __future__ import annotations

import pytest

from scripts import benchmark_okx_index_routing as bench


class _IndexClient:
    def __init__(self, *, recent_gap=False):
        self.calls = []
        self.recent_gap = recent_gap

    def limiter_activity(self):
        return {path: {"grants_total": sum(p == path for p, _ in self.calls)} for path, _ in self.calls}

    def get(self, path, params):
        self.calls.append((path, dict(params)))
        end = int(params["after"])
        rows = [[str(ts), "100", "101", "99", "100", "1"]
                for ts in range(end - 60_000, end - (int(params["limit"]) + 1) * 60_000, -60_000)]
        if self.recent_gap and path == bench.INDEX_PRICE_RECENT_ENDPOINT:
            # A recent endpoint is allowed to be incomplete: the real reader
            # must recover at the same cursor, not accept the short data.
            rows = rows[:10]
        return {"data": rows}


def _probe(client, **kwargs):
    now = 30_000 * 60_000
    return bench.run_probe(client, inst_id="BTC-USDT", start_ms=now - 1440 * 60_000,
                           end_ms=now - 60_000, now_ms=now, **kwargs)


def test_benchmark_records_exact_coverage_and_actual_route_counts_without_source_writes():
    result = _probe(_IndexClient())
    assert result["state"] == "accepted"
    assert result["exact_parity"] and result["requested_coverage"]
    assert result["source_files_written"] == 0
    assert [trial["variant"] for trial in result["trials"]] == ["history_only", "hybrid", "hybrid", "history_only"]
    assert all(trial["rows"] == 1440 for trial in result["trials"])
    for trial in result["trials"]:
        expected = {bench.INDEX_PRICE_HISTORY_ENDPOINT: 15} if trial["variant"] == "history_only" else {
            bench.INDEX_PRICE_HISTORY_ENDPOINT: 2, bench.INDEX_PRICE_RECENT_ENDPOINT: 13,
        }
        assert trial["acquisition"]["page_calls"] == expected
        assert {key: val for key, val in trial["transport_grants"].items() if val} == expected


def test_benchmark_retains_fallback_evidence_even_when_parity_recovers():
    result = _probe(_IndexClient(recent_gap=True))
    assert result["state"] == "accepted"
    for trial in result["trials"]:
        if trial["variant"] == "hybrid":
            assert trial["acquisition"]["fallback_reason"] == "recent_short_or_noncontiguous"
            assert trial["acquisition"]["page_calls"] == {
                bench.INDEX_PRICE_RECENT_ENDPOINT: 1, bench.INDEX_PRICE_HISTORY_ENDPOINT: 15,
            }


def test_benchmark_budget_exhaustion_is_not_accepted(monkeypatch):
    times = iter((0.0, 2.0, 2.0))
    monkeypatch.setattr(bench.time, "monotonic", lambda: next(times))
    client = _IndexClient()
    result = _probe(client, budget_seconds=1)
    assert result["state"] == "budget_exceeded"
    assert result["trials"] == []
    assert result["failure"]["type"] == "ProbeBudgetExceeded"
    assert not result["exact_parity"] and not result["requested_coverage"]
    assert not client.calls


@pytest.mark.parametrize("kwargs", [{"repetitions": 0}, {"repetitions": 4}, {"repetitions": True}, {"budget_seconds": 0}, {"budget_seconds": float("nan")}, {"budget_seconds": float("inf")}])
def test_benchmark_rejects_unbounded_or_malformed_limits_before_request(kwargs):
    client = _IndexClient()
    with pytest.raises(ValueError):
        _probe(client, **kwargs)
    assert client.calls == []


def test_benchmark_unfinished_or_oversized_window_is_not_fetched():
    client = _IndexClient()
    for start, end, now in ((0, 60_000, 60_000), (0, 3000 * 60_000, 4000 * 60_000)):
        with pytest.raises(ValueError):
            bench.run_probe(client, inst_id="BTC-USDT", start_ms=start, end_ms=end, now_ms=now)
    assert client.calls == []
