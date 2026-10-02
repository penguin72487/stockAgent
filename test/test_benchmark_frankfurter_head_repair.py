from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import pytest

from scripts import benchmark_frankfurter_head_repair as probe
from test_frankfurter_head_coverage import existing_pair, http_error, pivot_payload


class FixedClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 1, 0, 0, tzinfo=timezone.utc)


def test_probe_is_bounded_reproducible_and_never_promotes(monkeypatch, tmp_path):
    monkeypatch.setattr(probe, "datetime", FixedClock)
    for code in probe.PAIRS:
        existing_pair(tmp_path, code[:3], code[3:])
    hashes = {path.name: probe.sha256_file(path) for path in tmp_path.iterdir()}

    def fetch(url, timeout):
        if url.endswith("/latest"):
            return {"date": "2026-09-30"}
        parsed = urlparse(url)
        base = parse_qs(parsed.query)["from"][0]
        if "1999-01-04..2000-01-12" in parsed.path:
            if base == "EUR":
                return pivot_payload()
            raise http_error()
        return {"rates": {"2026-09-28": {"USD": .188}, "2026-09-29": {"USD": .187},
                          "2026-09-30": {"USD": .189}}}

    monkeypatch.setattr(probe.fx, "_get_json", fetch)
    result = probe.run_probe(tmp_path)
    assert result["state"] == "accepted_isolated_candidate"
    assert len(result["requests"]) == 14
    assert result["shared_pivot_source_files"] == 1 and result["recheck_network_requests"] == 0
    assert len(result["pivot_sources"]) == 1
    source = result["pivot_sources"][0]
    import json
    assert probe.sha256_bytes((json.dumps(source["source_evidence"], ensure_ascii=False, indent=2) + "\n").encode()) == source["file_sha256"]
    assert all(item["head_proof"]["pivot_source_sha256"] == source["file_sha256"] for item in result["shadow_pairs"])
    assert result["source_files_written"] == result["source_summaries_written"] == 0
    assert not result["promoted"] and not result["process_cold_start_measured"]
    assert all(item["rows_before"] == 2 and item["rows_after"] == 5 for item in result["shadow_pairs"])
    assert all(result["source_hashes_unchanged"].values())
    assert {path.name: probe.sha256_file(path) for path in tmp_path.iterdir()} == hashes
    assert probe.fx._get_json is fetch


@pytest.mark.parametrize(("timeout", "budget"), [(0, 60), (16, 60), (True, 60), (10, 0),
                                                 (10, 61), (10, float("inf")), (10, float("nan"))])
def test_invalid_budget_rejected_before_any_get(monkeypatch, tmp_path, timeout, budget):
    monkeypatch.setattr(probe.fx, "_get_json", lambda *a: pytest.fail("network must not be used"))
    with pytest.raises(ValueError):
        probe.run_probe(tmp_path, timeout=timeout, budget_seconds=budget)


def test_budget_expiry_is_not_success_and_restores_the_transport(monkeypatch, tmp_path):
    calls = [0]

    def advancing_clock():
        calls[0] += 1
        return calls[0] * 10

    fetch = lambda *a: pytest.fail("expired benchmark must not request data")
    monkeypatch.setattr(probe.fx, "_get_json", fetch)
    monkeypatch.setattr(probe.time, "monotonic", advancing_clock)
    result = probe.run_probe(tmp_path, budget_seconds=1)
    assert result["state"] == "not_accepted" and result["failure"]["type"] == "TimeoutError"
    assert result["requests"] == [] and probe.fx._get_json is fetch
