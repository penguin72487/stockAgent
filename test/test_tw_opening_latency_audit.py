from datetime import datetime, timedelta
import json

import pytest

from scripts.audit_tw_opening_latency import opening_coverage_milestones, opening_publication_slo


def event(provider, elapsed_ms, observations=(), **overrides):
    start = datetime.fromisoformat("2026-10-13T09:00:00+08:00")
    return {"session_date": "2026-10-13", "provider": provider,
        "observed_at_taipei": (start + timedelta(milliseconds=elapsed_ms)).isoformat(),
        "event": "observations_delta", "observations": list(observations), **overrides}


def write(path, rows):
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def test_asof_counts_keep_scope_unknown_and_no_first_print_distinct(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("mis", 0, event="started"), event("shioaji_shared", 0, event="started"),
        event("mis", 75, [{"symbol": "A", "state": "observed_no_first_print"}]),
        event("shioaji_shared", 250, [{"symbol": "A", "state": "valid_open"}]),
        event("mis", 501, [{"symbol": "B", "state": "valid_open"},
                            {"symbol": "X", "state": "valid_open"}]),
        event("mis", 600, [{"symbol": "B", "state": "no_source_observation"}]),
        event("finmind_snapshot", 10000, [{"symbol": "C", "state": "valid_open"}]),
        event("mis", 11000, event="finished", state="failed")])
    before = path.read_bytes()
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B", "C"])
    assert result["scope_basis"] == "canonical_receipt_posthoc_axis_not_backdated_registry"
    assert result["measurement_boundary"] == "source_validation_event_clock_not_durable_quorum_or_broker_fill"
    assert result["failed_or_degraded_jobs"] == {"mis": 1}
    samples = {row["after_open_ms"]: row for row in result["milestones"]}
    assert (samples[100]["valid_open_count"], samples[100]["observed_no_print_count"], samples[100]["unknown_count"]) == (0, 1, 2)
    assert (samples[250]["valid_open_count"], samples[250]["observed_no_print_count"], samples[250]["unknown_count"]) == (1, 0, 2)
    assert samples[500]["valid_open_count"] == 1
    assert samples[1000]["valid_open_count"] == samples[5000]["valid_open_count"] == 2
    assert samples[5000]["unknown_count"] == 1
    assert samples[5000]["providers"]["finmind_snapshot"]["valid_open_count"] == 0
    assert samples[10000]["valid_open_count"] == samples[20000]["valid_open_count"] == samples[30000]["valid_open_count"] == 3
    assert samples[30000]["unknown_count"] == 0
    assert path.read_bytes() == before


@pytest.mark.parametrize("bad", [None, {}, "broken", {"session_date": "2026-10-13"}])
def test_malformed_evidence_remains_visible_as_degraded(tmp_path, bad):
    path = tmp_path / "events.jsonl"
    write(path, [bad, event("mis", 100, [{"symbol": "A", "state": "valid_open"}])])
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])
    assert result["state"] == "degraded"
    assert result["malformed_lines"] == 1
    assert result["milestones"][0]["unknown_count"] == 1


def test_preopen_other_day_and_naive_time_cannot_count_as_today(tmp_path):
    path = tmp_path / "events.jsonl"
    observation = [{"symbol": "A", "state": "valid_open"}]
    write(path, [event("mis", -1, observation), event("mis", 0, observation, session_date="2026-10-12"),
                event("mis", 1, observation, observed_at_taipei="2026-10-13T09:00:00.001")])
    result = opening_coverage_milestones(path, "2026-10-13", ["A"])
    assert all(row["unknown_count"] == 1 for row in result["milestones"])
    assert result["malformed_lines"] == 1


def test_missing_evidence_is_unavailable_not_zero_latency(tmp_path):
    result = opening_coverage_milestones(tmp_path / "missing", "2026-10-13", ["A"])
    assert result["state"] == "unavailable"
    assert "milestones" not in result


def test_legacy_aggregate_without_ids_is_not_reported_as_zero_quotes(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("shioaji_shared", 1100, event="first_batch", requested_count=2,
                     response_count=2, valid_open_count=1)])
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])
    assert result["state"] == "insufficient_evidence"
    assert not result["per_symbol_schema_usable"]
    assert all(row["valid_open_count"] is None and row["unknown_count"] is None for row in result["milestones"])
    assert result["legacy_aggregate_events"][0]["valid_open_count"] == 1


@pytest.mark.parametrize("symbols", [[], ["A", "A"]])
def test_audit_rejects_invalid_scope(tmp_path, symbols):
    with pytest.raises(ValueError, match="nonempty unique"):
        opening_coverage_milestones(tmp_path / "missing", "2026-10-13", symbols)


def publication(market_name, elapsed_ms, **overrides):
    gate = datetime.fromisoformat("2026-10-14T09:00:00+08:00")
    return {"status": "ready", "session_date": "2026-10-14", "market": market_name,
        "signal_id": market_name + "-immutable", "gate_at": gate.isoformat(),
        "signal_ready_at": (gate + timedelta(milliseconds=elapsed_ms - 1)).isoformat(),
        "artifact_published_at": (gate + timedelta(milliseconds=elapsed_ms)).isoformat(),
        "published_from_open_ms": 0, **overrides}


def test_staged_slo_uses_all_mode_original_clocks_not_cached_milliseconds():
    attempts = {"fast": {"attempt_count": 1, "failed_count": 0, "ready": [publication("fast", 250)]},
        "slow": {"attempt_count": 4, "failed_count": 3, "ready": [publication("slow", 30001), publication("slow", 20000)]}}
    result = opening_publication_slo("2026-10-14", attempts, ["fast", "slow"])
    assert result["state"] == "observed"
    assert result["first_mode_published_from_open_ms"] == 250
    assert result["all_modes_published_from_open_ms"] == 20000
    assert [row["passed"] for row in result["stages"]] == [True, True, False, False, False]
    assert result["modes"]["slow"]["failed_count"] == 3


def test_missing_mode_cannot_earn_a_stage_from_fast_subset():
    result = opening_publication_slo("2026-10-14", {"fast": {"ready": [publication("fast", 250)]}}, ["fast", "missing"])
    assert result["state"] == "incomplete"
    assert result["published_mode_count"] == 1
    assert result["all_modes_published_from_open_ms"] is None
    assert all(not row["passed"] and row["state"] == "incomplete" for row in result["stages"])


@pytest.mark.parametrize("overrides", [
    {"artifact_published_at": "2026-10-14T09:00:00.500"},
    {"artifact_published_at": "2026-10-14T08:59:59+08:00"},
    {"artifact_published_at": "2026-10-14T09:00:00.498+08:00"},
    {"signal_ready_at": "2026-10-14T08:59:59+08:00"},
    {"gate_at": "2026-10-14T08:59:00+08:00"},
    {"session_date": "2026-10-13"}, {"market": "other"}, {"signal_id": None},
    {"status": "failed"}, {"artifact_published_at": None},
])
def test_invalid_or_backdated_publication_cannot_pass_slo(overrides):
    result = opening_publication_slo("2026-10-14", {"mode": {"ready": [publication("mode", 500, **overrides)]}}, ["mode"])
    assert result["modes"]["mode"]["invalid_ready_records"] == 1
    assert result["all_modes_published_from_open_ms"] is None
    assert not any(row["passed"] for row in result["stages"])


def test_equivalent_utc_clocks_preserve_the_publication_boundary():
    row = publication("mode", 1000, signal_ready_at="2026-10-14T01:00:00.999+00:00",
        artifact_published_at="2026-10-14T01:00:01+00:00")
    result = opening_publication_slo("2026-10-14", {"mode": {"ready": [row]}}, ["mode"])
    assert result["all_modes_published_from_open_ms"] == 1000
    assert all(stage["passed"] for stage in result["stages"])


@pytest.mark.parametrize("markets", [[], ["mode", "mode"]])
def test_publication_slo_requires_explicit_unique_scope(markets):
    with pytest.raises(ValueError, match="nonempty unique"):
        opening_publication_slo("2026-10-14", {}, markets)
