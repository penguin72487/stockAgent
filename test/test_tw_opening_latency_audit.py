from datetime import datetime, timedelta
import json

import pytest

from scripts.audit_tw_opening_latency import opening_coverage_milestones


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
