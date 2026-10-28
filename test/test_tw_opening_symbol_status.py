from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json

import pytest

from scripts.audit_tw_opening_symbol_status import classify_symbols, receipt_snapshot
from scripts import audit_tw_opening_symbol_status as audit


def test_symbol_cli_keeps_unknowns_on_closed_day_and_skips_unneeded_refresh(tmp_path, monkeypatch):
    from stockagent.live import market_config

    receipt, official = fixtures()
    source, output = tmp_path / "receipt.json", tmp_path / "audit.json"
    source.write_text(json.dumps(receipt))
    expected = classify_symbols(receipt, official)
    source_before = source.read_bytes()
    cfg = market_config.LiveMarketConfig(market="A", label="A", config_path="unused",
        day_trade_simulation_enabled=True, holidays=(receipt["session_date"],))
    monkeypatch.setattr(market_config, "load_market_configs", lambda _: {"A": cfg})
    monkeypatch.setattr(audit, "load_current_universe", lambda _: official)
    def unexpected_provider(*args, **kwargs):
        raise AssertionError("a verified closure must not trigger a full quote refresh")
    monkeypatch.setattr(audit.quotes, "fetch_tw_mis_last_prices", unexpected_provider)
    monkeypatch.setattr(audit.sys, "argv", ["audit", "--receipt", str(source),
        "--output", str(output), "--refresh-unknown-mis"])
    assert audit.main() == 0
    report = json.loads(output.read_text())
    assert all(report[key] == value for key, value in expected.items())
    assert report["session_applicability"]["state"] == "not_applicable_closed"
    assert report["provider_requests_requested"] and not report["provider_requests_enabled"]
    assert report["fresh_mis"] == {"state": "skipped_verified_calendar_closed", "provider_requests": 0}
    assert source.read_bytes() == source_before and not report["production_ledger_modified"]


def test_journal_deadlines_keep_all_symbols_early_failures_and_late_recovery(tmp_path):
    receipt, official = fixtures()
    gate = datetime.fromisoformat("2026-10-22T09:00:00+08:00")
    scope_hash = hashlib.sha256("\n".join(sorted(receipt["requested_symbols"])).encode()).hexdigest()

    def event(ms, kind, **details):
        return {"session_date": receipt["session_date"], "provider": "mis", "owner_pid": 1,
            "job_id": "one", "event": kind, "observed_at_taipei": (gate + timedelta(milliseconds=ms)).isoformat(), **details}

    rows = [event(1, "started", requested_count=5, requested_scope_sha256=scope_hash),
        event(200, "observations_delta", observations=[{"symbol": "OPEN", "state": "valid_open"},
            {"symbol": "ZERO", "state": "observed_no_first_print"}]),
        event(300, "finished", state="failed", http_status=403),
        event(400, "observations_delta", observations=[{"symbol": "OPEN", "state": "stale_or_preopen_time"}]),
        event(12000, "observations_delta", observations=[{"symbol": "UNKNOWN", "state": "valid_open"}])]
    # Producers can append concurrently: event clocks, not append order, own deadlines.
    path = tmp_path / "acquisition.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in reversed(rows)))
    original = path.read_bytes()
    result = audit.journal_symbol_timeline(path, receipt, official)
    symbols = {row["symbol"]: row for row in result["symbols"]}
    assert result["state"] == "observed" and result["requested_count"] == len(symbols) == 5
    assert symbols["OPEN"]["deadlines"]["11000"]["state"] == "valid_open"
    assert symbols["UNKNOWN"]["deadlines"]["11000"]["state"] == "unknown"
    assert symbols["UNKNOWN"]["deadlines"]["20000"]["state"] == "valid_open"
    assert symbols["UNKNOWN"]["first_valid_open_after_open_ms"] == 12000
    assert symbols["ZERO"]["deadlines"]["11000"]["absence_clock_unverified"]
    assert symbols["ABSENT"]["official_status"] == "not_in_verified_current_registry"
    assert symbols["HALT"]["official_status"] == "halted"
    assert result["journal_prefix"]["sha256"] == hashlib.sha256(original).hexdigest()
    assert result["failure_jobs"][0]["full_scope_verified"]
    assert path.read_bytes() == original


def test_journal_partial_failed_scope_is_not_guessed_per_symbol(tmp_path):
    receipt, official = fixtures()
    path = tmp_path / "acquisition.jsonl"
    rows = [{"session_date": receipt["session_date"], "provider": "fugle", "owner_pid": 1,
        "job_id": "partial", "observed_at_taipei": "2026-10-22T09:00:00.100+08:00",
        "event": "started", "requested_count": 1, "requested_scope_sha256": "different"},
        {"session_date": receipt["session_date"], "provider": "fugle", "owner_pid": 1,
        "job_id": "partial", "observed_at_taipei": "2026-10-22T09:00:00.200+08:00",
        "event": "finished", "state": "failed", "http_status": 403}]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = audit.journal_symbol_timeline(path, receipt, official)
    assert result["failure_jobs"][0]["reasons"] == ["permission_or_subscription"]
    assert not result["failure_jobs"][0]["full_scope_verified"]
    assert all(not row["deadlines"]["11000"]["full_scope_failed_providers"] for row in result["symbols"])


@pytest.mark.parametrize("bulk", [False, True])
def test_new_standard_counts_fresh_no_first_as_reference_candidate_and_keeps_proof_clock(tmp_path, bulk):
    from stockagent.live.opening_price_journal import pack_observation_details
    receipt, official = fixtures()
    gate = int(datetime.fromisoformat("2026-10-22T09:00:00+08:00").timestamp() * 1000)
    def observation(proof):
        return {"symbol": "ZERO", "state": "observed_no_first_print", "source": "opening:shared",
            "open_price": None, "available": True, "market_timestamp_ms": gate,
            "received_timestamp_ms": gate + 16000, "simtrade": False,
            "no_first_print_observed_timestamp_ms": proof}
    rows = [observation(gate + 100)] * (128 if bulk else 1)
    event = {"session_date": receipt["session_date"], "provider": "mis", "event": "observations_delta",
        "observed_at_taipei": "2026-10-22T09:00:16.100+08:00", **pack_observation_details({"observations": rows})}
    path = tmp_path / "journal.jsonl"
    path.write_text(json.dumps(event) + "\n")
    stale = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(17000,))
    assert stale["milestones"][0]["input_reference_candidate_count"] == 0, "new delivery cannot refresh old absence"
    rows = [observation(gate + 16000)] * len(rows)
    event.update(pack_observation_details({"observations": rows}))
    path.write_text(json.dumps(event) + "\n")
    good = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(17000,), include_reference_rows=True)
    assert good["milestones"][0]["counts"]["observed_no_first_print"] == 1
    assert good["milestones"][0]["input_reference_candidate_count"] == 1
    projected = audit.journal_reference_receipt(receipt, good, 17000)
    _, snapshot = receipt_snapshot(projected)
    assert audit.quotes.tw_session_no_open_print_mask(snapshot, session_date=receipt["session_date"],
        asof_timestamp_ms=gate + 17000).tolist() == [False, True, False, False, False]
    assert projected["rows"]["ZERO"]["open_prices"] is None
    assert projected["rows"]["ZERO"]["volumes"] == 0
    assert receipt["captured_at_taipei"].endswith("09:00:01+08:00")


def test_only_original_direct_mis_receive_clock_can_explain_legacy_absence(tmp_path):
    receipt, official = fixtures()
    gate = int(datetime.fromisoformat("2026-10-22T09:00:00+08:00").timestamp() * 1000)
    row = {"symbol": "ZERO", "state": "observed_no_first_print", "source": "twse_tpex:mis",
        "open_price": None, "available": True, "market_timestamp_ms": gate,
        "received_timestamp_ms": gate + 100, "simtrade": False}
    event = {"session_date": receipt["session_date"], "provider": "mis", "event": "observations_delta",
        "observed_at_taipei": "2026-10-22T09:00:00.200+08:00", "observations": [row]}
    path = tmp_path / "journal.jsonl"
    for source, expected in [("twse_tpex:mis", 1), ("twse_tpex:mis:shared_opening_snapshot", 0)]:
        row["source"] = source
        path.write_text(json.dumps(event) + "\n")
        result = audit.journal_symbol_timeline(path, receipt, official)
        assert result["milestones"][0]["input_reference_candidate_count"] == expected


def test_private_projection_cannot_import_a_later_first_trade(tmp_path):
    receipt, official = fixtures()
    path = tmp_path / "journal.jsonl"
    gate = int(datetime.fromisoformat("2026-10-22T09:00:00+08:00").timestamp() * 1000)
    event = {"session_date": receipt["session_date"], "provider": "mis", "event": "observations_delta",
        "observed_at_taipei": "2026-10-22T09:00:02+08:00", "observations": [{"symbol": "OPEN",
            "state": "valid_open", "source": "twse_tpex:mis", "open_price": 100., "available": True,
            "market_timestamp_ms": gate + 1900, "received_timestamp_ms": gate + 1950, "simtrade": False}]}
    path.write_text(json.dumps(event) + "\n")
    result = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(1000, 3000), include_reference_rows=True)
    assert audit.journal_reference_receipt(receipt, result, 1000)["rows"]["OPEN"]["open_prices"] is None
    assert audit.journal_reference_receipt(receipt, result, 3000)["rows"]["OPEN"]["open_prices"] == 100.


def test_corrupt_bulk_event_does_not_publish_a_valid_prefix(tmp_path):
    from stockagent.live.opening_price_journal import pack_observation_details
    receipt, official = fixtures()
    observations = [{"symbol": "OPEN", "state": "valid_open", "source": "fixture", "open_price": 100.,
        "available": True, "market_timestamp_ms": 1, "received_timestamp_ms": 2, "simtrade": False}] * 128
    row = {"session_date": receipt["session_date"], "provider": "mis", "event": "observations_delta",
        "observed_at_taipei": "2026-10-22T09:00:00.200+08:00", **pack_observation_details({"observations": observations})}
    row["observations"][-1].pop()
    path = tmp_path / "acquisition.jsonl"
    path.write_text(json.dumps(row) + "\n" + '{"unfinished":')
    result = audit.journal_symbol_timeline(path, receipt, official)
    assert result["state"] == "degraded"
    assert result["journal_prefix"]["malformed_lines"] == [1, 2]
    assert all(row["first_valid_open_after_open_ms"] is None for row in result["symbols"])


def test_invalid_state_late_in_event_cannot_publish_earlier_symbols(tmp_path):
    receipt, official = fixtures()
    path = tmp_path / "acquisition.jsonl"
    path.write_text(json.dumps({"session_date": receipt["session_date"], "provider": "mis",
        "observed_at_taipei": "2026-10-22T09:00:00.200+08:00", "event": "observations_delta",
        "observations": [{"symbol": "OPEN", "state": "valid_open"},
                         {"symbol": "ZERO", "state": None}]}) + "\n")
    result = audit.journal_symbol_timeline(path, receipt, official)
    assert result["journal_prefix"]["malformed_lines"] == [1]
    assert all(row["first_valid_open_after_open_ms"] is None for row in result["symbols"])


@pytest.mark.parametrize("project_headers", [False, True])
@pytest.mark.parametrize("received,validated,reason,gap_ms", [
    ("2026-10-22T09:00:00.100+08:00", "2026-10-22T09:00:00.400+08:00",
     "full_market_validation_after_event", 200.0),
    ("2026-10-22T09:00:00.300+08:00", "2026-10-22T09:00:00.150+08:00",
     "full_market_receive_after_validation", 150.0),
    ("2026-10-22T08:59:59.900+08:00", "2026-10-22T09:00:00.150+08:00",
     "preopen_full_market_receipt", None),
    ("2026-10-22T09:00:00.100", "2026-10-22T09:00:00.150+08:00",
     "naive_full_market_clock", None),
    ("2026-10-22T09:00:00.100+08:00", "2026-10-22T09:00:00.150",
     "naive_full_market_clock", None),
])
def test_full_market_clock_rejection_is_diagnostic_not_a_repaired_observation(
    tmp_path, project_headers, received, validated, reason, gap_ms,
):
    receipt, official = fixtures()
    row = {"session_date": receipt["session_date"], "provider": "finmind_snapshot",
        "event": "provider_snapshot", "observed_at_taipei": "2026-10-22T09:00:00.200+08:00",
        "coverage": {"contract": "finmind_unfiltered_market_snapshot_coverage_v1",
            "received_at_taipei": received, "validated_at_taipei": validated,
            "valid_open_symbols": ["OPEN"], "rejected_symbols": {"no_positive_volume_rows": ["ZERO"]}}}
    path = tmp_path / "journal.jsonl"
    original = (json.dumps(row) + "\n").encode()
    path.write_bytes(original)
    result = audit.journal_symbol_timeline(path, receipt, official, project_headers=project_headers)
    diagnostic = {"line": 1, "reason": reason}
    if gap_ms is not None:
        diagnostic["clock_order_gap_ms"] = gap_ms
    assert result["contract"] == "official_axis_opening_symbol_deadlines_v3"
    assert result["state"] == "degraded"
    assert result["journal_prefix"]["malformed_lines"] == [1]
    assert result["journal_prefix"]["invalid_event_diagnostics"] == [diagnostic]
    assert all(row["first_valid_open_after_open_ms"] is None for row in result["symbols"])
    assert all(row["deadlines"]["11000"]["state"] == "unknown" for row in result["symbols"])
    with pytest.raises(ValueError, match="verified journal generation"):
        audit.journal_reference_receipt(receipt, result, 11000)
    assert path.read_bytes() == original, "do not repair clocks or rewrite source evidence"


def test_opening_only_horizon_excludes_late_events_not_bad_evidence(tmp_path):
    receipt, official = fixtures()
    late = {"session_date": receipt["session_date"], "provider": "finmind_snapshot",
        "event": "provider_snapshot", "observed_at_taipei": "2026-10-22T09:30:00+08:00",
        "coverage": {"contract": "finmind_unfiltered_market_snapshot_coverage_v1",
            "received_at_taipei": "2026-10-22T09:30:00.900+08:00",
            "validated_at_taipei": "2026-10-22T09:30:00.100+08:00",
            "valid_open_symbols": ["OPEN"], "rejected_symbols": {}}}
    path = tmp_path / "journal.jsonl"
    raw = (json.dumps(late) + "\n").encode()
    path.write_bytes(raw)
    full = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(1000,))
    bounded = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(1000,),
        observation_horizon_ms=1000, include_reference_rows=True)
    assert full["state"] == "degraded"
    assert bounded["state"] == "observed"
    assert bounded["outside_observation_horizon_events"] == 1
    assert bounded["observation_horizon_ms"] == 1000
    assert bounded["journal_prefix"]["sha256"] == full["journal_prefix"]["sha256"]
    assert bounded["milestones"][0]["counts"] == {"unknown": len(receipt["requested_symbols"])}
    assert not any(row["first_valid_by_provider_ms"] for row in bounded["symbols"])
    assert audit.journal_reference_receipt(receipt, bounded, 1000)["rows"]["OPEN"]["open_prices"] is None
    with pytest.raises(ValueError, match="horizon"):
        audit.journal_symbol_timeline(path, receipt, official, cutoffs=(11000,), observation_horizon_ms=1000)
    # An unreadable header cannot be claimed to be a later out-of-scope event.
    path.write_bytes(raw + b"{broken\n")
    broken = audit.journal_symbol_timeline(path, receipt, official, cutoffs=(1000,), observation_horizon_ms=1000)
    assert broken["state"] == "degraded"
    assert broken["journal_prefix"]["malformed_lines"] == [2]


def test_rejection_diagnostics_distinguish_partial_line_without_exposing_payload(tmp_path):
    receipt, official = fixtures()
    path = tmp_path / "journal.jsonl"
    bad_clock = {"session_date": receipt["session_date"], "provider": "mis", "event": "unknown",
        "observed_at_taipei": "invalid-clock", "private": "do-not-expose"}
    path.write_bytes((json.dumps(bad_clock) + '\n{"unfinished":').encode())
    result = audit.journal_symbol_timeline(path, receipt, official)
    diagnostics = result["journal_prefix"]["invalid_event_diagnostics"]
    assert diagnostics == [{"line": 1, "reason": "invalid_event"},
                           {"line": 2, "reason": "partial_journal_line"}]
    assert "do-not-expose" not in json.dumps(diagnostics)


def test_projected_headers_retain_full_decoder_error_semantics(tmp_path):
    receipt, official = fixtures()
    path = tmp_path / "acquisition.jsonl"
    base = {"session_date": receipt["session_date"], "provider": "mis",
        "observed_at_taipei": "2026-10-22T09:00:00.200+08:00", "event": "unknown_event"}
    rows = [base, {**base, "provider": "merged", "observed_at_taipei": "bad-clock"},
        {**base, "provider": None}, {**base, "observed_at_taipei": "2026-10-22T09:00:00"},
        {**base, "event": "observations_delta", "observations": [{"symbol": "OPEN", "state": "valid_open"}]}]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    control = audit.journal_symbol_timeline(path, receipt, official, project_headers=False)
    candidate = audit.journal_symbol_timeline(path, receipt, official, project_headers=True)
    for result in (control, candidate):
        result.pop("read_and_analysis_ms")
    assert candidate == control
    assert candidate["journal_prefix"]["malformed_lines"] == [2, 3, 4]


def test_decoder_benchmark_keeps_abba_order_and_rejects_output_drift(tmp_path, monkeypatch):
    def analysis(*args, project_headers=True, **kwargs):
        return {"state": "observed", "journal_prefix": {"sha256": "fixed"},
                "read_and_analysis_ms": 1 if project_headers else 2, "symbols": ["same"]}
    monkeypatch.setattr(audit, "journal_symbol_timeline", analysis)
    result = audit.benchmark_journal_decoders(tmp_path, {}, {}, blocks=2)
    assert [row["decoder"] for row in result["samples"]] == ["full", "projected", "projected", "full"] * 2
    assert result["parity"] and result["medians_ms"] == {"full": 2, "projected": 1}
    monkeypatch.setattr(audit, "journal_symbol_timeline", lambda *args, **kwargs: {
        **analysis(*args, **kwargs), "symbols": [str(kwargs["project_headers"])]})
    assert not audit.benchmark_journal_decoders(tmp_path, {}, {}, blocks=1)["parity"]


def fixtures():
    stamp = int(datetime.fromisoformat("2026-10-22T09:00:00+08:00").timestamp()*1000)
    receipt = {"session_date": "2026-10-22", "captured_at_taipei": "2026-10-22T09:00:01+08:00",
        "requested_symbols": ["OPEN", "ZERO", "UNKNOWN", "HALT", "ABSENT"],
        "official_trading_universe": {"fingerprint": "official-1"},
        "rows": {"OPEN": {"available": True, "open_prices": 100., "price": 100.,
                          "timestamp_ms": stamp, "open_timestamp_ms": stamp},
                 "ZERO": {"available": True, "volumes": 0., "price": 90.,
                          "timestamp_ms": stamp, "no_open_print_observed_ms": stamp}}}
    official = {"session_date": "2026-10-22", "observed_at_taipei": "2026-10-22T08:30:00+08:00",
        "fingerprint": "official-1", "records": [
            {"symbol": symbol, "name": symbol, "market": "tpex", "listing_date": "2020-01-01",
             "trading_status": "halted" if symbol == "HALT" else "listed_for_regular_trading"}
            for symbol in ("OPEN", "ZERO", "UNKNOWN", "HALT")]}
    return receipt, official


def test_unknown_is_not_delisted_or_no_first_print_and_only_evidenced_halt_excludes():
    receipt, official = fixtures()
    report = classify_symbols(receipt, official)
    assert report["unknown_count"] == 3
    assert report["unknown_official_status_counts"] == {
        "listed_for_regular_trading": 1, "halted": 1, "not_in_verified_current_registry": 1}
    assert len(report["observed_no_first_print_symbols"]) == 1
    by_symbol = {row["symbol"]: row for row in report["unknown_symbols"]}
    assert by_symbol["HALT"]["exclusion_allowed"]
    assert not by_symbol["UNKNOWN"]["exclusion_allowed"]
    assert not by_symbol["ABSENT"]["exclusion_allowed"]  # Absence is not proof of delisting.


def test_later_print_does_not_rewrite_original_unknown_state():
    receipt, official = fixtures()
    later = deepcopy(receipt)
    later["captured_at_taipei"] = "2026-10-22T09:30:00+08:00"
    later["rows"]["UNKNOWN"] = deepcopy(receipt["rows"]["OPEN"])
    report = classify_symbols(receipt, official, comparison=later)
    row = next(row for row in report["unknown_symbols"] if row["symbol"] == "UNKNOWN")
    assert row["state"] == "no_source_observation"
    assert row["later_observation"]["state"] == "valid_open"


@pytest.mark.parametrize("bad", ["future", "naive", "wrong_session", "duplicate"])
def test_invalid_receipt_cannot_invent_dated_source_evidence(bad):
    receipt, _ = fixtures()
    if bad == "future":
        receipt["rows"]["ZERO"]["no_open_print_observed_ms"] += 2000
    elif bad == "naive":
        receipt["captured_at_taipei"] = "2026-10-22T09:00:01"
    elif bad == "wrong_session":
        receipt["session_date"] = "2026-10-21"
    else:
        receipt["requested_symbols"].append("ZERO")
    with pytest.raises(ValueError):
        receipt_snapshot(receipt)


def test_stale_and_zero_volume_without_official_absence_proof_stay_unknown():
    receipt, official = fixtures()
    receipt["rows"]["ZERO"]["no_open_print_observed_ms"] = 0
    report = classify_symbols(receipt, official)
    assert report["unknown_count"] == 4
    assert not report["observed_no_first_print_symbols"]


def test_foreign_universe_generation_is_not_silently_matched():
    receipt, official = fixtures()
    official["fingerprint"] = "official-2"
    with pytest.raises(ValueError, match="generation differs"):
        classify_symbols(receipt, official)


def test_whole_second_capture_accepts_fractional_rows_but_not_next_second():
    receipt, _ = fixtures()
    capture_ms = int(datetime.fromisoformat(receipt["captured_at_taipei"]).timestamp() * 1000)
    receipt["rows"]["OPEN"]["timestamp_ms"] = capture_ms + 999
    receipt_snapshot(receipt)
    receipt["rows"]["OPEN"]["timestamp_ms"] += 1
    with pytest.raises(ValueError, match="after its capture clock"):
        receipt_snapshot(receipt)


def test_later_stale_broker_row_cannot_renew_old_no_first_print_evidence():
    receipt, official = fixtures()
    later = deepcopy(receipt)
    later["captured_at_taipei"] = "2026-10-22T09:30:00+08:00"
    later["rows"]["ZERO"]["timestamp_ms"] += 30*60*1000
    report = classify_symbols(receipt, official, comparison=later)
    row = report["observed_no_first_print_symbols"][0]["later_observation"]
    assert row["state"] == "stale_no_first_print_proof"
    assert row["no_first_print_evidence_age_ms"] == 30*60*1000
    assert row["no_first_print_observed_timestamp_ms"] == receipt["rows"]["ZERO"]["no_open_print_observed_ms"]


def test_comparison_cannot_silently_change_official_membership_generation():
    receipt, official = fixtures()
    later = deepcopy(receipt)
    later["official_trading_universe"]["fingerprint"] = "official-other"
    with pytest.raises(ValueError, match="comparison/official scope generation"):
        classify_symbols(receipt, official, comparison=later)


def test_source_bytes_are_read_once_and_the_hash_binds_that_exact_revision(tmp_path, monkeypatch):
    receipt, official = fixtures()
    path, output = tmp_path / "receipt.json", tmp_path / "audit.json"
    raw = json.dumps(receipt).encode()
    path.write_bytes(raw)
    read_bytes, calls = type(path).read_bytes, []
    def read_once(self):
        value = read_bytes(self)
        if self == path:
            calls.append(self)
            self.write_text(json.dumps({"later_revision": True}))
        return value
    monkeypatch.setattr(type(path), "read_bytes", read_once)
    monkeypatch.setattr(audit, "load_current_universe", lambda day: official)
    monkeypatch.setattr(audit.sys, "argv", ["audit", "--receipt", str(path), "--output", str(output)])
    assert audit.main() == 0
    report = json.loads(output.read_text())
    assert len(calls) == 1
    assert report["source_sha256"] == hashlib.sha256(raw).hexdigest()
    assert report["unknown_count"] == 3
