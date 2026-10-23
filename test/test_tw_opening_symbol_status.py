from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json

import pytest

from scripts.audit_tw_opening_symbol_status import classify_symbols, receipt_snapshot
from scripts import audit_tw_opening_symbol_status as audit


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
