import copy
from datetime import date
import json
from pathlib import Path

import pytest

from scripts import audit_tw_day_trade_private_candidate_chain as audit


def test_pin_checks_detect_modified_files_and_conflicting_hashes(tmp_path):
    path = tmp_path / "source.json"
    path.write_text("original")
    original = audit.owner._sha256(path)
    pins = audit._Pins()
    pins.check(path, original)
    pins.check(path, original)
    with pytest.raises(RuntimeError, match="conflicting"):
        pins.check(path, "0" * 64)
    path.write_text("modified")
    with pytest.raises(RuntimeError, match="changed"):
        pins.unchanged()


def test_pin_rejects_missing_or_invalid_hash(tmp_path):
    path = tmp_path / "source"
    path.write_text("data")
    with pytest.raises(ValueError, match="SHA-256"):
        audit._Pins().check(path, "short")
    with pytest.raises(RuntimeError, match="source pin changed"):
        audit._Pins().check(path, "f" * 64)


def test_zero_source_proof_may_gain_sources_but_never_lose_or_change_one():
    original = [{"path": "/old", "sha256": "a" * 64}]
    new = [*original, {"path": "/new", "sha256": "b" * 64}]
    assert audit._source_pins_retained(original, new)
    assert not audit._source_pins_retained(original, [])
    assert not audit._source_pins_retained([], new)
    assert not audit._source_pins_retained(original, [{"path": "/old", "sha256": "c" * 64}])


def _write_json(path, obj):
    path.write_text(json.dumps(obj), encoding="utf-8")


@pytest.mark.parametrize("damage", [None, "receipt", "state", "publication", "event", "fill",
                                    "marks", "coverage", "zero_source", "inherited"])
def test_projection_requires_exact_candidate_and_source_proof(tmp_path, damage):
    market = "example"
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    receipt_path = tmp_path / "private_candidate_receipt.json"
    _write_json(receipt_path, {"fixture": True})
    digest = audit.owner._sha256(receipt_path)
    mode = {"market": market, "total_equity_twd": 123}
    events = candidate / "events.jsonl"
    fills = candidate / "fills.jsonl"
    events.write_text('{"event":1}\n')
    fills.write_text('{"fill":1}\n')
    (candidate / "marks.jsonl").write_text('{"minute":"09:01"}\n')
    snapshot = tmp_path / "snapshot.json"
    _write_json(snapshot, {"modes": {market: mode}, "private_chain_projection": {
        "candidate_receipt": str(receipt_path), "candidate_receipt_sha256": digest,
        "publication_authorized": False}})
    marks = tmp_path / "marks.jsonl"
    marks.write_text('{"minute":"previous"}\n{"minute":"09:01"}\n')
    coverage = {"minute_points": 270, "unsourced_held_price_minutes": 0}
    extra = [{"path": "/source", "sha256": "a" * 64}]
    receipt = {"market": market, "candidate_file_sha256": {
        "events.jsonl": audit.owner._sha256(events), "fills.jsonl": audit.owner._sha256(fills)}}
    anchor = {"private_chain_projection": True, "private_candidate_receipt": str(receipt_path),
        "private_candidate_receipt_sha256": digest, "strict_minute_coverage": copy.deepcopy(coverage),
        "observed_zero_trade_source_pins": copy.deepcopy(extra), "source_pins": {
            "snapshot": {"path": str(snapshot)}, "events": {"sha256": audit.owner._sha256(events)},
            "fills": {"sha256": audit.owner._sha256(fills)}, "marks": {"path": str(marks)}}}
    if damage == "receipt": anchor["private_candidate_receipt_sha256"] = "wrong"
    if damage == "state": mode = {**mode, "total_equity_twd": 124}
    if damage == "publication":
        value = json.loads(snapshot.read_text())
        value["private_chain_projection"]["publication_authorized"] = True
        _write_json(snapshot, value)
    if damage == "event": anchor["source_pins"]["events"]["sha256"] = "wrong"
    if damage == "fill": anchor["source_pins"]["fills"]["sha256"] = "wrong"
    if damage == "marks": marks.write_text('{"minute":"previous"}\n{"minute":"09:02"}\n')
    if damage == "coverage": anchor["strict_minute_coverage"]["minute_points"] = 269
    if damage == "zero_source": anchor["observed_zero_trade_source_pins"] = []
    if damage == "inherited": anchor["inherited_unverified_minute_gaps"] = [{"session_date": "2026-10-30"}]
    kwargs = {"prior_receipt_path": receipt_path, "prior_receipt": receipt,
              "prior_candidate": candidate, "prior_mode": mode, "coverage": coverage, "extra_pins": extra}
    if damage:
        with pytest.raises(RuntimeError):
            audit._check_projection(anchor, **kwargs)
    else:
        audit._check_projection(anchor, **kwargs)


def test_candidate_must_be_inside_isolated_workspace(tmp_path):
    with pytest.raises(ValueError, match="isolated"):
        audit._candidate(tmp_path / "receipt.json", market="m", expected_day=None,
                         build_root=tmp_path / "build", spec=None, pins=audit._Pins())


@pytest.mark.parametrize("signal", ["", "plain", "x:published-minute-repair:nope",
                                    "x:published-minute-repair:123456789abc:published-minute-repair:123456789abc"])
def test_decision_audit_rejects_untrusted_replay_tokens(tmp_path, signal):
    receipt = {"session": [{"session_date": "2026-11-04", "signal_id": signal}]}
    with pytest.raises(RuntimeError, match="identity|token"):
        audit._audit_published_decisions(receipt, candidate=tmp_path, market="m", day=date(2026, 11, 4),
                                         plan={}, pins=audit._Pins())


def test_decision_audit_uses_original_plan_and_explicit_replay_token(monkeypatch, tmp_path):
    receipt = {"session": [{"session_date": "2026-11-04",
                            "signal_id": "s:published-minute-repair:123456789abc"}]}
    plan = {"versions": [{"signal_id": "s"}]}
    def check(candidate, manifest):
        assert candidate == tmp_path
        assert manifest["published_revision_plan"] is plan
        assert manifest["sessions"][0]["repair_token"] == "123456789abc"
        assert manifest["contract"] == audit.owner.PUBLISHED_SESSION_CONTRACT
        return {"original_targets_and_clocks_verified": True}
    monkeypatch.setattr(audit.chain.provisional_audit, "audit_published_decisions", check)
    result = audit._audit_published_decisions(receipt, candidate=tmp_path, market="m", day=date(2026, 11, 4),
                                              plan=plan, pins=audit._Pins())
    assert result["original_targets_and_clocks_verified"]


def test_inherited_unknown_minutes_cannot_disappear_or_change():
    older = {"session_date": "2026-10-30", "candidate_receipt": "/old", "candidate_receipt_sha256": "a" * 64,
             "unknown_carried_minute_symbols": {"2026-10-30T10:32": ["6213"]}}
    newer = {"session_date": "2026-11-02", "candidate_receipt": "/new", "candidate_receipt_sha256": "b" * 64,
             "unknown_carried_minute_symbols": {}}
    expected = {**older, "missing_is_not_zero_and_fills_are_not_invented": True}
    sessions = [newer, older]
    assert audit._verify_inherited_gaps(sessions, {"2026-10-30": [expected]}) == [expected]
    with pytest.raises(RuntimeError, match="lost or changed"):
        audit._verify_inherited_gaps(sessions, {"2026-10-30": []})
    altered = {**expected, "unknown_carried_minute_symbols": {"2026-10-30T10:32": ["9999"]}}
    with pytest.raises(RuntimeError, match="lost or changed"):
        audit._verify_inherited_gaps(sessions, {"2026-10-30": [altered]})
