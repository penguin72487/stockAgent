from datetime import date
import hashlib
import json
from types import SimpleNamespace

import polars as pl
import pytest

from scripts import audit_tw_day_trade_signal_revisions as audit
from stockagent.live.market_config import SIGNAL_UPDATE_PROGRESSIVE
from stockagent.live.opening_signal_revision import revision_fields


def _artifact(root, signal_id, published):
    path = root / "2026-10-28 09:00:01" / signal_id / "summary.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"signal_id": signal_id, "market": "m", "execution_mode": "tw_day_trade",
                                "generated_at": "2026-10-28T09:00:01+08:00",
                                "artifact_published_at": published,
                                "execution_pointer_written_at": published}))
    pl.DataFrame({"symbol": ["2330", "0050"], "target_weight": [0.1, 0.0]}).write_parquet(
        path.with_name("target_weights.parquet"))
    return path


def _inventory(events):
    return {"contract": "first_published_signal_and_local_minute_inventory_v1",
            "session_date": "2026-10-28", "simulation_only": True, "production_order_possible": False,
            "logical_event_journal_sha256": "journal-sha", "modes": {"m": {
                "first_published_signal_id": "first", "first_published_at": "2026-10-28T09:00:02+08:00",
                "registration_count": len(events), "registrations": events}}}


def _event(signal, when):
    return {"signal_id": signal, "recorded_at": when, "simulation_replay": False}


def test_revision_audit_detects_future_publication_without_collapsing(tmp_path):
    _artifact(tmp_path, "first", "2026-10-28T09:00:02.900000+08:00")
    _artifact(tmp_path, "later", "2026-10-28T09:02:00.100000+08:00")
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path)
    events = [_event("first", "2026-10-28T09:00:02+08:00"),
              _event("later", "2026-10-28T09:01:00+08:00")]
    report = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    mode = report["modes"]["m"]
    assert [x["signal_id"] for x in mode["versions"]] == ["first", "later"]
    assert mode["versions"][0]["errors"] == []  # same second is not order proof
    assert mode["versions"][0]["weights_sha256"]
    assert mode["versions"][1]["errors"] == ["published_after_registration"]
    assert report["all_registered_artifacts_valid_and_causal"] is False
    assert report["ledger_modified"] is False


def test_revision_audit_exposes_missing_duplicate_and_regressed_versions(tmp_path):
    _artifact(tmp_path, "first", "2026-10-28T09:00:02+08:00")
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path)
    events = [_event("first", "2026-10-28T09:00:03+08:00"),
              _event("first", "2026-10-28T09:00:02+08:00"),
              _event("missing", "2026-10-28T09:00:04+08:00")]
    result = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    assert result["modes"]["m"]["issue_counts"] == {
        "missing_or_duplicate_signal_id": 1, "missing_signal_artifact": 1, "registration_clock_regressed": 1}


def test_pointer_write_clock_is_an_upper_bound_not_exact_visibility(tmp_path):
    path = _artifact(tmp_path, "first", "2026-10-28T09:00:02.950000+08:00")
    summary = json.loads(path.read_text())
    summary["execution_pointer_written_at"] = "2026-10-28T09:00:03.100000+08:00"
    path.write_text(json.dumps(summary))
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path)
    events = [_event("first", "2026-10-28T09:00:02+08:00")]
    result = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    assert result["modes"]["m"]["versions"][0]["errors"] == [
        "publication_order_unresolved_within_clock_intervals"]
    assert not result["all_registered_artifacts_valid_and_causal"]
    assert result["pointer_written_at_is_visibility_upper_bound_not_exact_clock"]


def test_reversed_rich_artifact_clock_fails_closed(tmp_path):
    path = _artifact(tmp_path, "first", "2026-10-28T09:00:02+08:00")
    summary = json.loads(path.read_text())
    summary["execution_pointer_written_at"] = "2026-10-28T09:00:03+08:00"
    summary["artifact_completed_at"] = "2026-10-28T09:00:02.500000+08:00"
    path.write_text(json.dumps(summary))
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path)
    events = [_event("first", "2026-10-28T09:00:04+08:00")]
    result = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    assert result["modes"]["m"]["versions"][0]["errors"] == ["artifact_completed_before_pointer_write"]
    assert not result["all_registered_artifacts_valid_and_causal"]


def test_revision_audit_rejects_wrong_or_non_paper_inventory(tmp_path):
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path)
    inventory = _inventory([])
    inventory["production_order_possible"] = True
    with pytest.raises(ValueError, match="simulation-only"):
        audit.inspect_revisions(day=date(2026, 10, 28), inventory=inventory, specs=[spec])
    inventory["production_order_possible"] = False
    with pytest.raises(ValueError, match="enabled modes differ"):
        audit.inspect_revisions(day=date(2026, 10, 28), inventory=inventory, specs=[])


def test_revision_audit_preserves_and_verifies_progressive_lineage(tmp_path):
    first_path = _artifact(tmp_path, "first", "2026-10-28T09:00:02+08:00")
    second_path = _artifact(tmp_path, "later", "2026-10-28T09:00:04+08:00")
    first = json.loads(first_path.read_text())
    first.update(revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id="first", input_fingerprint="a" * 64))
    first_path.write_text(json.dumps(first))
    second = json.loads(second_path.read_text())
    second.update(revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id="later",
                                  input_fingerprint="b" * 64, previous=first))
    second_path.write_text(json.dumps(second))
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path,
                           signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)
    events = [_event("first", "2026-10-28T09:00:03+08:00"),
              _event("later", "2026-10-28T09:00:05+08:00")]
    result = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    assert result["all_registered_artifacts_valid_and_causal"]
    assert result["modes"]["m"]["versions"][1]["opening_signal_revision"]["parent_signal_id"] == "first"
    second["opening_signal_revision"]["parent_signal_id"] = "unrelated"
    second_path.write_text(json.dumps(second))
    result = audit.inspect_revisions(day=date(2026, 10, 28), inventory=_inventory(events), specs=[spec])
    assert result["modes"]["m"]["issue_counts"]["revision_parent_index_mismatch"] == 1


def test_publications_are_not_falsely_labelled_as_registered_fills(tmp_path):
    first_path = _artifact(tmp_path, "first", "2026-10-28T09:00:02+08:00")
    later_path = _artifact(tmp_path, "later", "2026-10-28T09:00:04+08:00")
    first = json.loads(first_path.read_text())
    first.update(revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id="first", input_fingerprint="a" * 64))
    first_path.write_text(json.dumps(first))
    later = json.loads(later_path.read_text())
    later.update(revision_fields(policy=SIGNAL_UPDATE_PROGRESSIVE, signal_id="later",
                                 input_fingerprint="b" * 64, previous=first))
    later_path.write_text(json.dumps(later))
    inventory = _inventory([_event("later", "2026-10-28T09:01:00+08:00")])
    inventory["modes"]["m"].update(summary_path=str(first_path),
                                    summary_sha256=hashlib.sha256(first_path.read_bytes()).hexdigest())
    spec = SimpleNamespace(market="m", signal_market=None, live_output_dir=tmp_path,
                           signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)
    report = audit.inspect_publications(day=date(2026, 10, 28), inventory=inventory, specs=[spec])
    mode = report["modes"]["m"]
    assert mode["publication_count"] == 2
    assert mode["published_but_not_registered_count"] == 1
    assert not mode["versions"][0]["was_registered"]
    assert mode["versions"][1]["ledger_registration_times"] == ["2026-10-28T09:01:00+08:00"]
    assert report["status"] == "published_source_audit_only_not_ledger_registration_or_replay"
    assert report["production_order_possible"] is False
