from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import audit_tw_day_trade_historical_replay_inputs as diagnostic


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def anchor_receipt(tmp_path):
    state = tmp_path / "state.json"
    state.write_text("{}")
    code = tmp_path / "audit.py"
    code.write_text("verified")
    receipt = {"contract": "historical_flat_paper_anchor_component_v1",
               "status": "prior_flat_anchor_component_verified_not_replayed",
               "market": "m", "target_session": "2026-10-30", "simulation_only": True,
               "ledger_modified": False, "publication_authorized": False,
               "source_pins": {"anchor_state": {"path": str(state), "sha256": _sha(state)}},
               "implementation_sha256": {str(code): _sha(code)}}
    return receipt, state, code


def test_checked_anchor_accepts_only_exact_pinned_paper_evidence(anchor_receipt):
    receipt, state, _ = anchor_receipt
    assert diagnostic._checked_anchor(receipt, market="m", day=date(2026, 10, 30)) == state.parent
    with pytest.raises(ValueError, match="matching"):
        diagnostic._checked_anchor(receipt, market="other", day=date(2026, 10, 30))
    with pytest.raises(ValueError, match="matching"):
        diagnostic._checked_anchor(receipt, market="m", day=date(2026, 10, 29))


def test_checked_anchor_rejects_source_or_implementation_change(anchor_receipt):
    receipt, state, code = anchor_receipt
    state.write_text('{"changed":true}')
    with pytest.raises(RuntimeError, match="anchor source changed"):
        diagnostic._checked_anchor(receipt, market="m", day=date(2026, 10, 30))
    state.write_text("{}")
    code.write_text("different")
    with pytest.raises(RuntimeError, match="implementation changed"):
        diagnostic._checked_anchor(receipt, market="m", day=date(2026, 10, 30))


def test_prepare_rejects_current_or_future_session_before_io(tmp_path):
    with pytest.raises(ValueError, match="completed historical"):
        diagnostic.prepare(day=date(9999, 1, 1), market="m", anchor_receipt=tmp_path / "x",
                           scope_root=tmp_path, minute_root=tmp_path, price_limit_root=tmp_path,
                           output_root=tmp_path / "out", markets_dir=tmp_path)


def test_prepare_keeps_unpriced_target_and_never_creates_a_fill(tmp_path, monkeypatch):
    anchor = tmp_path / "anchor"
    anchor.mkdir()
    state = anchor / "state.json"
    state.write_text(json.dumps({"modes": {"m": {"session_date": "2026-10-29"}}}))
    code = tmp_path / "auditor.py"
    code.write_text("pinned")
    receipt = {"contract": "historical_flat_paper_anchor_component_v1",
               "status": "prior_flat_anchor_component_verified_not_replayed",
               "market": "m", "target_session": "2026-10-30", "simulation_only": True,
               "ledger_modified": False, "publication_authorized": False,
               "source_pins": {"anchor_state": {"path": str(state), "sha256": _sha(state)}},
               "implementation_sha256": {str(code): _sha(code)}}
    receipt_path = tmp_path / "anchor.json"
    receipt_path.write_text(json.dumps(receipt))
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"config_fingerprint": "c", "checkpoint_fingerprint": "k",
                                   "generated_at": "2026-10-30T09:02:00+08:00"}))
    scope = tmp_path / "scope/m"
    scope.mkdir(parents=True)
    plan = {"market": "m", "session_date": "2026-10-30", "plan_sha256": "pinned",
            "selected_revision_count": 2,
            "versions": [{"summary_path": str(summary), "signal_id": "s"}]}
    inventory = {"session_date": "2026-10-30", "plan_sha256": "pinned", "ledger_modified": False,
                 "nonzero_target_count": 2, "official_open_unavailable_symbols": ["5906"],
                 "unresolved_official_open_symbols": ["5906"]}
    (scope / "published_revision_plan.json").write_text(json.dumps(plan))
    (scope / "source_inventory.json").write_text(json.dumps(inventory))
    monkeypatch.setattr(diagnostic.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path)})
    monkeypatch.setattr(diagnostic.workspace_volume, "admit", lambda **kwargs: None)
    monkeypatch.setattr(diagnostic.repair, "_require_missing_session_target", lambda *a, **kw: None)
    monkeypatch.setattr(diagnostic.repair, "_spec", lambda *a: (SimpleNamespace(), object()))
    monkeypatch.setattr(diagnostic.repair, "_published_revision_inventory", lambda *a: plan)
    monkeypatch.setattr(diagnostic.repair, "_anchor", lambda mode: {"session_date": mode["session_date"]})
    def prepared(manifest, spec, live_config, *, fetch_current):
        assert fetch_current is True and manifest["simulation_only"] is True
        assert manifest["nontrade_tick_receipt"] is None
        return [{"day": date(2026, 10, 30), "revisions": [{}], "bars": {"2330": {}}}], []
    monkeypatch.setattr(diagnostic.repair, "_prepared_sessions", prepared)
    monkeypatch.setattr(diagnostic.repair, "_assert_source_pins", lambda pins: None)
    out = tmp_path / "build/private"
    result = diagnostic.prepare(day=date(2026, 10, 30), market="m", anchor_receipt=receipt_path,
                                scope_root=scope.parent, minute_root=tmp_path, price_limit_root=tmp_path,
                                output_root=out, markets_dir=tmp_path)
    assert result["status"] == "inputs_checked_not_replayed"
    assert result["unresolved_official_open_symbols"] == ["5906"]
    assert result["paper_fills_created"] == 0 and result["ledger_modified"] is False
    assert (out / "historical_input_diagnostic.json").is_file()
