from datetime import date
from pathlib import Path

import pytest

from scripts import build_tw_day_trade_private_historical_candidate as candidate


@pytest.mark.parametrize("damage", [None, "missing_open", "unknown_open", "wrong_market", "wrong_day", "changed_plan"])
def test_private_candidate_refuses_incomplete_or_changed_decision_scope(monkeypatch, damage):
    plan = {"market": "m", "session_date": "2026-10-30", "plan_sha256": "plan-sha"}
    inventory = {"session_date": "2026-10-30", "ledger_modified": False, "plan_sha256": "plan-sha",
                 "official_open_unavailable_symbols": [], "unresolved_official_open_symbols": []}
    if damage == "missing_open": inventory["official_open_unavailable_symbols"] = ["5906"]
    if damage == "unknown_open": inventory["unresolved_official_open_symbols"] = ["5906"]
    if damage == "wrong_market": plan["market"] = "other"
    if damage == "wrong_day": inventory["session_date"] = "2026-10-29"
    if damage == "changed_plan": inventory["plan_sha256"] = "other"
    monkeypatch.setattr(candidate.owner, "_object", lambda path: plan if path.name == "published_revision_plan.json" else inventory)
    monkeypatch.setattr(candidate.owner, "_published_revision_inventory", lambda spec, day: {"plan_sha256": "plan-sha"})
    kwargs = {"scope": Path("/scope"), "spec": object(), "market": "m", "day": date(2026, 10, 30)}
    if damage:
        with pytest.raises(RuntimeError): candidate._check_plan(**kwargs)
    else:
        assert candidate._check_plan(**kwargs) == (plan, inventory)


@pytest.mark.parametrize("change", ["publication", "broker_order", "modified", "wrong_session", "unverified"])
def test_private_candidate_rejects_untrusted_anchor_receipt(monkeypatch, change):
    proof = {"contract": "historical_flat_snapshot_anchor_component_v1",
             "status": "snapshot_accounts_verified_not_replayed", "target_session": "2026-10-30",
             "simulation_only": True, "broker_orders_created": 0, "ledger_modified": False,
             "publication_authorized": False}
    if change == "publication": proof["publication_authorized"] = True
    if change == "broker_order": proof["broker_orders_created"] = 1
    if change == "modified": proof["ledger_modified"] = True
    if change == "wrong_session": proof["target_session"] = "2026-10-29"
    if change == "unverified": proof["status"] = "candidate"
    monkeypatch.setattr(candidate.owner, "_object", lambda path: proof)
    with pytest.raises(ValueError):
        candidate._audited_mode(Path("receipt"), market="m", day=date(2026, 10, 30), live_dir=Path("live"))


def test_private_candidate_cannot_write_into_live_root(monkeypatch, tmp_path):
    monkeypatch.setattr(candidate.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path / "workspace")})
    monkeypatch.setattr(candidate.workspace_volume, "admit", lambda **kwargs: pytest.fail("capacity should not be queried"))
    with pytest.raises(ValueError, match="isolated"):
        candidate.build(day=date(2026, 10, 30), market="m", anchor_receipt=tmp_path / "receipt",
            scope_root=tmp_path, minute_root=tmp_path, price_limit_root=tmp_path,
            output_root=tmp_path / "live", live_dir=tmp_path / "live", markets_dir=tmp_path)


def test_private_candidate_uses_canonical_limit_snapshot_or_verified_overlay(monkeypatch, tmp_path):
    monkeypatch.setattr(candidate, "ROOT", tmp_path)
    canonical = tmp_path / "artifacts/live/tw_price_limits"
    canonical.mkdir(parents=True)
    with pytest.raises(ValueError, match="exact-session"):
        candidate._price_limit_source(canonical, day=date(2026, 11, 2))
    (canonical / "2026-11-02.parquet").write_bytes(b"fixture")
    assert candidate._price_limit_source(canonical, day=date(2026, 11, 2)) is None
    overlay = tmp_path / "overlay"
    overlay.mkdir()
    with pytest.raises(ValueError, match="source manifest"):
        candidate._price_limit_source(overlay, day=date(2026, 11, 2))
    (overlay / "finmind_price_limits_source.json").write_text("{}")
    assert candidate._price_limit_source(overlay, day=date(2026, 11, 2)) == str(overlay)
