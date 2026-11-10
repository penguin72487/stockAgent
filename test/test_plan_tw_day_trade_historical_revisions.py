from datetime import date
from pathlib import Path

import pytest

from scripts import plan_tw_day_trade_historical_revisions as planner


def test_plan_uses_canonical_revision_owner_without_ledger_writes(monkeypatch):
    seen = []
    monkeypatch.setattr(planner, "_spec", lambda market, root, progressive: (market, None))

    def inventory(spec, day):
        seen.append((spec, day))
        return {"market": spec, "session_date": day.isoformat(), "plan_sha256": "source-pin"}

    monkeypatch.setattr(planner, "_published_revision_inventory", inventory)
    result = planner.plan(session=date(2026, 1, 2), markets=["a", "b"], markets_dir=Path("."))
    assert seen == [("a", date(2026, 1, 2)), ("b", date(2026, 1, 2))]
    assert result["status"] == "sources_pinned_not_replayed"
    assert result["later_session_accounting_replay_required"] is True
    assert result["ledger_modified"] is False
    assert result["broker_orders_created"] == 0


def test_plan_rejects_duplicate_markets_and_does_not_hide_source_errors(monkeypatch):
    with pytest.raises(ValueError, match="unique"):
        planner.plan(session=date(2026, 1, 2), markets=["a", "a"], markets_dir=Path("."))
    monkeypatch.setattr(planner, "_spec", lambda market, root, progressive: (market, None))
    monkeypatch.setattr(planner, "_published_revision_inventory",
                        lambda *_: (_ for _ in ()).throw(RuntimeError("changed source")))
    with pytest.raises(RuntimeError, match="changed source"):
        planner.plan(session=date(2026, 1, 2), markets=["a"], markets_dir=Path("."))


def test_plan_receipt_is_private_and_never_overwrites_an_existing_file(tmp_path):
    target = tmp_path / "receipt.json"
    planner._write_new_plan(target, {"status": "first"})
    assert target.stat().st_mode & 0o777 == 0o600
    original = target.read_bytes()
    with pytest.raises(FileExistsError):
        planner._write_new_plan(target, {"status": "second"})
    assert target.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["receipt.json"]
