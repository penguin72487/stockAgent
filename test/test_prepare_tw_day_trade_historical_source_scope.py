from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from scripts import prepare_tw_day_trade_historical_source_scope as scope


def _version(root: Path, name: str, rows: dict, *, selected: bool = True) -> dict:
    folder = root / name
    folder.mkdir()
    summary = folder / "summary.json"
    summary.write_text("{}")
    weights = folder / "target_weights.parquet"
    pl.DataFrame(rows).write_parquet(weights)
    return {"summary_path": str(summary), "weights_path": str(weights),
            "effective_entry_at": "2026-01-02T09:01:00+08:00" if selected else None,
            "publication_upper_bound": "2026-01-02T09:00:30+08:00"}


def test_scope_keeps_every_nonzero_target_even_below_one_lot(tmp_path):
    a = _version(tmp_path, "a", {"symbol": ["2330", "2317", "2454"],
                                  "target_weight": [0.0000001, -0.3, 0.0]})
    b = _version(tmp_path, "b", {"symbol": ["0050", "2330"], "target_weight": [0.5, 0.0]})
    ignored = _version(tmp_path, "ignored", {"symbol": ["9999"], "target_weight": [1.0]},
                       selected=False)
    assert scope._nonzero_targets([a, b, ignored]) == {"2330", "2317", "0050"}


@pytest.mark.parametrize("rows", [
    {"symbol": ["2330", "2330"], "target_weight": [0.1, 0.2]},
    {"symbol": ["2330"], "target_weight": [float("nan")]},
    {"symbol": [""], "target_weight": [0.1]},
])
def test_scope_rejects_ambiguous_targets(tmp_path, rows):
    version = _version(tmp_path, "a", rows)
    with pytest.raises(ValueError, match="invalid or duplicate"):
        scope._nonzero_targets([version])


def test_prepare_uses_strict_plan_and_retains_unknown_open(monkeypatch, tmp_path):
    version = _version(tmp_path, "v", {"symbol": ["2330", "2317", "2454"],
                                        "target_weight": [0.00001, -0.3, 0.2]})
    plan = {"market": "m", "session_date": "2026-01-02", "plan_sha256": "pinned",
            "versions": [version]}
    raw = {"session_date": "2026-01-02", "status": "sources_pinned_not_replayed",
           "simulation_only": True, "production_order_possible": False,
           "ledger_modified": False, "source_plans": [plan]}
    revision_plan = tmp_path / "revision.json"
    revision_plan.write_text(json.dumps(raw))
    calls = []
    monkeypatch.setattr(scope, "_spec", lambda market, root, progressive:
                        (SimpleNamespace(market=market), None))
    monkeypatch.setattr(scope, "_published_revision_inventory",
                        lambda spec, day: calls.append((spec.market, day)) or plan)
    monkeypatch.setattr(scope, "_official_open_map",
                        lambda *args, **kwargs: ({"2330": 100.0}, {"twse": 1}, {"2317"}))
    monkeypatch.setattr(scope.workspace_volume, "configuration",
                        lambda: {"mount_point": str(tmp_path)})
    monkeypatch.setattr(scope.workspace_volume, "admit", lambda **kwargs: None)
    output = tmp_path / "build" / "private"
    result = scope.prepare(revision_plan=revision_plan, markets_dir=tmp_path, output_root=output)
    inventory = json.loads((output / "m/source_inventory.json").read_text())
    assert calls == [("m", date(2026, 1, 2))] * 2
    assert inventory["minute_symbols"] == ["2330"]
    assert inventory["official_no_trade_symbols"] == ["2317"]
    assert inventory["unresolved_official_open_symbols"] == ["2454"]
    assert result["ledger_modified"] is False and result["broker_orders_created"] == 0
    with pytest.raises(FileExistsError):
        scope.prepare(revision_plan=revision_plan, markets_dir=tmp_path, output_root=output)


def test_prepare_rejects_changed_revision_before_any_output(monkeypatch, tmp_path):
    raw = {"session_date": "2026-01-02", "status": "sources_pinned_not_replayed",
           "simulation_only": True, "production_order_possible": False,
           "ledger_modified": False, "source_plans": [{"market": "m", "plan_sha256": "old"}]}
    revision_plan = tmp_path / "revision.json"
    revision_plan.write_text(json.dumps(raw))
    monkeypatch.setattr(scope, "_spec", lambda market, root, progressive:
                        (SimpleNamespace(market=market), None))
    monkeypatch.setattr(scope, "_published_revision_inventory", lambda *args: {"plan_sha256": "new"})
    monkeypatch.setattr(scope.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path)})
    monkeypatch.setattr(scope.workspace_volume, "admit", lambda **kwargs: None)
    output = tmp_path / "build" / "private"
    with pytest.raises(RuntimeError, match="revisions changed"):
        scope.prepare(revision_plan=revision_plan, markets_dir=tmp_path, output_root=output)
    assert not output.exists()


def test_official_open_lookup_covers_symbols_introduced_by_later_revision(monkeypatch, tmp_path):
    first = _version(tmp_path, "first", {"symbol": ["2330"], "target_weight": [0.1]})
    later = _version(tmp_path, "later", {"symbol": ["2317"], "target_weight": [-0.1]})
    plan = {"market": "m", "session_date": "2026-01-02", "plan_sha256": "pinned",
            "versions": [first, later]}
    raw = {"session_date": "2026-01-02", "status": "sources_pinned_not_replayed",
           "simulation_only": True, "production_order_possible": False,
           "ledger_modified": False, "source_plans": [plan]}
    revision_plan = tmp_path / "revision.json"
    revision_plan.write_text(json.dumps(raw))
    monkeypatch.setattr(scope, "_spec", lambda market, root, progressive:
                        (SimpleNamespace(market=market), None))
    monkeypatch.setattr(scope, "_published_revision_inventory", lambda *args: plan)
    def official(specs, selected, session, **kwargs):
        assert {row["symbol"] for row in selected["m"][4]} == {"2330", "2317"}
        return {"2330": 100, "2317": 150}, {"twse": 2}, set()
    monkeypatch.setattr(scope, "_official_open_map", official)
    monkeypatch.setattr(scope.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path)})
    monkeypatch.setattr(scope.workspace_volume, "admit", lambda **kwargs: None)
    output = tmp_path / "build" / "private"
    scope.prepare(revision_plan=revision_plan, markets_dir=tmp_path, output_root=output)
    inventory = json.loads((output / "m/source_inventory.json").read_text())
    assert inventory["minute_symbols"] == ["2317", "2330"]
    assert len(inventory["scope_implementation_sha256"]) == 64


def test_capacity_failure_happens_before_expensive_revision_scan(monkeypatch, tmp_path):
    raw = {"session_date": "2026-01-02", "status": "sources_pinned_not_replayed",
           "simulation_only": True, "production_order_possible": False,
           "ledger_modified": False, "source_plans": [{"market": "m", "plan_sha256": "old"}]}
    revision_plan = tmp_path / "revision.json"
    revision_plan.write_text(json.dumps(raw))
    monkeypatch.setattr(scope.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path)})
    def reject(**kwargs):
        raise RuntimeError("physical capacity unavailable")
    monkeypatch.setattr(scope.workspace_volume, "admit", reject)
    monkeypatch.setattr(scope, "_spec", lambda *args: pytest.fail("must not scan before admission"))
    with pytest.raises(RuntimeError, match="physical capacity unavailable"):
        scope.prepare(revision_plan=revision_plan, markets_dir=tmp_path,
                      output_root=tmp_path / "build" / "private")
