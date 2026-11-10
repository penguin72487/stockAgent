from datetime import datetime, timedelta
import json

import polars as pl
import pytest

from scripts.audit_tw_day_trade_provisional_source import audit_marks, audit_published_decisions
from scripts.repair_tw_day_trade_flat_opening import _sha256


def test_audit_detects_falsely_fresh_and_unproven_carry():
    day = "2026-11-04"
    start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
    mode = {"market": "m", "positions": {"p": {"session_date": day, "symbol": "2330",
        "entry_at": start.isoformat(), "exit_at": f"{day}T09:03:00+08:00"}}}
    marks = []
    for i in range(270):
        held, stale = i < 2, i == 1
        marks.append({"market": "m", "session_date": day,
            "minute": (start + timedelta(minutes=i)).isoformat(),
            "historical_minute_replay": True, "provisional_minute_source_coverage": True,
            "open_position_count": int(held), "stale_position_count": int(stale),
            "last_trade_carried_position_count": int(stale), "fresh_trade_position_count": int(held and not stale),
            "missing_price_position_count": 0, "valuation_stale": stale})
    bars = {"2330": {f"{day}T09:01+08:00": {"close": 100., "volume_shares": 1000}}}
    assert audit_marks(mode, marks, bars, day=day)["carried_valuation_minutes"] == 1
    marks[1].update(valuation_stale=False, stale_position_count=0,
                    last_trade_carried_position_count=0, fresh_trade_position_count=1)
    with pytest.raises(RuntimeError, match="falsely reports"):
        audit_marks(mode, marks, bars, day=day)
    marks[1].update(valuation_stale=True, stale_position_count=1,
                    last_trade_carried_position_count=1, fresh_trade_position_count=0)
    with pytest.raises(RuntimeError, match="falsely reports"):
        audit_marks(mode, marks, {"2330": {}}, day=day)


def _published_candidate(tmp_path):
    day, market, token = "2026-11-04", "m", "repair1"
    versions, rows = [], []
    for index, (published, effective) in enumerate([
        (f"{day}T09:03:00+08:00", f"{day}T09:01:00+08:00"),
        (f"{day}T09:03:30+08:00", f"{day}T09:05:00+08:00"),
    ]):
        summary, weights = tmp_path / f"summary{index}.json", tmp_path / f"weights{index}.parquet"
        summary.write_text("{}", encoding="utf-8")
        pl.DataFrame({"symbol": ["2330", "0050"], "target_weight": [0.2 + index * 0.1, -0.1]}).write_parquet(weights)
        source_id = f"signal-{index}"
        version = {"signal_id": source_id, "summary_path": str(summary), "weights_path": str(weights),
                   "summary_sha256": _sha256(summary), "weights_sha256": _sha256(weights),
                   "publication_upper_bound": published, "effective_entry_at": effective,
                   "signal_started_at": published}
        versions.append(version)
        for symbol, weight in [("2330", 0.2 + index * 0.1), ("0050", -0.1)]:
            rows.append({"market": market, "session_date": day,
                         "signal_id": f"{source_id}:published-minute-repair:{token}",
                         "symbol": symbol, "target_weight": weight, "simulation_replay": True,
                         "counterfactual_open_replay": True, "replay_basis": "test_contract",
                         "signal_source_path": str(summary), "signal_at": effective,
                         "recorded_at": effective, "source_signal_at": published,
                         "opening_signal_revision": {"index": index}})
    orders = [{"market": market, "session_date": day, "simulation_only": True, "symbol": "2330",
               "order_id": "o", "quantity": 1000, "purpose": "entry"}]
    fills = [{"market": market, "session_date": day, "simulation_only": True,
              "symbol": "2330", "order_id": "o"}]
    for name, values in [("signals.jsonl", rows), ("orders.jsonl", orders), ("fills.jsonl", fills)]:
        (tmp_path / name).write_text("".join(json.dumps(row) + "\n" for row in values), encoding="utf-8")
    for name in ("marks.jsonl", "events.jsonl"):
        (tmp_path / name).write_text("", encoding="utf-8")
    manifest = {"market": market, "contract": "test_contract", "sessions": [{"session_date": day,
                 "repair_token": token}], "published_revision_plan": {"versions": versions}}
    return manifest, rows, orders, fills


def test_decision_audit_checks_all_versions_and_late_original_clock(tmp_path):
    manifest, _, _, _ = _published_candidate(tmp_path)
    proof = audit_published_decisions(tmp_path, manifest)
    assert proof["published_decisions"] == 2
    assert proof["signal_rows"] == 4
    assert proof["paper_order_rows"] == proof["paper_fill_rows"] == 1
    assert proof["first_signal_later_than_counterfactual"] is True


@pytest.mark.parametrize("corruption", ["changed_weight", "missing_row", "duplicate_row", "clock", "fill_order"])
def test_decision_audit_rejects_missing_or_modified_evidence(tmp_path, corruption):
    manifest, rows, orders, fills = _published_candidate(tmp_path)
    if corruption == "changed_weight":
        rows[0]["target_weight"] = 0.3
    elif corruption == "missing_row":
        rows.pop()
    elif corruption == "duplicate_row":
        rows.append(rows[0])
    elif corruption == "clock":
        rows[0]["source_signal_at"] = "2026-11-04T09:00:00+08:00"
    else:
        fills[0]["order_id"] = "missing"
    for name, values in [("signals.jsonl", rows), ("orders.jsonl", orders), ("fills.jsonl", fills)]:
        (tmp_path / name).write_text("".join(json.dumps(row) + "\n" for row in values), encoding="utf-8")
    with pytest.raises(RuntimeError):
        audit_published_decisions(tmp_path, manifest)


def test_decision_audit_rejects_later_revision_before_full_minute(tmp_path):
    manifest, _, _, _ = _published_candidate(tmp_path)
    manifest["published_revision_plan"]["versions"][1]["effective_entry_at"] = "2026-11-04T09:04:00+08:00"
    with pytest.raises(RuntimeError, match="before publication"):
        audit_published_decisions(tmp_path, manifest)


def test_decision_audit_rejects_foreign_event(tmp_path):
    manifest, _, _, _ = _published_candidate(tmp_path)
    (tmp_path / "events.jsonl").write_text('{"market":"other","session_date":"2026-11-04"}\n')
    with pytest.raises(RuntimeError, match="foreign replay"):
        audit_published_decisions(tmp_path, manifest)
