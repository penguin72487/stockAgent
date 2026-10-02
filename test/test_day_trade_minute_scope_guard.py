from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from scripts import maintain_tw_day_trade_minute_curves as maintenance
from scripts import rebuild_tw_day_trade_minute_curves as rebuild
from stockagent.live.tw_day_trade_simulation import TERMINAL_CLOSE_UNLIMITED_CONTRACT


def _scope_fixture(root: Path, event: dict) -> None:
    (root / "rebuild_receipt.json").write_text(json.dumps({"sessions": []}))
    (root / "state.json").write_text(json.dumps({"modes": {
        "active": {"session_date": "2026-09-30", "positions": {}, "engine_status": "active"},
    }}))
    (root / "events.jsonl").write_text(json.dumps(event) + "\n")


def _settlement() -> dict:
    return {
        "event": "unlimited_close_paper_settlement", "market": "active",
        "recorded_at": "2026-09-29T06:01:01Z",
        "assumption_contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT,
        "settled_count": 7, "remaining_count": 0,
    }


def test_scope_accepts_explicit_completed_paper_settlement(tmp_path: Path) -> None:
    _scope_fixture(tmp_path, _settlement())
    assert maintenance._completed_scope(tmp_path) == (["2026-09-29"], {"active"})


@pytest.mark.parametrize("invalid", [
    {"remaining_count": 1}, {"remaining_count": False}, {"remaining_count": "0"},
    {"remaining_count": None}, {"settled_count": 0}, {"settled_count": -1},
    {"settled_count": True}, {"settled_count": "7"}, {"settled_count": None},
    {"assumption_contract": "unknown"}, {"market": "retired"},
    {"event": "signal_registered"},
])
def test_scope_rejects_incomplete_or_ambiguous_paper_settlement(
    tmp_path: Path, invalid: dict,
) -> None:
    _scope_fixture(tmp_path, _settlement() | invalid)
    assert maintenance._completed_scope(tmp_path) == ([], {"active"})


def _receipt() -> dict:
    return {
        "start_date": "2026-09-28", "end_date": "2026-09-29",
        "strategy": {"session_dates": ["2026-09-28", "2026-09-29"],
                     "markets": ["a", "b"], "generated_rows": 1080},
    }


def test_scope_metadata_is_shared_and_not_a_full_source_acceptance() -> None:
    # Deliberately no source/hash/NAV proof: this gate only checks scope.
    assert rebuild.minute_curve_scope_failures(
        _receipt(), completed_session_dates=["2026-09-28", "2026-09-29"],
        expected_markets={"a", "b"},
    ) == []


@pytest.mark.parametrize("path,value,reason", [
    ("session_dates", ["2026-09-28"], "session dates"),
    ("session_dates", ["2026-09-28", "2026-09-29", "2026-09-29"], "session dates"),
    ("markets", ["a"], "mode set"),
    ("markets", ["a", "b", "b"], "mode set"),
    ("generated_rows", 1079, "row count"),
    ("generated_rows", "1080", "row count"),
    ("generated_rows", True, "row count"),
    ("start_date", "2026-09-27", "start date"),
    ("end_date", "2026-09-30", "end date"),
])
def test_scope_metadata_rejects_mismatches(path: str, value, reason: str) -> None:
    receipt = _receipt()
    (receipt if path in {"start_date", "end_date"} else receipt["strategy"])[path] = value
    failures = rebuild.minute_curve_scope_failures(
        receipt, completed_session_dates=["2026-09-28", "2026-09-29"],
        expected_markets={"a", "b"},
    )
    assert any(reason in failure for failure in failures)


@pytest.mark.parametrize("sessions,markets", [
    (["2026-09-28"], None), (None, ["a"]), ([], ["a"]),
    (["2026-09-28", "2026-09-28", "2026-09-29"], ["a"]),
    (["2026-09-27", "2026-09-29"], ["a"]),
    (["2026-09-28", "2026-09-30"], ["a"]),
    (["20260928", "2026-09-29"], ["a"]),
    (["2026-09-28", "2026-09-29"], ["a", "a"]),
    (["2026-09-28", "2026-09-29"], [" "]),
])
def test_expected_scope_rejects_bad_arguments_before_source_reads(sessions, markets) -> None:
    with pytest.raises(ValueError):
        rebuild._expected_minute_scope(SimpleNamespace(
            expected_session_date=sessions, expected_market=markets,
        ), start=date(2026, 9, 28), end=date(2026, 9, 29))


def test_expected_scope_is_optional_for_existing_callers() -> None:
    assert rebuild._expected_minute_scope(
        SimpleNamespace(), start=date(2026, 9, 28), end=date(2026, 9, 29),
    ) is None


def test_wrong_scope_never_reaches_live_publication(tmp_path: Path, monkeypatch) -> None:
    live = tmp_path / "live"
    live.mkdir()
    for name, content in (("marks.jsonl", ""), ("fills.jsonl", ""),
                          ("orders.jsonl", ""), ("benchmark_history.json", '{"marks":[]}')):
        (live / name).write_text(content)
    originals = {path.name: path.read_bytes() for path in live.iterdir()}
    output = tmp_path / "output"
    monkeypatch.setattr(sys, "argv", ["rebuild", "--state-dir", str(live),
        "--start-date", "2026-09-29", "--end-date", "2026-09-29",
        "--expected-session-date", "2026-09-29", "--expected-market", "active",
        "--output-dir", str(output), "--simulation", "--publish"])

    class Store:
        kbar_roots = ()
        tick_minute_roots = ()

        def __init__(self, *args, **kwargs):
            pass

        def prepare(self, required):
            pass

        def coverage(self, required):
            return {"missing_pairs": 0}

        def missing_pairs(self, required):
            return []

        def assert_sources_unchanged(self):
            pytest.fail("wrong scope must be rejected before the publication gate")

    monkeypatch.setattr(rebuild, "MinutePriceStore", Store)
    monkeypatch.setattr(rebuild, "load_positions", lambda *a, **k: {})
    monkeypatch.setattr(rebuild, "required_symbol_dates", lambda *a, **k: {})
    monkeypatch.setattr(rebuild, "rebuild_strategy_marks", lambda *a, **k: ([], {
        "session_dates": ["2026-09-29"], "markets": ["wrong"], "generated_rows": 270,
    }))
    monkeypatch.setattr(rebuild, "rebuild_benchmark_history", lambda *a, **k: ({"marks": []}, {}))
    with pytest.raises(RuntimeError, match="scope mismatch; publication refused"):
        rebuild.main()
    assert {path.name: path.read_bytes() for path in live.iterdir()} == originals
    assert not (output / "minute_curve_receipt.json").exists()


@pytest.mark.parametrize("failed_stage", ["strategy", "price", "benchmark"])
def test_maintenance_forwards_scope_and_records_post_validation_failure(
    tmp_path: Path, monkeypatch, failed_stage: str,
) -> None:
    day = "2026-09-29"
    status = tmp_path / "status.json"
    status.write_text('{"status":"failed","error":"old attempt"}')
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=True,
    ))
    (tmp_path / "state.json").write_text('{"modes":{"active":{}}}')
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: ([day], {"active"}))
    monkeypatch.setattr(maintenance, "_missing_endpoints_with_cache", lambda *a, **k: ([], {}))
    monkeypatch.setattr(maintenance, "_tx_benchmark_source_state", lambda *a: {"ready": True})
    monkeypatch.setattr(maintenance, "_stock_calendar_source_state", lambda *a: {"ready": True})
    monkeypatch.setattr(maintenance, "historical_query_is_protected", lambda _: False)
    calls = {"strategy": 0, "price": 0, "benchmark": 0}
    marker = ValueError("current attempt evidence rejected")

    def checked(stage):
        def validate(*args, **kwargs):
            calls[stage] += 1
            if calls[stage] == 1:
                return None
            if stage == failed_stage:
                raise marker
            if stage == "price":
                return {"unverified_opening_rows": 0, "unverified_interior_rows": 0}
            return {"verified": True}
        return validate

    monkeypatch.setattr(maintenance, "_validate_current", checked("strategy"))
    monkeypatch.setattr(maintenance, "_inspect_strategy_price_provenance", checked("price"))
    monkeypatch.setattr(maintenance, "_validate_benchmarks", checked("benchmark"))

    def run(command, **kwargs):
        if command[1].endswith("rebuild_tw_day_trade_minute_curves.py"):
            assert command[command.index("--expected-session-date") + 1] == day
            assert command[command.index("--expected-market") + 1] == "active"
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(maintenance.subprocess, "run", run)
    with pytest.raises(ValueError) as caught:
        maintenance.main()
    assert caught.value is marker
    payload = json.loads(status.read_text())
    assert payload["status"] == "failed"
    assert payload["failed_stage"] == "post_publication_validation"
    assert payload["completed_session_dates"] == [day]
    assert payload["error_type"] == "ValueError"
    assert payload["error"] == str(marker)
    assert payload["stage_seconds"]["post_validation"] >= 0
