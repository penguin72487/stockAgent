from dataclasses import replace
import copy
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts.add_tw_day_trade_paper_account import (
    compose_account,
    validate_new_identity,
    verify_completed_prefix,
    validate_composed_history,
)
from scripts.promote_tw_day_trade_replay import _acquire_engine_lock
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
from test_tw_day_trade_simulation import (
    _eligibility,
    _now,
    _quote,
    _row,
    _spec,
    _summary,
)


@pytest.fixture
def accounts(tmp_path):
    for name in ("old", "new"):
        workspace = tmp_path / name
        workspace.mkdir()
        spec = replace(_spec(workspace), market=name, label=name)
        engine = TwDayTradeSimulationEngine(workspace / "ledger")
        engine.update_readiness([spec], now=_now(8, 45))
        assert (
            engine.register_signal(
                spec=spec,
                summary=_summary("signal-" + name),
                signal_rows=[_row()],
                quotes={"2330": _quote()},
                eligibility=_eligibility(),
                eligibility_coverage={},
                now=_now(9, 1, 6),
            )
            == "registered"
        )
        engine._persist(_now(9, 1, 6))
    return tmp_path / "old/ledger", tmp_path / "new/ledger", tmp_path / "merged"


def test_composition_preserves_open_old_book_and_every_old_ledger_byte(accounts):
    live, candidate, merged = accounts
    before_state = json.loads((live / "state.json").read_text())
    before_files = {
        path.relative_to(live): path.read_bytes()
        for path in live.rglob("*")
        if path.is_file()
    }
    assert before_state["modes"]["old"]["open_position_count"] > 0
    receipt = compose_account(live, candidate, merged, "new")
    after = json.loads((merged / "state.json").read_text())
    assert after["modes"]["old"] == before_state["modes"]["old"]
    assert after["modes"]["new"]["initial_capital_twd"] == 10_000_000
    assert receipt["old_modes_unchanged"] and receipt["old_ledger_prefixes_unchanged"]
    for relative, contents in before_files.items():
        assert (live / relative).read_bytes() == contents
        if relative.suffix == ".jsonl":
            assert (merged / relative).read_bytes().startswith(contents)
    # Each position is an independent object; no cross-account capital or
    # inventory pooling is introduced by account composition.
    engine = TwDayTradeSimulationEngine(merged)
    old = copy.deepcopy(engine.state["modes"]["old"])
    engine.state["modes"]["new"]["total_equity_twd"] -= 100
    new_positions = engine.state["modes"]["new"]["positions"]
    next(iter(new_positions.values()))["signed_shares"] = 0
    assert engine.state["modes"]["old"] == old


@pytest.mark.parametrize(
    "conflict", ["mode", "history", "foreign", "symlink", "existing_destination"]
)
def test_refuses_replacement_or_unsafe_merge(accounts, conflict):
    live, candidate, merged = accounts
    if conflict == "mode":
        payload = json.loads((live / "state.json").read_text())
        payload["modes"]["new"] = {}
        (live / "state.json").write_text(json.dumps(payload))
    elif conflict == "history":
        with (live / "events.jsonl").open("a") as stream:
            stream.write('{"market":"new"}\n')
    elif conflict == "foreign":
        with (candidate / "events.jsonl").open("a") as stream:
            stream.write('{"market":"not-authorized"}\n')
    elif conflict == "symlink":
        (candidate / "escape").symlink_to(live, target_is_directory=True)
    else:
        merged.mkdir()
    before = (live / "state.json").read_bytes()
    with pytest.raises(ValueError):
        compose_account(live, candidate, merged, "new")
    assert (live / "state.json").read_bytes() == before


def test_live_writer_lock_is_mandatory(accounts):
    live, candidate, _ = accounts
    handle = _acquire_engine_lock(live)
    try:
        with pytest.raises(RuntimeError, match="live writer"):
            _acquire_engine_lock(live)
        validate_new_identity(live, candidate, "new")  # read-only plan is safe
    finally:
        handle.close()


@pytest.mark.parametrize("financial_writer", [False, True])
def test_independent_query_ipc_is_not_financial_state(
    accounts, monkeypatch, financial_writer
):
    from scripts import add_tw_day_trade_paper_account as addition

    live, candidate, merged = accounts
    broker = live / "quote_broker/requests"
    broker.mkdir(parents=True)
    (broker / "first.json").write_text('{"query":1}')
    real_copy = addition.shutil.copytree

    def copy(*args, **kwargs):
        result = real_copy(*args, **kwargs)
        if Path(args[0]) == live:
            if financial_writer:
                with (live / "fills.jsonl").open("a") as handle:
                    handle.write('{"market":"old","changed":true}\n')
            else:
                (broker / "second.json").write_text('{"query":2}')
                (live / "preopen_readiness.json").write_text('{"ready":false}')
        return result

    monkeypatch.setattr(addition.shutil, "copytree", copy)
    if financial_writer:
        with pytest.raises(
            ValueError, match="existing file changed|live source changed|truncated"
        ):
            compose_account(live, candidate, merged, "new")
    else:
        result = compose_account(live, candidate, merged, "new")
        assert result["old_modes_unchanged"]
        assert not (merged / "quote_broker").exists()
        assert (broker / "first.json").exists() and (broker / "second.json").exists()


def test_accepted_history_allows_only_new_session_append(tmp_path):
    path = tmp_path / "marks.jsonl"
    original = b'{"session_date":"2026-09-09","value":1}\n'
    digest = hashlib.sha256(original).hexdigest()
    path.write_bytes(original + b'{"session_date":"2026-09-10"}\n')
    assert verify_completed_prefix(path, digest, "2026-09-09") == len(original)
    path.write_bytes(original + b'{"session_date":"2026-09-09"}\n')
    with pytest.raises(ValueError, match="historical row appended"):
        verify_completed_prefix(path, digest, "2026-09-09")
    path.write_bytes(original.replace(b'"value":1', b'"value":2'))
    with pytest.raises(ValueError, match="prefix changed"):
        verify_completed_prefix(path, digest, "2026-09-09")


def test_account_discovery_does_not_freeze_the_number_of_models(tmp_path, monkeypatch):
    from stockagent.live import market_config
    from types import SimpleNamespace

    configs = {
        name: SimpleNamespace(enabled=True, day_trade_simulation_enabled=True)
        for name in ("original", "added")
    }
    configs["not-paper"] = SimpleNamespace(
        enabled=True, day_trade_simulation_enabled=False
    )
    configs["disabled"] = SimpleNamespace(
        enabled=False, day_trade_simulation_enabled=True
    )
    monkeypatch.setattr(market_config, "load_market_configs", lambda _: configs)
    assert market_config.enabled_day_trade_markets(tmp_path) == ("added", "original")
    configs.clear()
    with pytest.raises(ValueError, match="no enabled"):
        market_config.enabled_day_trade_markets(tmp_path)


@pytest.mark.parametrize("remove_minute", [False, True])
def test_composed_history_validates_full_grid_without_rewriting_old_marks(
    accounts, remove_minute
):
    live, candidate, merged = accounts
    from scripts.promote_tw_day_trade_replay import _sha256, MINUTE_CURVE_CONTRACT

    day = "2026-08-13"
    benchmarks = []
    for benchmark, clock, count in (
        ("benchmark_0050", "09:00", 271),
        ("benchmark_2330", "09:00", 271),
        ("benchmark_tx_continuous", "08:45", 300),
    ):
        base = datetime.fromisoformat(f"{day}T{clock}:00+08:00")
        benchmarks.extend(
            {
                "benchmark_id": benchmark,
                "session_date": day,
                "minute": (base + timedelta(minutes=i)).isoformat(timespec="minutes"),
            }
            for i in range(count)
        )
    for root, market in ((live, "old"), (candidate, "new")):
        base = datetime.fromisoformat(f"{day}T09:01:00+08:00")
        rows = [
            {
                "session_date": day,
                "market": market,
                "minute": (base + timedelta(minutes=i)).isoformat(timespec="minutes"),
                "historical_minute_replay": True,
                "minute_valuation_contract": MINUTE_CURVE_CONTRACT,
                "valuation_source": "shioaji_historical_1m_close_with_last_trade_carry",
                "fresh_trade_notional_coverage_ratio": 1.0,
                "fresh_trade_position_count": 0,
                "last_trade_carried_position_count": 0,
                "missing_price_position_count": 0,
                "valuation_executable": False,
            }
            for i in range(270)
        ]
        (root / "marks.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        (root / "benchmark_history.json").write_text(json.dumps({"marks": benchmarks}))
        receipt = {
            "simulation_only": True,
            "production_order_possible": False,
            "start_date": day,
            "end_date": day,
            "minute_contract": MINUTE_CURVE_CONTRACT,
            "linear_interpolation_used": False,
            "accepted_09_01_strategy_and_13_30_endpoints_preserved": True,
            "independent_carried_valuation_parity_passed": True,
            "independent_carried_valuation_parity_required": True,
            "carried_inventory_revalued_from_unchanged_executions": True,
            "unchanged_fills_sha256": _sha256(root / "fills.jsonl"),
            "coverage_after_fetch": {"missing_pairs": 0},
            "strategy": {
                "session_dates": [day],
                "markets": [market],
                "generated_rows": 270,
                "differing_original_equity_points": 0,
                "maximum_original_equity_difference_twd": 0.0,
            },
            "outputs": {"marks": {"sha256": _sha256(root / "marks.jsonl")}},
        }
        (root / "minute_curve_receipt.json").write_text(json.dumps(receipt))
    compose_account(live, candidate, merged, "new")
    before = (merged / "marks.jsonl").read_bytes()
    if remove_minute:
        (merged / "marks.jsonl").write_bytes(before.split(b"\n", 1)[1])
        with pytest.raises(RuntimeError, match="cardinality mismatch"):
            validate_composed_history(live, candidate, merged, "new")
    else:
        result = validate_composed_history(live, candidate, merged, "new")
        assert result["minute_curves"]["validated_rows"] == 540
        assert result["minute_curves"]["unverified_historical_interior_rows"] == 0
        assert (merged / "marks.jsonl").read_bytes() == before
