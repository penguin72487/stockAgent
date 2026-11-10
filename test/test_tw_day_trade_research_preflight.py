from pathlib import Path
from dataclasses import replace
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import rebuild_tw_day_trade_open_price_replay as replay
from scripts import run_tw_day_trade_simulation as runtime


def test_research_mode_inspection_never_weakens_live_accounting_gate(tmp_path, monkeypatch):
    live = MagicMock()
    live.enabled = False
    live.day_trade_simulation_enabled = True
    live.config_path = "candidate.yaml"
    live.current_capital = None
    live.initial_capital = 2_000_000
    live.checkpoint_path = None
    live.day_trade_margin_action_data_dir = None
    live.day_trade_rule_data_dir = None
    experiment = MagicMock()
    experiment.trading.execution_mode = "tw_day_trade"
    experiment.trading.tw_day_trade_short_limit_up_penalty_rate = 0.07
    experiment.trading.tw_day_trade_lot_size = 1_000
    experiment.data.parquet_root = str(tmp_path)
    monkeypatch.setattr(runtime, "load_market_configs", lambda _root: {"2m": live})
    monkeypatch.setattr(runtime, "load_config", lambda _path: experiment)
    monkeypatch.setattr(runtime, "resolved_live_output_dir", lambda _live: tmp_path / "signals")
    monkeypatch.setattr(runtime, "resolve_day_trade_rule_data_dir", lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(runtime, "_fee_schedule", lambda _exp: None)
    monkeypatch.setattr(runtime, "ModeSpec", lambda **values: SimpleNamespace(**values))

    specs, _, errors = runtime._mode_specs(tmp_path, include_disabled=True)
    assert not specs and "unsupported_paper_accounting" in errors["2m"]
    specs, _, errors = runtime._mode_specs(tmp_path, include_disabled=True,
                                            research_accounting_preflight_only=True)
    assert not errors and specs[0].research_short_limit_up_close_penalty_rate == 0.07
    live.market = "2m"
    monkeypatch.setattr(runtime, "load_market_config", lambda _path: live)
    specs, _, errors = runtime._mode_specs(tmp_path, include_disabled=True,
        isolated_provisional_research_replay=True, preflight_market_config=tmp_path / "candidate.yaml")
    assert not errors and specs[0].research_short_limit_up_close_penalty_rate == 0.07
    live.enabled = True
    specs, _, errors = runtime._mode_specs(tmp_path, include_disabled=True,
                                            research_accounting_preflight_only=True)
    assert not specs and "unsupported_paper_accounting" in errors["2m"]
    specs, _, errors = runtime._mode_specs(tmp_path, include_disabled=True,
        isolated_provisional_research_replay=True, preflight_market_config=tmp_path / "candidate.yaml")
    assert not specs and "unsupported_paper_accounting" in errors["2m"]
    with pytest.raises(ValueError, match="requires disabled"):
        runtime._mode_specs(tmp_path, research_accounting_preflight_only=True)
    with pytest.raises(ValueError, match="limited to research inspection"):
        runtime._mode_specs(tmp_path, preflight_market_config=tmp_path / "candidate.yaml")
    with pytest.raises(ValueError, match="separate from isolated"):
        runtime._mode_specs(tmp_path, include_disabled=True, research_accounting_preflight_only=True,
                            promoted_research_state_dir=tmp_path / "live")


def test_nonzero_research_cost_requires_exact_promoted_admission(tmp_path, monkeypatch):
    from stockagent.live import day_trade_addition

    experiment = SimpleNamespace(trading=SimpleNamespace(tw_day_trade_short_limit_up_penalty_rate=0.07))
    context = {"live": tmp_path, "market": "2m", "initial_capital": 2_000_000.0,
               "checkpoint": tmp_path / "checkpoint.pt", "config": tmp_path / "candidate.yaml"}
    with pytest.raises(ValueError, match="unsupported_paper_accounting"):
        runtime._require_supported_paper_accounting(experiment)
    observed = []
    monkeypatch.setattr(day_trade_addition, "verify_promoted_research_admission",
                        lambda **kwargs: observed.append(kwargs))
    runtime._require_supported_paper_accounting(experiment, promoted_research=context)
    assert observed == [{**context, "rate": 0.07}]
    def reject(**_kwargs):
        raise ValueError("promotion receipt not accepted")
    monkeypatch.setattr(day_trade_addition, "verify_promoted_research_admission", reject)
    with pytest.raises(ValueError, match="promotion receipt"):
        runtime._require_supported_paper_accounting(experiment, promoted_research=context)


@pytest.mark.parametrize("missing", ["--preflight-only", "--local-only", "--include-disabled", "--only-market"])
def test_research_preflight_flag_cannot_enable_replay_or_network(tmp_path, monkeypatch, missing):
    flags = ["--preflight-only", "--local-only", "--include-disabled", "--only-market"]
    flags.remove(missing)
    argv = ["replay", "--state-dir", str(tmp_path / "candidate"), "--start-date", "2026-01-02",
            "--end-date", "2026-01-02", "--inspect-disabled-research-accounting",
            "--preflight-market-config", str(tmp_path / "market.yaml")]
    for flag in flags:
        argv.append(flag)
        if flag == "--only-market":
            argv.append("2m")
    monkeypatch.setattr("sys.argv", argv)
    monkeypatch.setattr(replay, "_mode_specs", lambda *_a, **_kw: pytest.fail("rejected before loading execution specs"))
    with pytest.raises(ValueError, match="research accounting inspection requires"):
        replay.main()


def test_research_preflight_refuses_receipt_outside_isolated_directory(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["replay", "--state-dir", str(tmp_path / "candidate"),
        "--start-date", "2026-01-02", "--end-date", "2026-01-02", "--inspect-disabled-research-accounting",
        "--preflight-only", "--local-only", "--include-disabled", "--only-market", "2m",
        "--preflight-market-config", str(tmp_path / "market.yaml"),
        "--preflight-output", str(tmp_path / "elsewhere.json")])
    monkeypatch.setattr(replay, "_mode_specs", lambda *_a, **_kw: pytest.fail("must reject before specs"))
    with pytest.raises(ValueError, match="receipt must stay"):
        replay.main()


def test_nontrade_flag_alone_cannot_enable_any_replay(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["replay", "--state-dir", str(tmp_path / "candidate"),
        "--start-date", "2026-01-02", "--end-date", "2026-01-02", "--provisional-nontrade-accounting"])
    monkeypatch.setattr(replay, "_mode_specs", lambda *_a, **_kw: pytest.fail("must reject before specs"))
    with pytest.raises(ValueError, match="requires the isolated provisional research replay"):
        replay.main()


def test_isolated_provisional_mode_must_be_labelled_and_cannot_be_used_live(tmp_path):
    from stockagent.live.tw_day_trade_simulation import ENTRY_FILL_POLICY_0901_MINUTE_PRICE, TwDayTradeSimulationEngine
    from test_tw_day_trade_simulation import _spec
    original = _spec(tmp_path)
    with pytest.raises(ValueError, match="one volume-capped historical decision"):
        replace(original, historical_counterfactual_provisional=True)
    spec = replace(original, entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
                   historical_counterfactual_provisional=True)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    with pytest.raises(ValueError, match="restricted to a labelled historical replay"):
        engine.register_signal(spec=spec, summary={"signal_id": "x"}, signal_rows=[], quotes={},
                               eligibility={}, eligibility_coverage={}, counterfactual_open_replay=False)


@pytest.mark.parametrize("nav", [None, 0, -1, float("nan"), float("inf")])
def test_provisional_replay_stops_on_unavailable_or_nonpositive_nav(nav):
    mode = {"provisional_minute_source_coverage": True, "total_equity_twd": nav}
    with pytest.raises(RuntimeError, match="provisional_research_"):
        replay._require_provisional_research_nav(mode, datetime.fromisoformat("2026-01-02T09:01:00+08:00"))
    # Existing non-provisional paper accounts are not changed by the gate.
    replay._require_provisional_research_nav({"total_equity_twd": nav}, datetime.now())


def test_provisional_terminal_fill_requires_a_positive_observed_1330_bar():
    day = date(2026, 1, 2)
    key = "2026-01-02T13:30+08:00"
    quotes = {symbol: {"session_close_evidence": {"price": 100}} for symbol in ("A", "B", "C", "D", "E")}
    bars = {"A": {key: {"volume_shares": 1000, "close": 100}}, "B": {key: {"volume_shares": 0, "close": 100}},
            "C": {key: {"volume_shares": None, "close": 100}},
            "D": {"2026-01-02T13:29+08:00": {"volume_shares": 1000, "close": 100}},
            "E": {key: {"volume_shares": 1000, "close": 99}}}
    assert replay._observed_terminal_quotes_only(quotes, bars, trading_date=day) == {"A": quotes["A"]}
