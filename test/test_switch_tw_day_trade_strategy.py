from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import switch_tw_day_trade_strategy as strategy_switch


@pytest.mark.parametrize("version,expected", [
    (None, False), (0, False), (True, False), ("1", False),
    (strategy_switch.TW_ORDER_PRICE_CONTRACT_VERSION - 1, False),
    (strategy_switch.TW_ORDER_PRICE_CONTRACT_VERSION, True),
])
def test_current_deployment_requires_product_aware_tick_receipt(tmp_path, version, expected):
    day = date(2026, 2, 25)
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"checkpoint_fingerprint": "exact"}))
    (tmp_path / "promotion_receipt.json").write_text(json.dumps({
        "acceptance": {"session_dates": [day.isoformat()], "minute_curve_validation": {"validated_rows": 270}},
    }))
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({
        "source_signal_ledger": {"replacement_signal_markets": ["target"]},
        "replay_contract": {"entry": strategy_switch.REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
                            "order_price_contract_version": version},
        "sessions": [{"modes": [{"market": "target", "summary_path": str(summary)}]}],
    }))
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"target": {"initial_capital_twd": 10000000}}}))
    result = strategy_switch._current_deployment_evidence(
        live_dir=tmp_path, market="target", live_output=tmp_path,
        checkpoint_fingerprint="exact", initial_capital_twd=10000000,
        start_date=day, end_date=day, session_count=1, expected_minute_rows=270,
    )
    assert result["matches"] is expected
    assert result["reasons"] == ([] if expected else ["order_price_contract_version_mismatch"])


@pytest.mark.parametrize("full,accepted", [(False, False), (True, True)])
def test_explicit_full_0901_deployment_cannot_be_mistaken_for_volume_sweep(tmp_path, full, accepted):
    day = date(2026, 2, 25)
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"checkpoint_fingerprint": "exact"}))
    (tmp_path / "promotion_receipt.json").write_text(json.dumps({
        "acceptance": {"session_dates": [str(day)], "minute_curve_validation": {"validated_rows": 270}}}))
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({
        "source_signal_ledger": {"replacement_signal_markets": ["target"]},
        "replay_contract": {"entry": strategy_switch.REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
                            "order_price_contract_version": strategy_switch.TW_ORDER_PRICE_CONTRACT_VERSION},
        "sessions": [{"modes": [{"market": "target", "summary_path": str(summary)}]}]}))
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"target": {"initial_capital_twd": 10000000}}}))
    result = strategy_switch._current_deployment_evidence(live_dir=tmp_path, market="target",
        live_output=tmp_path, checkpoint_fingerprint="exact", initial_capital_twd=10000000,
        start_date=day, end_date=day, session_count=1, expected_minute_rows=270,
        **({"required_replay_entry_contract": strategy_switch.REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL}
           if full else {}))
    assert result["matches"] is accepted


def test_addition_full_0901_flag_is_explicit_and_scoped_to_new_mode(tmp_path, monkeypatch):
    default = strategy_switch.build_parser().parse_args(["plan", "--add-mode"])
    assert strategy_switch._addition_replay_flags(default, market="new", live=tmp_path) == []
    full = strategy_switch.build_parser().parse_args(["apply", "--add-mode", "--historical-full-fill-0901"])
    assert strategy_switch._addition_replay_flags(full, market="new", live=tmp_path) == [
        "--historical-full-fill-0901", "--source-ledger-dir", str(tmp_path),
        "--allow-unpinned-market", "new"]
    monkeypatch.setattr("sys.argv", ["switch", "apply", "--historical-full-fill-0901"])
    with pytest.raises(ValueError, match="requires --add-mode"):
        strategy_switch.main()


@pytest.mark.parametrize("flag", ["--add-mode", "--candidate-preflight"])
@pytest.mark.parametrize("year, first_day", [(2026, "2026-01-02"), (2027, "2027-01-04")])
def test_new_mode_default_is_the_first_verified_session_of_the_current_year(
    monkeypatch, tmp_path, flag, year, first_day,
):
    args = strategy_switch.build_parser().parse_args(
        ["plan", flag, "--tw-public-dir", str(tmp_path)])
    calls = []
    first = date.fromisoformat(first_day)
    def calendar(**kwargs):
        calls.append(kwargs)
        return first, [first], "verified-calendar"
    monkeypatch.setattr(strategy_switch, "_latest_completed_session", calendar)
    observed = datetime(year, 11, 9, 5, 0, tzinfo=strategy_switch.TAIPEI)
    assert strategy_switch._requested_start_date(args, observed=observed) == first
    assert calls == [{"tw_public_dir": tmp_path, "start_date": date(year, 1, 1)}]


def test_explicit_and_existing_replacement_start_dates_never_query_the_default_calendar(monkeypatch):
    def unavailable(**_kwargs):
        pytest.fail("explicit or legacy replacement date must not query the default calendar")
    monkeypatch.setattr(strategy_switch, "_latest_completed_session", unavailable)
    parser = strategy_switch.build_parser()
    explicit = parser.parse_args(["plan", "--add-mode", "--start-date", "2026-06-01"])
    replacement = parser.parse_args(["plan"])
    assert strategy_switch._requested_start_date(explicit) == date(2026, 6, 1)
    assert strategy_switch._requested_start_date(replacement) == strategy_switch.DEFAULT_START_DATE


def test_current_year_default_rejects_empty_or_previous_year_calendar(monkeypatch):
    args = strategy_switch.build_parser().parse_args(["plan", "--add-mode"])
    now = datetime(2027, 1, 1, 8, 0, tzinfo=strategy_switch.TAIPEI)
    monkeypatch.setattr(strategy_switch, "_latest_completed_session",
                        lambda **_kwargs: (date(2026, 12, 31), [], "digest"))
    with pytest.raises(RuntimeError, match="no receipt-verified completed TAIEX session"):
        strategy_switch._requested_start_date(args, observed=now)


def test_requested_artifact_missing_never_uses_selected_other_model(tmp_path: Path) -> None:
    selected = tmp_path / "layernorm"
    selected.mkdir()
    config = SimpleNamespace(output_dir=str(selected), fold_id=11, checkpoint_path="unused")
    with pytest.raises(FileNotFoundError, match="requested training artifact is unavailable"):
        strategy_switch._require_requested_artifact(config, tmp_path / "attention_layernorm")


def test_2m_unit_mgda_candidate_is_exact_and_not_enabled() -> None:
    from stockagent.live.market_config import SIGNAL_UPDATE_PROGRESSIVE, load_market_config

    root = Path(__file__).resolve().parents[1]
    candidate = root / "configs/deployments/candidates/tw_day_trade_unit_mgda_2m_20261103.yaml"
    config = load_market_config(candidate)
    artifact = ("/srv/stockagent-workspace/restore/completed-return-staging/"
        "1080930381086c7312e42d065d5df049e5f8f23a77d50cd4d745136d25b1f222/"
        "ingress-staging/vastai1T/artifact-auto-training-1a385d89ef07093c/artifacts/"
        "markets/tw_day_trade_unit_mgda_capital_limitup_20261103_v1/_capacity/"
        "capital__twd_2000000/attempt-00/training")
    assert config.market == "tw_day_trade_unit_mgda_2m_20261103"
    assert config.label == "參數空間 Unit MGDA 當沖研究模擬（初始 200 萬）"
    assert config.enabled is False
    assert config.day_trade_simulation_enabled is True
    assert config.day_trade_signal_update_policy == SIGNAL_UPDATE_PROGRESSIVE
    assert config.initial_capital == 2_000_000.0
    assert config.fold_id == 10
    assert config.output_dir == artifact
    assert config.checkpoint_path == f"{artifact}/fold_10/checkpoint_best.pt"
    assert config.model_scoped_live_output is True


def test_staged_2m_mode_shares_the_existing_unit_mgda_service_contract():
    from stockagent.live.market_config import load_market_config

    root = Path(__file__).resolve().parents[1] / "services/discord_bot/markets"
    existing = load_market_config(root / "tw_day_trade_unit_mgda.yaml")
    added = load_market_config(root / "tw_day_trade_unit_mgda_2m_20261103.yaml")
    assert added.market != existing.market
    assert added.label == "台股・參數空間 Unit MGDA 當沖研究模擬（初始 200 萬）"
    assert (added.initial_capital, existing.initial_capital) == (2_000_000, 10_000_000)
    assert added.checkpoint_path != existing.checkpoint_path
    for field in (
        "market_type", "timezone", "open_time", "close_time", "schedule_time",
        "preopen_prepare_time", "summary_time", "data_ready_time",
        "day_trade_simulation_enabled", "day_trade_simulation_state_dir",
        "day_trade_opening_reference_policy", "day_trade_signal_update_policy",
        "day_trade_strict_intraday", "day_trade_terminal_liquidation_unlimited_capacity",
        "day_trade_quote_interval_seconds", "device", "top_n", "min_abs_delta",
    ):
        assert getattr(added, field) == getattr(existing, field), field


def test_requested_artifact_rejects_similar_but_different_selected_run(tmp_path: Path) -> None:
    selected = tmp_path / "layernorm"
    requested = tmp_path / "attention_layernorm"
    selected.mkdir()
    requested.mkdir()
    config = SimpleNamespace(output_dir=str(selected), fold_id=11, checkpoint_path="unused")
    with pytest.raises(ValueError, match="differs from resolved model selection"):
        strategy_switch._require_requested_artifact(config, requested)


def test_requested_artifact_checkpoint_cannot_escape_selected_root(tmp_path: Path) -> None:
    requested = tmp_path / "attention_layernorm"
    requested.mkdir()
    config = SimpleNamespace(output_dir=str(requested), fold_id=11,
                             checkpoint_path=str(tmp_path / "other/fold_11/checkpoint_best.pt"))
    with pytest.raises(ValueError, match="does not belong"):
        strategy_switch._require_requested_artifact(config, requested)


def test_requested_artifact_exact_identity_is_not_execution_parity(tmp_path: Path) -> None:
    requested = tmp_path / "attention_layernorm"
    fold = requested / "fold_11"
    fold.mkdir(parents=True)
    checkpoint = fold / "checkpoint_best.pt"
    checkpoint.write_bytes(b"exact checkpoint")
    config = SimpleNamespace(output_dir=str(requested), fold_id=11, checkpoint_path=str(checkpoint))
    alias = tmp_path / "alias"
    alias.symlink_to(requested, target_is_directory=True)
    evidence = strategy_switch._require_requested_artifact(config, alias)
    assert evidence["identity_matches"] is True
    assert evidence["training_execution_parity_proven"] is False
    assert evidence["checkpoint_sha256"] == strategy_switch._sha256(checkpoint)
    assert strategy_switch._require_requested_artifact(config, None) is None


def test_requested_artifact_gate_runs_before_calendar_or_live_state(monkeypatch, tmp_path: Path) -> None:
    args = strategy_switch.build_parser().parse_args([
        "plan", "--expected-artifact-root", str(tmp_path / "missing"),
    ])
    monkeypatch.setattr(strategy_switch, "load_market_config", lambda path: SimpleNamespace())
    monkeypatch.setattr(strategy_switch, "load_config", lambda path: pytest.fail("must not load a different experiment"))
    with pytest.raises(FileNotFoundError, match="requested training artifact is unavailable"):
        strategy_switch._build_plan(args)


def test_plan_rejects_research_cost_before_touching_live_state(monkeypatch, tmp_path: Path) -> None:
    args = strategy_switch.build_parser().parse_args(["plan", "--add-mode"])
    candidate = SimpleNamespace(config_path=str(tmp_path / "candidate.yaml"))
    experiment = SimpleNamespace(trading=SimpleNamespace(
        execution_mode="tw_day_trade",
        tw_day_trade_short_limit_up_penalty_rate=0.07,
    ))
    monkeypatch.setattr(strategy_switch, "load_market_config", lambda path: candidate)
    monkeypatch.setattr(strategy_switch, "_require_requested_artifact", lambda *_: None)
    monkeypatch.setattr(strategy_switch, "load_config", lambda path: experiment)
    monkeypatch.setattr(strategy_switch, "_latest_completed_session", lambda **_: pytest.fail("must not start history"))

    with pytest.raises(ValueError, match="unsupported_paper_accounting"):
        strategy_switch._build_plan(args)


def test_research_addition_staging_requires_exact_user_scope_and_native_capability(monkeypatch, tmp_path):
    checkpoint = tmp_path / "checkpoint.pt"
    checkpoint.write_bytes(b"exact checkpoint")
    config = SimpleNamespace(config_path=str(tmp_path / "experiment.yaml"),
        day_trade_simulation_enabled=True, enabled=False, initial_capital=2_000_000.0,
        current_capital=None, checkpoint_path=str(checkpoint))
    experiment = SimpleNamespace(trading=SimpleNamespace(
        execution_mode="tw_day_trade", tw_day_trade_short_limit_up_penalty_rate=0.07))
    monkeypatch.setattr(strategy_switch, "load_market_config", lambda _: config)
    monkeypatch.setattr(strategy_switch, "_require_requested_artifact", lambda *_: None)
    monkeypatch.setattr(strategy_switch, "load_config", lambda _: experiment)
    monkeypatch.setattr(strategy_switch, "_fold_lifecycle_evidence", lambda **_: {"fold_status": "complete"})
    monkeypatch.setattr(strategy_switch, "_native_research_accounting_capability", lambda: {"engine": "verified"})
    monkeypatch.setattr(strategy_switch, "_build_addition_plan", lambda *_: {"operation": "add_independent_mode"})
    args = strategy_switch.build_parser().parse_args(["plan", "--add-mode", "--expected-initial-capital", "2000000",
        "--prepared-addition-component", str(tmp_path / "candidate"), "--staged-signal-root", str(tmp_path / "signals"),
        "--allow-provisional-addition-history"])
    result = strategy_switch._build_plan(args)
    assert result["research_runtime_capability"] == {"engine": "verified"}
    assert result["isolated_provisional_research_signals"] is True
    args.staged_signal_root = None
    with pytest.raises(ValueError, match="unsupported_paper_accounting"):
        strategy_switch._build_plan(args)


def test_native_research_capability_requires_running_verified_engine(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from stockagent.live import service_code_release as release

    receipt = tmp_path / "release.json"
    receipt.write_text("pinned native release")
    monkeypatch.setattr(release, "configured_release", lambda *_a, **_kw: receipt)
    monkeypatch.setattr(release, "native_owner_pid", lambda *_a, **_kw: 123)
    checks = []
    monkeypatch.setattr(release, "verify_owner_namespace", lambda *_a, **_kw: checks.append(True))
    marker = "RESEARCH_ACCOUNTING_CAPABILITY=promoted_research_paper_accounting_admission_v1\n"
    monkeypatch.setattr(release, "run_in_namespace", lambda *_a, **_kw:
                        SimpleNamespace(returncode=0, stdout=marker, stderr=""))
    proof = strategy_switch._native_research_accounting_capability()
    assert proof["native_engine_pid"] == 123 and len(checks) == 2
    assert proof["native_data_or_warmup_readiness_claimed"] is False
    monkeypatch.setattr(release, "run_in_namespace", lambda *_a, **_kw:
                        SimpleNamespace(returncode=0, stdout="wrong capability\n", stderr=""))
    with pytest.raises(ValueError, match="does not yet support"):
        strategy_switch._native_research_accounting_capability()


def test_provisional_addition_verifies_signals_only_via_disabled_isolated_mode(monkeypatch, tmp_path):
    from datetime import date

    config = tmp_path / "candidate.yaml"
    summary, weights = tmp_path / "summary.json", tmp_path / "weights.parquet"
    summary.write_text("summary")
    weights.write_bytes(b"weights")
    calls = []
    monkeypatch.setattr(strategy_switch, "load_market_config", lambda _: SimpleNamespace(market="new"))
    def specs(*args, **kwargs):
        calls.append((args, kwargs))
        return [SimpleNamespace(market="new")], {}, {}
    monkeypatch.setattr(strategy_switch, "_mode_specs", specs)
    monkeypatch.setattr(strategy_switch, "_latest_valid_signal", lambda *_:
        (None, summary, weights, {"counterfactual_signal_regeneration": True,
         "checkpoint_fingerprint": "abc", "signal_id": "replay"}, []))
    plan = {"operation": "add_independent_mode", "isolated_provisional_research_signals": True,
            "replacement": {"market_config_path": str(config), "stable_market_id": "new", "checkpoint_fingerprint": "abc"}}
    result = strategy_switch._verify_generated_signals(plan, [date(2026, 1, 2)], tmp_path)
    assert result["sessions"] == 1
    assert calls[0][1] == {"include_disabled": True, "isolated_provisional_research_replay": True,
                           "preflight_market_config": config}


def test_prepared_addition_requires_exact_session_checkpoint_capital_and_signal_lineage(tmp_path, monkeypatch):
    from scripts import promote_tw_day_trade_replay as promoter
    candidate = tmp_path / "prepared"
    candidate.mkdir()
    (candidate / "state.json").write_text(json.dumps({"modes": {"new": {
        "checkpoint_fingerprint": "checkpoint", "initial_capital_twd": 2_000_000.0}}}))
    day = "2026-11-09"
    row = {"market": "new", "signal_id": "signal", "summary_sha256": "a" * 64,
           "weights_sha256": "b" * 64}
    (candidate / "rebuild_receipt.json").write_text(json.dumps({"sessions": [
        {"session_date": day, "modes": [row]}]}))
    monkeypatch.setattr(promoter, "_validate_rebuild", lambda *a, **kw: {
        "session_dates": [day], "provisional_research": {"market": "new"}})
    verified = [{"session_date": day, "signal_id": "signal", "summary_sha256": "a" * 64,
                 "weights_sha256": "b" * 64}]
    result = strategy_switch._verify_prepared_addition_component(candidate, market="new",
        checkpoint_fingerprint="checkpoint", initial_capital=2_000_000.0,
        sessions=[date.fromisoformat(day)], verified_signals=verified,
        allow_provisional_research=True)
    assert result["session_count"] == 1
    assert result["provisional_research"]["market"] == "new"
    for changes in ({"checkpoint_fingerprint": "wrong"}, {"initial_capital": 10_000_000.0},
                    {"sessions": [date(2026, 11, 6)]},
                    {"verified_signals": [{**verified[0], "weights_sha256": "c" * 64}]}):
        args = {"market": "new", "checkpoint_fingerprint": "checkpoint",
                "initial_capital": 2_000_000.0, "sessions": [date.fromisoformat(day)],
                "verified_signals": verified, "allow_provisional_research": True}
        args.update(changes)
        with pytest.raises(ValueError):
            strategy_switch._verify_prepared_addition_component(candidate, **args)


@pytest.mark.parametrize("argv, message", [
    (["apply", "--add-mode", "--prepared-addition-component", "/tmp/component"],
     "requires --add-mode and --staged-signal-root"),
    (["apply", "--allow-provisional-addition-history"], "requires --prepared-addition-component"),
    (["apply", "--add-mode", "--staged-signal-root", "/tmp/signals",
      "--prepared-addition-component", "/tmp/component", "--resume-addition-candidate", "/tmp/old"],
     "without resume"),
])
def test_prepared_addition_cli_fails_closed_for_ambiguous_scope(monkeypatch, argv, message):
    monkeypatch.setattr("sys.argv", ["switch", *argv])
    with pytest.raises(ValueError, match=message):
        strategy_switch.main()


def test_published_addition_keeps_provisional_quality_even_when_current_health_is_good():
    assert strategy_switch._has_provisional_addition_history({"new_component_acceptance": {
        "provisional_research": {"market": "new", "source_gaps_may_change_results": True}}})
    assert not strategy_switch._has_provisional_addition_history({"new_component_acceptance": {}})


def test_prepared_addition_capacity_holds_reservation_and_checks_physical_backing(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from stockagent.data_sync import cache_capacity, completed_forward_capacity
    live, component, signals, operations, controls = [tmp_path / name for name in
        ("live", "component", "signals", "operations", "controls")]
    for root in (live, component, signals, operations, controls):
        root.mkdir()
    for root, name in ((live, "fills.jsonl"), (live, "orders.jsonl"),
                       (component, "marks.jsonl"), (signals, "summary.json")):
        (root / name).write_bytes(b"source bytes")
    active, backing = [], []
    @contextmanager
    def reserve(_root, **kwargs):
        active.append(kwargs["maximum_growth_bytes"])
        yield {"other_reserved_growth_bytes": 0, "control_bytes": 128 * 1024}
        active.clear()
    monkeypatch.setattr(completed_forward_capacity, "reserve_capacity", reserve)
    monkeypatch.setattr(cache_capacity, "_control_directory", lambda _: controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume",
                        lambda path, amount: backing.append(amount) or {"state": "admitted"})
    monkeypatch.setattr(cache_capacity, "load_capacity_policy", lambda: {"minimum_free_bytes": 1})
    monkeypatch.setattr(cache_capacity, "load_acquisition_verification_reserve", lambda: 1)
    with strategy_switch._prepared_addition_capacity_lease(
        live=live, component=component, signals=signals, operations=operations,
        accepted_signals=operations / "new-signals", market="new") as lease:
        assert active and lease["maximum_growth_bytes"] == active[0]
        with lease["journal_capacity_reservation"](64):
            assert len(backing) == 2
    assert active == []


def test_prepared_addition_capacity_rejects_symlink_and_failed_physical_admission(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from stockagent.data_sync import cache_capacity, completed_forward_capacity
    live, component, signals, operations, controls = [tmp_path / name for name in
        ("live", "component", "signals", "operations", "controls")]
    for root in (live, component, signals, operations, controls):
        root.mkdir()
    (live / "redirect").symlink_to(component, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        with strategy_switch._prepared_addition_capacity_lease(
            live=live, component=component, signals=signals, operations=operations,
            accepted_signals=operations / "new-signals", market="new"):
            pass
    (live / "redirect").unlink()
    @contextmanager
    def reserve(_root, **kwargs):
        yield {"other_reserved_growth_bytes": 0, "control_bytes": 0}
    monkeypatch.setattr(completed_forward_capacity, "reserve_capacity", reserve)
    monkeypatch.setattr(cache_capacity, "_control_directory", lambda _: controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume",
                        lambda *_: (_ for _ in ()).throw(OSError("physical unavailable")))
    monkeypatch.setattr(cache_capacity, "load_capacity_policy", lambda: {"minimum_free_bytes": 1})
    monkeypatch.setattr(cache_capacity, "load_acquisition_verification_reserve", lambda: 1)
    with pytest.raises(OSError, match="physical unavailable"):
        with strategy_switch._prepared_addition_capacity_lease(
            live=live, component=component, signals=signals, operations=operations,
            accepted_signals=operations / "new-signals", market="new"):
            pass


def test_prepared_addition_holds_capacity_lease_through_apply(tmp_path, monkeypatch):
    from contextlib import contextmanager
    active = []
    @contextmanager
    def lease(**kwargs):
        active.append(True)
        yield {"lease": "held"}
        active.clear()
    def apply(_args, _plan, *, capacity_lease=None):
        assert active == [True] and capacity_lease == {"lease": "held"}
        return {"status": "candidate_only"}
    monkeypatch.setattr(strategy_switch, "_prepared_addition_capacity_lease", lease)
    monkeypatch.setattr(strategy_switch, "_apply_addition_impl", apply)
    args = SimpleNamespace(prepared_addition_component=tmp_path / "component", live_dir=tmp_path / "live",
                           staged_signal_root=tmp_path / "signals", operations_root=tmp_path / "operations")
    plan = {"replacement": {"stable_market_id": "new", "live_signal_output_dir": str(tmp_path / "accepted")}}
    assert strategy_switch._apply_addition(args, plan)["status"] == "candidate_only"
    assert active == []


def test_resumed_provisional_addition_holds_capacity_without_recloning_live(tmp_path, monkeypatch):
    from contextlib import contextmanager
    candidate = tmp_path / "candidate"
    component = candidate / "evidence/new"
    component.mkdir(parents=True)
    (candidate / "rebuild_receipt.json").write_text(json.dumps({"new_mode_component": "evidence/new"}))
    active = []
    @contextmanager
    def lease(**kwargs):
        assert kwargs["resume_candidate"] == candidate
        assert kwargs["component"] == component
        active.append(True)
        yield {"lease": "resume-held"}
        active.clear()
    def apply(_args, _plan, *, capacity_lease):
        assert active == [True] and capacity_lease == {"lease": "resume-held"}
        return {"status": "verified-not-promoted"}
    monkeypatch.setattr(strategy_switch, "_prepared_addition_capacity_lease", lease)
    monkeypatch.setattr(strategy_switch, "_apply_addition_impl", apply)
    args = SimpleNamespace(prepared_addition_component=None, resume_addition_candidate=candidate,
        allow_provisional_addition_history=True, live_dir=tmp_path / "live",
        staged_signal_root=tmp_path / "signals", operations_root=tmp_path / "operations")
    plan = {"replacement": {"stable_market_id": "new", "live_signal_output_dir": str(tmp_path / "accepted")}}
    assert strategy_switch._apply_addition(args, plan)["status"] == "verified-not-promoted"
    assert active == []


def test_disabled_preflight_keeps_accounting_blocker_without_admitting_deployment(monkeypatch, tmp_path):
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--expected-initial-capital", "2000000",
    ])
    checkpoint = tmp_path / "checkpoint.pt"
    checkpoint.write_bytes(b"exact-2m-checkpoint")
    config = SimpleNamespace(config_path=str(tmp_path / "experiment.yaml"),
        day_trade_simulation_enabled=True, enabled=False, initial_capital=2_000_000.0,
        current_capital=None, checkpoint_path=str(checkpoint))
    experiment = SimpleNamespace(trading=SimpleNamespace(
        execution_mode="tw_day_trade", tw_day_trade_short_limit_up_penalty_rate=0.07))
    monkeypatch.setattr(strategy_switch, "load_market_config", lambda _: config)
    monkeypatch.setattr(strategy_switch, "_require_requested_artifact", lambda *_: {"identity_matches": True})
    monkeypatch.setattr(strategy_switch, "load_config", lambda _: experiment)
    monkeypatch.setattr(strategy_switch, "_fold_lifecycle_evidence", lambda **_: {"fold_status": "complete"})

    def inspect(_args, **kwargs):
        assert kwargs["capital"] == 2_000_000.0
        assert kwargs["lifecycle"]["fold_status"] == "complete"
        assert kwargs["requested_artifact"]["identity_matches"] is True
        assert kwargs["accounting_blockers"][0]["code"] == "unsupported_paper_accounting"
        return {"plan_complete": False, "read_only": True, "mutations": [],
                "blockers": list(kwargs["accounting_blockers"])}

    monkeypatch.setattr(strategy_switch, "_candidate_preflight", inspect)
    result = strategy_switch._build_plan(args)
    assert result["read_only"] is True and result["plan_complete"] is False
    assert result["mutations"] == []
    args.candidate_preflight = False
    args.add_mode = True
    with pytest.raises(ValueError, match="unsupported_paper_accounting"):
        strategy_switch._build_plan(args)


def test_selected_config_cannot_erase_the_artifact_research_cost() -> None:
    penalty = {"contract": "official_limit_up_close_source_residual_short_expense_v1",
        "assessment": "after_source_capacity_before_synthetic_terminal_liquidation",
        "base": "uncovered_short_shares_times_official_close", "rate": 0.07,
        "research_only": True}
    artifact = {"mode_details": {"short_limit_up_close_penalty": penalty}}
    config = lambda rate: SimpleNamespace(trading=SimpleNamespace(
        tw_day_trade_short_limit_up_penalty_rate=rate))
    strategy_switch._require_research_cost_identity(artifact, config(0.07))
    strategy_switch._require_research_cost_identity({}, config(0.0))
    with pytest.raises(ValueError, match="differs between artifact"):
        strategy_switch._require_research_cost_identity(artifact, config(0.0))
    with pytest.raises(ValueError, match="absent from the selected artifact"):
        strategy_switch._require_research_cost_identity({}, config(0.07))
    with pytest.raises(ValueError, match="invalid research close penalty"):
        strategy_switch._require_research_cost_identity(
            {"mode_details": {"short_limit_up_close_penalty": penalty | {"research_only": False}}},
            config(0.07))


def test_flat_live_state_refuses_an_open_position(tmp_path: Path) -> None:
    state = {
        "modes": {
            "target": {
                "open_position_count": 1,
                "positions": {"2330": {"signed_shares": 1000}},
            }
        }
    }
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(RuntimeError, match="positions are open"):
        strategy_switch._flat_live_state(tmp_path, {"target"})


@pytest.mark.parametrize(("field", "value"), [
    ("ledger_state_divergence", {"kind": "partial"}),
    ("entry_retry_pending_commit", {"contract": "pending"}),
    ("research_terminal_pending_commit", {"contract": "pending"}),
    ("pending_signal_id", "not-yet-committed"),
    ("pending_entry_shares", 1000),
    ("pending_reduction_shares", 1000),
    ("pending_entry_source_count", 1),
    ("unresolved_exit_count", 1),
    ("pending_entry_orders", {"2330": {"status": "waiting_source"}}),
    ("pending_reduction_orders", {"2330": {"status": "working"}}),
    ("pending_entry_orders", {"2330": {"status": "unrecognized"}}),
    ("pending_entry_orders", {"2330": {"status": "cancelled_unknown"}}),
])
def test_flat_live_state_cannot_hide_unresolved_paper_work(tmp_path, field, value):
    state = {"modes": {"target": {"open_position_count": 0, "positions": {}, field: value}}}
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(RuntimeError, match="unresolved paper activity"):
        strategy_switch._flat_live_state(tmp_path, {"target"})


def test_flat_live_state_rejects_unproven_terminal_order_labels(tmp_path):
    state = {"modes": {"target": {"open_position_count": 0, "positions": {},
        "pending_entry_orders": {"a": {"status": "cancelled_exit_priority"},
                                 "b": {"status": "inventory_target_retained"}},
        "pending_reduction_orders": {"c": {"status": "filled"}}}}}
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(RuntimeError, match="unproven terminal paper orders"):
        strategy_switch._flat_live_state(tmp_path, {"target"})


@pytest.mark.parametrize("damage", [None, "missing_event", "wrong_quantity", "future_settlement", "wrong_market"])
def test_flat_live_state_checks_evidenced_same_day_terminal_orders(tmp_path, monkeypatch, damage):
    from test_tw_day_trade_flat_opening_repair import _settled_terminal_orders
    mode, event = _settled_terminal_orders()
    mode["engine_status"] = "session_flat_after_exit"
    if damage == "wrong_quantity": event["cancelled"][1]["shares"] += 1000
    if damage == "future_settlement": mode["closing_auction_settled_at"] = "2026-10-07T14:01:00+08:00"
    if damage == "wrong_market": mode["market"] = "other"
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"m": mode}}), encoding="utf-8")
    (tmp_path / "events.jsonl").write_text("" if damage == "missing_event" else json.dumps(event) + "\n")
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 7, 14, tzinfo=tz)
    monkeypatch.setattr(strategy_switch, "datetime", FixedDatetime)
    if damage:
        with pytest.raises(RuntimeError, match="unproven terminal paper orders"):
            strategy_switch._flat_live_state(tmp_path, {"m"})
    else:
        assert strategy_switch._flat_live_state(tmp_path, {"m"})["flat_markets"] == ["m"]


@pytest.mark.parametrize(("field", "value", "message"), [
    ("open_position_count", 0.5, "invalid open position count"),
    ("open_position_count", -1, "invalid open position count"),
    ("open_position_count", True, "invalid open position count"),
    ("positions", {"2330": {"signed_shares": 0.5}}, "invalid signed shares"),
    ("positions", {"2330": {"signed_shares": "0"}}, "invalid signed shares"),
    ("positions", {"2330": {"signed_shares": False}}, "invalid signed shares"),
    ("positions", {"2330": {}}, "invalid signed shares"),
])
def test_flat_live_state_rejects_malformed_quantities(tmp_path, field, value, message):
    state = {"modes": {"target": {"open_position_count": 0, "positions": {}, field: value}}}
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(RuntimeError, match=message):
        strategy_switch._flat_live_state(tmp_path, {"target"})


def test_formal_addition_rejects_config_outside_active_market_directory(tmp_path):
    markets = tmp_path / "markets"
    markets.mkdir()
    args = SimpleNamespace(markets_dir=markets)
    with pytest.raises(ValueError, match="staged inside the active markets directory"):
        strategy_switch._build_addition_plan(args, SimpleNamespace(market="2m"),
            tmp_path / "outside.yaml", tmp_path / "experiment.yaml", None, None, {}, 2_000_000)


def test_candidate_preflight_is_not_an_applicable_plan(monkeypatch) -> None:
    args = strategy_switch.build_parser().parse_args(["plan", "--candidate-preflight"])
    monkeypatch.setattr(strategy_switch.tempfile, "mkdtemp", lambda **_kwargs: pytest.fail("must not create a candidate directory"))
    with pytest.raises(RuntimeError, match="read-only candidate preflight cannot be applied"):
        strategy_switch._apply(args, {"plan_complete": False})
    args.candidate_preflight = False
    with pytest.raises(RuntimeError, match="read-only candidate preflight cannot be applied"):
        strategy_switch._apply(args, {"plan_complete": False})


def test_retained_endpoint_preflight_reports_exact_missing_pairs(tmp_path):
    from datetime import date

    day = date(2026, 10, 30)
    rows = [
        {"session_date": str(day), "market": "a", "minute": f"{day}T09:01+08:00"},
        {"session_date": str(day), "market": "a", "minute": f"{day}T13:30+08:00"},
        {"session_date": str(day), "market": "b", "minute": f"{day}T09:01+08:00"},
    ]
    (tmp_path / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = strategy_switch._retained_minute_endpoint_preflight(
        tmp_path, sessions=[day], markets={"a", "b"})
    assert result["status"] == "blocked"
    assert result["missing_session_markets"] == [
        {"session_date": str(day), "market": "b", "missing_clocks": ["13:30"]}]
    assert result["full_minute_grid_or_source_validation_claimed"] is False
    with pytest.raises(RuntimeError, match="minute endpoints are missing"):
        strategy_switch._apply(SimpleNamespace(candidate_preflight=False),
                               {"status": "blocked_retained_minute_endpoints"})


def test_retained_endpoint_preflight_is_not_full_minute_acceptance(tmp_path):
    from datetime import date

    day = date(2026, 10, 30)
    rows = [{"session_date": str(day), "market": "a", "minute": f"{day}T{clock}+08:00"}
            for clock in ("09:01", "13:30")]
    (tmp_path / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = strategy_switch._retained_minute_endpoint_preflight(tmp_path, sessions=[day], markets={"a"})
    assert result["status"] == "passed_necessary_condition_only"
    assert result["missing_session_markets"] == []
    assert result["full_minute_grid_or_source_validation_claimed"] is False


def test_retained_endpoint_preflight_rejects_changed_journal_generation(tmp_path, monkeypatch):
    from datetime import date
    from stockagent.live import paper_journal

    day = date(2026, 10, 30)
    (tmp_path / "marks.jsonl").write_text("")
    generations = iter([{"generation": 1}, {"generation": 2}])
    monkeypatch.setattr(paper_journal, "layout_generations", lambda *_a, **_k: next(generations))
    with pytest.raises(RuntimeError, match="changed during endpoint preflight"):
        strategy_switch._retained_minute_endpoint_preflight(tmp_path, sessions=[day], markets={"a"})


@pytest.mark.parametrize("physical_binding", [False, True])
def test_candidate_preflight_retains_modes_and_reports_blockers(monkeypatch, tmp_path, physical_binding):
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--live-dir", str(tmp_path),
        "--markets-dir", str(tmp_path / "markets"),
    ])
    existing = {"mode1", "mode2", "mode3", "mode4"}
    state = {"modes": {market: {"open_position_count": 1, "positions": {"2330": {"signed_shares": 1000}}} for market in existing}}
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state))
    before = state_path.read_bytes()
    monkeypatch.setattr(strategy_switch, "_active_specs", lambda _path: ([], {}, sorted(existing)))
    config_path = tmp_path / "experiment.yaml"
    config_path.write_text("source-config")
    checkpoint = tmp_path / "checkpoint.pt"
    checkpoint.write_bytes(b"selected-checkpoint")
    monkeypatch.setattr(strategy_switch, "_market_config_paths", lambda _path: {market: config_path for market in existing})
    start, end = date(2026, 2, 25), date(2026, 10, 12)
    monkeypatch.setattr(strategy_switch, "_latest_completed_session", lambda **_kwargs: (end, [start, end], "calendar-proof"))
    monkeypatch.setattr(strategy_switch, "resolved_live_output_dir", lambda _config: tmp_path / "signals/new-model")
    market_config = SimpleNamespace(
        market="fifth", enabled=False, label="new mode", checkpoint_path=str(checkpoint), fold_id=11,
    )
    experiment = SimpleNamespace(data=SimpleNamespace(factorized_feature_manifest=str(tmp_path / "missing.json")))
    if physical_binding:
        physical = tmp_path / "local-physical.parquet"
        physical.write_bytes(b"metadata-only-no-source-acceptance")
        experiment.data.day_trade_inference_public_feature_path = str(physical)
        experiment.data.day_trade_physical_public_feature_path = str(tmp_path / "training-host/missing.parquet")
        experiment.trading = SimpleNamespace(execution_mode="tw_day_trade",
            tw_day_trade_unlimited_margin_conversion=True)
    result = strategy_switch._candidate_preflight(
        args, market_config=market_config, market_config_path=tmp_path / "market.yaml",
        experiment_path=config_path, experiment=experiment,
        requested_artifact={"identity_matches": True}, lifecycle={"fold_status": "complete"}, capital=10000000,
        accounting_blockers=({"code": "unsupported_paper_accounting", "detail": "unverified close expense"},),
    )
    assert result["status"] == "blocked_preflight"
    assert result["read_only"] is True
    assert result["plan_complete"] is False
    assert set(result["active_markets"]) == existing
    assert set(result["proposed_markets"]) == existing | {"fifth"}
    assert result["mutations"] == []
    assert state_path.read_bytes() == before
    assert result["history"]["expected_strategy_minute_rows_after_addition"] == 2 * 5 * 270
    codes = {row["code"] for row in result["blockers"]}
    assert {"live_flat_gate_not_ready", "factorized_manifest_missing", "additive_cutover_not_verified",
            "unsupported_paper_accounting"} <= codes
    assert result["paper_accounting"]["status"] == "unsupported"
    assert result["paper_accounting"]["deployment_admitted"] is False
    if physical_binding:
        assert any(row["field"] == "day_trade_inference_public_feature_path" and row["exists"]
                   for row in result["source_dependencies"])
        assert all(row["field"] != "day_trade_physical_public_feature_path"
                   for row in result["source_dependencies"])
        assert all("training-host" not in row["detail"] for row in result["blockers"])
        assert experiment.data.day_trade_physical_public_feature_path.endswith("training-host/missing.parquet")


@pytest.mark.parametrize("end_date", ["latest", "2026-11-09"])
def test_current_session_supports_explicit_same_day_end_without_fabricating_data(monkeypatch, tmp_path, end_date):
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--start-date", "2026-11-09", "--end-date", end_date,
        "--tw-public-dir", str(tmp_path),
    ])
    monkeypatch.setattr(strategy_switch, "tw_stock_day_decision", lambda *a, **k: SimpleNamespace(
        status="scheduled_open", reason="official calendar"))
    monkeypatch.setattr(strategy_switch, "_latest_completed_session", lambda **kw: (
        date(2026, 11, 6), [date(2026, 11, 6)], "calendar"))
    history, forward = strategy_switch._candidate_history_scope(
        args, market_config=SimpleNamespace(holidays=()), active_count=6,
        observed=datetime(2026, 11, 9, 8, 59, tzinfo=strategy_switch.TAIPEI),
    )
    assert forward is True
    assert history["end_date"] is None
    assert history["session_count"] == 0
    assert history["current_session_data_complete"] is False


@pytest.mark.parametrize("changes, expected", [
    ({}, set()),
    ({"decision_session": "2026-11-06"}, {"factorized_decision_session_mismatch"}),
    ({"completed_session": "2026-11-05"}, {"factorized_completed_session_mismatch"}),
    ({"decision_session": None, "completed_session": None}, {
        "factorized_decision_session_mismatch", "factorized_completed_session_mismatch"}),
])
def test_forward_candidate_checks_projection_clocks_not_only_manifest_ready(changes, expected):
    features = {"required": True, "ready": True, "member_hashes_verified": True,
        "checkpoint_input_parity_proven": True,
        "paper_inference": {"decision_session": "2026-11-09", "completed_session": "2026-11-06"} | changes}
    history = {"current_session_date": "2026-11-09", "latest_completed_session": "2026-11-06"}
    assert {row["code"] for row in strategy_switch._forward_candidate_feature_blockers(features, history)} == expected


def test_forward_candidate_exposes_metadata_only_and_remains_non_applicable(monkeypatch, tmp_path):
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--live-dir", str(tmp_path), "--start-date", "2026-11-09",
    ])
    config_path, checkpoint = tmp_path / "experiment.yaml", tmp_path / "checkpoint.pt"
    config_path.write_text("test")
    checkpoint.write_bytes(b"selected-checkpoint")
    monkeypatch.setattr(strategy_switch, "_active_specs", lambda _: ([], {}, ["existing"]))
    monkeypatch.setattr(strategy_switch, "_market_config_paths", lambda _: {})
    monkeypatch.setattr(strategy_switch, "_flat_live_state", lambda *_: {"flat_markets": ["existing"]})
    monkeypatch.setattr(strategy_switch, "resolved_live_output_dir", lambda _: tmp_path / "signals")
    monkeypatch.setattr(strategy_switch, "_candidate_history_scope", lambda *a, **k: ({
        "scope": "current_session_forward_only", "current_session_date": "2026-11-09",
        "latest_completed_session": "2026-11-06", "session_count": 0}, True))
    monkeypatch.setattr(strategy_switch, "inspect_live_feature_source", lambda _: {
        "required": True, "ready": True, "blockers": [], "member_hashes_verified": False,
        "checkpoint_input_parity_proven": False, "paper_inference": {
            "decision_session": "2026-11-09", "completed_session": "2026-11-06"}})
    config = SimpleNamespace(enabled=False, market="new", label="candidate", fold_id=10,
        checkpoint_path=str(checkpoint))
    result = strategy_switch._candidate_preflight(args, market_config=config,
        market_config_path=tmp_path / "market.yaml", experiment_path=config_path,
        experiment=SimpleNamespace(data=SimpleNamespace()), requested_artifact={}, lifecycle={}, capital=2_000_000)
    codes = {row["code"] for row in result["blockers"]}
    assert {"factorized_members_not_verified", "checkpoint_input_parity_not_verified",
            "forward_only_addition_not_admitted"} <= codes
    assert result["read_only"] and not result["plan_complete"] and result["mutations"] == []
    with pytest.raises(RuntimeError, match="read-only candidate preflight cannot be applied"):
        strategy_switch._apply(args, result)


@pytest.mark.parametrize("hour,minute,phase", [
    (8, 59, "scheduled_not_started"),
    (9, 0, "in_progress"),
    (13, 29, "in_progress"),
    (13, 30, "waiting_completed_receipt"),
    (14, 0, "waiting_completed_receipt"),
])
def test_current_session_candidate_is_pending_not_a_completed_replay(monkeypatch, tmp_path, hour, minute, phase):
    day = date(2026, 11, 9)
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--start-date", str(day), "--end-date", "latest",
        "--tw-public-dir", str(tmp_path),
    ])
    calls = []

    def completed(**kwargs):
        calls.append(kwargs)
        return date(2026, 11, 6), [date(2026, 11, 6)], "verified-prior-calendar"

    monkeypatch.setattr(strategy_switch, "_latest_completed_session", completed)
    monkeypatch.setattr(strategy_switch, "tw_stock_day_decision", lambda *a, **k: SimpleNamespace(
        status="scheduled_open", reason="verified official schedule"))
    result, forward = strategy_switch._candidate_history_scope(
        args, market_config=SimpleNamespace(holidays=()), active_count=6,
        observed=datetime(2026, 11, 9, hour, minute, tzinfo=strategy_switch.TAIPEI),
    )
    assert forward is True
    assert result["scope"] == "current_session_forward_only"
    assert result["start_date"] == "2026-11-09"
    assert result["end_date"] is None
    assert result["latest_completed_session"] == "2026-11-06"
    assert result["session_count"] == 0
    assert result["current_session_phase"] == phase
    assert result["current_session_data_complete"] is False
    assert result["expected_strategy_minute_rows_after_addition"] == 0
    assert result["expected_rows_after_current_session_completes"] == 7 * 270
    assert calls[0]["start_date"] < day


@pytest.mark.parametrize("status", ["closed", "unknown"])
def test_current_session_candidate_rejects_unverified_or_closed_calendar(monkeypatch, tmp_path, status):
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--start-date", "2026-11-09", "--tw-public-dir", str(tmp_path),
    ])
    monkeypatch.setattr(strategy_switch, "tw_stock_day_decision", lambda *a, **k: SimpleNamespace(
        status=status, reason="closed or unavailable"))
    monkeypatch.setattr(strategy_switch, "_latest_completed_session",
                        lambda **kw: pytest.fail("unverified calendar must not be admitted"))
    with pytest.raises(ValueError, match="not confirmed open"):
        strategy_switch._candidate_history_scope(
            args, market_config=SimpleNamespace(holidays=()), active_count=6,
            observed=datetime(2026, 11, 9, 8, 0, tzinfo=strategy_switch.TAIPEI),
        )


def test_current_session_candidate_becomes_completed_only_on_actual_receipt(monkeypatch, tmp_path):
    today, previous = date(2026, 11, 9), date(2026, 11, 6)
    args = strategy_switch.build_parser().parse_args([
        "plan", "--candidate-preflight", "--start-date", str(today), "--tw-public-dir", str(tmp_path),
    ])
    monkeypatch.setattr(strategy_switch, "tw_stock_day_decision", lambda *a, **k: SimpleNamespace(
        status="actual_open", reason="receipt-verified actual session"))
    monkeypatch.setattr(strategy_switch, "_latest_completed_session",
                        lambda **kw: (today, [previous, today], "completed-calendar"))
    result, forward = strategy_switch._candidate_history_scope(
        args, market_config=SimpleNamespace(holidays=()), active_count=6,
        observed=datetime(2026, 11, 9, 15, 0, tzinfo=strategy_switch.TAIPEI),
    )
    assert forward is False
    assert result["start_date"] == result["end_date"] == "2026-11-09"
    assert result["session_count"] == 1
    assert result["expected_strategy_minute_rows_after_addition"] == 7 * 270


def test_candidate_preflight_requires_a_disabled_new_market(monkeypatch, tmp_path):
    args = strategy_switch.build_parser().parse_args(["plan", "--candidate-preflight"])
    monkeypatch.setattr(strategy_switch, "_active_specs", lambda _path: ([], {}, ["existing"]))
    for config in (SimpleNamespace(enabled=True, market="new"), SimpleNamespace(enabled=False, market="existing")):
        with pytest.raises(ValueError, match="disabled independent market"):
            strategy_switch._candidate_preflight(
                args, market_config=config, market_config_path=tmp_path / "market",
                experiment_path=tmp_path / "config", experiment=SimpleNamespace(),
                requested_artifact=None, lifecycle={}, capital=10000000,
            )


def test_fold_lifecycle_requires_complete_matching_tw_day_trade_contract(
    tmp_path: Path,
) -> None:
    output = tmp_path / "layernorm"
    fold = output / "fold_11"
    fold.mkdir(parents=True)
    checkpoint = fold / "checkpoint_best.pt"
    weights = fold / "daily_weights.parquet"
    selection = tmp_path / "selection.yaml"
    checkpoint.write_bytes(b"checkpoint")
    weights.write_bytes(b"weights")
    selection.write_text("mode: manual\n", encoding="utf-8")
    (fold / "fold_complete.json").write_text(
        json.dumps(
            {
                "status": "complete",
                "fold_id": 11,
                "checkpoint_path": checkpoint.name,
                "test_date_start": "2026-01-02",
                "test_date_end": "2026-08-19",
            }
        ),
        encoding="utf-8",
    )
    (fold / "mode_artifact_contract.json").write_text(
        json.dumps(
            {
                "execution_mode": "tw_day_trade",
                "decision_clock": "observed_session_open",
                "execution_clock": "right_labelled_minute",
            }
        ),
        encoding="utf-8",
    )
    config = SimpleNamespace(
        fold_id=11,
        output_dir=str(output),
        weights_path=str(weights),
        model_selection_path=str(selection),
    )

    evidence = strategy_switch._fold_lifecycle_evidence(
        market_config=config,
        market_config_path=tmp_path / "market.yaml",
        checkpoint=checkpoint,
    )

    assert evidence["fold_status"] == "complete"
    assert evidence["execution_mode"] == "tw_day_trade"
    assert len(evidence["weights_sha256"]) == 64


def test_dashboard_acceptance_keeps_data_gaps_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_markets = {"target", "control"}

    def fake_get(url: str) -> dict[str, object]:
        if "/api/signals?" in url:
            return {"total": 1}
        if url.endswith("/api/status"):
            return {
                "health": "degraded",
                "operational_issues": [{"code": "entry_no_fill"}],
                "service_sync": {
                    "synchronized": True,
                    "revision_lag": 0,
                    "enabled_markets": sorted(expected_markets),
                },
                "ledger_integrity": {"ready": True, "divergence_count": 0},
            }
        return {"history": []}

    monkeypatch.setattr(strategy_switch, "_json_get", fake_get)
    monkeypatch.setattr(
        strategy_switch,
        "_visible_history_dates",
        lambda _payload, _market: ["2026-02-25", "2026-09-04"],
    )

    result = strategy_switch._verify_dashboard(
        market="target",
        start_date="2026-02-25",
        end_date="2026-09-04",
        expected_markets=expected_markets,
    )

    assert result["local_status"]["health"] == "degraded"
    assert result["local_status"]["operational_issues"] == [{"code": "entry_no_fill"}]


@pytest.fixture
def prepared_history(tmp_path):
    live, candidate = tmp_path / "live", tmp_path / "candidate"
    live.mkdir()
    candidate.mkdir()
    prefix = tmp_path / "prefix"
    prefix.mkdir()
    checkpoint = tmp_path / "checkpoint.pt"
    checkpoint.write_bytes(b"selected checkpoint")
    config = SimpleNamespace(checkpoint_path=str(checkpoint), current_capital=10000000,
                             initial_capital=10000000)
    days = [date(2026, 1, 2), date(2026, 2, 25)]
    events = live / "events.jsonl"
    events.write_text(json.dumps({"event": "signal_registered", "market": "target",
        "signal_id": "original", "recorded_at": "2026-02-25T09:00:00+08:00"}) + "\n")
    for root in (live, candidate):
        (root / "state.json").write_text(json.dumps({"modes": {
            "target": {"initial_capital_twd": 10000000, "positions": {}, "open_position_count": 0}}}))
    sessions = []
    for day, signal_id, folder in ((days[0], "counterfactual", prefix / "target"),
                                   (days[1], "original", tmp_path / "retained")):
        folder.mkdir()
        summary, weights = folder / "summary.json", folder / "weights.parquet"
        summary.write_text(json.dumps({"signal_id": signal_id,
            "counterfactual_signal_regeneration": signal_id == "counterfactual",
            "simulation_only": True, "production_order_possible": False,
            "checkpoint_fingerprint": strategy_switch.short_file_fingerprint(checkpoint),
            "replay_effective_signal_at": f"{day}T09:00:00+08:00"}))
        weights.write_bytes(b"immutable weights")
        sessions.append({"session_date": str(day), "modes": [{"market": "target", "signal_id": signal_id,
            "summary_path": str(summary), "summary_sha256": strategy_switch._sha256(summary),
            "weights_path": str(weights), "weights_sha256": strategy_switch._sha256(weights)}]})
    receipt = {"simulation_only": True, "production_order_possible": False,
               "missing_signal_root": str(prefix), "sessions": sessions,
               "source_signal_ledger": {"path": str(events), "sha256": strategy_switch._sha256(events),
                                        "replacement_signal_markets": [], "replaced_pinned_signal_keys": 0}}
    (candidate / "rebuild_receipt.json").write_text(json.dumps(receipt))
    return SimpleNamespace(live=live, candidate=candidate, receipt=receipt, days=days,
                           configs={"target": config}, prefix=prefix)


def _prepared_evidence(fixture):
    return strategy_switch._prepared_history_evidence(
        fixture.candidate, fixture.live, sessions=fixture.days, configs=fixture.configs, manifests=())


def test_history_extension_retains_existing_decisions_and_marks_only_missing_prefix(prepared_history):
    fixture = prepared_history
    before = (fixture.live / "events.jsonl").read_bytes()
    result = _prepared_evidence(fixture)
    assert result["retained_decision_count"] == 1
    assert result["counterfactual_prefix_count"] == 1
    assert result["prefix_last_session"] == "2026-01-02"
    assert len(result["signal_artifacts"]) == 2
    assert (fixture.live / "events.jsonl").read_bytes() == before
    strategy_switch._require_history_source_unchanged(fixture.live, result["source_streams"])
    with (fixture.live / "events.jsonl").open("a") as stream:
        stream.write("another decision\n")
    with pytest.raises(RuntimeError, match="execution changed"):
        strategy_switch._require_history_source_unchanged(fixture.live, result["source_streams"])


@pytest.mark.parametrize("defect,message", [
    ("replacement", "cannot replace"), ("source_drift", "current live decision"),
    ("signal_id", "changed a retained signal"), ("hash", "artifact changed"),
    ("not_counterfactual", "selected marked counterfactual"),
    ("wrong_checkpoint", "selected marked counterfactual"),
    ("missing_mode", "cover every official session"), ("duplicate", "invalid or duplicate"),
    ("production_order", "simulation-only"), ("capital", "changed initial capital"),
])
def test_history_extension_refuses_unverified_or_replaced_history(prepared_history, defect, message):
    fixture = prepared_history
    row = fixture.receipt["sessions"][0]["modes"][0]
    if defect == "replacement":
        fixture.receipt["source_signal_ledger"]["replacement_signal_markets"] = ["target"]
    elif defect == "source_drift":
        fixture.receipt["source_signal_ledger"]["sha256"] = "wrong"
    elif defect == "signal_id":
        fixture.receipt["sessions"][1]["modes"][0]["signal_id"] = "changed"
    elif defect == "hash":
        row["weights_sha256"] = "wrong"
    elif defect in {"not_counterfactual", "wrong_checkpoint"}:
        path = Path(row["summary_path"])
        summary = json.loads(path.read_text())
        summary["counterfactual_signal_regeneration" if defect == "not_counterfactual" else "checkpoint_fingerprint"] = False
        path.write_text(json.dumps(summary))
        row["summary_sha256"] = strategy_switch._sha256(path)
    elif defect == "missing_mode":
        fixture.receipt["sessions"].pop()
    elif defect == "duplicate":
        fixture.receipt["sessions"].append(fixture.receipt["sessions"][0])
    elif defect == "production_order":
        fixture.receipt["production_order_possible"] = True
    elif defect == "capital":
        path = fixture.candidate / "state.json"
        state = json.loads(path.read_text())
        state["modes"]["target"]["initial_capital_twd"] = 1
        path.write_text(json.dumps(state))
    (fixture.candidate / "rebuild_receipt.json").write_text(json.dumps(fixture.receipt))
    with pytest.raises(ValueError, match=message):
        _prepared_evidence(fixture)


def test_history_extension_reuses_candidate_without_regenerating_retained_signals(monkeypatch, prepared_history, tmp_path):
    fixture = prepared_history
    proof = _prepared_evidence(fixture)
    stages = []
    monkeypatch.setattr(strategy_switch, "_run", lambda *_a, **_k: pytest.fail("must not regenerate any decisions"))
    result, repairs = strategy_switch._prepare_replacement_candidate(
        SimpleNamespace(), {"operation": "extend_history", "prepared_history": proof},
        candidate=fixture.candidate, live_dir=fixture.live, sessions=fixture.days,
        run_dir=tmp_path, stages=stages)
    assert result["replacement_signal_markets"] == [] and repairs == []
    assert stages[0]["stage"] == "retain_decisions_and_accept_prepared_history"


@pytest.mark.parametrize("clock", ["2000-01-01T00:00:00+08:00", "2999-01-01T00:00:00+08:00", "invalid", "2026-01-01T00:00:00"])
def test_engine_sync_never_accepts_a_replay_or_stale_heartbeat(monkeypatch, tmp_path, clock):
    from datetime import datetime
    calls = []
    def read(_path):
        calls.append(_path)
        return {"enabled_markets": ["target"], "mode_count": 1, "ledger_integrity_ready": True,
                "heartbeat_at": clock if len(calls) == 1 else datetime.now(strategy_switch.TAIPEI).isoformat()}
    monkeypatch.setattr(strategy_switch, "_object", read)
    monkeypatch.setattr(strategy_switch.wall_time, "sleep", lambda _seconds: None)
    result = strategy_switch._wait_engine_sync(tmp_path, {"target"}, timeout_seconds=1)
    assert len(calls) == 2 and result["heartbeat_at"] != clock


def test_discord_sync_never_accepts_a_stale_ready_receipt(monkeypatch):
    from datetime import datetime
    calls = []
    def read(_path):
        calls.append(_path)
        return {"updated_at": "2000-01-01T00:00:00+08:00" if len(calls) == 1 else datetime.now(strategy_switch.TAIPEI).isoformat(),
                "run_id": "new-owner", "discord_connected": True, "core_health": "ready",
                "scheduled_day_trade_markets": ["target"], "engine_run_id": "current-engine",
                "startup_inference_warmup": {"status": "ready", "ready_count": 1, "run_id": "new-owner",
                    "markets": [{"market": "target", "checkpoint_fingerprint": "exact", "status": "ready"}]}}
    monkeypatch.setattr(strategy_switch, "_object", read)
    monkeypatch.setattr(strategy_switch.wall_time, "sleep", lambda _seconds: None)
    result = strategy_switch._wait_discord_ready(expected_engine_run_id="current-engine", expected_markets={"target"},
        target_market="target", checkpoint_fingerprint="exact", previous_run_id="old-owner", timeout_seconds=1)
    assert len(calls) == 2 and result["run_id"] == "new-owner"


@pytest.mark.parametrize("defect", ["old_warm_owner", "missing_warm_owner", "failed_mode",
                                  "extra_mode", "missing_mode", "duplicate_mode", "wrong_checkpoint",
                                  "wrong_engine", "disconnected", "old_scheduled_modes"])
def test_discord_sync_requires_complete_current_owner_proof(monkeypatch, defect):
    from copy import deepcopy
    from datetime import datetime
    valid = {"updated_at": datetime.now(strategy_switch.TAIPEI).isoformat(),
             "run_id": "new-owner", "discord_connected": True, "core_health": "ready",
             "scheduled_day_trade_markets": ["target"], "engine_run_id": "current-engine",
             "startup_inference_warmup": {"status": "ready", "ready_count": 1, "run_id": "new-owner",
                 "markets": [{"market": "target", "checkpoint_fingerprint": "exact", "status": "ready"}]}}
    broken = deepcopy(valid)
    warm = broken["startup_inference_warmup"]
    if defect == "old_warm_owner":
        warm["run_id"] = "old-owner"
    elif defect == "missing_warm_owner":
        warm.pop("run_id")
    elif defect == "failed_mode":
        warm["markets"][0]["status"] = "failed"
    elif defect == "extra_mode":
        warm["markets"].append({"market": "other", "status": "ready"})
    elif defect == "missing_mode":
        warm["markets"] = []
    elif defect == "duplicate_mode":
        warm["markets"] *= 2
    elif defect == "wrong_checkpoint":
        warm["markets"][0]["checkpoint_fingerprint"] = "other"
    elif defect == "wrong_engine":
        broken["engine_run_id"] = "old-engine"
    elif defect == "disconnected":
        broken["discord_connected"] = False
    else:
        broken["scheduled_day_trade_markets"] = ["other"]
    reads = []
    def read(_path):
        reads.append(_path)
        return broken if len(reads) == 1 else valid
    monkeypatch.setattr(strategy_switch, "_object", read)
    monkeypatch.setattr(strategy_switch.wall_time, "sleep", lambda _seconds: None)
    result = strategy_switch._wait_discord_ready(expected_engine_run_id="current-engine", expected_markets={"target"},
        target_market="target", checkpoint_fingerprint="exact", previous_run_id="old-owner", timeout_seconds=1)
    assert len(reads) == 2 and result is valid


@pytest.fixture
def consumer_recovery(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from stockagent.live import day_trade_maintenance
    live, run = tmp_path / "live", tmp_path / "run"
    live.mkdir()
    run.mkdir()
    args = SimpleNamespace(live_dir=live, operations_root=tmp_path / "operations")
    plan = {"operation": "add_independent_mode", "active_markets": ["target"],
            "current_deployment": {"matches": True},
            "replacement": {"stable_market_id": "target", "checkpoint_fingerprint": "exact"},
            "history": {"start_date": "2026-01-02", "end_date": "2026-10-21",
                        "expected_strategy_minute_rows": 270}}
    previous = {"status": "failed_retryable", "promotion_completed": True, "plan": plan,
                "run_dir": str(run), "error": "original readonly environment failure"}
    promotion = {"acceptance": {"minute_curve_validation": {"validated_rows": 270}},
                 "rollback_directory": str(tmp_path / "rollback")}
    (live / "promotion_receipt.json").write_text(json.dumps(promotion))
    status_path = args.operations_root / "target" / "status.json"
    status_path.parent.mkdir(parents=True)
    status_path.write_text(json.dumps(previous))
    discord_path = tmp_path / "discord.json"
    discord_path.write_text(json.dumps({"run_id": "current-owner"}))
    monkeypatch.setattr(strategy_switch, "DISCORD_STATUS_PATH", discord_path)
    events, writes = [], []
    lease = {"held": False}
    @contextmanager
    def control_lease(**_kwargs):
        assert not lease["held"]
        lease["held"] = True
        events.append("lease")
        try:
            yield
        finally:
            lease["held"] = False
            events.append("release")
    monkeypatch.setattr(day_trade_maintenance, "control_lease", control_lease)
    save = strategy_switch._atomic_json
    def write(path, payload):
        writes.append((path, lease["held"]))
        save(path, payload)
    monkeypatch.setattr(strategy_switch, "_atomic_json", write)
    monkeypatch.setattr(strategy_switch, "_flat_live_state", lambda *_args: events.append("flat"))
    monkeypatch.setattr(strategy_switch, "_wait_engine_sync", lambda *_args: {"engine_run_id": "engine"})
    monkeypatch.setattr(strategy_switch, "_ensure_discord_deployment_schedule", lambda *_a, **_k: {"market": "target"})
    def restart(service):
        events.append(service)
        return {"service": service, "active": True}
    monkeypatch.setattr(strategy_switch, "_restart_and_wait_active", restart)
    monkeypatch.setattr(strategy_switch, "_wait_active", lambda *_a, **_k: None)
    monkeypatch.setattr(strategy_switch, "_verify_dashboard", lambda **_kwargs: {"local_status": {"operational_issues": []}})
    return SimpleNamespace(args=args, plan=plan, previous=previous, events=events, writes=writes,
                           lease=lease, status_path=status_path, run=run, live=live)


@pytest.mark.parametrize("already_ready", [True, False])
def test_promoted_consumer_recovery_reuses_only_admitted_current_owner(consumer_recovery, monkeypatch, already_ready):
    fixture = consumer_recovery
    waits = []
    def wait(**kwargs):
        waits.append(kwargs)
        if not already_ready and len(waits) == 1:
            raise RuntimeError("not ready")
        return {"run_id": "current-owner" if already_ready else "new-owner", "startup_inference_warmup": {"status": "ready"}}
    monkeypatch.setattr(strategy_switch, "_wait_discord_ready", wait)
    result = strategy_switch._recover_promoted_consumers(fixture.args, fixture.plan, fixture.previous)
    assert result["status"] == "complete"
    assert result["second_ledger_promotion_performed"] is False
    assert (strategy_switch.DISCORD_SERVICE in fixture.events) is not already_ready
    assert strategy_switch.PUBLIC_SERVICE not in fixture.events
    assert result["public_restart"]["restarted"] is False
    assert len(waits) == (1 if already_ready else 2)
    assert waits[0]["previous_run_id"] == "" and waits[0]["timeout_seconds"] == 1
    if already_ready:
        assert result["discord"]["restart"]["restarted"] is False
    else:
        assert waits[1]["previous_run_id"] == "current-owner"
    assert fixture.events[0:2] == ["lease", "flat"]
    assert fixture.events[-1] == "release"
    assert all(held for _, held in fixture.writes)
    assert json.loads(fixture.status_path.read_text()) == result
    assert json.loads(Path(result["consumer_recovery_receipt"]).read_text())["status"] == "complete"


@pytest.mark.parametrize("stage", ["flat_and_engine_sync", "promoted_minute_scope", "schedule_update",
                                 "discord_warmup", "public_restart", "dashboard_acceptance"])
def test_consumer_recovery_failure_keeps_current_error_and_promoted_ledger(consumer_recovery, monkeypatch, stage):
    fixture = consumer_recovery
    def fail(*_args, **_kwargs):
        raise RuntimeError(f"current {stage} failure")
    monkeypatch.setattr(strategy_switch, "_wait_discord_ready", lambda **_kwargs: {"run_id": "current-owner"})
    if stage == "flat_and_engine_sync":
        monkeypatch.setattr(strategy_switch, "_flat_live_state", fail)
    elif stage == "promoted_minute_scope":
        (fixture.live / "promotion_receipt.json").write_text(json.dumps({"acceptance": {"minute_curve_validation": {"validated_rows": 0}}}))
    elif stage == "schedule_update":
        monkeypatch.setattr(strategy_switch, "_ensure_discord_deployment_schedule", fail)
    elif stage == "discord_warmup":
        monkeypatch.setattr(strategy_switch, "_wait_discord_ready", fail)
    elif stage == "public_restart":
        monkeypatch.setattr(strategy_switch, "_verify_dashboard", fail)
        monkeypatch.setattr(strategy_switch, "_restart_and_wait_active", fail)
    else:
        monkeypatch.setattr(strategy_switch, "_verify_dashboard", fail)
    message = "minute scope differs" if stage == "promoted_minute_scope" else f"current {stage} failure"
    with pytest.raises(RuntimeError, match=message):
        strategy_switch._recover_promoted_consumers(fixture.args, fixture.plan, fixture.previous)
    failed = json.loads(fixture.status_path.read_text())
    assert message in failed["error"]
    assert failed["previous_consumer_failure"] == fixture.previous["error"]
    assert failed["consumer_recovery"]["stage"] == stage
    assert failed["promotion_completed"] is True and failed["second_ledger_promotion_performed"] is False
    assert json.loads(Path(failed["consumer_recovery_receipt"]).read_text())["status"] == "failed_retryable"
    assert all(held for _, held in fixture.writes)
    assert fixture.events[-1] == "release"
    if stage in {"flat_and_engine_sync", "promoted_minute_scope", "schedule_update"}:
        assert strategy_switch.PUBLIC_SERVICE not in fixture.events
        assert strategy_switch.DISCORD_SERVICE not in fixture.events


def test_consumer_recovery_without_lease_never_overwrites_another_owner(consumer_recovery, monkeypatch):
    from contextlib import contextmanager
    from stockagent.live import day_trade_maintenance
    fixture = consumer_recovery
    @contextmanager
    def refused_lease(**_kwargs):
        raise RuntimeError("another owner holds the control lease")
        yield
    monkeypatch.setattr(day_trade_maintenance, "control_lease", refused_lease)
    with pytest.raises(RuntimeError, match="another owner"):
        strategy_switch._recover_promoted_consumers(fixture.args, fixture.plan, fixture.previous)
    assert json.loads(fixture.status_path.read_text()) == fixture.previous
    assert fixture.status_path not in [path for path, _held in fixture.writes]
    assert fixture.events == []


def test_consumer_recovery_retains_truthful_dashboard_gaps(consumer_recovery, monkeypatch):
    fixture = consumer_recovery
    monkeypatch.setattr(strategy_switch, "_wait_discord_ready", lambda **_kwargs: {"run_id": "current-owner"})
    monkeypatch.setattr(strategy_switch, "_verify_dashboard", lambda **_kwargs: {"local_status": {"operational_issues": ["source gap"]}})
    result = strategy_switch._recover_promoted_consumers(fixture.args, fixture.plan, fixture.previous)
    assert result["status"] == result["consumer_recovery"]["status"] == "complete_with_data_gaps"


@pytest.mark.parametrize("failure", ["inactive", "unverified_api"])
def test_consumer_recovery_restarts_unaccepted_public_owner_once(consumer_recovery, monkeypatch, failure):
    fixture = consumer_recovery
    monkeypatch.setattr(strategy_switch, "_wait_discord_ready", lambda **_kwargs: {"run_id": "current-owner"})
    checks = []
    def active(*_args, **_kwargs):
        if failure == "inactive":
            raise RuntimeError("public service is inactive")
    def dashboard(**_kwargs):
        checks.append("read")
        if failure == "unverified_api" and len(checks) == 1:
            raise RuntimeError("public ledger mode set differs")
        return {"local_status": {"operational_issues": []}}
    monkeypatch.setattr(strategy_switch, "_wait_active", active)
    monkeypatch.setattr(strategy_switch, "_verify_dashboard", dashboard)
    result = strategy_switch._recover_promoted_consumers(fixture.args, fixture.plan, fixture.previous)
    assert result["status"] == "complete"
    assert fixture.events.count(strategy_switch.PUBLIC_SERVICE) == 1
    assert strategy_switch.DISCORD_SERVICE not in fixture.events
    assert len(checks) == (1 if failure == "inactive" else 2)
    assert result["consumer_recovery"]["public_initial_acceptance_error"]
    assert all(held for _, held in fixture.writes)


@pytest.mark.parametrize("defect", ["operation", "unpromoted", "current_identity", "checkpoint", "history"])
def test_consumer_recovery_cannot_admit_another_operation(consumer_recovery, defect):
    from copy import deepcopy
    fixture = consumer_recovery
    plan, previous = deepcopy(fixture.plan), deepcopy(fixture.previous)
    if defect == "operation":
        plan["operation"] = "replace_strategy"
    elif defect == "unpromoted":
        previous["promotion_completed"] = False
    elif defect == "current_identity":
        plan["current_deployment"]["matches"] = False
    elif defect == "checkpoint":
        plan["replacement"]["checkpoint_fingerprint"] = "other"
    else:
        plan["history"]["end_date"] = "2026-10-22"
    with pytest.raises(ValueError, match="exact already-promoted"):
        strategy_switch._recover_promoted_consumers(fixture.args, plan, previous)
    assert fixture.events == [] and fixture.writes == []
