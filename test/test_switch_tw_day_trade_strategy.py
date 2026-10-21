from __future__ import annotations

import json
from datetime import date
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


def test_requested_artifact_missing_never_uses_selected_other_model(tmp_path: Path) -> None:
    selected = tmp_path / "layernorm"
    selected.mkdir()
    config = SimpleNamespace(output_dir=str(selected), fold_id=11, checkpoint_path="unused")
    with pytest.raises(FileNotFoundError, match="requested training artifact is unavailable"):
        strategy_switch._require_requested_artifact(config, tmp_path / "attention_layernorm")


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


def test_candidate_preflight_is_not_an_applicable_plan(monkeypatch) -> None:
    args = strategy_switch.build_parser().parse_args(["plan", "--candidate-preflight"])
    monkeypatch.setattr(strategy_switch.tempfile, "mkdtemp", lambda **_kwargs: pytest.fail("must not create a candidate directory"))
    with pytest.raises(RuntimeError, match="read-only candidate preflight cannot be applied"):
        strategy_switch._apply(args, {"plan_complete": False})
    args.candidate_preflight = False
    with pytest.raises(RuntimeError, match="read-only candidate preflight cannot be applied"):
        strategy_switch._apply(args, {"plan_complete": False})


def test_candidate_preflight_retains_modes_and_reports_blockers(monkeypatch, tmp_path):
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
    result = strategy_switch._candidate_preflight(
        args, market_config=market_config, market_config_path=tmp_path / "market.yaml",
        experiment_path=config_path, experiment=experiment,
        requested_artifact={"identity_matches": True}, lifecycle={"fold_status": "complete"}, capital=10000000,
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
    assert {"live_flat_gate_not_ready", "factorized_manifest_missing", "additive_cutover_not_verified"} <= codes


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
                "startup_inference_warmup": {"status": "ready", "ready_count": 1,
                    "markets": [{"market": "target", "checkpoint_fingerprint": "exact"}]}}
    monkeypatch.setattr(strategy_switch, "_object", read)
    monkeypatch.setattr(strategy_switch.wall_time, "sleep", lambda _seconds: None)
    result = strategy_switch._wait_discord_ready(expected_engine_run_id="current-engine", expected_markets={"target"},
        target_market="target", checkpoint_fingerprint="exact", previous_run_id="old-owner", timeout_seconds=1)
    assert len(calls) == 2 and result["run_id"] == "new-owner"
