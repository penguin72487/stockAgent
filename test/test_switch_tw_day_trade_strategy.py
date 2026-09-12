from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import switch_tw_day_trade_strategy as strategy_switch


@pytest.mark.parametrize("version,expected", [(None, False), (0, False), (True, False), ("1", False), (1, True)])
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
