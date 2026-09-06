from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import switch_tw_day_trade_strategy as strategy_switch


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
