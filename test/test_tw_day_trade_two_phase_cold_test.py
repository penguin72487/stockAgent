from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

from stockagent.backtest.tw_execution import TaiwanFeeSchedule
from stockagent.live.tw_day_trade_simulation import ModeSpec
from scripts.run_tw_day_trade_two_phase_cold_test import (
    EXPECTED_MARKETS,
    _run_real_model_inference_shadow,
    run_two_phase_cold_test,
)


def test_two_phase_cold_start_repairs_then_executes_all_modes(
    tmp_path: Path,
) -> None:
    report = run_two_phase_cold_test(
        session_date=date(2026, 8, 26),
        output_root=tmp_path / "cold-test",
        run_id="unit",
    )

    assert report["status"] == "ok"
    assert report["simulation_only"] is True
    assert report["production_order_possible"] is False
    assert report["active_markets"] == list(EXPECTED_MARKETS)
    assert report["registered_public_dataset_count"] == 156
    assert report["phases"]["preopen_missing"]["ready"] is False
    assert report["phases"]["preopen_repaired"]["ready"] is True
    assert report["phases"]["opening_missing"] == {
        "signal_pointers_absent": True,
        "fill_ledger_absent": True,
    }
    assert set(report["phases"]["signal_ready_for_execution"].values()) == {
        "ready"
    }
    assert set(report["phases"]["opening_executed"]["results"].values()) == {
        "registered"
    }
    assert report["phases"]["opening_executed"]["post_open_gate_ready"] is True
    assert report["phases"]["intraday_postclose"]["all_modes_flat"] is True
    assert report["phases"]["intraday_postclose"]["restart_count"] == 2
    assert (
        report["phases"]["intraday_postclose"][
            "duplicate_fill_count_after_completed_restart"
        ]
        == 0
    )
    assert report["phases"]["intraday_postclose"]["terminal_fill_contract"] == (
        "simulation_terminal_ledger_not_exchange_fill"
    )
    assert Path(report["artifacts"]["phase3_intraday_postclose"]).is_file()
    assert all(row["passed"] for row in report["checks"])


def test_two_phase_cold_start_uses_0901_official_open_and_never_touches_live_state(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "cold-test"
    report = run_two_phase_cold_test(
        session_date=date(2026, 8, 26),
        output_root=output_root,
        run_id="safety",
    )

    modes = report["phases"]["opening_executed"]["modes"]
    assert modes["tw_day_trade_100m"]["side"] == "long"
    assert modes["tw_day_trade_100m"]["entry_price"] == 999.0
    assert modes["tw_day_trade_multi_basis"]["side"] == "short"
    assert modes["tw_day_trade_multi_basis"]["entry_price"] == 999.0
    assert modes["tw_day_trade_multi_basis_projection_l1_gelu"]["side"] == "long"
    assert modes["tw_day_trade_multi_basis_projection_l1_gelu"]["entry_price"] == 999.0
    assert all(row["filled_shares"] == 1_000 for row in modes.values())
    assert all(row["requested_shares"] == 1_000 for row in modes.values())
    assert all(row["entry_unfilled_shares"] == 0 for row in modes.values())
    assert all(row["displayed_best_volume_shares"] == 1 for row in modes.values())
    assert all(row["paper_market_fill"] is False for row in modes.values())
    assert all(
        row["counterfactual_open_price_fill"] is True for row in modes.values()
    )
    assert all(row["synthetic_fill"] is False for row in modes.values())

    isolation = report["isolation"]
    assert isolation == {
        "sandbox": str(output_root / "runs" / "safety" / "sandbox"),
        "live_root_modified": False,
        "live_services_modified": False,
        "production_ledger_modified": False,
        "network_access_performed": False,
        "shioaji_login_performed": False,
        "broker_order_api_called": False,
    }
    latest = json.loads((output_root / "latest.json").read_text(encoding="utf-8"))
    report_path = Path(latest["report_path"])
    assert report_path.is_file()
    assert (report_path.parent / "report.sha256").is_file()


def test_real_model_shadow_requires_cold_then_hot_cache_proof(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from stockagent.live import signal_engine

    specs = []
    for market in EXPECTED_MARKETS:
        checkpoint = tmp_path / market / "fold_01" / "checkpoint_best.pt"
        checkpoint.parent.mkdir(parents=True)
        checkpoint.write_bytes(b"checkpoint")
        specs.append(
            ModeSpec(
                market=market,
                label=market,
                initial_capital_twd=10_000_000.0,
                config_path="fixture.yaml",
                checkpoint_path=str(checkpoint),
                parquet_root=tmp_path / "stocks",
                live_output_dir=tmp_path / "signals" / market,
                fee_schedule=TaiwanFeeSchedule(),
            )
        )

    call_counts: dict[str, int] = {}

    def fake_generate_live_signal(**kwargs):
        market = kwargs["market"]
        call_counts[market] = call_counts.get(market, 0) + 1
        hot = call_counts[market] == 2
        return SimpleNamespace(
            summary={
                "execution_mode": "tw_day_trade",
                "live_session_open_feature_applied": True,
                "opening_price_available_count": 10,
                "panel_date": "2026-08-26 09:00:00",
                "feature_cutoff_date": "2026-08-25 00:00:00",
                "symbol_count": 10,
                "target_gross": 1.0,
                "live_latency": {
                    "panel_cache_hit": hot,
                    "checkpoint_cache_hit": hot,
                    "model_cache_hit": hot,
                },
            },
            weights_rows=[{}] * 10,
        )

    monkeypatch.setattr(signal_engine, "generate_live_signal", fake_generate_live_signal)
    monkeypatch.setattr(signal_engine, "clear_live_inference_memory_cache", lambda: None)

    result = _run_real_model_inference_shadow(
        specs,
        session=date(2026, 8, 26),
    )

    assert result["status"] == "ready"
    assert result["model_compute"] == "real_config_checkpoint_panel_and_gpu"
    assert call_counts == {market: 2 for market in EXPECTED_MARKETS}
    assert all(
        set(row["hot_cache_proof"].values()) == {True}
        for row in result["markets"].values()
    )
