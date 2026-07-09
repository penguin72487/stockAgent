from pathlib import Path

import polars as pl

from scripts.repair_live_signal_artifacts import _repair_frame, _row_invariant_issues


def test_repair_frame_respects_saved_side_masks_without_tw_recompute(tmp_path: Path) -> None:
    path = tmp_path / "target_weights.parquet"
    pl.DataFrame(
        [
            {
                "date": "2026-06-30",
                "symbol": "NO_SUCH_SYMBOL",
                "tradable": True,
                "can_buy": True,
                "can_sell": False,
                "current_weight": 0.10,
                "model_weight": -0.50,
                "target_weight": -0.20,
                "delta_weight": -0.30,
                "abs_delta_weight": 0.30,
                "action": "SELL",
                "constraint": "",
            },
            {
                "date": "2026-06-30",
                "symbol": "NO_SUCH_SYMBOL_2",
                "tradable": True,
                "can_buy": False,
                "can_sell": True,
                "current_weight": -0.10,
                "model_weight": 0.50,
                "target_weight": 0.20,
                "delta_weight": 0.30,
                "abs_delta_weight": 0.30,
                "action": "BUY",
                "constraint": "",
            },
        ]
    ).write_parquet(path)

    repaired, changed = _repair_frame(path)
    rows = repaired.to_dicts()

    assert changed > 0
    assert rows[0]["target_weight"] == rows[0]["current_weight"]
    assert rows[0]["delta_weight"] == 0.0
    assert rows[0]["action"] == "HOLD"
    assert rows[0]["constraint"] == "sell_blocked"
    assert rows[1]["target_weight"] == rows[1]["current_weight"]
    assert rows[1]["delta_weight"] == 0.0
    assert rows[1]["action"] == "HOLD"
    assert rows[1]["constraint"] == "buy_blocked"
    assert not any(_row_invariant_issues(row) for row in rows)


def test_repair_frame_zeroes_untradable_weights(tmp_path: Path) -> None:
    path = tmp_path / "target_weights.parquet"
    pl.DataFrame(
        [
            {
                "date": "2026-06-30",
                "symbol": "HALTED",
                "tradable": False,
                "can_buy": False,
                "can_sell": False,
                "current_weight": -0.25,
                "model_weight": -0.50,
                "target_weight": -0.40,
                "delta_weight": -0.15,
                "abs_delta_weight": 0.15,
                "action": "SELL",
                "constraint": "",
                "portfolio_contribution": -0.01,
            }
        ]
    ).write_parquet(path)

    repaired, changed = _repair_frame(path)
    row = repaired.to_dicts()[0]

    assert changed > 0
    assert row["current_weight"] == 0.0
    assert row["model_weight"] == 0.0
    assert row["target_weight"] == 0.0
    assert row["delta_weight"] == 0.0
    assert row["abs_delta_weight"] == 0.0
    assert row["action"] == "HOLD"
    assert row["constraint"] == "not_tradable"
    assert row["position_status"] == "untradable"
    assert row["portfolio_contribution"] == 0.0
    assert _row_invariant_issues(row) == []
