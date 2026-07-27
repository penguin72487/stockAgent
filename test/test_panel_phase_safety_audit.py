from __future__ import annotations

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.explainability import _subset_panel_symbols as _explain_subset_symbols
from stockagent.live.signal_engine import (
    _require_single_target_live_weights,
    _tail_panel_dates,
)
from stockagent.training.trainer import (
    _checkpoint_manifest,
    _subset_panel_symbols as _trainer_subset_symbols,
)


def _panel() -> PanelData:
    rows, symbols = 3, 3
    mask = np.ones((rows, symbols), dtype=np.bool_)
    return PanelData(
        dates=np.arange(
            np.datetime64("2026-01-02"),
            np.datetime64("2026-01-05"),
            dtype="datetime64[D]",
        ),
        symbols=["A", "B", "C"],
        feature_names=["feature"],
        features=np.zeros((rows, symbols, 1), dtype=np.float32),
        returns_1d=np.zeros((rows, symbols), dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros((rows,), dtype=np.float32),
        close_prices=np.full((rows, symbols), 100.0, dtype=np.float32),
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        can_short_open_open_mask=np.array(
            [
                [True, False, False],
                [False, True, False],
                [False, False, True],
            ],
            dtype=np.bool_,
        ),
        force_short_cover_mask=np.zeros_like(mask),
        force_exit_mask=np.zeros_like(mask),
        daily_volumes=np.full((rows, symbols), 1_000.0, dtype=np.float32),
        unresolved_corporate_action_mask=np.zeros_like(mask),
        short_margin_rate=np.full((rows, symbols), 0.9, dtype=np.float32),
    )


def test_live_tail_preserves_open_session_short_eligibility() -> None:
    panel = _panel()

    tail = _tail_panel_dates(panel, 2)

    assert np.array_equal(
        tail.can_short_open_open_mask,
        panel.can_short_open_open_mask[-2:],
    )


def test_trainer_symbol_alignment_preserves_and_fail_closes_open_short_mask() -> None:
    panel = _panel()

    aligned = _trainer_subset_symbols(
        panel,
        ["C", "MISSING", "A"],
        allow_missing_masked=True,
    )

    assert np.array_equal(
        aligned.can_short_open_open_mask[:, [0, 2]],
        panel.can_short_open_open_mask[:, [2, 0]],
    )
    assert not aligned.can_short_open_open_mask[:, 1].any()


def test_explainability_symbol_alignment_preserves_open_short_mask() -> None:
    panel = _panel()

    aligned = _explain_subset_symbols(panel, ["C", "A"])

    assert np.array_equal(
        aligned.can_short_open_open_mask,
        panel.can_short_open_open_mask[:, [2, 0]],
    )


@pytest.mark.parametrize(
    ("execution_mode", "phases"),
    [("tw_cash", 2), ("tw_overnight", 3)],
)
def test_live_preview_rejects_phase_actions_instead_of_collapsing_them(
    execution_mode: str,
    phases: int,
) -> None:
    actions = torch.zeros((1, phases, 3), dtype=torch.float32)

    with pytest.raises(RuntimeError, match="phase-aware model actions"):
        _require_single_target_live_weights(
            actions,
            execution_mode=execution_mode,
            expected_symbols=3,
        )


def test_live_preview_accepts_exact_legacy_single_target_shape() -> None:
    weights = torch.zeros((1, 3), dtype=torch.float32)

    assert (
        _require_single_target_live_weights(
            weights,
            execution_mode="naive",
            expected_symbols=3,
        )
        is weights
    )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_carrying_checkpoint_fingerprints_open_session_short_mask(
    execution_mode: str,
) -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.trading.execution_mode = execution_mode
    baseline_panel = _panel()
    changed_panel = _panel()
    changed_panel.can_short_open_open_mask[1, 1] = False

    baseline = _checkpoint_manifest(baseline_panel, config)
    changed = _checkpoint_manifest(changed_panel, config)

    assert (
        "can_short_open_open_mask"
        in baseline["contracts"]["data"]["panel_arrays"]
    )
    assert baseline["fingerprints"]["data"] != changed["fingerprints"]["data"]
