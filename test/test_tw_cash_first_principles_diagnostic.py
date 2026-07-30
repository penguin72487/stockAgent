from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import torch

from stockagent.training import trainer as trainer_module


REPO_ROOT = Path(__file__).resolve().parents[1]
TW_CASH_ROOT = (
    REPO_ROOT / "artifacts/markets/tw_public_candles_tw_cash_lookback32"
)
TW_DAY_TRADE_ROOT = (
    REPO_ROOT / "artifacts/markets/tw_public_candles_tw_day_trade_lookback32"
)


def _fold_artifacts(root: Path, filename: str) -> list[Path]:
    return sorted(root.glob(f"fold_*/{filename}"))


def _defaulted_fold_count(paths: list[Path]) -> int:
    count = 0
    for path in paths:
        with np.load(path) as artifact:
            count += int(np.asarray(artifact["settlement_default"], dtype=bool).any())
    return count


def test_saved_tw_cash_artifacts_expose_forced_gross_redundant_phase_targets() -> None:
    """Characterize the exact point-in-time artifacts behind the diagnosis."""

    cash_continuous = _fold_artifacts(
        TW_CASH_ROOT, "test_backtest_continuous_surrogate.npz"
    )
    cash_integer = _fold_artifacts(TW_CASH_ROOT, "test_integer_share_backtest.npz")
    day_trade_continuous = _fold_artifacts(
        TW_DAY_TRADE_ROOT, "test_backtest_continuous_surrogate.npz"
    )
    day_trade_integer = _fold_artifacts(
        TW_DAY_TRADE_ROOT, "test_integer_share_backtest.npz"
    )
    if not all(
        len(paths) == 12
        for paths in (
            cash_continuous,
            cash_integer,
            day_trade_continuous,
            day_trade_integer,
        )
    ):
        pytest.skip("the compared 12-fold experiment artifacts are not available")

    cosine_values: list[np.ndarray] = []
    gross_rows = 0
    full_gross_rows = 0
    for path in cash_continuous:
        with np.load(path) as artifact:
            requested = np.asarray(
                artifact["requested_weights_history"], dtype=np.float64
            )
        assert requested.ndim == 3 and requested.shape[1] == 2
        phase_gross = np.abs(requested).sum(axis=2)
        active = np.all(phase_gross > 1.0e-12, axis=1)
        gross_rows += int(phase_gross.size)
        full_gross_rows += int(
            np.isclose(phase_gross, 1.0, rtol=1.0e-6, atol=1.0e-6).sum()
        )

        open_targets = requested[active, 0]
        close_targets = requested[active, 1]
        denominator = np.linalg.norm(open_targets, axis=1) * np.linalg.norm(
            close_targets, axis=1
        )
        cosine_values.append(
            np.divide(
                (open_targets * close_targets).sum(axis=1),
                denominator,
                out=np.zeros_like(denominator),
                where=denominator > 0.0,
            )
        )

    phase_cosine = np.concatenate(cosine_values)
    assert full_gross_rows / gross_rows > 0.999
    assert float(np.median(phase_cosine)) > 0.99
    assert _defaulted_fold_count(cash_continuous) == 9
    assert _defaulted_fold_count(cash_integer) == 7
    assert _defaulted_fold_count(day_trade_continuous) == 0
    assert _defaulted_fold_count(day_trade_integer) == 0


@pytest.mark.xfail(
    strict=True,
    reason=(
        "known diagnostic gap: absorbing default is stored as -inf, but the "
        "current tensor metric helper replaces -inf with a zero-return row"
    ),
)
def test_tensor_metrics_treat_absorbing_default_as_total_loss() -> None:
    metrics = trainer_module._compute_metrics_from_tensors(
        torch.tensor([0.01, float("-inf"), 0.0], dtype=torch.float64),
        torch.zeros(3, dtype=torch.float64),
        torch.zeros(3, dtype=torch.float64),
    )

    assert metrics["cumulative_return"] == pytest.approx(-1.0)
