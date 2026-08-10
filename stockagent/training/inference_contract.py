"""Narrow public boundary from deployment into training-owned contracts.

Checkpoint manifests and panel-universe alignment still have one canonical
implementation in ``trainer``.  These lazy delegates prevent importing the
entire training orchestration graph merely to import live utilities, while
giving deployment a stable non-private API for the remaining shared semantics.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from stockagent.config import ExperimentConfig
    from stockagent.data.panel import PanelData
    from stockagent.data.walkforward import WalkForwardFold


def configure_inference_runtime(config: ExperimentConfig) -> None:
    from stockagent.training.trainer import _configure_backtest_runtime_from_config

    _configure_backtest_runtime_from_config(config)


def build_checkpoint_manifest(
    panel: PanelData,
    config: ExperimentConfig,
    *,
    include_data_content: bool = True,
) -> dict[str, Any]:
    from stockagent.training.trainer import _checkpoint_manifest

    return _checkpoint_manifest(
        panel,
        config,
        include_data_content=include_data_content,
    )


def align_panel_to_checkpoint_universe(
    panel: PanelData,
    fold_dir: Path,
    state_dict: Mapping[str, Any],
    *,
    context: str,
    allow_missing_masked: bool = False,
) -> PanelData:
    from stockagent.training.trainer import _align_panel_to_state_dict_universe

    return _align_panel_to_state_dict_universe(
        panel,
        fold_dir,
        state_dict,
        context=context,
        allow_missing_masked=allow_missing_masked,
    )


def validate_checkpoint_manifest(
    checkpoint: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    checkpoint_path: Path,
    scope: str = "resume",
    expected_fold: WalkForwardFold | None = None,
    expected_train_years: Sequence[int] | None = None,
) -> None:
    from stockagent.training.trainer import _validate_checkpoint_manifest

    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=checkpoint_path,
        scope=scope,
        expected_fold=expected_fold,
        expected_train_years=expected_train_years,
    )


__all__ = [
    "align_panel_to_checkpoint_universe",
    "build_checkpoint_manifest",
    "configure_inference_runtime",
    "validate_checkpoint_manifest",
]
