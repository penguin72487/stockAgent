"""Canonical checkpoint semantics shared by training and deployment.

This module owns checkpoint schema construction/validation and ordered panel
universe alignment. It intentionally has no dependency on trainer orchestration.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict
from functools import partial
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import torch

from stockagent.backtest.simulator import CANONICAL_BACKTEST_CONTRACT_VERSION
from stockagent.backtest.tw_execution import (
    TW_CARRYING_EXECUTION_MODES,
    TW_STOCK_EXECUTION_MODES,
    normalize_execution_mode,
)
from stockagent.backtest.tw_index_futures import (
    TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION,
)
from stockagent.backtest.tw_index_derivatives_day import (
    TW_INDEX_DERIVATIVES_DAY_BACKTEST_CONTRACT_VERSION,
)
from stockagent.config import ExperimentConfig
from stockagent.data.panel import PanelData
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_DERIVATIVE_CANDIDATE_CONTRACT_VERSION,
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
)
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.data.walkforward import WalkForwardFold, normalize_lookback_context
from stockagent.models.factory import _feature_indices_from_patterns
from stockagent.models.transformer_base_portfolio import (
    TransformerBasePortfolioModel,
    _normalize_ffn_type,
    _normalize_norm_type,
)
from stockagent.portfolio_contract import (
    normalize_portfolio_activation,
    normalize_portfolio_mode,
    normalize_portfolio_output_mode,
)


def _normalize_risk_objective(loss_type: str) -> str:
    objective = str(loss_type).strip().lower()
    if objective in {
        "sharpe",
        "sharp",
        "sharpck",
        "sharpe_ratio",
        "sortino",
        "log_utility",
        "log_util",
        "kelly",
        "growth",
        "mean_log_return",
        "rank",
        "rank_ic",
        "ic",
        "multitask_rank_ic",
        "pure_rank",
        "rank_only",
        "score_rank",
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
        "factor_generalization",
        "factor",
        "factor_ic",
        "characteristic_factor",
        "portfolio_autoencoder",
        "bottleneck_portfolio_autoencoder",
        "autoencoder_portfolio",
    }:
        if objective in {"bottleneck_portfolio_autoencoder", "autoencoder_portfolio"}:
            return "portfolio_autoencoder"
        if objective in {"factor", "factor_ic", "characteristic_factor"}:
            return "factor_generalization"
        if objective in {"rank", "ic", "multitask_rank_ic"}:
            return "rank_ic"
        if objective in {"rank_only", "score_rank"}:
            return "pure_rank"
        if objective in {"sharp", "sharpck", "sharpe_ratio"}:
            return "sharpe"
        if objective in {"log_util", "kelly", "growth", "mean_log_return"}:
            return "log_utility"
        if objective in {"cvar", "cvar_drawdown", "excess_cvar"}:
            return "excess_cvar_drawdown"
        if objective in {"outperformance_budget", "outperformance_first"}:
            return "outperformance_risk_budget"
        return objective
    return "sharpe"


def _normalized_model_name(model_name: str) -> str:
    return model_name.strip().lower().replace("-", "_")


def _is_tree_model_name(model_name: str) -> bool:
    normalized = _normalized_model_name(model_name)
    return normalized in {"lightgbm", "lgbm", "xgboost", "xgb"}


def _stable_fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _array_content_fingerprint(value: np.ndarray | None) -> dict[str, Any]:
    return array_content_fingerprint(value)


def _panel_array_content_fingerprint(
    panel: PanelData,
    name: str,
    value: np.ndarray | None,
) -> dict[str, Any]:
    """Reuse an immutable cache-generation digest when its ABI still matches."""

    cached = getattr(panel, "content_fingerprints", None)
    cached_value = None if not isinstance(cached, Mapping) else cached.get(name)
    if isinstance(cached_value, Mapping):
        if value is None:
            if cached_value.get("present") is False:
                return dict(cached_value)
        else:
            array = np.asarray(value)
            if (
                cached_value.get("present") is True
                and cached_value.get("shape") == [int(size) for size in array.shape]
                and cached_value.get("dtype") == str(array.dtype)
                and isinstance(cached_value.get("sha256"), str)
            ):
                return dict(cached_value)
    return _array_content_fingerprint(value)


_TEMPORAL_BASIS_MODEL_CONFIG_FIELDS = (
    "temporal_basis_families",
    "temporal_basis_components",
    "temporal_basis_input",
)


def _project_temporal_basis_model_config(
    values: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep disabled model branches compatible with pre-feature checkpoints."""

    projected = dict(values)
    if not projected.get("temporal_basis_families"):
        for field_name in _TEMPORAL_BASIS_MODEL_CONFIG_FIELDS:
            projected.pop(field_name, None)
    elif str(projected.get("temporal_basis_input", "embedded")) == "embedded":
        # Preserve pre-option basis checkpoint fingerprints.  Embedded is the
        # historical behavior and does not alter parameters or the forward path.
        projected.pop("temporal_basis_input", None)
    if int(projected.get("daily_context_layers", 0) or 0) == 0:
        # These controls have no parameters or forward-path effect until the
        # daily-context branch has at least one layer.  Omitting them preserves
        # model fingerprints written before the disabled branch was added,
        # while an enabled branch remains part of the strict model contract.
        projected.pop("daily_context_layers", None)
        projected.pop("daily_context_lookback", None)
        projected.pop("daily_context_pooling", None)
    return projected


def _configuration_fingerprint_snapshot(config: ExperimentConfig) -> dict[str, Any]:
    """Return a semantic config snapshot while omitting disabled new branches."""

    snapshot = asdict(config)
    training = snapshot.get("training")
    if isinstance(training, dict):
        for config_name in (
            "transformer_base_portfolio",
            "financial_transformer",
        ):
            values = training.get(config_name)
            if isinstance(values, Mapping):
                training[config_name] = _project_temporal_basis_model_config(values)
    return snapshot


def _active_model_config(config: ExperimentConfig) -> dict[str, Any]:
    normalized = _normalized_model_name(config.training.model_name)
    if normalized in {
        "cross_sectional_index_derivatives_day",
        "cross_sectional_index_derivatives_day_model",
        "tw_index_derivatives_day",
    }:
        return {
            "config_name": "financial_transformer",
            "contract_name": "cross_sectional_index_derivatives_day",
            "values": _project_temporal_basis_model_config(
                asdict(config.training.financial_transformer)
            ),
        }
    if normalized in {
        "cross_sectional_index_futures",
        "cross_sectional_index_futures_model",
        "tw_index_futures",
    }:
        return {
            "config_name": "transformer_base_portfolio",
            "contract_name": "cross_sectional_index_futures",
            "values": _project_temporal_basis_model_config(
                asdict(config.training.transformer_base_portfolio)
            ),
        }
    aliases = {
        "cross_sectional_mlp": "mlp",
        "ft": "ft_transformer",
        "transformer": "ft_transformer",
        "tabresnet": "tabular_resnet",
        "resnet": "tabular_resnet",
        "simple_multi_stock_tcn": "multi_stock_tcn",
        "mean_pool_tcn": "multi_stock_tcn",
        "efficient_tcn_tabular_set_portfolio_model": "efficient_tcn_tabular_set_portfolio",
        "efficient_portfolio": "efficient_tcn_tabular_set_portfolio",
        "lite_isab_portfolio": "efficient_tcn_tabular_set_portfolio",
        "latent_factor_market_token_portfolio_model": "latent_factor_market_token_portfolio",
        "latent_factor_market_token": "latent_factor_market_token_portfolio",
        "lfmt_portfolio": "latent_factor_market_token_portfolio",
        "latent_market_token_portfolio": "latent_factor_market_token_portfolio",
        "low_rank_market_transformer_portfolio_model": "low_rank_market_transformer_portfolio",
        "temporal_latent_factor_market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "factorized_market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "market_transformer_portfolio": "low_rank_market_transformer_portfolio",
        "lrmt_portfolio": "low_rank_market_transformer_portfolio",
        "lgbm": "lightgbm",
        "xgb": "xgboost",
        "bottleneck_autoencoder": "bottleneck_portfolio_autoencoder",
        "portfolio_autoencoder": "bottleneck_portfolio_autoencoder",
        "bpae": "bottleneck_portfolio_autoencoder",
        "transformer_base_portfolio_model": "transformer_base_portfolio",
        "flash_transformer_portfolio": "transformer_base_portfolio",
        "scalable_transformer_portfolio": "transformer_base_portfolio",
        "multi_axis_transformer_portfolio": "transformer_base_portfolio",
        "tbp": "transformer_base_portfolio",
        "gradient_boosted_portfolio_transformer_model": "gradient_boosted_portfolio_transformer",
        "gradient_boosted_transformer_portfolio": "gradient_boosted_portfolio_transformer",
        "boosted_portfolio_transformer": "gradient_boosted_portfolio_transformer",
        "boosted_transformer_portfolio": "gradient_boosted_portfolio_transformer",
        "gbpt": "gradient_boosted_portfolio_transformer",
        "cstpm": "cross_sectional_temporal_portfolio_model",
        "portfolio_multitask": "cross_sectional_temporal_portfolio_model",
        "tcn_hybrid": "tcn_hybrid_tabular_resnet",
        "tcn_tabresnet": "tcn_hybrid_tabular_resnet",
        "temporal_resnet": "temporal_tabular_resnet",
        "temporal_tabresnet": "temporal_tabular_resnet",
    }
    candidates = [normalized, aliases.get(normalized, "")]
    for candidate in candidates:
        model_config = getattr(config.training, candidate, None)
        if model_config is not None:
            return {
                "config_name": candidate,
                "values": _project_temporal_basis_model_config(asdict(model_config)),
            }
    raise ValueError(
        f"Cannot resolve active model config for training.model_name={config.training.model_name!r}"
    )


def _active_scheduler_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Fingerprint only the scheduler branch that can affect future optimizer steps."""
    training = config.training
    raw_name = str(training.lr_scheduler or "none").strip().lower().replace("-", "_")
    aliases = {
        "cosine_warmup": "warmup_cosine",
        "linear_warmup_cosine": "warmup_cosine",
        "reduce_on_plateau": "plateau",
        "reduce_lr_on_plateau": "plateau",
    }
    name = aliases.get(raw_name, raw_name)
    enabled = bool(training.enable_lr_scheduler) and name not in {
        "",
        "none",
        "off",
        "false",
        "disabled",
    }
    if name not in {"cosine", "warmup_cosine", "step", "plateau"}:
        enabled = False
    contract: dict[str, Any] = {"enabled": enabled}
    if not enabled:
        return contract

    contract["name"] = name
    if name == "cosine":
        configured_t_max = int(training.lr_scheduler_t_max)
        contract.update(
            {
                "t_max": (
                    configured_t_max
                    if configured_t_max > 0
                    else max(1, int(training.epochs))
                ),
                "t_max_source": "configured" if configured_t_max > 0 else "epochs",
                "eta_min": float(training.lr_scheduler_eta_min),
            }
        )
    elif name == "warmup_cosine":
        configured_total_steps = int(training.lr_scheduler_t_max)
        contract.update(
            {
                "configured_total_steps": max(0, configured_total_steps),
                "derived_epochs": (
                    max(1, int(training.epochs))
                    if configured_total_steps <= 0
                    else None
                ),
                "eta_min": max(0.0, float(training.lr_scheduler_eta_min)),
                "warmup_steps": max(0, int(training.lr_scheduler_warmup_steps)),
                "interval": "step",
            }
        )
    elif name == "step":
        contract.update(
            {
                "step_size": max(1, int(training.lr_scheduler_step_size)),
                "gamma": float(training.lr_scheduler_gamma),
            }
        )
    else:
        contract.update(
            {
                "gamma": float(training.lr_scheduler_gamma),
                "patience": max(1, int(training.lr_scheduler_patience)),
                "threshold": float(training.lr_scheduler_threshold),
            }
        )
    return contract


def _active_multitask_checkpoint_contract(
    config: ExperimentConfig,
    objective: str,
) -> dict[str, Any]:
    """Return only multitask controls consumed by ``risk_aware_loss`` for this objective."""
    cfg = config.training.multitask_loss
    objective = _normalize_risk_objective(objective)
    if objective == "pure_rank":
        return {"rank_ic_weight": float(cfg.rank_ic_weight)}
    if objective == "rank_ic":
        direction_weight = max(0.0, float(cfg.direction_weight))
        volatility_weight = max(0.0, float(cfg.volatility_regime_weight))
        contract = {
            "rank_ic_weight": float(cfg.rank_ic_weight),
            "direction_weight": direction_weight,
            "volatility_regime_weight": volatility_weight,
        }
        if volatility_weight > 0.0:
            contract.update(
                {
                    "regime_up_threshold": float(cfg.regime_up_threshold),
                    "regime_down_threshold": float(cfg.regime_down_threshold),
                }
            )
        return contract
    if objective == "factor_generalization":
        # The factor objective consumes these two target-regime thresholds even
        # though its remaining weights live in factor_generalization_loss.
        if (
            float(config.training.factor_generalization_loss.regime_stability_weight)
            > 0.0
        ):
            return {
                "regime_up_threshold": float(cfg.regime_up_threshold),
                "regime_down_threshold": float(cfg.regime_down_threshold),
            }
        return {}
    if objective == "portfolio_autoencoder":
        return {}

    volatility_weight = max(0.0, float(cfg.volatility_regime_weight))
    contract = {
        "return_rank_ic_weight": max(0.0, float(cfg.return_rank_ic_weight)),
        "direction_weight": max(0.0, float(cfg.direction_weight)),
        "volatility_regime_weight": volatility_weight,
        "concentration_weight": max(0.0, float(cfg.concentration_weight)),
        "net_exposure_weight": max(0.0, float(cfg.net_exposure_weight)),
    }
    if volatility_weight > 0.0:
        contract.update(
            {
                "regime_up_threshold": float(cfg.regime_up_threshold),
                "regime_down_threshold": float(cfg.regime_down_threshold),
            }
        )
    return contract


def _active_factor_loss_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    cfg = config.training.factor_generalization_loss
    contract: dict[str, Any] = {
        "slope_tstat_weight": max(0.0, float(cfg.slope_tstat_weight)),
        "rank_ic_weight": max(0.0, float(cfg.rank_ic_weight)),
        "factor_sharpe_weight": max(0.0, float(cfg.factor_sharpe_weight)),
        "block_stability_weight": max(0.0, float(cfg.block_stability_weight)),
        "regime_stability_weight": max(0.0, float(cfg.regime_stability_weight)),
        "consistency_weight": max(0.0, float(cfg.consistency_weight)),
        "net_exposure_weight": max(0.0, float(cfg.net_exposure_weight)),
        "gross_exposure_weight": max(0.0, float(cfg.gross_exposure_weight)),
        "concentration_weight": max(0.0, float(cfg.concentration_weight)),
        "turnover_weight": max(0.0, float(cfg.turnover_weight)),
        "score_l2_weight": max(0.0, float(cfg.score_l2_weight)),
        "factor_temperature": max(0.05, float(cfg.factor_temperature)),
    }
    if contract["block_stability_weight"] > 0.0:
        contract["block_count"] = max(2, int(cfg.block_count))
        contract["worst_fraction"] = min(max(float(cfg.worst_fraction), 0.0), 1.0)
    if contract["consistency_weight"] > 0.0:
        contract["augmentation"] = {
            "feature_dropout": min(
                max(float(cfg.augmentation_feature_dropout), 0.0), 0.95
            ),
            "stock_dropout": min(max(float(cfg.augmentation_stock_dropout), 0.0), 0.95),
            "time_dropout": min(max(float(cfg.augmentation_time_dropout), 0.0), 0.95),
            "noise_std": max(0.0, float(cfg.augmentation_noise_std)),
        }
    return contract


def _active_autoencoder_loss_checkpoint_contract(
    config: ExperimentConfig,
) -> dict[str, float]:
    cfg = config.training.portfolio_autoencoder_loss
    return {
        "lambda_turnover": max(0.0, float(cfg.lambda_turnover)),
        "lambda_concentration": max(0.0, float(cfg.lambda_concentration)),
        "lambda_latent": max(0.0, float(cfg.lambda_latent)),
    }


def _training_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Return the explicit schema-4 controls that can change a resumed trajectory.

    Machine-local execution, compilation, ordinary cache placement, DDP, eval
    chunking, VRAM, plotting, explainability, table, and post-processing settings
    deliberately remain in ``configuration`` only.  A cache option that changes
    immutable tensor storage precision is a semantic exception recorded below.
    Active model architecture is fingerprinted independently by the model contract.
    """
    training = config.training
    objective = _normalize_risk_objective(training.loss_type)

    # Tree estimators fit one fully materialized dataset and own their fit
    # hyperparameters in the active model contract. They never construct the
    # neural optimizer, scheduler, mini-batches, AMP/scaler, finite-step checks,
    # or fold warm-start state below. The normalized objective still determines
    # the persisted validation loss and objective-labelled evaluation artifacts.
    if _is_tree_model_name(training.model_name):
        return {"objective": objective}

    compaction_mode = _normalize_train_symbol_compaction(
        training.train_symbol_compaction
    )
    batching_contract: dict[str, Any] = {
        "batch_size_train": int(training.batch_size_train),
        "min_batch_size": int(training.min_batch_size),
        "auto_batch_size": bool(training.auto_batch_size),
        "train_symbol_compaction": compaction_mode,
    }
    if compaction_mode != "none":
        batching_contract["train_symbol_compaction_bucket_size"] = (
            _normalize_train_symbol_compaction_bucket_size(
                training.train_symbol_compaction_bucket_size
            )
        )
    contract: dict[str, Any] = {
        "seed": int(training.seed),
        "objective": objective,
        "optimizer": {
            "name": "AdamW",
            "learning_rate": float(training.learning_rate),
            "weight_decay": float(training.weight_decay),
            "grad_clip_norm": float(training.grad_clip_norm),
        },
        "scheduler": _active_scheduler_checkpoint_contract(config),
        "batching": batching_contract,
        "fold_continuation": {
            "warm_start_from_previous_fold": bool(
                training.warm_start_from_previous_fold
            ),
        },
        "validation_and_stopping": {
            "early_stopping_no_improve_ratio": float(
                training.early_stopping_no_improve_ratio
            ),
            "early_stopping_min_delta": float(training.early_stopping_min_delta),
            "best_checkpoint_max_epoch": int(training.best_checkpoint_max_epoch),
            "val_interval_epochs": int(training.val_interval_epochs),
        },
        "finite_checks": {
            "finite_check_interval_steps": max(
                0,
                int(training.finite_check_interval_steps),
            ),
        },
        "precision": {
            "amp_dtype": str(config.environment.amp_dtype),
            "use_tensor_cores": bool(config.environment.use_tensor_cores),
        },
    }
    execution_mode = normalize_execution_mode(config.trading.execution_mode)
    if (
        execution_mode in TW_CARRYING_EXECUTION_MODES
        or execution_mode == "tw_day_trade"
    ):
        contract["settlement_gradient_horizon_rows"] = max(
            0,
            int(getattr(training, "tw_continuous_gradient_horizon_rows", 32)),
        )
    if bool(getattr(training, "cache_train_features_in_amp_dtype", False)):
        # This opt-in changes the immutable feature storage seen by the train
        # executor. Keep default=false schema-4 checkpoints backward compatible,
        # but never resume an opted-in trajectory under a different precision path.
        contract["precision"]["cache_train_features_in_amp_dtype"] = True
    # These rank objectives return before portfolio post-processing/backtest in
    # risk_aware_loss, so changing the loss activation cannot change gradients.
    if objective not in {"pure_rank", "rank_ic"}:
        contract["loss_portfolio_activation"] = normalize_portfolio_activation(
            _training_loss_portfolio_activation(config)
        )
        loss_min_trade_weight = _training_loss_min_trade_weight(config)
        if loss_min_trade_weight != float(config.trading.min_trade_weight):
            contract["loss_min_trade_weight"] = loss_min_trade_weight
    multitask = _active_multitask_checkpoint_contract(config, objective)
    if multitask:
        contract["multitask_loss"] = multitask
    if objective == "factor_generalization":
        contract["factor_generalization_loss"] = (
            _active_factor_loss_checkpoint_contract(config)
        )
    elif objective == "portfolio_autoencoder":
        contract["portfolio_autoencoder_loss"] = (
            _active_autoencoder_loss_checkpoint_contract(config)
        )
    return contract


def _training_checkpoint_contract_schema_3(
    config: ExperimentConfig,
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    """Exact training contract written by schema 3 before semantic layering."""
    contract = asdict(config.training)
    for config_name in ("transformer_base_portfolio", "financial_transformer"):
        model_values = contract.get(config_name)
        if isinstance(model_values, Mapping):
            contract[config_name] = _project_temporal_basis_model_config(model_values)
    transformer_values = contract.get("transformer_base_portfolio")
    if isinstance(transformer_values, Mapping):
        projected_transformer_values = dict(transformer_values)
        projected_transformer_values.pop("use_latent_factors", None)
        projected_transformer_values.pop("use_market_tokens", None)
        if _active_model_config(config)["config_name"] == "transformer_base_portfolio":
            projected_transformer_values = _legacy_transformer_base_switch_projection(
                transformer_values
            )
        contract["transformer_base_portfolio"] = projected_transformer_values
    # This storage-precision option was introduced by schema 4.  It must not be
    # synthesized into a historical schema-3 fingerprint; the compatibility
    # constraint below separately prevents unsafe optimizer resume when enabled.
    contract.pop("cache_train_features_in_amp_dtype", None)
    contract.pop("compile_eval_model", None)
    # This decoupling control did not exist in schema 3; historical runs always
    # used trading.min_trade_weight in the loss.
    contract.pop("loss_min_trade_weight", None)
    # Schema 3 recorded this field even though no executor ever consumed it.
    # Reconstruct the only honest/effective historical value after removing the
    # no-op from TrainingConfig so old fingerprints remain verifiable.
    contract["train_symbol_subsample_ratio"] = 1.0
    contract["lr_scheduler_interval"] = str(legacy_lr_scheduler_interval)
    contract["precision"] = {
        "amp_dtype": str(config.environment.amp_dtype),
        "use_tensor_cores": bool(config.environment.use_tensor_cores),
    }
    return contract


def _training_checkpoint_contract_schema_2(
    config: ExperimentConfig,
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    """Compatibility contract emitted by the first layered-manifest release."""
    training = config.training
    model_values = _active_model_config(config)["values"]
    return {
        "seed": int(training.seed),
        "loss_type": str(training.loss_type),
        "loss_portfolio_activation": str(training.loss_portfolio_activation),
        "optimizer": {
            "name": "AdamW",
            "learning_rate": float(training.learning_rate),
            "weight_decay": float(training.weight_decay),
            "grad_clip_norm": float(training.grad_clip_norm),
        },
        "scheduler": {
            "enabled": bool(training.enable_lr_scheduler),
            "name": str(training.lr_scheduler),
            "interval": str(legacy_lr_scheduler_interval),
            "t_max": int(training.lr_scheduler_t_max),
            "eta_min": float(training.lr_scheduler_eta_min),
            "warmup_steps": int(training.lr_scheduler_warmup_steps),
            "step_size": int(training.lr_scheduler_step_size),
            "gamma": float(training.lr_scheduler_gamma),
            "patience": int(training.lr_scheduler_patience),
            "threshold": float(training.lr_scheduler_threshold),
        },
        "batching": {
            "batch_size_train": int(training.batch_size_train),
            "train_symbol_subsample_ratio": 1.0,
            "train_symbol_compaction": str(training.train_symbol_compaction),
            "train_symbol_compaction_bucket_size": int(
                training.train_symbol_compaction_bucket_size
            ),
        },
        "precision": {
            "amp_dtype": str(config.environment.amp_dtype),
            "use_tensor_cores": bool(config.environment.use_tensor_cores),
        },
        "model_training_outputs": {
            name: model_values[name]
            for name in ("return_aux", "return_aux_details")
            if name in model_values
        },
        "multitask_loss": asdict(training.multitask_loss),
        "factor_generalization_loss": asdict(training.factor_generalization_loss),
        "portfolio_autoencoder_loss": asdict(training.portfolio_autoencoder_loss),
    }


_PRE_EXECUTION_MODE_TRADING_FIELDS = (
    "frequency",
    "buy_fee_rate",
    "sell_fee_rate",
    "long_only",
    "max_turnover_ratio",
    "max_volume_participation",
    "volume_participation_equity",
    "reporting_leverage",
    "min_trade_weight",
    "portfolio_activation",
)


def _trading_checkpoint_contract_schema_3(
    config: ExperimentConfig,
) -> dict[str, Any]:
    """Reproduce the trading payload written before execution modes existed."""

    return {
        name: getattr(config.trading, name)
        for name in _PRE_EXECUTION_MODE_TRADING_FIELDS
    }


def _trading_checkpoint_contract_schema_2(config: ExperimentConfig) -> dict[str, Any]:
    """Map the reporting-only leverage name back to the schema-2 spelling."""
    contract = _trading_checkpoint_contract_schema_3(config)
    contract["leverage"] = contract.pop("reporting_leverage")
    return contract


def _legacy_checkpoint_setting_contract(
    config: ExperimentConfig,
    active_model: Mapping[str, Any],
    *,
    legacy_lr_scheduler_interval: str = "epoch",
) -> dict[str, Any]:
    legacy_model_values = dict(active_model["values"])
    if str(active_model.get("config_name", "")) == "transformer_base_portfolio":
        legacy_model_values = _legacy_transformer_base_switch_projection(
            legacy_model_values
        )
    return {
        "model_name": str(config.training.model_name),
        "model": legacy_model_values,
        "loss_type": str(config.training.loss_type),
        "loss_portfolio_activation": str(config.training.loss_portfolio_activation),
        "trading": _trading_checkpoint_contract_schema_2(config),
        "evaluation": asdict(config.evaluation),
        "multitask_loss": asdict(config.training.multitask_loss),
        "optimizer": {
            "learning_rate": float(config.training.learning_rate),
            "weight_decay": float(config.training.weight_decay),
            "lr_scheduler": str(config.training.lr_scheduler),
            "lr_scheduler_interval": str(legacy_lr_scheduler_interval),
            "lr_scheduler_warmup_steps": int(config.training.lr_scheduler_warmup_steps),
        },
        "train_symbol_compaction": str(config.training.train_symbol_compaction),
    }


def _trading_checkpoint_contract(config: ExperimentConfig) -> dict[str, Any]:
    """Return trading controls consumed by canonical loss/backtest execution."""
    trading = config.trading
    execution_mode = str(trading.execution_mode)
    contract: dict[str, Any] = {
        "canonical_backtest_contract_version": int(CANONICAL_BACKTEST_CONTRACT_VERSION),
        "execution_mode": execution_mode,
        "buy_fee_rate": float(trading.buy_fee_rate),
        "sell_fee_rate": float(trading.sell_fee_rate),
        "long_only": bool(trading.long_only),
        "max_turnover_ratio": float(trading.max_turnover_ratio),
        "max_volume_participation": float(trading.max_volume_participation),
        "volume_participation_equity": float(trading.volume_participation_equity),
        "min_trade_weight": float(trading.min_trade_weight),
        "portfolio_activation": normalize_portfolio_activation(
            trading.portfolio_activation
        ),
    }
    if execution_mode in {"tw_index_futures_day", "tw_index_derivatives_day"}:
        contract["taiwan_index_futures_day"] = {
            "backtest_contract_version": int(
                TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION
            ),
            "data_path": str(trading.tw_index_futures_data_path),
            "reference_product": str(trading.tw_index_futures_reference_product),
            "initial_capital": float(trading.tw_index_futures_initial_capital),
            "max_abs_exposure": float(trading.tw_index_futures_max_abs_exposure),
            "exchange_and_clearing_fee_per_side_twd": list(
                trading.tw_index_futures_exchange_and_clearing_fee_per_side_twd
            ),
            "total_fee_per_side_twd": list(
                trading.tw_index_futures_total_fee_per_side_twd
            ),
            "sell_transaction_tax_rate": float(
                trading.tw_index_futures_sell_transaction_tax_rate
            ),
            "slippage_points_per_side": list(
                trading.tw_index_futures_slippage_points_per_side
            ),
            "basket_fee_penalty": float(trading.tw_index_futures_basket_fee_penalty),
        }
    if execution_mode == "tw_index_derivatives_day":
        contract["taiwan_index_derivatives_day"] = {
            "backtest_contract_version": int(
                TW_INDEX_DERIVATIVES_DAY_BACKTEST_CONTRACT_VERSION
            ),
            "candidate_contract_version": int(
                TAIFEX_DERIVATIVE_CANDIDATE_CONTRACT_VERSION
            ),
            "action_count": int(TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4),
            "monthly_options_data_path": str(
                trading.tw_index_options_monthly_data_path
            ),
            "weekly_options_data_path": str(
                trading.tw_index_options_weekly_data_path
            ),
            "maximum_capital_fraction": float(
                trading.tw_index_derivatives_day_maximum_capital_fraction
            ),
            "option_fixed_fee_per_contract_per_side_twd": float(
                trading.tw_index_derivatives_day_option_fixed_fee_per_contract_per_side_twd
            ),
            "option_sell_transaction_tax_rate": float(
                trading.tw_index_derivatives_day_option_transaction_tax_rate
            ),
            "option_slippage_points_per_side": float(
                trading.tw_index_derivatives_day_option_slippage_points_per_side
            ),
            "option_price_source": "taifex_daily_first_last_trade_proxy",
            "option_shorting": False,
        }
    if execution_mode in TW_STOCK_EXECUTION_MODES:
        contract["taiwan_execution"] = {
            "commission_rate": float(trading.tw_commission_rate),
            "commission_discount": float(trading.tw_commission_discount),
            "commission_rebate_timing": str(trading.tw_commission_rebate_timing),
            "stock_sell_tax": float(trading.tw_stock_sell_tax),
            "etf_sell_tax": float(trading.tw_etf_sell_tax),
            "day_trade_stock_sell_tax": float(trading.tw_day_trade_stock_sell_tax),
            "day_trade_etf_sell_tax": float(trading.tw_day_trade_etf_sell_tax),
            "minimum_commission": float(trading.tw_minimum_commission),
            "commission_rounding": str(trading.tw_commission_rounding),
            "tax_rounding": str(trading.tw_tax_rounding),
            "settlement_lag_sessions": int(trading.tw_settlement_lag_sessions),
            "cash_lot_size": int(trading.tw_cash_lot_size),
            "day_trade_lot_size": int(trading.tw_day_trade_lot_size),
            "short_initial_margin_rate": float(trading.tw_short_initial_margin_rate),
            "short_maintenance_ratio": float(trading.tw_short_maintenance_ratio),
            "short_lot_size": int(trading.tw_short_lot_size),
            "short_handling_fee_rate": float(trading.tw_short_handling_fee_rate),
            "short_capacity_limit_enabled": bool(
                trading.tw_short_capacity_limit_enabled
            ),
            "corporate_action_mode": str(trading.tw_corporate_action_mode),
        }
        benchmark_sensitive_objectives = {
            "excess_cvar_drawdown",
            "cvar",
            "cvar_drawdown",
            "excess_cvar",
            "outperformance_risk_budget",
            "outperformance_budget",
            "outperformance_first",
        }
        benchmark_affects_optimizer = (
            _normalize_risk_objective(config.training.loss_type)
            in benchmark_sensitive_objectives
            or float(config.training.multitask_loss.volatility_regime_weight) > 0.0
        )
        if execution_mode == "tw_day_trade" and benchmark_affects_optimizer:
            # Reporting-only benchmark repairs are checkpoint-compatible with
            # objectives such as log_utility that never consume the benchmark.
            # Benchmark-sensitive objectives must not resume across alignment
            # semantics because their optimizer trajectories do change.
            contract["taiwan_execution"]["benchmark_alignment"] = (
                "prior_adjusted_close_to_execution_close_v1"
            )
        if str(trading.tw_corporate_action_mode) == "exact":
            contract["taiwan_execution"]["corporate_action_claim_queue_sessions"] = int(
                trading.tw_corporate_action_claim_queue_sessions
            )
    return contract


_SCHEMA_3_MODEL_RUNTIME_FIELDS = {
    "checkpoint_blocks",
    "max_full_tokens",
    "gpu_device_id",
    "n_jobs",
    "return_aux",
    "return_aux_details",
    "sdpa_batch_limit",
    "use_gpu",
    "use_flash_attention",
}

_SCHEMA_4_MODEL_RUNTIME_FIELDS = _SCHEMA_3_MODEL_RUNTIME_FIELDS | {
    "temporal_checkpoint",
}


def _legacy_transformer_base_switch_projection(
    values: Mapping[str, Any],
) -> dict[str, Any]:
    """Project new bottleneck switches onto pre-switch checkpoint semantics.

    Omit the new fields and, only when the switches change the requested preset,
    rewrite it to the effective historical mode. This lets schema 1--3 accept an
    architecture-identical cross-preset spelling. ``latent_only`` remains a new,
    unmatchable mode and therefore cannot resume an old optimizer trajectory.
    """
    projected = dict(values)
    use_latent_factors = projected.pop("use_latent_factors", None)
    use_market_tokens = projected.pop("use_market_tokens", None)
    requested_mode = TransformerBasePortfolioModel._normalize_attention_mode(
        str(projected["attention_mode"])
    )
    effective_mode = TransformerBasePortfolioModel._resolve_attention_mode(
        requested_mode,
        use_latent_factors=use_latent_factors,
        use_market_tokens=use_market_tokens,
    )
    if effective_mode != requested_mode:
        projected["attention_mode"] = effective_mode
    return projected


def _schema_3_checkpoint_model_values(
    active_model: Mapping[str, Any],
) -> dict[str, Any]:
    """Reproduce schema 3's exact generic model-field filtering."""
    values = dict(active_model["values"])
    if str(active_model.get("config_name", "")) == "transformer_base_portfolio":
        values = _legacy_transformer_base_switch_projection(values)
    return {
        name: value
        for name, value in values.items()
        if name not in _SCHEMA_3_MODEL_RUNTIME_FIELDS
    }


def _effective_model_portfolio_mode(
    config: ExperimentConfig,
    config_name: str,
    values: Mapping[str, Any],
) -> str:
    if "portfolio_mode" in values:
        raw_mode = str(values["portfolio_mode"] or "").strip().lower().replace("-", "_")
        if raw_mode in {"", "auto"}:
            raw_mode = "long_only" if config.trading.long_only else "long_short"
        return normalize_portfolio_mode(raw_mode)
    if config_name == "bottleneck_portfolio_autoencoder":
        is_long_short = bool(values.get("long_short", False)) and not bool(
            config.trading.long_only
        )
        return "long_short" if is_long_short else "long_only"
    return "long_only" if config.trading.long_only else "long_short"


def _transformer_base_checkpoint_model_values(
    config: ExperimentConfig,
    values: Mapping[str, Any],
    feature_names: Sequence[str],
) -> dict[str, Any]:
    """Resolve only architecture/behavior fields active in the selected TBP mode."""
    requested_mode = TransformerBasePortfolioModel._normalize_attention_mode(
        str(values["attention_mode"])
    )
    mode = TransformerBasePortfolioModel._resolve_attention_mode(
        requested_mode,
        use_latent_factors=values.get("use_latent_factors"),
        use_market_tokens=values.get("use_market_tokens"),
    )
    pooling = TransformerBasePortfolioModel._normalize_pooling(
        str(values["temporal_pooling"])
    )
    portfolio_mode = _effective_model_portfolio_mode(
        config,
        "transformer_base_portfolio",
        values,
    )
    output_mode = normalize_portfolio_output_mode(str(values["portfolio_output_mode"]))
    head_layers = max(0, int(values["head_layers"]))
    contract: dict[str, Any] = {
        "d_model": int(values["d_model"]),
        "attention_mode": mode,
        "use_time_pos": bool(values["use_time_pos"]),
        "use_symbol_pos": bool(values["use_symbol_pos"]),
        "input_dropout": float(values["input_dropout"]),
        "norm_type": _normalize_norm_type(str(values["norm_type"])),
        "ffn_type": _normalize_ffn_type(str(values["ffn_type"])),
        "qk_norm": bool(values["qk_norm"]),
        "temporal_pooling": pooling,
        "head_layers": head_layers,
        "dropout": float(values["dropout"]),
        "default_temperature": float(values["default_temperature"]),
        "portfolio_mode": portfolio_mode,
        "portfolio_output_mode": output_mode,
    }
    if not bool(values.get("sanitize_inputs", True)):
        contract["sanitize_inputs"] = False
    if bool(values.get("amp_native_position_add", False)):
        contract["amp_native_position_add"] = True
    if bool(values.get("temporal_self_attention_fast_path", False)):
        contract["temporal_self_attention_fast_path"] = True
    if head_layers > 0:
        contract["head_hidden_dim"] = int(values["head_hidden_dim"])
    if portfolio_mode == "long_short":
        contract["center_long_short_logits"] = bool(values["center_long_short_logits"])
    if output_mode == "activation_l1":
        contract["portfolio_activation"] = normalize_portfolio_activation(
            config.trading.portfolio_activation
        )

    categorical_indices = _feature_indices_from_patterns(
        feature_names,
        values.get("categorical_feature_names", []),
    )
    contract["categorical_feature_indices"] = categorical_indices
    if categorical_indices:
        contract["categorical_feature_names"] = [
            str(feature_names[index]) for index in categorical_indices
        ]
        contract["categorical_embedding_dim"] = max(
            1,
            int(values["categorical_embedding_dim"]),
        )
        contract["categorical_embedding_cardinality"] = max(
            2,
            int(values["categorical_embedding_cardinality"]),
        )

    temporal_layers = max(0, int(values["temporal_layers"]))
    if mode == "full":
        # Full attention does not execute temporal blocks, but the constructor
        # still materializes them.  Their count/FFN width therefore determine
        # whether strict state_dict loading is possible.
        if temporal_layers > 0:
            contract["checkpoint_only_temporal_blocks"] = {
                "layers": temporal_layers,
                "ffn_mult": int(values["temporal_ffn_mult"]),
            }
    else:
        contract["temporal_layers"] = temporal_layers
        if temporal_layers > 0:
            contract.update(
                {
                    "temporal_heads": int(values["temporal_heads"]),
                    "temporal_ffn_mult": int(values["temporal_ffn_mult"]),
                    "rope_temporal": bool(values["rope_temporal"]),
                }
            )
            if bool(values["rope_temporal"]):
                contract["rope_base"] = float(values["rope_base"])
            if pooling == "last":
                contract["temporal_query_mode"] = (
                    TransformerBasePortfolioModel._normalize_temporal_query_mode(
                        str(values["temporal_query_mode"])
                    )
                )

    if mode == "full":
        layers = max(0, int(values["joint_layers"]))
        contract["joint_layers"] = layers
        if layers > 0:
            contract["joint_heads"] = int(values["joint_heads"])
            contract["joint_ffn_mult"] = int(values["joint_ffn_mult"])
    elif mode == "axial":
        layers = max(0, int(values["cross_layers"]))
        contract["cross_layers"] = layers
        if layers > 0:
            contract["cross_heads"] = int(values["cross_heads"])
            contract["cross_ffn_mult"] = int(values["cross_ffn_mult"])
    elif mode in {"latent", "latent_only"}:
        contract.update(
            {
                "latent_layers": max(1, int(values["latent_layers"])),
                "num_latent_factors": max(1, int(values["num_latent_factors"])),
                "market_layers": max(1, int(values["market_layers"])),
                "cross_heads": int(values["cross_heads"]),
                "cross_ffn_mult": int(values["cross_ffn_mult"]),
            }
        )
        if mode == "latent":
            contract["num_market_tokens"] = max(1, int(values["num_market_tokens"]))
    elif mode == "market_token":
        contract.update(
            {
                "num_market_tokens": max(1, int(values["num_market_tokens"])),
                "market_layers": max(1, int(values["market_layers"])),
                "cross_heads": int(values["cross_heads"]),
                "cross_ffn_mult": int(values["cross_ffn_mult"]),
            }
        )
    return contract


def _checkpoint_model_values(
    config: ExperimentConfig,
    active_model: Mapping[str, Any],
    feature_names: Sequence[str],
) -> dict[str, Any]:
    config_name = str(active_model["config_name"])
    values = dict(active_model["values"])
    if config_name == "transformer_base_portfolio":
        return _transformer_base_checkpoint_model_values(
            config,
            values,
            feature_names,
        )

    contract = {
        name: value
        for name, value in values.items()
        if name not in _SCHEMA_4_MODEL_RUNTIME_FIELDS
    }
    effective_mode = _effective_model_portfolio_mode(config, config_name, values)
    contract.pop("portfolio_mode", None)
    contract["portfolio_mode"] = effective_mode
    output_mode: str | None = None
    if "portfolio_output_mode" in contract:
        output_mode = normalize_portfolio_output_mode(
            str(contract["portfolio_output_mode"])
        )
        contract["portfolio_output_mode"] = output_mode
    if output_mode in {None, "activation_l1"}:
        contract["portfolio_activation"] = normalize_portfolio_activation(
            config.trading.portfolio_activation
        )
    return contract


def _checkpoint_manifest(
    panel: PanelData,
    config: ExperimentConfig,
    *,
    include_data_content: bool = True,
) -> dict[str, Any]:
    """Build a portable, content-addressed checkpoint compatibility manifest."""
    execution_mode = normalize_execution_mode(config.trading.execution_mode)
    effective_short_open = (
        panel.can_short_open_mask
        if panel.can_short_open_mask is not None
        else panel.can_sell_mask
    )
    effective_short_open_at_open = (
        panel.can_short_open_open_mask
        if panel.can_short_open_open_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    effective_force_cover = (
        panel.force_short_cover_mask
        if panel.force_short_cover_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    effective_force_exit = (
        panel.force_exit_mask
        if panel.force_exit_mask is not None
        else np.zeros_like(panel.tradable_mask, dtype=bool)
    )
    schema_2_force_exit_compatible = not bool(
        np.asarray(effective_force_exit, dtype=bool).any()
    )
    if include_data_content:
        fingerprint = partial(_panel_array_content_fingerprint, panel)
        panel_arrays = {
            "dates": fingerprint("dates", panel.dates),
            "features": fingerprint("features", panel.features),
            "returns_1d": fingerprint("returns_1d", panel.returns_1d),
            "tradable_mask": fingerprint("tradable_mask", panel.tradable_mask),
            "alive_mask": fingerprint("alive_mask", panel.alive_mask),
            "benchmark_returns": fingerprint(
                "benchmark_returns", panel.benchmark_returns
            ),
            "close_prices": fingerprint("close_prices", panel.close_prices),
            "daily_volumes": fingerprint("daily_volumes", panel.daily_volumes),
            "can_buy_mask": fingerprint("can_buy_mask", panel.can_buy_mask),
            "can_sell_mask": fingerprint("can_sell_mask", panel.can_sell_mask),
            "can_short_open_mask": fingerprint(
                "can_short_open_mask", effective_short_open
            ),
            "force_short_cover_mask": fingerprint(
                "force_short_cover_mask", effective_force_cover
            ),
            "force_exit_mask": fingerprint("force_exit_mask", effective_force_exit),
        }
        if execution_mode == "tw_day_trade":
            panel_arrays.update(
                {
                    "open_prices": fingerprint("open_prices", panel.open_prices),
                    "intraday_returns": fingerprint(
                        "intraday_returns", panel.intraday_returns
                    ),
                    "day_trade_eligible_mask": fingerprint(
                        "day_trade_eligible_mask", panel.day_trade_eligible_mask
                    ),
                    "day_trade_can_short_open_mask": fingerprint(
                        "day_trade_can_short_open_mask",
                        panel.day_trade_can_short_open_mask,
                    ),
                    "day_trade_can_buy_open_mask": fingerprint(
                        "day_trade_can_buy_open_mask",
                        panel.day_trade_can_buy_open_mask,
                    ),
                    "day_trade_can_sell_open_mask": fingerprint(
                        "day_trade_can_sell_open_mask",
                        panel.day_trade_can_sell_open_mask,
                    ),
                }
            )
        if execution_mode in {"tw_index_futures_day", "tw_index_derivatives_day"}:
            futures_market = panel.index_futures_day_session
            if futures_market is None:
                raise ValueError(
                    "tw_index_futures_day checkpoint manifest requires futures data"
                )
            if (
                futures_market.rolling_buy_hold_log_returns is None
                or futures_market.rolling_buy_hold_tradable_mask is None
                or futures_market.front_month_roll_mask is None
            ):
                raise ValueError(
                    "tw_index_futures_day checkpoint manifest requires futures "
                    "data contract v2 rolling benchmark arrays"
                )
            panel_arrays.update(
                {
                    "index_futures_dates": _array_content_fingerprint(
                        futures_market.dates
                    ),
                    "index_futures_contract_months": _array_content_fingerprint(
                        futures_market.contract_months
                    ),
                    "index_futures_open_prices": _array_content_fingerprint(
                        futures_market.open_prices
                    ),
                    "index_futures_close_prices": _array_content_fingerprint(
                        futures_market.close_prices
                    ),
                    "index_futures_volumes": _array_content_fingerprint(
                        futures_market.volumes
                    ),
                    "index_futures_log_returns": _array_content_fingerprint(
                        futures_market.log_returns
                    ),
                    "index_futures_tradable_mask": _array_content_fingerprint(
                        futures_market.tradable_mask
                    ),
                    "index_futures_rolling_buy_hold_log_returns": (
                        _array_content_fingerprint(
                            futures_market.rolling_buy_hold_log_returns
                        )
                    ),
                    "index_futures_rolling_buy_hold_tradable_mask": (
                        _array_content_fingerprint(
                            futures_market.rolling_buy_hold_tradable_mask
                        )
                    ),
                    "index_futures_front_month_roll_mask": (
                        _array_content_fingerprint(
                            futures_market.front_month_roll_mask
                        )
                    ),
                }
            )
            if execution_mode == "tw_index_derivatives_day":
                option_chain = panel.index_options_chain_day_session
                candidates = panel.index_derivatives_day_candidates
                if option_chain is None or candidates is None:
                    raise ValueError(
                        "tw_index_derivatives_day checkpoint manifest requires "
                        "full-chain option data and causal candidates"
                    )
                tenor_panel = futures_market.require_tenor_panel()
                panel_arrays.update(
                    {
                        "index_futures_tenor_contract_months": _array_content_fingerprint(
                            tenor_panel[0]
                        ),
                        "index_futures_tenor_open_close": _array_content_fingerprint(
                            np.stack((tenor_panel[1], tenor_panel[4]), axis=-1)
                        ),
                        "index_futures_tenor_tradable_mask": _array_content_fingerprint(
                            tenor_panel[7]
                        ),
                        "index_options_chain_offsets": _array_content_fingerprint(
                            option_chain.row_offsets
                        ),
                        "index_options_chain_slots": _array_content_fingerprint(
                            option_chain.slot_indices
                        ),
                        "index_options_chain_series": _array_content_fingerprint(
                            option_chain.option_series
                        ),
                        "index_options_chain_strikes": _array_content_fingerprint(
                            option_chain.strikes
                        ),
                        "index_options_chain_open_close": _array_content_fingerprint(
                            np.column_stack(
                                (option_chain.open_prices, option_chain.close_prices)
                            )
                        ),
                        "index_derivatives_futures_contract_months": _array_content_fingerprint(
                            candidates.futures_contract_months
                        ),
                        "index_derivatives_candidate_mask": _array_content_fingerprint(
                            candidates.candidate_mask()
                        ),
                        "index_derivatives_candidate_features": _array_content_fingerprint(
                            candidates.option_candidate_features
                        ),
                        "index_derivatives_simple_returns": _array_content_fingerprint(
                            candidates.simple_returns()
                        ),
                        "index_derivatives_option_sparse_indices": _array_content_fingerprint(
                            candidates.option_sparse_indices
                        ),
                    }
                )
        if execution_mode in TW_CARRYING_EXECUTION_MODES:
            uses_full_avoidance = (
                config.trading.tw_corporate_action_mode == "avoid"
                and panel.corporate_action_avoidance_mask is not None
            )
            action_mask_name = (
                "corporate_action_avoidance_mask"
                if uses_full_avoidance
                else "unresolved_corporate_action_mask"
            )
            action_mask = (
                panel.corporate_action_avoidance_mask
                if uses_full_avoidance
                else panel.unresolved_corporate_action_mask
            )
            panel_arrays.update(
                {
                    "can_short_open_open_mask": fingerprint(
                        "can_short_open_open_mask",
                        effective_short_open_at_open,
                    ),
                    "corporate_action_execution_mask": fingerprint(
                        action_mask_name,
                        action_mask,
                    ),
                    "short_margin_rate": fingerprint(
                        "short_margin_rate", panel.short_margin_rate
                    ),
                }
            )
            if config.trading.tw_corporate_action_mode == "exact":
                panel_arrays.update(
                    {
                        "cash_dividend_yield": fingerprint(
                            "cash_dividend_yield", panel.cash_dividend_yield
                        ),
                        "cash_dividend_payment_delay_sessions": (
                            fingerprint(
                                "cash_dividend_payment_delay_sessions",
                                panel.cash_dividend_payment_delay_sessions,
                            )
                        ),
                    }
                )
            if bool(config.trading.tw_short_capacity_limit_enabled):
                # Capacity changes optimizer trajectories only when the
                # explicit broker-inventory ceiling is active.
                panel_arrays["short_capacity_shares"] = fingerprint(
                    "short_capacity_shares", panel.short_capacity_shares
                )
        schema_2_panel_arrays = {
            name: value
            for name, value in panel_arrays.items()
            if name != "force_exit_mask"
        }
        schema_3_without_force_exit_panel_arrays = dict(schema_2_panel_arrays)
    else:
        panel_arrays = {"omitted_for_inference": True}
        schema_2_panel_arrays = dict(panel_arrays)
        schema_3_without_force_exit_panel_arrays = dict(panel_arrays)
    preprocessing_contract = {
        "benchmark_name": str(config.data.benchmark_name),
        "security_filter": str(config.data.security_filter),
        "usd_only_trading_pairs": bool(config.data.usd_only_trading_pairs),
        "tradable_mode": str(config.data.tradable_mode),
        "trading_volume_policy": str(config.data.trading_volume_policy),
        "use_tw_public_features": bool(config.data.use_tw_public_features),
        "use_tw_public_rules": bool(config.data.use_tw_public_rules),
        "tw_public_market_symbol": str(config.data.tw_public_market_symbol),
        "feature_include": list(config.data.feature_include),
        "feature_exclude": list(config.data.feature_exclude),
    }
    if config.data.feature_zero_fill:
        preprocessing_contract["feature_zero_fill"] = list(
            config.data.feature_zero_fill
        )
    if config.data.feature_shift_next_session:
        preprocessing_contract["feature_shift_next_session"] = list(
            config.data.feature_shift_next_session
        )
    if config.data.allow_same_close_feature_approximation:
        preprocessing_contract["allow_same_close_feature_approximation"] = True
    schema_2_preprocessing_contract = {
        name: value
        for name, value in preprocessing_contract.items()
        if name != "use_tw_public_rules"
    }
    data_schema_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "preprocessing": preprocessing_contract,
    }
    data_contract = {
        **data_schema_contract,
        "panel_arrays": panel_arrays,
    }
    schema_2_data_schema_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "preprocessing": schema_2_preprocessing_contract,
    }
    schema_2_data_contract = {
        **schema_2_data_schema_contract,
        "panel_arrays": schema_2_panel_arrays,
    }
    schema_3_without_force_exit_data_contract = {
        **data_schema_contract,
        "panel_arrays": schema_3_without_force_exit_panel_arrays,
    }
    active_model = _active_model_config(config)
    symbols = [str(symbol) for symbol in panel.symbols]
    feature_names = [str(name) for name in panel.feature_names]
    model_contract = {
        "model_name": active_model.get("contract_name", active_model["config_name"]),
        "model": _checkpoint_model_values(config, active_model, feature_names),
        "lookback": int(config.training.lookback),
        "num_symbols": int(panel.num_symbols),
        "num_features": int(len(panel.feature_names)),
        "symbols": symbols,
        "feature_names": feature_names,
    }
    schema_3_model_contract = {
        "model_name": active_model.get("contract_name", active_model["config_name"]),
        "model": _schema_3_checkpoint_model_values(active_model),
        "lookback": int(config.training.lookback),
        "num_symbols": int(panel.num_symbols),
        "num_features": int(len(panel.feature_names)),
        "symbols": symbols,
        "feature_names": feature_names,
    }
    walk_forward_contract = asdict(config.walk_forward)
    pre_lookback_context_walk_forward_contract = dict(walk_forward_contract)
    # Checkpoint schemas 1-4 existed before cross-split feature context was a
    # configurable contract. Their only honest interpretation is today's
    # split_only + no context-only panel years default.
    pre_lookback_context_walk_forward_contract.pop("lookback_context", None)
    pre_lookback_context_walk_forward_contract.pop("split_start_year", None)
    uses_historical_lookback_defaults = (
        normalize_lookback_context(config.walk_forward.lookback_context) == "split_only"
        and config.walk_forward.split_start_year is None
    )
    historical_walk_forward_contract = (
        pre_lookback_context_walk_forward_contract
        if uses_historical_lookback_defaults
        else walk_forward_contract
    )
    contracts = {
        "data": data_contract,
        "model": model_contract,
        "training": _training_checkpoint_contract(config),
        "evaluation": asdict(config.evaluation),
        "trading": _trading_checkpoint_contract(config),
        "walk_forward": walk_forward_contract,
    }
    fingerprints = {
        name: _stable_fingerprint(contract) for name, contract in contracts.items()
    }
    fingerprints["data_schema"] = _stable_fingerprint(data_schema_contract)
    settings_fingerprint = _stable_fingerprint(
        {name: fingerprints[name] for name in contracts if name != "data"}
    )
    legacy_data_contract = {
        "symbols": [str(symbol) for symbol in panel.symbols],
        "feature_names": [str(name) for name in panel.feature_names],
        "lookback": int(config.training.lookback),
        "tradable_mode": str(config.data.tradable_mode),
        "feature_include": list(config.data.feature_include),
        "feature_exclude": list(config.data.feature_exclude),
        "use_tw_public_features": bool(config.data.use_tw_public_features),
    }
    legacy_intervals = ("epoch", "step", "batch")
    legacy_setting_contracts = {
        interval: _legacy_checkpoint_setting_contract(
            config,
            active_model,
            legacy_lr_scheduler_interval=interval,
        )
        for interval in legacy_intervals
    }
    legacy_setting_contract = legacy_setting_contracts["epoch"]
    configuration_snapshot = _configuration_fingerprint_snapshot(config)

    schema_3_contracts = {
        "data": data_contract,
        "model": schema_3_model_contract,
        "training": _training_checkpoint_contract_schema_3(config),
        "evaluation": asdict(config.evaluation),
        "trading": _trading_checkpoint_contract_schema_3(config),
        "walk_forward": historical_walk_forward_contract,
    }
    schema_3_fingerprints = {
        name: _stable_fingerprint(contract)
        for name, contract in schema_3_contracts.items()
    }
    schema_3_fingerprints["data_schema"] = _stable_fingerprint(data_schema_contract)
    schema_3_interval_fingerprints: dict[str, dict[str, str]] = {}
    schema_3_interval_contracts: dict[str, dict[str, Any]] = {}
    for interval in legacy_intervals[1:]:
        interval_contracts = dict(schema_3_contracts)
        interval_contracts["training"] = _training_checkpoint_contract_schema_3(
            config,
            legacy_lr_scheduler_interval=interval,
        )
        interval_fingerprints = dict(schema_3_fingerprints)
        interval_fingerprints["training"] = _stable_fingerprint(
            interval_contracts["training"]
        )
        schema_3_interval_contracts[interval] = interval_contracts
        schema_3_interval_fingerprints[interval] = interval_fingerprints
    schema_3_without_force_exit_contracts = dict(schema_3_contracts)
    schema_3_without_force_exit_contracts["data"] = (
        schema_3_without_force_exit_data_contract
    )
    schema_3_without_force_exit_fingerprints = dict(schema_3_fingerprints)
    schema_3_without_force_exit_fingerprints["data"] = _stable_fingerprint(
        schema_3_without_force_exit_data_contract
    )

    schema_2_contracts = dict(schema_3_contracts)
    schema_2_contracts["data"] = schema_2_data_contract
    schema_2_contracts["training"] = _training_checkpoint_contract_schema_2(config)
    schema_2_contracts["trading"] = _trading_checkpoint_contract_schema_2(config)
    schema_2_fingerprints = dict(schema_3_fingerprints)
    schema_2_fingerprints["data"] = _stable_fingerprint(schema_2_data_contract)
    schema_2_fingerprints["data_schema"] = _stable_fingerprint(
        schema_2_data_schema_contract
    )
    schema_2_fingerprints["training"] = _stable_fingerprint(
        schema_2_contracts["training"]
    )
    schema_2_fingerprints["trading"] = _stable_fingerprint(
        schema_2_contracts["trading"]
    )
    schema_2_interval_fingerprints: dict[str, dict[str, str]] = {}
    schema_2_interval_contracts: dict[str, dict[str, Any]] = {}
    for interval in legacy_intervals[1:]:
        interval_contracts = dict(schema_2_contracts)
        interval_contracts["training"] = _training_checkpoint_contract_schema_2(
            config,
            legacy_lr_scheduler_interval=interval,
        )
        interval_fingerprints = dict(schema_2_fingerprints)
        interval_fingerprints["training"] = _stable_fingerprint(
            interval_contracts["training"]
        )
        schema_2_interval_contracts[interval] = interval_contracts
        schema_2_interval_fingerprints[interval] = interval_fingerprints
    schema_4_pre_lookback_context_fingerprints: dict[str, str] = {}
    if uses_historical_lookback_defaults:
        schema_4_pre_lookback_context_fingerprints = dict(fingerprints)
        schema_4_pre_lookback_context_fingerprints["walk_forward"] = (
            _stable_fingerprint(pre_lookback_context_walk_forward_contract)
        )
    return {
        "schema_version": 4,
        "contracts": contracts,
        "fingerprints": fingerprints,
        # Preserve the complete submitted configuration for audit/debugging.
        # Resume compatibility is checked through the semantic layers above so
        # machine-local runner/output paths do not make a portable checkpoint
        # unreadable on another host.
        "configuration": configuration_snapshot,
        "configuration_fingerprint": _stable_fingerprint(configuration_snapshot),
        "compatibility_fingerprints": {
            "schema_4_pre_lookback_context": schema_4_pre_lookback_context_fingerprints,
            "schema_3": schema_3_fingerprints,
            "schema_3_without_force_exit": schema_3_without_force_exit_fingerprints,
            "schema_3_lr_interval_step": schema_3_interval_fingerprints["step"],
            "schema_3_lr_interval_batch": schema_3_interval_fingerprints["batch"],
            "schema_2": schema_2_fingerprints,
            "schema_2_lr_interval_step": schema_2_interval_fingerprints["step"],
            "schema_2_lr_interval_batch": schema_2_interval_fingerprints["batch"],
        },
        "compatibility_contracts": {
            "schema_3": schema_3_contracts,
            "schema_3_without_force_exit": schema_3_without_force_exit_contracts,
            "schema_3_lr_interval_step": schema_3_interval_contracts["step"],
            "schema_3_lr_interval_batch": schema_3_interval_contracts["batch"],
            "schema_2": schema_2_contracts,
            "schema_2_lr_interval_step": schema_2_interval_contracts["step"],
            "schema_2_lr_interval_batch": schema_2_interval_contracts["batch"],
        },
        "compatibility_constraints": {
            "schema_1": {
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
                "execution_mode_is_naive": str(config.trading.execution_mode)
                == "naive",
            },
            "schema_2": {
                # Schema 2 predates terminal-exit events. It is safe to resume
                # only when the current panel would not exercise that new rule.
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
                "execution_mode_is_naive": str(config.trading.execution_mode)
                == "naive",
            },
            "schema_3": {
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
                "execution_mode_is_naive": str(config.trading.execution_mode)
                == "naive",
            },
            "schema_3_without_force_exit": {
                "force_exit_mask_is_empty": schema_2_force_exit_compatible,
                "amp_feature_cache_disabled": not bool(
                    config.training.cache_train_features_in_amp_dtype
                ),
                "execution_mode_is_naive": str(config.trading.execution_mode)
                == "naive",
            },
        },
        # Stable aliases keep old tooling readable while layered validation
        # uses the independently actionable layer fingerprints above.
        "data_fingerprint": fingerprints["data"],
        "settings_fingerprint": settings_fingerprint,
        "legacy_fingerprints": {
            "data_fingerprint": _stable_fingerprint(legacy_data_contract),
            "settings_fingerprint": _stable_fingerprint(legacy_setting_contract),
        },
        "legacy_settings_compatibility_fingerprints": [
            _stable_fingerprint(legacy_setting_contracts[interval])
            for interval in legacy_intervals
        ],
    }


def _validate_checkpoint_fold_contract(
    checkpoint: Mapping[str, Any],
    *,
    checkpoint_path: Path,
    expected_fold: WalkForwardFold | None,
    expected_train_years: Sequence[int] | None,
) -> None:
    if expected_fold is not None:
        fold_fields = {
            "fold_id": int(expected_fold.fold_id),
            "train_years": [int(year) for year in expected_fold.train_years],
            "val_years": [int(year) for year in expected_fold.val_years],
            "test_years": [int(year) for year in expected_fold.test_years],
        }
        fold_mismatches = [
            name for name, value in fold_fields.items() if checkpoint.get(name) != value
        ]
        if fold_mismatches:
            details = ", ".join(
                f"{name}: saved={checkpoint.get(name)} current={fold_fields[name]}"
                for name in fold_mismatches
            )
            raise RuntimeError(
                f"Checkpoint fold contract mismatch ({details}): {checkpoint_path}."
            )
    elif expected_train_years is not None:
        years = [int(year) for year in expected_train_years]
        if checkpoint.get("train_years") != years:
            raise RuntimeError(
                "Checkpoint training-year contract mismatch "
                f"(saved={checkpoint.get('train_years')} current={years}): {checkpoint_path}."
            )


def _validate_checkpoint_manifest(
    checkpoint: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    checkpoint_path: Path,
    scope: str = "resume",
    expected_fold: WalkForwardFold | None = None,
    expected_train_years: Sequence[int] | None = None,
) -> None:
    _validate_checkpoint_fold_contract(
        checkpoint,
        checkpoint_path=checkpoint_path,
        expected_fold=expected_fold,
        expected_train_years=expected_train_years,
    )
    normalized_scope = str(scope).strip().lower().replace("-", "_")
    scopes = {
        "resume": (
            "data",
            "model",
            "training",
            "evaluation",
            "trading",
            "walk_forward",
        ),
        "artifact": (
            "data",
            "model",
            "training",
            "evaluation",
            "trading",
            "walk_forward",
        ),
        # New dates and labels are expected at inference time. Preserve the
        # feature/universe and model semantics without hashing future rows.
        "inference": ("data_schema", "model"),
        "model": ("data_schema", "model"),
    }
    if normalized_scope not in scopes:
        raise ValueError(f"Unknown checkpoint validation scope: {scope!r}")

    actual = checkpoint.get("experiment_manifest")
    if actual is None:
        expected_execution_mode = normalize_execution_mode(
            expected.get("contracts", {})
            .get("trading", {})
            .get("execution_mode", "naive")
        )
        if (
            normalized_scope in {"resume", "artifact"}
            and expected_execution_mode != "naive"
        ):
            raise RuntimeError(
                "Legacy checkpoint without a semantic manifest predates the "
                f"{expected_execution_mode} execution contract and cannot safely "
                f"resume or regenerate canonical artifacts: {checkpoint_path}. "
                "Use it for inference only or start a fresh training run."
            )
        amp_feature_cache_enabled = bool(
            expected.get("contracts", {})
            .get("training", {})
            .get("precision", {})
            .get("cache_train_features_in_amp_dtype", False)
        )
        if normalized_scope in {"resume", "artifact"} and amp_feature_cache_enabled:
            raise RuntimeError(
                "Legacy checkpoint without a semantic manifest cannot safely resume "
                "with cache_train_features_in_amp_dtype=true because its feature-storage "
                f"precision was not fingerprinted: {checkpoint_path}. Start a fresh training run."
            )
        print(
            f"[checkpoint] legacy checkpoint has no fingerprint; loading compatibly: {checkpoint_path}"
        )
        return
    if not isinstance(actual, Mapping):
        raise RuntimeError(
            f"Checkpoint experiment_manifest must be a mapping, got {type(actual).__name__}: "
            f"{checkpoint_path}."
        )

    try:
        actual_schema = int(actual.get("schema_version", 1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Checkpoint manifest schema_version is invalid: {checkpoint_path}."
        ) from exc
    if actual_schema not in {1, 2, 3, 4}:
        raise RuntimeError(
            "Unsupported checkpoint manifest schema "
            f"{actual_schema}: {checkpoint_path}. This runtime supports schemas 1 through 4; "
            "use a runtime that understands the saved schema instead of guessing compatibility."
        )
    if actual_schema < 4 and normalized_scope in {"resume", "artifact"}:
        schema_constraints = expected.get("compatibility_constraints", {}).get(
            f"schema_{actual_schema}",
            {},
        )
        if not bool(schema_constraints.get("amp_feature_cache_disabled", True)):
            raise RuntimeError(
                f"Schema {actual_schema} checkpoint cannot safely resume with "
                "cache_train_features_in_amp_dtype=true because that immutable "
                f"feature-storage precision was not fingerprinted: {checkpoint_path}. "
                "Start a fresh training run."
            )
        if not bool(schema_constraints.get("execution_mode_is_naive", True)):
            raise RuntimeError(
                f"Schema {actual_schema} checkpoint predates trading.execution_mode "
                "and therefore can resume only under execution_mode=naive: "
                f"{checkpoint_path}. Load model weights for inference or start "
                "a fresh Taiwan-execution training run."
            )
    expected_for_validation: Mapping[str, Any] = expected
    legacy_expected: Mapping[str, Any] = {}
    if actual_schema < 2:
        schema_1_constraints = expected.get("compatibility_constraints", {}).get(
            "schema_1",
            {},
        )
        if normalized_scope in {"resume", "artifact"} and not bool(
            schema_1_constraints.get("force_exit_mask_is_empty", True)
        ):
            raise RuntimeError(
                "Schema 1 checkpoint cannot safely resume with a non-empty "
                f"force_exit_mask because that execution rule was not fingerprinted: {checkpoint_path}. "
                "Start a fresh training run so terminal exits are part of the checkpoint contract."
            )
        legacy_expected = expected.get("legacy_fingerprints", expected)
        legacy_settings_candidates = set(
            expected.get(
                "legacy_settings_compatibility_fingerprints",
                [legacy_expected.get("settings_fingerprint")],
            )
        )
        mismatches = []
        if actual.get("data_fingerprint") != legacy_expected.get("data_fingerprint"):
            mismatches.append("data_fingerprint")
        if actual.get("settings_fingerprint") not in legacy_settings_candidates:
            mismatches.append("settings_fingerprint")
    else:
        actual_fingerprints = actual.get("fingerprints", {})
        if not isinstance(actual_fingerprints, Mapping):
            raise RuntimeError(
                "Checkpoint manifest fingerprints must be a mapping: "
                f"{checkpoint_path}."
            )
        if actual_schema == 2:
            schema_2_constraints = expected.get("compatibility_constraints", {}).get(
                "schema_2", {}
            )
            if normalized_scope in {"resume", "artifact"} and not bool(
                schema_2_constraints.get("force_exit_mask_is_empty", True)
            ):
                raise RuntimeError(
                    "Schema 2 checkpoint cannot safely resume with a non-empty "
                    f"force_exit_mask because that execution rule was not fingerprinted: {checkpoint_path}. "
                    "Start a fresh training run so terminal exits are part of the checkpoint contract."
                )
            compatibility = expected.get("compatibility_fingerprints", {})
            expected_fingerprints = compatibility.get(
                "schema_2",
                expected.get("fingerprints", {}),
            )
            for key in (
                "schema_2_lr_interval_step",
                "schema_2_lr_interval_batch",
            ):
                candidate = compatibility.get(key, {})
                if actual_fingerprints.get("training") == candidate.get("training"):
                    expected_fingerprints = dict(expected_fingerprints)
                    expected_fingerprints["training"] = candidate["training"]
                    break
        elif actual_schema == 3:
            compatibility = expected.get("compatibility_fingerprints", {})
            schema_3_fingerprints = compatibility.get(
                "schema_3",
                expected.get("fingerprints", {}),
            )
            schema_3_without_force_exit = compatibility.get(
                "schema_3_without_force_exit",
                schema_3_fingerprints,
            )
            expected_fingerprints = dict(schema_3_fingerprints)
            if actual_fingerprints.get("data") == schema_3_without_force_exit.get(
                "data"
            ):
                constraints = expected.get("compatibility_constraints", {}).get(
                    "schema_3_without_force_exit",
                    {},
                )
                if normalized_scope in {"resume", "artifact"} and not bool(
                    constraints.get("force_exit_mask_is_empty", True)
                ):
                    raise RuntimeError(
                        "Schema 3 checkpoint without a force_exit_mask fingerprint "
                        "cannot safely resume with non-empty terminal-exit events: "
                        f"{checkpoint_path}. Start a fresh training run."
                    )
                expected_fingerprints["data"] = schema_3_without_force_exit["data"]
            # The removed lr_scheduler_interval never affected runtime cadence.
            # Accept every spelling emitted by schema 3 while keeping all other
            # historical training fields exact.
            for key in (
                "schema_3_lr_interval_step",
                "schema_3_lr_interval_batch",
            ):
                candidate = compatibility.get(key, {})
                if actual_fingerprints.get("training") == candidate.get("training"):
                    expected_fingerprints["training"] = candidate["training"]
                    break
        else:
            expected_fingerprints = expected.get("fingerprints", {})
            compatibility = expected.get("compatibility_fingerprints", {})
            pre_context = compatibility.get(
                "schema_4_pre_lookback_context",
                {},
            )
            if actual_fingerprints.get("walk_forward") == pre_context.get(
                "walk_forward"
            ):
                expected_fingerprints = dict(expected_fingerprints)
                expected_fingerprints["walk_forward"] = pre_context["walk_forward"]
        expected_for_validation = {"fingerprints": expected_fingerprints}
        mismatches = [
            name
            for name in scopes[normalized_scope]
            if actual_fingerprints.get(name) != expected_fingerprints.get(name)
        ]
    if mismatches:
        details = ", ".join(
            f"{name}: saved={actual.get('fingerprints', {}).get(name, actual.get(name))} "
            f"current={legacy_expected.get(name) if actual_schema < 2 else expected_for_validation.get('fingerprints', {}).get(name)}"
            for name in mismatches
        )
        raise RuntimeError(
            f"Checkpoint semantic fingerprint mismatch ({details}): {checkpoint_path}. "
            "Use a matching config/data schema or start a fresh output directory."
        )


def _state_dict_symbol_count(state_dict: Mapping[str, Any]) -> int | None:
    for key, value in state_dict.items():
        if (
            str(key).endswith("symbol_position")
            and torch.is_tensor(value)
            and value.ndim == 4
        ):
            return int(value.size(2))
    return None


def _symbols_from_weight_table(path: Path) -> list[str]:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        import pyarrow.parquet as pq

        columns = pq.read_schema(path).names
    elif suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            columns = next(csv.reader(handle))
    else:
        raise ValueError(
            f"Unsupported daily weight table format {path.suffix!r}: {path}"
        )
    return [str(column) for column in columns if str(column) != "date"]


def _weight_table_symbols(fold_dir: Path) -> tuple[list[str], Path] | tuple[None, None]:
    path = weight_table_path(fold_dir)
    if path is None:
        return None, None
    return _symbols_from_weight_table(path), path


def _subset_panel_symbols(
    panel: PanelData,
    symbols: Sequence[str],
    *,
    allow_missing_masked: bool = False,
) -> PanelData:
    index_by_symbol = {str(symbol): idx for idx, symbol in enumerate(panel.symbols)}
    missing = [str(symbol) for symbol in symbols if str(symbol) not in index_by_symbol]
    if missing and not allow_missing_masked:
        preview = ", ".join(missing[:10])
        raise ValueError(
            f"Cannot align panel to checkpoint universe; {len(missing)} trained symbols are missing "
            f"from the current panel: {preview}"
        )
    if missing:
        print(
            f"[inference] restoring {len(missing)} checkpoint-only symbols "
            "as permanently masked slots"
        )
    indices = np.asarray(
        [index_by_symbol.get(str(symbol), -1) for symbol in symbols],
        dtype=np.int64,
    )
    present = indices >= 0
    shape_2d = (panel.num_dates, len(symbols))

    def aligned_3d(values: np.ndarray, fill: int | float | bool) -> np.ndarray:
        out = np.full((*shape_2d, values.shape[2]), fill, dtype=values.dtype)
        out[:, present, :] = values[:, indices[present], :]
        return out

    def aligned_2d(
        values: np.ndarray | None,
        fill: int | float | bool,
    ) -> np.ndarray | None:
        if values is None:
            return None
        out = np.full(shape_2d, fill, dtype=values.dtype)
        out[:, present] = values[:, indices[present]]
        return out

    return PanelData(
        dates=panel.dates,
        symbols=[str(symbol) for symbol in symbols],
        feature_names=list(panel.feature_names),
        features=aligned_3d(panel.features, 0.0),
        returns_1d=aligned_2d(panel.returns_1d, 0.0),
        tradable_mask=aligned_2d(panel.tradable_mask, False),
        alive_mask=aligned_2d(panel.alive_mask, False),
        benchmark_returns=panel.benchmark_returns,
        close_prices=aligned_2d(panel.close_prices, np.nan),
        can_buy_mask=aligned_2d(panel.can_buy_mask, False),
        can_sell_mask=aligned_2d(panel.can_sell_mask, False),
        can_short_open_mask=aligned_2d(panel.can_short_open_mask, False),
        can_short_open_open_mask=aligned_2d(panel.can_short_open_open_mask, False),
        force_short_cover_mask=aligned_2d(panel.force_short_cover_mask, False),
        force_exit_mask=aligned_2d(panel.force_exit_mask, False),
        daily_volumes=aligned_2d(panel.daily_volumes, 0.0),
        open_prices=aligned_2d(panel.open_prices, np.nan),
        intraday_returns=aligned_2d(panel.intraday_returns, 0.0),
        day_trade_eligible_mask=aligned_2d(panel.day_trade_eligible_mask, False),
        day_trade_can_short_open_mask=aligned_2d(
            panel.day_trade_can_short_open_mask, False
        ),
        day_trade_can_buy_open_mask=aligned_2d(
            panel.day_trade_can_buy_open_mask, False
        ),
        day_trade_can_sell_open_mask=aligned_2d(
            panel.day_trade_can_sell_open_mask, False
        ),
        raw_close_returns_1d=aligned_2d(panel.raw_close_returns_1d, 0.0),
        corporate_action_avoidance_mask=aligned_2d(
            panel.corporate_action_avoidance_mask, False
        ),
        unresolved_corporate_action_mask=aligned_2d(
            panel.unresolved_corporate_action_mask, False
        ),
        cash_dividend_yield=aligned_2d(panel.cash_dividend_yield, 0.0),
        cash_dividend_payment_delay_sessions=aligned_2d(
            panel.cash_dividend_payment_delay_sessions, 0
        ),
        short_capacity_shares=aligned_2d(panel.short_capacity_shares, 0),
        short_margin_rate=aligned_2d(panel.short_margin_rate, np.nan),
        content_fingerprints=None,
        index_futures_day_session=panel.index_futures_day_session,
        index_futures_reference_product=panel.index_futures_reference_product,
        index_options_monthly_day_session=panel.index_options_monthly_day_session,
        index_options_weekly_day_session=panel.index_options_weekly_day_session,
        index_options_chain_day_session=panel.index_options_chain_day_session,
        index_derivatives_day_candidates=panel.index_derivatives_day_candidates,
        index_derivatives_candidate_features=panel.index_derivatives_candidate_features,
        index_derivatives_candidate_mask=panel.index_derivatives_candidate_mask,
        index_derivatives_simple_returns=panel.index_derivatives_simple_returns,
    )


def _align_panel_to_state_dict_universe(
    panel: PanelData,
    fold_dir: Path,
    state_dict: Mapping[str, Any],
    *,
    checkpoint_symbols: Sequence[str] | None = None,
    context: str = "inference",
    allow_missing_masked: bool = False,
) -> PanelData:
    if checkpoint_symbols is not None:
        trained_symbols = [str(symbol) for symbol in checkpoint_symbols]
        if len(trained_symbols) != len(set(trained_symbols)):
            raise ValueError("checkpoint manifest symbol universe contains duplicates")
        if trained_symbols == list(panel.symbols):
            return panel
        print(
            f"[{context}] aligning panel to checkpoint manifest universe: "
            f"current={panel.num_symbols}, checkpoint={len(trained_symbols)}"
        )
        return _subset_panel_symbols(
            panel,
            trained_symbols,
            allow_missing_masked=allow_missing_masked,
        )

    expected_symbols = _state_dict_symbol_count(state_dict)
    if expected_symbols is None:
        return panel

    trained_symbols, weights_path = _weight_table_symbols(fold_dir)
    if trained_symbols is None:
        if int(panel.num_symbols) != int(expected_symbols):
            raise ValueError(
                f"Checkpoint expects {expected_symbols} symbols but current panel has {panel.num_symbols}; "
                f"cannot align because daily_weights table is missing in {fold_dir}."
            )
        return panel

    # ``symbol_position`` can be a configured positional-capacity tensor rather
    # than a one-row-per-security universe manifest.  This is the case when a
    # full panel is trained/evaluated with symbol indices mapped into a bounded
    # position table.  The emitted weight table is the authoritative ordered
    # universe whenever it exists, so do not reject it merely because its width
    # differs from the positional capacity stored in the checkpoint.
    if list(trained_symbols) == list(panel.symbols):
        return panel

    trained_set = set(trained_symbols)
    extras = [str(symbol) for symbol in panel.symbols if str(symbol) not in trained_set]
    authoritative_symbols = len(trained_symbols)
    if int(panel.num_symbols) != authoritative_symbols or extras:
        preview = ", ".join(extras[:10])
        print(
            f"[{context}] aligning panel symbols to checkpoint universe from {weights_path}: "
            f"current={panel.num_symbols}, checkpoint={authoritative_symbols}, "
            f"removed={len(extras)}, positional_capacity={expected_symbols}"
            + (f" ({preview})" if preview else "")
        )
    else:
        print(
            f"[{context}] reordering panel symbols to match checkpoint universe from {weights_path}"
        )
    return _subset_panel_symbols(
        panel,
        trained_symbols,
        allow_missing_masked=allow_missing_masked,
    )


def _normalize_train_symbol_compaction(value: object) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    if normalized in {"", "0", "false", "no", "off", "none", "disabled"}:
        return "none"
    if normalized in {"1", "true", "yes", "on", "train_union", "union", "active_union"}:
        return "train_union"
    raise ValueError("training.train_symbol_compaction must be 'none' or 'train_union'")


def _normalize_train_symbol_compaction_bucket_size(value: object) -> int:
    if value is None:
        return 0
    bucket_size = int(value)
    if bucket_size < 0:
        raise ValueError("training.train_symbol_compaction_bucket_size must be >= 0")
    return bucket_size


def _training_loss_portfolio_activation(config: ExperimentConfig) -> str:
    activation = (
        str(getattr(config.training, "loss_portfolio_activation", "auto"))
        .strip()
        .lower()
        .replace("-", "_")
    )
    if activation in {"", "auto", "trading", "same", "same_as_trading"}:
        return str(config.trading.portfolio_activation)
    return activation


def _training_loss_min_trade_weight(config: ExperimentConfig) -> float:
    configured = getattr(config.training, "loss_min_trade_weight", None)
    if configured is None:
        return max(0.0, float(config.trading.min_trade_weight))
    return max(0.0, float(configured))


def build_checkpoint_manifest(
    panel: PanelData,
    config: ExperimentConfig,
    *,
    include_data_content: bool = True,
) -> dict[str, Any]:
    """Build the canonical semantic manifest persisted with checkpoints."""
    return _checkpoint_manifest(
        panel, config, include_data_content=include_data_content
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
    """Validate a saved manifest under resume, artifact, or model scope."""
    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=checkpoint_path,
        scope=scope,
        expected_fold=expected_fold,
        expected_train_years=expected_train_years,
    )


def align_panel_to_checkpoint_universe(
    panel: PanelData,
    fold_dir: Path,
    state_dict: Mapping[str, Any],
    *,
    checkpoint_symbols: Sequence[str] | None = None,
    context: str = "inference",
    allow_missing_masked: bool = False,
) -> PanelData:
    """Restore the exact ordered universe expected by a checkpoint."""
    return _align_panel_to_state_dict_universe(
        panel,
        fold_dir,
        state_dict,
        checkpoint_symbols=checkpoint_symbols,
        context=context,
        allow_missing_masked=allow_missing_masked,
    )


def subset_panel_symbols(
    panel: PanelData,
    symbols: Sequence[str],
    *,
    allow_missing_masked: bool = False,
) -> PanelData:
    """Project every symbol-indexed panel array through one ordered mapping."""
    return _subset_panel_symbols(
        panel, symbols, allow_missing_masked=allow_missing_masked
    )


def state_dict_symbol_count(state_dict: Mapping[str, Any]) -> int | None:
    return _state_dict_symbol_count(state_dict)


def weight_table_symbols(path: Path) -> list[str]:
    """Read the ordered symbol schema from a daily-weight artifact."""

    return _symbols_from_weight_table(path)


def weight_table_path(fold_dir: Path) -> Path | None:
    """Resolve the canonical daily-weight artifact for a fold."""

    for candidate in (
        fold_dir / "daily_weights.parquet",
        fold_dir / "daily_weights.csv",
    ):
        if candidate.is_file():
            return candidate
    return None


def checkpoint_manifest_symbols(
    checkpoint: Mapping[str, Any],
) -> list[str] | None:
    """Return the checkpoint's authoritative ordered symbol universe.

    Schema-aware validation happens separately, but alignment must occur before
    model validation can compare the current manifest. Fail closed when the
    saved model and data contracts disagree instead of guessing from a weight
    table or positional-capacity tensor.
    """

    manifest = checkpoint.get("experiment_manifest")
    if not isinstance(manifest, Mapping):
        return None
    contracts = manifest.get("contracts")
    if not isinstance(contracts, Mapping):
        return None

    candidates: list[tuple[str, list[str]]] = []
    for contract_name in ("model", "data"):
        contract = contracts.get(contract_name)
        if not isinstance(contract, Mapping) or "symbols" not in contract:
            continue
        raw_symbols = contract.get("symbols")
        if not isinstance(raw_symbols, Sequence) or isinstance(
            raw_symbols, (str, bytes, bytearray)
        ):
            raise ValueError(
                f"checkpoint manifest {contract_name}.symbols must be a sequence"
            )
        symbols = [str(symbol) for symbol in raw_symbols]
        if len(symbols) != len(set(symbols)):
            raise ValueError(
                f"checkpoint manifest {contract_name}.symbols contains duplicates"
            )
        candidates.append((contract_name, symbols))

    if not candidates:
        return None
    authoritative_name, authoritative = candidates[0]
    for contract_name, symbols in candidates[1:]:
        if symbols != authoritative:
            raise ValueError(
                "checkpoint manifest ordered symbol universes disagree: "
                f"{authoritative_name} != {contract_name}"
            )
    return authoritative


__all__ = [
    "align_panel_to_checkpoint_universe",
    "build_checkpoint_manifest",
    "checkpoint_manifest_symbols",
    "state_dict_symbol_count",
    "subset_panel_symbols",
    "validate_checkpoint_manifest",
    "weight_table_path",
    "weight_table_symbols",
]
