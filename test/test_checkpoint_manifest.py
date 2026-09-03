from __future__ import annotations

import copy
import json
import random
from pathlib import Path

import numpy as np
import pytest
import torch

import stockagent.training.trainer as trainer_module
from stockagent.backtest.simulator import CANONICAL_BACKTEST_CONTRACT_VERSION
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.explainability import load_model_from_checkpoint
from stockagent.training.checkpoint_contract import checkpoint_manifest_symbols
from stockagent.training.trainer import (
    _align_panel_to_state_dict_universe,
    _checkpoint_manifest,
    _atomic_torch_save,
    _capture_rng_state,
    _fold_dir,
    _latest_group_checkpoint,
    _load_checkpoint,
    _load_completed_fold_result,
    _load_state_dict,
    _save_tree_checkpoint_metadata,
    _restore_rng_state,
    _validate_checkpoint_effective_train_batch_size,
    _validate_checkpoint_manifest,
)


def test_checkpoint_alignment_treats_weight_table_as_authoritative_universe(tmp_path: Path) -> None:
    panel = _panel()
    fold_dir = tmp_path / "fold_00"
    fold_dir.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table(
            {
                "date": ["2024-01-02"],
                "1101": [0.1],
                "2330": [0.2],
                "0050": [0.3],
            }
        ),
        fold_dir / "daily_weights.parquet",
    )
    state_dict = {"symbol_position": torch.zeros((1, 1, 2, 8))}

    aligned = _align_panel_to_state_dict_universe(panel, fold_dir, state_dict)

    assert aligned is panel


def test_checkpoint_manifest_universe_overrides_symbol_position_capacity(
    tmp_path: Path,
) -> None:
    panel = _panel()
    fold_dir = tmp_path / "fold_00"
    fold_dir.mkdir()
    state_dict = {"symbol_position": torch.zeros((1, 1, 2, 8))}

    aligned = _align_panel_to_state_dict_universe(
        panel,
        fold_dir,
        state_dict,
        checkpoint_symbols=["1101", "2330", "0050"],
    )

    assert aligned is panel


def test_checkpoint_manifest_symbols_drive_order_without_weight_table(
    tmp_path: Path,
) -> None:
    panel = _panel()
    fold_dir = tmp_path / "fold_00"
    fold_dir.mkdir()
    state_dict = {"symbol_position": torch.zeros((1, 1, 8, 8))}
    checkpoint = {
        "experiment_manifest": {
            "contracts": {
                "model": {"symbols": ["2330", "1101"]},
                "data": {"symbols": ["2330", "1101"]},
            }
        }
    }

    symbols = checkpoint_manifest_symbols(checkpoint)
    aligned = _align_panel_to_state_dict_universe(
        panel,
        fold_dir,
        state_dict,
        checkpoint_symbols=symbols,
    )

    assert symbols == ["2330", "1101"]
    assert aligned.symbols == ["2330", "1101"]
    assert np.array_equal(aligned.features[:, 0], panel.features[:, 1])
    assert np.array_equal(aligned.features[:, 1], panel.features[:, 0])


def test_checkpoint_manifest_symbol_contract_disagreement_fails_closed() -> None:
    checkpoint = {
        "experiment_manifest": {
            "contracts": {
                "model": {"symbols": ["1101", "2330"]},
                "data": {"symbols": ["2330", "1101"]},
            }
        }
    }

    with pytest.raises(ValueError, match="ordered symbol universes disagree"):
        checkpoint_manifest_symbols(checkpoint)


def test_live_alignment_restores_missing_checkpoint_symbols_as_masked_slots(
    tmp_path: Path,
) -> None:
    panel = _panel()
    fold_dir = tmp_path / "fold_00"
    fold_dir.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table(
            {
                "date": ["2024-01-02"],
                "1101": [0.1],
                "MISSING": [0.0],
                "2330": [0.2],
            }
        ),
        fold_dir / "daily_weights.parquet",
    )
    state_dict = {"symbol_position": torch.zeros((1, 1, 3, 8))}

    aligned = _align_panel_to_state_dict_universe(
        panel,
        fold_dir,
        state_dict,
        allow_missing_masked=True,
    )

    assert aligned.symbols == ["1101", "MISSING", "2330"]
    assert np.array_equal(aligned.features[:, 0], panel.features[:, 0])
    assert np.array_equal(aligned.features[:, 2], panel.features[:, 1])
    assert not aligned.tradable_mask[:, 1].any()
    assert not aligned.alive_mask[:, 1].any()
    assert np.isnan(aligned.close_prices[:, 1]).all()
    assert aligned.content_fingerprints is None


def _panel() -> PanelData:
    dates = np.arange(
        np.datetime64("2024-01-02"),
        np.datetime64("2024-01-08"),
        dtype="datetime64[D]",
    )
    rows, symbols, features = len(dates), 3, 2
    feature_values = np.arange(rows * symbols * features, dtype=np.float32).reshape(
        rows, symbols, features
    )
    returns = np.linspace(-0.03, 0.04, rows * symbols, dtype=np.float32).reshape(
        rows, symbols
    )
    tradable = np.ones((rows, symbols), dtype=bool)
    can_buy = tradable.copy()
    can_sell = tradable.copy()
    can_short_open = tradable.copy()
    force_short_cover = np.zeros_like(tradable)
    force_exit = np.zeros_like(tradable)
    return PanelData(
        dates=dates,
        symbols=["1101", "2330", "0050"],
        feature_names=["return_1d", "volume_z"],
        features=feature_values,
        returns_1d=returns,
        tradable_mask=tradable,
        alive_mask=tradable.copy(),
        benchmark_returns=np.linspace(-0.01, 0.01, rows, dtype=np.float32),
        close_prices=np.full((rows, symbols), 100.0, dtype=np.float32),
        daily_volumes=np.full((rows, symbols), 1_000.0, dtype=np.float32),
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        can_short_open_mask=can_short_open,
        force_short_cover_mask=force_short_cover,
        force_exit_mask=force_exit,
        short_capacity_shares=np.full(
            (rows, symbols), 1_000, dtype=np.int64
        ),
        short_margin_rate=np.full(
            (rows, symbols), 0.9, dtype=np.float32
        ),
    )


def _config():
    return load_config("configs/experiment_baseline.yaml")


def _day_trade_minute_panel() -> PanelData:
    panel = _panel()
    rows, symbols = panel.tradable_mask.shape
    panel.open_prices = np.full((rows, symbols), 99.0, dtype=np.float32)
    panel.intraday_returns = np.zeros((rows, symbols), dtype=np.float32)
    panel.day_trade_eligible_mask = panel.tradable_mask.copy()
    panel.day_trade_can_short_open_mask = panel.tradable_mask.copy()
    panel.day_trade_can_buy_open_mask = panel.tradable_mask.copy()
    panel.day_trade_can_sell_open_mask = panel.tradable_mask.copy()
    panel.day_trade_minute_execution = np.zeros(
        (rows, symbols, 23), dtype=np.float32
    )
    return panel


def _fold(*, val_year: int = 2025) -> WalkForwardFold:
    return WalkForwardFold(
        fold_id=7,
        train_indices=np.array([0, 1], dtype=np.int64),
        val_indices=np.array([2, 3], dtype=np.int64),
        test_indices=np.array([4, 5], dtype=np.int64),
        train_years=[2023, 2024],
        val_years=[val_year],
        test_years=[2026],
    )


@pytest.mark.parametrize(
    "field, mutate",
    [
        ("dates", lambda value: value.__setitem__(0, np.datetime64("2023-12-29"))),
        ("features", lambda value: value.__setitem__((1, 2, 0), 999.0)),
        ("returns_1d", lambda value: value.__setitem__((2, 1), -0.75)),
        ("tradable_mask", lambda value: value.__setitem__((3, 0), False)),
        ("can_buy_mask", lambda value: value.__setitem__((3, 1), False)),
        ("can_sell_mask", lambda value: value.__setitem__((3, 2), False)),
        ("can_short_open_mask", lambda value: value.__setitem__((4, 0), False)),
        ("force_short_cover_mask", lambda value: value.__setitem__((4, 1), True)),
        ("force_exit_mask", lambda value: value.__setitem__((4, 2), True)),
        ("daily_volumes", lambda value: value.__setitem__((5, 2), 5_000.0)),
    ],
)
def test_checkpoint_data_fingerprint_hashes_actual_panel_content(field, mutate) -> None:
    config = _config()
    baseline_panel = _panel()
    changed_panel = _panel()
    mutate(getattr(changed_panel, field))

    baseline = _checkpoint_manifest(baseline_panel, config)
    changed = _checkpoint_manifest(changed_panel, config)

    assert baseline["fingerprints"]["data"] != changed["fingerprints"]["data"]


def test_strict_minute_tape_content_owns_resume_fingerprint() -> None:
    config = load_config("configs/markets/tw_day_trade_1m_strict_exact_2020.yaml")
    baseline_panel = _day_trade_minute_panel()
    changed_panel = copy.deepcopy(baseline_panel)
    changed_panel.day_trade_minute_execution[2, 1, 4] = 101.25

    baseline = _checkpoint_manifest(baseline_panel, config)
    changed = _checkpoint_manifest(changed_panel, config)

    assert baseline["fingerprints"]["data"] != changed["fingerprints"]["data"]
    assert (
        baseline["contracts"]["data"]["preprocessing"]
        ["day_trade_minute_execution_allow_daily_proxy"]
        is False
    )


def test_hybrid_minute_checkpoint_before_tape_hash_remains_compatible(
    tmp_path: Path,
) -> None:
    config = load_config("configs/markets/tw_day_trade_1m_realistic.yaml")
    current = _checkpoint_manifest(_day_trade_minute_panel(), config)
    historical = copy.deepcopy(current)
    historical_arrays = historical["contracts"]["data"]["panel_arrays"]
    historical_arrays.pop("day_trade_minute_execution")
    historical["fingerprints"]["data"] = trainer_module._stable_fingerprint(
        historical["contracts"]["data"]
    )

    _validate_checkpoint_manifest(
        {"experiment_manifest": historical},
        current,
        checkpoint_path=tmp_path / "hybrid_pre_tape_hash.pt",
        scope="resume",
    )


def test_strict_minute_checkpoint_cannot_omit_tape_hash(tmp_path: Path) -> None:
    config = load_config("configs/markets/tw_day_trade_1m_strict_exact_2020.yaml")
    current = _checkpoint_manifest(_day_trade_minute_panel(), config)
    historical = copy.deepcopy(current)
    historical_arrays = historical["contracts"]["data"]["panel_arrays"]
    historical_arrays.pop("day_trade_minute_execution")
    historical["fingerprints"]["data"] = trainer_module._stable_fingerprint(
        historical["contracts"]["data"]
    )

    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": historical},
            current,
            checkpoint_path=tmp_path / "strict_pre_tape_hash.pt",
            scope="resume",
        )


@pytest.mark.parametrize(
    "field, mutate",
    [
        (
            "short_capacity_shares",
            lambda value: value.__setitem__((2, 1), 7_000),
        ),
        (
            "short_margin_rate",
            lambda value: value.__setitem__((3, 2), 1.2),
        ),
    ],
)
def test_tw_cash_checkpoint_fingerprints_point_in_time_short_contract(
    field,
    mutate,
) -> None:
    config = _config()
    config.trading.execution_mode = "tw_cash"
    baseline_panel = _panel()
    changed_panel = _panel()
    mutate(getattr(changed_panel, field))

    baseline = _checkpoint_manifest(baseline_panel, config)
    changed = _checkpoint_manifest(changed_panel, config)

    panel_arrays = baseline["contracts"]["data"]["panel_arrays"]
    assert {"short_capacity_shares", "short_margin_rate"} <= set(panel_arrays)
    assert baseline["fingerprints"]["data"] != changed["fingerprints"]["data"]


def test_disabled_short_capacity_limit_is_semantic_and_omits_unused_data_hash() -> None:
    config = _config()
    config.trading.execution_mode = "tw_cash"
    config.trading.long_only = False
    config.trading.tw_short_capacity_limit_enabled = False
    baseline_panel = _panel()
    changed_panel = _panel()
    changed_panel.short_capacity_shares[2, 1] = 7_000

    baseline = _checkpoint_manifest(baseline_panel, config)
    changed_data = _checkpoint_manifest(changed_panel, config)
    taiwan = baseline["contracts"]["trading"]["taiwan_execution"]
    assert taiwan["short_capacity_limit_enabled"] is False
    assert (
        "short_capacity_shares"
        not in baseline["contracts"]["data"]["panel_arrays"]
    )
    assert baseline["fingerprints"]["data"] == changed_data["fingerprints"]["data"]

    enabled_config = copy.deepcopy(config)
    enabled_config.trading.tw_short_capacity_limit_enabled = True
    enabled = _checkpoint_manifest(baseline_panel, enabled_config)
    assert enabled["fingerprints"]["trading"] != baseline["fingerprints"]["trading"]


def test_checkpoint_manifest_blocks_semantic_training_changes() -> None:
    panel = _panel()
    baseline_config = _config()
    baseline = _checkpoint_manifest(panel, baseline_config)["fingerprints"]

    mutations = [
        ("training", lambda cfg: setattr(cfg.training, "seed", cfg.training.seed + 1)),
        (
            "training",
            lambda cfg: setattr(
                cfg.training,
                "batch_size_train",
                cfg.training.batch_size_train + 1,
            ),
        ),
        ("training", lambda cfg: setattr(cfg.training, "auto_batch_size", not cfg.training.auto_batch_size)),
        (
            "training",
            lambda cfg: setattr(
                cfg.training,
                "warm_start_from_previous_fold",
                not cfg.training.warm_start_from_previous_fold,
            ),
        ),
        (
            "training",
            lambda cfg: setattr(
                cfg.training,
                "pretrained_initialization_validation_guard",
                not cfg.training.pretrained_initialization_validation_guard,
            ),
        ),
        (
            "training",
            lambda cfg: setattr(
                cfg.training,
                "early_stopping_min_delta",
                cfg.training.early_stopping_min_delta + 0.001,
            ),
        ),
        (
            "training",
            lambda cfg: setattr(cfg.training, "learning_rate", cfg.training.learning_rate * 2),
        ),
        (
            "training",
            lambda cfg: setattr(cfg.training, "loss_portfolio_activation", "tanh"),
        ),
        (
            "training",
            lambda cfg: setattr(cfg.training, "loss_min_trade_weight", 0.001),
        ),
        (
            "training",
            lambda cfg: setattr(cfg.training.multitask_loss, "direction_weight", 0.987),
        ),
        ("training", lambda cfg: setattr(cfg.environment, "amp_dtype", "fp16")),
        ("evaluation", lambda cfg: setattr(cfg.evaluation, "gamma_cvar", 0.987)),
        ("trading", lambda cfg: setattr(cfg.trading, "sell_fee_rate", 0.0123)),
        ("walk_forward", lambda cfg: setattr(cfg.walk_forward, "val_years", 2)),
        (
            "walk_forward",
                lambda cfg: setattr(
                    cfg.walk_forward,
                    "lookback_context",
                    "split_only",
                ),
        ),
        (
            "walk_forward",
            lambda cfg: setattr(cfg.walk_forward, "split_start_year", 2020),
        ),
        (
            "model",
            lambda cfg: setattr(
                cfg.training.transformer_base_portfolio,
                "d_model",
                cfg.training.transformer_base_portfolio.d_model + 8,
            ),
        ),
        (
            "model",
            lambda cfg: setattr(
                cfg.training.transformer_base_portfolio,
                "market_layers",
                cfg.training.transformer_base_portfolio.market_layers + 1,
            ),
        ),
    ]

    for expected_layer, mutate in mutations:
        changed_config = copy.deepcopy(baseline_config)
        mutate(changed_config)
        changed = _checkpoint_manifest(panel, changed_config)["fingerprints"]
        assert changed[expected_layer] != baseline[expected_layer]


def test_transformer_bottleneck_switches_are_semantic_and_legacy_compatible() -> None:
    panel = _panel()
    baseline_config = _config()
    baseline = _checkpoint_manifest(panel, baseline_config)
    baseline_model = baseline["contracts"]["model"]["model"]
    assert baseline_model["attention_mode"] == "market_token"

    explicit_legacy_config = copy.deepcopy(baseline_config)
    explicit_legacy_config.training.transformer_base_portfolio.use_latent_factors = False
    explicit_legacy_config.training.transformer_base_portfolio.use_market_tokens = True
    explicit_legacy = _checkpoint_manifest(panel, explicit_legacy_config)
    assert explicit_legacy["fingerprints"] == baseline["fingerprints"]
    assert (
        explicit_legacy["legacy_fingerprints"]
        == baseline["legacy_fingerprints"]
    )

    cross_preset_config = copy.deepcopy(baseline_config)
    cross_preset_config.training.transformer_base_portfolio.attention_mode = "latent"
    cross_preset_config.training.transformer_base_portfolio.use_latent_factors = False
    cross_preset_config.training.transformer_base_portfolio.use_market_tokens = True
    cross_preset = _checkpoint_manifest(panel, cross_preset_config)
    assert cross_preset["fingerprints"] == baseline["fingerprints"]
    assert (
        cross_preset["compatibility_fingerprints"]
        == baseline["compatibility_fingerprints"]
    )
    assert cross_preset["legacy_fingerprints"] == baseline["legacy_fingerprints"]

    latent_only_config = copy.deepcopy(baseline_config)
    latent_only_config.training.transformer_base_portfolio.use_latent_factors = True
    latent_only_config.training.transformer_base_portfolio.use_market_tokens = False
    latent_only = _checkpoint_manifest(panel, latent_only_config)
    latent_only_model = latent_only["contracts"]["model"]["model"]
    assert latent_only_model["attention_mode"] == "latent_only"
    assert "num_latent_factors" in latent_only_model
    assert "num_market_tokens" not in latent_only_model
    assert latent_only["fingerprints"]["model"] != baseline["fingerprints"]["model"]
    assert latent_only["fingerprints"]["training"] == baseline["fingerprints"]["training"]

    changed_inactive_market_count = copy.deepcopy(latent_only_config)
    changed_inactive_market_count.training.transformer_base_portfolio.num_market_tokens += 3
    changed_inactive = _checkpoint_manifest(panel, changed_inactive_market_count)
    assert (
        changed_inactive["fingerprints"]["model"]
        == latent_only["fingerprints"]["model"]
    )

    schema_3_default = baseline["compatibility_contracts"]["schema_3"]
    assert "use_latent_factors" not in schema_3_default["model"]["model"]
    assert "use_market_tokens" not in schema_3_default["model"]["model"]
    assert (
        "use_latent_factors"
        not in schema_3_default["training"]["transformer_base_portfolio"]
    )
    schema_3_latent_only = latent_only["compatibility_contracts"]["schema_3"]
    assert schema_3_latent_only["model"]["model"]["attention_mode"] == "latent_only"
    assert "use_latent_factors" not in schema_3_latent_only["model"]["model"]
    assert "use_market_tokens" not in schema_3_latent_only["model"]["model"]


def test_inactive_transformer_switches_do_not_poison_legacy_training_contract() -> None:
    panel = _panel()
    baseline_config = _config()
    baseline_config.training.model_name = "mlp"
    baseline = _checkpoint_manifest(panel, baseline_config)

    changed_config = copy.deepcopy(baseline_config)
    changed_config.training.transformer_base_portfolio.use_latent_factors = True
    changed_config.training.transformer_base_portfolio.use_market_tokens = False
    changed = _checkpoint_manifest(panel, changed_config)

    assert changed["fingerprints"] == baseline["fingerprints"]
    assert (
        changed["compatibility_fingerprints"]["schema_3"]["training"]
        == baseline["compatibility_fingerprints"]["schema_3"]["training"]
    )
    assert changed["legacy_fingerprints"] == baseline["legacy_fingerprints"]


def test_amp_feature_cache_checkpoint_contract_is_default_compatible_and_opt_in_strict(
    tmp_path: Path,
) -> None:
    panel = _panel()
    default_config = _config()
    assert default_config.training.cache_train_features_in_amp_dtype is False
    default_manifest = _checkpoint_manifest(panel, default_config)
    default_precision = default_manifest["contracts"]["training"]["precision"]
    assert "cache_train_features_in_amp_dtype" not in default_precision

    opted_in_config = copy.deepcopy(default_config)
    opted_in_config.training.cache_train_features_in_amp_dtype = True
    opted_in_manifest = _checkpoint_manifest(panel, opted_in_config)
    assert (
        opted_in_manifest["contracts"]["training"]["precision"][
            "cache_train_features_in_amp_dtype"
        ]
        is True
    )
    assert (
        opted_in_manifest["fingerprints"]["training"]
        != default_manifest["fingerprints"]["training"]
    )

    checkpoint = {"experiment_manifest": default_manifest}
    with pytest.raises(RuntimeError, match="training"):
        _validate_checkpoint_manifest(
            checkpoint,
            opted_in_manifest,
            checkpoint_path=tmp_path / "pre_amp_feature_cache.pt",
            scope="resume",
        )


def test_checkpoint_manifest_records_but_does_not_block_execution_and_inactive_settings() -> None:
    panel = _panel()
    baseline_config = _config()
    baseline = _checkpoint_manifest(panel, baseline_config)
    mutations = [
        lambda cfg: setattr(cfg.training, "epochs", cfg.training.epochs + 1),
        lambda cfg: setattr(cfg.training, "non_blocking_transfer", not cfg.training.non_blocking_transfer),
        lambda cfg: setattr(cfg.training, "multi_gpu_strategy", "distributed_data_parallel"),
        lambda cfg: setattr(cfg.training, "ddp_bucket_cap_mb", cfg.training.ddp_bucket_cap_mb + 4),
        lambda cfg: setattr(cfg.training, "enable_torch_compile", not cfg.training.enable_torch_compile),
        lambda cfg: setattr(cfg.training, "compile_loss", not bool(cfg.training.compile_loss)),
        lambda cfg: setattr(
            cfg.training,
            "compile_model_dynamic_symbols",
            not cfg.training.compile_model_dynamic_symbols,
        ),
        lambda cfg: setattr(
            cfg.training,
            "compile_loss_dynamic_symbols",
            not cfg.training.compile_loss_dynamic_symbols,
        ),
        lambda cfg: setattr(
            cfg.training,
            "compile_eval_model",
            not cfg.training.compile_eval_model,
        ),
        lambda cfg: setattr(
            cfg.training,
            "tw_continuous_compile_chunk_rows",
            cfg.training.tw_continuous_compile_chunk_rows + 1,
        ),
        lambda cfg: setattr(cfg.training, "torchinductor_cache_dir", "/tmp/other-cache"),
        lambda cfg: setattr(cfg.training, "batch_size_eval", cfg.training.batch_size_eval + 1),
        lambda cfg: setattr(cfg.training, "eval_model_chunk_rows", 7),
        lambda cfg: setattr(cfg.training, "vram_budget_gb", cfg.training.vram_budget_gb + 1.0),
        lambda cfg: setattr(cfg.training, "record_epoch_curve", not cfg.training.record_epoch_curve),
        lambda cfg: setattr(
            cfg.training,
            "checkpoint_finite_check",
            not cfg.training.checkpoint_finite_check,
        ),
        lambda cfg: setattr(cfg.training, "explain_top_k", cfg.training.explain_top_k + 1),
        lambda cfg: setattr(cfg.training, "save_daily_weights_table", not cfg.training.save_daily_weights_table),
        lambda cfg: setattr(cfg.training, "postprocess_benchmark_max_rows", 13),
        lambda cfg: setattr(cfg.training, "lr_scheduler_gamma", 0.123),
        lambda cfg: setattr(cfg.training.multitask_loss, "rank_ic_weight", 0.987),
        lambda cfg: setattr(cfg.training.factor_generalization_loss, "factor_sharpe_weight", 0.987),
        lambda cfg: setattr(cfg.training.portfolio_autoencoder_loss, "lambda_latent", 0.987),
        lambda cfg: setattr(cfg.training.mlp, "hidden_dim", cfg.training.mlp.hidden_dim + 8),
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "return_aux_details",
            not cfg.training.transformer_base_portfolio.return_aux_details,
        ),
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "use_flash_attention",
            not cfg.training.transformer_base_portfolio.use_flash_attention,
        ),
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "max_full_tokens",
            cfg.training.transformer_base_portfolio.max_full_tokens + 1,
        ),
        # The baseline uses market_token attention, so these architecture knobs
        # belong to inactive modes and must not poison resume compatibility.
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "latent_layers",
            cfg.training.transformer_base_portfolio.latent_layers + 1,
        ),
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "joint_layers",
            cfg.training.transformer_base_portfolio.joint_layers + 1,
        ),
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "cross_layers",
            cfg.training.transformer_base_portfolio.cross_layers + 1,
        ),
        # No categorical pattern matches this test panel, so embedding width is inactive.
        lambda cfg: setattr(
            cfg.training.transformer_base_portfolio,
            "categorical_embedding_dim",
            cfg.training.transformer_base_portfolio.categorical_embedding_dim + 1,
        ),
        lambda cfg: setattr(cfg.trading, "reporting_leverage", cfg.trading.reporting_leverage + 1.0),
        lambda cfg: setattr(cfg.runner, "output_dir", "machine-local-output"),
    ]

    for mutate in mutations:
        changed_config = copy.deepcopy(baseline_config)
        mutate(changed_config)
        changed = _checkpoint_manifest(panel, changed_config)
        assert changed["fingerprints"] == baseline["fingerprints"]
        assert changed["configuration_fingerprint"] != baseline["configuration_fingerprint"]


@pytest.mark.parametrize("objective", ["pure_rank", "rank_ic"])
def test_rank_objectives_ignore_inactive_loss_portfolio_activation(objective: str) -> None:
    panel = _panel()
    config = _config()
    config.training.loss_type = objective
    baseline = _checkpoint_manifest(panel, config)

    changed_config = copy.deepcopy(config)
    changed_config.training.loss_portfolio_activation = "tanh"
    changed = _checkpoint_manifest(panel, changed_config)

    assert changed["fingerprints"] == baseline["fingerprints"]
    assert changed["configuration_fingerprint"] != baseline["configuration_fingerprint"]


@pytest.mark.parametrize("objective", ["pure_rank", "rank_ic"])
def test_rank_objectives_ignore_inactive_loss_min_trade_weight(objective: str) -> None:
    panel = _panel()
    config = _config()
    config.training.loss_type = objective
    baseline = _checkpoint_manifest(panel, config)

    changed_config = copy.deepcopy(config)
    changed_config.training.loss_min_trade_weight = 0.001
    changed = _checkpoint_manifest(panel, changed_config)

    assert changed["fingerprints"] == baseline["fingerprints"]
    assert changed["configuration_fingerprint"] != baseline["configuration_fingerprint"]


@pytest.mark.parametrize(
    ("model_name", "active_config_name"),
    [("lightgbm", "lightgbm"), ("xgboost", "xgboost")],
)
def test_tree_training_contract_excludes_neural_executor_settings(
    model_name: str,
    active_config_name: str,
) -> None:
    panel = _panel()
    config = _config()
    config.training.model_name = model_name
    baseline = _checkpoint_manifest(panel, config)

    neural_only_mutations = [
        lambda cfg: setattr(cfg.training, "seed", cfg.training.seed + 1),
        lambda cfg: setattr(cfg.training, "batch_size_train", cfg.training.batch_size_train + 1),
        lambda cfg: setattr(cfg.training, "min_batch_size", cfg.training.min_batch_size + 1),
        lambda cfg: setattr(cfg.training, "auto_batch_size", not cfg.training.auto_batch_size),
        lambda cfg: setattr(cfg.training, "learning_rate", cfg.training.learning_rate * 2),
        lambda cfg: setattr(cfg.training, "weight_decay", cfg.training.weight_decay + 0.01),
        lambda cfg: setattr(cfg.training, "grad_clip_norm", cfg.training.grad_clip_norm + 1.0),
        lambda cfg: setattr(cfg.training, "enable_lr_scheduler", not cfg.training.enable_lr_scheduler),
        lambda cfg: setattr(
            cfg.training,
            "finite_check_interval_steps",
            cfg.training.finite_check_interval_steps + 1,
        ),
        lambda cfg: setattr(
            cfg.training,
            "checkpoint_finite_check",
            not cfg.training.checkpoint_finite_check,
        ),
        lambda cfg: setattr(
            cfg.training,
            "warm_start_from_previous_fold",
            not cfg.training.warm_start_from_previous_fold,
        ),
        lambda cfg: setattr(cfg.environment, "amp_dtype", "fp16"),
        lambda cfg: setattr(cfg.training, "loss_portfolio_activation", "tanh"),
        lambda cfg: setattr(cfg.training.multitask_loss, "direction_weight", 0.987),
        lambda cfg: setattr(
            cfg.training.factor_generalization_loss,
            "factor_sharpe_weight",
            0.987,
        ),
        lambda cfg: setattr(
            cfg.training.portfolio_autoencoder_loss,
            "lambda_latent",
            0.987,
        ),
    ]
    for mutate in neural_only_mutations:
        changed_config = copy.deepcopy(config)
        mutate(changed_config)
        changed = _checkpoint_manifest(panel, changed_config)
        assert changed["fingerprints"] == baseline["fingerprints"]
        assert changed["configuration_fingerprint"] != baseline["configuration_fingerprint"]

    changed_model_config = copy.deepcopy(config)
    active_config = getattr(changed_model_config.training, active_config_name)
    active_config.n_estimators += 1
    changed_model = _checkpoint_manifest(panel, changed_model_config)
    assert changed_model["fingerprints"]["training"] == baseline["fingerprints"]["training"]
    assert changed_model["fingerprints"]["model"] != baseline["fingerprints"]["model"]


def test_tree_training_contract_normalizes_objective_aliases() -> None:
    panel = _panel()
    config = _config()
    config.training.model_name = "lightgbm"
    config.training.loss_type = "log_utility"
    baseline = _checkpoint_manifest(panel, config)

    alias_config = copy.deepcopy(config)
    alias_config.training.loss_type = "kelly"
    alias = _checkpoint_manifest(panel, alias_config)

    assert alias["fingerprints"] == baseline["fingerprints"]
    assert alias["configuration_fingerprint"] != baseline["configuration_fingerprint"]


def test_checkpoint_manifest_uses_only_active_scheduler_and_objective_loss_blocks() -> None:
    panel = _panel()

    step_config = _config()
    step_config.training.enable_lr_scheduler = True
    step_config.training.lr_scheduler = "step"
    step_manifest = _checkpoint_manifest(panel, step_config)
    changed_step = copy.deepcopy(step_config)
    changed_step.training.lr_scheduler_gamma = 0.123
    assert (
        _checkpoint_manifest(panel, changed_step)["fingerprints"]["training"]
        != step_manifest["fingerprints"]["training"]
    )
    inactive_step = copy.deepcopy(step_config)
    inactive_step.training.lr_scheduler_eta_min *= 2
    assert (
        _checkpoint_manifest(panel, inactive_step)["fingerprints"]["training"]
        == step_manifest["fingerprints"]["training"]
    )

    factor_config = _config()
    factor_config.training.loss_type = "factor_generalization"
    factor_manifest = _checkpoint_manifest(panel, factor_config)
    changed_factor = copy.deepcopy(factor_config)
    changed_factor.training.factor_generalization_loss.factor_sharpe_weight += 0.5
    assert (
        _checkpoint_manifest(panel, changed_factor)["fingerprints"]["training"]
        != factor_manifest["fingerprints"]["training"]
    )
    inactive_factor = copy.deepcopy(factor_config)
    inactive_factor.training.portfolio_autoencoder_loss.lambda_latent += 0.5
    assert (
        _checkpoint_manifest(panel, inactive_factor)["fingerprints"]["training"]
        == factor_manifest["fingerprints"]["training"]
    )

    autoencoder_config = _config()
    autoencoder_config.training.loss_type = "portfolio_autoencoder"
    autoencoder_manifest = _checkpoint_manifest(panel, autoencoder_config)
    changed_autoencoder = copy.deepcopy(autoencoder_config)
    changed_autoencoder.training.portfolio_autoencoder_loss.lambda_latent += 0.5
    assert (
        _checkpoint_manifest(panel, changed_autoencoder)["fingerprints"]["training"]
        != autoencoder_manifest["fingerprints"]["training"]
    )


def test_checkpoint_manifest_records_complete_configuration_fingerprint() -> None:
    config = _config()
    manifest = _checkpoint_manifest(_panel(), config)
    assert manifest["configuration"]["training"]["epochs"] == config.training.epochs
    assert len(manifest["configuration_fingerprint"]) == 64
    changed = copy.deepcopy(config)
    changed.runner.output_dir = "a-different-machine-local-output"
    changed_manifest = _checkpoint_manifest(_panel(), changed)
    assert changed_manifest["configuration_fingerprint"] != manifest["configuration_fingerprint"]
    # Machine-local runner paths are recorded for audit, but do not alter the
    # semantic resume layers.
    assert changed_manifest["fingerprints"] == manifest["fingerprints"]


def test_prior_backtest_contract_cannot_resume_but_can_infer(
    tmp_path: Path,
) -> None:
    expected = _checkpoint_manifest(_panel(), _config())
    assert (
        expected["contracts"]["trading"]["canonical_backtest_contract_version"]
        == CANONICAL_BACKTEST_CONTRACT_VERSION
    )
    prior_contract = copy.deepcopy(expected)
    prior_contract["contracts"]["trading"]["canonical_backtest_contract_version"] = (
        CANONICAL_BACKTEST_CONTRACT_VERSION - 1
    )
    prior_contract["fingerprints"]["trading"] = trainer_module._stable_fingerprint(
        prior_contract["contracts"]["trading"]
    )
    checkpoint = {"experiment_manifest": prior_contract}

    with pytest.raises(RuntimeError, match="trading"):
        _validate_checkpoint_manifest(
            checkpoint,
            expected,
            checkpoint_path=tmp_path / "prior_backtest_contract.pt",
            scope="resume",
        )

    # Model weights remain structurally valid for inference; only replaying the
    # old optimizer trajectory under a different accounting rule is forbidden.
    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=tmp_path / "prior_backtest_contract.pt",
        scope="inference",
    )


def test_taiwan_settlement_gradient_horizon_is_checkpoint_semantic() -> None:
    panel = _panel()
    naive = _config()
    naive_changed = copy.deepcopy(naive)
    naive_changed.training.tw_continuous_gradient_horizon_rows += 8
    assert (
        _checkpoint_manifest(panel, naive)["fingerprints"]["training"]
        == _checkpoint_manifest(panel, naive_changed)["fingerprints"]["training"]
    )

    for execution_mode in ("tw_cash", "tw_day_trade", "tw_overnight"):
        taiwan = copy.deepcopy(naive)
        taiwan.trading.execution_mode = execution_mode
        taiwan_changed = copy.deepcopy(taiwan)
        taiwan_changed.training.tw_continuous_gradient_horizon_rows += 8
        assert (
            _checkpoint_manifest(panel, taiwan)["fingerprints"]["training"]
            != _checkpoint_manifest(panel, taiwan_changed)["fingerprints"]["training"]
        )


def test_taiwan_short_execution_settings_are_checkpoint_semantics() -> None:
    config = _config()
    config.trading.execution_mode = "tw_cash"
    baseline = _checkpoint_manifest(_panel(), config)
    taiwan = baseline["contracts"]["trading"]["taiwan_execution"]
    fields = {
        "short_initial_margin_rate": "tw_short_initial_margin_rate",
        "short_maintenance_ratio": "tw_short_maintenance_ratio",
        "short_lot_size": "tw_short_lot_size",
        "short_handling_fee_rate": "tw_short_handling_fee_rate",
    }
    assert set(fields).issubset(taiwan)

    for contract_name, config_name in fields.items():
        changed = copy.deepcopy(config)
        original = getattr(changed.trading, config_name)
        setattr(
            changed.trading,
            config_name,
            original + (1 if isinstance(original, int) else 0.01),
        )
        changed_manifest = _checkpoint_manifest(_panel(), changed)
        assert (
            changed_manifest["contracts"]["trading"]["taiwan_execution"][
                contract_name
            ]
            != taiwan[contract_name]
        )
        assert (
            changed_manifest["fingerprints"]["trading"]
            != baseline["fingerprints"]["trading"]
        )

    assert taiwan["corporate_action_mode"] == "avoid"
    exact = copy.deepcopy(config)
    exact.trading.tw_corporate_action_mode = "exact"
    exact_manifest = _checkpoint_manifest(_panel(), exact)
    assert exact_manifest["fingerprints"]["trading"] != baseline["fingerprints"]["trading"]

    longer_queue = copy.deepcopy(exact)
    longer_queue.trading.tw_corporate_action_claim_queue_sessions += 1
    longer_queue_manifest = _checkpoint_manifest(_panel(), longer_queue)
    assert (
        longer_queue_manifest["fingerprints"]["trading"]
        != exact_manifest["fingerprints"]["trading"]
    )


@pytest.mark.parametrize(
    ("config_name", "replacement"),
    [
        ("tw_commission_discount", 0.35),
        ("tw_commission_rebate_timing", "daily_close"),
    ],
)
def test_taiwan_commission_rebate_settings_are_checkpoint_semantics(
    config_name: str,
    replacement: object,
) -> None:
    config = _config()
    config.trading.execution_mode = "tw_cash"
    baseline = _checkpoint_manifest(_panel(), config)

    changed = copy.deepcopy(config)
    setattr(changed.trading, config_name, replacement)
    changed_manifest = _checkpoint_manifest(_panel(), changed)

    assert (
        changed_manifest["fingerprints"]["trading"]
        != baseline["fingerprints"]["trading"]
    )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
@pytest.mark.parametrize("scope", ["resume", "artifact"])
def test_manifestless_checkpoint_cannot_replay_taiwan_execution_contract(
    tmp_path: Path,
    execution_mode: str,
    scope: str,
) -> None:
    config = _config()
    config.trading.execution_mode = execution_mode
    expected = _checkpoint_manifest(_panel(), config)

    with pytest.raises(RuntimeError, match="without a semantic manifest"):
        _validate_checkpoint_manifest(
            {},
            expected,
            checkpoint_path=tmp_path / "manifestless.pt",
            scope=scope,
        )

    # Structural weight loading remains allowed; only semantic trajectory and
    # canonical artifact replay are unknowable.
    _validate_checkpoint_manifest(
        {},
        expected,
        checkpoint_path=tmp_path / "manifestless.pt",
        scope="inference",
    )


def test_checkpoint_rng_state_restores_python_numpy_and_torch_streams() -> None:
    random.seed(901)
    np.random.seed(902)
    torch.manual_seed(903)
    state = _capture_rng_state()

    expected = (
        random.random(),
        np.random.random(4),
        torch.rand(4),
    )
    # Consume and perturb all streams before restoring.
    random.seed(1)
    np.random.seed(2)
    torch.manual_seed(3)
    assert _restore_rng_state(state) is True
    actual = (
        random.random(),
        np.random.random(4),
        torch.rand(4),
    )

    assert actual[0] == expected[0]
    np.testing.assert_array_equal(actual[1], expected[1])
    assert torch.equal(actual[2], expected[2])


def test_checkpoint_rng_capture_uses_only_current_cuda_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int] = []
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 3)
    monkeypatch.setattr(
        torch.cuda,
        "get_rng_state",
        lambda device: calls.append(int(device)) or torch.tensor([1, 2, 3], dtype=torch.uint8),
    )
    monkeypatch.setattr(
        torch.cuda,
        "get_rng_state_all",
        lambda: (_ for _ in ()).throw(AssertionError("must not capture foreign CUDA streams")),
    )

    state = _capture_rng_state()

    assert state["schema_version"] == 3
    assert state["torch_cuda_device_index"] == 3
    assert state["torch_cuda_current"] == bytes([1, 2, 3])
    assert "torch_cuda" not in state
    assert calls == [3]


def test_checkpoint_rng_restore_accepts_legacy_all_device_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    random.seed(41)
    np.random.seed(42)
    torch.manual_seed(43)
    modern = _capture_rng_state()
    expected = (random.random(), np.random.random(), torch.rand(()))
    legacy = dict(modern)
    legacy["schema_version"] = 1
    legacy.pop("torch_cuda_current", None)
    legacy.pop("torch_cuda_device_index", None)
    legacy["torch_cuda"] = []

    random.seed(1)
    np.random.seed(2)
    torch.manual_seed(3)
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    assert _restore_rng_state(legacy) is True

    assert random.random() == expected[0]
    assert np.random.random() == expected[1]
    assert torch.equal(torch.rand(()), expected[2])


def test_checkpoint_rng_restore_maps_rank_local_stream_to_current_cuda_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _capture_rng_state()
    saved_stream = torch.tensor([9, 8, 7], dtype=torch.uint8)
    state["torch_cuda_current"] = saved_stream
    state["torch_cuda_device_index"] = 5
    restored: list[tuple[torch.Tensor, int]] = []
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 1)
    monkeypatch.setattr(
        torch.cuda,
        "set_rng_state",
        lambda value, device: restored.append((value.clone(), int(device))),
    )
    monkeypatch.setattr(
        torch.cuda,
        "set_rng_state_all",
        lambda value: (_ for _ in ()).throw(AssertionError("must not restore foreign CUDA streams")),
    )

    assert _restore_rng_state(state) is True
    assert len(restored) == 1
    assert torch.equal(restored[0][0], saved_stream)
    assert restored[0][1] == 1


def test_checkpoint_rng_world_expansion_preserves_existing_ranks_and_derives_new_ones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    random.seed(101)
    np.random.seed(102)
    torch.manual_seed(103)
    rank0_state = _capture_rng_state()
    rank0_expected = (random.random(), np.random.random(3), torch.rand(3))

    random.seed(201)
    np.random.seed(202)
    torch.manual_seed(203)
    rank1_state = _capture_rng_state()
    rank1_expected = (random.random(), np.random.random(3), torch.rand(3))
    wrapper = {
        "schema_version": 2,
        "world_size": 2,
        "by_rank": [rank0_state, rank1_state],
    }

    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 1)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 3)
    assert _restore_rng_state(wrapper) is True
    rank1_actual = (random.random(), np.random.random(3), torch.rand(3))
    assert rank1_actual[0] == rank1_expected[0]
    np.testing.assert_array_equal(rank1_actual[1], rank1_expected[1])
    assert torch.equal(rank1_actual[2], rank1_expected[2])

    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 2)
    assert _restore_rng_state(wrapper) is True
    derived_first = (random.random(), np.random.random(3), torch.rand(3))
    random.seed(1)
    np.random.seed(2)
    torch.manual_seed(3)
    assert _restore_rng_state(wrapper) is True
    derived_second = (random.random(), np.random.random(3), torch.rand(3))
    assert derived_first[0] == derived_second[0]
    np.testing.assert_array_equal(derived_first[1], derived_second[1])
    assert torch.equal(derived_first[2], derived_second[2])
    assert derived_first[0] != rank1_expected[0]
    assert not np.array_equal(derived_first[1], rank1_expected[1])
    assert not torch.equal(derived_first[2], rank1_expected[2])

    # Shrinking keeps the first saved ranks byte-for-byte exact as well.
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 0)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 1)
    assert _restore_rng_state(wrapper) is True
    rank0_actual = (random.random(), np.random.random(3), torch.rand(3))
    assert rank0_actual[0] == rank0_expected[0]
    np.testing.assert_array_equal(rank0_actual[1], rank0_expected[1])
    assert torch.equal(rank0_actual[2], rank0_expected[2])


def test_checkpoint_rng_single_rank_payload_derives_new_ddp_rank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    random.seed(301)
    np.random.seed(302)
    torch.manual_seed(303)
    state = _capture_rng_state()
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    monkeypatch.setattr(trainer_module, "_distributed_rank", lambda: 1)
    monkeypatch.setattr(trainer_module, "_distributed_world_size", lambda: 2)

    assert _restore_rng_state(state) is True
    first = (random.random(), np.random.random(), torch.rand(()))
    assert _restore_rng_state(state) is True
    second = (random.random(), np.random.random(), torch.rand(()))
    assert first[0] == second[0]
    assert first[1] == second[1]
    assert torch.equal(first[2], second[2])


@pytest.mark.parametrize(
    "prefix",
    ["module._orig_mod.", "_orig_mod.module.", "module._orig_mod.module."],
)
def test_checkpoint_state_dict_load_strips_nested_ddp_compile_wrappers(prefix: str) -> None:
    source = torch.nn.Linear(3, 2)
    target = torch.nn.Linear(3, 2)
    wrapped_state = {f"{prefix}{key}": value.clone() for key, value in source.state_dict().items()}

    _load_state_dict(target, wrapped_state)

    for key, expected in source.state_dict().items():
        assert torch.equal(target.state_dict()[key], expected)


def test_inference_manifest_skips_expensive_content_hash_but_preserves_model_schema() -> None:
    panel = _panel()
    config = _config()
    full = _checkpoint_manifest(panel, config)
    inference = _checkpoint_manifest(panel, config, include_data_content=False)

    assert inference["contracts"]["data"]["panel_arrays"] == {"omitted_for_inference": True}
    assert inference["fingerprints"]["data_schema"] == full["fingerprints"]["data_schema"]
    assert inference["fingerprints"]["model"] == full["fingerprints"]["model"]


def test_disabled_daily_context_is_backward_compatible_but_enabled_branch_is_not() -> None:
    panel = _panel()
    config = load_config("configs/deployments/tw_day_trade.yaml")
    financial = config.training.financial_transformer
    assert financial.daily_context_layers == 0

    baseline = _checkpoint_manifest(panel, config)
    model_values = baseline["contracts"]["model"]["model"]
    assert "daily_context_layers" not in model_values
    assert "daily_context_lookback" not in model_values
    assert "daily_context_pooling" not in model_values

    disabled_changed = copy.deepcopy(config)
    disabled_changed.training.financial_transformer.daily_context_lookback = 17
    disabled_changed.training.financial_transformer.daily_context_pooling = "mean"
    disabled_manifest = _checkpoint_manifest(panel, disabled_changed)
    assert disabled_manifest["fingerprints"]["model"] == baseline["fingerprints"]["model"]

    enabled = copy.deepcopy(config)
    enabled.training.financial_transformer.daily_context_layers = 1
    enabled_manifest = _checkpoint_manifest(panel, enabled)
    assert enabled_manifest["fingerprints"]["model"] != baseline["fingerprints"]["model"]


def test_checkpoint_validation_scopes_resume_and_future_inference_data(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    saved_manifest = _checkpoint_manifest(panel, config)
    checkpoint = {
        "experiment_manifest": saved_manifest,
        "fold_id": 7,
        "train_years": [2023, 2024],
        "val_years": [2025],
        "test_years": [2026],
    }
    checkpoint_path = tmp_path / "checkpoint.pt"

    changed_config = copy.deepcopy(config)
    changed_config.training.learning_rate *= 2
    changed_manifest = _checkpoint_manifest(panel, changed_config)
    with pytest.raises(RuntimeError, match="training"):
        _validate_checkpoint_manifest(
            checkpoint,
            changed_manifest,
            checkpoint_path=checkpoint_path,
            scope="resume",
            expected_fold=_fold(),
        )
    _validate_checkpoint_manifest(
        checkpoint,
        changed_manifest,
        checkpoint_path=checkpoint_path,
        scope="inference",
        expected_fold=_fold(),
    )

    aux_config = copy.deepcopy(config)
    aux_config.training.transformer_base_portfolio.return_aux_details = not (
        aux_config.training.transformer_base_portfolio.return_aux_details
    )
    aux_manifest = _checkpoint_manifest(panel, aux_config)
    assert aux_manifest["configuration_fingerprint"] != saved_manifest["configuration_fingerprint"]
    assert aux_manifest["fingerprints"] == saved_manifest["fingerprints"]
    _validate_checkpoint_manifest(
        checkpoint,
        aux_manifest,
        checkpoint_path=checkpoint_path,
        scope="resume",
    )
    _validate_checkpoint_manifest(
        checkpoint,
        aux_manifest,
        checkpoint_path=checkpoint_path,
        scope="inference",
    )

    incompatible_model_config = copy.deepcopy(config)
    incompatible_model_config.training.transformer_base_portfolio.d_model += 8
    incompatible_model_manifest = _checkpoint_manifest(panel, incompatible_model_config)
    with pytest.raises(RuntimeError, match="model"):
        _validate_checkpoint_manifest(
            checkpoint,
            incompatible_model_manifest,
            checkpoint_path=checkpoint_path,
            scope="inference",
        )

    changed_panel = _panel()
    changed_panel.features[0, 0, 0] += 1.0
    future_manifest = _checkpoint_manifest(changed_panel, config)
    with pytest.raises(RuntimeError, match="data"):
        _validate_checkpoint_manifest(
            checkpoint,
            future_manifest,
            checkpoint_path=checkpoint_path,
            scope="resume",
        )
    _validate_checkpoint_manifest(
        checkpoint,
        future_manifest,
        checkpoint_path=checkpoint_path,
        scope="inference",
    )


def test_checkpoint_validation_rejects_wrong_fold_year_contract(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    checkpoint = {
        "experiment_manifest": _checkpoint_manifest(panel, config),
        "fold_id": 7,
        "train_years": [2023, 2024],
        "val_years": [2025],
        "test_years": [2026],
    }

    with pytest.raises(RuntimeError, match="fold contract"):
        _validate_checkpoint_manifest(
            checkpoint,
            checkpoint["experiment_manifest"],
            checkpoint_path=tmp_path / "checkpoint.pt",
            scope="resume",
            expected_fold=_fold(val_year=2024),
        )


def test_checkpoint_loader_safely_allows_local_posix_paths(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "checkpoint_best.pt"
    _atomic_torch_save(
        {
            "model_state_dict": {"weight": torch.ones(1)},
            "artifact_root": tmp_path,
        },
        checkpoint_path,
    )

    checkpoint = _load_checkpoint(checkpoint_path)

    assert checkpoint["artifact_root"] == tmp_path
    torch.testing.assert_close(
        checkpoint["model_state_dict"]["weight"],
        torch.ones(1),
    )


def test_explainability_rejects_checkpoint_feature_schema_mismatch(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    checkpoint_path = tmp_path / "checkpoint_best.pt"
    _atomic_torch_save(
        {
            "experiment_manifest": _checkpoint_manifest(panel, config),
            "model_state_dict": {},
        },
        checkpoint_path,
    )
    incompatible_panel = _panel()
    incompatible_panel.feature_names[0] = "different_feature"

    with pytest.raises(RuntimeError, match="data_schema|model"):
        load_model_from_checkpoint(
            config,
            incompatible_panel,
            checkpoint_path,
            torch.device("cpu"),
        )


def test_schema_v1_checkpoint_fingerprints_remain_loadable(tmp_path: Path) -> None:
    manifest = _checkpoint_manifest(_panel(), _config())
    checkpoint = {
        "experiment_manifest": {
            "schema_version": 1,
            **manifest["legacy_fingerprints"],
        }
    }

    _validate_checkpoint_manifest(
        checkpoint,
        manifest,
        checkpoint_path=tmp_path / "legacy_checkpoint.pt",
        scope="resume",
    )


def test_schema_v4_before_lookback_context_remains_compatible_at_defaults(
    tmp_path: Path,
) -> None:
    panel = _panel()
    config = _config()
    config.walk_forward.lookback_context = "split_only"
    assert config.walk_forward.lookback_context == "split_only"
    assert config.walk_forward.split_start_year is None
    current = _checkpoint_manifest(panel, config)
    historical = copy.deepcopy(current)
    historical["schema_version"] = 4
    historical["contracts"]["walk_forward"].pop("lookback_context")
    historical["contracts"]["walk_forward"].pop("split_start_year")
    historical["fingerprints"]["walk_forward"] = trainer_module._stable_fingerprint(
        historical["contracts"]["walk_forward"]
    )

    _validate_checkpoint_manifest(
        {"experiment_manifest": historical},
        current,
        checkpoint_path=tmp_path / "schema_v4_pre_lookback_context.pt",
        scope="resume",
    )


def test_schema_v4_before_lookback_context_cannot_resume_panel_history(
    tmp_path: Path,
) -> None:
    panel = _panel()
    config = _config()
    config.walk_forward.lookback_context = "panel_history"
    config.walk_forward.split_start_year = 2020
    current = _checkpoint_manifest(panel, config)
    historical = copy.deepcopy(current)
    historical["schema_version"] = 4
    historical["contracts"]["walk_forward"].pop("lookback_context")
    historical["contracts"]["walk_forward"].pop("split_start_year")
    historical["fingerprints"]["walk_forward"] = trainer_module._stable_fingerprint(
        historical["contracts"]["walk_forward"]
    )

    with pytest.raises(RuntimeError, match="walk_forward"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": historical},
            current,
            checkpoint_path=tmp_path / "schema_v4_pre_lookback_context.pt",
            scope="resume",
        )


def test_schema_v4_added_inactive_external_and_projection_fields_remain_compatible(
    tmp_path: Path,
) -> None:
    panel = _panel()
    config = _config()
    config.training.model_name = "financial_transformer"
    config.data.use_external_features = False
    active = (
        config.training.financial_transformer
        if config.training.model_name == "financial_transformer"
        else config.training.transformer_base_portfolio
    )
    active.projection_l1_scale_by_active_count = False
    current = _checkpoint_manifest(panel, config)
    historical = copy.deepcopy(current)
    preprocessing = historical["contracts"]["data"]["preprocessing"]
    for name in (
        "use_external_features",
        "external_feature_path",
        "external_market_symbol",
    ):
        preprocessing.pop(name)
    historical["contracts"]["model"]["model"].pop(
        "projection_l1_scale_by_active_count"
    )
    assert (
        "futures_denomination_aware_output"
        not in current["contracts"]["model"]["model"]
    )
    assert (
        "futures_current_open_feature"
        not in current["contracts"]["model"]["model"]
    )
    historical["fingerprints"]["data_schema"] = trainer_module._stable_fingerprint(
        {
            "symbols": historical["contracts"]["data"]["symbols"],
            "feature_names": historical["contracts"]["data"]["feature_names"],
            "preprocessing": preprocessing,
        }
    )
    historical["fingerprints"]["data"] = trainer_module._stable_fingerprint(
        historical["contracts"]["data"]
    )
    historical["fingerprints"]["model"] = trainer_module._stable_fingerprint(
        historical["contracts"]["model"]
    )

    _validate_checkpoint_manifest(
        {"experiment_manifest": historical},
        current,
        checkpoint_path=tmp_path / "schema_v4_before_inactive_fields.pt",
        scope="model",
    )
    _validate_checkpoint_manifest(
        {"experiment_manifest": historical},
        current,
        checkpoint_path=tmp_path / "schema_v4_before_inactive_fields.pt",
        scope="resume",
    )

    explicit_inactive_futures = copy.deepcopy(current)
    explicit_inactive_futures["contracts"]["model"]["model"].update(
        {
            "futures_denomination_aware_output": False,
            "futures_current_open_feature": False,
        }
    )
    explicit_inactive_futures["fingerprints"]["model"] = (
        trainer_module._stable_fingerprint(
            explicit_inactive_futures["contracts"]["model"]
        )
    )
    _validate_checkpoint_manifest(
        {"experiment_manifest": explicit_inactive_futures},
        current,
        checkpoint_path=tmp_path / "schema_v4_with_inactive_futures_fields.pt",
        scope="model",
    )

    external_enabled = copy.deepcopy(config)
    external_enabled.data.use_external_features = True
    with pytest.raises(RuntimeError, match="data_schema"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": historical},
            _checkpoint_manifest(panel, external_enabled),
            checkpoint_path=tmp_path / "external_enabled.pt",
            scope="model",
        )

    scaled_projection = copy.deepcopy(config)
    scaled_active = (
        scaled_projection.training.financial_transformer
        if scaled_projection.training.model_name == "financial_transformer"
        else scaled_projection.training.transformer_base_portfolio
    )
    scaled_active.projection_l1_scale_by_active_count = True
    with pytest.raises(RuntimeError, match="model"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": historical},
            _checkpoint_manifest(panel, scaled_projection),
            checkpoint_path=tmp_path / "projection_scaled.pt",
            scope="model",
        )


def test_schema_v4_inactive_causal_feature_normalizer_spelling_is_compatible(
    tmp_path: Path,
) -> None:
    panel = _panel()
    config = _config()
    config.training.model_name = "financial_transformer"
    active = config.training.financial_transformer
    active.causal_feature_rms_normalization = False
    active.causal_feature_min_active_dates = 1
    active.causal_feature_scale_epsilon = 1e-6
    current = _checkpoint_manifest(panel, config)
    assert (
        "causal_feature_rms_normalization"
        not in current["contracts"]["model"]["model"]
    )

    explicit_inactive = copy.deepcopy(current)
    explicit_inactive["contracts"]["model"]["model"].update(
        {
            "causal_feature_rms_normalization": False,
            "causal_feature_min_active_dates": 1,
            "causal_feature_scale_epsilon": 1e-6,
        }
    )
    explicit_inactive["fingerprints"]["model"] = (
        trainer_module._stable_fingerprint(
            explicit_inactive["contracts"]["model"]
        )
    )
    _validate_checkpoint_manifest(
        {"experiment_manifest": explicit_inactive},
        current,
        checkpoint_path=tmp_path / "inactive_causal_normalizer.pt",
        scope="model",
    )

    enabled = copy.deepcopy(config)
    enabled.training.financial_transformer.causal_feature_rms_normalization = True
    with pytest.raises(RuntimeError, match="model"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": current},
            _checkpoint_manifest(panel, enabled),
            checkpoint_path=tmp_path / "enabled_causal_normalizer.pt",
            scope="model",
        )


def test_schema_v1_removed_scheduler_interval_spellings_remain_loadable(tmp_path: Path) -> None:
    manifest = _checkpoint_manifest(_panel(), _config())
    for settings_fingerprint in manifest[
        "legacy_settings_compatibility_fingerprints"
    ]:
        checkpoint_manifest = {
            "schema_version": 1,
            **manifest["legacy_fingerprints"],
            "settings_fingerprint": settings_fingerprint,
        }
        _validate_checkpoint_manifest(
            {"experiment_manifest": checkpoint_manifest},
            manifest,
            checkpoint_path=tmp_path / "legacy_scheduler_interval.pt",
            scope="resume",
        )


def _historical_schema_manifest(
    source_manifest: dict,
    schema_version: int,
) -> dict:
    """Build the minimal payload emitted by that historical schema."""
    if schema_version == 1:
        return {
            "schema_version": 1,
            **copy.deepcopy(source_manifest["legacy_fingerprints"]),
        }
    compatibility_key = f"schema_{schema_version}"
    contracts = copy.deepcopy(
        source_manifest["compatibility_contracts"][compatibility_key]
    )
    fingerprints = copy.deepcopy(
        source_manifest["compatibility_fingerprints"][compatibility_key]
    )
    if schema_version == 3:
        # This flag was introduced in schema 4. Never synthesize it into a
        # schema-3 fixture merely because the current TrainingConfig has it.
        contracts["training"].pop("cache_train_features_in_amp_dtype", None)
        fingerprints["training"] = trainer_module._stable_fingerprint(
            contracts["training"]
        )
    assert "cache_train_features_in_amp_dtype" not in contracts["training"]
    return {
        "schema_version": schema_version,
        "contracts": contracts,
        "fingerprints": fingerprints,
    }


@pytest.mark.parametrize("schema_version", [1, 2, 3])
def test_historical_schema_cannot_resume_amp_feature_cache_opt_in(
    tmp_path: Path,
    schema_version: int,
) -> None:
    panel = _panel()
    historical_config = _config()
    assert historical_config.training.cache_train_features_in_amp_dtype is False
    historical = _historical_schema_manifest(
        _checkpoint_manifest(panel, historical_config),
        schema_version,
    )
    current_config = copy.deepcopy(historical_config)
    current_config.training.cache_train_features_in_amp_dtype = True
    current = _checkpoint_manifest(panel, current_config)
    checkpoint = {"experiment_manifest": historical}
    checkpoint_path = tmp_path / f"schema_{schema_version}_pre_amp_cache.pt"

    with pytest.raises(RuntimeError, match="cache_train_features_in_amp_dtype|fingerprint mismatch|training"):
        _validate_checkpoint_manifest(
            checkpoint,
            current,
            checkpoint_path=checkpoint_path,
            scope="resume",
        )

    # Weight loading and future inference do not replay the historical
    # optimizer trajectory or its immutable feature representation.
    for scope in ("inference", "model"):
        _validate_checkpoint_manifest(
            checkpoint,
            current,
            checkpoint_path=checkpoint_path,
            scope=scope,
        )


@pytest.mark.parametrize("schema_version", [1, 2, 3])
def test_historical_schema_remains_compatible_when_amp_feature_cache_is_default_off(
    tmp_path: Path,
    schema_version: int,
) -> None:
    panel = _panel()
    config = _config()
    assert config.training.cache_train_features_in_amp_dtype is False
    current = _checkpoint_manifest(panel, config)
    checkpoint = {
        "experiment_manifest": _historical_schema_manifest(current, schema_version)
    }

    for scope in ("resume", "inference", "model"):
        _validate_checkpoint_manifest(
            checkpoint,
            current,
            checkpoint_path=tmp_path / f"schema_{schema_version}_default_amp_cache.pt",
            scope=scope,
        )


@pytest.mark.parametrize("scope", ["resume", "artifact"])
def test_schema_v1_rejects_unfingerprinted_terminal_exit_rules(
    tmp_path: Path,
    scope: str,
) -> None:
    panel = _panel()
    baseline = _checkpoint_manifest(panel, _config())
    checkpoint = {
        "experiment_manifest": {
            "schema_version": 1,
            **baseline["legacy_fingerprints"],
        }
    }
    panel.force_exit_mask[0, 0] = True

    with pytest.raises(RuntimeError, match="Schema 1.*force_exit_mask"):
        _validate_checkpoint_manifest(
            checkpoint,
            _checkpoint_manifest(panel, _config()),
            checkpoint_path=tmp_path / "schema_v1.pt",
            scope=scope,
        )


def test_schema_v2_layered_checkpoint_remains_resume_compatible(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    expected = _checkpoint_manifest(panel, config)
    schema_v2 = copy.deepcopy(expected)
    schema_v2["schema_version"] = 2
    schema_v2["fingerprints"] = copy.deepcopy(
        expected["compatibility_fingerprints"]["schema_2"]
    )
    assert schema_v2["fingerprints"]["trading"] != expected["fingerprints"]["trading"]
    checkpoint = {
        "experiment_manifest": schema_v2,
        "train_years": [2023, 2024],
    }

    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=tmp_path / "schema_v2.pt",
        scope="resume",
        expected_train_years=[2023, 2024],
    )

    panel_with_terminal_exit = _panel()
    panel_with_terminal_exit.force_exit_mask[4, 2] = True
    with pytest.raises(RuntimeError, match="Schema 2.*force_exit_mask"):
        _validate_checkpoint_manifest(
            checkpoint,
            _checkpoint_manifest(panel_with_terminal_exit, config),
            checkpoint_path=tmp_path / "schema_v2.pt",
            scope="resume",
        )

    changed = copy.deepcopy(config)
    changed.training.learning_rate *= 2
    with pytest.raises(RuntimeError, match="training"):
        _validate_checkpoint_manifest(
            checkpoint,
            _checkpoint_manifest(panel, changed),
            checkpoint_path=tmp_path / "schema_v2.pt",
            scope="resume",
        )


def test_schema_v3_exact_contract_remains_resume_compatible(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    expected = _checkpoint_manifest(panel, config)
    schema_v3 = copy.deepcopy(expected)
    schema_v3["schema_version"] = 3
    schema_v3["fingerprints"] = copy.deepcopy(
        expected["compatibility_fingerprints"]["schema_3"]
    )
    schema_v3["contracts"] = copy.deepcopy(
        expected["compatibility_contracts"]["schema_3"]
    )
    checkpoint = {"experiment_manifest": schema_v3}

    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=tmp_path / "schema_v3.pt",
        scope="resume",
    )

    # Schema 4 deliberately lets users extend epochs, but an already-written
    # schema-3 checkpoint must still be checked against schema 3's exact contract.
    changed = copy.deepcopy(config)
    changed.training.epochs += 1
    with pytest.raises(RuntimeError, match="training"):
        _validate_checkpoint_manifest(
            checkpoint,
            _checkpoint_manifest(panel, changed),
            checkpoint_path=tmp_path / "schema_v3.pt",
            scope="resume",
        )


@pytest.mark.parametrize("schema_version", [1, 2, 3])
def test_pre_execution_mode_checkpoint_cannot_resume_taiwan_accounting(
    tmp_path: Path,
    schema_version: int,
) -> None:
    panel = _panel()
    naive = _checkpoint_manifest(panel, _config())
    historical = _historical_schema_manifest(naive, schema_version)

    if schema_version >= 2:
        historical_trading = historical["contracts"]["trading"]
        assert "execution_mode" not in historical_trading
        assert not any(str(name).startswith("tw_") for name in historical_trading)

    real_config = _config()
    real_config.trading.execution_mode = "tw_day_trade"
    expected_real = _checkpoint_manifest(panel, real_config)
    checkpoint = {"experiment_manifest": historical}

    with pytest.raises(RuntimeError, match="predates trading.execution_mode"):
        _validate_checkpoint_manifest(
            checkpoint,
            expected_real,
            checkpoint_path=tmp_path / f"schema_v{schema_version}.pt",
            scope="resume",
        )

    # Model structure remains usable for inference; only the unrecorded
    # optimizer/accounting trajectory is forbidden.
    _validate_checkpoint_manifest(
        checkpoint,
        expected_real,
        checkpoint_path=tmp_path / f"schema_v{schema_version}.pt",
        scope="inference",
    )


@pytest.mark.parametrize(
    "schema_version,compatibility_key",
    [
        (2, "schema_2_lr_interval_step"),
        (2, "schema_2_lr_interval_batch"),
        (3, "schema_3_lr_interval_step"),
        (3, "schema_3_lr_interval_batch"),
    ],
)
def test_removed_scheduler_interval_schema_variants_remain_loadable(
    tmp_path: Path,
    schema_version: int,
    compatibility_key: str,
) -> None:
    expected = _checkpoint_manifest(_panel(), _config())
    actual = copy.deepcopy(expected)
    actual["schema_version"] = schema_version
    actual["fingerprints"] = copy.deepcopy(
        expected["compatibility_fingerprints"][compatibility_key]
    )
    actual["contracts"] = copy.deepcopy(
        expected["compatibility_contracts"][compatibility_key]
    )
    _validate_checkpoint_manifest(
        {"experiment_manifest": actual},
        expected,
        checkpoint_path=tmp_path / f"schema_{schema_version}_interval.pt",
        scope="resume",
    )


def test_schema_v3_without_force_exit_is_conservative(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    expected = _checkpoint_manifest(panel, config)
    schema_v3 = copy.deepcopy(expected)
    schema_v3["schema_version"] = 3
    schema_v3["fingerprints"] = copy.deepcopy(
        expected["compatibility_fingerprints"]["schema_3_without_force_exit"]
    )
    schema_v3["contracts"] = copy.deepcopy(
        expected["compatibility_contracts"]["schema_3_without_force_exit"]
    )
    checkpoint = {"experiment_manifest": schema_v3}

    _validate_checkpoint_manifest(
        checkpoint,
        expected,
        checkpoint_path=tmp_path / "schema_v3_without_force.pt",
        scope="resume",
    )

    panel.force_exit_mask[0, 0] = True
    nonempty_expected = _checkpoint_manifest(panel, config)
    with pytest.raises(RuntimeError, match="Schema 3.*force_exit_mask|terminal-exit"):
        _validate_checkpoint_manifest(
            checkpoint,
            nonempty_expected,
            checkpoint_path=tmp_path / "schema_v3_without_force.pt",
            scope="resume",
        )
    # Loading model weights for future inference does not replay old trades.
    _validate_checkpoint_manifest(
        checkpoint,
        nonempty_expected,
        checkpoint_path=tmp_path / "schema_v3_without_force.pt",
        scope="inference",
    )


def test_effective_train_batch_size_guards_cross_machine_resume(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checkpoint_path = tmp_path / "checkpoint_last.pt"
    manifest = _checkpoint_manifest(_panel(), _config())
    checkpoint = {
        "experiment_manifest": manifest,
        "effective_train_batch_size": 32,
    }
    _validate_checkpoint_effective_train_batch_size(
        checkpoint,
        effective_train_batch_size=32,
        checkpoint_path=checkpoint_path,
    )
    with pytest.raises(RuntimeError, match="effective training batch size mismatch"):
        _validate_checkpoint_effective_train_batch_size(
            checkpoint,
            effective_train_batch_size=16,
            checkpoint_path=checkpoint_path,
        )

    schema_v3 = copy.deepcopy(manifest)
    schema_v3["schema_version"] = 3
    _validate_checkpoint_effective_train_batch_size(
        {"experiment_manifest": schema_v3},
        effective_train_batch_size=32,
        checkpoint_path=checkpoint_path,
    )
    assert "cannot be verified" in capsys.readouterr().out

    with pytest.raises(RuntimeError, match="missing effective_train_batch_size"):
        _validate_checkpoint_effective_train_batch_size(
            {"experiment_manifest": manifest},
            effective_train_batch_size=32,
            checkpoint_path=checkpoint_path,
        )


@pytest.mark.parametrize(
    "invalid_manifest",
    [
        [],
        {"schema_version": 0},
        {"schema_version": "future"},
        {"schema_version": 5, "fingerprints": {}},
        {"schema_version": 4, "fingerprints": []},
    ],
)
def test_checkpoint_validation_rejects_malformed_or_unknown_manifest_schema(
    tmp_path: Path,
    invalid_manifest,
) -> None:
    with pytest.raises(RuntimeError, match="manifest|schema"):
        _validate_checkpoint_manifest(
            {"experiment_manifest": invalid_manifest},
            _checkpoint_manifest(_panel(), _config()),
            checkpoint_path=tmp_path / "unsupported.pt",
            scope="resume",
        )


def test_tree_sidecar_makes_completed_fold_manifest_verifiable(tmp_path: Path) -> None:
    panel = _panel()
    config = _config()
    manifest = _checkpoint_manifest(panel, config)
    fold = _fold()
    fold_dir = _fold_dir(tmp_path, fold.fold_id)
    fold_dir.mkdir(parents=True)
    metrics = {
        "fold_id": fold.fold_id,
        "train_years": fold.train_years,
        "val_years": fold.val_years,
        "test_years": fold.test_years,
        "best_val_loss": -1.0,
        "val_ic": {},
        "val_metrics": {},
        "test_ic": {},
        "test_metrics": {},
        "test_integer_metrics": None,
    }
    (fold_dir / "metrics.json").write_text(json.dumps(metrics), encoding="utf-8")
    (fold_dir / "model.pt").write_bytes(b"tree pickle placeholder")
    (fold_dir / "test_backtest.npz").write_bytes(b"backtest placeholder")
    (fold_dir / "deployment_test_backtest.npz").write_bytes(b"deployment placeholder")
    (fold_dir / "fold_complete.json").write_text(
        json.dumps(
            {
                "status": "complete",
                "artifact_scope_version": 2,
                "test_scope": "full_horizon",
                "deployment_scope": "stitched_deployment",
            }
        ),
        encoding="utf-8",
    )
    checkpoint_path = fold_dir / "checkpoint_best.pt"
    _save_tree_checkpoint_metadata(
        checkpoint_path,
        fold=fold,
        best_val_loss=-1.0,
        experiment_manifest=manifest,
    )

    sidecar = _load_checkpoint(checkpoint_path)
    assert sidecar["checkpoint_kind"] == "tree_model_metadata"
    result = _load_completed_fold_result(
        tmp_path,
        fold.fold_id,
        expected_manifest=manifest,
        expected_fold=fold,
    )
    assert result is not None
    assert result.fold_id == fold.fold_id


def test_atomic_checkpoint_save_preserves_previous_file_on_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint_path = tmp_path / "checkpoint.pt"
    checkpoint_path.write_bytes(b"previous readable checkpoint")

    def _fail_save(payload, path):
        Path(path).write_bytes(b"partial replacement")
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(torch, "save", _fail_save)
    with pytest.raises(OSError, match="interrupted"):
        _atomic_torch_save({"epoch": 3}, checkpoint_path)

    assert checkpoint_path.read_bytes() == b"previous readable checkpoint"
    assert not list(tmp_path.glob(".*.tmp"))


def test_atomic_checkpoint_save_rejects_empty_completed_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint_path = tmp_path / "checkpoint.pt"
    _atomic_torch_save({"epoch": 2}, checkpoint_path)
    previous = checkpoint_path.read_bytes()

    def _empty_save(_payload, path):
        Path(path).write_bytes(b"")

    monkeypatch.setattr(torch, "save", _empty_save)
    with pytest.raises(OSError, match="temporary file is empty"):
        _atomic_torch_save({"epoch": 3}, checkpoint_path)

    assert checkpoint_path.read_bytes() == previous
    assert _load_checkpoint(checkpoint_path)["epoch"] == 2
    assert not list(tmp_path.glob(".*.tmp"))


def test_latest_group_checkpoint_skips_empty_newest_candidate(
    tmp_path: Path,
) -> None:
    healthy = tmp_path / "train_2014-2015" / "checkpoint_last.pt"
    corrupt = tmp_path / "train_2014-2015-2016" / "checkpoint_last.pt"
    _atomic_torch_save({"epoch": 100}, healthy)
    corrupt.parent.mkdir(parents=True)
    corrupt.write_bytes(b"")

    selected = _latest_group_checkpoint(tmp_path)

    assert selected == (healthy, [2014, 2015])
