import math
from pathlib import Path

import numpy as np
import pytest
import torch
import yaml

from stockagent.backtest.simulator import _resolve_exposure_budget, run_backtest, run_backtest_torch
from stockagent.config import (
    DataConfig,
    EnvironmentConfig,
    EvaluationConfig,
    GradientBoostedPortfolioTransformerConfig,
    RunnerConfig,
    TradingConfig,
    TrainingConfig,
    WalkForwardConfig,
    _dataclass_default_values,
    _nested_training_schemas,
    load_config,
)
from stockagent.models.factory import build_model


def _write_minimal_config(tmp_path: Path, *, training_overrides: dict | None = None) -> Path:
    config_path = tmp_path / "config.yaml"
    training = {
        "non_blocking_transfer": True,
        "model_name": "transformer_base_portfolio",
    }
    training.update(training_overrides or {})
    config_path.write_text(
        yaml.safe_dump(
            {
                "experiment_name": "gross-leverage-test",
                "environment": {
                    "device": "cuda",
                    "use_tensor_cores": True,
                    "amp_dtype": "bf16",
                },
                "data": {
                    "parquet_root": "data_yahoo/tw_stocks",
                    "benchmark_name": "2330",
                },
                "walk_forward": {
                    "min_train_years": 1,
                    "val_years": 1,
                    "require_future_test_year": False,
                },
                "trading": {
                    "frequency": "daily",
                    "buy_fee_rate": 0.000855,
                    "sell_fee_rate": 0.003855,
                    "long_only": False,
                    "gross_leverage": 2.5,
                },
                "training": training,
                "evaluation": {
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_load_config_migrates_legacy_gross_leverage_to_reporting_leverage(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.trading.reporting_leverage == 2.5
    assert not hasattr(config.trading, "gross_leverage")


def test_panel_start_date_is_normalized_and_matches_walk_forward_year(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["data"]["panel_start_date"] = "2005-01-01"
    payload["walk_forward"]["expected_first_year"] = 2005
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    config = load_config(config_path)

    assert config.data.panel_start_date == "2005-01-01"


def test_panel_start_date_rejects_mismatched_walk_forward_year(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["data"]["panel_start_date"] = "2005-01-01"
    payload["walk_forward"]["expected_first_year"] = 2000
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="expected_first_year must match"):
        load_config(config_path)


@pytest.mark.parametrize(
    "feature",
    [
        "twpub_monthly_revenue_log",
        "twpub_cumulative_revenue_yoy",
        "twpub_financial_eps",
        "twpub_insider_holdings_log",
        "twpub_borrow_available_log",
        "twpub_sbl_balance_log",
        "twpub_short_sale_available_log",
        "twpub_tdcc_holder_count_log",
        "twpub_company_industry_code",
    ],
)
def test_load_config_rejects_permanently_disabled_snapshot_features(
    tmp_path: Path,
    feature: str,
) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["data"]["feature_include"] = [feature]
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="permanently disabled snapshot-only"):
        load_config(config_path)


def test_load_config_migrates_legacy_leverage_to_reporting_leverage(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["trading"].pop("gross_leverage")
    payload["trading"]["leverage"] = 1.75
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    config = load_config(config_path)

    assert config.trading.reporting_leverage == 1.75
    assert not hasattr(config.trading, "leverage")


def test_reporting_leverage_rejects_conflicting_legacy_alias(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["trading"]["reporting_leverage"] = 1.5
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="reporting_leverage conflicts"):
        load_config(config_path)


def test_loaded_defaults_match_dataclass_defaults_for_every_config_section(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["walk_forward"].pop("min_train_years")
    payload["walk_forward"].pop("require_future_test_year")
    payload["trading"].pop("gross_leverage")
    payload["training"].pop("model_name")
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    config = load_config(config_path)
    top_level_schemas = {
        "runner": RunnerConfig,
        "environment": EnvironmentConfig,
        "data": DataConfig,
        "walk_forward": WalkForwardConfig,
        "trading": TradingConfig,
        "training": TrainingConfig,
        "evaluation": EvaluationConfig,
    }
    nested_names = set(_nested_training_schemas())
    for section_name, schema in top_level_schemas.items():
        actual_section = getattr(config, section_name)
        for field_name, expected in _dataclass_default_values(schema).items():
            if section_name == "training" and field_name in nested_names:
                continue
            assert getattr(actual_section, field_name) == expected, (
                f"{section_name}.{field_name} loader default drifted from "
                f"{schema.__name__}.{field_name}"
            )

    for section_name, schema in _nested_training_schemas().items():
        actual_section = getattr(config.training, section_name)
        for field_name, expected in _dataclass_default_values(schema).items():
            assert getattr(actual_section, field_name) == expected, (
                f"training.{section_name}.{field_name} loader default drifted from "
                f"{schema.__name__}.{field_name}"
            )


def test_one_model_block_cannot_change_another_model_blocks_defaults(tmp_path: Path) -> None:
    config = load_config(
        _write_minimal_config(
            tmp_path,
            training_overrides={"transformer_base_portfolio": {"d_model": 96}},
        )
    )

    assert config.training.transformer_base_portfolio.d_model == 96
    assert config.training.gradient_boosted_portfolio_transformer.d_model == (
        _dataclass_default_values(GradientBoostedPortfolioTransformerConfig)["d_model"]
    )


def test_load_config_accepts_independent_transformer_bottleneck_switches(
    tmp_path: Path,
) -> None:
    config = load_config(
        _write_minimal_config(
            tmp_path,
            training_overrides={
                "transformer_base_portfolio": {
                    "use_latent_factors": True,
                    "use_market_tokens": False,
                }
            },
        )
    )

    assert config.training.transformer_base_portfolio.use_latent_factors is True
    assert config.training.transformer_base_portfolio.use_market_tokens is False


def test_load_config_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "duplicate.yaml"
    config_path.write_text(
        """
experiment_name: duplicate-key-test
training:
  non_blocking_transfer: true
  save_best_val_artifacts: false
  save_best_val_artifacts: true
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate YAML key 'save_best_val_artifacts'"):
        load_config(config_path)


@pytest.mark.parametrize("legacy_interval", ["step", "batch", "epoch"])
def test_load_config_removes_noop_lr_scheduler_interval(
    tmp_path: Path,
    legacy_interval: str,
) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "enable_lr_scheduler": True,
            "lr_scheduler": "warmup_cosine",
            "lr_scheduler_warmup_steps": 123,
            "lr_scheduler_interval": legacy_interval,
        },
    )

    config = load_config(config_path)

    assert config.training.lr_scheduler == "warmup_cosine"
    assert config.training.lr_scheduler_warmup_steps == 123
    assert not hasattr(config.training, "lr_scheduler_interval")


def test_load_config_rejects_unknown_legacy_lr_scheduler_interval(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cadence is fixed"):
        load_config(
            _write_minimal_config(
                tmp_path,
                training_overrides={"lr_scheduler_interval": "sometimes"},
            )
        )


def test_legacy_batch_size_is_migrated_to_both_canonical_batch_fields(tmp_path: Path) -> None:
    config = load_config(
        _write_minimal_config(tmp_path, training_overrides={"batch_size": 7})
    )

    assert config.training.batch_size_train == 7
    assert config.training.batch_size_eval == 7
    assert not hasattr(config.training, "batch_size")


def test_legacy_noop_symbol_subsample_default_is_silently_removed(tmp_path: Path) -> None:
    config = load_config(
        _write_minimal_config(
            tmp_path,
            training_overrides={"train_symbol_subsample_ratio": 1.0},
        )
    )

    assert not hasattr(config.training, "train_symbol_subsample_ratio")


def test_unimplemented_symbol_subsample_value_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="never implemented"):
        load_config(
            _write_minimal_config(
                tmp_path,
                training_overrides={"train_symbol_subsample_ratio": 0.5},
            )
        )


@pytest.mark.parametrize(
    "key_path",
    [
        ("environment", "use_tensor_cores"),
        ("trading", "long_only"),
        ("training", "non_blocking_transfer"),
        ("training", "enable_torch_compile"),
        ("training", "compile_loss"),
        ("training", "transformer_base_portfolio", "return_aux"),
        ("training", "transformer_base_portfolio", "use_latent_factors"),
        ("training", "transformer_base_portfolio", "use_market_tokens"),
    ],
)
def test_load_config_rejects_string_booleans(tmp_path: Path, key_path: tuple[str, ...]) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    target = payload
    for key in key_path[:-1]:
        target = target.setdefault(key, {})
    target[key_path[-1]] = "false"
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=r"YAML true/false") as exc_info:
        load_config(config_path)

    assert ".".join(key_path) in str(exc_info.value)


def test_legacy_csv_output_flags_migrate_to_canonical_table_flags(tmp_path: Path) -> None:
    config = load_config(
        _write_minimal_config(
            tmp_path,
            training_overrides={
                "save_daily_weights_csv": False,
                "save_integer_share_daily_weights_csv": False,
                "save_integer_share_holdings_csv": False,
            },
        )
    )

    assert config.training.save_daily_weights_table is False
    assert config.training.save_integer_share_daily_weights_table is False
    assert config.training.save_integer_share_holdings_table is False
    assert not hasattr(config.training, "save_daily_weights_csv")
    assert not hasattr(config.training, "save_integer_share_daily_weights_csv")
    assert not hasattr(config.training, "save_integer_share_holdings_csv")


@pytest.mark.parametrize(
    ("key_path", "value"),
    [
        (("environment", "conda_env"), "fintech"),
        (("environment", "target_vram_fraction"), 0.8),
        (("data", "universe_mode"), "all_daily_symbols"),
        (("data", "use_rapids"), True),
        (("data", "benchmark_source"), "legacy"),
        (("trading", "cash_allowed"), True),
        (("trading", "use_all_tradable_symbols"), True),
        (("evaluation", "primary_baseline"), "universe_average"),
        (("evaluation", "metrics"), ["sharpe"]),
        (("training", "backend"), "pytorch"),
        (("training", "target"), "next_1d_rank"),
        (("training", "batch_mode"), "time_window_x_all_symbols"),
        (("training", "top_k"), 10),
        (("training", "prefer_fp16"), False),
        (("training", "data_parallel_device_ids"), [0, 1]),
        (("training", "data_parallel_output_device"), 0),
        (("training", "data_parallel_disable_panel_forward"), True),
        (("training", "data_parallel_compile_model"), True),
        (("training", "data_parallel_threaded_replicas"), True),
        (("training", "cross_sectional_temporal_portfolio_model", "stock_embedding_dim"), 64),
        (("training", "cross_sectional_temporal_portfolio_model", "temporal_blocks"), 2),
        (("training", "cross_sectional_temporal_portfolio_model", "scorer"), "tabular_resnet"),
        (("training", "cross_sectional_temporal_portfolio_model", "reranker"), "set_transformer"),
    ],
)
def test_load_config_rejects_removed_noop_keys(
    tmp_path: Path,
    key_path: tuple[str, ...],
    value: object,
) -> None:
    config_path = _write_minimal_config(tmp_path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    target = payload
    for key in key_path[:-1]:
        target = target.setdefault(key, {})
    target[key_path[-1]] = value
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError) as exc_info:
        load_config(config_path)

    assert ".".join(key_path) in str(exc_info.value)


def test_load_config_rejects_removed_data_parallel_strategy(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={"multi_gpu_strategy": "data_parallel"},
    )

    with pytest.raises(ValueError, match=r"data_parallel.*removed.*distributed_data_parallel"):
        load_config(config_path)


def test_cstpm_config_has_one_effective_name_per_model_knob(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "model_name": "cross_sectional_temporal_portfolio_model",
            "cross_sectional_temporal_portfolio_model": {
                "candidate_k": 7,
                "trade_k": 3,
                "scorer_hidden": 48,
                "scorer_blocks": 3,
                "d_model": 24,
                "heads": 4,
                "layers": 2,
                "dropout": 0.0,
            },
        },
    )
    config = load_config(config_path)
    model = build_model(
        config=config,
        lookback=4,
        num_features=5,
        num_symbols=12,
    )

    assert model.candidate_top_m == 7
    assert model.portfolio_top_k == 3
    assert model.stock_scorer[0].out_features == 48
    assert model.reranker_input_proj.in_features == 25
    assert model.reranker_input_proj.out_features == 24
    assert len(model.reranker.layers) == 2
    assert model.reranker.layers[0].self_attn.num_heads == 4


def test_load_config_supports_relative_base_config_deep_merge(tmp_path: Path) -> None:
    base_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "batch_size_train": 64,
            "multitask_loss": {"concentration_weight": 0.01},
            "transformer_base_portfolio": {
                "dropout": 0.1,
                "portfolio_output_mode": "logits",
            },
        },
    )
    child_path = tmp_path / "child.yaml"
    child_path.write_text(
        yaml.safe_dump(
            {
                "base_config": base_path.name,
                "experiment_name": "child-config",
                "runner": {"output_dir": "artifacts/child"},
                "training": {
                    "learning_rate": 0.0002,
                    "multitask_loss": {"return_rank_ic_weight": 0.07},
                    "transformer_base_portfolio": {"dropout": 0.2},
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_config(child_path)

    assert config.experiment_name == "child-config"
    assert config.runner.output_dir == "artifacts/child"
    assert config.training.batch_size_train == 64
    assert math.isclose(config.training.learning_rate, 0.0002)
    assert math.isclose(config.training.multitask_loss.concentration_weight, 0.01)
    assert math.isclose(config.training.multitask_loss.return_rank_ic_weight, 0.07)
    assert config.training.transformer_base_portfolio.portfolio_output_mode == "logits"
    assert math.isclose(config.training.transformer_base_portfolio.dropout, 0.2)


def test_latefold_objective_configs_load_and_keep_ddp_transformer() -> None:
    from stockagent.training.trainer import _normalize_risk_objective

    expected = {
        "configs/markets/tw_parallel_latefold_stable.yaml": "log_utility",
        "configs/markets/tw_parallel_latefold_sharpck.yaml": "sharpe",
        "configs/markets/tw_parallel_latefold_sortino.yaml": "sortino",
    }
    output_dirs: set[str] = set()
    for path, objective in expected.items():
        config = load_config(path)
        output_dirs.add(config.runner.output_dir)
        assert config.runner.start_fold == 24
        assert config.training.model_name == "transformer_base_portfolio"
        assert config.training.multi_gpu_strategy == "distributed_data_parallel"
        assert config.training.transformer_base_portfolio.portfolio_output_mode == "logits"
        assert _normalize_risk_objective(config.training.loss_type) == objective
    assert len(output_dirs) == len(expected)


def test_backtest_exposure_budget_caps_multiplier_at_one() -> None:
    assert _resolve_exposure_budget(2.5) == 1.0

    weights = torch.tensor([[1.0, -1.0]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=2.5,
        can_buy_mask=tradable,
        can_sell_mask=tradable,
    )

    expected_weights = torch.tensor([[0.5, -0.5]], dtype=torch.float32)
    assert torch.allclose(result.weights_history.cpu(), expected_weights, atol=1e-7, rtol=1e-6)
    assert torch.allclose(result.weights_history.abs().sum(dim=1).cpu(), torch.tensor([1.0]), atol=1e-7, rtol=1e-6)


def test_backtest_converts_asset_log_returns_to_portfolio_log_return() -> None:
    asset_log_return = math.log(0.4)
    expected_strategy_log_return = math.log1p(0.6)

    weights_np = np.array([[-1.0]], dtype=np.float32)
    returns_np = np.array([[asset_log_return]], dtype=np.float32)
    tradable_np = np.ones_like(weights_np, dtype=bool)
    benchmark_np = np.zeros((1,), dtype=np.float32)

    numpy_result = run_backtest(
        weights_np,
        returns_np,
        tradable_np,
        benchmark_np,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
        can_buy_mask=tradable_np,
        can_sell_mask=tradable_np,
    )

    weights_t = torch.from_numpy(weights_np)
    returns_t = torch.from_numpy(returns_np)
    tradable_t = torch.from_numpy(tradable_np)
    benchmark_t = torch.from_numpy(benchmark_np)
    torch_result = run_backtest_torch(
        weights_t,
        returns_t,
        tradable_t,
        benchmark_t,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
        can_buy_mask=tradable_t,
        can_sell_mask=tradable_t,
    )
    dense_result = run_backtest_torch(
        weights_t,
        returns_t,
        tradable_t,
        benchmark_t,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
        can_buy_mask=tradable_t,
        can_sell_mask=tradable_t,
    )

    assert math.isclose(float(numpy_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)
    assert math.isclose(float(torch_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)
    assert math.isclose(float(dense_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)


def test_load_config_defaults_best_val_artifact_switches_off(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is False
    assert config.training.save_best_val_fold_artifacts is False
    assert config.training.save_best_val_fold_plots is False
    assert config.training.curve_test_interval == 1


def test_load_config_rejects_removed_eval_backtest_engine(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={"eval_backtest_engine": "triton"},
    )

    with pytest.raises(ValueError, match="eval_backtest_engine"):
        load_config(config_path)


def test_load_config_best_val_artifacts_master_switch_enables_fold_artifacts(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path, training_overrides={"save_best_val_artifacts": True})

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is True
    assert config.training.save_best_val_fold_artifacts is True
    assert config.training.save_best_val_fold_plots is True


def test_load_config_preserves_best_checkpoint_max_epoch(tmp_path: Path) -> None:
    default_config_path = _write_minimal_config(tmp_path)
    default_config = load_config(default_config_path)

    capped_config_path = _write_minimal_config(tmp_path, training_overrides={"best_checkpoint_max_epoch": 12})
    capped_config = load_config(capped_config_path)

    assert default_config.training.best_checkpoint_max_epoch == 0
    assert capped_config.training.best_checkpoint_max_epoch == 12


@pytest.mark.parametrize(
    ("raw_mode", "expected_mode"),
    [
        ("activation-l1", "activation_l1"),
        ("raw_l1", "l1"),
        ("raw-scores", "logits"),
        ("signed-action-softmax", "signed_softmax"),
        ("signed-action-sparsemax", "signed_sparsemax"),
        ("signed-action-entmax", "signed_entmax15"),
        ("differentiable-projection", "projection_l1"),
    ],
)
def test_load_config_normalizes_portfolio_output_mode_aliases(
    tmp_path: Path,
    raw_mode: str,
    expected_mode: str,
) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "transformer_base_portfolio": {
                "portfolio_output_mode": raw_mode,
            }
        },
    )

    config = load_config(config_path)

    assert config.training.transformer_base_portfolio.portfolio_output_mode == expected_mode


@pytest.mark.parametrize("raw_mode", ["adaptive-gross-l1", "adaptive-gross-net-l1", "gated-net-l1"])
def test_load_config_rejects_removed_gated_portfolio_modes(tmp_path: Path, raw_mode: str) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "transformer_base_portfolio": {
                "portfolio_output_mode": raw_mode,
            }
        },
    )

    with pytest.raises(ValueError, match="portfolio_output_mode"):
        load_config(config_path)


def test_load_config_rejects_unknown_portfolio_output_mode(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "transformer_base_portfolio": {
                "portfolio_output_mode": "mystery_mode",
            }
        },
    )

    with pytest.raises(ValueError, match="portfolio_output_mode"):
        load_config(config_path)


def test_tw_public_select_uses_full_gross_long_short_l1_contract() -> None:
    config = load_config("configs/markets/tw_public_select.yaml")

    model = config.training.transformer_base_portfolio
    assert config.trading.long_only is False
    assert model.portfolio_mode == "long_short"
    assert model.portfolio_output_mode == "l1"
    assert config.training.loss_portfolio_activation == "pre_normalized"
    assert config.trading.portfolio_activation == "pre_normalized"


def test_tw_public_candles_select_uses_dual_5090_tw_cash_contract() -> None:
    config = load_config("configs/markets/tw_public_lanten_market_candles_select.yaml")

    model = config.training.financial_transformer
    assert config.trading.execution_mode == "tw_cash"
    assert config.trading.tw_corporate_action_mode == "avoid"
    assert config.trading.tw_short_capacity_limit_enabled is False
    assert config.trading.long_only is False
    assert model.portfolio_output_mode == "l1"
    assert config.training.transformer_base_portfolio.portfolio_output_mode == "l1"
    assert config.training.loss_portfolio_activation == "pre_normalized"
    assert config.trading.portfolio_activation == "pre_normalized"
    assert config.training.multi_gpu_strategy == "auto"
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 128
    assert config.environment.cpu_threads == 128
    assert config.environment.torch_compile_threads == 16


def test_tw_public_candles_all_folds_uses_runnable_tw_day_trade_contract() -> None:
    config = load_config("configs/markets/tw_public_lanten_market_candles.yaml")

    assert config.trading.execution_mode == "tw_day_trade"
    assert config.data.panel_start_date == "2014-01-01"
    assert config.data.benchmark_name == "2330"
    assert config.walk_forward.expected_first_year == 2014
    assert config.trading.tw_corporate_action_mode == "avoid"
    assert config.trading.tw_short_capacity_limit_enabled is False
    assert config.training.financial_transformer.portfolio_output_mode == "l1"
    assert config.training.multi_gpu_strategy == "auto"
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 128
    assert config.environment.cpu_threads == 128
    assert config.environment.torch_compile_threads == 16
    assert config.runner.resume is True


def test_tw_rule_switch_is_independent_from_tw_model_features() -> None:
    tw = load_config("configs/markets/tw.yaml")
    tw_public = load_config("configs/markets/tw_public.yaml")
    derived_tw = load_config("configs/markets/tw_parallel_latefold_stable.yaml")

    assert tw.data.use_tw_public_rules is True
    assert tw.data.use_tw_public_features is False
    assert tw_public.data.use_tw_public_rules is True
    assert tw_public.data.use_tw_public_features is True
    assert derived_tw.data.use_tw_public_rules is True


@pytest.mark.parametrize(
    "config_path",
    [
        "configs/markets/crypto.yaml",
        "configs/markets/forex.yaml",
        "configs/markets/us.yaml",
    ],
)
def test_non_tw_market_configs_disable_tw_public_rules(config_path: str) -> None:
    assert load_config(config_path).data.use_tw_public_rules is False
