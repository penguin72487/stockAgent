from __future__ import annotations

from pathlib import Path

import torch

from stockagent.config import load_config
from stockagent.explainability_cross_asset import _portfolio_weights_from_scores
from stockagent.models.factory import build_model


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = (
    REPO_ROOT
    / "configs/markets/tw_futures_portfolio_day_multi_basis_projection_l1.yaml"
)


def test_futures_multi_basis_config_preserves_execution_and_fee_contract() -> None:
    config = load_config(CONFIG_PATH)

    assert config.trading.execution_mode == "tw_futures_portfolio_day"
    assert config.trading.long_only is False
    assert config.trading.portfolio_activation == "pre_normalized"
    assert config.trading.buy_fee_rate == 0.0
    assert config.trading.sell_fee_rate == 0.0
    assert config.trading.tw_futures_portfolio_fee_large_twd == 60.0
    assert config.trading.tw_futures_portfolio_fee_standard_twd == 24.0
    assert config.trading.tw_futures_portfolio_fee_stock_twd == 40.0
    assert config.trading.tw_futures_portfolio_fee_micro_twd == 16.0
    assert (
        config.trading.tw_futures_portfolio_data_path
        == "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
    )
    assert config.walk_forward.lookback_context == "panel_history"


def test_futures_multi_basis_config_uses_formal_projection_l1_baseline() -> None:
    config = load_config(CONFIG_PATH)
    model = config.training.financial_transformer

    assert config.training.model_name == "financial_transformer"
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 128
    assert config.training.loss_type == "log_utility"
    assert config.training.loss_portfolio_activation == "pre_normalized"
    assert model.portfolio_output_mode == "projection_l1"
    assert model.portfolio_mode == "long_short"
    assert model.center_long_short_logits is False
    assert model.projection_l1_scale_by_active_count is True
    assert model.categorical_feature_names == ["taifex_product_id"]
    assert model.categorical_embedding_cardinality == 1024
    assert config.training.early_stopping_no_improve_ratio == 0.1
    assert model.temporal_basis_input == "input_features"
    assert model.temporal_basis_components == 4
    assert model.temporal_basis_families == [
        "haar",
        "swt_db2",
        "swt_sym4",
        "wavelet_packet",
        "walsh",
        "fourier",
        "dct",
        "dpss",
        "local_cosine",
        "morlet",
        "exponential",
        "laguerre",
        "difference",
        "ar_innovation",
        "bspline",
        "legendre",
        "chebyshev",
        "learned",
    ]
    assert model.use_latent_factors is True
    assert model.use_market_tokens is True
    assert model.use_time_pos is False
    assert model.rope_temporal is False
    assert model.temporal_pooling == "attention"
    assert model.temporal_query_mode == "full_then_last"
    transformer_base = config.training.transformer_base_portfolio
    assert transformer_base.use_time_pos is False
    assert transformer_base.rope_temporal is False
    assert transformer_base.temporal_pooling == "attention"
    assert transformer_base.temporal_query_mode == "full_then_last"
    assert config.runner.output_dir.endswith(
        "tw_futures_portfolio_day_multi_basis_projection_l1_"
        "no_time_encoding_attention_full_then_last_v1"
    )
    assert config.training.record_epoch_curve is True
    assert config.training.curve_plot_interval == 1
    assert config.training.defer_epoch_curve_plot_until_end is False


def test_futures_multi_basis_projection_keeps_shared_direction_and_cash() -> None:
    config = load_config(CONFIG_PATH)
    model = build_model(
        config=config,
        lookback=32,
        num_features=3,
        num_symbols=4,
        feature_names=["f0", "f1", "f2"],
    ).eval()
    assert model.center_long_short_logits is False
    assert model.projection_l1_scale_by_active_count is True

    # A common score is the model's market-direction degree of freedom.  The
    # old de-meaned head erased it exactly; the corrected head maps score 0.5 to
    # 50% gross regardless of whether two or four contracts are active.
    with torch.no_grad():
        for parameter in model.score_head.parameters():
            parameter.zero_()
        model.score_head[-1].bias.fill_(0.5)
        x = torch.zeros((2, 32, 4, 3), dtype=torch.float32)
        mask = torch.tensor(
            [[True, True, True, True], [True, True, False, False]],
        )
        weights = model(x, mask, return_aux=False)

    torch.testing.assert_close(
        weights,
        torch.tensor(
            [[0.125, 0.125, 0.125, 0.125], [0.25, 0.25, 0.0, 0.0]],
        ),
    )
    torch.testing.assert_close(
        weights.abs().sum(dim=1),
        torch.tensor([0.5, 0.5]),
    )

    # Explainability/inference must consume the exact same score semantics.  In
    # particular, it must not silently re-introduce cross-sectional centering.
    explained_weights = _portfolio_weights_from_scores(
        model,
        torch.full((2, 4), 0.5),
        mask,
    )
    torch.testing.assert_close(explained_weights, weights)
