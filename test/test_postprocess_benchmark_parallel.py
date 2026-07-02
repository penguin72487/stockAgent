from __future__ import annotations

from types import SimpleNamespace

import pytest
import torch

from scripts import benchmark_postprocess as bp
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel


def _config(*, long_only: bool) -> SimpleNamespace:
    return SimpleNamespace(
        trading=SimpleNamespace(
            buy_fee_rate=0.001,
            sell_fee_rate=0.002,
            long_only=long_only,
            max_turnover_ratio=0.35,
        )
    )


def _buffers() -> dict[str, torch.Tensor]:
    generator = torch.Generator().manual_seed(7)
    t_len = 13
    symbols = 9
    return {
        "scores": torch.randn((t_len, symbols), generator=generator, dtype=torch.float32),
        "future_returns": torch.randn((t_len, symbols), generator=generator, dtype=torch.float32) * 0.015,
        "tradable_mask": torch.ones((t_len, symbols), dtype=torch.bool),
        "can_buy_mask": torch.ones((t_len, symbols), dtype=torch.bool),
        "can_sell_mask": torch.ones((t_len, symbols), dtype=torch.bool),
        "benchmark": torch.randn((t_len,), generator=generator, dtype=torch.float32) * 0.01,
    }


@pytest.mark.parametrize("long_only", [False, True])
def test_batched_postprocess_sweep_matches_single_backtests(long_only: bool) -> None:
    config = _config(long_only=long_only)
    buffers = _buffers()
    rows = bp._run_sweep(
        buffers=buffers,
        mode="raw_logits",
        model_output_mode="logits",
        activations=["identity", "tanh"],
        thresholds=[0.0, 0.05],
        config=config,
        scan_chunk_size=4,
        sweep_batch_size=3,
    )

    assert len(rows) == 4
    for row in rows:
        backtest = bp._run_single_backtest(
            buffers=buffers,
            config=config,
            activation=str(row["activation"]),
            threshold=float(row["min_trade_weight"]),
            scan_chunk_size=4,
            return_weights_history=True,
        )
        metrics = bp._compute_metrics_from_tensors(
            backtest.strategy_returns,
            backtest.benchmark_returns,
            backtest.turnovers,
        )
        diagnostics = bp._weight_diagnostics(backtest.weights_history)
        for key, expected in {**metrics, **diagnostics}.items():
            assert float(row[key]) == pytest.approx(float(expected), abs=1e-6, rel=1e-6)


def test_raw_logits_from_aux_matches_direct_logits_mode() -> None:
    torch.manual_seed(11)
    model = TransformerBasePortfolioModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="temporal_only",
        use_flash_attention=False,
        use_time_pos=True,
        use_symbol_pos=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=1,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        cross_layers=0,
        joint_layers=0,
        latent_layers=0,
        market_layers=0,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        default_temperature=1.3,
        portfolio_mode="long_short",
        portfolio_activation="identity",
        portfolio_output_mode="projection_l1",
        return_aux=False,
        return_aux_details=False,
    )
    model.eval()
    x = torch.randn((2, 3, 5, 4), dtype=torch.float32)
    mask = torch.tensor(
        [
            [True, True, False, True, True],
            [True, False, True, True, False],
        ],
        dtype=torch.bool,
    )

    with torch.inference_mode():
        trained_output = model(x, mask, return_aux=True)
        _, aux = bp._extract_weights_and_aux(trained_output)
        raw_from_aux = bp._raw_logits_from_aux(aux=aux, model=model, mask=mask)
        model.portfolio_output_mode = "logits"
        raw_direct = model(x, mask, return_aux=False)

    assert raw_from_aux is not None
    torch.testing.assert_close(raw_from_aux, raw_direct, atol=1e-6, rtol=1e-6)
