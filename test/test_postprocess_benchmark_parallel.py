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
        ),
        training=SimpleNamespace(
            chunk_rows=0,
            eval_model_chunk_rows="auto",
            eval_auto_chunk_rows_cap=16,
            eval_backtest_chunk_rows=512,
            eval_backtest_chunk_rows_auto=True,
        ),
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


def test_chunk_resolution_passes_total_rows_to_backtest_policy() -> None:
    config = _config(long_only=False)

    assert bp._resolve_benchmark_chunk_rows(
        config,
        total_rows=211,
        model_chunk_rows=None,
        scan_chunk_size=None,
    ) == (16, 256)
    assert bp._resolve_benchmark_chunk_rows(
        config,
        total_rows=211,
        model_chunk_rows=7,
        scan_chunk_size=9,
    ) == (7, 9)


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


def test_multi_device_scheduler_preserves_candidate_order_and_results() -> None:
    config = _config(long_only=False)
    buffers = _buffers()
    candidates = [
        {
            "mode": "raw_logits",
            "model_output_mode": "logits",
            "candidate": f"raw:{activation}",
            "activation": activation,
            "min_trade_weight": threshold,
        }
        for activation in ("identity", "tanh")
        for threshold in (0.0, 0.05)
    ]
    serial, _ = bp._batched_backtest_candidates(
        buffers=buffers,
        config=config,
        candidates=candidates,
        scan_chunk_size=4,
    )
    parallel, _ = bp._batched_backtest_candidates(
        buffers=buffers,
        replica_buffers=[{name: value.clone() for name, value in buffers.items()}],
        config=config,
        candidates=candidates,
        scan_chunk_size=4,
    )

    assert [row["candidate"] for row in parallel] == [row["candidate"] for row in serial]
    assert [row["min_trade_weight"] for row in parallel] == [
        row["min_trade_weight"] for row in serial
    ]
    for actual, expected in zip(parallel, serial, strict=True):
        assert actual["sweep_device_count"] == 2
        for key in ("sharpe", "sortino", "cumulative_return", "turnover", "avg_gross"):
            assert float(actual[key]) == pytest.approx(float(expected[key]), abs=1e-6, rel=1e-6)


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
