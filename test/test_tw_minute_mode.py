from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.tw_minute import (
    MinuteExecutionConfig,
    execute_minute_target,
    force_close_minute_session,
    initialize_minute_execution_state,
    minute_target_weights_from_logits,
)
from stockagent.config import load_config
from stockagent.data.tw_minute import (
    MINUTE_FEATURE_COLUMNS,
    MinuteDayPanel,
    MinuteFeatureNormalizer,
    load_minute_dataset_index,
)
from stockagent.training.minute import (
    _build_windows,
    _execute_minute_chunk,
    run_minute_training,
)
from stockagent.training.lifecycle import validate_completed_training_artifacts
from stockagent.training import minute as minute_training
from stockagent.models.factory import build_model
from stockagent.models.transformer_base_portfolio import (
    action_channels_for_execution_mode,
)


def _config() -> MinuteExecutionConfig:
    return MinuteExecutionConfig(
        initial_equity=1_000_000.0,
        gross_exposure=0.9,
        maximum_name_weight=0.6,
        maximum_volume_participation=0.1,
        maximum_order_notional=1_000_000.0,
        slippage_bps_per_side=2.0,
    )


def _step(state, target: torch.Tensor, *, allow_new_entries: bool = True):
    return execute_minute_target(
        state,
        target_weights=target,
        execution_mask=torch.tensor([True, True]),
        execution_open=torch.tensor([100.0, 50.0]),
        exit_close=torch.tensor([100.0, 50.0]),
        future_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
        allow_new_entries=allow_new_entries,
    )


def test_minute_split_is_chronological_and_progress_counts_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_batches: list[list[int]] = []
    progress_instances: list[object] = []

    class FakeCache:
        hits = 0
        misses = 0
        cache_bytes = 0

        def get_many(self, indices: list[int]) -> list[int]:
            self.misses += len(indices)
            return list(indices)

    class FakeProgress:
        def __init__(self, *, total: int) -> None:
            self.total = total
            self.updated = 0
            self.postfixes: list[dict[str, object]] = []

        def update(self, amount: int) -> None:
            self.updated += amount

        def set_postfix(
            self, values: dict[str, object], *, refresh: bool = True
        ) -> None:
            del refresh
            self.postfixes.append(values)

        def close(self) -> None:
            return None

    def fake_progress_bar(*_args, total=None, **_kwargs):
        progress = FakeProgress(total=int(total))
        progress_instances.append(progress)
        return progress

    def fake_run_day_batch(*, days, **_kwargs):
        indices = [int(value) for value in days]
        seen_batches.append(indices)
        rows = [
            {
                "date": str(np.datetime64("2020-01-01") + np.timedelta64(index, "D")),
                "net_return": 0.0,
                "log_return": 0.0,
                "turnover_notional": 0.0,
                "explicit_fees": 0.0,
                "slippage_cost": 0.0,
            }
            for index in indices
        ]
        return (
            rows,
            np.zeros((len(indices), 1), dtype=np.float64),
            minute_training.TimingBreakdown(batches=1),
        )

    monkeypatch.setattr(minute_training, "_progress_bar", fake_progress_bar)
    monkeypatch.setattr(minute_training, "_run_day_batch", fake_run_day_batch)
    config = SimpleNamespace(
        training=SimpleNamespace(
            batch_size_train=2,
            batch_size_eval=3,
            minute_decision_chunk_rows=10,
            grad_clip_norm=1.0,
        )
    )

    _summary, rows, _weights, timing = minute_training._run_split(
        name="chronological-train",
        indices=np.asarray([0, 1, 2, 3, 4], dtype=np.int64),
        dataset=object(),
        normalizer=object(),
        model=torch.nn.Identity(),
        model_forward=lambda *_args: None,
        execution_forward=lambda *_args: (),
        day_cache=FakeCache(),
        config=config,
        execution_config=_config(),
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        device=torch.device("cpu"),
        amp_dtype=None,
        optimizer=object(),
        benchmark_symbol_index=0,
    )

    assert seen_batches == [[0, 1], [2, 3], [4]]
    assert [row["date"] for row in rows] == [
        "2020-01-01",
        "2020-01-02",
        "2020-01-03",
        "2020-01-04",
        "2020-01-05",
    ]
    progress = progress_instances[0]
    assert isinstance(progress, FakeProgress)
    assert progress.total == 5
    assert progress.updated == 5
    assert timing.batches == 3


def test_minute_logits_are_masked_capped_and_leave_cash() -> None:
    weights = minute_target_weights_from_logits(
        torch.tensor([[10.0, 0.0, -10.0]]),
        torch.tensor([[True, True, False]]),
        gross_exposure=0.9,
        maximum_name_weight=0.4,
    )

    assert weights[0, 0] == pytest.approx(0.4)
    assert weights[0, 2] == 0.0
    assert float(weights.sum()) < 0.9


def test_minute_executor_trades_only_target_delta_and_forces_flat() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    first = _step(state, torch.tensor([0.5, 0.0]))
    second = _step(first.state, torch.tensor([0.5, 0.0]))

    assert float(first.turnover_notional) > 0.0
    assert float(second.turnover_notional) < float(first.turnover_notional) * 0.01
    closed = force_close_minute_session(
        second.state,
        session_close=torch.tensor([100.0, 50.0]),
        session_exit_mask=torch.tensor([True, True]),
        last_minute_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
    )
    assert torch.count_nonzero(closed.state.shares) == 0
    assert float(closed.state.equity) < _config().initial_equity


def test_last_decision_cannot_open_new_inventory() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    result = _step(state, torch.tensor([0.5, 0.0]), allow_new_entries=False)

    assert torch.count_nonzero(result.state.shares) == 0
    assert float(result.turnover_notional) == 0.0


def test_minute_execution_is_differentiable_through_fees_and_price_move() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    logits = torch.tensor([[1.0, 0.0]], requires_grad=True)
    target = minute_target_weights_from_logits(
        logits,
        torch.tensor([[True, True]]),
        gross_exposure=0.9,
        maximum_name_weight=0.6,
    )[0]
    result = execute_minute_target(
        state,
        target_weights=target,
        execution_mask=torch.tensor([True, True]),
        execution_open=torch.tensor([100.0, 50.0]),
        exit_close=torch.tensor([101.0, 49.0]),
        future_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
        allow_new_entries=True,
    )
    loss = -torch.log1p(result.net_return)
    loss.backward()

    assert logits.grad is not None
    assert torch.isfinite(logits.grad).all()
    assert float(logits.grad.abs().sum()) > 0.0


def test_batched_minute_executor_matches_independent_day_oracle_and_gradients() -> None:
    batched_logits = torch.tensor(
        [[1.0, -0.25], [-0.5, 0.75]], requires_grad=True
    )
    mask = torch.ones_like(batched_logits, dtype=torch.bool)
    batched_target = minute_target_weights_from_logits(
        batched_logits,
        mask,
        gross_exposure=0.9,
        maximum_name_weight=0.6,
    )
    batched_state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    opens = torch.tensor([[100.0, 50.0], [80.0, 40.0]])
    closes = torch.tensor([[101.0, 49.0], [79.0, 42.0]])
    volumes = torch.full((2, 2), 1_000_000.0)
    buy_rates = torch.tensor([0.001, 0.001])
    sell_rates = torch.tensor([0.002, 0.002])
    batched = execute_minute_target(
        batched_state,
        target_weights=batched_target,
        execution_mask=mask,
        execution_open=opens,
        exit_close=closes,
        future_volume_shares=volumes,
        buy_fee_rates=buy_rates,
        sell_fee_rates=sell_rates,
        config=_config(),
        allow_new_entries=True,
    )
    batched_loss = -torch.log1p(batched.net_return).sum()
    batched_loss.backward()

    oracle_logits = batched_logits.detach().clone().requires_grad_(True)
    oracle_equity: list[torch.Tensor] = []
    oracle_returns: list[torch.Tensor] = []
    for row in range(2):
        target = minute_target_weights_from_logits(
            oracle_logits[row : row + 1],
            mask[row : row + 1],
            gross_exposure=0.9,
            maximum_name_weight=0.6,
        )[0]
        state = initialize_minute_execution_state(
            num_symbols=2,
            config=_config(),
            device=torch.device("cpu"),
        )
        result = execute_minute_target(
            state,
            target_weights=target,
            execution_mask=mask[row],
            execution_open=opens[row],
            exit_close=closes[row],
            future_volume_shares=volumes[row],
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            config=_config(),
            allow_new_entries=True,
        )
        oracle_equity.append(result.state.equity)
        oracle_returns.append(result.net_return)
    oracle_loss = -torch.log1p(torch.stack(oracle_returns)).sum()
    oracle_loss.backward()

    torch.testing.assert_close(batched.state.equity, torch.stack(oracle_equity))
    torch.testing.assert_close(batched.net_return, torch.stack(oracle_returns))
    torch.testing.assert_close(batched_logits.grad, oracle_logits.grad)


def test_bounded_minute_chunk_matches_sequential_batched_oracle_and_gradients() -> None:
    targets = torch.tensor(
        [
            [[0.5, 0.2], [0.4, 0.3], [0.6, 0.1]],
            [[0.2, 0.5], [0.3, 0.4], [0.1, 0.6]],
        ],
        requires_grad=True,
    )
    masks = torch.ones(2, 3, 2, dtype=torch.bool)
    opens = torch.tensor(
        [
            [[100.0, 50.0], [101.0, 49.0], [100.0, 51.0]],
            [[80.0, 40.0], [79.0, 42.0], [81.0, 41.0]],
        ]
    )
    closes = opens * torch.tensor([[[1.001, 0.999]]])
    volumes = torch.full((2, 3, 2), 1_000_000.0)
    allow_entries = torch.tensor([True, True, False])
    buy_rates = torch.tensor([0.001, 0.001])
    sell_rates = torch.tensor([0.002, 0.002])
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    chunk = _execute_minute_chunk(
        state.cash,
        state.shares,
        state.last_prices,
        state.equity,
        targets,
        masks,
        opens,
        closes,
        volumes,
        allow_entries,
        buy_fee_rates=buy_rates,
        sell_fee_rates=sell_rates,
        config=_config(),
    )
    chunk_loss = -chunk[4].sum()
    chunk_loss.backward()

    oracle_targets = targets.detach().clone().requires_grad_(True)
    oracle_state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    oracle_log = torch.zeros(2)
    oracle_turnover = torch.zeros(2)
    oracle_fees = torch.zeros(2)
    oracle_slippage = torch.zeros(2)
    for minute in range(3):
        result = execute_minute_target(
            oracle_state,
            target_weights=oracle_targets[:, minute],
            execution_mask=masks[:, minute],
            execution_open=opens[:, minute],
            exit_close=closes[:, minute],
            future_volume_shares=volumes[:, minute],
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            config=_config(),
            allow_new_entries=allow_entries[minute],
        )
        oracle_state = result.state
        oracle_log = oracle_log + torch.log1p(
            torch.clamp(result.net_return, min=-0.999999)
        )
        oracle_turnover = oracle_turnover + result.turnover_notional
        oracle_fees = oracle_fees + result.explicit_fees
        oracle_slippage = oracle_slippage + result.slippage_cost
    (-oracle_log.sum()).backward()

    for actual, expected in zip(
        chunk[:4],
        (
            oracle_state.cash,
            oracle_state.shares,
            oracle_state.last_prices,
            oracle_state.equity,
        ),
    ):
        torch.testing.assert_close(actual, expected)
    for actual, expected in zip(
        chunk[4:],
        (oracle_log, oracle_turnover, oracle_fees, oracle_slippage),
    ):
        torch.testing.assert_close(actual, expected)
    torch.testing.assert_close(targets.grad, oracle_targets.grad)


def test_batched_panel_slabs_match_independent_canonical_slab_forwards() -> None:
    config = load_config(
        Path(__file__).parents[1] / "configs/markets/tw_minute.yaml"
    )
    config.training.lookback = 2
    config.training.enable_torch_compile = False
    config.training.transformer_base_portfolio.attention_mode = "temporal_only"
    config.training.transformer_base_portfolio.use_latent_factors = False
    config.training.transformer_base_portfolio.use_market_tokens = False
    config.training.transformer_base_portfolio.d_model = 8
    config.training.transformer_base_portfolio.temporal_layers = 1
    config.training.transformer_base_portfolio.temporal_heads = 1
    config.training.transformer_base_portfolio.temporal_ffn_mult = 1
    config.training.transformer_base_portfolio.head_hidden_dim = 8
    model = build_model(
        config=config,
        lookback=2,
        num_features=len(MINUTE_FEATURE_COLUMNS),
        num_symbols=3,
        feature_names=MINUTE_FEATURE_COLUMNS,
    ).eval()
    generator = torch.Generator().manual_seed(7)
    slabs = torch.randn(
        2, 4, 3, len(MINUTE_FEATURE_COLUMNS), generator=generator
    )
    mask = torch.tensor(
        [
            [[True, True, False], [True, True, True], [True, False, True]],
            [[True, True, True], [False, True, True], [True, True, False]],
        ]
    )

    with torch.no_grad():
        batched = model.forward_from_batched_panel_slabs(
            slabs, mask, return_aux=False
        )
        oracle = torch.cat(
            [
                model.forward_from_panel_slab(
                    slabs[day], mask[day], return_aux=False
                )
                for day in range(2)
            ],
            dim=0,
        )

    torch.testing.assert_close(batched, oracle)


def test_official_tw_minute_config_uses_a_separate_training_contract() -> None:
    config_path = Path(__file__).parents[1] / "configs/markets/tw_minute.yaml"
    config = load_config(config_path)

    assert config.trading.execution_mode == "tw_minute"
    assert config.trading.frequency == "minute"
    assert config.trading.long_only is True
    assert config.training.transformer_base_portfolio.portfolio_output_mode == "logits"
    assert config.data.minute_require_research_ready is True
    assert action_channels_for_execution_mode("tw_minute") == ("target",)


def test_model_mask_does_not_consume_next_bar_or_session_exit_availability() -> None:
    features = np.ones((270, 2, len(MINUTE_FEATURE_COLUMNS)), dtype=np.float32)
    feature_mask = np.ones((270, 2), dtype=bool)
    execution_mask = np.zeros((270, 2), dtype=bool)
    session_exit_mask = np.zeros(2, dtype=bool)
    day = MinuteDayPanel(
        trade_date=np.datetime64("2026-01-02"),
        features=features,
        feature_mask=feature_mask,
        execution_mask=execution_mask,
        execution_open=np.zeros((270, 2), dtype=np.float32),
        exit_close=np.zeros((270, 2), dtype=np.float32),
        future_volume_shares=np.zeros((270, 2), dtype=np.float32),
        session_close=np.zeros(2, dtype=np.float32),
        session_exit_mask=session_exit_mask,
    )

    _, model_mask = _build_windows(day, np.asarray([1]), lookback=2)

    assert model_mask.tolist() == [[True, True]]


def test_minute_partition_loads_sparse_full_market_rows_into_dense_day(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute"
    partition = root / "trade_date=2026-01-02"
    partition.mkdir(parents=True)
    output = partition / "data.parquet"
    feature_values = {
        name: [float(index + 1), float(index + 2)]
        for index, name in enumerate(MINUTE_FEATURE_COLUMNS)
    }
    frame = pl.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 2, 9, 1),
                datetime(2026, 1, 2, 9, 2),
            ],
            "symbol": ["0050", "2330"],
            **feature_values,
            "feature_valid": [True, True],
            "label_valid_1m": [True, True],
            "execution_open_next_1m": [100.0, 200.0],
            "exit_close_next_1m": [101.0, 202.0],
            "future_volume_shares_next_1m": [1_000_000.0, 2_000_000.0],
            "session_close": [102.0, 203.0],
            "session_exit_valid": [True, True],
        }
    )
    frame.write_parquet(output)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    counts = {name: 2 for name in MINUTE_FEATURE_COLUMNS}
    sums = {
        name: float(sum(feature_values[name])) for name in MINUTE_FEATURE_COLUMNS
    }
    squares = {
        name: float(sum(value * value for value in feature_values[name]))
        for name in MINUTE_FEATURE_COLUMNS
    }
    summary = {
        "trade_date": "2026-01-02",
        "output": str(output),
        "output_sha256": digest,
        "feature_counts": counts,
        "feature_sums": sums,
        "feature_sum_squares": squares,
    }
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "status": "research_ready",
                "research_ready": True,
                "source": "shioaji_kbars_1m",
                "decision_clock": "completed_right_labelled_1m_bar",
                "execution_clock": "next_1m_bar_open_proxy",
                "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
                "symbols": ["0050", "2330"],
                "dates": ["2026-01-02"],
                "partitions": [summary],
            }
        ),
        encoding="utf-8",
    )
    dataset = load_minute_dataset_index(root)
    normalizer = MinuteFeatureNormalizer(
        mean=np.zeros(len(MINUTE_FEATURE_COLUMNS), dtype=np.float32),
        scale=np.ones(len(MINUTE_FEATURE_COLUMNS), dtype=np.float32),
        counts=np.full(len(MINUTE_FEATURE_COLUMNS), 2, dtype=np.int64),
    )
    day = dataset.load_day(0, normalizer=normalizer)

    assert day.features.shape == (270, 2, len(MINUTE_FEATURE_COLUMNS))
    assert day.feature_mask[0, 0]
    assert day.feature_mask[1, 1]
    assert not day.feature_mask[0, 1]
    assert day.execution_open[0, 0] == pytest.approx(100.0)
    assert day.exit_close[1, 1] == pytest.approx(202.0)
    assert day.session_close.tolist() == pytest.approx([102.0, 203.0])


def test_minute_training_reuses_canonical_group_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "minute"
    symbols = ["0050", "2330"]
    date_strings = [
        "2020-01-02",
        "2021-01-04",
        "2022-01-03",
        "2023-01-03",
    ]
    partitions: list[dict[str, object]] = []
    for date_offset, date_string in enumerate(date_strings):
        partition = root / f"trade_date={date_string}"
        partition.mkdir(parents=True)
        output = partition / "data.parquet"
        rows = len(symbols) * 2
        feature_values = {
            name: [
                float(date_offset + feature_index + row_index + 1)
                for row_index in range(rows)
            ]
            for feature_index, name in enumerate(MINUTE_FEATURE_COLUMNS)
        }
        frame = pl.DataFrame(
            {
                "ts": [
                    datetime.fromisoformat(f"{date_string} 09:01:00"),
                    datetime.fromisoformat(f"{date_string} 09:01:00"),
                    datetime.fromisoformat(f"{date_string} 09:02:00"),
                    datetime.fromisoformat(f"{date_string} 09:02:00"),
                ],
                "symbol": symbols * 2,
                **feature_values,
                "feature_valid": [True] * rows,
                "label_valid_1m": [True] * rows,
                "execution_open_next_1m": [100.0, 200.0] * 2,
                "exit_close_next_1m": [100.5, 201.0] * 2,
                "future_volume_shares_next_1m": [1_000_000.0] * rows,
                "session_close": [100.5, 201.0] * 2,
                "session_exit_valid": [True] * rows,
            }
        )
        frame.write_parquet(output)
        partitions.append(
            {
                "trade_date": date_string,
                "output": str(output),
                "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "feature_counts": {
                    name: rows for name in MINUTE_FEATURE_COLUMNS
                },
                "feature_sums": {
                    name: float(sum(values))
                    for name, values in feature_values.items()
                },
                "feature_sum_squares": {
                    name: float(sum(value * value for value in values))
                    for name, values in feature_values.items()
                },
            }
        )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "status": "research_ready",
                "research_ready": True,
                "source": "shioaji_kbars_1m",
                "decision_clock": "completed_right_labelled_1m_bar",
                "execution_clock": "next_1m_bar_open_proxy",
                "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
                "symbols": symbols,
                "dates": date_strings,
                "partitions": partitions,
            }
        ),
        encoding="utf-8",
    )

    config = load_config(Path(__file__).parents[1] / "configs/markets/tw_minute.yaml")
    config.data.minute_parquet_root = str(root)
    config.data.benchmark_name = symbols[0]
    config.data.minute_verify_partition_sha256 = True
    config.environment.device = "cpu"
    config.environment.amp_dtype = "tf32"
    config.training.enable_torch_compile = False
    config.training.enable_lr_scheduler = False
    config.training.epochs = 1
    config.training.lookback = 2
    config.training.batch_size_train = 2
    config.training.batch_size_eval = 2
    config.training.record_epoch_curve = True
    config.training.early_stopping_no_improve_ratio = 0.0
    config.training.transformer_base_portfolio.attention_mode = "temporal_only"
    config.training.transformer_base_portfolio.use_latent_factors = False
    config.training.transformer_base_portfolio.use_market_tokens = False
    config.training.transformer_base_portfolio.d_model = 8
    config.training.transformer_base_portfolio.temporal_layers = 1
    config.training.transformer_base_portfolio.temporal_heads = 1
    config.training.transformer_base_portfolio.temporal_ffn_mult = 1
    config.training.transformer_base_portfolio.joint_layers = 1
    config.training.transformer_base_portfolio.joint_heads = 1
    config.training.transformer_base_portfolio.joint_ffn_mult = 1
    config.training.transformer_base_portfolio.head_hidden_dim = 8
    config.trading.tw_minute_max_name_weight = 0.5
    config.trading.tw_minute_first_decision_minute = 2
    config.trading.tw_minute_last_entry_minute = 2
    config.trading.tw_minute_last_decision_minute = 2
    monkeypatch.setattr(
        "stockagent.training.trainer._run_epoch_curve_plot_once",
        lambda *_args, **_kwargs: {},
    )

    output_dir = tmp_path / "artifacts"
    results = run_minute_training(
        config,
        output_dir=output_dir,
        mode="train",
        resume=True,
        start_fold=None,
        max_folds=1,
        active_strategy="none",
    )

    group_dir = output_dir / "train_2020-2021"
    assert (group_dir / "checkpoint_last.pt").is_file()
    assert (output_dir / "fold_01" / "checkpoint_best.pt").is_file()
    assert (output_dir / "fold_01" / "metrics.json").is_file()
    assert (output_dir / "fold_01" / "model.pt").is_file()
    assert (output_dir / "fold_01" / "test_backtest.npz").is_file()
    assert (output_dir / "fold_01" / "deployment_test_backtest.npz").is_file()
    assert (output_dir / "fold_01" / "equity_curve.png").is_file()
    assert (output_dir / "fold_01" / "equity_curve_log.png").is_file()
    assert (output_dir / "fold_01" / "annual_performance.png").is_file()
    assert (output_dir / "fold_01" / "annual_report.txt").is_file()
    assert (output_dir / "fold_01" / "plot_timing.json").is_file()
    assert (output_dir / "fold_01" / "mode_artifact_contract.json").is_file()
    assert (output_dir / "summary.json").is_file()
    run_manifest = json.loads(
        (output_dir / "minute_run_manifest.json").read_text(encoding="utf-8")
    )
    canonical_run_manifest = json.loads(
        (output_dir / "run_manifest.json").read_text(encoding="utf-8")
    )
    progress = json.loads(
        (output_dir / "progress.json").read_text(encoding="utf-8")
    )
    artifact_contract = json.loads(
        (output_dir / "fold_01" / "mode_artifact_contract.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_manifest["sample_order_contract"] == "chronological_sessions"
    assert run_manifest == canonical_run_manifest
    assert progress["state"] == "complete"
    assert progress["phase"] == "complete"
    assert progress["group"]["name"] == "train_2020-2021"
    assert progress["fold"]["completed_ids"] == [1]
    assert artifact_contract["sample_order_contract"] == "chronological_sessions"
    assert artifact_contract["product_family"] == "taiwan_stock_etf"
    assert artifact_contract["frequency"] == "one_minute"
    curve_rows = [
        json.loads(line)
        for line in (group_dir / "epoch_curve.jsonl").read_text().splitlines()
    ]
    assert len(curve_rows) == 1
    assert curve_rows[0]["train_loss"] is not None
    assert curve_rows[0]["val_mean"] is not None
    assert curve_rows[0]["epoch_total_s"] > 0.0
    assert curve_rows[0]["minute_days_per_train_batch"] == 2
    assert curve_rows[0]["minute_decision_chunk_rows"] == 10
    assert curve_rows[0]["minute_optimizer_steps_per_epoch"] == 1
    assert curve_rows[0]["minute_train_cache_misses"] == 2
    assert curve_rows[0]["minute_val_cache_misses"] == 1
    conformance = validate_completed_training_artifacts(
        output_dir,
        fold_ids=[1],
        group_names=["train_2020-2021"],
    )
    conformance.require()
    assert results[0]["status"] == "complete"
