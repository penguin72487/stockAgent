"""Central training-mode contracts and specialized-runner dispatch.

The outer training lifecycle is shared.  A mode spec changes only the market
clock, product family, sample ownership, and (when necessary) the specialized
dataset/execution adapter.  Keeping this registry centralized prevents
``train.py`` from growing one ad-hoc branch per product or frequency.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
import json
from pathlib import Path
from typing import Any, Callable, Final, Iterable

import numpy as np

from stockagent.backtest.tw_execution import (
    EXECUTION_MODES,
    normalize_execution_mode,
)
from stockagent.data_sync.desync_snapshots import atomic_write_json


@dataclass(frozen=True, slots=True)
class TrainingModeSpec:
    """Static contract separating shared lifecycle from mode-only semantics."""

    execution_mode: str
    product_family: str
    frequency: str
    decision_clock: str
    execution_clock: str
    recurrent_state_scope: str
    terminal_policy: str
    split_ownership: str
    sample_order_contract: str = "objective_defined"
    benchmark_contract: str = "mode_defined"
    weight_snapshot_contract: str = "mode_defined"
    turnover_contract: str = "mode_defined"
    runner_module: str | None = None
    runner_name: str | None = None
    dataset_root_config_path: str | None = None
    supports_isolated_folds: bool = True

    @property
    def specialized(self) -> bool:
        return self.runner_module is not None

    def __post_init__(self) -> None:
        mode = normalize_execution_mode(self.execution_mode)
        if mode != self.execution_mode:
            raise ValueError(
                f"training mode specs must use canonical names: {self.execution_mode!r}"
            )
        if (self.runner_module is None) != (self.runner_name is None):
            raise ValueError(
                "runner_module and runner_name must either both be set or both omitted"
            )
        required = {
            "product_family": self.product_family,
            "frequency": self.frequency,
            "decision_clock": self.decision_clock,
            "execution_clock": self.execution_clock,
            "recurrent_state_scope": self.recurrent_state_scope,
            "terminal_policy": self.terminal_policy,
            "split_ownership": self.split_ownership,
            "sample_order_contract": self.sample_order_contract,
            "benchmark_contract": self.benchmark_contract,
            "weight_snapshot_contract": self.weight_snapshot_contract,
            "turnover_contract": self.turnover_contract,
        }
        empty = sorted(name for name, value in required.items() if not str(value).strip())
        if empty:
            raise ValueError(f"training mode spec has empty fields: {empty}")


_MODE_SPECS: Final[tuple[TrainingModeSpec, ...]] = (
    TrainingModeSpec(
        execution_mode="naive",
        product_family="configured_cash_or_crypto_market",
        frequency="configured",
        decision_clock="completed_configured_bar",
        execution_clock="canonical_next_bar_return_contract",
        recurrent_state_scope="cross_bar_portfolio",
        terminal_policy="configured_backtest_horizon",
        split_ownership="year_expanding_walk_forward",
    ),
    TrainingModeSpec(
        execution_mode="crypto_perpetual",
        product_family="crypto_linear_perpetual",
        frequency="daily_24h_utc_anchored",
        decision_clock="prior_utc_calendar_day_complete_at_0000_utc",
        execution_clock="next_trade_official_1m_kline_open_at_0005_utc_after_five_minute_lag",
        recurrent_state_scope="cross_session_position_cash_and_funding",
        terminal_policy="marked_open_position_at_configured_backtest_horizon",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract="configured_perpetual_price_plus_funding_total_return",
        weight_snapshot_contract="post_rebalance_linear_perpetual_notional_over_nav",
        turnover_contract="gross_rebalance_notional_over_opening_nav",
    ),
    TrainingModeSpec(
        execution_mode="tw_cash",
        product_family="taiwan_stock_etf",
        frequency="daily_dual_auction",
        decision_clock="completed_prior_session_plus_observed_open",
        execution_clock="session_open_and_close_auctions",
        recurrent_state_scope="cross_session_t_plus_2_account",
        terminal_policy="canonical_cash_account_terminal_state",
        split_ownership="year_expanding_walk_forward",
    ),
    TrainingModeSpec(
        execution_mode="tw_day_trade",
        product_family="taiwan_stock_etf",
        frequency="daily_open_to_close",
        decision_clock="observed_session_open",
        execution_clock="same_session_open_then_close",
        recurrent_state_scope="cross_session_t_plus_2_account",
        terminal_policy="flat_each_session_with_settlement_state",
        split_ownership="year_expanding_walk_forward",
    ),
    TrainingModeSpec(
        execution_mode="tw_overnight",
        product_family="taiwan_stock_etf",
        frequency="daily_next_session",
        decision_clock="completed_prior_session_plus_observed_open",
        execution_clock="open_and_close_auctions_across_one_session",
        recurrent_state_scope="one_session_cohort_plus_t_plus_2_account",
        terminal_policy="cohort_closed_by_next_close",
        split_ownership="year_expanding_walk_forward",
    ),
    TrainingModeSpec(
        execution_mode="tw_futures_portfolio_day",
        product_family=(
            "taifex_historical_and_current_stock_etf_domestic_and_foreign_"
            "index_futures"
        ),
        frequency="daily_general_session",
        decision_clock="completed_prior_general_session_and_public_features",
        execution_clock="next_general_session_open_then_next_open_or_own_close",
        recurrent_state_scope="cross_session_independent_delivery_month_positions",
        terminal_policy="mandatory_own_contract_close_at_expiry_or_fold_end",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract="tx_front_month_same_physical_contract_no_roll_gap",
        weight_snapshot_contract="open_target_before_mandatory_close_liquidation",
        turnover_contract="gross_notional_change_plus_mandatory_close_over_nav",
    ),
    TrainingModeSpec(
        execution_mode="tw_stock_context_futures_portfolio",
        product_family=(
            "full_taiwan_cash_stock_context_to_taifex_historical_and_current_"
            "stock_etf_domestic_and_foreign_index_futures"
        ),
        frequency="daily_general_session",
        decision_clock=(
            "completed_prior_cash_stock_and_futures_general_sessions_before_open"
        ),
        execution_clock="next_general_session_open_then_next_open_or_own_close",
        recurrent_state_scope="cross_session_independent_delivery_month_positions",
        terminal_policy="mandatory_own_contract_close_at_expiry_or_fold_end",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract="tx_front_month_same_physical_contract_no_roll_gap",
        weight_snapshot_contract="open_target_before_mandatory_close_liquidation",
        turnover_contract="gross_notional_change_plus_mandatory_close_over_nav",
    ),
    TrainingModeSpec(
        execution_mode="tw_stock_futures_day_trade",
        product_family="taifex_single_stock_futures_with_full_stock_context",
        frequency="daily_open_to_close",
        decision_clock="completed_prior_stock_and_taifex_sessions_before_0845",
        execution_clock="selected_nearby_future_day_open_then_same_day_close",
        recurrent_state_scope="daily_flat_independent_futures_notional",
        terminal_policy="every_filled_contract_flat_at_same_session_close",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract="equal_weight_selected_executable_stock_futures_open_to_close",
        weight_snapshot_contract="filled_opening_futures_notional_over_initial_capital",
        turnover_contract="two_way_futures_notional_over_initial_capital",
    ),
    TrainingModeSpec(
        execution_mode="tw_stock_futures_day_trade_0900",
        product_family="taifex_single_stock_futures_with_full_stock_context",
        frequency="daily_0900_to_close",
        decision_clock=(
            "cash_open_observed_at_0900_after_all_session_t_open_features_arrive"
        ),
        execution_clock=(
            "selected_nearby_future_first_strictly_later_public_trade_"
            "through_090059_then_same_day_close"
        ),
        recurrent_state_scope="daily_flat_independent_futures_notional_with_cash",
        terminal_policy="every_filled_contract_flat_at_same_session_close",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract=(
            "equal_weight_selected_executable_stock_futures_0900_to_close"
        ),
        weight_snapshot_contract=(
            "filled_0900_futures_notional_over_initial_capital_with_residual_cash"
        ),
        turnover_contract="two_way_futures_notional_over_initial_capital",
    ),
    TrainingModeSpec(
        execution_mode="tw_stock_futures_day_trade_0900_integer",
        product_family=(
            "taifex_standard_and_mini_single_stock_futures_with_full_stock_context"
        ),
        frequency="daily_0900_to_close",
        decision_clock=(
            "cash_open_observed_at_0900_after_all_session_t_open_features_arrive"
        ),
        execution_clock=(
            "taifex_day_session_open_0845_daily_proxy_then_same_day_close"
        ),
        recurrent_state_scope=(
            "daily_flat_integer_contracts_with_fully_collateralized_equity"
        ),
        terminal_policy="every_filled_contract_flat_at_same_session_close",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract=(
            "equal_weight_executable_stock_futures_candidate_tier_mean"
        ),
        weight_snapshot_contract=(
            "actual_integer_open_notional_over_current_opening_equity"
        ),
        turnover_contract="two_way_actual_integer_notional_over_opening_equity",
    ),
    TrainingModeSpec(
        execution_mode="tw_index_futures_day",
        product_family="taiwan_index_futures_tx_mtx_tmf",
        frequency="daily_session",
        decision_clock="canonical_daily_feature_cutoff",
        execution_clock="taifex_day_session_execution",
        recurrent_state_scope="single_session_index_exposure",
        terminal_policy="flat_at_session_terminal",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract=(
            "tx_front_month_1x_long_gross_roll_at_preceding_close_"
            "same_contract_close_to_close_v2"
        ),
    ),
    TrainingModeSpec(
        execution_mode="tw_index_derivatives_day",
        product_family="taiwan_index_futures_tx_mtx_tmf_and_txo_long_options",
        frequency="daily_session",
        decision_clock="completed_prior_stock_session_features",
        execution_clock="taifex_day_session_first_last_trade_proxy",
        recurrent_state_scope="single_session_joint_derivative_budget",
        terminal_policy="all_futures_and_options_flat_at_session_terminal",
        split_ownership="year_expanding_walk_forward",
        benchmark_contract=(
            "tx_front_month_1x_long_gross_roll_at_preceding_close_"
            "same_contract_close_to_close_v2"
        ),
        weight_snapshot_contract=(
            "relative_e1_e6_plus_4096_causal_txo_candidates_v4"
        ),
        turnover_contract="gross_open_and_close_derivative_notional_over_nav",
    ),
    TrainingModeSpec(
        execution_mode="tw_minute",
        product_family="taiwan_stock_etf",
        frequency="one_minute",
        decision_clock="completed_right_labelled_1m_bar",
        execution_clock="next_1m_bar_open_proxy",
        recurrent_state_scope="within_session_cash_and_inventory",
        terminal_policy="mandatory_session_close_flatten",
        split_ownership="year_expanding_walk_forward",
        sample_order_contract="chronological_sessions",
        benchmark_contract=(
            "configured_symbol_adjusted_close_buy_hold_first_to_last_v1"
        ),
        weight_snapshot_contract="realised_weights_before_mandatory_close_flatten",
        turnover_contract="gross_traded_notional_over_daily_initial_equity",
        runner_module="stockagent.training.minute",
        runner_name="run_minute_training",
        dataset_root_config_path="data.minute_parquet_root",
        supports_isolated_folds=True,
    ),
    TrainingModeSpec(
        execution_mode="tw_index_derivatives_tick",
        product_family="taiwan_index_futures_tx",
        frequency="completed_second",
        decision_clock="completed_public_second_observable_at_next_second",
        execution_clock="strictly_later_latency_eligible_five_level_book",
        recurrent_state_scope="within_session_futures_exposure",
        terminal_policy="mandatory_session_flatten",
        split_ownership="day_count_expanding_walk_forward",
        sample_order_contract="chronological_sessions",
        benchmark_contract="cash_zero_return_until_derivative_benchmark_adapter",
        weight_snapshot_contract="terminal_flat_exposure_with_tick_audit_in_daily_curve",
        turnover_contract="absolute_normalized_tx_exposure_change",
        runner_module="stockagent.training.index_derivatives_tick",
        runner_name="run_index_derivatives_tick_training",
        dataset_root_config_path="data.index_derivatives_tick_dataset_root",
        supports_isolated_folds=False,
    ),
    TrainingModeSpec(
        execution_mode="tw_index_options_tick_long",
        product_family="taiwan_index_options_txo_long_only",
        frequency="completed_second",
        decision_clock="completed_public_second_observable_at_next_second",
        execution_clock="strictly_later_latency_eligible_call_put_books",
        recurrent_state_scope="within_session_option_premium_account",
        terminal_policy="mandatory_call_put_flatten",
        split_ownership="day_count_expanding_walk_forward",
        sample_order_contract="chronological_sessions",
        benchmark_contract="cash_zero_return_until_option_benchmark_adapter",
        weight_snapshot_contract="terminal_flat_contracts_with_tick_audit_in_daily_curve",
        turnover_contract="call_put_contract_turnover_count",
        runner_module="stockagent.training.index_derivatives_tick",
        runner_name="run_index_derivatives_tick_training",
        dataset_root_config_path="data.index_derivatives_tick_dataset_root",
        supports_isolated_folds=False,
    ),
    TrainingModeSpec(
        execution_mode="tw_index_options_tick_short",
        product_family="taiwan_index_options_txo_margin_short_enabled",
        frequency="completed_second",
        decision_clock="completed_public_second_observable_at_next_second",
        execution_clock="strictly_later_latency_eligible_call_put_books",
        recurrent_state_scope="within_session_option_premium_margin_account",
        terminal_policy="mandatory_call_put_flatten_or_absorbing_ruin",
        split_ownership="day_count_expanding_walk_forward",
        sample_order_contract="chronological_sessions",
        benchmark_contract="cash_zero_return_until_option_benchmark_adapter",
        weight_snapshot_contract="terminal_flat_contracts_with_tick_audit_in_daily_curve",
        turnover_contract="call_put_contract_turnover_count",
        runner_module="stockagent.training.index_derivatives_tick",
        runner_name="run_index_derivatives_tick_training",
        dataset_root_config_path="data.index_derivatives_tick_dataset_root",
        supports_isolated_folds=False,
    ),
)

TRAINING_MODE_SPECS: Final[dict[str, TrainingModeSpec]] = {
    spec.execution_mode: spec for spec in _MODE_SPECS
}

if set(TRAINING_MODE_SPECS) != set(EXECUTION_MODES):
    missing = sorted(set(EXECUTION_MODES) - set(TRAINING_MODE_SPECS))
    extra = sorted(set(TRAINING_MODE_SPECS) - set(EXECUTION_MODES))
    raise RuntimeError(
        f"training mode registry differs from execution registry: missing={missing}, extra={extra}"
    )


def training_mode_spec(execution_mode: object) -> TrainingModeSpec:
    return TRAINING_MODE_SPECS[normalize_execution_mode(execution_mode)]


def write_training_json(path: str | Path, payload: Any) -> None:
    """Canonical durable JSON writer for every specialized training adapter."""

    serializable = json.loads(
        json.dumps(payload, ensure_ascii=False, default=str)
    )
    atomic_write_json(Path(path), serializable)


def date_training_group_name(dates: Any) -> str:
    """Return a stable canonical train-group directory for day-count folds."""

    values = np.asarray(dates, dtype="datetime64[D]").reshape(-1)
    if values.size == 0 or np.isnat(values).any():
        raise ValueError("date training groups require non-empty finite dates")
    if np.any(values[1:] <= values[:-1]):
        raise ValueError("date training groups require strictly increasing dates")
    first = str(values[0]).replace("-", "")
    last = str(values[-1]).replace("-", "")
    return f"train_{first}-{last}"


def chronological_session_indices(indices: Iterable[int]) -> list[int]:
    """Materialize a strictly chronological session sequence without shuffling."""

    values = [int(value) for value in indices]
    if any(right <= left for left, right in zip(values, values[1:])):
        raise ValueError(
            "chronological session training requires strictly increasing indices"
        )
    return values


def _nested_config_value(config: object, dotted_path: str | None) -> Any:
    value: Any = config
    if dotted_path is None:
        return None
    for name in dotted_path.split("."):
        value = getattr(value, name)
    return value


def dispatch_specialized_training_mode(
    config: object,
    *,
    output_dir: str | Path,
    mode: str,
    resume: bool,
    start_fold: int | None,
    max_folds: int | None,
    active_strategy: str,
    isolate_train_folds: bool,
    startup_checkpoint: Callable[..., Any] | None = None,
) -> bool:
    """Run a registered non-daily adapter, returning whether it handled the run."""

    spec = training_mode_spec(getattr(getattr(config, "trading"), "execution_mode"))
    if not spec.specialized:
        return False
    if isolate_train_folds and not spec.supports_isolated_folds:
        raise ValueError(
            f"{spec.execution_mode} does not yet support runner.isolate_train_folds; "
            "use its canonical in-process group checkpoint resume"
        )
    assert spec.runner_module is not None and spec.runner_name is not None
    runner_module = import_module(spec.runner_module)
    runner_name = (
        f"{spec.runner_name}_isolated"
        if isolate_train_folds
        else spec.runner_name
    )
    runner = getattr(runner_module, runner_name)
    if startup_checkpoint is not None:
        startup_checkpoint(
            f"{spec.execution_mode}_runner_dispatch",
            dataset_root=str(
                _nested_config_value(config, spec.dataset_root_config_path)
            ),
            product_family=spec.product_family,
            frequency=spec.frequency,
            decision_clock=spec.decision_clock,
            execution_clock=spec.execution_clock,
        )
    runner(
        config,
        output_dir=output_dir,
        mode=mode,
        resume=bool(resume),
        start_fold=start_fold,
        max_folds=max_folds,
        active_strategy=str(active_strategy),
    )
    if startup_checkpoint is not None:
        startup_checkpoint(f"{spec.execution_mode}_runner_complete")
    return True


__all__ = [
    "TRAINING_MODE_SPECS",
    "TrainingModeSpec",
    "dispatch_specialized_training_mode",
    "chronological_session_indices",
    "date_training_group_name",
    "training_mode_spec",
    "write_training_json",
]
