"""Configuration and causal dataset helpers for the daily futures strategy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from numbers import Real
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from stockagent.backtest.tw_index_futures import FuturesCostSchedule
from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_index_futures import (
    TaiwanIndexFuturesDaySession,
    normalize_taifex_index_futures_product,
)
from stockagent.data.walkforward import normalize_lookback_context
from stockagent.models.cross_sectional_index_futures import (
    CrossSectionalIndexFuturesModel,
)


@dataclass(frozen=True, slots=True)
class TaiwanIndexFuturesDayStrategyConfig:
    base_experiment_config: Path
    output_dir: Path
    futures_data_path: Path
    reference_product: str = "TX"
    initial_capital: float = 1_000_000.0
    max_abs_exposure: float = 1.0
    continuous_round_trip_cost_rate: float = 0.00009
    exchange_and_clearing_fee_per_side_twd: tuple[float, ...] = (
        20.0,
        12.5,
        8.0,
    )
    broker_fee_per_side_twd: tuple[float, ...] = (0.0, 0.0, 0.0)
    slippage_points_per_side: tuple[float, ...] = (0.0, 0.0, 0.0)
    basket_fee_penalty: float = 1.0
    epochs: int = 50
    batch_size: int = 32
    eval_batch_size: int = 64
    learning_rate: float = 3e-4
    weight_decay: float = 1e-4
    start_fold: int = 1
    max_folds: int | None = None
    seed: int = 42
    enable_torch_compile: bool = True
    model_overrides: dict[str, Any] | None = None

    @property
    def cost_schedule(self) -> FuturesCostSchedule:
        return FuturesCostSchedule(
            exchange_and_clearing_fee_per_side_twd=(
                self.exchange_and_clearing_fee_per_side_twd
            ),
            broker_fee_per_side_twd=self.broker_fee_per_side_twd,
            slippage_points_per_side=self.slippage_points_per_side,
            basket_fee_penalty=self.basket_fee_penalty,
        )


def _triple(
    raw: object,
    *,
    name: str,
    default: tuple[float, float, float],
) -> tuple[float, float, float]:
    if raw is not None and (
        isinstance(raw, (str, bytes)) or not hasattr(raw, "__iter__")
    ):
        raise ValueError(f"{name} must contain TX, MTX, and TMF values")
    values = default if raw is None else tuple(float(value) for value in raw)
    if len(values) != 3:
        raise ValueError(f"{name} must contain TX, MTX, and TMF values")
    return (float(values[0]), float(values[1]), float(values[2]))


def load_tw_index_futures_day_strategy_config(
    path: str | Path,
) -> tuple[ExperimentConfig, TaiwanIndexFuturesDayStrategyConfig]:
    source = Path(path).expanduser().resolve()
    with source.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError("futures strategy config must be a YAML mapping")
    unknown = sorted(
        set(raw)
        - {
            "base_experiment_config",
            "output_dir",
            "futures_data_path",
            "reference_product",
            "initial_capital",
            "max_abs_exposure",
            "continuous_round_trip_cost_rate",
            "exchange_and_clearing_fee_per_side_twd",
            "broker_fee_per_side_twd",
            "slippage_points_per_side",
            "basket_fee_penalty",
            "epochs",
            "batch_size",
            "eval_batch_size",
            "learning_rate",
            "weight_decay",
            "start_fold",
            "max_folds",
            "seed",
            "enable_torch_compile",
            "model_overrides",
        }
    )
    if unknown:
        raise ValueError(f"unknown futures strategy config keys: {unknown}")
    base_value = str(raw.get("base_experiment_config") or "").strip()
    data_value = str(raw.get("futures_data_path") or "").strip()
    output_value = str(raw.get("output_dir") or "").strip()
    if not base_value or not data_value or not output_value:
        raise ValueError(
            "base_experiment_config, futures_data_path, and output_dir are required"
        )

    def resolve(value: str) -> Path:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = source.parent / candidate
        return candidate.resolve()

    base_path = resolve(base_value)
    base_config = load_config(base_path)
    max_folds_raw = raw.get("max_folds")
    max_folds = None if max_folds_raw is None else int(max_folds_raw)
    model_overrides = raw.get("model_overrides")
    if model_overrides is not None and not isinstance(model_overrides, dict):
        raise ValueError("model_overrides must be a mapping")
    strategy = TaiwanIndexFuturesDayStrategyConfig(
        base_experiment_config=base_path,
        output_dir=resolve(output_value),
        futures_data_path=resolve(data_value),
        reference_product=normalize_taifex_index_futures_product(
            raw.get("reference_product", "TX")
        ),
        initial_capital=float(raw.get("initial_capital", 1_000_000.0)),
        max_abs_exposure=float(raw.get("max_abs_exposure", 1.0)),
        continuous_round_trip_cost_rate=float(
            raw.get("continuous_round_trip_cost_rate", 0.00009)
        ),
        exchange_and_clearing_fee_per_side_twd=_triple(
            raw.get("exchange_and_clearing_fee_per_side_twd"),
            name="exchange_and_clearing_fee_per_side_twd",
            default=(20.0, 12.5, 8.0),
        ),
        broker_fee_per_side_twd=_triple(
            raw.get("broker_fee_per_side_twd"),
            name="broker_fee_per_side_twd",
            default=(0.0, 0.0, 0.0),
        ),
        slippage_points_per_side=_triple(
            raw.get("slippage_points_per_side"),
            name="slippage_points_per_side",
            default=(0.0, 0.0, 0.0),
        ),
        basket_fee_penalty=float(raw.get("basket_fee_penalty", 1.0)),
        epochs=int(raw.get("epochs", 50)),
        batch_size=int(raw.get("batch_size", 32)),
        eval_batch_size=int(raw.get("eval_batch_size", 64)),
        learning_rate=float(raw.get("learning_rate", 3e-4)),
        weight_decay=float(raw.get("weight_decay", 1e-4)),
        start_fold=int(raw.get("start_fold", 1)),
        max_folds=max_folds,
        seed=int(raw.get("seed", 42)),
        enable_torch_compile=bool(raw.get("enable_torch_compile", True)),
        model_overrides=(
            None if model_overrides is None else dict(model_overrides)
        ),
    )
    if strategy.epochs < 1:
        raise ValueError("epochs must be positive")
    if strategy.batch_size < 1 or strategy.eval_batch_size < 1:
        raise ValueError("batch sizes must be positive")
    if strategy.start_fold < 1:
        raise ValueError("start_fold must be positive")
    if strategy.max_folds is not None and strategy.max_folds < 1:
        raise ValueError("max_folds must be positive or null")
    for name in (
        "initial_capital",
        "max_abs_exposure",
        "continuous_round_trip_cost_rate",
        "learning_rate",
        "weight_decay",
    ):
        value = getattr(strategy, name)
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not math.isfinite(float(value))
            or float(value) < 0.0
        ):
            raise ValueError(f"{name} must be a finite non-negative real")
    if strategy.initial_capital <= 0.0:
        raise ValueError("initial_capital must be positive")
    if not 0.0 < strategy.max_abs_exposure <= 1.0:
        raise ValueError("max_abs_exposure must be in (0, 1]")
    if strategy.learning_rate <= 0.0:
        raise ValueError("learning_rate must be positive")
    # Constructor validation is the single fee/sizing contract.
    _ = strategy.cost_schedule
    return base_config, strategy


def decision_indices_for_futures_day_session(
    panel: PanelData,
    market: TaiwanIndexFuturesDaySession,
    owned_indices: np.ndarray,
    *,
    lookback: int,
    lookback_context: str,
    reference_product: str = "TX",
) -> np.ndarray:
    """Return causal trade dates whose feature window ends at ``t-1``."""

    if int(lookback) < 1:
        raise ValueError("lookback must be positive")
    if not np.array_equal(
        np.asarray(panel.dates, dtype="datetime64[D]"),
        np.asarray(market.dates, dtype="datetime64[D]"),
    ):
        raise ValueError("stock panel and futures market dates must be aligned")
    owned = np.asarray(owned_indices, dtype=np.int64)
    if owned.ndim != 1 or owned.size == 0:
        return np.empty(0, dtype=np.int64)
    if bool(np.any(owned[1:] <= owned[:-1])):
        raise ValueError("owned_indices must be strictly increasing")
    if int(owned[0]) < 0 or int(owned[-1]) >= panel.num_dates:
        raise IndexError("owned_indices contains a row outside the panel")
    context = normalize_lookback_context(lookback_context)
    earliest = int(lookback)
    if context == "split_only":
        earliest = int(owned[0]) + int(lookback)
    reference_valid = market.reference_tradable_mask(reference_product)
    prior_alive = np.zeros(panel.num_dates, dtype=bool)
    if panel.num_dates > 1:
        prior_alive[1:] = np.asarray(panel.alive_mask[:-1], dtype=bool).any(axis=1)
    keep = (
        (owned >= earliest)
        & reference_valid[owned]
        & prior_alive[owned]
    )
    return owned[keep]


def decision_stock_mask(panel: PanelData, decision_indices: np.ndarray) -> np.ndarray:
    indices = np.asarray(decision_indices, dtype=np.int64)
    if indices.ndim != 1:
        raise ValueError("decision_indices must be one-dimensional")
    if indices.size and (int(indices.min()) < 1 or int(indices.max()) >= panel.num_dates):
        raise IndexError("decision_indices must be in [1, panel.num_dates)")
    return np.asarray(panel.alive_mask[indices - 1], dtype=bool)


def model_feature_end_indices(decision_indices: np.ndarray) -> np.ndarray:
    indices = np.asarray(decision_indices, dtype=np.int64)
    if indices.ndim != 1:
        raise ValueError("decision_indices must be one-dimensional")
    if indices.size and bool(np.any(indices < 1)):
        raise ValueError("futures decisions require a preceding stock session")
    return indices - 1


def build_index_futures_model(
    base_config: ExperimentConfig,
    panel: PanelData,
    strategy: TaiwanIndexFuturesDayStrategyConfig,
) -> CrossSectionalIndexFuturesModel:
    raw = asdict(base_config.training.transformer_base_portfolio)
    categorical_names = raw.pop("categorical_feature_names", [])
    categorical_indices = [
        panel.feature_names.index(name)
        for name in categorical_names
        if name in panel.feature_names
    ]
    raw.update(strategy.model_overrides or {})
    raw.pop("categorical_feature_names", None)
    raw.update(
        lookback=int(base_config.training.lookback),
        num_features=len(panel.feature_names),
        num_symbols=len(panel.symbols),
        symbol_position_capacity=len(panel.symbols),
        portfolio_mode="long_short",
        portfolio_activation="identity",
        portfolio_output_mode="logits",
        return_aux=False,
        return_aux_details=False,
        categorical_feature_indices=categorical_indices,
        runtime_shape_check=bool(base_config.training.runtime_shape_check),
        allow_dynamic_symbols=False,
        max_abs_exposure=float(strategy.max_abs_exposure),
    )
    return CrossSectionalIndexFuturesModel(**raw)


__all__ = [
    "TaiwanIndexFuturesDayStrategyConfig",
    "build_index_futures_model",
    "decision_indices_for_futures_day_session",
    "decision_stock_mask",
    "load_tw_index_futures_day_strategy_config",
    "model_feature_end_indices",
]
