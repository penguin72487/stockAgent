from __future__ import annotations

from copy import deepcopy
from dataclasses import MISSING, asdict, dataclass, field, fields, is_dataclass
from datetime import date
import fnmatch
from pathlib import Path
from typing import Any, get_args, get_type_hints

import yaml

from stockagent.backtest.tw_execution import (
    TW_CARRYING_EXECUTION_MODES,
    TaiwanFeeSchedule,
    TaiwanMarginShortSchedule,
    normalize_execution_mode,
)
from stockagent.data.walkforward import normalize_lookback_context
from stockagent.portfolio_contract import (
    DEFAULT_PORTFOLIO_ACTIVATION,
    normalize_portfolio_activation,
    normalize_portfolio_output_mode,
)

_CONFIG_INHERITANCE_KEYS = ("base_config", "base_configs", "extends", "inherits")

# Permanent model-input denylist. These TW public families are current snapshots
# without an immutable point-in-time archive. Keep their source columns for data
# provenance, but never allow them into a model feature schema.
FORBIDDEN_SNAPSHOT_ONLY_FEATURE_PATTERNS = (
    "twpub_monthly_revenue_*",
    "twpub_cumulative_revenue_yoy",
    "twpub_financial_*",
    "twpub_insider_*",
    "twpub_borrow_*",
    "twpub_sbl_*",
    "twpub_short_sale_available_*",
    "twpub_tdcc_*",
    "twpub_company_*",
)
DAY_TRADE_OPEN_GAP_FEATURE = "next_session_open_gap_logret"

# The first multi-action execution contract is deliberately narrow.  These are
# the two model families whose final stock scorer has an explicit action-channel
# axis; every other model still emits one scalar target per symbol.
_TW_PHASE_HEAD_MODEL_NAMES = frozenset(
    {
        "transformer_base_portfolio",
        "transformer_base_portfolio_model",
        "flash_transformer_portfolio",
        "scalable_transformer_portfolio",
        "multi_axis_transformer_portfolio",
        "tbp",
        "financial_transformer",
        "financial_transformer_model",
        "financial_token_transformer",
        "financial_tokenized_transformer",
    }
)
_TW_FINANCIAL_PHASE_HEAD_MODEL_NAMES = frozenset(
    {
        "financial_transformer",
        "financial_transformer_model",
        "financial_token_transformer",
        "financial_tokenized_transformer",
    }
)

# Keep this synchronized with the canonical phase-action boundary in
# stockagent.training.loss.risk_aware_loss.  Rank/factor/autoencoder objectives
# need a different label and attribution contract for [T,P,S] actions and must
# not silently flatten or select one phase.
_TW_PHASE_RETURN_OBJECTIVES = frozenset(
    {
        "log_utility",
        "log_util",
        "kelly",
        "growth",
        "mean_log_return",
    }
)


def _normalized_contract_name(value: object) -> str:
    return str(value).strip().lower().replace("-", "_")


def _validate_tw_phase_mode_contract(
    *,
    execution_mode: str,
    model_name: object,
    loss_type: object,
    model_portfolio_output_mode: object,
    trading_portfolio_activation: object,
    loss_portfolio_activation: object,
    return_rank_ic_weight: object,
    direction_weight: object,
    explain_after_each_fold: object,
) -> None:
    """Fail closed before a phase action is interpreted as a scalar target."""

    if execution_mode not in TW_CARRYING_EXECUTION_MODES:
        return

    normalized_model = _normalized_contract_name(model_name)
    if normalized_model not in _TW_PHASE_HEAD_MODEL_NAMES:
        raise ValueError(
            f"trading.execution_mode={execution_mode!r} requires a model with "
            "an explicit phase-action head; supported training.model_name "
            "families are 'transformer_base_portfolio' and "
            f"'financial_transformer', got {model_name!r}"
        )

    normalized_objective = _normalized_contract_name(loss_type)
    if normalized_objective not in _TW_PHASE_RETURN_OBJECTIVES:
        raise ValueError(
            f"trading.execution_mode={execution_mode!r} currently supports only "
            "canonical path-dependent log-utility objectives "
            f"{sorted(_TW_PHASE_RETURN_OBJECTIVES)}; rank, factor, "
            "autoencoder, and other scalar-target objectives are not defined "
            f"for phase actions, got training.loss_type={loss_type!r}"
        )

    unsupported_auxiliary_losses = {
        "training.multitask_loss.return_rank_ic_weight": float(
            return_rank_ic_weight
        ),
        "training.multitask_loss.direction_weight": float(direction_weight),
    }
    enabled_unsupported_auxiliary_losses = [
        name
        for name, weight in unsupported_auxiliary_losses.items()
        if weight > 0.0
    ]
    if enabled_unsupported_auxiliary_losses:
        raise ValueError(
            f"trading.execution_mode={execution_mode!r} does not define a "
            "single daily cross-sectional rank or direction target for "
            "multi-phase [T,P,S] actions; "
            + ", ".join(enabled_unsupported_auxiliary_losses)
            + " must be 0"
        )
    if bool(explain_after_each_fold):
        raise ValueError(
            f"trading.execution_mode={execution_mode!r} does not yet support "
            "training.explain_after_each_fold: phase actions [B,P,S] need "
            "phase-labelled attribution, and the tw_overnight due-exit "
            "fraction is not a signed portfolio exposure"
        )

    output_mode = normalize_portfolio_output_mode(
        str(model_portfolio_output_mode)
    )
    trading_activation = normalize_portfolio_activation(
        str(trading_portfolio_activation)
    )
    raw_loss_activation = _normalized_contract_name(
        loss_portfolio_activation
    )
    loss_activation = (
        trading_activation
        if raw_loss_activation
        in {"", "auto", "trading", "same", "same_as_trading"}
        else normalize_portfolio_activation(raw_loss_activation)
    )
    processed_output = output_mode != "logits"
    activations = {
        "trading.portfolio_activation": trading_activation,
        "training.loss_portfolio_activation": loss_activation,
    }
    if output_mode == "activation_l1":
        raise ValueError(
            f"trading.execution_mode={execution_mode!r} does not support "
            "portfolio_output_mode='activation_l1': the current phase model "
            "and resolved-action consumer share one portfolio_activation "
            "field, so pre_normalized consumption would silently replace the "
            "model activation with identity. Use portfolio_output_mode='l1' "
            "or 'logits' until those activation settings are separated."
        )
    if processed_output:
        mismatched = [
            name
            for name, activation in activations.items()
            if activation != "pre_normalized"
        ]
        if mismatched:
            raise ValueError(
                f"trading.execution_mode={execution_mode!r} with model "
                f"portfolio_output_mode={output_mode!r} already emits resolved "
                "phase actions; "
                + ", ".join(mismatched)
                + " must be 'pre_normalized' to prevent a second "
                "sigmoid/activation/L1 transform"
            )
    else:
        mismatched = [
            name
            for name, activation in activations.items()
            if activation == "pre_normalized"
        ]
        if mismatched:
            raise ValueError(
                f"trading.execution_mode={execution_mode!r} with model "
                "portfolio_output_mode='logits' emits unresolved raw phase "
                "logits; "
                + ", ".join(mismatched)
                + " cannot be 'pre_normalized'"
            )


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects ambiguous duplicate mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ValueError(
                f"Unhashable YAML mapping key at {key_node.start_mark}"
            ) from exc
        if duplicate:
            raise ValueError(
                f"Duplicate YAML key {key!r} at {key_node.start_mark}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)

_REMOVED_CONFIG_KEY_GUIDANCE = {
    "environment.target_vram_fraction": "use training.target_vram_fraction",
    "environment.conda_env": "select the runtime with scripts/runtime_env.sh, FINTECH_ENV_PATH, or PYTHON_BIN",
    "data.universe_mode": "the universe is derived from parquet files plus daily alive/tradable masks",
    "data.use_rapids": "use data.panel_backend (auto, polars_lazy, polars_streaming, or pyarrow)",
    "data.benchmark_required": "set data.benchmark_name to the required benchmark symbol",
    "data.benchmark_source": "set data.benchmark_name to the benchmark symbol",
    "trading.use_all_tradable_symbols": "use data.tradable_mode",
    "evaluation.primary_baseline": "use data.benchmark_name",
    "training.target": "use training.loss_type for the optimization objective",
    "training.batch_mode": "the canonical windowed tensor pipeline is selected automatically",
    "training.materialize_window_tensors": "the persistent materialized-window executor was removed; neural training always starts from lazy WindowedSplitTensors",
    "training.fused_log_utility_loss": "the fused shortcut was removed; use the canonical risk_aware_loss with optional training.compile_loss",
    "training.top_k": "use an active model's position-selection field when that model supports one",
    "training.prefer_fp16": "use environment.amp_dtype (bf16 is the baseline)",
    "training.data_parallel_device_ids": "launch distributed_data_parallel with torchrun and select devices via CUDA_VISIBLE_DEVICES",
    "training.data_parallel_output_device": "DataParallel was removed; distributed ranks select their own local device",
    "training.data_parallel_disable_panel_forward": "DataParallel was removed; the canonical DDP windowed path selects panel forward automatically",
    "training.data_parallel_compile_model": "use training.enable_torch_compile on the canonical single-device or DDP path",
    "training.data_parallel_threaded_replicas": "DataParallel was removed; use distributed_data_parallel",
    "training.save_daily_weights_csv": "use training.save_daily_weights_table",
    "training.save_integer_share_daily_weights_csv": "use training.save_integer_share_daily_weights_table",
    "training.save_integer_share_holdings_csv": "use training.save_integer_share_holdings_table",
    "training.cross_sectional_temporal_portfolio_model.stock_embedding_dim": "use d_model",
    "training.cross_sectional_temporal_portfolio_model.stock_hidden_dim": "use scorer_hidden",
    "training.cross_sectional_temporal_portfolio_model.stock_n_blocks": "use scorer_blocks",
    "training.cross_sectional_temporal_portfolio_model.cross_hidden_dim": "use d_model",
    "training.cross_sectional_temporal_portfolio_model.cross_heads": "use heads",
    "training.cross_sectional_temporal_portfolio_model.cross_layers": "use layers",
    "training.cross_sectional_temporal_portfolio_model.candidate_top_m": "use candidate_k",
    "training.cross_sectional_temporal_portfolio_model.portfolio_top_k": "use trade_k",
    "training.cross_sectional_temporal_portfolio_model.temporal_hidden_dim": "the model uses flattened lookback inputs and has no separate temporal branch",
    "training.cross_sectional_temporal_portfolio_model.temporal_blocks": "the model uses flattened lookback inputs and has no separate temporal branch",
    "training.cross_sectional_temporal_portfolio_model.temporal_kernel_size": "the model uses flattened lookback inputs and has no separate temporal branch",
}


def _is_strict_bool_annotation(annotation: Any) -> bool:
    if annotation is bool:
        return True
    args = set(get_args(annotation))
    return bool in args and args.issubset({bool, type(None)})


def _validate_config_bool_values(
    payload: dict[str, Any],
    dataclass_type: type[Any],
    *,
    section: str,
) -> None:
    hints = get_type_hints(dataclass_type)
    invalid: list[str] = []
    for key, annotation in hints.items():
        if key not in payload or payload[key] is None or not _is_strict_bool_annotation(annotation):
            continue
        value = payload[key]
        if type(value) is not bool:
            full_key = f"{section}.{key}" if section else key
            invalid.append(f"{full_key}={value!r} ({type(value).__name__})")
    if invalid:
        raise ValueError(
            "Boolean config values must use YAML true/false, not strings or numbers: "
            + ", ".join(invalid)
        )


def _validate_config_keys(
    payload: dict[str, Any],
    dataclass_type: type[Any],
    *,
    section: str,
) -> None:
    allowed = {item.name for item in fields(dataclass_type)}
    unknown = sorted(set(payload).difference(allowed))
    if unknown:
        details: list[str] = []
        for key in unknown:
            full_key = f"{section}.{key}" if section else key
            guidance = _REMOVED_CONFIG_KEY_GUIDANCE.get(full_key)
            details.append(f"{full_key} ({guidance})" if guidance else full_key)
        raise ValueError("Unknown or removed config key(s): " + ", ".join(details))
    _validate_config_bool_values(payload, dataclass_type, section=section)


def _dataclass_default_values(dataclass_type: type[Any]) -> dict[str, Any]:
    """Return independent defaults, representing nested dataclasses as mappings."""
    defaults: dict[str, Any] = {}
    for item in fields(dataclass_type):
        if item.default is not MISSING:
            value = deepcopy(item.default)
        elif item.default_factory is not MISSING:
            value = item.default_factory()
        else:
            continue
        defaults[item.name] = asdict(value) if is_dataclass(value) else deepcopy(value)
    return defaults


def _set_dataclass_defaults(
    payload: dict[str, Any],
    dataclass_type: type[Any],
    *,
    exclude: set[str] | None = None,
) -> None:
    excluded = exclude or set()
    for key, value in _dataclass_default_values(dataclass_type).items():
        if key not in excluded:
            payload.setdefault(key, value)


def _set_legacy_alias_defaults(payload: dict[str, Any], aliases: dict[str, Any]) -> None:
    """Apply only explicitly supplied legacy values before canonical defaults."""
    for key, value in aliases.items():
        if value is not MISSING:
            payload.setdefault(key, value)


def _deep_merge_config(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            merged[key] = _deep_merge_config(merged[key], value) if key in merged else value
        return merged
    return override


def _normalize_base_config_refs(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        return [str(value)]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item).strip()]
    raise ValueError(
        "config inheritance keys must be a path string or a list of path strings"
    )


def _load_raw_config(path: str | Path, stack: tuple[Path, ...] = ()) -> dict[str, Any]:
    config_path = Path(path).expanduser()
    if not config_path.is_absolute():
        config_path = config_path.resolve()
    if config_path in stack:
        cycle = " -> ".join(str(item) for item in (*stack, config_path))
        raise ValueError(f"Config inheritance cycle detected: {cycle}")
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.load(handle, Loader=_UniqueKeySafeLoader) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {config_path}")

    base_refs: list[str] = []
    for key in _CONFIG_INHERITANCE_KEYS:
        base_refs.extend(_normalize_base_config_refs(raw.pop(key, None)))
    if not base_refs:
        return raw

    merged: dict[str, Any] = {}
    next_stack = (*stack, config_path)
    for ref in base_refs:
        base_path = Path(ref).expanduser()
        if not base_path.is_absolute():
            base_path = config_path.parent / base_path
        merged = _deep_merge_config(merged, _load_raw_config(base_path, next_stack))
    return _deep_merge_config(merged, raw)


def _normalize_string_list(value: Any, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raise ValueError(f"{field_name} must be a list or comma-separated string, got {type(value).__name__}")

    items: list[str] = []
    for item in raw_items:
        text = str(item).strip()
        if not text or text.startswith("#"):
            continue
        items.append(text)
    return items


def _normalize_optional_positive_int(value: Any, *, field_name: str) -> int | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "0", "auto", "none", "off", "false"}:
        return None
    resolved = int(value)
    if resolved < 1:
        raise ValueError(f"{field_name} must be >= 1, got {resolved}")
    return resolved


@dataclass(slots=True)
class RunnerConfig:
    output_dir: str = "artifacts"
    require_cuda: bool = True
    mode: str = "train"
    resume: bool = True
    post_train_infer: bool = True
    start_fold: int | None = None


@dataclass(slots=True)
class EnvironmentConfig:
    device: str
    use_tensor_cores: bool
    amp_dtype: str
    cudnn_benchmark: bool = True
    cpu_threads: int | None = None
    torch_compile_threads: int | None = None


@dataclass(slots=True)
class DataConfig:
    parquet_root: str
    benchmark_name: str
    # Inclusive lower bound for the model panel.  Source archives may retain
    # older rows for provenance even when that interval cannot support an
    # unbiased training universe.
    panel_start_date: str | None = None
    security_filter: str = "none"
    usd_only_trading_pairs: bool = False
    tradable_mode: str = "tradable"
    trading_volume_policy: str = "auto"
    panel_backend: str = "auto"
    panel_load_workers: int = 4
    live_tail_panel_rows: int = 0
    use_tw_public_features: bool = False
    use_tw_public_rules: bool = False
    tw_public_feature_path: str = "data_tw_public/features/tw_public_stock_daily.parquet"
    tw_public_market_symbol: str = "__MARKET__"
    feature_include: list[str] = field(default_factory=list)
    feature_exclude: list[str] = field(default_factory=list)
    feature_zero_fill: list[str] = field(default_factory=list)
    # Explicitly append the open[t]/close[t-1] execution-context feature.  It
    # is valid only for tw_day_trade and is never part of the default schema.
    day_trade_open_feature: bool = False
    # Features whose session-t value is not available early enough for a
    # session-t decision.  Panel construction exposes that value on the next
    # panel session while preserving the source's original dated archive.
    feature_shift_next_session: list[str] = field(default_factory=list)
    # Explicit research-only opt-in for using final session-t aggregates in a
    # model that approximates execution at that same close.
    allow_same_close_feature_approximation: bool = False


@dataclass(slots=True)
class WalkForwardConfig:
    min_train_years: int = 1
    val_years: int = 1
    require_future_test_year: bool = True
    expected_first_year: int | None = None
    require_contiguous_years: bool = False
    # ``split_only`` requires every feature window to begin inside its owned
    # train/validation/test split. ``panel_history`` keeps target ownership
    # unchanged while allowing the window to read older, already-observed panel
    # rows. This is the causal long-lookback mode.
    lookback_context: str = "split_only"
    # Optional first year that owns train/validation/test targets. Older panel
    # years remain available only as lookback context when requested above.
    split_start_year: int | None = None


@dataclass(slots=True)
class TradingConfig:
    frequency: str
    buy_fee_rate: float
    sell_fee_rate: float
    long_only: bool
    max_turnover_ratio: float = 0.0
    max_volume_participation: float = 0.0
    volume_participation_equity: float = 1_000_000.0
    # Reporting/post-processing multiplier only. Canonical train/eval exposure is 1.0.
    reporting_leverage: float = 1.0
    min_trade_weight: float = 0.0
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION
    # Execution accounting is selected independently from portfolio construction.
    # ``naive`` preserves the historical continuous-weight, immediate-fee path.
    execution_mode: str = "naive"
    tw_commission_rate: float = 0.001425
    tw_commission_discount: float = 0.6
    tw_stock_sell_tax: float = 0.003
    tw_etf_sell_tax: float = 0.001
    tw_day_trade_stock_sell_tax: float = 0.0015
    tw_day_trade_etf_sell_tax: float = 0.001
    # Optional broker/account profile for exact integer evaluation.  Defaults
    # intentionally preserve pure proportional fees; e.g. a 20 TWD minimum is
    # common but not universal across brokers and order channels.
    tw_minimum_commission: float = 0.0
    tw_commission_rounding: str = "none"
    tw_tax_rounding: str = "none"
    tw_settlement_lag_sessions: int = 2
    # ``exact`` consumes receipt-verified issuer cash amounts/payment dates;
    # unsupported events still use pre-event liquidation. ``avoid`` liquidates
    # before every known action and creates no entitlement claim.
    tw_corporate_action_mode: str = "avoid"
    tw_corporate_action_claim_queue_sessions: int = 256
    tw_cash_lot_size: int = 1
    tw_day_trade_lot_size: int = 1000
    # A negative tw_cash position is a separate margin-short liability, never
    # a negative cash-share holding.  These fields are serialized as part of
    # TradingConfig and therefore participate in checkpoint compatibility.
    tw_short_initial_margin_rate: float = 0.9
    tw_short_maintenance_ratio: float = 1.3
    tw_short_lot_size: int = 1000
    tw_short_handling_fee_rate: float = 0.0
    # Broker borrow inventory is not universally available historically.  Keep
    # the realistic fail-closed capacity ceiling by default, but allow an
    # explicit counterfactual account contract that leaves eligible shorts
    # uncapped while preserving every other short-sale rule.
    tw_short_capacity_limit_enabled: bool = True


@dataclass(slots=True)
class MLPModelConfig:
    hidden_dim: int = 128
    hidden_layers: int = 2
    embedding_dim: int = 64
    dropout: float = 0.1


@dataclass(slots=True)
class FTTransformerModelConfig:
    d_token: int = 64
    n_layers: int = 2
    n_heads: int = 4
    ffn_dim: int = 256
    dropout: float = 0.1
    use_cls_token: bool = True


@dataclass(slots=True)
class TabularResNetModelConfig:
    embedding_dim: int = 64
    hidden_dim: int = 128
    n_blocks: int = 4
    dropout: float = 0.1


@dataclass(slots=True)
class TemporalTabularResNetModelConfig:
    temporal_hidden_dim: int = 64
    temporal_layers: int = 1
    temporal_dropout: float = 0.1
    embedding_dim: int = 64
    hidden_dim: int = 128
    n_blocks: int = 4
    dropout: float = 0.1


@dataclass(slots=True)
class TCNHybridTabularResNetModelConfig:
    embedding_dim: int = 64
    encoder_hidden_dim: int = 128
    encoder_blocks: int = 2
    tcn_blocks: int = 3
    tcn_kernel_size: int = 3
    dropout: float = 0.1


@dataclass(slots=True)
class MultiStockTCNModelConfig:
    hidden_channels: int = 64
    embedding_dim: int = 64
    tcn_blocks: int = 4
    tcn_kernel_size: int = 3
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    tcn_conv_mode: str = "separable"
    conv_layers_per_block: int = 1
    norm_type: str = "none"
    sanitize_inputs: bool = False


@dataclass(slots=True)
class EfficientTCNTabularSetPortfolioModelConfig:
    temporal_enabled: bool = True
    temporal_dim: int = 16
    temporal_hidden_channels: int = 32
    temporal_dilations: list[int] = field(default_factory=lambda: [1, 2])
    temporal_kernel_size: int = 3
    tabular_dim: int = 64
    tabular_hidden_dim: int = 128
    tabular_blocks: int = 2
    model_dim: int = 64
    set_enabled: bool = True
    num_inducing_points: int = 16
    num_heads: int = 4
    ffn_mult: int = 2
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    residual_scale: float = 0.5
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True


@dataclass(slots=True)
class LatentFactorMarketTokenPortfolioModelConfig:
    temporal_enabled: bool = True
    temporal_dim: int = 16
    temporal_hidden_channels: int = 32
    temporal_dilations: list[int] = field(default_factory=lambda: [1, 2])
    temporal_kernel_size: int = 3
    tabular_dim: int = 64
    tabular_hidden_dim: int = 128
    tabular_blocks: int = 2
    stock_embedding_dim: int = 64
    num_latent_factors: int = 32
    num_market_tokens: int = 4
    num_heads: int = 4
    ffn_mult: int = 2
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    residual_scale: float = 0.5
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True


@dataclass(slots=True)
class LowRankMarketTransformerPortfolioModelConfig:
    feature_dim: int = 24
    temporal_mixer: str = "conv"
    temporal_layers: int = 1
    temporal_heads: int = 2
    temporal_ffn_dim: int = 48
    temporal_dropout: float = 0.1
    temporal_pooling: str = "last"
    temporal_kernel_size: int = 5
    temporal_dilations: list[int] = field(default_factory=lambda: [1])
    temporal_checkpoint: bool = True
    stock_embedding_dim: int = 24
    num_latent_factors: int = 8
    num_market_tokens: int = 4
    cross_heads: int = 2
    cross_ffn_mult: int = 1
    head_hidden_dim: int = 24
    head_layers: int = 1
    dropout: float = 0.1
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True
    return_aux_details: bool = False


@dataclass(slots=True)
class TransformerBasePortfolioModelConfig:
    d_model: int = 64
    attention_mode: str = "latent"
    use_latent_factors: bool | None = None
    use_market_tokens: bool | None = None
    use_flash_attention: bool = True
    use_time_pos: bool = True
    use_symbol_pos: bool = True
    symbol_position_capacity: int | None = None
    input_dropout: float = 0.0
    sanitize_inputs: bool = True
    amp_native_position_add: bool = False
    temporal_self_attention_fast_path: bool = False
    compiled_cross_attention_backend: str = "auto"
    sdpa_batch_limit: int = 4096
    norm_type: str = "rmsnorm"
    ffn_type: str = "swiglu"
    qk_norm: bool = True
    rope_temporal: bool = True
    rope_base: float = 10000.0
    temporal_layers: int = 2
    temporal_heads: int = 4
    temporal_ffn_mult: int = 2
    temporal_pooling: str = "attention"
    temporal_query_mode: str = "full_then_last"
    cross_layers: int = 1
    cross_heads: int = 4
    cross_ffn_mult: int = 2
    joint_layers: int = 2
    joint_heads: int = 4
    joint_ffn_mult: int = 2
    latent_layers: int = 1
    num_latent_factors: int = 16
    num_market_tokens: int = 4
    market_layers: int = 1
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    portfolio_output_mode: str = "activation_l1"
    center_long_short_logits: bool = True
    max_full_tokens: int = 4096
    checkpoint_blocks: bool = False
    return_aux: bool = True
    return_aux_details: bool = False
    categorical_feature_names: list[str] = field(
        default_factory=lambda: [
            "twpub_company_industry_code",
            "twpub_company_is_foreign",
            "twpub_company_has_preferred_stock",
        ]
    )
    categorical_embedding_dim: int = 4
    categorical_embedding_cardinality: int = 512


@dataclass(slots=True)
class FinancialTransformerModelConfig(TransformerBasePortfolioModelConfig):
    candle_dropout: float = 0.0


@dataclass(slots=True)
class GradientBoostedPortfolioTransformerConfig:
    d_model: int = 64
    temporal_layers: int = 2
    temporal_heads: int = 4
    temporal_ffn_mult: int = 2
    market_layers: int = 1
    market_heads: int = 4
    market_ffn_mult: int = 2
    num_market_tokens: int = 4
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    input_dropout: float = 0.0
    use_time_pos: bool = True
    use_symbol_pos: bool = False
    dynamic_market_tokens: bool = True
    dynamic_token_gate_init: float = 0.1
    num_residual_stages: int = 2
    stage_eta: list[float] = field(default_factory=lambda: [0.5, 0.25])
    trainable_eta: bool = True
    eta_max: float = 1.0
    detach_stage_condition: bool = True
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    portfolio_output_mode: str = "projection_l1"
    center_final_logits: bool = True
    return_aux: bool = True
    return_aux_details: bool = False


@dataclass(slots=True)
class BottleneckPortfolioAutoencoderConfig:
    d_model: int = 128
    z_dim: int = 32
    temporal_type: str = "gru"
    temporal_layers: int = 1
    asset_encoder_type: str = "transformer"
    asset_encoder_layers: int = 2
    n_heads: int = 4
    num_inducing_points: int = 32
    ffn_mult: int = 2
    dropout: float = 0.1
    long_short: bool = True
    noise_std: float = 0.01
    return_aux: bool = True


@dataclass(slots=True)
class CrossSectionalTemporalPortfolioModelConfig:
    dropout: float = 0.1
    regime_classes: int = 3
    candidate_k: int = 64
    trade_k: int = 10
    scorer_hidden: int = 128
    scorer_blocks: int = 2
    d_model: int = 128
    heads: int = 4
    layers: int = 2


@dataclass(slots=True)
class MultitaskLossConfig:
    rank_ic_weight: float = 0.20
    return_rank_ic_weight: float = 0.0
    direction_weight: float = 0.05
    volatility_regime_weight: float = 0.05
    concentration_weight: float = 0.005
    net_exposure_weight: float = 0.0
    regime_up_threshold: float = 0.002
    regime_down_threshold: float = -0.002


@dataclass(slots=True)
class FactorGeneralizationLossConfig:
    slope_tstat_weight: float = 1.0
    rank_ic_weight: float = 0.5
    factor_sharpe_weight: float = 0.25
    block_stability_weight: float = 0.20
    regime_stability_weight: float = 0.20
    consistency_weight: float = 0.05
    net_exposure_weight: float = 0.05
    gross_exposure_weight: float = 0.02
    concentration_weight: float = 0.02
    turnover_weight: float = 0.02
    score_l2_weight: float = 0.001
    factor_temperature: float = 1.0
    block_count: int = 4
    worst_fraction: float = 0.25
    augmentation_feature_dropout: float = 0.10
    augmentation_stock_dropout: float = 0.05
    augmentation_time_dropout: float = 0.05
    augmentation_noise_std: float = 0.01


@dataclass(slots=True)
class PortfolioAutoencoderLossConfig:
    lambda_turnover: float = 0.1
    lambda_concentration: float = 0.01
    lambda_latent: float = 0.001


@dataclass(slots=True)
class LightGBMModelConfig:
    use_gpu: bool = True
    gpu_device_id: int = 0
    n_estimators: int = 300
    num_leaves: int = 63
    max_depth: int = -1
    learning_rate: float = 0.05
    subsample: float = 0.9
    colsample_bytree: float = 0.9
    reg_lambda: float = 1.0
    n_jobs: int = -1
    random_state: int = 42


@dataclass(slots=True)
class XGBoostModelConfig:
    use_gpu: bool = True
    gpu_device_id: int = 0
    n_estimators: int = 300
    max_depth: int = 8
    learning_rate: float = 0.05
    subsample: float = 0.9
    colsample_bytree: float = 0.9
    reg_lambda: float = 1.0
    n_jobs: int = -1
    random_state: int = 42


@dataclass(slots=True)
class TrainingConfig:
    non_blocking_transfer: bool
    model_name: str = "mlp"
    seed: int = 42
    multi_gpu_strategy: str = "auto"
    ddp_bucket_cap_mb: int = 4
    enable_torch_compile: bool = True
    auto_torch_compile_sharpe: bool = False
    torch_compile_mode: str = "reduce-overhead"
    torchinductor_cache_dir: str = "~/.cache/torchinductor"
    triton_cache_dir: str = "~/.cache/triton"
    cuda_cache_path: str = "~/.cache/nv_cuda"
    compile_loss: bool | None = None
    # Executor-only optimization: compile the panel-slab model with a symbolic
    # stock axis so expanding walk-forward folds can reuse one Inductor graph.
    # Batch/time/feature axes remain static and assets are never padded.
    compile_model_dynamic_symbols: bool = False
    # Executor-only optimization: keep the train batch/time axis static while
    # allowing one compiled canonical-loss graph to serve changing symbol
    # counts across expanding walk-forward folds.
    compile_loss_dynamic_symbols: bool = False
    # Compile a separate inference-mode fixed-shape panel-slab forward for
    # repeated validation/test passes. This never shares the train autograd
    # graph because grad mode is a distinct compiler ABI.
    compile_eval_model: bool = False
    loss_portfolio_activation: str = "auto"
    loss_min_trade_weight: float | None = None
    warm_start_from_previous_fold: bool = False
    chunk_rows: int = 0
    eval_model_chunk_rows: int | str = "auto"
    eval_backtest_chunk_rows: int = 512
    eval_backtest_chunk_rows_auto: bool = True
    eval_backtest_compile: bool | None = None
    eval_auto_chunk_rows_cap: int = 16
    train_symbol_compaction: str = "none"
    train_symbol_compaction_bucket_size: int = 0
    backtest_autotune: bool = True
    backtest_compile: bool = True
    backtest_compile_stateful: bool = True
    backtest_compile_dynamic: bool = False
    # Compile Taiwan's sequential settlement ledger in bounded time chunks.
    # Zero disables chunk compilation; eight avoids full-horizon graph blowup.
    tw_continuous_compile_chunk_rows: int = 8
    # Bound reverse-mode recurrence without changing the forward settlement
    # ledger. Zero keeps full-horizon BPTT; a positive value detaches only the
    # carried normalized account state at this many-session boundaries.
    tw_continuous_gradient_horizon_rows: int = 32
    inference_backtest_autotune: bool | None = None
    inference_backtest_compile: bool | None = None
    backtest_verbose: bool = False
    strict_no_fallback: bool = False
    backtest_checkpoint_chunk_rows: int = 0
    runtime_shape_check: bool = False
    allow_dynamic_symbols: bool = True
    lookback: int = 1
    batch_size_train: int = 32
    batch_size_eval: int = 32
    min_batch_size: int = 1
    auto_batch_size: bool = False
    vram_budget_gb: float = 8.0
    vram_safety_margin_gb: float = 1.0
    target_vram_fraction: float = 0.85
    epochs: int = 10
    early_stopping_no_improve_ratio: float = 0.2
    early_stopping_min_delta: float = 0.0
    best_checkpoint_max_epoch: int = 0
    val_interval_epochs: int = 1
    curve_test_interval: int = 1
    record_epoch_curve: bool = True
    curve_plot_interval: int = 1
    curve_plot_async: bool = True
    plot_backend: str = "auto"
    epoch_test_curve: bool = True
    defer_epoch_curve_plot_until_end: bool = True
    debug_timing_sync: bool = False
    explain_after_each_fold: bool = False
    explain_top_k: int = 20
    explain_max_rows: int = 32
    explain_ig_steps: int = 0
    explain_ig_batch_size: int = 1
    explain_sample_method: str = "even"
    explain_perturb: bool = False
    explain_perturb_batch_size: int = 1
    explain_perturb_max_auto_batch_size: int = 1
    explain_perturb_max_input_elements: int = 8_000_000
    explain_counterfactual_compile: bool = False
    explain_write_plots: bool = False
    explain_report_style: str = "none"
    explain_plot_theme: str = "paper"
    explain_standard_plots: bool = False
    explain_interactive_plots: bool = False
    explain_shap_enabled: bool = False
    explain_shap_mode: str = "score_head_surrogate"
    explain_j_lens_enabled: bool = False
    explain_j_lens_intervention_fraction: float = 0.01
    explain_case_study_top_k: int = 5
    explain_regime_analysis: bool = False
    explain_fold_stability: bool = False
    explain_umap_enabled: bool = False
    explain_umap_max_points: int = 1000
    explain_umap_max_projections: int = 0
    explain_umap_n_neighbors: int = 15
    explain_umap_min_dist: float = 0.1
    explain_cross_asset_enabled: bool = False
    explain_cross_asset_max_sources: int = 8
    explain_cross_asset_max_targets: int = 8
    explain_cross_asset_top_edges: int = 150
    explain_cross_asset_source_chunk_size: int = 1
    explain_cross_asset_max_repeated_rows: int = 48
    explain_cross_asset_perturb_scale: float = 1.0
    explain_cross_asset_shocks: list[str] = field(
        default_factory=lambda: ["zero", "momentum", "gap", "volume", "volatility", "liquidity"]
    )
    explain_cross_asset_attention_flow: bool = True
    explain_cross_asset_attention_capture_rows: int = 1
    explain_cross_asset_validated_transmission: bool = True
    explain_cross_asset_role_embedding: bool = False
    explain_cross_asset_graph_backend: str = "cugraph"
    explain_cross_asset_graph_benchmark_min_edges: int = 1_000_000
    explain_cross_asset_graph_explainability: bool = True
    explain_cross_asset_graph_betweenness_max_vertices: int = 512
    explain_cross_asset_graph_plot_max_nodes: int = 80
    table_output_format: str = "csv"
    save_daily_weights_table: bool = True
    save_integer_share_daily_weights_table: bool = True
    save_integer_share_holdings_table: bool = True
    backtest_artifact_compression: str = "none"
    save_best_val_artifacts: bool = False
    save_best_val_fold_artifacts: bool = False
    save_best_val_fold_plots: bool = False
    postprocess_benchmark_after_fold: bool = False
    postprocess_benchmark_after_best_val: bool = False
    postprocess_benchmark_split: str = "test"
    postprocess_benchmark_activations: str = "identity,softsign,tanh,isru,erf,atan,gd"
    postprocess_benchmark_thresholds: str = "0,0.0001,0.00025,0.0005,0.001,0.0025,0.005,0.01,0.02"
    postprocess_benchmark_rank_metric: str = "sharpe"
    postprocess_benchmark_plot_metrics: str = "sharpe,sortino,cumulative_return"
    postprocess_benchmark_plot_top_n: int = 20
    postprocess_benchmark_backtest_compile: bool = False
    postprocess_benchmark_max_rows: int = 0
    postprocess_benchmark_strict: bool = False
    cache_train_tensors_on_gpu: bool = True
    cache_eval_tensors_on_gpu: bool = True
    cache_train_features_in_amp_dtype: bool = False
    learning_rate: float = 1e-3
    enable_lr_scheduler: bool = True
    lr_scheduler: str = "none"  # "none", "cosine", "step", "plateau"
    lr_scheduler_t_max: int = 0
    lr_scheduler_eta_min: float = 1e-5
    lr_scheduler_warmup_steps: int = 0
    lr_scheduler_step_size: int = 50
    lr_scheduler_gamma: float = 0.5
    lr_scheduler_patience: int = 5
    lr_scheduler_threshold: float = 1e-4
    weight_decay: float = 1e-5
    grad_clip_norm: float = 1.0
    finite_check_interval_steps: int = 0
    checkpoint_finite_check: bool = True
    loss_type: str = "mse"  # "mse", "pure_rank", "rank_ic", "sharpe", "sortino", "log_utility", etc.
    mlp: MLPModelConfig = field(default_factory=MLPModelConfig)
    ft_transformer: FTTransformerModelConfig = field(default_factory=FTTransformerModelConfig)
    tabular_resnet: TabularResNetModelConfig = field(default_factory=TabularResNetModelConfig)
    multi_stock_tcn: MultiStockTCNModelConfig = field(default_factory=MultiStockTCNModelConfig)
    efficient_tcn_tabular_set_portfolio: EfficientTCNTabularSetPortfolioModelConfig = field(
        default_factory=EfficientTCNTabularSetPortfolioModelConfig
    )
    latent_factor_market_token_portfolio: LatentFactorMarketTokenPortfolioModelConfig = field(
        default_factory=LatentFactorMarketTokenPortfolioModelConfig
    )
    low_rank_market_transformer_portfolio: LowRankMarketTransformerPortfolioModelConfig = field(
        default_factory=LowRankMarketTransformerPortfolioModelConfig
    )
    transformer_base_portfolio: TransformerBasePortfolioModelConfig = field(
        default_factory=TransformerBasePortfolioModelConfig
    )
    financial_transformer: FinancialTransformerModelConfig = field(
        default_factory=FinancialTransformerModelConfig
    )
    gradient_boosted_portfolio_transformer: GradientBoostedPortfolioTransformerConfig = field(
        default_factory=GradientBoostedPortfolioTransformerConfig
    )
    bottleneck_portfolio_autoencoder: BottleneckPortfolioAutoencoderConfig = field(default_factory=BottleneckPortfolioAutoencoderConfig)
    tcn_hybrid_tabular_resnet: TCNHybridTabularResNetModelConfig = field(default_factory=TCNHybridTabularResNetModelConfig)
    temporal_tabular_resnet: TemporalTabularResNetModelConfig = field(default_factory=TemporalTabularResNetModelConfig)
    cross_sectional_temporal_portfolio_model: CrossSectionalTemporalPortfolioModelConfig = field(default_factory=CrossSectionalTemporalPortfolioModelConfig)
    multitask_loss: MultitaskLossConfig = field(default_factory=MultitaskLossConfig)
    factor_generalization_loss: FactorGeneralizationLossConfig = field(default_factory=FactorGeneralizationLossConfig)
    portfolio_autoencoder_loss: PortfolioAutoencoderLossConfig = field(default_factory=PortfolioAutoencoderLossConfig)
    lightgbm: LightGBMModelConfig = field(default_factory=LightGBMModelConfig)
    xgboost: XGBoostModelConfig = field(default_factory=XGBoostModelConfig)


@dataclass(slots=True)
class EvaluationConfig:
    gamma_sharpe: float = 1.0
    gamma_excess: float = 1.0
    gamma_cvar: float = 1.0
    cvar_alpha: float = 0.95
    gamma_drawdown: float = 0.0
    drawdown_target: float = 0.2
    gamma_turnover: float = 0.0
    gamma_underperformance: float = 1.0
    excess_target: float = 0.0
    cvar_budget: float = 0.03
    drawdown_budget: float = 0.2
    turnover_budget: float = 0.3
    gamma_cvar_budget: float = 1.0
    gamma_drawdown_budget: float = 1.0
    gamma_turnover_budget: float = 0.0
    eval_log_utility_pre_log_power: float = 0.0
    eval_log_utility_periods_per_year: float = 252.0
    eval_log_utility_log_shift: float = 0.0


@dataclass(slots=True)
class ExperimentConfig:
    experiment_name: str
    runner: RunnerConfig
    environment: EnvironmentConfig
    data: DataConfig
    walk_forward: WalkForwardConfig
    trading: TradingConfig
    training: TrainingConfig
    evaluation: EvaluationConfig


def _nested_training_schemas() -> dict[str, type[Any]]:
    return {
        "mlp": MLPModelConfig,
        "ft_transformer": FTTransformerModelConfig,
        "tabular_resnet": TabularResNetModelConfig,
        "multi_stock_tcn": MultiStockTCNModelConfig,
        "efficient_tcn_tabular_set_portfolio": EfficientTCNTabularSetPortfolioModelConfig,
        "latent_factor_market_token_portfolio": LatentFactorMarketTokenPortfolioModelConfig,
        "low_rank_market_transformer_portfolio": LowRankMarketTransformerPortfolioModelConfig,
        "transformer_base_portfolio": TransformerBasePortfolioModelConfig,
        "financial_transformer": FinancialTransformerModelConfig,
        "gradient_boosted_portfolio_transformer": GradientBoostedPortfolioTransformerConfig,
        "bottleneck_portfolio_autoencoder": BottleneckPortfolioAutoencoderConfig,
        "tcn_hybrid_tabular_resnet": TCNHybridTabularResNetModelConfig,
        "temporal_tabular_resnet": TemporalTabularResNetModelConfig,
        "cross_sectional_temporal_portfolio_model": CrossSectionalTemporalPortfolioModelConfig,
        "multitask_loss": MultitaskLossConfig,
        "factor_generalization_loss": FactorGeneralizationLossConfig,
        "portfolio_autoencoder_loss": PortfolioAutoencoderLossConfig,
        "lightgbm": LightGBMModelConfig,
        "xgboost": XGBoostModelConfig,
    }


def _validate_raw_config_bool_types(raw: dict[str, Any]) -> None:
    section_schemas: dict[str, type[Any]] = {
        "runner": RunnerConfig,
        "environment": EnvironmentConfig,
        "data": DataConfig,
        "walk_forward": WalkForwardConfig,
        "trading": TradingConfig,
        "training": TrainingConfig,
        "evaluation": EvaluationConfig,
    }
    for section_name, schema in section_schemas.items():
        payload = raw.get(section_name)
        if payload is None:
            continue
        if not isinstance(payload, dict):
            raise ValueError(f"Config section {section_name!r} must be a YAML mapping")
        _validate_config_bool_values(payload, schema, section=section_name)

    training = raw.get("training")
    if not isinstance(training, dict):
        return
    for section_name, schema in _nested_training_schemas().items():
        payload = training.get(section_name)
        if payload is None:
            continue
        if not isinstance(payload, dict):
            raise ValueError(f"Config section 'training.{section_name}' must be a YAML mapping")
        _validate_config_bool_values(payload, schema, section=f"training.{section_name}")


def _merge_defaults(raw: dict[str, Any]) -> dict[str, Any]:
    runner = raw.setdefault("runner", {})
    _set_dataclass_defaults(runner, RunnerConfig)

    walk_forward = raw.setdefault("walk_forward", {})
    _set_dataclass_defaults(walk_forward, WalkForwardConfig)
    walk_forward["lookback_context"] = normalize_lookback_context(
        walk_forward["lookback_context"]
    )
    if walk_forward["split_start_year"] is not None:
        walk_forward["split_start_year"] = int(walk_forward["split_start_year"])

    environment = raw.setdefault("environment", {})
    _set_dataclass_defaults(environment, EnvironmentConfig)
    environment["cpu_threads"] = _normalize_optional_positive_int(
        environment.get("cpu_threads"),
        field_name="environment.cpu_threads",
    )
    environment["torch_compile_threads"] = _normalize_optional_positive_int(
        environment.get("torch_compile_threads"),
        field_name="environment.torch_compile_threads",
    )

    training = raw.setdefault("training", {})
    if "eval_backtest_engine" in training:
        raise ValueError(
            "training.eval_backtest_engine has been removed; "
            "eval/test backtests use the canonical Torch engine plus optional torch.compile."
        )
    legacy_batch_size = training.pop("batch_size", None)
    if legacy_batch_size is not None:
        training.setdefault("batch_size_train", legacy_batch_size)
        training.setdefault("batch_size_eval", legacy_batch_size)

    # This key was exposed for several releases but was never consumed by any
    # executor.  Accept the historical no-op default so existing configs keep
    # loading, while rejecting non-default values rather than pretending that
    # symbol subsampling happened.
    legacy_symbol_subsample = training.pop("train_symbol_subsample_ratio", None)
    if legacy_symbol_subsample is not None and float(legacy_symbol_subsample) != 1.0:
        raise ValueError(
            "training.train_symbol_subsample_ratio was removed because symbol "
            "subsampling was never implemented; remove the key (only the legacy "
            "no-op value 1.0 can be migrated)."
        )

    # Scheduler cadence was exposed as a setting but never selected runtime
    # behavior: warmup-cosine always steps per batch and every other scheduler
    # always steps per epoch. Accept historical spellings while removing the
    # misleading public knob.
    legacy_scheduler_interval = training.pop("lr_scheduler_interval", None)
    if legacy_scheduler_interval is not None:
        normalized_interval = str(legacy_scheduler_interval).strip().lower()
        if normalized_interval not in {"step", "batch", "epoch"}:
            raise ValueError(
                "training.lr_scheduler_interval was removed because scheduler "
                "cadence is fixed by scheduler type; legacy values must be one "
                "of: step, batch, epoch."
            )

    legacy_table_aliases = {
        "save_daily_weights_csv": "save_daily_weights_table",
        "save_integer_share_daily_weights_csv": "save_integer_share_daily_weights_table",
        "save_integer_share_holdings_csv": "save_integer_share_holdings_table",
    }
    for legacy_key, canonical_key in legacy_table_aliases.items():
        legacy_value = training.pop(legacy_key, None)
        if legacy_value is not None and type(legacy_value) is not bool:
            raise ValueError(
                f"training.{legacy_key} must use YAML true/false, got "
                f"{legacy_value!r} ({type(legacy_value).__name__})"
            )
        if legacy_value is not None:
            training.setdefault(canonical_key, legacy_value)

    # Preserve the historical master-switch shorthand before dataclass defaults
    # make it impossible to distinguish an omitted child flag from explicit false.
    if "save_best_val_artifacts" not in training and "save_best_val_fold_artifacts" in training:
        training["save_best_val_artifacts"] = training["save_best_val_fold_artifacts"]
    if "save_best_val_fold_artifacts" not in training and "save_best_val_artifacts" in training:
        training["save_best_val_fold_artifacts"] = training["save_best_val_artifacts"]
    if "save_best_val_fold_plots" not in training and "save_best_val_fold_artifacts" in training:
        training["save_best_val_fold_plots"] = training["save_best_val_fold_artifacts"]

    nested_training_keys = set(_nested_training_schemas())
    _set_dataclass_defaults(training, TrainingConfig, exclude=nested_training_keys)

    multi_gpu_strategy = str(training["multi_gpu_strategy"]).strip().lower().replace("-", "_")
    multi_gpu_aliases = {
        "": "none",
        "0": "none",
        "false": "none",
        "off": "none",
        "no": "none",
        "single": "none",
        "single_gpu": "none",
        "auto": "auto",
        "ddp": "distributed_data_parallel",
        "distributed": "distributed_data_parallel",
        "distributed_data_parallel": "distributed_data_parallel",
        "torch_ddp": "distributed_data_parallel",
    }
    if multi_gpu_strategy in {"dp", "dataparallel", "data_parallel", "torch_data_parallel"}:
        raise ValueError(
            "training.multi_gpu_strategy='data_parallel' has been removed; "
            "use 'distributed_data_parallel' with torchrun"
        )
    multi_gpu_strategy = multi_gpu_aliases.get(multi_gpu_strategy, multi_gpu_strategy)
    if multi_gpu_strategy not in {"auto", "none", "distributed_data_parallel"}:
        raise ValueError("training.multi_gpu_strategy must be one of: auto, none, distributed_data_parallel")
    training["multi_gpu_strategy"] = multi_gpu_strategy
    training["ddp_bucket_cap_mb"] = int(training["ddp_bucket_cap_mb"])
    training["tw_continuous_compile_chunk_rows"] = max(
        0,
        int(training["tw_continuous_compile_chunk_rows"]),
    )
    training["tw_continuous_gradient_horizon_rows"] = max(
        0,
        int(training["tw_continuous_gradient_horizon_rows"]),
    )
    tw_compile_rows = training["tw_continuous_compile_chunk_rows"]
    tw_gradient_rows = training["tw_continuous_gradient_horizon_rows"]
    if (
        tw_compile_rows > 0
        and tw_gradient_rows > 0
        and tw_gradient_rows % tw_compile_rows != 0
    ):
        raise ValueError(
            "training.tw_continuous_gradient_horizon_rows must be zero or "
            "an exact multiple of tw_continuous_compile_chunk_rows"
        )
    training["best_checkpoint_max_epoch"] = max(0, int(training["best_checkpoint_max_epoch"]))
    save_best_val_artifacts = bool(training["save_best_val_artifacts"])
    training["save_best_val_artifacts"] = save_best_val_artifacts
    training["save_best_val_fold_artifacts"] = (
        bool(training["save_best_val_fold_artifacts"]) and save_best_val_artifacts
    )
    training["save_best_val_fold_plots"] = (
        bool(training["save_best_val_fold_plots"]) and training["save_best_val_fold_artifacts"]
    )
    postprocess_split = str(training["postprocess_benchmark_split"]).strip().lower()
    if postprocess_split not in {"train", "val", "test"}:
        raise ValueError("training.postprocess_benchmark_split must be one of: train, val, test")
    training["postprocess_benchmark_split"] = postprocess_split
    postprocess_metric = str(training["postprocess_benchmark_rank_metric"]).strip().lower()
    metric_aliases = {
        "sharp": "sharpe",
        "cum_return": "cumulative_return",
        "return": "cumulative_return",
        "returns": "cumulative_return",
        "total_return": "cumulative_return",
        "total_returns": "cumulative_return",
    }
    postprocess_metric = metric_aliases.get(postprocess_metric, postprocess_metric)
    valid_postprocess_metrics = {
        "sharpe",
        "sortino",
        "calmar",
        "cumulative_return",
        "annualized_return",
        "cagr",
        "daily_hit_rate",
        "excess_return_vs_benchmark",
        "max_drawdown",
        "turnover",
    }
    if postprocess_metric not in valid_postprocess_metrics:
        raise ValueError(
            "training.postprocess_benchmark_rank_metric must be one of "
            f"{sorted(valid_postprocess_metrics)}, got {postprocess_metric!r}"
        )
    training["postprocess_benchmark_rank_metric"] = postprocess_metric
    plot_metrics: list[str] = []
    for raw_metric in str(training["postprocess_benchmark_plot_metrics"]).split(","):
        metric = metric_aliases.get(raw_metric.strip().lower().replace("-", "_"), raw_metric.strip().lower().replace("-", "_"))
        if not metric:
            continue
        if metric not in valid_postprocess_metrics:
            raise ValueError(
                "training.postprocess_benchmark_plot_metrics must contain only "
                f"{sorted(valid_postprocess_metrics)}, got {metric!r}"
            )
        if metric not in plot_metrics:
            plot_metrics.append(metric)
    if postprocess_metric not in plot_metrics:
        plot_metrics.insert(0, postprocess_metric)
    training["postprocess_benchmark_plot_metrics"] = ",".join(plot_metrics)
    training["postprocess_benchmark_plot_top_n"] = max(
        1, int(training["postprocess_benchmark_plot_top_n"])
    )
    training["postprocess_benchmark_max_rows"] = max(
        0, int(training["postprocess_benchmark_max_rows"])
    )

    # Model-specific blocks.
    legacy_hidden_dim = training.get("hidden_dim", MISSING)
    legacy_hidden_layers = training.get("hidden_layers", MISSING)
    legacy_embedding_dim = training.get("embedding_dim", MISSING)
    legacy_transformer_layers = training.get("transformer_layers", MISSING)
    legacy_transformer_heads = training.get("transformer_heads", MISSING)
    legacy_transformer_ffn_dim = training.get("transformer_ffn_dim", MISSING)
    legacy_transformer_use_cls_token = training.get("transformer_use_cls_token", MISSING)
    legacy_dropout = training.get("dropout", MISSING)

    mlp = training.setdefault("mlp", {})
    _set_legacy_alias_defaults(
        mlp,
        {
            "hidden_dim": legacy_hidden_dim,
            "hidden_layers": legacy_hidden_layers,
            "embedding_dim": legacy_embedding_dim,
            "dropout": legacy_dropout,
        },
    )
    _set_dataclass_defaults(mlp, MLPModelConfig)

    ft_transformer = training.setdefault("ft_transformer", {})
    _set_legacy_alias_defaults(
        ft_transformer,
        {
            "d_token": legacy_embedding_dim,
            "n_layers": legacy_transformer_layers,
            "n_heads": legacy_transformer_heads,
            "ffn_dim": legacy_transformer_ffn_dim,
            "dropout": legacy_dropout,
            "use_cls_token": legacy_transformer_use_cls_token,
        },
    )
    _set_dataclass_defaults(ft_transformer, FTTransformerModelConfig)

    tabular_resnet = training.setdefault("tabular_resnet", {})
    _set_legacy_alias_defaults(
        tabular_resnet,
        {
            "embedding_dim": (
                max(64, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "hidden_dim": max(128, int(legacy_hidden_dim)) if legacy_hidden_dim is not MISSING else MISSING,
            "dropout": legacy_dropout,
        },
    )
    _set_dataclass_defaults(tabular_resnet, TabularResNetModelConfig)

    multi_stock_tcn = training.setdefault("multi_stock_tcn", {})
    _set_legacy_alias_defaults(
        multi_stock_tcn,
        {
            "hidden_channels": (
                max(32, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "embedding_dim": (
                max(32, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "head_hidden_dim": (
                max(64, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "dropout": legacy_dropout,
        },
    )
    _set_dataclass_defaults(multi_stock_tcn, MultiStockTCNModelConfig)

    efficient_tcn_tabular_set_portfolio = training.setdefault("efficient_tcn_tabular_set_portfolio", {})
    _set_legacy_alias_defaults(
        efficient_tcn_tabular_set_portfolio,
        {"dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        efficient_tcn_tabular_set_portfolio,
        EfficientTCNTabularSetPortfolioModelConfig,
    )

    latent_factor_market_token_portfolio = training.setdefault("latent_factor_market_token_portfolio", {})
    _set_legacy_alias_defaults(
        latent_factor_market_token_portfolio,
        {"dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        latent_factor_market_token_portfolio,
        LatentFactorMarketTokenPortfolioModelConfig,
    )

    low_rank_market_transformer_portfolio = training.setdefault("low_rank_market_transformer_portfolio", {})
    _set_legacy_alias_defaults(
        low_rank_market_transformer_portfolio,
        {"temporal_dropout": legacy_dropout, "dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        low_rank_market_transformer_portfolio,
        LowRankMarketTransformerPortfolioModelConfig,
    )

    financial_transformer_overrides = deepcopy(training.get("financial_transformer", {}))

    transformer_base_portfolio = training.setdefault("transformer_base_portfolio", {})
    _set_legacy_alias_defaults(transformer_base_portfolio, {"dropout": legacy_dropout})
    _set_dataclass_defaults(transformer_base_portfolio, TransformerBasePortfolioModelConfig)
    transformer_base_portfolio["portfolio_output_mode"] = normalize_portfolio_output_mode(
        transformer_base_portfolio.get("portfolio_output_mode")
    )
    transformer_base_portfolio["categorical_feature_names"] = _normalize_string_list(
        transformer_base_portfolio.get("categorical_feature_names"),
        field_name="training.transformer_base_portfolio.categorical_feature_names",
    )
    transformer_base_portfolio["categorical_embedding_dim"] = max(
        1, int(transformer_base_portfolio["categorical_embedding_dim"])
    )
    transformer_base_portfolio["categorical_embedding_cardinality"] = max(
        2, int(transformer_base_portfolio["categorical_embedding_cardinality"])
    )

    # Financial Transformer extends the market-specific Transformer Base
    # settings. Its YAML section only needs to declare Candle Encoder overrides.
    financial_transformer = _deep_merge_config(
        deepcopy(transformer_base_portfolio),
        financial_transformer_overrides,
    )
    training["financial_transformer"] = financial_transformer
    _set_legacy_alias_defaults(financial_transformer, {"dropout": legacy_dropout})
    _set_dataclass_defaults(financial_transformer, FinancialTransformerModelConfig)
    financial_transformer["portfolio_output_mode"] = normalize_portfolio_output_mode(
        financial_transformer.get("portfolio_output_mode")
    )
    financial_transformer["categorical_feature_names"] = _normalize_string_list(
        financial_transformer.get("categorical_feature_names"),
        field_name="training.financial_transformer.categorical_feature_names",
    )
    financial_transformer["categorical_embedding_dim"] = max(
        1, int(financial_transformer["categorical_embedding_dim"])
    )
    financial_transformer["categorical_embedding_cardinality"] = max(
        2, int(financial_transformer["categorical_embedding_cardinality"])
    )

    gradient_boosted_portfolio_transformer = training.setdefault("gradient_boosted_portfolio_transformer", {})
    gradient_defaults = _dataclass_default_values(GradientBoostedPortfolioTransformerConfig)
    _set_legacy_alias_defaults(
        gradient_boosted_portfolio_transformer,
        {"dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        gradient_boosted_portfolio_transformer,
        GradientBoostedPortfolioTransformerConfig,
    )
    raw_stage_eta = gradient_boosted_portfolio_transformer["stage_eta"]
    if isinstance(raw_stage_eta, str):
        stage_eta = [float(item.strip()) for item in raw_stage_eta.split(",") if item.strip()]
    else:
        stage_eta = [float(item) for item in (raw_stage_eta or gradient_defaults["stage_eta"])]
    gradient_boosted_portfolio_transformer["stage_eta"] = stage_eta or deepcopy(gradient_defaults["stage_eta"])
    gradient_boosted_portfolio_transformer["portfolio_output_mode"] = normalize_portfolio_output_mode(
        gradient_boosted_portfolio_transformer.get("portfolio_output_mode")
    )

    bottleneck_portfolio_autoencoder = training.setdefault("bottleneck_portfolio_autoencoder", {})
    _set_legacy_alias_defaults(
        bottleneck_portfolio_autoencoder,
        {"dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        bottleneck_portfolio_autoencoder,
        BottleneckPortfolioAutoencoderConfig,
    )

    tcn_hybrid_tabular_resnet = training.setdefault("tcn_hybrid_tabular_resnet", {})
    _set_legacy_alias_defaults(
        tcn_hybrid_tabular_resnet,
        {
            "embedding_dim": (
                max(64, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "encoder_hidden_dim": (
                max(128, int(legacy_hidden_dim)) if legacy_hidden_dim is not MISSING else MISSING
            ),
            "dropout": legacy_dropout,
        },
    )
    _set_dataclass_defaults(tcn_hybrid_tabular_resnet, TCNHybridTabularResNetModelConfig)

    temporal_tabular_resnet = training.setdefault("temporal_tabular_resnet", {})
    _set_legacy_alias_defaults(
        temporal_tabular_resnet,
        {
            "temporal_hidden_dim": (
                max(64, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "temporal_dropout": legacy_dropout,
            "embedding_dim": (
                max(64, int(legacy_embedding_dim)) if legacy_embedding_dim is not MISSING else MISSING
            ),
            "hidden_dim": max(128, int(legacy_hidden_dim)) if legacy_hidden_dim is not MISSING else MISSING,
            "dropout": legacy_dropout,
        },
    )
    _set_dataclass_defaults(temporal_tabular_resnet, TemporalTabularResNetModelConfig)

    cross_sectional_temporal_portfolio_model = training.setdefault("cross_sectional_temporal_portfolio_model", {})
    _set_legacy_alias_defaults(
        cross_sectional_temporal_portfolio_model,
        {"dropout": legacy_dropout},
    )
    _set_dataclass_defaults(
        cross_sectional_temporal_portfolio_model,
        CrossSectionalTemporalPortfolioModelConfig,
    )

    multitask_loss = training.setdefault("multitask_loss", {})
    _set_dataclass_defaults(multitask_loss, MultitaskLossConfig)

    factor_generalization_loss = training.setdefault("factor_generalization_loss", {})
    _set_dataclass_defaults(factor_generalization_loss, FactorGeneralizationLossConfig)

    portfolio_autoencoder_loss = training.setdefault("portfolio_autoencoder_loss", {})
    _set_dataclass_defaults(portfolio_autoencoder_loss, PortfolioAutoencoderLossConfig)

    lightgbm = training.setdefault("lightgbm", {})
    _set_dataclass_defaults(lightgbm, LightGBMModelConfig)

    xgboost = training.setdefault("xgboost", {})
    _set_dataclass_defaults(xgboost, XGBoostModelConfig)

    # Remove legacy flat model keys from normalized payload.
    training.pop("hidden_dim", None)
    training.pop("hidden_layers", None)
    training.pop("embedding_dim", None)
    training.pop("transformer_layers", None)
    training.pop("transformer_heads", None)
    training.pop("transformer_ffn_dim", None)
    training.pop("transformer_use_cls_token", None)
    training.pop("dropout", None)

    evaluation = raw.setdefault("evaluation", {})
    _set_dataclass_defaults(evaluation, EvaluationConfig)

    data = raw.setdefault("data", {})

    trading = raw.setdefault("trading", {})

    # Effective legacy migration: data.tw_limit_up_down_guard=true selects the
    # canonical TW limit-guard tradability mode.
    legacy_tw_guard = bool(data.pop("tw_limit_up_down_guard", False))

    raw_tradable_mode = data.get("tradable_mode", None)
    raw_buy_mode = data.pop("buy_tradable_mode", None)
    raw_sell_mode = data.pop("sell_tradable_mode", None)

    if raw_tradable_mode is not None:
        data["tradable_mode"] = raw_tradable_mode
    elif raw_buy_mode is not None and raw_sell_mode is not None:
        buy_mode_normalized = str(raw_buy_mode).strip().lower()
        sell_mode_normalized = str(raw_sell_mode).strip().lower()
        if buy_mode_normalized != sell_mode_normalized:
            raise ValueError(
                "data.buy_tradable_mode and data.sell_tradable_mode must be identical; "
                f"got {raw_buy_mode!r} and {raw_sell_mode!r}"
            )
        data["tradable_mode"] = buy_mode_normalized
    elif raw_buy_mode is not None:
        data["tradable_mode"] = raw_buy_mode
    elif raw_sell_mode is not None:
        data["tradable_mode"] = raw_sell_mode
    elif legacy_tw_guard:
        data["tradable_mode"] = "tw_limit_guard"
    else:
        data["tradable_mode"] = "tradable"
    _set_dataclass_defaults(data, DataConfig)

    valid_tradable_modes = {"tradable", "tw_limit_guard"}
    mode = str(data["tradable_mode"]).strip().lower()
    if mode not in valid_tradable_modes:
        raise ValueError(
            f"data.tradable_mode must be one of {sorted(valid_tradable_modes)}, got {data['tradable_mode']!r}"
        )
    data["tradable_mode"] = mode
    trading_volume_policy = str(data["trading_volume_policy"]).strip().lower()
    valid_volume_policies = {"auto", "required", "optional"}
    if trading_volume_policy not in valid_volume_policies:
        raise ValueError(
            "data.trading_volume_policy must be one of "
            f"{sorted(valid_volume_policies)}, got {data['trading_volume_policy']!r}"
        )
    data["trading_volume_policy"] = trading_volume_policy
    security_filter = str(data["security_filter"]).strip().lower()
    if security_filter in {"", "off", "false"}:
        security_filter = "none"
    valid_security_filters = {"none", "broker_tradable"}
    if security_filter not in valid_security_filters:
        raise ValueError(
            "data.security_filter must be one of "
            f"{sorted(valid_security_filters)}, got {data['security_filter']!r}"
        )
    data["security_filter"] = security_filter
    panel_backend = str(data["panel_backend"]).strip().lower()
    valid_panel_backends = {"auto", "polars", "polars_lazy", "polars_streaming", "pyarrow"}
    if panel_backend not in valid_panel_backends:
        raise ValueError(
            f"data.panel_backend must be one of {sorted(valid_panel_backends)}, got {data['panel_backend']!r}"
        )
    data["panel_backend"] = panel_backend
    data["panel_load_workers"] = max(0, int(data["panel_load_workers"]))
    data["live_tail_panel_rows"] = max(0, int(data["live_tail_panel_rows"]))
    raw_panel_start_date = data.get("panel_start_date")
    if raw_panel_start_date is None or not str(raw_panel_start_date).strip():
        data["panel_start_date"] = None
    else:
        panel_start_date = str(raw_panel_start_date).strip()
        try:
            parsed_panel_start_date = date.fromisoformat(panel_start_date)
        except ValueError as exc:
            raise ValueError(
                "data.panel_start_date must be an ISO date (YYYY-MM-DD) or null, "
                f"got {raw_panel_start_date!r}"
            ) from exc
        data["panel_start_date"] = parsed_panel_start_date.isoformat()
        expected_first_year = walk_forward.get("expected_first_year")
        if (
            expected_first_year is not None
            and int(expected_first_year) != parsed_panel_start_date.year
        ):
            raise ValueError(
                "walk_forward.expected_first_year must match the year of "
                "data.panel_start_date; got "
                f"{expected_first_year!r} and {data['panel_start_date']!r}"
            )
        split_start_year = walk_forward.get("split_start_year")
        if (
            split_start_year is not None
            and int(split_start_year) < parsed_panel_start_date.year
        ):
            raise ValueError(
                "walk_forward.split_start_year cannot precede the panel start year; "
                f"got {split_start_year!r} and {data['panel_start_date']!r}"
            )
    data["use_tw_public_features"] = bool(data["use_tw_public_features"])
    data["use_tw_public_rules"] = bool(data["use_tw_public_rules"])
    data["tw_public_feature_path"] = str(data["tw_public_feature_path"] or "").strip()
    tw_public_market_symbol_default = _dataclass_default_values(DataConfig)["tw_public_market_symbol"]
    data["tw_public_market_symbol"] = (
        str(data["tw_public_market_symbol"] or tw_public_market_symbol_default).strip()
        or tw_public_market_symbol_default
    )
    data["feature_include"] = _normalize_string_list(data["feature_include"], field_name="data.feature_include")
    data["day_trade_open_feature"] = bool(data["day_trade_open_feature"])
    if (
        data["day_trade_open_feature"]
        and DAY_TRADE_OPEN_GAP_FEATURE not in data["feature_include"]
    ):
        data["feature_include"].append(DAY_TRADE_OPEN_GAP_FEATURE)
    forbidden_snapshot_features = [
        feature
        for feature in data["feature_include"]
        if any(
            fnmatch.fnmatchcase(feature, pattern)
            for pattern in FORBIDDEN_SNAPSHOT_ONLY_FEATURE_PATTERNS
        )
    ]
    if forbidden_snapshot_features:
        raise ValueError(
            "data.feature_include contains permanently disabled snapshot-only "
            f"features: {forbidden_snapshot_features}"
        )
    data["feature_exclude"] = _normalize_string_list(data["feature_exclude"], field_name="data.feature_exclude")
    data["feature_zero_fill"] = _normalize_string_list(
        data["feature_zero_fill"], field_name="data.feature_zero_fill"
    )
    data["feature_shift_next_session"] = _normalize_string_list(
        data["feature_shift_next_session"],
        field_name="data.feature_shift_next_session",
    )
    data["allow_same_close_feature_approximation"] = bool(
        data["allow_same_close_feature_approximation"]
    )
    plot_backend = str(training["plot_backend"]).strip().lower()
    valid_plot_backends = {"auto", "matplotlib", "rapids_datashader"}
    if plot_backend not in valid_plot_backends:
        raise ValueError(
            f"training.plot_backend must be one of {sorted(valid_plot_backends)}, got {training['plot_backend']!r}"
        )
    training["plot_backend"] = plot_backend
    if bool(training["strict_no_fallback"]) and bool(training["explain_write_plots"]) and plot_backend == "auto":
        raise ValueError(
            "training.plot_backend cannot be 'auto' when strict_no_fallback=true and explain_write_plots=true; "
            "choose 'rapids_datashader' or 'matplotlib' explicitly."
        )
    report_style = str(training["explain_report_style"]).strip().lower()
    if report_style not in {"paper", "standard", "none"}:
        raise ValueError("training.explain_report_style must be one of: paper, standard, none")
    training["explain_report_style"] = report_style
    plot_theme = str(training["explain_plot_theme"]).strip().lower()
    if plot_theme not in {"paper", "standard"}:
        raise ValueError("training.explain_plot_theme must be one of: paper, standard")
    training["explain_plot_theme"] = plot_theme
    shap_mode = str(training["explain_shap_mode"]).strip().lower()
    valid_shap_modes = {"score_head_surrogate", "surrogate", "score_head", "off", "none"}
    if shap_mode not in valid_shap_modes:
        raise ValueError(f"training.explain_shap_mode must be one of {sorted(valid_shap_modes)}")
    training["explain_shap_mode"] = shap_mode
    j_lens_intervention_fraction = float(training["explain_j_lens_intervention_fraction"])
    if not 0.0 <= j_lens_intervention_fraction <= 1.0:
        raise ValueError("training.explain_j_lens_intervention_fraction must be between 0 and 1")
    training["explain_j_lens_intervention_fraction"] = j_lens_intervention_fraction
    training["explain_case_study_top_k"] = max(1, int(training["explain_case_study_top_k"]))
    training["explain_ig_batch_size"] = max(0, int(training["explain_ig_batch_size"]))
    training["explain_perturb_batch_size"] = max(0, int(training["explain_perturb_batch_size"]))
    training["explain_perturb_max_auto_batch_size"] = max(
        1, int(training["explain_perturb_max_auto_batch_size"])
    )
    training["explain_perturb_max_input_elements"] = max(
        1, int(training["explain_perturb_max_input_elements"])
    )
    training["explain_umap_max_points"] = max(0, int(training["explain_umap_max_points"]))
    training["explain_umap_max_projections"] = max(0, int(training["explain_umap_max_projections"]))
    training["explain_umap_n_neighbors"] = max(2, int(training["explain_umap_n_neighbors"]))
    training["explain_umap_min_dist"] = max(0.0, float(training["explain_umap_min_dist"]))
    training["explain_cross_asset_max_sources"] = max(1, int(training["explain_cross_asset_max_sources"]))
    training["explain_cross_asset_max_targets"] = max(1, int(training["explain_cross_asset_max_targets"]))
    training["explain_cross_asset_top_edges"] = max(1, int(training["explain_cross_asset_top_edges"]))
    training["explain_cross_asset_source_chunk_size"] = max(
        1, int(training["explain_cross_asset_source_chunk_size"])
    )
    training["explain_cross_asset_max_repeated_rows"] = max(
        1, int(training["explain_cross_asset_max_repeated_rows"])
    )
    training["explain_cross_asset_perturb_scale"] = float(training["explain_cross_asset_perturb_scale"])
    raw_cross_shocks = training["explain_cross_asset_shocks"]
    if isinstance(raw_cross_shocks, str):
        cross_shocks = [value.strip().lower() for value in raw_cross_shocks.split(",") if value.strip()]
    else:
        cross_shocks = [str(value).strip().lower() for value in raw_cross_shocks if str(value).strip()]
    training["explain_cross_asset_shocks"] = cross_shocks or [
        "zero",
        "momentum",
        "gap",
        "volume",
        "volatility",
        "liquidity",
    ]
    training["explain_cross_asset_attention_capture_rows"] = max(
        1, int(training["explain_cross_asset_attention_capture_rows"])
    )
    graph_backend = str(training["explain_cross_asset_graph_backend"]).strip().lower()
    if graph_backend not in {"auto", "polars", "cugraph"}:
        raise ValueError("training.explain_cross_asset_graph_backend must be one of: auto, polars, cugraph")
    training["explain_cross_asset_graph_backend"] = graph_backend
    training["explain_cross_asset_graph_benchmark_min_edges"] = max(
        0, int(training["explain_cross_asset_graph_benchmark_min_edges"])
    )
    training["explain_cross_asset_graph_explainability"] = bool(
        training["explain_cross_asset_graph_explainability"]
    )
    training["explain_cross_asset_graph_betweenness_max_vertices"] = max(
        0, int(training["explain_cross_asset_graph_betweenness_max_vertices"])
    )
    training["explain_cross_asset_graph_plot_max_nodes"] = max(
        5, int(training["explain_cross_asset_graph_plot_max_nodes"])
    )
    backtest_artifact_compression = str(training["backtest_artifact_compression"]).strip().lower()
    if backtest_artifact_compression not in {"none", "compressed"}:
        raise ValueError("training.backtest_artifact_compression must be one of: none, compressed")
    training["backtest_artifact_compression"] = backtest_artifact_compression
    legacy_gross_leverage = trading.pop("gross_leverage", None)
    legacy_leverage = trading.pop("leverage", None)
    reporting_leverage = trading.get("reporting_leverage")
    legacy_values = [
        (name, value)
        for name, value in (
            ("leverage", legacy_leverage),
            ("gross_leverage", legacy_gross_leverage),
        )
        if value is not None
    ]
    if reporting_leverage is None and legacy_values:
        trading["reporting_leverage"] = legacy_values[0][1]
        reporting_leverage = legacy_values[0][1]
    if reporting_leverage is not None:
        conflicting_aliases = [
            name
            for name, value in legacy_values
            if float(value) != float(reporting_leverage)
        ]
        if conflicting_aliases:
            raise ValueError(
                "trading.reporting_leverage conflicts with legacy alias(es): "
                + ", ".join(conflicting_aliases)
            )
    _set_dataclass_defaults(trading, TradingConfig)
    trading["execution_mode"] = normalize_execution_mode(trading["execution_mode"])
    normalized_phase_model = _normalized_contract_name(training["model_name"])
    phase_model_config = (
        training["financial_transformer"]
        if normalized_phase_model in _TW_FINANCIAL_PHASE_HEAD_MODEL_NAMES
        else training["transformer_base_portfolio"]
    )
    _validate_tw_phase_mode_contract(
        execution_mode=trading["execution_mode"],
        model_name=training["model_name"],
        loss_type=training["loss_type"],
        model_portfolio_output_mode=phase_model_config[
            "portfolio_output_mode"
        ],
        trading_portfolio_activation=trading["portfolio_activation"],
        loss_portfolio_activation=training["loss_portfolio_activation"],
        return_rank_ic_weight=training["multitask_loss"][
            "return_rank_ic_weight"
        ],
        direction_weight=training["multitask_loss"]["direction_weight"],
        explain_after_each_fold=training["explain_after_each_fold"],
    )
    if (
        DAY_TRADE_OPEN_GAP_FEATURE in data["feature_include"]
        and trading["execution_mode"] != "tw_day_trade"
        and trading["execution_mode"] not in TW_CARRYING_EXECUTION_MODES
    ):
        raise ValueError(
            f"data.feature_include={DAY_TRADE_OPEN_GAP_FEATURE!r} is available "
            "only with a phase-aware Taiwan execution mode; its row t value "
            "contains the next session's opening quote"
        )
    taiwan_fee_schedule = TaiwanFeeSchedule(
        commission_rate=trading["tw_commission_rate"],
        commission_discount=trading["tw_commission_discount"],
        stock_sell_tax=trading["tw_stock_sell_tax"],
        etf_sell_tax=trading["tw_etf_sell_tax"],
        day_trade_stock_sell_tax=trading["tw_day_trade_stock_sell_tax"],
        day_trade_etf_sell_tax=trading["tw_day_trade_etf_sell_tax"],
        minimum_commission=trading["tw_minimum_commission"],
        commission_rounding=trading["tw_commission_rounding"],
        tax_rounding=trading["tw_tax_rounding"],
        settlement_lag_sessions=trading["tw_settlement_lag_sessions"],
        cash_lot_size=trading["tw_cash_lot_size"],
        day_trade_default_lot_size=trading["tw_day_trade_lot_size"],
    )
    # Keep the normalized config payload canonical and typed exactly like the
    # validated execution schedule consumed by the backtest executor.
    trading["tw_commission_rate"] = taiwan_fee_schedule.commission_rate
    trading["tw_commission_discount"] = taiwan_fee_schedule.commission_discount
    trading["tw_stock_sell_tax"] = taiwan_fee_schedule.stock_sell_tax
    trading["tw_etf_sell_tax"] = taiwan_fee_schedule.etf_sell_tax
    trading["tw_day_trade_stock_sell_tax"] = (
        taiwan_fee_schedule.day_trade_stock_sell_tax
    )
    trading["tw_day_trade_etf_sell_tax"] = taiwan_fee_schedule.day_trade_etf_sell_tax
    trading["tw_minimum_commission"] = taiwan_fee_schedule.minimum_commission
    trading["tw_commission_rounding"] = taiwan_fee_schedule.commission_rounding
    trading["tw_tax_rounding"] = taiwan_fee_schedule.tax_rounding
    trading["tw_settlement_lag_sessions"] = taiwan_fee_schedule.settlement_lag_sessions
    trading["tw_cash_lot_size"] = taiwan_fee_schedule.cash_lot_size
    trading["tw_day_trade_lot_size"] = taiwan_fee_schedule.day_trade_default_lot_size
    taiwan_short_schedule = TaiwanMarginShortSchedule(
        initial_margin_rate=trading["tw_short_initial_margin_rate"],
        maintenance_ratio=trading["tw_short_maintenance_ratio"],
        lot_size=trading["tw_short_lot_size"],
        handling_fee_rate=trading["tw_short_handling_fee_rate"],
    )
    trading["tw_short_initial_margin_rate"] = taiwan_short_schedule.initial_margin_rate
    trading["tw_short_maintenance_ratio"] = taiwan_short_schedule.maintenance_ratio
    trading["tw_short_lot_size"] = taiwan_short_schedule.lot_size
    trading["tw_short_handling_fee_rate"] = taiwan_short_schedule.handling_fee_rate
    if not isinstance(trading["tw_short_capacity_limit_enabled"], bool):
        raise ValueError(
            "trading.tw_short_capacity_limit_enabled must be a boolean"
        )
    corporate_action_mode = str(
        trading["tw_corporate_action_mode"]
    ).strip().lower()
    if corporate_action_mode not in {"avoid", "exact"}:
        raise ValueError(
            "trading.tw_corporate_action_mode must be 'avoid' or 'exact'"
        )
    trading["tw_corporate_action_mode"] = corporate_action_mode
    claim_queue = trading["tw_corporate_action_claim_queue_sessions"]
    if isinstance(claim_queue, bool) or not isinstance(claim_queue, int):
        raise ValueError(
            "trading.tw_corporate_action_claim_queue_sessions must be an integer"
        )
    if claim_queue < trading["tw_settlement_lag_sessions"]:
        raise ValueError(
            "trading.tw_corporate_action_claim_queue_sessions must be at least "
            "tw_settlement_lag_sessions"
        )
    # Report/post-processing leverage only. Canonical model/loss/backtest exposure stays unlevered.
    trading["max_turnover_ratio"] = max(0.0, float(trading["max_turnover_ratio"]))
    trading["max_volume_participation"] = max(0.0, float(trading["max_volume_participation"]))
    trading["volume_participation_equity"] = max(1e-12, float(trading["volume_participation_equity"]))
    trading["reporting_leverage"] = max(0.0, float(trading["reporting_leverage"]))
    trading["min_trade_weight"] = max(0.0, float(trading["min_trade_weight"]))
    trading["portfolio_activation"] = normalize_portfolio_activation(trading["portfolio_activation"])
    evaluation = raw.setdefault("evaluation", {})
    loss_activation = str(training["loss_portfolio_activation"]).strip().lower().replace("-", "_")
    if loss_activation in {"", "auto", "trading", "same", "same_as_trading"}:
        training["loss_portfolio_activation"] = "auto"
    else:
        training["loss_portfolio_activation"] = normalize_portfolio_activation(loss_activation)
    loss_min_trade_weight = training["loss_min_trade_weight"]
    if loss_min_trade_weight is None or str(loss_min_trade_weight).strip().lower() in {
        "",
        "auto",
        "trading",
        "same",
        "same_as_trading",
    }:
        training["loss_min_trade_weight"] = None
    else:
        training["loss_min_trade_weight"] = max(0.0, float(loss_min_trade_weight))
    fee_per_side_raw = trading.get("fee_per_side", None)
    buy_fee_raw = trading.get("buy_fee_rate", None)
    sell_fee_raw = trading.get("sell_fee_rate", None)

    if buy_fee_raw is None and sell_fee_raw is None:
        fee = float(fee_per_side_raw or 0.0)
        trading["buy_fee_rate"] = fee
        trading["sell_fee_rate"] = fee
    else:
        trading["buy_fee_rate"] = float(buy_fee_raw if buy_fee_raw is not None else fee_per_side_raw or 0.0)
        trading["sell_fee_rate"] = float(sell_fee_raw if sell_fee_raw is not None else fee_per_side_raw or 0.0)

    # Legacy key is accepted as input but removed from the normalized config payload.
    trading.pop("fee_per_side", None)
    return raw


def load_config(path: str | Path) -> ExperimentConfig:
    raw = _load_raw_config(path)
    _validate_raw_config_bool_types(raw)
    raw = _merge_defaults(raw)
    training_raw = raw["training"]
    _validate_config_keys(raw, ExperimentConfig, section="")
    _validate_config_keys(raw["runner"], RunnerConfig, section="runner")
    _validate_config_keys(raw["environment"], EnvironmentConfig, section="environment")
    _validate_config_keys(raw["data"], DataConfig, section="data")
    _validate_config_keys(raw["walk_forward"], WalkForwardConfig, section="walk_forward")
    _validate_config_keys(raw["trading"], TradingConfig, section="trading")
    _validate_config_keys(training_raw, TrainingConfig, section="training")
    _validate_config_keys(raw["evaluation"], EvaluationConfig, section="evaluation")
    nested_training_schemas = _nested_training_schemas()
    for section_name, schema in nested_training_schemas.items():
        _validate_config_keys(
            training_raw[section_name],
            schema,
            section=f"training.{section_name}",
        )
    return ExperimentConfig(
        experiment_name=raw["experiment_name"],
        runner=RunnerConfig(**raw["runner"]),
        environment=EnvironmentConfig(**raw["environment"]),
        data=DataConfig(**raw["data"]),
        walk_forward=WalkForwardConfig(**raw["walk_forward"]),
        trading=TradingConfig(**raw["trading"]),
        training=TrainingConfig(
            non_blocking_transfer=training_raw["non_blocking_transfer"],
            model_name=training_raw["model_name"],
            seed=training_raw["seed"],
            multi_gpu_strategy=training_raw["multi_gpu_strategy"],
            ddp_bucket_cap_mb=training_raw["ddp_bucket_cap_mb"],
            enable_torch_compile=training_raw["enable_torch_compile"],
            auto_torch_compile_sharpe=training_raw["auto_torch_compile_sharpe"],
            torch_compile_mode=training_raw["torch_compile_mode"],
            torchinductor_cache_dir=training_raw["torchinductor_cache_dir"],
            triton_cache_dir=training_raw["triton_cache_dir"],
            cuda_cache_path=training_raw["cuda_cache_path"],
            compile_loss=training_raw["compile_loss"],
            compile_model_dynamic_symbols=training_raw["compile_model_dynamic_symbols"],
            compile_loss_dynamic_symbols=training_raw["compile_loss_dynamic_symbols"],
            compile_eval_model=training_raw["compile_eval_model"],
            loss_portfolio_activation=training_raw["loss_portfolio_activation"],
            loss_min_trade_weight=training_raw["loss_min_trade_weight"],
            warm_start_from_previous_fold=training_raw["warm_start_from_previous_fold"],
            chunk_rows=training_raw["chunk_rows"],
            eval_model_chunk_rows=training_raw["eval_model_chunk_rows"],
            eval_backtest_chunk_rows=training_raw["eval_backtest_chunk_rows"],
            eval_backtest_chunk_rows_auto=training_raw["eval_backtest_chunk_rows_auto"],
            eval_backtest_compile=training_raw["eval_backtest_compile"],
            eval_auto_chunk_rows_cap=training_raw["eval_auto_chunk_rows_cap"],
            train_symbol_compaction=training_raw["train_symbol_compaction"],
            train_symbol_compaction_bucket_size=int(training_raw["train_symbol_compaction_bucket_size"]),
            backtest_autotune=training_raw["backtest_autotune"],
            backtest_compile=training_raw["backtest_compile"],
            backtest_compile_stateful=training_raw["backtest_compile_stateful"],
            backtest_compile_dynamic=training_raw["backtest_compile_dynamic"],
            tw_continuous_compile_chunk_rows=training_raw[
                "tw_continuous_compile_chunk_rows"
            ],
            tw_continuous_gradient_horizon_rows=training_raw[
                "tw_continuous_gradient_horizon_rows"
            ],
            inference_backtest_autotune=training_raw["inference_backtest_autotune"],
            inference_backtest_compile=training_raw["inference_backtest_compile"],
            backtest_verbose=training_raw["backtest_verbose"],
            strict_no_fallback=training_raw["strict_no_fallback"],
            backtest_checkpoint_chunk_rows=training_raw["backtest_checkpoint_chunk_rows"],
            runtime_shape_check=training_raw["runtime_shape_check"],
            allow_dynamic_symbols=training_raw["allow_dynamic_symbols"],
            lookback=training_raw["lookback"],
            batch_size_train=training_raw["batch_size_train"],
            batch_size_eval=training_raw["batch_size_eval"],
            min_batch_size=training_raw["min_batch_size"],
            auto_batch_size=training_raw["auto_batch_size"],
            vram_budget_gb=training_raw["vram_budget_gb"],
            vram_safety_margin_gb=training_raw["vram_safety_margin_gb"],
            target_vram_fraction=training_raw["target_vram_fraction"],
            epochs=training_raw["epochs"],
            early_stopping_no_improve_ratio=training_raw["early_stopping_no_improve_ratio"],
            early_stopping_min_delta=training_raw["early_stopping_min_delta"],
            best_checkpoint_max_epoch=training_raw["best_checkpoint_max_epoch"],
            val_interval_epochs=training_raw["val_interval_epochs"],
            curve_test_interval=training_raw["curve_test_interval"],
            record_epoch_curve=training_raw["record_epoch_curve"],
            curve_plot_interval=training_raw["curve_plot_interval"],
            curve_plot_async=training_raw["curve_plot_async"],
            plot_backend=training_raw["plot_backend"],
            epoch_test_curve=training_raw["epoch_test_curve"],
            defer_epoch_curve_plot_until_end=training_raw["defer_epoch_curve_plot_until_end"],
            debug_timing_sync=training_raw["debug_timing_sync"],
            explain_after_each_fold=training_raw["explain_after_each_fold"],
            explain_top_k=training_raw["explain_top_k"],
            explain_max_rows=training_raw["explain_max_rows"],
            explain_ig_steps=training_raw["explain_ig_steps"],
            explain_ig_batch_size=training_raw["explain_ig_batch_size"],
            explain_sample_method=training_raw["explain_sample_method"],
            explain_perturb=training_raw["explain_perturb"],
            explain_perturb_batch_size=training_raw["explain_perturb_batch_size"],
            explain_perturb_max_auto_batch_size=training_raw["explain_perturb_max_auto_batch_size"],
            explain_perturb_max_input_elements=training_raw["explain_perturb_max_input_elements"],
            explain_counterfactual_compile=training_raw["explain_counterfactual_compile"],
            explain_write_plots=training_raw["explain_write_plots"],
            explain_report_style=training_raw["explain_report_style"],
            explain_plot_theme=training_raw["explain_plot_theme"],
            explain_standard_plots=training_raw["explain_standard_plots"],
            explain_interactive_plots=training_raw["explain_interactive_plots"],
            explain_shap_enabled=training_raw["explain_shap_enabled"],
            explain_shap_mode=training_raw["explain_shap_mode"],
            explain_j_lens_enabled=training_raw["explain_j_lens_enabled"],
            explain_j_lens_intervention_fraction=training_raw["explain_j_lens_intervention_fraction"],
            explain_case_study_top_k=training_raw["explain_case_study_top_k"],
            explain_regime_analysis=training_raw["explain_regime_analysis"],
            explain_fold_stability=training_raw["explain_fold_stability"],
            explain_umap_enabled=training_raw["explain_umap_enabled"],
            explain_umap_max_points=training_raw["explain_umap_max_points"],
            explain_umap_max_projections=training_raw["explain_umap_max_projections"],
            explain_umap_n_neighbors=training_raw["explain_umap_n_neighbors"],
            explain_umap_min_dist=training_raw["explain_umap_min_dist"],
            explain_cross_asset_enabled=training_raw["explain_cross_asset_enabled"],
            explain_cross_asset_max_sources=training_raw["explain_cross_asset_max_sources"],
            explain_cross_asset_max_targets=training_raw["explain_cross_asset_max_targets"],
            explain_cross_asset_top_edges=training_raw["explain_cross_asset_top_edges"],
            explain_cross_asset_source_chunk_size=training_raw["explain_cross_asset_source_chunk_size"],
            explain_cross_asset_max_repeated_rows=training_raw["explain_cross_asset_max_repeated_rows"],
            explain_cross_asset_perturb_scale=training_raw["explain_cross_asset_perturb_scale"],
            explain_cross_asset_shocks=training_raw["explain_cross_asset_shocks"],
            explain_cross_asset_attention_flow=training_raw["explain_cross_asset_attention_flow"],
            explain_cross_asset_attention_capture_rows=training_raw["explain_cross_asset_attention_capture_rows"],
            explain_cross_asset_validated_transmission=training_raw["explain_cross_asset_validated_transmission"],
            explain_cross_asset_role_embedding=training_raw["explain_cross_asset_role_embedding"],
            explain_cross_asset_graph_backend=training_raw["explain_cross_asset_graph_backend"],
            explain_cross_asset_graph_benchmark_min_edges=training_raw[
                "explain_cross_asset_graph_benchmark_min_edges"
            ],
            explain_cross_asset_graph_explainability=training_raw["explain_cross_asset_graph_explainability"],
            explain_cross_asset_graph_betweenness_max_vertices=training_raw[
                "explain_cross_asset_graph_betweenness_max_vertices"
            ],
            explain_cross_asset_graph_plot_max_nodes=training_raw["explain_cross_asset_graph_plot_max_nodes"],
            table_output_format=training_raw["table_output_format"],
            save_daily_weights_table=training_raw["save_daily_weights_table"],
            save_integer_share_daily_weights_table=training_raw["save_integer_share_daily_weights_table"],
            save_integer_share_holdings_table=training_raw["save_integer_share_holdings_table"],
            backtest_artifact_compression=training_raw["backtest_artifact_compression"],
            save_best_val_artifacts=training_raw["save_best_val_artifacts"],
            save_best_val_fold_artifacts=training_raw["save_best_val_fold_artifacts"],
            save_best_val_fold_plots=training_raw["save_best_val_fold_plots"],
            postprocess_benchmark_after_fold=training_raw["postprocess_benchmark_after_fold"],
            postprocess_benchmark_after_best_val=training_raw["postprocess_benchmark_after_best_val"],
            postprocess_benchmark_split=training_raw["postprocess_benchmark_split"],
            postprocess_benchmark_activations=training_raw["postprocess_benchmark_activations"],
            postprocess_benchmark_thresholds=training_raw["postprocess_benchmark_thresholds"],
            postprocess_benchmark_rank_metric=training_raw["postprocess_benchmark_rank_metric"],
            postprocess_benchmark_plot_metrics=training_raw["postprocess_benchmark_plot_metrics"],
            postprocess_benchmark_plot_top_n=training_raw["postprocess_benchmark_plot_top_n"],
            postprocess_benchmark_backtest_compile=training_raw["postprocess_benchmark_backtest_compile"],
            postprocess_benchmark_max_rows=training_raw["postprocess_benchmark_max_rows"],
            postprocess_benchmark_strict=training_raw["postprocess_benchmark_strict"],
            cache_train_tensors_on_gpu=training_raw["cache_train_tensors_on_gpu"],
            cache_eval_tensors_on_gpu=training_raw["cache_eval_tensors_on_gpu"],
            cache_train_features_in_amp_dtype=training_raw["cache_train_features_in_amp_dtype"],
            learning_rate=training_raw["learning_rate"],
            enable_lr_scheduler=training_raw["enable_lr_scheduler"],
            lr_scheduler=training_raw["lr_scheduler"],
            lr_scheduler_t_max=training_raw["lr_scheduler_t_max"],
            lr_scheduler_eta_min=training_raw["lr_scheduler_eta_min"],
            lr_scheduler_warmup_steps=training_raw["lr_scheduler_warmup_steps"],
            lr_scheduler_step_size=training_raw["lr_scheduler_step_size"],
            lr_scheduler_gamma=training_raw["lr_scheduler_gamma"],
            lr_scheduler_patience=training_raw["lr_scheduler_patience"],
            lr_scheduler_threshold=training_raw["lr_scheduler_threshold"],
            weight_decay=training_raw["weight_decay"],
            grad_clip_norm=training_raw["grad_clip_norm"],
            finite_check_interval_steps=training_raw["finite_check_interval_steps"],
            checkpoint_finite_check=training_raw["checkpoint_finite_check"],
            loss_type=training_raw["loss_type"],
            mlp=MLPModelConfig(**training_raw["mlp"]),
            ft_transformer=FTTransformerModelConfig(**training_raw["ft_transformer"]),
            tabular_resnet=TabularResNetModelConfig(**training_raw["tabular_resnet"]),
            multi_stock_tcn=MultiStockTCNModelConfig(**training_raw["multi_stock_tcn"]),
            efficient_tcn_tabular_set_portfolio=EfficientTCNTabularSetPortfolioModelConfig(
                **training_raw["efficient_tcn_tabular_set_portfolio"]
            ),
            latent_factor_market_token_portfolio=LatentFactorMarketTokenPortfolioModelConfig(
                **training_raw["latent_factor_market_token_portfolio"]
            ),
            low_rank_market_transformer_portfolio=LowRankMarketTransformerPortfolioModelConfig(
                **training_raw["low_rank_market_transformer_portfolio"]
            ),
            transformer_base_portfolio=TransformerBasePortfolioModelConfig(
                **training_raw["transformer_base_portfolio"]
            ),
            financial_transformer=FinancialTransformerModelConfig(
                **training_raw["financial_transformer"]
            ),
            gradient_boosted_portfolio_transformer=GradientBoostedPortfolioTransformerConfig(
                **training_raw["gradient_boosted_portfolio_transformer"]
            ),
            bottleneck_portfolio_autoencoder=BottleneckPortfolioAutoencoderConfig(
                **training_raw["bottleneck_portfolio_autoencoder"]
            ),
            tcn_hybrid_tabular_resnet=TCNHybridTabularResNetModelConfig(**training_raw["tcn_hybrid_tabular_resnet"]),
            temporal_tabular_resnet=TemporalTabularResNetModelConfig(**training_raw["temporal_tabular_resnet"]),
            cross_sectional_temporal_portfolio_model=CrossSectionalTemporalPortfolioModelConfig(**training_raw["cross_sectional_temporal_portfolio_model"]),
            multitask_loss=MultitaskLossConfig(**training_raw["multitask_loss"]),
            factor_generalization_loss=FactorGeneralizationLossConfig(**training_raw["factor_generalization_loss"]),
            portfolio_autoencoder_loss=PortfolioAutoencoderLossConfig(**training_raw["portfolio_autoencoder_loss"]),
            lightgbm=LightGBMModelConfig(**training_raw["lightgbm"]),
            xgboost=XGBoostModelConfig(**training_raw["xgboost"]),
        ),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
