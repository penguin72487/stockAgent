from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class EnvironmentConfig:
    conda_env: str
    device: str
    use_tensor_cores: bool
    amp_dtype: str
    target_vram_fraction: float = 1


@dataclass(slots=True)
class DataConfig:
    parquet_root: str
    benchmark_name: str
    benchmark_required: bool
    benchmark_source: str
    universe_mode: str
    use_rapids: bool = True


@dataclass(slots=True)
class WalkForwardConfig:
    min_train_years: int
    val_years: int = 1
    require_future_test_year: bool = True


@dataclass(slots=True)
class TradingConfig:
    frequency: str
    fee_per_side: float
    long_only: bool
    cash_allowed: bool
    use_all_tradable_symbols: bool
    backtest_rule: str = "day_trade"
    lot_size: int = 1000
    min_fee_per_side: float = 20.0
    buy_fee_rate: float = 0.0
    sell_fee_rate: float = 0.0


@dataclass(slots=True)
class TrainingConfig:
    backend: str
    target: str
    batch_mode: str
    non_blocking_transfer: bool
    enable_torch_compile: bool = False
    torch_compile_mode: str = "reduce-overhead"
    compile_max_autotune_gemm: str = "auto"
    compile_autotune_min_sms: int = 80
    chunk_rows: int = 0
    lookback: int = 1
    batch_size: int = 32
    batch_size_train: int = 32
    batch_size_eval: int = 32
    min_batch_size: int = 1
    auto_batch_size: bool = False
    batch_safety_factor: float = 0.8
    auto_batch_safety_factor: float = 0.8
    compile_batch_safety_factor: float = 0.85
    vram_budget_gb: float = 8.0
    vram_safety_margin_gb: float = 1.0
    target_vram_fraction: float = 1
    epochs: int = 1000
    learning_rate: float = 1e-3
    hidden_dim: int = 1024
    hidden_layers: int = 2
    dropout: float = 0.1
    top_k: int = 20
    num_workers: int = 0
    weight_decay: float = 1e-5
    loss_type: str = "mse"  # "mse" or "sharpe"


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]
    gamma_sharpe: float = 1.0
    gamma_turnover: float = 0.1


@dataclass(slots=True)
class ModelConfig:
    name: str = "mlp"
    config_path: str = ""
    params: dict[str, Any] | None = None


@dataclass(slots=True)
class ExperimentConfig:
    experiment_name: str
    environment: EnvironmentConfig
    data: DataConfig
    walk_forward: WalkForwardConfig
    trading: TradingConfig
    training: TrainingConfig
    evaluation: EvaluationConfig
    model: ModelConfig


def _merge_defaults(raw: dict[str, Any]) -> dict[str, Any]:
    walk_forward = raw.setdefault("walk_forward", {})
    walk_forward.setdefault("min_train_years", 1)
    walk_forward.setdefault("val_years", 1)
    walk_forward.setdefault("require_future_test_year", True)

    training = raw.setdefault("training", {})
    training.setdefault("lookback", 1)
    training.setdefault("batch_size", 32)
    training.setdefault("batch_size_train", training.get("batch_size", 32))
    training.setdefault("batch_size_eval", training.get("batch_size", 32))
    training.setdefault("min_batch_size", 1)
    training.setdefault("auto_batch_size", False)
    training.setdefault("batch_safety_factor", training.get("auto_batch_safety_factor", 0.8))
    training.setdefault("auto_batch_safety_factor", 0.8)
    training.setdefault("compile_batch_safety_factor", 0.85)
    training.setdefault("enable_torch_compile", False)
    training.setdefault("torch_compile_mode", "reduce-overhead")
    training.setdefault("compile_max_autotune_gemm", "auto")
    training.setdefault("compile_autotune_min_sms", 80)
    training.setdefault("chunk_rows", 0)
    training.setdefault("vram_budget_gb", 8.0)
    training.setdefault("vram_safety_margin_gb", 1.0)
    training.setdefault("target_vram_fraction", 0.85)
    training.setdefault("epochs", 10)
    training.setdefault("learning_rate", 1e-3)
    training.setdefault("hidden_dim", 128)
    training.setdefault("hidden_layers", 2)
    training.setdefault("dropout", 0.1)
    training.setdefault("top_k", 20)
    training.setdefault("num_workers", 0)
    training.setdefault("weight_decay", 1e-5)
    training.setdefault("loss_type", "mse")

    evaluation = raw.setdefault("evaluation", {})
    evaluation.setdefault("gamma_sharpe", 1.0)
    evaluation.setdefault("gamma_turnover", 0.1)

    trading = raw.setdefault("trading", {})
    trading.setdefault("backtest_rule", "day_trade")

    rule_defaults = {
        "day_trade": {
            "lot_size": 1000,
            "buy_fee_rate": 0.001425,
            "sell_fee_rate": 0.002925,
            "min_fee_per_side": 20.0,
        },
        "basic": {
            "lot_size": 1,
            "buy_fee_rate": 0.001425,
            "sell_fee_rate": 0.004425,
            "min_fee_per_side": 20.0,
        },
        "overnight": {
            "lot_size": 1,
            "buy_fee_rate": 0.001425,
            "sell_fee_rate": 0.004425,
            "min_fee_per_side": 20.0,
        },
    }
    rule_name = str(trading.get("backtest_rule", "day_trade")).strip().lower().replace("-", "_")
    if rule_name not in rule_defaults:
        raise ValueError(
            "Unknown trading.backtest_rule. Expected one of day_trade, basic, overnight; "
            f"got {trading.get('backtest_rule')!r}"
        )
    trading["backtest_rule"] = rule_name
    trading.update(rule_defaults[rule_name])
    trading.setdefault("buy_fee_rate", trading.get("fee_per_side", 0.0))
    trading.setdefault("sell_fee_rate", trading.get("fee_per_side", 0.0))

    data = raw.setdefault("data", {})
    data.setdefault("use_rapids", True)

    model = raw.setdefault("model", {})
    model.setdefault("name", "mlp")
    model.setdefault("config_path", "")
    model.setdefault("params", {})
    return raw


def _load_model_params(raw: dict[str, Any], config_path: Path) -> dict[str, Any]:
    model_section = raw.setdefault("model", {})
    model_name = str(model_section.get("name", "mlp"))
    model_params = dict(model_section.get("params", {}) or {})
    external_path = str(model_section.get("config_path", "") or "")

    if not external_path:
        default_path = config_path.parent / "models" / f"{model_name}.yaml"
        if default_path.exists():
            external_path = str(default_path)

    if external_path:
        resolved = Path(external_path)
        if not resolved.is_absolute():
            resolved = (config_path.parent / resolved).resolve()
        with resolved.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Model config must be a mapping: {resolved}")
        model_params = {**loaded, **model_params}
        model_section["config_path"] = str(resolved)

    if model_name in {"portfolio_transformer", "portfolio_tf"}:
        model_params = _apply_portfolio_backend_report(
            model_params=model_params,
            config_path=config_path,
        )

    model_section["params"] = model_params
    return raw


def _resolve_report_path(report_path: str, config_path: Path) -> Path | None:
    candidate = Path(report_path)
    candidates: list[Path] = []
    if candidate.is_absolute():
        candidates.append(candidate)
    else:
        # Common layouts:
        # - config under configs/, report under artifacts/
        # - direct execution from workspace root
        candidates.append((config_path.parent / candidate).resolve())
        candidates.append((config_path.parent.parent / candidate).resolve())
        candidates.append((Path.cwd() / candidate).resolve())

    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def _apply_portfolio_backend_report(model_params: dict[str, Any], config_path: Path) -> dict[str, Any]:
    params = dict(model_params)
    requested_backend = str(params.get("attention_backend", "auto")).lower()
    auto_select = bool(params.get("auto_select_backend_from_report", True))
    if requested_backend != "auto" or not auto_select:
        params.setdefault("attention_backend_source", "config")
        return params

    report_ref = str(params.get("acceleration_report_path", "artifacts/acceleration_report.json"))
    report_path = _resolve_report_path(report_ref, config_path)
    if report_path is None:
        params.setdefault("attention_backend_source", f"auto(no-report:{report_ref})")
        return params

    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        params.setdefault("attention_backend_source", f"auto(report-read-failed:{report_path})")
        return params

    checks = payload.get("checks", [])
    if not isinstance(checks, list):
        params.setdefault("attention_backend_source", f"auto(report-invalid:{report_path})")
        return params

    best_backend: str | None = None
    best_ms: float | None = None
    for item in checks:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", ""))
        if not name.startswith("portfolio_backend:"):
            continue
        backend = name.split(":", 1)[1].strip().lower()
        if backend == "auto":
            continue
        if not bool(item.get("ok", False)):
            continue
        ms = item.get("ms")
        if not isinstance(ms, (int, float)):
            continue
        ms_value = float(ms)
        if best_ms is None or ms_value < best_ms:
            best_ms = ms_value
            best_backend = backend

    if best_backend is None:
        params.setdefault("attention_backend_source", f"auto(no-usable-check:{report_path})")
        return params

    params["attention_backend"] = best_backend
    params["attention_backend_source"] = f"report:{report_path}"
    params["attention_backend_ms"] = best_ms
    return params


def load_config(path: str | Path) -> ExperimentConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    raw = _merge_defaults(raw)
    raw = _load_model_params(raw, config_path.resolve())
    return ExperimentConfig(
        experiment_name=raw["experiment_name"],
        environment=EnvironmentConfig(**raw["environment"]),
        data=DataConfig(**raw["data"]),
        walk_forward=WalkForwardConfig(**raw["walk_forward"]),
        trading=TradingConfig(**raw["trading"]),
        training=TrainingConfig(**raw["training"]),
        evaluation=EvaluationConfig(**raw["evaluation"]),
        model=ModelConfig(**raw["model"]),
    )
