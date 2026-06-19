from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData, build_panel
from stockagent.live.portfolio_state import (
    build_rebalance_rows,
    estimate_benchmark_return,
    estimate_drifted_weights,
    top_weight_rows,
)
from stockagent.live.quote_provider import PriceSnapshot, fetch_yahoo_last_prices, load_prices_csv, load_symbol_name_map
from stockagent.live.report_formatter import format_signal_message
from stockagent.models.factory import build_model
from stockagent.training.trainer import (
    _autocast_context,
    _call_model,
    _extract_weights_and_aux,
    _load_checkpoint,
    _load_state_dict,
    _resolve_amp_dtype,
    _resolve_device,
)


@dataclass(slots=True)
class LiveSignalResult:
    summary: dict[str, Any]
    weights_rows: list[dict[str, Any]]
    rebalance_rows: list[dict[str, Any]]
    message: str
    output_dir: str | None = None


def _date_string(value: object) -> str:
    try:
        return str(np.datetime_as_string(np.asarray(value).astype("datetime64[D]"), unit="D"))
    except Exception:
        text = str(value)
        return text[:10] if len(text) >= 10 else text


def _build_panel(config: ExperimentConfig) -> PanelData:
    return build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
    )


def _discover_latest_fold(output_dir: str | Path) -> int:
    candidates: list[int] = []
    for path in Path(output_dir).glob("fold_*/checkpoint_best.pt"):
        try:
            candidates.append(int(path.parent.name.removeprefix("fold_")))
        except ValueError:
            continue
    if not candidates:
        raise FileNotFoundError(f"No checkpoint_best.pt found under {output_dir}")
    return max(candidates)


def _resolve_checkpoint(output_dir: str | Path, fold_id: int | None, checkpoint_path: str | Path | None) -> tuple[int, Path]:
    if checkpoint_path is not None:
        path = Path(checkpoint_path)
        if not path.exists():
            raise FileNotFoundError(path)
        if fold_id is not None:
            return int(fold_id), path
        try:
            return int(path.parent.name.removeprefix("fold_")), path
        except ValueError:
            return -1, path

    resolved_fold = _discover_latest_fold(output_dir) if fold_id is None else int(fold_id)
    path = Path(output_dir) / f"fold_{resolved_fold:02d}" / "checkpoint_best.pt"
    if not path.exists():
        raise FileNotFoundError(path)
    return resolved_fold, path


def _read_table(path: Path):
    import polars as pl

    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    if path.suffix == ".csv":
        return pl.read_csv(path)
    raise ValueError(f"Unsupported table format: {path}")


def _default_weights_path(output_dir: str | Path, fold_id: int) -> Path | None:
    fold_dir = Path(output_dir) / f"fold_{int(fold_id):02d}"
    for name in ("daily_weights.parquet", "daily_weights.csv"):
        path = fold_dir / name
        if path.exists():
            return path
    return None


def _load_previous_weights(
    symbols: list[str],
    *,
    output_dir: str | Path,
    fold_id: int,
    weights_path: str | Path | None,
    asof_date: str | None,
) -> tuple[np.ndarray, str | None, str | None]:
    path = Path(weights_path) if weights_path is not None else _default_weights_path(output_dir, fold_id)
    if path is None or not path.exists():
        return np.zeros((len(symbols),), dtype=np.float64), None, None

    frame = _read_table(path)
    if "date" not in frame.columns or frame.height == 0:
        return np.zeros((len(symbols),), dtype=np.float64), None, str(path)

    if asof_date:
        asof = np.datetime64(asof_date, "D")
        keep = []
        for raw in frame.get_column("date").to_list():
            try:
                keep.append(np.datetime64(str(raw)[:10], "D") <= asof)
            except Exception:
                keep.append(True)
        if any(keep):
            import polars as pl

            frame = frame.filter(pl.Series(keep))

    frame = frame.sort("date")
    row = frame.tail(1).to_dicts()[0]
    weights = np.zeros((len(symbols),), dtype=np.float64)
    for idx, symbol in enumerate(symbols):
        value = row.get(symbol)
        if value is None:
            continue
        try:
            weights[idx] = float(value)
        except Exception:
            continue
    return weights, _date_string(row.get("date")), str(path)


def _resolve_panel_index(panel: PanelData, panel_date: str | None, lookback: int) -> int:
    if panel_date is None or str(panel_date).strip().lower() in {"", "latest", "last"}:
        idx = int(panel.num_dates - 1)
    else:
        target = np.datetime64(str(panel_date), "D")
        dates = np.asarray(panel.dates).astype("datetime64[D]")
        matches = np.flatnonzero(dates == target)
        if matches.size == 0:
            raise ValueError(f"panel_date={panel_date!r} not found in panel dates")
        idx = int(matches[-1])
    if idx < int(lookback) - 1:
        raise ValueError(f"panel index {idx} does not have lookback={lookback} history")
    return idx


def _find_panel_date_index(panel: PanelData, date_text: str | None) -> int | None:
    if not date_text:
        return None
    try:
        target = np.datetime64(str(date_text)[:10], "D")
    except Exception:
        return None
    dates = np.asarray(panel.dates).astype("datetime64[D]")
    matches = np.flatnonzero(dates == target)
    if matches.size == 0:
        return None
    return int(matches[-1])


def _price_snapshot(
    *,
    source: str,
    symbols: list[str],
    fallback_prices: np.ndarray,
    parquet_root: str | Path,
    prices_csv: str | Path | None,
    yahoo_chunk_size: int,
) -> PriceSnapshot:
    source_norm = str(source).strip().lower()
    if source_norm == "panel":
        prices = np.asarray(fallback_prices, dtype=np.float64).copy()
        return PriceSnapshot(prices=prices, source="panel_close", available_count=int(np.isfinite(prices).sum()))
    if source_norm == "csv":
        if prices_csv is None:
            raise ValueError("--prices-csv is required when price_source=csv")
        return load_prices_csv(prices_csv, symbols, fallback_prices)
    if source_norm == "yahoo":
        return fetch_yahoo_last_prices(
            symbols,
            fallback_prices,
            parquet_root=parquet_root,
            chunk_size=yahoo_chunk_size,
        )
    raise ValueError(f"price_source must be one of panel/csv/yahoo, got {source!r}")


def _write_outputs(result: LiveSignalResult, output_root: str | Path, asof_date: str) -> str:
    import polars as pl

    output_dir = Path(output_root) / "live_signals" / str(asof_date)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(result.summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "discord_message.md").write_text(result.message, encoding="utf-8")
    pl.DataFrame(result.weights_rows).write_parquet(output_dir / "target_weights.parquet")
    pl.DataFrame(result.rebalance_rows).write_parquet(output_dir / "rebalance.parquet")
    return str(output_dir)


def generate_live_signal(
    *,
    config_path: str | Path = "configs/experiment_baseline.yaml",
    output_dir: str | Path | None = None,
    fold_id: int | None = None,
    checkpoint_path: str | Path | None = None,
    weights_path: str | Path | None = None,
    panel_date: str | None = None,
    asof_date: str | None = None,
    price_source: str = "panel",
    prices_csv: str | Path | None = None,
    yahoo_chunk_size: int = 80,
    device: str | None = None,
    top_n: int = 20,
    min_abs_delta: float = 0.001,
    write: bool = True,
) -> LiveSignalResult:
    config = load_config(config_path)
    if device is not None:
        config.environment.device = str(device)
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1" if config.training.strict_no_fallback else "0"

    resolved_output_dir = Path(output_dir if output_dir is not None else config.runner.output_dir)
    resolved_fold_id, checkpoint = _resolve_checkpoint(resolved_output_dir, fold_id, checkpoint_path)

    panel = _build_panel(config)
    symbol_names = load_symbol_name_map(config.data.parquet_root)
    panel_idx = _resolve_panel_index(panel, panel_date, config.training.lookback)
    panel_date_str = _date_string(panel.dates[panel_idx])
    resolved_asof = asof_date or panel_date_str

    panel_prices = np.asarray(panel.close_prices[panel_idx], dtype=np.float64)
    price_snapshot = _price_snapshot(
        source=price_source,
        symbols=panel.symbols,
        fallback_prices=panel_prices,
        parquet_root=config.data.parquet_root,
        prices_csv=prices_csv,
        yahoo_chunk_size=yahoo_chunk_size,
    )
    current_prices = price_snapshot.prices

    previous_weights, previous_weights_date, previous_weights_path = _load_previous_weights(
        panel.symbols,
        output_dir=resolved_output_dir,
        fold_id=resolved_fold_id,
        weights_path=weights_path,
        asof_date=panel_date_str,
    )
    drift_base_idx = _find_panel_date_index(panel, previous_weights_date)
    if drift_base_idx is None:
        drift_base_idx = panel_idx
    drift_base_date = _date_string(panel.dates[drift_base_idx])
    drift_base_prices = np.asarray(panel.close_prices[drift_base_idx], dtype=np.float64)
    drift = estimate_drifted_weights(previous_weights, drift_base_prices, current_prices)

    runtime_device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    non_blocking = bool(config.training.non_blocking_transfer and runtime_device.type == "cuda")

    checkpoint_payload = _load_checkpoint(checkpoint)
    state_dict = checkpoint_payload.get("model_state_dict")
    if not isinstance(state_dict, dict):
        raise ValueError(f"Checkpoint does not contain model_state_dict: {checkpoint}")

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
    ).to(runtime_device)
    _load_state_dict(model, state_dict)
    model.eval()

    start = panel_idx - int(config.training.lookback) + 1
    x_np = np.nan_to_num(panel.features[start : panel_idx + 1], nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    mask_np = np.asarray(panel.tradable_mask[panel_idx], dtype=bool)
    can_buy_np = np.asarray(panel.can_buy_mask[panel_idx] if panel.can_buy_mask is not None else mask_np, dtype=bool)
    can_sell_np = np.asarray(panel.can_sell_mask[panel_idx] if panel.can_sell_mask is not None else mask_np, dtype=bool)

    with torch.inference_mode():
        x = torch.from_numpy(x_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking)
        mask = torch.from_numpy(mask_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking)
        with _autocast_context(runtime_device, amp_dtype):
            model_output = _call_model(model, x, mask, return_aux=False)
            model_weights_t, _ = _extract_weights_and_aux(model_output)
        zero_returns = torch.zeros_like(model_weights_t, dtype=torch.float32)
        initial = torch.from_numpy(drift.weights.astype(np.float32)).to(device=runtime_device, non_blocking=non_blocking)
        backtest = run_backtest_torch(
            model_weights_t.float(),
            zero_returns,
            mask,
            torch.zeros((1,), device=runtime_device, dtype=torch.float32),
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            gross_leverage=config.trading.gross_leverage,
            can_buy_mask=torch.from_numpy(can_buy_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking),
            can_sell_mask=torch.from_numpy(can_sell_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking),
            return_weights_history=True,
            initial_weights=initial,
        )
        model_weights = model_weights_t[0].detach().float().cpu().numpy().astype(np.float64)
        target_weights = backtest.final_weights.detach().float().cpu().numpy().astype(np.float64)
        turnover = float(backtest.turnovers[0].detach().float().cpu().item())
        estimated_trade_cost = -float(backtest.strategy_returns[0].detach().float().cpu().item())

    benchmark_simple = estimate_benchmark_return(
        panel.symbols,
        config.data.benchmark_name,
        drift_base_prices,
        current_prices,
        tradable_mask=mask_np,
    )
    rebalance_rows = build_rebalance_rows(
        panel.symbols,
        drift.weights,
        target_weights,
        current_prices,
        drift_base_prices,
        symbol_names=symbol_names,
        min_abs_delta=min_abs_delta,
    )
    top_positions = top_weight_rows(panel.symbols, target_weights, current_prices, symbol_names=symbol_names, top_n=top_n)

    weights_rows: list[dict[str, Any]] = []
    price_return = np.divide(
        current_prices,
        drift_base_prices,
        out=np.ones_like(current_prices, dtype=np.float64),
        where=np.isfinite(current_prices) & np.isfinite(drift_base_prices) & (drift_base_prices > 0.0),
    ) - 1.0
    for idx, symbol in enumerate(panel.symbols):
        weights_rows.append(
            {
                "date": resolved_asof,
                "panel_date": panel_date_str,
                "symbol": str(symbol),
                "name": str(symbol_names.get(str(symbol), "")),
                "model_weight": float(model_weights[idx]),
                "current_weight": float(drift.weights[idx]),
                "target_weight": float(target_weights[idx]),
                "delta_weight": float(target_weights[idx] - drift.weights[idx]),
                "base_price": float(drift_base_prices[idx]) if np.isfinite(drift_base_prices[idx]) else None,
                "panel_price": float(panel_prices[idx]) if np.isfinite(panel_prices[idx]) else None,
                "current_price": float(current_prices[idx]) if np.isfinite(current_prices[idx]) else None,
                "price_return": float(price_return[idx]) if np.isfinite(price_return[idx]) else None,
                "tradable": bool(mask_np[idx]),
                "can_buy": bool(can_buy_np[idx]),
                "can_sell": bool(can_sell_np[idx]),
            }
        )

    summary: dict[str, Any] = {
        "asof_date": resolved_asof,
        "panel_date": panel_date_str,
        "fold_id": int(resolved_fold_id),
        "checkpoint_path": str(checkpoint),
        "previous_weights_date": previous_weights_date,
        "previous_weights_path": previous_weights_path,
        "drift_base_date": drift_base_date,
        "price_source": price_snapshot.source,
        "price_timestamp": price_snapshot.timestamp,
        "price_available_count": int(price_snapshot.available_count),
        "symbol_count": int(panel.num_symbols),
        "valid_price_count": int(drift.valid_price_count),
        "portfolio_simple_return": float(drift.simple_return),
        "portfolio_log_return": float(drift.log_return),
        "benchmark_simple_return": float(benchmark_simple),
        "turnover": turnover,
        "estimated_trade_cost": estimated_trade_cost,
        "current_gross": float(np.abs(drift.weights).sum(dtype=np.float64)),
        "target_gross": float(np.abs(target_weights).sum(dtype=np.float64)),
        "top_positions": top_positions,
        "rebalance": rebalance_rows[: max(0, int(top_n))],
    }
    message = format_signal_message(summary, max_rows=top_n)
    result = LiveSignalResult(
        summary=summary,
        weights_rows=weights_rows,
        rebalance_rows=rebalance_rows,
        message=message,
        output_dir=None,
    )
    if write:
        result.output_dir = _write_outputs(result, resolved_output_dir, resolved_asof)
        result.summary["output_dir"] = result.output_dir
        (Path(result.output_dir) / "summary.json").write_text(
            json.dumps(result.summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return result
