from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
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
    portfolio_risk_summary,
    top_weight_rows,
)
from stockagent.live.quote_provider import PriceSnapshot, fetch_yahoo_last_prices, load_prices_csv, load_symbol_name_map
from stockagent.live.report_formatter import format_signal_message
from stockagent.live.market_status import cumulative_recent_returns, short_file_fingerprint
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

    output_dir = Path(output_root) / str(asof_date)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(result.summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "discord_message.md").write_text(result.message, encoding="utf-8")
    pl.DataFrame(result.weights_rows).write_parquet(output_dir / "target_weights.parquet")
    pl.DataFrame(result.rebalance_rows).write_parquet(output_dir / "rebalance.parquet")
    return str(output_dir)


def _signal_output_dir(output_root: str | Path, asof_date: str, signal_id: str | None) -> Path:
    root = Path(output_root) / str(asof_date)
    if signal_id:
        return root / str(signal_id)
    return root


def _write_outputs_to_dir(result: LiveSignalResult, output_dir: str | Path) -> str:
    import polars as pl

    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    (path / "summary.json").write_text(
        json.dumps(result.summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / "discord_message.md").write_text(result.message, encoding="utf-8")
    pl.DataFrame(result.weights_rows).write_parquet(path / "target_weights.parquet")
    pl.DataFrame(result.rebalance_rows).write_parquet(path / "rebalance.parquet")
    return str(path)


def _make_signal_id(market: str, asof_date: str) -> str:
    prefix = str(market or "default").strip() or "default"
    stamp = datetime.now().strftime("%H%M%S")
    return f"{prefix}-{asof_date}-{stamp}-{uuid.uuid4().hex[:6]}"


def _top_score_drivers(
    symbols: list[str],
    scores: np.ndarray | None,
    target_weights: np.ndarray,
    current_prices: np.ndarray,
    *,
    symbol_names: dict[str, str] | None,
    top_n: int = 8,
) -> list[dict[str, Any]]:
    if scores is None:
        return []
    score_arr = np.nan_to_num(np.asarray(scores, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    weights = np.nan_to_num(np.asarray(target_weights, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    prices = np.asarray(current_prices, dtype=np.float64)
    order = np.argsort(-np.abs(score_arr))
    rows: list[dict[str, Any]] = []
    for idx in order[: max(0, int(top_n))]:
        rows.append(
            {
                "symbol": str(symbols[int(idx)]),
                "name": str((symbol_names or {}).get(str(symbols[int(idx)]), "")),
                "score": float(score_arr[int(idx)]),
                "target_weight": float(weights[int(idx)]),
                "current_price": float(prices[int(idx)]) if np.isfinite(prices[int(idx)]) else None,
            }
        )
    return rows


def _feature_driver_summary(
    feature_names: list[str],
    latest_features: np.ndarray,
    target_weights: np.ndarray,
    *,
    top_n: int = 8,
) -> list[dict[str, Any]]:
    feature_values = np.nan_to_num(np.asarray(latest_features, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    weights = np.abs(np.nan_to_num(np.asarray(target_weights, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0))
    denom = float(weights.sum(dtype=np.float64))
    if denom <= 0.0 or feature_values.ndim != 2:
        return []
    scores = np.sum(np.abs(feature_values) * weights[:, None], axis=0) / denom
    order = np.argsort(-scores)
    rows: list[dict[str, Any]] = []
    for idx in order[: max(0, int(top_n))]:
        rows.append({"feature": str(feature_names[int(idx)]), "weighted_abs_value": float(scores[int(idx)])})
    return rows


def _risk_warnings(
    *,
    turnover: float,
    target_risk: dict[str, float],
    max_turnover_warning: float,
    max_top_weight_warning: float,
    max_gross_warning: float | None,
    recent_performance: dict[str, Any] | None,
) -> list[str]:
    warnings: list[str] = []
    if np.isfinite(turnover) and turnover > float(max_turnover_warning):
        warnings.append(f"turnover {turnover:.1%} exceeds {float(max_turnover_warning):.1%}")
    top_abs = float(target_risk.get("top_abs_weight", 0.0))
    if np.isfinite(top_abs) and top_abs > float(max_top_weight_warning):
        warnings.append(f"top weight {top_abs:.1%} exceeds {float(max_top_weight_warning):.1%}")
    gross = float(target_risk.get("gross", 0.0))
    if max_gross_warning is not None and np.isfinite(gross) and gross > float(max_gross_warning):
        warnings.append(f"gross exposure {gross:.1%} exceeds {float(max_gross_warning):.1%}")
    if recent_performance is not None:
        excess = recent_performance.get("excess_return")
        try:
            if float(excess) < 0.0:
                warnings.append(
                    f"recent {int(recent_performance.get('window_days', 0))}d underperformed benchmark by {abs(float(excess)):.1%}"
                )
        except Exception:
            pass
    return warnings


def generate_live_signal(
    *,
    market: str | None = None,
    market_label: str | None = None,
    config_path: str | Path = "configs/markets/tw.yaml",
    output_dir: str | Path | None = None,
    live_output_dir: str | Path | None = None,
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
    signal_id: str | None = None,
    benchmark_window_days: int = 20,
    max_turnover_warning: float = 1.5,
    max_top_weight_warning: float = 0.1,
    max_gross_warning: float | None = None,
    write: bool = True,
) -> LiveSignalResult:
    config = load_config(config_path)
    if device is not None:
        config.environment.device = str(device)
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1" if config.training.strict_no_fallback else "0"

    resolved_output_dir = Path(output_dir if output_dir is not None else config.runner.output_dir)
    resolved_fold_id, checkpoint = _resolve_checkpoint(resolved_output_dir, fold_id, checkpoint_path)
    market_id = str(market or "").strip()
    market_name = str(market_label or market_id or "").strip()

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
            model_output = _call_model(model, x, mask, return_aux=True)
            model_weights_t, aux = _extract_weights_and_aux(model_output)
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

    score_values: np.ndarray | None = None
    if aux is not None:
        score_tensor = aux.get("centered_score_logits")
        if score_tensor is None:
            score_tensor = aux.get("score_logits")
        if score_tensor is None:
            score_tensor = aux.get("rank_logits")
        if score_tensor is not None:
            score_values = score_tensor[0].detach().float().cpu().numpy().astype(np.float64)

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
    current_risk = portfolio_risk_summary(drift.weights)
    target_risk = portfolio_risk_summary(target_weights)
    recent_performance = cumulative_recent_returns(checkpoint, window_days=benchmark_window_days)
    risk_warnings = _risk_warnings(
        turnover=turnover,
        target_risk=target_risk,
        max_turnover_warning=max_turnover_warning,
        max_top_weight_warning=max_top_weight_warning,
        max_gross_warning=max_gross_warning if max_gross_warning is not None else float(config.trading.gross_leverage) * 1.05,
        recent_performance=recent_performance,
    )
    score_drivers = _top_score_drivers(
        panel.symbols,
        score_values,
        target_weights,
        current_prices,
        symbol_names=symbol_names,
        top_n=min(8, max(1, int(top_n))),
    )
    feature_drivers = _feature_driver_summary(
        panel.feature_names,
        x_np[-1],
        target_weights,
        top_n=min(8, max(1, int(top_n))),
    )
    confidence_proxy = None
    if score_values is not None:
        valid_scores = np.asarray(score_values, dtype=np.float64)[mask_np]
        if valid_scores.size:
            confidence_proxy = float(np.nanstd(valid_scores))

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

    resolved_signal_id = signal_id or _make_signal_id(market_id, resolved_asof)
    summary: dict[str, Any] = {
        "signal_id": resolved_signal_id,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "asof_date": resolved_asof,
        "market": market_id,
        "market_label": market_name,
        "panel_date": panel_date_str,
        "fold_id": int(resolved_fold_id),
        "checkpoint_path": str(checkpoint),
        "checkpoint_mtime": datetime.fromtimestamp(checkpoint.stat().st_mtime).astimezone().isoformat(timespec="seconds"),
        "checkpoint_fingerprint": short_file_fingerprint(checkpoint),
        "config_path": str(config_path),
        "config_fingerprint": short_file_fingerprint(Path(config_path)),
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
        "current_gross": float(current_risk["gross"]),
        "target_gross": float(target_risk["gross"]),
        "current_risk": current_risk,
        "target_risk": target_risk,
        "risk_warnings": risk_warnings,
        "recent_performance": recent_performance,
        "model_explanation": {
            "source": "score logits plus weighted latest-feature proxy",
            "confidence_proxy_score_std": confidence_proxy,
            "top_score_drivers": score_drivers,
            "top_feature_drivers": feature_drivers,
        },
        "top_positions": top_positions,
        "rebalance": rebalance_rows[: max(0, int(top_n))],
    }
    if write:
        if live_output_dir is not None:
            output_root = Path(live_output_dir)
        elif market_id:
            output_root = resolved_output_dir / "live_signals" / market_id
        else:
            output_root = resolved_output_dir / "live_signals"
        output_path = _signal_output_dir(output_root, resolved_asof, resolved_signal_id)
        summary["output_dir"] = str(output_path)
        summary["summary_path"] = str(output_path / "summary.json")
        summary["weights_path"] = str(output_path / "target_weights.parquet")
        summary["rebalance_path"] = str(output_path / "rebalance.parquet")
        summary["discord_message_path"] = str(output_path / "discord_message.md")
    message = format_signal_message(summary, max_rows=top_n)
    result = LiveSignalResult(
        summary=summary,
        weights_rows=weights_rows,
        rebalance_rows=rebalance_rows,
        message=message,
        output_dir=None,
    )
    if write:
        result.output_dir = _write_outputs_to_dir(result, summary["output_dir"])
        result.summary["output_dir"] = result.output_dir
        (Path(result.output_dir) / "summary.json").write_text(
            json.dumps(result.summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return result
