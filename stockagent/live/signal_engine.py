from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import numpy as np
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData, build_panel, build_tail_panel
from stockagent.live.portfolio_state import (
    build_rebalance_rows,
    classify_rebalance_action,
    estimate_benchmark_return,
    estimate_drifted_weights,
    portfolio_risk_summary,
)
from stockagent.live.quote_provider import (
    PriceSnapshot,
    fetch_tw_mis_last_prices,
    fetch_yahoo_last_prices,
    load_prices_csv,
    load_symbol_name_map,
)
from stockagent.live.report_formatter import format_signal_message, is_display_position_row
from stockagent.live.market_status import cumulative_recent_returns, short_file_fingerprint
from stockagent.live.time_display import DEFAULT_DISPLAY_TIMEZONE, display_timezone_label
from stockagent.models.factory import build_model
from stockagent.training.trainer import (
    _autocast_context,
    _call_model,
    _checkpoint_manifest,
    _extract_weights_and_aux,
    _align_panel_to_state_dict_universe,
    _configure_backtest_runtime_from_config,
    _load_checkpoint,
    _load_state_dict,
    _resolve_amp_dtype,
    _resolve_device,
    _validate_checkpoint_manifest,
)


@dataclass(slots=True)
class LiveSignalResult:
    summary: dict[str, Any]
    weights_rows: list[dict[str, Any]]
    rebalance_rows: list[dict[str, Any]]
    decision_rows: list[dict[str, Any]]
    message: str
    output_dir: str | None = None


LIVE_SIGNAL_WEIGHTS_NAME = "live_signal_weights.parquet"
ProgressCallback = Callable[[dict[str, Any]], None]
_LIVE_PANEL_CACHE_MAX_ENTRIES = 3
_LIVE_PANEL_CACHE: OrderedDict[str, PanelData] = OrderedDict()
_LIVE_PANEL_CACHE_LOCK = threading.Lock()


def clear_live_panel_memory_cache() -> None:
    with _LIVE_PANEL_CACHE_LOCK:
        _LIVE_PANEL_CACHE.clear()


def _live_panel_cache_key(
    config: ExperimentConfig,
    *,
    live_tail_rows: int,
    panel_kwargs: dict[str, Any],
) -> str:
    digest = hashlib.sha256()
    digest.update(str(Path(config.data.parquet_root).resolve()).encode("utf-8"))
    digest.update(str(int(live_tail_rows)).encode("ascii"))
    digest.update(repr(sorted(panel_kwargs.items())).encode("utf-8"))
    root = Path(config.data.parquet_root)
    source_paths = sorted(root.glob("*_features.parquet"))
    external_path = panel_kwargs.get("external_feature_path")
    if external_path:
        source_paths.append(Path(str(external_path)))
    symbols_path = root / "symbols.csv"
    if symbols_path.exists():
        source_paths.append(symbols_path)
    for path in source_paths:
        stat = path.stat()
        digest.update(str(path.resolve()).encode("utf-8"))
        digest.update(
            f":{stat.st_size}:{stat.st_mtime_ns}:{stat.st_ctime_ns}".encode("ascii")
        )
    return digest.hexdigest()


def _cached_live_panel(key: str) -> PanelData | None:
    with _LIVE_PANEL_CACHE_LOCK:
        panel = _LIVE_PANEL_CACHE.get(key)
        if panel is not None:
            _LIVE_PANEL_CACHE.move_to_end(key)
        return panel


def _remember_live_panel(key: str, panel: PanelData) -> None:
    with _LIVE_PANEL_CACHE_LOCK:
        _LIVE_PANEL_CACHE[key] = panel
        _LIVE_PANEL_CACHE.move_to_end(key)
        while len(_LIVE_PANEL_CACHE) > _LIVE_PANEL_CACHE_MAX_ENTRIES:
            _LIVE_PANEL_CACHE.popitem(last=False)


def _emit_progress(
    callback: ProgressCallback | None,
    *,
    label: str,
    step: int,
    total: int,
    message: str,
) -> None:
    if callback is None:
        return
    try:
        callback(
            {
                "label": label,
                "step": int(step),
                "total": int(total),
                "message": str(message),
                "timestamp": datetime.now().isoformat(timespec="seconds"),
            }
        )
    except Exception:
        return


def _date_string(value: object) -> str:
    raw_text = str(value).replace("T", " ")
    has_time = ":" in raw_text
    try:
        dt = np.asarray(value).astype("datetime64[s]")
        text = str(np.datetime_as_string(dt, unit="s")).replace("T", " ")
        if text.endswith(" 00:00:00") and not has_time:
            return text[:10]
        return text
    except Exception:
        text = raw_text
        if len(text) >= 19 and not text.endswith(" 00:00:00"):
            return text[:19]
        if len(text) >= 19 and has_time:
            return text[:19]
        return text[:10] if len(text) >= 10 else text


def _datetime64_second(value: object) -> np.datetime64 | None:
    text = str(value or "").replace("T", " ").strip()
    if not text or text.lower() in {"nat", "none", "null"}:
        return None
    try:
        return np.datetime64(text.replace(" ", "T"), "s")
    except Exception:
        try:
            return np.datetime64(text[:10], "D").astype("datetime64[s]")
        except Exception:
            return None


def _build_panel(config: ExperimentConfig, *, live_tail: bool = False) -> PanelData:
    live_tail_rows = int(getattr(config.data, "live_tail_panel_rows", 0) or 0)
    use_tw_public_features = bool(config.data.use_tw_public_features)
    use_tw_public_rules = bool(config.data.use_tw_public_rules)
    use_tw_public_data = use_tw_public_features or use_tw_public_rules
    panel_kwargs = {
        "benchmark_name": config.data.benchmark_name,
        "usd_only_trading_pairs": config.data.usd_only_trading_pairs,
        "tradable_mode": config.data.tradable_mode,
        "trading_volume_policy": config.data.trading_volume_policy,
        "security_filter": config.data.security_filter,
        "strict_no_fallback": config.training.strict_no_fallback,
        "panel_backend": config.data.panel_backend,
        "panel_load_workers": config.data.panel_load_workers,
        "external_feature_path": (
            config.data.tw_public_feature_path if use_tw_public_data else None
        ),
        "external_market_symbol": config.data.tw_public_market_symbol,
        "external_include_features": use_tw_public_features,
        "external_include_rules": use_tw_public_rules,
        "external_data_required": use_tw_public_data,
        "feature_include": config.data.feature_include,
        "feature_exclude": config.data.feature_exclude,
        "feature_zero_fill": config.data.feature_zero_fill,
        "panel_start_date": config.data.panel_start_date,
    }
    if live_tail:
        if live_tail_rows <= 0:
            live_tail_rows = max(int(config.training.lookback) + 8, 48)
        cache_key = _live_panel_cache_key(
            config,
            live_tail_rows=live_tail_rows,
            panel_kwargs=panel_kwargs,
        )
        cached = _cached_live_panel(cache_key)
        if cached is not None:
            print(
                f"[panel] live memory cache hit dates={cached.num_dates} "
                f"symbols={cached.num_symbols}"
            )
            return cached
        panel = build_tail_panel(
            config.data.parquet_root,
            tail_rows=live_tail_rows,
            benchmark_name=config.data.benchmark_name,
            usd_only_trading_pairs=config.data.usd_only_trading_pairs,
            tradable_mode=config.data.tradable_mode,
            trading_volume_policy=config.data.trading_volume_policy,
            security_filter=config.data.security_filter,
            strict_no_fallback=config.training.strict_no_fallback,
            panel_load_workers=config.data.panel_load_workers,
            external_feature_path=(
                config.data.tw_public_feature_path if use_tw_public_data else None
            ),
            external_market_symbol=config.data.tw_public_market_symbol,
            external_include_features=use_tw_public_features,
            external_include_rules=use_tw_public_rules,
            external_data_required=use_tw_public_data,
            feature_include=config.data.feature_include,
            feature_exclude=config.data.feature_exclude,
            feature_zero_fill=config.data.feature_zero_fill,
            panel_start_date=config.data.panel_start_date,
        )
        _remember_live_panel(cache_key, panel)
        return panel
    return build_panel(
        config.data.parquet_root,
        **panel_kwargs,
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
        return pl.read_csv(path, infer_schema_length=None)
    raise ValueError(f"Unsupported table format: {path}")


def _is_intraday_frequency(frequency: object) -> bool:
    text = str(frequency or "").strip().lower().replace("_", "-")
    if not text:
        return False
    if text in {"bar", "intraday", "minute", "minutes", "15m", "15min", "15-min", "15-minute"}:
        return True
    return text.endswith("m") and text[:-1].isdigit()


def _tail_panel_dates(panel: PanelData, rows: int) -> PanelData:
    count = max(1, int(rows))
    if int(panel.num_dates) <= count:
        return panel
    slc = slice(int(panel.num_dates) - count, int(panel.num_dates))
    return PanelData(
        dates=panel.dates[slc],
        symbols=list(panel.symbols),
        feature_names=list(panel.feature_names),
        features=panel.features[slc],
        returns_1d=panel.returns_1d[slc],
        tradable_mask=panel.tradable_mask[slc],
        alive_mask=panel.alive_mask[slc],
        benchmark_returns=panel.benchmark_returns[slc],
        close_prices=panel.close_prices[slc],
        can_buy_mask=panel.can_buy_mask[slc] if panel.can_buy_mask is not None else None,
        can_sell_mask=panel.can_sell_mask[slc] if panel.can_sell_mask is not None else None,
        can_short_open_mask=panel.can_short_open_mask[slc] if panel.can_short_open_mask is not None else None,
        force_short_cover_mask=panel.force_short_cover_mask[slc] if panel.force_short_cover_mask is not None else None,
        force_exit_mask=panel.force_exit_mask[slc] if panel.force_exit_mask is not None else None,
    )


def _candidate_weights_paths(output_dir: str | Path, fold_id: int, *, prefer_live_weights: bool = True) -> list[Path]:
    fold_dir = Path(output_dir) / f"fold_{int(fold_id):02d}"
    live_names = (LIVE_SIGNAL_WEIGHTS_NAME, "live_signal_weights.csv")
    artifact_names = ("daily_weights.parquet", "daily_weights.csv")
    names = (*live_names, *artifact_names) if prefer_live_weights else (*artifact_names, *live_names)
    paths: list[Path] = []
    for name in names:
        path = fold_dir / name
        if path.exists():
            paths.append(path)
    return paths


def _load_previous_weights(
    symbols: list[str],
    *,
    output_dir: str | Path,
    fold_id: int,
    weights_path: str | Path | None,
    asof_date: str | None,
    prefer_live_weights: bool = True,
    strictly_before_asof: bool = False,
) -> tuple[np.ndarray, str | None, str | None]:
    if weights_path is not None:
        explicit_path = Path(weights_path)
        paths = (
            _candidate_weights_paths(
                output_dir,
                fold_id,
                prefer_live_weights=True,
            )
            if prefer_live_weights
            else []
        )
        if explicit_path not in paths:
            paths.append(explicit_path)
    else:
        paths = _candidate_weights_paths(
            output_dir,
            fold_id,
            prefer_live_weights=prefer_live_weights,
        )
    if not paths:
        return np.zeros((len(symbols),), dtype=np.float64), None, None

    last_seen_path: str | None = None
    best_row: dict[str, Any] | None = None
    best_date: np.datetime64 | None = None
    best_path: str | None = None
    best_rank: int | None = None
    for path in paths:
        if not path.exists():
            continue
        last_seen_path = str(path)
        frame = _read_table(path)
        if "date" not in frame.columns or frame.height == 0:
            continue

        if asof_date:
            asof = _datetime64_second(asof_date)
            keep = []
            for raw in frame.get_column("date").to_list():
                raw_dt = _datetime64_second(raw)
                if asof is None or raw_dt is None:
                    keep.append(True)
                elif strictly_before_asof:
                    keep.append(bool(raw_dt < asof))
                else:
                    keep.append(bool(raw_dt <= asof))
            if any(keep):
                import polars as pl

                frame = frame.filter(pl.Series(keep))
            else:
                continue

        frame = frame.sort("date")
        row = frame.tail(1).to_dicts()[0]
        row_date = _datetime64_second(row.get("date"))
        path_rank = paths.index(path)
        if row_date is None:
            if best_row is None:
                best_row = row
                best_path = str(path)
                best_rank = path_rank
            continue
        if best_date is None or row_date > best_date or (row_date == best_date and path_rank < int(best_rank or 0)):
            best_row = row
            best_date = row_date
            best_path = str(path)
            best_rank = path_rank

    if best_row is None:
        return np.zeros((len(symbols),), dtype=np.float64), None, last_seen_path
    weights = np.zeros((len(symbols),), dtype=np.float64)
    for idx, symbol in enumerate(symbols):
        value = best_row.get(symbol)
        if value is None:
            continue
        try:
            weights[idx] = float(value)
        except Exception:
            continue
    return weights, _date_string(best_row.get("date")), best_path


def _resolve_panel_index(panel: PanelData, panel_date: str | None, lookback: int) -> int:
    if panel_date is None or str(panel_date).strip().lower() in {"", "latest", "last"}:
        idx = int(panel.num_dates - 1)
    else:
        target_dt = _datetime64_second(panel_date)
        dates_s = np.asarray(panel.dates).astype("datetime64[s]")
        matches = np.flatnonzero(dates_s == target_dt) if target_dt is not None else np.array([], dtype=np.int64)
        if matches.size == 0:
            target = np.datetime64(str(panel_date)[:10], "D")
            dates = np.asarray(panel.dates).astype("datetime64[D]")
            matches = np.flatnonzero(dates == target)
        if matches.size == 0:
            raise ValueError(f"panel_date={panel_date!r} not found in panel dates")
        idx = int(matches[-1])
    if idx < int(lookback) - 1:
        raise ValueError(f"panel index {idx} does not have lookback={lookback} history")
    return idx


def _resolve_usable_panel_index(panel: PanelData, panel_date: str | None, lookback: int) -> tuple[int, str | None]:
    idx = _resolve_panel_index(panel, panel_date, lookback)
    mask = np.asarray(panel.tradable_mask[idx], dtype=bool)
    if bool(mask.any()):
        return idx, None

    min_idx = int(lookback) - 1
    for candidate in range(idx - 1, min_idx - 1, -1):
        candidate_mask = np.asarray(panel.tradable_mask[candidate], dtype=bool)
        if bool(candidate_mask.any()):
            original = _date_string(panel.dates[idx])
            usable = _date_string(panel.dates[candidate])
            return candidate, f"panel `{original}` 沒有可交易標的，改用最近可用資料 `{usable}`。"

    raise ValueError(
        f"panel_date={_date_string(panel.dates[idx])!r} has no tradable symbols, "
        f"and no earlier usable row exists with lookback={lookback}"
    )


def _previous_usable_panel_date(panel: PanelData, panel_idx: int, lookback: int) -> str | None:
    min_idx = int(lookback) - 1
    for candidate in range(int(panel_idx) - 1, min_idx - 1, -1):
        candidate_mask = np.asarray(panel.tradable_mask[candidate], dtype=bool)
        if bool(candidate_mask.any()):
            return _date_string(panel.dates[candidate])
    return None


def _find_panel_date_index(panel: PanelData, date_text: str | None) -> int | None:
    if not date_text:
        return None
    target_dt = _datetime64_second(date_text)
    dates_s = np.asarray(panel.dates).astype("datetime64[s]")
    matches = np.flatnonzero(dates_s == target_dt) if target_dt is not None else np.array([], dtype=np.int64)
    if matches.size == 0:
        try:
            target = np.datetime64(str(date_text)[:10], "D")
        except Exception:
            return None
        dates = np.asarray(panel.dates).astype("datetime64[D]")
        matches = np.flatnonzero(dates == target)
    if matches.size == 0:
        return None
    return int(matches[-1])


def _weights_file_has_date(path: Path, date_text: str | None) -> bool:
    if not date_text:
        return False
    if not path.exists():
        return False
    try:
        frame = _read_table(path)
    except Exception:
        return False
    if "date" not in frame.columns or frame.height == 0:
        return False
    target = _datetime64_second(date_text)
    for raw in frame.get_column("date").to_list():
        raw_dt = _datetime64_second(raw)
        if target is not None and raw_dt is not None:
            if raw_dt == target:
                return True
            continue
        if _date_string(raw) == _date_string(date_text):
            return True
    return False


def _live_weights_has_date(fold_dir: str | Path, date_text: str | None) -> bool:
    return _weights_file_has_date(
        Path(fold_dir) / LIVE_SIGNAL_WEIGHTS_NAME,
        date_text,
    )


def _weights_history_has_date(fold_dir: str | Path, date_text: str | None) -> bool:
    root = Path(fold_dir)
    return any(
        _weights_file_has_date(root / name, date_text)
        for name in (
            LIVE_SIGNAL_WEIGHTS_NAME,
            "live_signal_weights.csv",
            "daily_weights.parquet",
            "daily_weights.csv",
        )
    )


def write_live_weights_history(
    fold_dir: str | Path,
    summary: dict[str, Any],
    weights_rows: list[dict[str, Any]],
) -> str | None:
    if not weights_rows:
        return None
    date_text = str(
        summary.get("weights_date")
        or summary.get("panel_data_date")
        or summary.get("panel_date")
        or summary.get("asof_date")
        or ""
    ).strip()
    if not date_text:
        return None

    import polars as pl

    path = Path(fold_dir) / LIVE_SIGNAL_WEIGHTS_NAME
    row: dict[str, Any] = {"date": date_text}
    for item in weights_rows:
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        try:
            row[symbol] = float(item.get("target_weight") or 0.0)
        except Exception:
            row[symbol] = 0.0
    new_frame = pl.DataFrame([row], infer_schema_length=None)
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, new_frame], how="diagonal_relaxed")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        combined = new_frame
    combined = combined.sort("date").unique(subset=["date"], keep="last", maintain_order=True).sort("date")
    combined.write_parquet(path)
    return str(path)


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
    if source_norm in {"tw", "twse", "tpex", "mis", "tw_mis"}:
        return fetch_tw_mis_last_prices(
            symbols,
            fallback_prices,
            parquet_root=parquet_root,
            chunk_size=yahoo_chunk_size,
        )
    raise ValueError(f"price_source must be one of panel/csv/yahoo/tw, got {source!r}")


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
    pl.DataFrame(result.decision_rows).write_parquet(output_dir / "decision_explanations.parquet")
    _write_text_artifacts(result, output_dir)
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
    pl.DataFrame(result.decision_rows).write_parquet(path / "decision_explanations.parquet")
    _write_text_artifacts(result, path)
    return str(path)


def _make_signal_id(market: str, asof_date: str) -> str:
    prefix = str(market or "default").strip() or "default"
    stamp = datetime.now().strftime("%H%M%S")
    return f"{prefix}-{asof_date}-{stamp}-{uuid.uuid4().hex[:6]}"


def _display_zone(timezone_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(str(timezone_name or DEFAULT_DISPLAY_TIMEZONE))
    except Exception:
        return ZoneInfo(DEFAULT_DISPLAY_TIMEZONE)


def _now_text(timezone_name: str | None) -> str:
    return datetime.now(_display_zone(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_daily_bar_time(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
    except Exception:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def _daily_bar_timestamp(value: str | None, daily_bar_time: str | None) -> str | None:
    if not value:
        return value
    bar_time = _normalize_daily_bar_time(daily_bar_time)
    if bar_time is None:
        return value
    text = str(value).replace("T", " ").strip()
    if len(text) < 10:
        return value
    normalized = text[:19]
    time_part = normalized[11:].strip() if len(normalized) > 10 else ""
    has_non_midnight_time = ":" in time_part and time_part not in {"00:00", "00:00:00"}
    if has_non_midnight_time:
        return normalized
    return f"{text[:10]} {bar_time}"


def _parse_local_datetime(value: str | None, timezone_name: str | None) -> datetime | None:
    text = str(value or "").strip().replace("T", " ")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text[:19])
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_display_zone(timezone_name))
    return parsed


def _daily_price_timestamp(
    *,
    price_snapshot: PriceSnapshot,
    resolved_asof: str,
    panel_display_date: str | None,
    daily_bar_time: str | None,
    source_timezone: str | None,
    intraday_frequency: bool,
) -> str | None:
    source = str(price_snapshot.source or "").strip().lower()
    if intraday_frequency:
        return price_snapshot.timestamp or panel_display_date
    if source.startswith("panel") or source in {"panel", "close", "panel_close"}:
        return panel_display_date

    close_time = _normalize_daily_bar_time(daily_bar_time)
    asof_dt = _parse_local_datetime(resolved_asof, source_timezone)
    if close_time is not None and asof_dt is not None:
        hour, minute, second = (int(part) for part in close_time.split(":"))
        close_dt = asof_dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if asof_dt >= close_dt:
            return close_dt.strftime("%Y-%m-%d %H:%M:%S")
    return price_snapshot.timestamp or resolved_asof


def _decision_weights_timestamp(
    *,
    panel_data_date: str,
    resolved_asof: str,
    uses_realtime_daily_prices: bool,
) -> str:
    if uses_realtime_daily_prices:
        return str(resolved_asof)
    return str(panel_data_date)


def _finite_float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not np.isfinite(number):
        return None
    return number


def _position_stock_return(weight: float, price_return: float | None) -> float | None:
    raw_return = _finite_float_or_none(price_return)
    if raw_return is None:
        return None
    position = _finite_float_or_none(weight)
    if position is None:
        return None
    if abs(position) < 1e-12:
        return 0.0
    return raw_return if position > 0.0 else -raw_return


def _position_portfolio_contribution(weight: float, price_return: float | None) -> float | None:
    raw_return = _finite_float_or_none(price_return)
    position = _finite_float_or_none(weight)
    if raw_return is None or position is None:
        return None
    return position * raw_return


def _fmt_md_value(value: Any, *, pct: bool = False, digits: int = 4) -> str:
    number = _finite_float_or_none(value)
    if number is None:
        text = "" if value is None else str(value)
    elif pct:
        text = f"{number * 100:.{digits}f}%"
    else:
        text = f"{number:.{digits}f}"
    return text.replace("|", "\\|").replace("\n", " ")


def _fmt_md_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _aux_scalar_by_symbol(
    aux: dict[str, torch.Tensor] | None,
    key: str,
    symbol_count: int,
    *,
    reduction: str = "mean",
) -> np.ndarray | None:
    if aux is None:
        return None
    tensor = aux.get(key)
    if tensor is None:
        return None
    try:
        arr = tensor[0].detach().float().cpu().numpy().astype(np.float64)
    except Exception:
        return None
    if arr.ndim == 0:
        return None
    if arr.shape[0] != int(symbol_count):
        return None
    if arr.ndim == 1:
        out = arr
    elif reduction == "norm":
        out = np.linalg.norm(arr.reshape((arr.shape[0], -1)), axis=1)
    else:
        out = np.nanmean(arr.reshape((arr.shape[0], -1)), axis=1)
    return np.asarray(out, dtype=np.float64)


def _abs_rank(values: np.ndarray | None, symbol_count: int) -> np.ndarray:
    ranks = np.zeros((int(symbol_count),), dtype=np.int64)
    if values is None:
        return ranks
    arr = np.nan_to_num(np.asarray(values, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    order = np.argsort(-np.abs(arr))
    ranks[order] = np.arange(1, len(order) + 1, dtype=np.int64)
    return ranks


def _constraint_note(
    *,
    tradable: bool,
    can_buy: bool,
    can_sell: bool,
    current_weight: float,
    target_weight: float,
) -> str:
    if not tradable:
        return "not_tradable"
    if target_weight > current_weight and not can_buy:
        return "buy_blocked"
    if target_weight < current_weight and not can_sell:
        return "sell_blocked"
    return ""


def _position_status(
    *,
    tradable: bool,
    current_weight: float,
    target_weight: float,
    model_weight: float,
    eps: float = 1e-9,
) -> str:
    current_active = abs(float(current_weight)) > float(eps)
    target_active = abs(float(target_weight)) > float(eps)
    model_active = abs(float(model_weight)) > float(eps)
    if not tradable:
        if current_active or target_active:
            return "locked_untradable"
        return "untradable"
    if target_active:
        return "active"
    if model_active:
        return "model_flattened_by_constraints"
    return "flat"


def _decision_reason(action: str, score: float | None, model_weight: float, constraint: str) -> str:
    pieces: list[str] = []
    if score is None:
        pieces.append("score_unavailable")
    elif score > 0.0:
        pieces.append("positive_score")
    elif score < 0.0:
        pieces.append("negative_score")
    else:
        pieces.append("neutral_score")
    if model_weight > 0.0:
        pieces.append("model_long")
    elif model_weight < 0.0:
        pieces.append("model_short")
    else:
        pieces.append("model_flat")
    pieces.append(f"action_{action.lower()}")
    if constraint:
        pieces.append(constraint)
    return "; ".join(pieces)


def _build_decision_rows(
    *,
    symbols: list[str],
    symbol_names: dict[str, str],
    asof_date: str,
    panel_date: str,
    model_weights: np.ndarray,
    current_weights: np.ndarray,
    target_weights: np.ndarray,
    scores: np.ndarray | None,
    current_prices: np.ndarray,
    base_prices: np.ndarray,
    price_returns: np.ndarray,
    tradable_mask: np.ndarray,
    can_buy_mask: np.ndarray,
    can_sell_mask: np.ndarray,
    aux: dict[str, torch.Tensor] | None,
) -> list[dict[str, Any]]:
    symbol_count = len(symbols)
    score_arr = None
    if scores is not None:
        score_arr = np.asarray(scores, dtype=np.float64)
    score_ranks = _abs_rank(score_arr, symbol_count)
    target_ranks = _abs_rank(target_weights, symbol_count)
    gate = _aux_scalar_by_symbol(aux, "stock_market_gate", symbol_count, reduction="mean")
    market_delta_norm = _aux_scalar_by_symbol(aux, "z_market_delta", symbol_count, reduction="norm")

    rows: list[dict[str, Any]] = []
    for idx, symbol in enumerate(symbols):
        current_weight = float(current_weights[idx])
        target_weight = float(target_weights[idx])
        delta_weight = float(target_weight - current_weight)
        action = classify_rebalance_action(current_weight, target_weight, delta_weight=delta_weight)
        score = _finite_float_or_none(score_arr[idx]) if score_arr is not None else None
        raw_price_return = _finite_float_or_none(price_returns[idx])
        constraint = _constraint_note(
            tradable=bool(tradable_mask[idx]),
            can_buy=bool(can_buy_mask[idx]),
            can_sell=bool(can_sell_mask[idx]),
            current_weight=current_weight,
            target_weight=target_weight,
        )
        rows.append(
            {
                "date": asof_date,
                "panel_date": panel_date,
                "symbol": str(symbol),
                "name": str(symbol_names.get(str(symbol), "")),
                "action": action,
                "decision_reason": _decision_reason(action, score, float(model_weights[idx]), constraint),
                "constraint": constraint,
                "score": score,
                "abs_score_rank": int(score_ranks[idx]),
                "model_weight": float(model_weights[idx]),
                "current_weight": current_weight,
                "target_weight": target_weight,
                "delta_weight": delta_weight,
                "abs_delta_weight": abs(delta_weight),
                "abs_target_rank": int(target_ranks[idx]),
                "trade_price": _finite_float_or_none(current_prices[idx]),
                "current_price": _finite_float_or_none(current_prices[idx]),
                "base_price": _finite_float_or_none(base_prices[idx]),
                "price_return": raw_price_return,
                "stock_return": _position_stock_return(current_weight, raw_price_return),
                "portfolio_contribution": _position_portfolio_contribution(current_weight, raw_price_return),
                "tradable": bool(tradable_mask[idx]),
                "can_buy": bool(can_buy_mask[idx]),
                "can_sell": bool(can_sell_mask[idx]),
                "position_status": _position_status(
                    tradable=bool(tradable_mask[idx]),
                    current_weight=current_weight,
                    target_weight=target_weight,
                    model_weight=float(model_weights[idx]),
                ),
                "stock_market_gate": _finite_float_or_none(gate[idx]) if gate is not None else None,
                "market_delta_norm": _finite_float_or_none(market_delta_norm[idx]) if market_delta_norm is not None else None,
            }
        )
    rows.sort(
        key=lambda row: (
            float(row.get("abs_delta_weight") or 0.0),
            abs(float(row.get("target_weight") or 0.0)),
            abs(float(row.get("score") or 0.0)),
        ),
        reverse=True,
    )
    return rows


def _top_position_rows_from_weights(rows: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    limit = max(0, int(top_n))
    if limit == 0:
        return []
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            abs(float(row.get("target_weight") or 0.0)),
            abs(float(row.get("current_weight") or 0.0)),
            abs(float(row.get("delta_weight") or 0.0)),
        ),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for row in sorted_rows:
        weight = float(row.get("target_weight") or 0.0)
        if not is_display_position_row(row):
            continue
        out.append(
            {
                "symbol": str(row.get("symbol") or ""),
                "name": str(row.get("name") or ""),
                "weight": weight,
                "abs_weight": abs(weight),
                "current_weight": float(row.get("current_weight") or 0.0),
                "target_weight": weight,
                "delta_weight": float(row.get("delta_weight") or 0.0),
                "current_price": row.get("current_price"),
                "model_weight": float(row.get("model_weight") or 0.0),
                "tradable": bool(row.get("tradable", True)),
                "can_buy": bool(row.get("can_buy", True)),
                "can_sell": bool(row.get("can_sell", True)),
                "position_status": str(row.get("position_status") or ""),
            }
        )
        if len(out) >= limit:
            break
    return out


def _markdown_table(title: str, rows: list[dict[str, Any]], columns: list[tuple[str, str, str]]) -> str:
    lines = [f"# {title}", "", f"rows: {len(rows)}", ""]
    lines.append("| " + " | ".join(label for label, _, _ in columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        cells: list[str] = []
        for _, key, kind in columns:
            if kind == "pct":
                cells.append(_fmt_md_value(row.get(key), pct=True, digits=4))
            elif kind == "price":
                cells.append(_fmt_md_value(row.get(key), digits=4))
            elif kind == "float":
                cells.append(_fmt_md_value(row.get(key), digits=6))
            else:
                cells.append(_fmt_md_text(row.get(key)))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def _action_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        action = str(row.get("action") or "UNKNOWN")
        counts[action] = counts.get(action, 0) + 1
    return dict(sorted(counts.items()))


def _top_action_rows(rows: list[dict[str, Any]], action: str, limit: int = 15) -> list[dict[str, Any]]:
    action_upper = action.upper()
    matched = [row for row in rows if str(row.get("action") or "").upper() == action_upper]
    return sorted(
        matched,
        key=lambda row: (
            abs(float(row.get("delta_weight") or 0.0)),
            abs(float(row.get("target_weight") or 0.0)),
            abs(float(row.get("score") or 0.0)),
        ),
        reverse=True,
    )[: max(0, int(limit))]


def _compact_decision_bullets(rows: list[dict[str, Any]], limit: int = 15) -> list[str]:
    lines: list[str] = []
    for row in rows[: max(0, int(limit))]:
        label = str(row.get("symbol") or "")
        name = str(row.get("name") or "").strip()
        if name:
            label += f" {name}"
        lines.append(
            "- "
            f"{label}: {row.get('action', 'HOLD')} "
            f"delta={_fmt_md_value(row.get('delta_weight'), pct=True, digits=2)} "
            f"target={_fmt_md_value(row.get('target_weight'), pct=True, digits=2)} "
            f"score={_fmt_md_value(row.get('score'), digits=4)} "
            f"rank={row.get('abs_score_rank', '')} "
            f"px={_fmt_md_value(row.get('trade_price'), digits=2)} "
            f"reason={row.get('decision_reason', '')}"
        )
    return lines


def _decision_report_markdown(summary: dict[str, Any], decision_rows: list[dict[str, Any]]) -> str:
    explanation = summary.get("model_explanation", {}) if isinstance(summary.get("model_explanation"), dict) else {}
    counts = _action_counts(decision_rows)
    lines = [
        "# Live Decision Explanation Report",
        "",
        "## Signal",
        "",
        f"- signal_id: `{summary.get('signal_id', 'n/a')}`",
        f"- market: `{summary.get('market', 'n/a')}` {summary.get('market_label', '') or ''}".rstrip(),
        f"- asof_date: `{summary.get('asof_date', 'n/a')}`",
        f"- panel_date: `{summary.get('panel_date', 'n/a')}`",
        f"- fold: `{summary.get('fold_id', 'n/a')}`",
        f"- price_source: `{summary.get('price_source', 'n/a')}`",
        f"- explanation_source: {explanation.get('source', 'score/weight decision table')}",
        f"- confidence_proxy_score_std: {_fmt_md_value(explanation.get('confidence_proxy_score_std'), digits=6)}",
        "",
        "## Action Counts",
        "",
    ]
    for action in ("BUY", "SELL", "REDUCE", "EXIT", "HOLD"):
        lines.append(f"- {action}: {counts.get(action, 0)}")
    unknown = sum(value for key, value in counts.items() if key not in {"BUY", "SELL", "REDUCE", "EXIT", "HOLD"})
    if unknown:
        lines.append(f"- UNKNOWN: {unknown}")

    top_features = explanation.get("top_feature_drivers") if isinstance(explanation.get("top_feature_drivers"), list) else []
    if top_features:
        lines.extend(["", "## Market-Level Feature Drivers", ""])
        for row in top_features:
            if isinstance(row, dict):
                lines.append(
                    f"- {row.get('feature')}: weighted_abs_value={_fmt_md_value(row.get('weighted_abs_value'), digits=6)}"
                )

    top_scores = explanation.get("top_score_drivers") if isinstance(explanation.get("top_score_drivers"), list) else []
    if top_scores:
        lines.extend(["", "## Largest Score Drivers", ""])
        for row in top_scores:
            if not isinstance(row, dict):
                continue
            label = str(row.get("symbol") or "")
            name = str(row.get("name") or "").strip()
            if name:
                label += f" {name}"
            lines.append(
                f"- {label}: score={_fmt_md_value(row.get('score'), digits=6)} "
                f"target={_fmt_md_value(row.get('target_weight'), pct=True, digits=2)} "
                f"px={_fmt_md_value(row.get('current_price'), digits=2)}"
            )

    for action in ("BUY", "SELL", "REDUCE", "EXIT", "HOLD"):
        rows = _top_action_rows(decision_rows, action, limit=20 if action != "HOLD" else 10)
        if not rows:
            continue
        lines.extend(["", f"## Top {action}", ""])
        lines.extend(_compact_decision_bullets(rows, limit=len(rows)))

    lines.extend(
        [
            "",
            "## Field Guide",
            "",
            "- score: model score/logit used for cross-sectional ranking. Positive usually supports long exposure; negative usually supports short or sell pressure.",
            "- model_weight: raw model portfolio weight before the trading simulator applies turnover, fee, leverage, and buy/sell constraints.",
            "- current_weight: drifted current holding before today's rebalance.",
            "- target_weight: final weight after the trading simulator and constraints.",
            "- delta_weight: target_weight minus current_weight. Positive means buy/add, negative means sell/reduce.",
            "- stock_market_gate: Transformer market-token gate when available. Higher means this stock's representation used more market-token context.",
            "- market_delta_norm: magnitude of market-token adjustment when available. Higher means market context changed the stock embedding more.",
            "- constraint: buy_blocked, sell_blocked, or not_tradable when the market mask affected the action.",
            "- position_status: active, flat, untradable, locked_untradable, or model_flattened_by_constraints. locked_untradable means the model assigned zero weight but the simulator preserved an existing position because the symbol cannot trade.",
            "- decision_reason: compact rule trace derived from score sign, model direction, action, and constraint.",
            "",
            "## Artifact Paths",
            "",
            f"- summary: `{summary.get('summary_path', 'n/a')}`",
            f"- full_table: `{summary.get('decision_explanation_path', 'n/a')}`",
            f"- markdown_table: `{summary.get('decision_explanation_markdown_path', 'n/a')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_text_artifacts(result: LiveSignalResult, output_dir: Path) -> None:
    positions = sorted(
        [
            row
            for row in result.weights_rows
            if is_display_position_row(row)
        ],
        key=lambda row: (abs(float(row.get("target_weight") or 0.0)), abs(float(row.get("delta_weight") or 0.0))),
        reverse=True,
    )
    (output_dir / "target_positions.md").write_text(
        _markdown_table(
            "Target Positions",
            positions,
            [
                ("symbol", "symbol", "text"),
                ("name", "name", "text"),
                ("target", "target_weight", "pct"),
                ("current", "current_weight", "pct"),
                ("delta", "delta_weight", "pct"),
                ("px", "current_price", "price"),
                ("stock_ret", "stock_return", "pct"),
                ("pnl_contrib", "portfolio_contribution", "pct"),
                ("score", "score", "float"),
                ("action", "action", "text"),
                ("status", "position_status", "text"),
            ],
        ),
        encoding="utf-8",
    )
    (output_dir / "rebalance.md").write_text(
        _markdown_table(
            "Rebalance",
            result.rebalance_rows,
            [
                ("symbol", "symbol", "text"),
                ("name", "name", "text"),
                ("action", "action", "text"),
                ("delta", "delta_weight", "pct"),
                ("now", "current_weight", "pct"),
                ("target", "target_weight", "pct"),
                ("px", "trade_price", "price"),
                ("stock_ret", "stock_return", "pct"),
                ("pnl_contrib", "portfolio_contribution", "pct"),
                ("status", "position_status", "text"),
            ],
        ),
        encoding="utf-8",
    )
    (output_dir / "decision_explanations.md").write_text(
        _markdown_table(
            "Decision Explanations",
            result.decision_rows,
            [
                ("symbol", "symbol", "text"),
                ("name", "name", "text"),
                ("action", "action", "text"),
                ("reason", "decision_reason", "text"),
                ("score", "score", "float"),
                ("score_rank", "abs_score_rank", "text"),
                ("target", "target_weight", "pct"),
                ("current", "current_weight", "pct"),
                ("delta", "delta_weight", "pct"),
                ("px", "trade_price", "price"),
                ("stock_ret", "stock_return", "pct"),
                ("pnl_contrib", "portfolio_contribution", "pct"),
                ("constraint", "constraint", "text"),
                ("status", "position_status", "text"),
                ("gate", "stock_market_gate", "float"),
                ("market_delta", "market_delta_norm", "float"),
            ],
        ),
        encoding="utf-8",
    )
    (output_dir / "model_explanation.json").write_text(
        json.dumps(result.summary.get("model_explanation", {}), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "decision_report.md").write_text(
        _decision_report_markdown(result.summary, result.decision_rows),
        encoding="utf-8",
    )


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
    market_notice: str | None = None,
    benchmark_window_days: int = 20,
    max_turnover_warning: float = 1.5,
    max_top_weight_warning: float = 0.1,
    max_gross_warning: float | None = None,
    data_timezone: str | None = None,
    display_timezone: str | None = DEFAULT_DISPLAY_TIMEZONE,
    daily_bar_time: str | None = None,
    write: bool = True,
    ensure_previous_signal: bool = True,
    previous_signal_backfill_limit: int = 8,
    progress_callback: ProgressCallback | None = None,
    progress_label: str | None = None,
    _panel_override: PanelData | None = None,
) -> LiveSignalResult:
    progress_total = 17
    progress_name = str(progress_label or f"live-signal:{market or 'default'}").strip()
    _emit_progress(progress_callback, label=progress_name, step=1, total=progress_total, message="load config")
    config = load_config(config_path)
    if device is not None:
        config.environment.device = str(device)
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = "1" if config.training.strict_no_fallback else "0"
    _configure_backtest_runtime_from_config(config)
    if getattr(config.training, "inference_backtest_autotune", None) is not None:
        os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = (
            "1" if bool(config.training.inference_backtest_autotune) else "0"
        )
    if getattr(config.training, "inference_backtest_compile", None) is not None:
        os.environ["STOCKAGENT_BACKTEST_COMPILE"] = (
            "1" if bool(config.training.inference_backtest_compile) else "0"
        )

    resolved_output_dir = Path(output_dir if output_dir is not None else config.runner.output_dir)
    resolved_fold_id, checkpoint = _resolve_checkpoint(resolved_output_dir, fold_id, checkpoint_path)
    _emit_progress(progress_callback, label=progress_name, step=2, total=progress_total, message=f"checkpoint fold={resolved_fold_id}")
    market_id = str(market or "").strip()
    market_name = str(market_label or market_id or "").strip()
    source_timezone = str(data_timezone or display_timezone or DEFAULT_DISPLAY_TIMEZONE)
    display_timezone_name = str(display_timezone or DEFAULT_DISPLAY_TIMEZONE)
    display_tz = _display_zone(display_timezone_name)
    generated_at_text = datetime.now(display_tz).isoformat(timespec="seconds")
    trading_frequency = str(getattr(config.trading, "frequency", "") or "")
    intraday_frequency = _is_intraday_frequency(trading_frequency)

    runtime_device = _resolve_device(config)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    non_blocking = bool(config.training.non_blocking_transfer and runtime_device.type == "cuda")

    checkpoint_payload = _load_checkpoint(checkpoint)
    _emit_progress(progress_callback, label=progress_name, step=3, total=progress_total, message="checkpoint loaded")
    state_dict = checkpoint_payload.get("model_state_dict")
    if not isinstance(state_dict, dict):
        raise ValueError(f"Checkpoint does not contain model_state_dict: {checkpoint}")

    panel = (
        _panel_override
        if _panel_override is not None
        else _build_panel(config, live_tail=True)
    )
    _emit_progress(progress_callback, label=progress_name, step=4, total=progress_total, message="panel ready")
    panel = _align_panel_to_state_dict_universe(
        panel,
        resolved_output_dir / f"fold_{resolved_fold_id:02d}",
        state_dict,
        context=f"live signal {market_id or resolved_fold_id}",
    )
    saved_fold_id = checkpoint_payload.get("fold_id")
    if saved_fold_id is not None and int(saved_fold_id) != int(resolved_fold_id):
        raise RuntimeError(
            f"checkpoint fold mismatch: path requests {resolved_fold_id}, payload contains {saved_fold_id}"
        )
    _validate_checkpoint_manifest(
        checkpoint_payload,
        _checkpoint_manifest(panel, config, include_data_content=False),
        checkpoint_path=checkpoint,
        scope="model",
    )
    _emit_progress(progress_callback, label=progress_name, step=5, total=progress_total, message="universe aligned")
    symbol_names = load_symbol_name_map(config.data.parquet_root)
    panel_idx, panel_fallback_notice = _resolve_usable_panel_index(panel, panel_date, config.training.lookback)
    panel_date_str = _date_string(panel.dates[panel_idx])
    panel_display_date = panel_date_str if intraday_frequency else _daily_bar_timestamp(panel_date_str, daily_bar_time)
    if panel_fallback_notice:
        market_notice = (
            f"{str(market_notice).strip()} {panel_fallback_notice}".strip()
            if market_notice
            else panel_fallback_notice
        )
    resolved_asof = asof_date or _now_text(source_timezone)
    _emit_progress(
        progress_callback,
        label=progress_name,
        step=6,
        total=progress_total,
        message=f"panel_date={panel_display_date}",
    )

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
    _emit_progress(
        progress_callback,
        label=progress_name,
        step=7,
        total=progress_total,
        message=f"prices source={price_snapshot.source} available={price_snapshot.available_count}/{panel.num_symbols}",
    )

    previous_price_source = str(price_snapshot.source or "").strip().lower()
    uses_realtime_daily_prices = (
        not intraday_frequency
        and previous_price_source
        and previous_price_source not in {"panel", "panel_close", "close"}
        and not previous_price_source.startswith("panel")
    )
    expected_previous_data_date = (
        panel_date_str if uses_realtime_daily_prices else _previous_usable_panel_date(panel, panel_idx, config.training.lookback)
    )
    fold_dir = checkpoint.parent
    _emit_progress(progress_callback, label=progress_name, step=8, total=progress_total, message="check previous signal")
    if (
        ensure_previous_signal
        and int(previous_signal_backfill_limit) > 0
        and write
        and not intraday_frequency
        and expected_previous_data_date
        and not _weights_history_has_date(fold_dir, expected_previous_data_date)
    ):
        previous_asof = _daily_bar_timestamp(expected_previous_data_date, daily_bar_time) or expected_previous_data_date
        previous_notice = (
            f"自動補生上一交易日 `{previous_asof}` 的 live signal，作為本次上個訊號/持倉基準。"
        )
        generate_live_signal(
            market=market_id,
            market_label=market_name,
            config_path=config_path,
            output_dir=resolved_output_dir,
            live_output_dir=live_output_dir,
            fold_id=resolved_fold_id,
            checkpoint_path=checkpoint,
            weights_path=None,
            panel_date=expected_previous_data_date,
            asof_date=previous_asof,
            price_source="panel",
            prices_csv=None,
            yahoo_chunk_size=yahoo_chunk_size,
            device=device,
            top_n=top_n,
            min_abs_delta=min_abs_delta,
            signal_id=None,
            market_notice=previous_notice,
            benchmark_window_days=benchmark_window_days,
            max_turnover_warning=max_turnover_warning,
            max_top_weight_warning=max_top_weight_warning,
            max_gross_warning=max_gross_warning,
            data_timezone=data_timezone,
            display_timezone=display_timezone,
            daily_bar_time=daily_bar_time,
            write=True,
            ensure_previous_signal=True,
            previous_signal_backfill_limit=max(0, int(previous_signal_backfill_limit) - 1),
            progress_callback=progress_callback,
            progress_label=f"{progress_name}:previous",
            _panel_override=panel,
        )

    previous_weights, previous_weights_date, previous_weights_path = _load_previous_weights(
        panel.symbols,
        output_dir=resolved_output_dir,
        fold_id=resolved_fold_id,
        weights_path=weights_path,
        asof_date=expected_previous_data_date or panel_date_str,
        prefer_live_weights=True,
        # expected_previous_data_date is already the resolved prior session for
        # panel-close signals (or today's panel date for realtime marks).
        strictly_before_asof=False,
    )
    previous_weights_data_date = previous_weights_date
    previous_weights_display_date = (
        previous_weights_date if intraday_frequency else _daily_bar_timestamp(previous_weights_date, daily_bar_time)
    )
    _emit_progress(
        progress_callback,
        label=progress_name,
        step=9,
        total=progress_total,
        message=f"previous_weights={previous_weights_display_date or previous_weights_date or 'none'}",
    )
    if expected_previous_data_date and previous_weights_data_date:
        expected_prev_dt = _datetime64_second(expected_previous_data_date)
        actual_prev_dt = _datetime64_second(previous_weights_data_date)
        if expected_prev_dt is not None and actual_prev_dt is not None and actual_prev_dt != expected_prev_dt:
            expected_display = (
                expected_previous_data_date
                if intraday_frequency
                else _daily_bar_timestamp(expected_previous_data_date, daily_bar_time)
            )
            gap_notice = (
                f"上一筆持倉 `{previous_weights_display_date}` 不是上一個可用交易日 `{expected_display}`；"
                "可能有缺少的 live weights，需要補推論。"
            )
            market_notice = f"{str(market_notice).strip()} {gap_notice}".strip() if market_notice else gap_notice
    drift_base_idx = _find_panel_date_index(panel, previous_weights_date)
    if drift_base_idx is None:
        drift_base_idx = panel_idx
    drift_base_date = _date_string(panel.dates[drift_base_idx])
    drift_base_display_date = drift_base_date if intraday_frequency else _daily_bar_timestamp(drift_base_date, daily_bar_time)
    drift_base_prices = np.asarray(panel.close_prices[drift_base_idx], dtype=np.float64)
    drift = estimate_drifted_weights(previous_weights, drift_base_prices, current_prices)
    _emit_progress(progress_callback, label=progress_name, step=10, total=progress_total, message="mark previous holdings")

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
        feature_names=panel.feature_names,
    ).to(runtime_device)
    _load_state_dict(model, state_dict)
    model.eval()
    _emit_progress(progress_callback, label=progress_name, step=11, total=progress_total, message="model ready")

    start = panel_idx - int(config.training.lookback) + 1
    x_np = np.nan_to_num(panel.features[start : panel_idx + 1], nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    mask_np = np.asarray(panel.tradable_mask[panel_idx], dtype=bool)
    can_buy_np = np.asarray(panel.can_buy_mask[panel_idx] if panel.can_buy_mask is not None else mask_np, dtype=bool)
    can_sell_np = np.asarray(panel.can_sell_mask[panel_idx] if panel.can_sell_mask is not None else mask_np, dtype=bool)
    can_short_open_np = np.asarray(
        panel.can_short_open_mask[panel_idx]
        if panel.can_short_open_mask is not None
        else can_sell_np,
        dtype=bool,
    )
    force_short_cover_np = np.asarray(
        panel.force_short_cover_mask[panel_idx]
        if panel.force_short_cover_mask is not None
        else np.zeros_like(mask_np, dtype=bool),
        dtype=bool,
    )
    force_exit_np = np.asarray(
        panel.force_exit_mask[panel_idx]
        if panel.force_exit_mask is not None
        else np.zeros_like(mask_np, dtype=bool),
        dtype=bool,
    )
    current_weights = np.asarray(drift.weights, dtype=np.float64).copy()

    with torch.inference_mode():
        x = torch.from_numpy(x_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking)
        mask = torch.from_numpy(mask_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking)
        with _autocast_context(runtime_device, amp_dtype):
            model_output = _call_model(model, x, mask, return_aux=True)
            model_weights_t, aux = _extract_weights_and_aux(model_output)
        _emit_progress(progress_callback, label=progress_name, step=12, total=progress_total, message="model inference done")
        zero_returns = torch.zeros_like(model_weights_t, dtype=torch.float32)
        initial = torch.from_numpy(current_weights.astype(np.float32)).to(device=runtime_device, non_blocking=non_blocking)
        backtest = run_backtest_torch(
            model_weights_t.float(),
            zero_returns,
            mask,
            torch.zeros((1,), device=runtime_device, dtype=torch.float32),
            buy_fee_rate=config.trading.buy_fee_rate,
            sell_fee_rate=config.trading.sell_fee_rate,
            long_only=config.trading.long_only,
            max_turnover_ratio=config.trading.max_turnover_ratio,
            gross_leverage=1.0,
            min_trade_weight=config.trading.min_trade_weight,
            portfolio_activation=config.trading.portfolio_activation,
            can_buy_mask=torch.from_numpy(can_buy_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking),
            can_sell_mask=torch.from_numpy(can_sell_np).unsqueeze(0).to(device=runtime_device, non_blocking=non_blocking),
            can_short_open_mask=torch.from_numpy(can_short_open_np).unsqueeze(0).to(
                device=runtime_device,
                non_blocking=non_blocking,
            ),
            force_short_cover_mask=torch.from_numpy(force_short_cover_np).unsqueeze(0).to(
                device=runtime_device,
                non_blocking=non_blocking,
            ),
            force_exit_mask=torch.from_numpy(force_exit_np).unsqueeze(0).to(
                device=runtime_device,
                non_blocking=non_blocking,
            ),
            return_weights_history=True,
            initial_weights=initial,
        )
        model_weights = model_weights_t[0].detach().float().cpu().numpy().astype(np.float64)
        target_weights = backtest.final_weights.detach().float().cpu().numpy().astype(np.float64)
        turnover = float(backtest.turnovers[0].detach().float().cpu().item())
        estimated_trade_cost = -float(backtest.strategy_returns[0].detach().float().cpu().item())
    _emit_progress(progress_callback, label=progress_name, step=13, total=progress_total, message="trading constraints applied")

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
        current_weights,
        target_weights,
        current_prices,
        drift_base_prices,
        symbol_names=symbol_names,
        min_abs_delta=min_abs_delta,
    )
    current_risk = portfolio_risk_summary(current_weights)
    target_risk = portfolio_risk_summary(target_weights)
    recent_performance = cumulative_recent_returns(checkpoint, window_days=benchmark_window_days)
    risk_warnings = _risk_warnings(
        turnover=turnover,
        target_risk=target_risk,
        max_turnover_warning=max_turnover_warning,
        max_top_weight_warning=max_top_weight_warning,
        max_gross_warning=max_gross_warning if max_gross_warning is not None else 1.05,
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
    _emit_progress(progress_callback, label=progress_name, step=14, total=progress_total, message="risk and explanations ready")

    weights_rows: list[dict[str, Any]] = []
    price_return = np.divide(
        current_prices,
        drift_base_prices,
        out=np.ones_like(current_prices, dtype=np.float64),
        where=np.isfinite(current_prices) & np.isfinite(drift_base_prices) & (drift_base_prices > 0.0),
    ) - 1.0
    for idx, symbol in enumerate(panel.symbols):
        delta_weight = float(target_weights[idx] - current_weights[idx])
        action = classify_rebalance_action(float(current_weights[idx]), float(target_weights[idx]), delta_weight=delta_weight)
        current_weight = float(current_weights[idx])
        raw_price_return = float(price_return[idx]) if np.isfinite(price_return[idx]) else None
        weights_rows.append(
            {
                "date": resolved_asof,
                "panel_date": panel_date_str,
                "symbol": str(symbol),
                "name": str(symbol_names.get(str(symbol), "")),
                "action": action,
                "score": _finite_float_or_none(score_values[idx]) if score_values is not None else None,
                "model_weight": float(model_weights[idx]),
                "current_weight": current_weight,
                "target_weight": float(target_weights[idx]),
                "delta_weight": delta_weight,
                "abs_delta_weight": abs(delta_weight),
                "base_price": float(drift_base_prices[idx]) if np.isfinite(drift_base_prices[idx]) else None,
                "panel_price": float(panel_prices[idx]) if np.isfinite(panel_prices[idx]) else None,
                "current_price": float(current_prices[idx]) if np.isfinite(current_prices[idx]) else None,
                "price_return": raw_price_return,
                "stock_return": _position_stock_return(current_weight, raw_price_return),
                "portfolio_contribution": _position_portfolio_contribution(current_weight, raw_price_return),
                "tradable": bool(mask_np[idx]),
                "can_buy": bool(can_buy_np[idx]),
                "can_sell": bool(can_sell_np[idx]),
                "position_status": _position_status(
                    tradable=bool(mask_np[idx]),
                    current_weight=current_weight,
                    target_weight=float(target_weights[idx]),
                    model_weight=float(model_weights[idx]),
                ),
            }
        )
    top_positions = _top_position_rows_from_weights(weights_rows, top_n)
    weight_meta_by_symbol = {str(row.get("symbol") or ""): row for row in weights_rows}
    for row in rebalance_rows:
        meta = weight_meta_by_symbol.get(str(row.get("symbol") or ""))
        if not meta:
            continue
        row["score"] = meta.get("score")
        row["model_weight"] = meta.get("model_weight")
        row["tradable"] = meta.get("tradable")
        row["can_buy"] = meta.get("can_buy")
        row["can_sell"] = meta.get("can_sell")
        row["position_status"] = meta.get("position_status")

    decision_rows = _build_decision_rows(
        symbols=panel.symbols,
        symbol_names=symbol_names,
        asof_date=resolved_asof,
        panel_date=panel_date_str,
        model_weights=model_weights,
        current_weights=current_weights,
        target_weights=target_weights,
        scores=score_values,
        current_prices=current_prices,
        base_prices=drift_base_prices,
        price_returns=price_return,
        tradable_mask=mask_np,
        can_buy_mask=can_buy_np,
        can_sell_mask=can_sell_np,
        aux=aux,
    )
    actionable_decisions = [row for row in decision_rows if str(row.get("action") or "") != "HOLD"]
    decision_action_counts = _action_counts(decision_rows)
    _emit_progress(progress_callback, label=progress_name, step=15, total=progress_total, message="rows formatted")

    resolved_signal_id = signal_id or _make_signal_id(market_id, resolved_asof)
    price_timestamp = _daily_price_timestamp(
        price_snapshot=price_snapshot,
        resolved_asof=resolved_asof,
        panel_display_date=panel_display_date,
        daily_bar_time=daily_bar_time,
        source_timezone=source_timezone,
        intraday_frequency=intraday_frequency,
    )
    weights_timestamp = _decision_weights_timestamp(
        panel_data_date=panel_date_str,
        resolved_asof=resolved_asof,
        uses_realtime_daily_prices=uses_realtime_daily_prices,
    )
    summary: dict[str, Any] = {
        "signal_id": resolved_signal_id,
        "generated_at": generated_at_text,
        "asof_date": resolved_asof,
        "market": market_id,
        "market_label": market_name,
        "panel_date": panel_display_date,
        "panel_data_date": panel_date_str,
        "weights_date": weights_timestamp,
        "trading_frequency": trading_frequency,
        "previous_period_label": "上個訊號到現在" if intraday_frequency else "上個交易日到現在",
        "previous_weights_policy": (
            "live_signal_before_asof"
            if previous_weights_path and Path(previous_weights_path).name.startswith("live_signal_weights")
            else "daily_weights_previous_trading_day"
        ),
        "data_timezone": source_timezone,
        "display_timezone": display_timezone_name,
        "display_timezone_label": display_timezone_label(display_timezone_name),
        "fold_id": int(resolved_fold_id),
        "checkpoint_path": str(checkpoint),
        "checkpoint_mtime": datetime.fromtimestamp(checkpoint.stat().st_mtime, tz=display_tz).isoformat(timespec="seconds"),
        "checkpoint_fingerprint": short_file_fingerprint(checkpoint),
        "config_path": str(config_path),
        "config_fingerprint": short_file_fingerprint(Path(config_path)),
        "previous_weights_date": previous_weights_display_date,
        "previous_weights_data_date": previous_weights_data_date,
        "previous_weights_path": previous_weights_path,
        "drift_base_date": drift_base_display_date,
        "drift_base_data_date": drift_base_date,
        "price_source": price_snapshot.source,
        "price_timestamp": price_timestamp,
        "price_data_date": price_timestamp,
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
        "market_notice": str(market_notice) if market_notice else None,
        "recent_performance": recent_performance,
        "model_explanation": {
            "source": "score logits, trading constraints, target weights, and weighted latest-feature proxy",
            "confidence_proxy_score_std": confidence_proxy,
            "top_score_drivers": score_drivers,
            "top_feature_drivers": feature_drivers,
            "decision_rows": int(len(decision_rows)),
            "actionable_decision_rows": int(len(actionable_decisions)),
            "action_counts": decision_action_counts,
            "aux_fields": sorted(str(key) for key in aux.keys()) if isinstance(aux, dict) else [],
        },
        "top_positions": top_positions,
        "rebalance": rebalance_rows[: max(0, int(top_n))],
        "decision_explanations": actionable_decisions[: max(0, int(top_n))],
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
        summary["decision_explanation_path"] = str(output_path / "decision_explanations.parquet")
        summary["positions_markdown_path"] = str(output_path / "target_positions.md")
        summary["rebalance_markdown_path"] = str(output_path / "rebalance.md")
        summary["decision_explanation_markdown_path"] = str(output_path / "decision_explanations.md")
        summary["decision_report_path"] = str(output_path / "decision_report.md")
        summary["model_explanation_path"] = str(output_path / "model_explanation.json")
        summary["discord_message_path"] = str(output_path / "discord_message.md")
    message = format_signal_message(summary, max_rows=top_n)
    _emit_progress(progress_callback, label=progress_name, step=16, total=progress_total, message="discord message ready")
    result = LiveSignalResult(
        summary=summary,
        weights_rows=weights_rows,
        rebalance_rows=rebalance_rows,
        decision_rows=decision_rows,
        message=message,
        output_dir=None,
    )
    if write:
        result.output_dir = _write_outputs_to_dir(result, summary["output_dir"])
        result.summary["output_dir"] = result.output_dir
        live_weights_path = write_live_weights_history(checkpoint.parent, result.summary, result.weights_rows)
        if live_weights_path:
            result.summary["live_weights_path"] = live_weights_path
        (Path(result.output_dir) / "summary.json").write_text(
            json.dumps(result.summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    _emit_progress(progress_callback, label=progress_name, step=17, total=progress_total, message="done")
    return result
