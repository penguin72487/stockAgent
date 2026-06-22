from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional fast metadata reader
    pq = None

from stockagent.config import load_config
from stockagent.live.market_config import LiveMarketConfig


FEATURE_SUFFIX = "_features.parquet"


@dataclass(slots=True)
class CheckpointInfo:
    fold_id: int | None
    path: Path
    mtime: str
    size_bytes: int
    fingerprint: str
    metrics_path: Path | None
    best_metric: str | None
    train_years: list[int]
    val_years: list[int]
    test_years: list[int]


@dataclass(slots=True)
class DataFreshness:
    parquet_root: Path | None
    last_data_date: str | None
    panel_date: str | None
    benchmark_date: str | None
    expected_latest_date: str | None
    fresh: bool
    reason: str | None
    scanned_files: int
    total_files: int
    sampled: bool


@dataclass(slots=True)
class MarketRuntimeStatus:
    cfg: LiveMarketConfig
    enabled: bool
    status: str
    checkpoint: CheckpointInfo | None
    data: DataFreshness
    market_open: bool
    market_open_reason: str | None
    config_fingerprint: str | None
    config_path: Path | None
    output_dir: Path | None


def resolve_repo_path(value: str | Path | None, *, root: Path | None = None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    base = root or Path.cwd()
    return base / path


def infer_market_type(cfg: LiveMarketConfig, parquet_root: str | Path | None = None) -> str:
    explicit = (cfg.market_type or "").strip().lower()
    if explicit:
        return explicit
    text = " ".join(str(part).lower() for part in (cfg.market, parquet_root or "", cfg.config_path))
    if "crypto" in text or "okx" in text or "bybit" in text:
        return "crypto"
    if "forex" in text or "fx" in text:
        return "forex"
    if "us" in text:
        return "us"
    if "tw" in text or "taiwan" in text:
        return "tw"
    return "generic"


def short_file_fingerprint(path: Path | None, *, chunk_bytes: int = 1024 * 1024) -> str | None:
    if path is None or not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    size = path.stat().st_size
    h.update(str(size).encode("ascii"))
    with path.open("rb") as handle:
        first = handle.read(chunk_bytes)
        h.update(first)
        if size > chunk_bytes:
            handle.seek(max(0, size - chunk_bytes))
            h.update(handle.read(chunk_bytes))
    return h.hexdigest()[:12]


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat(timespec="seconds")


def _latest_checkpoint(output_dir: Path | None) -> tuple[int | None, Path] | None:
    if output_dir is None or not output_dir.exists():
        return None
    candidates: list[tuple[int, Path]] = []
    for path in output_dir.glob("fold_*/checkpoint_best.pt"):
        try:
            fold = int(path.parent.name.removeprefix("fold_"))
        except ValueError:
            continue
        candidates.append((fold, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1]


def _read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _metric_line(metrics: dict[str, Any]) -> str | None:
    parts: list[str] = []
    if "best_val_loss" in metrics:
        try:
            parts.append(f"best_val_loss={float(metrics['best_val_loss']):.6g}")
        except Exception:
            pass
    for scope, key in (("val_metrics", "sharpe"), ("test_metrics", "sharpe"), ("test_metrics", "cumulative_return")):
        value = metrics.get(scope, {}).get(key) if isinstance(metrics.get(scope), dict) else None
        if value is None:
            continue
        label = f"{scope.removesuffix('_metrics')}_{key}"
        try:
            if key == "cumulative_return":
                parts.append(f"{label}={float(value):.2%}")
            else:
                parts.append(f"{label}={float(value):.3g}")
        except Exception:
            continue
    return " ".join(parts) if parts else None


def checkpoint_info(
    cfg: LiveMarketConfig,
    *,
    root: Path | None = None,
    output_dir: Path | None = None,
) -> CheckpointInfo | None:
    explicit = resolve_repo_path(cfg.checkpoint_path, root=root)
    fold_id: int | None = cfg.fold_id
    checkpoint: Path | None = None
    if explicit is not None:
        checkpoint = explicit if explicit.exists() else None
        if checkpoint is not None and fold_id is None:
            try:
                fold_id = int(checkpoint.parent.name.removeprefix("fold_"))
            except ValueError:
                fold_id = None
    elif fold_id is not None and output_dir is not None:
        candidate = output_dir / f"fold_{int(fold_id):02d}" / "checkpoint_best.pt"
        checkpoint = candidate if candidate.exists() else None
    else:
        latest = _latest_checkpoint(output_dir)
        if latest is not None:
            fold_id, checkpoint = latest
    if checkpoint is None:
        return None

    metrics_path = checkpoint.parent / "metrics.json"
    metrics_raw = _read_json(metrics_path)
    metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
    return CheckpointInfo(
        fold_id=fold_id,
        path=checkpoint,
        mtime=_mtime_iso(checkpoint),
        size_bytes=int(checkpoint.stat().st_size),
        fingerprint=short_file_fingerprint(checkpoint) or "",
        metrics_path=metrics_path if metrics_path.exists() else None,
        best_metric=_metric_line(metrics),
        train_years=[int(x) for x in metrics.get("train_years", []) if isinstance(x, int)],
        val_years=[int(x) for x in metrics.get("val_years", []) if isinstance(x, int)],
        test_years=[int(x) for x in metrics.get("test_years", []) if isinstance(x, int)],
    )


def _feature_path(root: Path, symbol: str | None) -> Path | None:
    if not symbol:
        return None
    direct = root / f"{symbol}{FEATURE_SUFFIX}"
    if direct.exists():
        return direct
    normalized = str(symbol).replace(".", "").replace("-", "").replace("_", "")
    for path in root.glob(f"*{FEATURE_SUFFIX}"):
        candidate = path.name.removesuffix(FEATURE_SUFFIX).replace(".", "").replace("-", "").replace("_", "")
        if candidate.upper() == normalized.upper():
            return path
    return None


def _date_to_text(value: Any, *, date_only: bool = True) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text or text.lower() in {"none", "nat"}:
        return None
    text = text.replace("T", " ")
    if date_only:
        return text[:10]
    if "." in text:
        text = text.split(".", 1)[0]
    return text[:19]


def _max_date_from_parquet(path: Path, *, date_only: bool = True) -> str | None:
    if pq is not None:
        try:
            meta = pq.ParquetFile(path).metadata
            date_idx: int | None = None
            schema = meta.schema
            for idx in range(schema.num_columns):
                if schema.column(idx).name == "date":
                    date_idx = idx
                    break
            if date_idx is not None:
                best: Any | None = None
                for row_group_idx in range(meta.num_row_groups):
                    stats = meta.row_group(row_group_idx).column(date_idx).statistics
                    if stats is None or stats.max is None:
                        continue
                    best = stats.max if best is None or str(stats.max) > str(best) else best
                parsed = _date_to_text(best, date_only=date_only)
                if parsed:
                    return parsed
        except Exception:
            pass
    try:
        import polars as pl

        value = pl.scan_parquet(path).select(pl.col("date").max().alias("max_date")).collect().item()
        return _date_to_text(value, date_only=date_only)
    except Exception:
        return None


def _parse_datetime_text(value: str, tz: ZoneInfo) -> datetime | None:
    text = str(value or "").replace("T", " ").strip()
    if not text:
        return None
    for fmt, size in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16), ("%Y-%m-%d", 10)):
        try:
            parsed = datetime.strptime(text[:size], fmt)
            return parsed.replace(tzinfo=tz)
        except Exception:
            continue
    return None


def _parse_hhmm(value: str | None, default: time) -> time:
    if not value:
        return default
    hour, minute = str(value).split(":", 1)
    return time(hour=int(hour), minute=int(minute[:2]))


def _nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    cur = date(year, month, 1)
    while cur.weekday() != weekday:
        cur += timedelta(days=1)
    return cur + timedelta(days=7 * (nth - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    cur = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    while cur.weekday() != weekday:
        cur -= timedelta(days=1)
    return cur


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _easter(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed(date(year, 6, 19)),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    return {day for day in holidays if day.year == year}


def is_trading_day(market_type: str, day: date, holidays: tuple[str, ...] = ()) -> bool:
    kind = market_type.lower()
    if kind == "crypto":
        return True
    if day.weekday() >= 5:
        return False
    if day.isoformat() in set(holidays):
        return False
    if kind == "us" and day in _us_market_holidays(day.year):
        return False
    return True


def expected_latest_data_date(
    cfg: LiveMarketConfig,
    *,
    market_type: str,
    now: datetime | None = None,
) -> str | None:
    kind = market_type.lower()
    if kind == "crypto":
        return None
    tz = ZoneInfo(cfg.timezone or "Asia/Taipei")
    local_now = now.astimezone(tz) if now is not None else datetime.now(tz)
    ready = _parse_hhmm(cfg.data_ready_time, time(hour=18, minute=0))
    candidate = local_now.date()
    if local_now.time() < ready:
        candidate -= timedelta(days=1)
    while not is_trading_day(kind, candidate, cfg.holidays):
        candidate -= timedelta(days=1)
    return candidate.isoformat()


def market_is_open(
    cfg: LiveMarketConfig,
    *,
    market_type: str,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    kind = market_type.lower()
    if kind == "crypto":
        return True, None
    tz = ZoneInfo(cfg.timezone or "Asia/Taipei")
    local_now = now.astimezone(tz) if now is not None else datetime.now(tz)
    if not is_trading_day(kind, local_now.date(), cfg.holidays):
        return False, f"{local_now.date().isoformat()} is not a trading day"
    if kind == "forex":
        return True, None
    open_t = _parse_hhmm(cfg.open_time, time(hour=9, minute=0))
    close_t = _parse_hhmm(cfg.close_time, time(hour=16, minute=0))
    if open_t <= local_now.time() <= close_t:
        return True, None
    return False, f"market closed at {local_now.strftime('%Y-%m-%d %H:%M %Z')}"


def data_freshness(
    cfg: LiveMarketConfig,
    *,
    root: Path | None = None,
    now: datetime | None = None,
) -> DataFreshness:
    config_path = resolve_repo_path(cfg.config_path, root=root)
    parquet_root: Path | None = None
    benchmark_name: str | None = None
    try:
        train_config = load_config(config_path or cfg.config_path)
        parquet_root = resolve_repo_path(train_config.data.parquet_root, root=root)
        benchmark_name = train_config.data.benchmark_name
    except Exception:
        return DataFreshness(None, None, None, None, None, False, "config load failed", 0, 0, False)

    if parquet_root is None or not parquet_root.exists():
        return DataFreshness(parquet_root, None, None, None, None, False, "parquet root missing", 0, 0, False)

    market_type = infer_market_type(cfg, parquet_root)
    crypto_intraday = market_type.lower() == "crypto"
    feature_files = sorted(parquet_root.glob(f"*{FEATURE_SUFFIX}"))
    total_files = len(feature_files)
    benchmark_path = _feature_path(parquet_root, benchmark_name)
    selected = list(feature_files)
    sampled = False
    limit = max(0, int(cfg.freshness_scan_limit))
    if limit > 0 and len(selected) > limit:
        sampled = True
        newest = sorted(selected, key=lambda path: path.stat().st_mtime, reverse=True)[:limit]
        selected = newest
        if benchmark_path is not None and benchmark_path not in selected:
            selected.append(benchmark_path)
    elif benchmark_path is not None and benchmark_path not in selected:
        selected.append(benchmark_path)

    last_data_date: str | None = None
    for path in selected:
        max_date = _max_date_from_parquet(path, date_only=not crypto_intraday)
        if max_date and (last_data_date is None or max_date > last_data_date):
            last_data_date = max_date
    benchmark_date = (
        _max_date_from_parquet(benchmark_path, date_only=not crypto_intraday)
        if benchmark_path is not None
        else None
    )
    panel_date = last_data_date
    expected = expected_latest_data_date(cfg, market_type=market_type, now=now)

    fresh = False
    reason: str | None = None
    if last_data_date is None:
        reason = "no dated parquet rows found"
    elif market_type == "crypto":
        tz = ZoneInfo(cfg.timezone or "UTC")
        local_now = now.astimezone(tz) if now is not None else datetime.now(tz)
        latest_dt = _parse_datetime_text(last_data_date, tz)
        if cfg.freshness_max_lag_minutes is not None and latest_dt is not None:
            age_minutes = max(0.0, (local_now - latest_dt).total_seconds() / 60.0)
            fresh = age_minutes <= max(0, int(cfg.freshness_max_lag_minutes))
            if not fresh:
                reason = f"latest data {last_data_date} is {age_minutes:.1f} minutes old"
        else:
            try:
                age_days = (local_now.date() - date.fromisoformat(last_data_date[:10])).days
            except Exception:
                age_days = 9999
            fresh = age_days <= max(0, int(cfg.freshness_max_lag_days))
            if not fresh:
                reason = f"latest data {last_data_date} is {age_days} days old"
    elif expected is not None:
        benchmark_fresh = benchmark_date is None or benchmark_date >= expected
        fresh = last_data_date >= expected and benchmark_fresh
        if not fresh:
            if last_data_date < expected:
                reason = f"latest data {last_data_date} older than expected {expected}"
            else:
                reason = f"benchmark data {benchmark_date} older than expected {expected}"
    else:
        fresh = True

    return DataFreshness(
        parquet_root=parquet_root,
        last_data_date=last_data_date,
        panel_date=panel_date,
        benchmark_date=benchmark_date,
        expected_latest_date=expected,
        fresh=fresh,
        reason=reason,
        scanned_files=len(selected),
        total_files=total_files,
        sampled=sampled,
    )


def runtime_status(
    cfg: LiveMarketConfig,
    *,
    root: Path | None = None,
    enabled_override: bool | None = None,
    now: datetime | None = None,
) -> MarketRuntimeStatus:
    config_path = resolve_repo_path(cfg.config_path, root=root)
    output_dir = resolve_repo_path(cfg.output_dir, root=root)
    freshness = data_freshness(cfg, root=root, now=now)
    market_type = infer_market_type(cfg, freshness.parquet_root)
    checkpoint = checkpoint_info(cfg, root=root, output_dir=output_dir)
    enabled = bool(cfg.enabled if enabled_override is None else enabled_override)
    is_open, open_reason = market_is_open(cfg, market_type=market_type, now=now)
    if not enabled:
        status = "disabled"
    elif checkpoint is None:
        status = "unsupported"
    elif not freshness.fresh:
        status = "stale"
    else:
        status = "ready"
    return MarketRuntimeStatus(
        cfg=cfg,
        enabled=enabled,
        status=status,
        checkpoint=checkpoint,
        data=freshness,
        market_open=is_open,
        market_open_reason=open_reason,
        config_fingerprint=short_file_fingerprint(config_path),
        config_path=config_path,
        output_dir=output_dir,
    )


def cumulative_recent_returns(path: Path | None, *, window_days: int) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    target: Path | None = None
    for name in ("daily_portfolio_returns.parquet", "daily_portfolio_returns.csv"):
        candidate = path.parent / name if path.name == "checkpoint_best.pt" else path / name
        if candidate.exists():
            target = candidate
            break
    if target is None:
        return None
    try:
        import polars as pl

        frame = pl.read_parquet(target) if target.suffix == ".parquet" else pl.read_csv(target)
        if frame.height == 0:
            return None
        frame = frame.sort("date").tail(max(1, int(window_days)))
        dates = frame.get_column("date").to_list()
        date_only = not any(":" in str(item) for item in dates)
        strategy = [float(x or 0.0) for x in frame.get_column("portfolio_return").to_list()]
        benchmark = [float(x or 0.0) for x in frame.get_column("benchmark_return").to_list()]
        strat_ret = math.exp(sum(strategy)) - 1.0
        bench_ret = math.exp(sum(benchmark)) - 1.0
        return {
            "window_days": int(frame.height),
            "start_date": _date_to_text(dates[0], date_only=date_only),
            "end_date": _date_to_text(dates[-1], date_only=date_only),
            "strategy_return": float(strat_ret),
            "benchmark_return": float(bench_ret),
            "excess_return": float(strat_ret - bench_ret),
            "source_path": str(target),
        }
    except Exception:
        return None
