from __future__ import annotations

import asyncio
import csv
import fcntl
import json
import math
import os
import re
import signal as signal_module
import subprocess
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ[key] = value


_load_env_file(Path(__file__).resolve().with_name(".env"))

try:
    import discord
    from discord import app_commands
    from discord.ext import tasks
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit("discord.py is required. Install with: pip install discord.py>=2.4") from exc

if (discord.version_info.major, discord.version_info.minor) < (2, 4):
    raise SystemExit(
        f"discord.py>=2.4 is required for User Install commands; found {discord.__version__}"
    )

from downloader.status import command_asset, command_option, first_download_failure
from stockagent.live.market_config import (
    LiveMarketConfig,
    load_market_configs,
    resolved_live_output_dir,
)
from stockagent.live.market_status import (
    MarketRuntimeStatus,
    is_trading_day,
    runtime_status,
    verified_tw_stock_session_day,
)
from stockagent.live.tw_day_trade_simulation import (
    EXIT_LIMIT_TIME,
    require_exact_session_eligibility,
    resolve_day_trade_rule_data_dir,
)
from stockagent.live.tw_day_trade_service_sync import (
    DISCORD_SERVICE_STATUS_FILENAME,
    load_service_sync,
    mode_from_service_sync,
)
from stockagent.live.model_deployment import (
    ModelDeployment,
    attempt_model_deployment,
    load_deployment,
)
from stockagent.config import load_config
from stockagent.live.capital import positive_float_or_none
from stockagent.live.quote_provider import (
    load_symbol_name_map,
    prepare_tw_price_limit_snapshot,
    tw_mis_opening_receipt_status,
    warm_tw_mis_quote_client,
)
from stockagent.live.portfolio_history import PortfolioHistoryResult, load_portfolio_history
from stockagent.live.report_formatter import (
    INVESTMENT_WARNING,
    MIN_DISPLAY_ABS_WEIGHT,
    format_signal_message,
    is_display_position_row,
)
from stockagent.live.signal_engine import (
    LiveSignalResult,
    clear_live_panel_memory_cache,
    generate_live_signal,
    write_live_weights_history,
)
from stockagent.live.service_notify import notify_systemd
from stockagent.live.stock_history import StockHistoryResult, load_stock_history
from stockagent.live.time_display import DEFAULT_DISPLAY_TIMEZONE, display_timezone_label, format_display_time


MIN_DISCORD_ROWS = 10
STATE_PATH = ROOT / "artifacts" / "discord_bot" / "state.json"
ERROR_LOG_PATH = ROOT / "artifacts" / "discord_bot" / "errors.log"
AUDIT_LOG_PATH = ROOT / "artifacts" / "discord_bot" / "audit_events.jsonl"
ARTIFACT_BACKFILL_STATUS_PATH = (
    ROOT / "artifacts" / "discord_bot" / "artifact_backfill_status.json"
)
PYTHON_EXECUTABLE_SENTINEL = "{python}"
_MODEL_INFERENCE_LOCK = threading.Lock()
_PRE_SIGNAL_SUCCESS_LOCK = threading.Lock()
_PRE_SIGNAL_RUN_LOCKS_LOCK = threading.Lock()
_PREWARM_RUN_LOCKS_LOCK = threading.Lock()
_PREOPEN_READINESS_LOCK = threading.Lock()
_SERVICE_STATUS_LOCK = threading.Lock()
_ARTIFACT_BACKFILL_STATUS_LOCK = threading.Lock()
_ERROR_LOG_LOCK = threading.Lock()
_PRE_SIGNAL_SUCCESS_AT: dict[tuple[str, ...], float] = {}
_PRE_SIGNAL_FAILURE_AT: dict[tuple[str, ...], tuple[float, str]] = {}
_PRE_SIGNAL_RUN_LOCKS: dict[tuple[str, ...], threading.Lock] = {}
_PREWARM_RUN_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_PREWARM_RESULTS: dict[tuple[str, str], LiveSignalResult] = {}
_BOT_RUN_STARTED_AT = datetime.now().astimezone().isoformat(timespec="seconds")
_BOT_RUN_ID = f"{os.getpid()}-{time.time_ns()}"


def _day_trade_state_dir() -> Path:
    configured = _env(
        "TW_DAY_TRADE_STATE_DIR",
        "artifacts/live/tw_day_trade_simulation",
    )
    return _resolve_repo_path(configured) or Path(str(configured))


def _discord_service_status_path() -> Path:
    return ROOT / "artifacts" / "discord_bot" / DISCORD_SERVICE_STATUS_FILENAME


class BotUserError(RuntimeError):
    code = "bot_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        if code is not None:
            self.code = code
        super().__init__(message)


class MarketUnsupportedError(BotUserError):
    code = "model_unsupported"

    def __init__(self, cfg: LiveMarketConfig) -> None:
        self.cfg = cfg
        super().__init__(_unsupported_message(cfg))


class MarketDisabledError(BotUserError):
    code = "market_disabled"

    def __init__(self, cfg: LiveMarketConfig) -> None:
        super().__init__(f"**{cfg.label}** 目前已停用。")


class DataStaleError(BotUserError):
    code = "data_stale"

    def __init__(self, cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> None:
        detail = status.data.reason or "data freshness check failed"
        super().__init__(
            "資料過期，目前不建議使用。\n"
            f"**{cfg.label}** latest=`{_display_cfg_time(cfg, status.data.last_data_date or 'n/a')}` "
            f"expected=`{_display_cfg_time(cfg, status.data.expected_latest_date or 'n/a')}` "
            f"display_tz=`{_display_tz_text(cfg)}` reason=`{detail}`"
        )


class MarketClosedError(BotUserError):
    code = "market_closed"

    def __init__(self, cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> None:
        super().__init__(f"**{cfg.label}** 目前休市或非交易時間：{status.market_open_reason or 'closed'}")


class PermissionDeniedError(BotUserError):
    code = "permission_denied"

    def __init__(self) -> None:
        super().__init__("權限不足：此指令需要 admin 或 trader role。")


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _env_int(name: str, default: int | None = None) -> int | None:
    raw = _env(name)
    if raw is None:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    return float(raw)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _public_broadcasts_enabled() -> bool:
    return _env_bool("STOCKAGENT_PUBLIC_BROADCASTS", False)


def _state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"markets": {}, "users": {}}
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"markets": {}, "users": {}}
    if not isinstance(raw, dict):
        return {"markets": {}, "users": {}}
    raw.setdefault("markets", {})
    raw.setdefault("users", {})
    return raw


def _write_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _market_state(market: str) -> dict[str, Any]:
    state = _state()
    markets = state.setdefault("markets", {})
    entry = markets.setdefault(str(market), {})
    return entry if isinstance(entry, dict) else {}


def _set_market_state(market: str, **values: Any) -> None:
    state = _state()
    markets = state.setdefault("markets", {})
    entry = markets.setdefault(str(market), {})
    if not isinstance(entry, dict):
        entry = {}
        markets[str(market)] = entry
    entry.update(values)
    _write_state(state)
    _clear_runtime_status_cache()


def _normalize_watch_symbol(symbol: Any) -> str:
    text = str(symbol or "").strip().strip("`").upper()
    for suffix in (".TW", ".TWO"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def _user_state_key(user_id: Any) -> str:
    value = str(user_id or "").strip()
    return value if value else "anonymous"


def _user_watchlist(user_id: Any, market: str) -> list[str]:
    state = _state()
    users = state.setdefault("users", {})
    entry = users.get(_user_state_key(user_id))
    if not isinstance(entry, dict):
        return []
    watchlists = entry.get("watchlists")
    if not isinstance(watchlists, dict):
        return []
    items = watchlists.get(str(market), [])
    if not isinstance(items, list):
        return []
    seen: set[str] = set()
    symbols: list[str] = []
    for item in items:
        symbol = _normalize_watch_symbol(item)
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


def _set_user_watchlist(user_id: Any, market: str, symbols: list[str]) -> list[str]:
    state = _state()
    users = state.setdefault("users", {})
    user_key = _user_state_key(user_id)
    entry = users.setdefault(user_key, {})
    if not isinstance(entry, dict):
        entry = {}
        users[user_key] = entry
    watchlists = entry.setdefault("watchlists", {})
    if not isinstance(watchlists, dict):
        watchlists = {}
        entry["watchlists"] = watchlists
    seen: set[str] = set()
    normalized: list[str] = []
    for item in symbols:
        symbol = _normalize_watch_symbol(item)
        if symbol and symbol not in seen:
            normalized.append(symbol)
            seen.add(symbol)
    watchlists[str(market)] = normalized
    _write_state(state)
    return normalized


def _add_user_watch_symbol(user_id: Any, market: str, symbol: Any) -> list[str]:
    items = _user_watchlist(user_id, market)
    normalized = _normalize_watch_symbol(symbol)
    if normalized and normalized not in items:
        items.append(normalized)
    return _set_user_watchlist(user_id, market, items)


def _remove_user_watch_symbol(user_id: Any, market: str, symbol: Any) -> list[str]:
    normalized = _normalize_watch_symbol(symbol)
    items = [item for item in _user_watchlist(user_id, market) if item != normalized]
    return _set_user_watchlist(user_id, market, items)


def _replace_user_watch_symbol(user_id: Any, market: str, old_symbol: Any, new_symbol: Any) -> list[str]:
    old_normalized = _normalize_watch_symbol(old_symbol)
    new_normalized = _normalize_watch_symbol(new_symbol)
    if not old_normalized or not new_normalized:
        raise BotUserError("update 需要提供 symbol 舊代號與 new_symbol 新代號。")
    items = _user_watchlist(user_id, market)
    replaced = False
    out: list[str] = []
    for item in items:
        if item == old_normalized:
            if new_normalized not in out:
                out.append(new_normalized)
            replaced = True
        elif item not in out:
            out.append(item)
    if not replaced and new_normalized not in out:
        out.append(new_normalized)
    return _set_user_watchlist(user_id, market, out)


def _clear_user_watchlist(user_id: Any, market: str) -> list[str]:
    return _set_user_watchlist(user_id, market, [])


def _user_subscriptions(user_id: Any) -> dict[str, dict[str, Any]]:
    state = _state()
    users = state.setdefault("users", {})
    entry = users.get(_user_state_key(user_id))
    if not isinstance(entry, dict):
        return {}
    subscriptions = entry.get("subscriptions")
    if not isinstance(subscriptions, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for market, value in subscriptions.items():
        if isinstance(value, dict) and value.get("enabled", True):
            result[str(market)] = {
                "enabled": True,
                "watchlist_only": bool(value.get("watchlist_only", True)),
            }
    return result


def _set_user_subscription(user_id: Any, market: str, *, watchlist_only: bool = True) -> dict[str, dict[str, Any]]:
    state = _state()
    users = state.setdefault("users", {})
    user_key = _user_state_key(user_id)
    entry = users.setdefault(user_key, {})
    if not isinstance(entry, dict):
        entry = {}
        users[user_key] = entry
    subscriptions = entry.setdefault("subscriptions", {})
    if not isinstance(subscriptions, dict):
        subscriptions = {}
        entry["subscriptions"] = subscriptions
    subscriptions[str(market)] = {"enabled": True, "watchlist_only": bool(watchlist_only)}
    _write_state(state)
    return _user_subscriptions(user_id)


def _remove_user_subscription(user_id: Any, market: str) -> dict[str, dict[str, Any]]:
    state = _state()
    users = state.setdefault("users", {})
    user_key = _user_state_key(user_id)
    entry = users.get(user_key)
    if isinstance(entry, dict):
        subscriptions = entry.get("subscriptions")
        if isinstance(subscriptions, dict):
            subscriptions.pop(str(market), None)
            _write_state(state)
    return _user_subscriptions(user_id)


def _clear_user_subscriptions(user_id: Any) -> dict[str, dict[str, Any]]:
    state = _state()
    users = state.setdefault("users", {})
    user_key = _user_state_key(user_id)
    entry = users.get(user_key)
    if isinstance(entry, dict):
        entry["subscriptions"] = {}
        _write_state(state)
    return {}


def _subscribed_users_for_market(market: str) -> list[tuple[str, dict[str, Any]]]:
    state = _state()
    users = state.setdefault("users", {})
    result: list[tuple[str, dict[str, Any]]] = []
    if not isinstance(users, dict):
        return result
    for user_id, entry in users.items():
        if not isinstance(entry, dict):
            continue
        subscriptions = entry.get("subscriptions")
        if not isinstance(subscriptions, dict):
            continue
        subscription = subscriptions.get(str(market))
        if isinstance(subscription, dict) and subscription.get("enabled", True):
            result.append(
                (
                    str(user_id),
                    {
                        "enabled": True,
                        "watchlist_only": bool(subscription.get("watchlist_only", True)),
                    },
                )
            )
    return result


def _market_enabled(cfg: LiveMarketConfig) -> bool:
    entry = _market_state(cfg.market)
    if "enabled" in entry:
        return bool(entry["enabled"])
    return bool(cfg.enabled)


def _market_schedule_time(cfg: LiveMarketConfig) -> str:
    entry = _market_state(cfg.market)
    value = entry.get("schedule_time") or cfg.schedule_time or bot.signal_time
    return str(value)


def _market_schedule_interval_minutes(cfg: LiveMarketConfig) -> int | None:
    entry = _market_state(cfg.market)
    value = entry.get("schedule_interval_minutes") or cfg.schedule_interval_minutes
    try:
        number = int(value)
    except Exception:
        return None
    return number if number > 0 else None


def _market_schedule_delay_seconds(cfg: LiveMarketConfig) -> int:
    entry = _market_state(cfg.market)
    value = entry.get("schedule_delay_seconds") if "schedule_delay_seconds" in entry else cfg.schedule_delay_seconds
    try:
        return max(0, int(value))
    except Exception:
        return 0


def _market_summary_time(cfg: LiveMarketConfig) -> str | None:
    entry = _market_state(cfg.market)
    value = entry.get("summary_time") or cfg.summary_time
    return str(value) if value else None


def _market_artifact_backfill_time(cfg: LiveMarketConfig) -> str | None:
    entry = _market_state(cfg.market)
    value = (
        entry.get("artifact_backfill_time")
        or entry.get("backfill_time")
        or cfg.data_ready_time
        or cfg.close_time
        or cfg.summary_time
        or cfg.schedule_time
    )
    return str(value) if value else None


def _market_initial_capital(cfg: LiveMarketConfig) -> float | None:
    entry = _market_state(cfg.market)
    return positive_float_or_none(entry.get("initial_capital")) or positive_float_or_none(getattr(cfg, "initial_capital", None))


def _market_current_capital(cfg: LiveMarketConfig) -> float | None:
    entry = _market_state(cfg.market)
    return positive_float_or_none(entry.get("current_capital")) or positive_float_or_none(getattr(cfg, "current_capital", None))


def _validate_hhmm(value: str) -> str:
    text = str(value).strip()
    parts = text.split(":", 1)
    if len(parts) != 2:
        raise ValueError("time must be HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("time must be HH:MM")
    return f"{hour:02d}:{minute:02d}"


def _error_log_limits() -> tuple[int, int]:
    max_bytes = max(
        1024 * 1024,
        _env_int("STOCKAGENT_DISCORD_ERROR_LOG_MAX_BYTES", 4 * 1024 * 1024)
        or 4 * 1024 * 1024,
    )
    generations = max(
        1,
        _env_int("STOCKAGENT_DISCORD_ERROR_LOG_GENERATIONS", 3) or 3,
    )
    return max_bytes, generations


def _rotate_error_log_if_needed() -> bool:
    """Bound the current log and leave a fresh path for hot-artifact sync."""

    max_bytes, generations = _error_log_limits()
    with _ERROR_LOG_LOCK:
        ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not ERROR_LOG_PATH.is_file() or ERROR_LOG_PATH.stat().st_size < max_bytes:
            return False
        oldest = ERROR_LOG_PATH.with_name(f"{ERROR_LOG_PATH.name}.{generations}")
        oldest.unlink(missing_ok=True)
        for generation in range(generations - 1, 0, -1):
            source = ERROR_LOG_PATH.with_name(f"{ERROR_LOG_PATH.name}.{generation}")
            if source.exists():
                os.replace(
                    source,
                    ERROR_LOG_PATH.with_name(
                        f"{ERROR_LOG_PATH.name}.{generation + 1}"
                    ),
                )
        os.replace(
            ERROR_LOG_PATH,
            ERROR_LOG_PATH.with_name(f"{ERROR_LOG_PATH.name}.1"),
        )
        # A fresh local path makes the local-wins artifact bridge replace its
        # stale hard link instead of restoring the just-rotated transport copy.
        ERROR_LOG_PATH.touch()
    return True


def _log_exception(context: str, exc: Exception) -> None:
    _rotate_error_log_if_needed()
    payload = [
        f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {context}",
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        "",
    ]
    with _ERROR_LOG_LOCK:
        ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ERROR_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(payload))


def _record_audit_event(signal_id: str, action: str, interaction: discord.Interaction, **extra: Any) -> None:
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "signal_id": signal_id,
        "action": action,
        "user_id": getattr(interaction.user, "id", None),
        "user": str(interaction.user),
        **extra,
    }
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def _markets_dir() -> Path:
    raw = _env("STOCKAGENT_MARKETS_DIR", "services/discord_bot/markets")
    path = Path(raw or "services/discord_bot/markets")
    return path if path.is_absolute() else ROOT / path


def _market_configs() -> dict[str, LiveMarketConfig]:
    configs = _market_configs_cached(str(_markets_dir()))
    if configs:
        return configs

    fold_raw = _env("STOCKAGENT_FOLD_ID")
    fallback = LiveMarketConfig(
        market=_env("STOCKAGENT_DEFAULT_MARKET", "default") or "default",
        label=_env("STOCKAGENT_MARKET_LABEL", _env("STOCKAGENT_DEFAULT_MARKET", "default") or "default") or "default",
        config_path=_env("STOCKAGENT_CONFIG", "configs/markets/tw.yaml") or "configs/markets/tw.yaml",
        output_dir=_env("STOCKAGENT_OUTPUT_DIR"),
        live_output_dir=_env("STOCKAGENT_LIVE_OUTPUT_DIR"),
        fold_id=int(fold_raw) if fold_raw else None,
        checkpoint_path=_env("STOCKAGENT_CHECKPOINT"),
        weights_path=_env("STOCKAGENT_WEIGHTS_PATH"),
        panel_date=_env("STOCKAGENT_PANEL_DATE", "latest") or "latest",
        price_source=_env("STOCKAGENT_PRICE_SOURCE", "panel") or "panel",
        prices_csv=_env("STOCKAGENT_PRICES_CSV"),
        device=_env("STOCKAGENT_DEVICE"),
        top_n=_env_int("STOCKAGENT_TOP_N", 20) or 20,
        min_abs_delta=_env_float("STOCKAGENT_MIN_ABS_DELTA", 0.001),
    )
    return {fallback.market: fallback}


@lru_cache(maxsize=8)
def _market_configs_cached(markets_dir: str) -> dict[str, LiveMarketConfig]:
    return load_market_configs(Path(markets_dir))


def _default_market() -> str:
    configured = _env("STOCKAGENT_DEFAULT_MARKET")
    configs = _market_configs()
    if configured and configured in configs:
        return configured
    if "tw" in configs:
        return "tw"
    return next(iter(configs))


def _resolve_market(market: str | None) -> LiveMarketConfig:
    configs = _market_configs()
    key = str(market or "").strip() or _default_market()
    if key not in configs:
        raise ValueError(f"unknown market={key!r}; available={', '.join(sorted(configs))}")
    return configs[key]


def _resolve_repo_path(value: str | None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _latest_checkpoint(output_dir: str | None) -> Path | None:
    root = _resolve_repo_path(output_dir)
    if root is None or not root.exists():
        return None
    candidates: list[tuple[int, Path]] = []
    for path in root.glob("fold_*/checkpoint_best.pt"):
        try:
            fold_id = int(path.parent.name.removeprefix("fold_"))
        except ValueError:
            continue
        candidates.append((fold_id, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[-1][1]


def _effective_market_config(cfg: LiveMarketConfig) -> LiveMarketConfig:
    effective = cfg
    if getattr(cfg, "model_auto_deploy", False):
        manifest_path = getattr(cfg, "model_deployment_manifest", None)
        deployment = load_deployment(manifest_path, root=ROOT) if manifest_path else None
        if deployment is not None:
            effective = replace(
                cfg,
                config_path=deployment.config_path,
                output_dir=deployment.output_dir,
                fold_id=deployment.fold_id,
                checkpoint_path=deployment.checkpoint_path,
                weights_path=deployment.weights_path,
            )
    if not getattr(effective, "model_scoped_live_output", False):
        return effective
    return replace(effective, live_output_dir=str(resolved_live_output_dir(effective)))


def _market_model_checkpoint(cfg: LiveMarketConfig) -> Path | None:
    cfg = _effective_market_config(cfg)
    explicit = _resolve_repo_path(getattr(cfg, "checkpoint_path", None))
    if explicit is not None:
        return explicit if explicit.exists() else None
    fold_id = getattr(cfg, "fold_id", None)
    output_dir = getattr(cfg, "output_dir", None)
    if fold_id is not None and output_dir:
        path = _resolve_repo_path(output_dir)
        if path is None:
            return None
        checkpoint = path / f"fold_{int(fold_id):02d}" / "checkpoint_best.pt"
        return checkpoint if checkpoint.exists() else None
    return _latest_checkpoint(output_dir)


def _market_fold_dir(cfg: LiveMarketConfig) -> Path:
    checkpoint = _market_model_checkpoint(cfg)
    if checkpoint is None:
        raise MarketUnsupportedError(cfg)
    return checkpoint.parent


def _market_has_model(cfg: LiveMarketConfig) -> bool:
    return _market_model_checkpoint(cfg) is not None


def _unsupported_message(cfg: LiveMarketConfig) -> str:
    message = getattr(cfg, "unsupported_message", None)
    if message:
        return str(message)
    label = getattr(cfg, "label", None) or getattr(cfg, "market", "market")
    return f"**{label}** 目前不支援：尚未上線可用模型。之後模型上線後就會支援。"


def _runtime_status(cfg: LiveMarketConfig) -> MarketRuntimeStatus:
    effective = _effective_market_config(cfg)
    return runtime_status(effective, root=ROOT, enabled_override=_market_enabled(cfg))


_RUNTIME_STATUS_CACHE: dict[str, tuple[float, MarketRuntimeStatus]] = {}


def _clear_runtime_status_cache() -> None:
    _RUNTIME_STATUS_CACHE.clear()


def _runtime_status_for_display(cfg: LiveMarketConfig) -> MarketRuntimeStatus:
    ttl = _env_float("STOCKAGENT_STATUS_CACHE_SECONDS", 15.0)
    if ttl <= 0:
        return _runtime_status(cfg)
    key = str(cfg.market)
    now = time.monotonic()
    cached = _RUNTIME_STATUS_CACHE.get(key)
    if cached is not None and now - cached[0] <= ttl:
        return cached[1]
    status = _runtime_status(cfg)
    _RUNTIME_STATUS_CACHE[key] = (now, status)
    return status


def _ensure_signal_ready(cfg: LiveMarketConfig, *, scheduled: bool = False) -> MarketRuntimeStatus:
    del scheduled
    status = _runtime_status(cfg)
    if not status.enabled:
        raise MarketDisabledError(cfg)
    if status.checkpoint is None:
        raise MarketUnsupportedError(cfg)
    return status


def _ensure_signal_ready_cached(cfg: LiveMarketConfig) -> MarketRuntimeStatus:
    status = _runtime_status_for_display(cfg)
    if not status.enabled:
        raise MarketDisabledError(cfg)
    if status.checkpoint is None:
        raise MarketUnsupportedError(cfg)
    return status


def _data_freshness_notice(status: MarketRuntimeStatus) -> str | None:
    if status.data.fresh:
        return None
    latest = _display_cfg_time(status.cfg, status.data.last_data_date or status.data.panel_date or "n/a")
    expected = _display_cfg_time(status.cfg, status.data.expected_latest_date or "n/a")
    reason = status.data.reason or "data freshness check failed"
    return (
        f"資料提醒：latest=`{latest}` expected=`{expected}` reason=`{reason}`；"
        "仍會產生訊號，請確認資料來源後再交易。"
    )


def _require_fresh_data_for_artifact_generation(cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> None:
    if status.data.fresh:
        return
    latest = _display_cfg_time(status.cfg, status.data.last_data_date or status.data.panel_date or "n/a")
    expected = _display_cfg_time(status.cfg, status.data.expected_latest_date or "n/a")
    reason = status.data.reason or "data freshness check failed"
    raise BotUserError(
        f"`{cfg.market}` data is stale after update; latest=`{latest}` expected=`{expected}` "
        f"reason=`{reason}`. 已停止生成訊號，避免用舊 panel。"
    )


def _market_notice(status: MarketRuntimeStatus) -> str | None:
    freshness_notice = _data_freshness_notice(status)
    if status.market_open:
        return freshness_notice
    data_date = _display_cfg_time(status.cfg, status.data.panel_date or status.data.last_data_date or "n/a")
    reason = status.market_open_reason or "market closed"
    if "not a trading day" in reason:
        notice = f"今天沒有開盤，使用最後可用資料 `{data_date}`（{_display_tz_text(status.cfg)}）產生訊號。"
    else:
        notice = f"目前非交易時間，使用最後可用資料 `{data_date}`（{_display_tz_text(status.cfg)}）產生訊號。"
    if freshness_notice:
        return f"{notice} {freshness_notice}"
    return notice


def _role_names_and_ids(interaction: discord.Interaction) -> tuple[set[str], set[int]]:
    roles = getattr(interaction.user, "roles", None) or []
    names = {str(getattr(role, "name", "")).strip().lower() for role in roles if str(getattr(role, "name", "")).strip()}
    ids = {int(getattr(role, "id")) for role in roles if getattr(role, "id", None) is not None}
    return names, ids


def _has_trader_permission(interaction: discord.Interaction, cfg: LiveMarketConfig | None = None) -> bool:
    permissions = getattr(interaction.user, "guild_permissions", None)
    if bool(getattr(permissions, "administrator", False)):
        return True
    names, ids = _role_names_and_ids(interaction)
    configured_names = {
        item.strip().lower()
        for item in (_env("STOCKAGENT_TRADER_ROLE_NAMES", "") or "").split(",")
        if item.strip()
    }
    configured_ids = {
        int(item)
        for item in (_env("STOCKAGENT_TRADER_ROLE_IDS", "") or "").split(",")
        if item.strip().isdigit()
    }
    if cfg is not None:
        configured_names |= {item.strip().lower() for item in cfg.trader_role_names if item.strip()}
        configured_ids |= {int(item) for item in cfg.trader_role_ids}
    configured_names |= {"trader", "traders", "交易員"}
    return bool(names & configured_names or ids & configured_ids)


def _require_trader_permission(interaction: discord.Interaction, cfg: LiveMarketConfig | None = None) -> None:
    if not _has_trader_permission(interaction, cfg):
        raise PermissionDeniedError()


def _scheduled_markets() -> list[str]:
    configs = _market_configs()
    raw = _env("STOCKAGENT_SCHEDULED_MARKETS")
    if raw:
        items = [item.strip() for item in raw.split(",") if item.strip()]
        if any(item.lower() in {"all", "*"} for item in items):
            items = sorted(configs)
        return [
            key
            for key in items
            if key in configs and _market_enabled(configs[key])
        ]
    return sorted(key for key, cfg in configs.items() if _market_enabled(cfg))


@lru_cache(maxsize=32)
def _scheduled_calendar_root(
    rule_data_dir: str,
    config_path: str,
) -> Path | None:
    if rule_data_dir:
        return _resolve_repo_path(rule_data_dir)
    if not config_path:
        return None
    try:
        experiment = load_config(_resolve_repo_path(config_path) or config_path)
    except Exception:
        return None
    return _resolve_repo_path(experiment.data.parquet_root)


def _scheduled_market_session_day(
    cfg: LiveMarketConfig,
    now: datetime,
) -> tuple[bool, str]:
    """Resolve the scheduled market session before any data/API work."""

    market = str(getattr(cfg, "market", "") or "").lower()
    kind = str(getattr(cfg, "market_type", "") or "").lower()
    if not kind:
        kind = (
            "crypto"
            if "crypto" in market
            else "forex"
            if "forex" in market or market == "fx"
            else "us"
            if market.startswith("us")
            else "tw"
            if market.startswith("tw")
            else "generic"
        )
    holidays = tuple(getattr(cfg, "holidays", ()) or ())
    if kind in {"tw", "taiwan"}:
        calendar_root = _scheduled_calendar_root(
            str(getattr(cfg, "day_trade_rule_data_dir", "") or ""),
            str(getattr(cfg, "config_path", "") or ""),
        )
        return verified_tw_stock_session_day(
            now.date(),
            holidays,
            parquet_root=calendar_root,
        )
    is_open = is_trading_day(kind, now.date(), holidays)
    return (
        is_open,
        "calendar session"
        if is_open
        else f"{now.date().isoformat()} is not a {kind} trading day",
    )


def _scheduled_signal_key(cfg: LiveMarketConfig, now: datetime) -> str | None:
    session_open, _session_reason = _scheduled_market_session_day(cfg, now)
    if not session_open:
        return None
    interval = _market_schedule_interval_minutes(cfg)
    if interval is not None:
        ready_time = now - timedelta(seconds=_market_schedule_delay_seconds(cfg))
        total_minutes = ready_time.hour * 60 + ready_time.minute
        bucket_minutes = (total_minutes // interval) * interval
        bucket = ready_time.replace(
            hour=bucket_minutes // 60,
            minute=bucket_minutes % 60,
            second=0,
            microsecond=0,
        )
        return f"{bucket.isoformat(timespec='minutes')}:{cfg.market}"
    schedule_time = _market_schedule_time(cfg)
    if now.strftime("%H:%M") == schedule_time:
        return f"{now.strftime('%Y-%m-%d')}:{cfg.market}"
    if not bool(getattr(cfg, "day_trade_simulation_enabled", False)):
        return None
    schedule_minutes = _hhmm_minutes(schedule_time)
    now_minutes = now.hour * 60 + now.minute
    exit_limit_minutes = EXIT_LIMIT_TIME.hour * 60 + EXIT_LIMIT_TIME.minute
    if (
        schedule_minutes is None
        or now_minutes < schedule_minutes
        or now_minutes >= exit_limit_minutes
    ):
        return None
    # A machine or bot restart after the configured minute must not silently
    # omit a paper strategy for the whole session.  The daily key deduplicates
    # within one process; scheduled_signal also checks durable artifacts so a
    # second restart does not generate the same session again.
    return f"{now.strftime('%Y-%m-%d')}:{cfg.market}"


def _scheduled_signal_requires_preopen_catch_up(
    cfg: LiveMarketConfig, now: datetime
) -> bool:
    return (
        bool(getattr(cfg, "day_trade_simulation_enabled", False))
        and now.strftime("%H:%M") != _market_schedule_time(cfg)
        and not _preopen_market_ready_for_session(cfg, now.date().isoformat())
    )


def _preopen_readiness_path() -> Path:
    configured = _env(
        "STOCKAGENT_PREOPEN_READINESS_PATH",
        "artifacts/discord_bot/preopen_readiness.json",
    )
    return _resolve_repo_path(configured) or Path(str(configured))


def _preopen_market_ready_for_session(
    cfg: LiveMarketConfig, session_date: str
) -> bool:
    try:
        payload = json.loads(
            _preopen_readiness_path().read_text(encoding="utf-8")
        )
    except (OSError, ValueError, TypeError):
        return False
    row = (payload.get("markets") or {}).get(str(cfg.market))
    if not isinstance(row, dict) or row.get("status") != "ready":
        return False
    if not _summary_date_matches(row.get("completed_at"), session_date):
        return False
    limits = row.get("preopen_price_limits")
    eligibility = row.get("same_session_eligibility")
    venues = eligibility.get("venues") if isinstance(eligibility, dict) else None
    return bool(
        row.get("panel_date")
        and row.get("checkpoint_fingerprint")
        and int(row.get("symbol_count") or 0) > 0
        and isinstance(limits, dict)
        and str(limits.get("trading_date") or "") == session_date
        and int(limits.get("prepared_count") or 0) > 0
        and isinstance(eligibility, dict)
        and str(eligibility.get("target_date") or "") == session_date
        and isinstance(venues, dict)
        and venues
        and all(
            isinstance(venue, dict) and bool(venue.get("covered"))
            for venue in venues.values()
        )
    )


def _preopen_market_final_armed_for_session(
    cfg: LiveMarketConfig, session_date: str
) -> bool:
    try:
        payload = json.loads(_preopen_readiness_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    row = (payload.get("markets") or {}).get(str(cfg.market))
    final_arm = row.get("final_arm") if isinstance(row, dict) else None
    if not isinstance(final_arm, dict):
        return False
    latency = final_arm.get("live_latency")
    latency = latency if isinstance(latency, dict) else {}
    opening_prewarm = final_arm.get("opening_source_prewarm")
    opening_prewarm = (
        opening_prewarm if isinstance(opening_prewarm, dict) else {}
    )
    return bool(
        final_arm.get("status") == "ready"
        and final_arm.get("run_id") == _BOT_RUN_ID
        and _summary_date_matches(final_arm.get("completed_at"), session_date)
        and latency.get("panel_cache_hit") is True
        and latency.get("checkpoint_cache_hit") is True
        and latency.get("model_cache_hit") is True
        and opening_prewarm.get("ready") is True
        and opening_prewarm.get("run_id") == _BOT_RUN_ID
        and opening_prewarm.get("source") == "twse_tpex:mis"
    )


def _preopen_market_symbol_count(cfg: LiveMarketConfig) -> int:
    try:
        payload = json.loads(_preopen_readiness_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return 0
    row = (payload.get("markets") or {}).get(str(cfg.market))
    return int(row.get("symbol_count") or 0) if isinstance(row, dict) else 0


def _day_trade_opening_quote_symbols(rows: list[dict[str, Any]]) -> list[str]:
    """Return the exact active universe whose opening prices can affect orders."""

    has_alive_contract = any("alive" in row for row in rows)
    selected: list[str] = []
    for row in rows:
        if has_alive_contract and row.get("alive") is not True:
            continue
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            selected.append(symbol)
    return list(dict.fromkeys(selected))


def _day_trade_opening_fallback_prices(
    rows: list[dict[str, Any]], symbols: list[str]
) -> np.ndarray:
    """Align fallback prices to the exact quote universe, never all weight rows."""

    prices_by_symbol: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol or symbol in prices_by_symbol:
            continue
        price = _float_or_none(row.get("current_price"))
        if price is not None and price > 0.0:
            prices_by_symbol[symbol] = float(price)
    return np.asarray(
        [prices_by_symbol.get(str(symbol), 1.0) for symbol in symbols],
        dtype=np.float64,
    )


def _warm_or_reuse_tw_mis_opening_receipt(
    *,
    parquet_root: str | Path,
    session_date: str,
    force_probe: bool = False,
) -> dict[str, Any]:
    """Prove MIS now, or reuse only a receipt-proven same-session opening."""

    try:
        return {
            **dict(warm_tw_mis_quote_client(force=force_probe)),
            "source": "twse_tpex:mis",
        }
    except Exception:
        receipt = tw_mis_opening_receipt_status(
            parquet_root=parquet_root,
            session_date=session_date,
        )
        if not receipt.get("ready"):
            raise
        print(
            "[preopen] TWSE MIS unavailable; reuse receipt-backed same-session "
            f"opening rows={receipt['row_count']} path={receipt['path']}",
            flush=True,
        )
        return {
            "ready": True,
            "source": "twse_tpex:mis",
            "proof": "receipt_backed_same_session_opening",
            **receipt,
        }


def _preopen_prepare_key(cfg: LiveMarketConfig, now: datetime) -> str | None:
    if _market_schedule_interval_minutes(cfg) is not None:
        return None
    configured_prepare_time = getattr(cfg, "preopen_prepare_time", None)
    if not configured_prepare_time:
        return None
    prepare_minutes = _hhmm_minutes(configured_prepare_time)
    open_minutes = _hhmm_minutes(getattr(cfg, "open_time", None) or "09:00")
    if prepare_minutes is None or open_minutes is None:
        return None
    now_minutes = now.hour * 60 + now.minute
    session_open, _session_reason = _scheduled_market_session_day(cfg, now)
    if not session_open:
        return None
    session_date = now.date().isoformat()
    if (
        now_minutes >= open_minutes
        and _day_trade_schedule_state(cfg, session_date) != "retry"
    ):
        # After the engine has accepted today's signal, another bot restart
        # must not rebuild the panel/model or re-probe MIS for that market.
        return None
    if prepare_minutes <= now_minutes < open_minutes:
        final_arm_lead = max(
            1,
            min(
                15,
                _env_int("STOCKAGENT_PREOPEN_FINAL_ARM_LEAD_MINUTES", 15) or 15,
            ),
        )
        if (
            now_minutes >= open_minutes - final_arm_lead
            and _preopen_market_ready_for_session(cfg, session_date)
            and not _preopen_market_final_armed_for_session(cfg, session_date)
        ):
            return f"{session_date}:{cfg.market}:preopen-final-arm"
        return f"{session_date}:{cfg.market}:preopen"
    exit_limit_minutes = EXIT_LIMIT_TIME.hour * 60 + EXIT_LIMIT_TIME.minute
    if (
        bool(getattr(cfg, "day_trade_simulation_enabled", False))
        and open_minutes <= now_minutes < exit_limit_minutes
        and not _preopen_market_ready_for_session(cfg, session_date)
    ):
        return f"{session_date}:{cfg.market}:preopen-catch-up"
    return None


def _artifact_backfill_key(cfg: LiveMarketConfig, now: datetime) -> str | None:
    if _market_schedule_interval_minutes(cfg) is not None:
        return None
    backfill_time = _market_artifact_backfill_time(cfg)
    if not backfill_time:
        return None
    session_open, _session_reason = _scheduled_market_session_day(cfg, now)
    if not session_open:
        # Daily market work follows a weekly exchange-session schedule.  In
        # particular, a Friday target must not turn into a Saturday/Sunday
        # retry loop merely because its date is older than the wall clock.
        return None
    target_date = now.date().isoformat()
    try:
        status = _runtime_status_for_display(cfg)
        if not bool(getattr(status.data, "fresh", False)):
            # The pre-signal hook activates an already accepted data release;
            # it is not a downloader.  Wait for the independent data monitor
            # and acceptance pipeline instead of retrying a validation command
            # that cannot make stale data current.
            return None
        target_date = (
            _date_key(status.data.expected_latest_date)
            or _date_key(status.data.last_data_date)
            or target_date
        )
    except Exception:
        # A status error must not disable the scheduler. Keep the wall-date
        # fallback so the task can surface and retry the underlying failure.
        pass
    ready_minutes = _hhmm_minutes(backfill_time)
    now_minutes = now.hour * 60 + now.minute
    if ready_minutes is None:
        return None
    if target_date >= now.date().isoformat() and now_minutes < ready_minutes:
        return None
    return f"{target_date}:{cfg.market}:artifact_backfill"


def _opening_critical_work_pending(observed: datetime | None = None) -> bool:
    """Keep non-opening history jobs off the critical preopen/signal path."""

    for cfg in _market_configs().values():
        if not bool(getattr(cfg, "day_trade_simulation_enabled", False)):
            continue
        tz = ZoneInfo(cfg.timezone or bot.tz.key)
        now = observed.astimezone(tz) if observed is not None else datetime.now(tz)
        session_open, _session_reason = _scheduled_market_session_day(cfg, now)
        if not session_open:
            continue
        prepare_minutes = _hhmm_minutes(
            getattr(cfg, "preopen_prepare_time", None) or "08:15"
        )
        open_minutes = _hhmm_minutes(getattr(cfg, "open_time", None) or "09:00")
        now_minutes = now.hour * 60 + now.minute
        if prepare_minutes is None or open_minutes is None:
            continue
        # Reserve the entire final preopen window and the first five minutes,
        # even if readiness completed early.  This preserves hot model/data
        # caches and prevents formal-history inference from racing 09:00.
        if prepare_minutes <= now_minutes < open_minutes + 5:
            return True
        exit_minutes = EXIT_LIMIT_TIME.hour * 60 + EXIT_LIMIT_TIME.minute
        if (
            open_minutes + 5 <= now_minutes < exit_minutes
            and _day_trade_schedule_state(cfg, now.date().isoformat()) == "retry"
        ):
            return True
    return False


def _scheduled_retry_delay_seconds() -> int:
    return max(1, _env_int("STOCKAGENT_SCHEDULED_RETRY_DELAY_SECONDS", 60) or 60)


def _day_trade_confirmation_delay_seconds() -> int:
    return max(
        1,
        _env_int("STOCKAGENT_DAY_TRADE_CONFIRMATION_DELAY_SECONDS", 2) or 2,
    )


def _day_trade_confirmation_timeout_seconds() -> float:
    return max(
        1.0,
        _env_float("STOCKAGENT_DAY_TRADE_CONFIRMATION_TIMEOUT_SECONDS", 5.0),
    )


def _mark_signal_retry(
    retry_after: dict[str, float],
    failure_counts: dict[str, int],
    key: str,
    *,
    day_trade: bool,
) -> float:
    """Use bounded exponential retries without inheriting slow batch cadence."""

    attempts = int(failure_counts.get(key, 0)) + 1
    failure_counts[key] = attempts
    if day_trade:
        base = max(
            0.1,
            _env_float("STOCKAGENT_DAY_TRADE_RETRY_BASE_SECONDS", 0.25),
        )
        delay = min(5.0, base * (2 ** min(attempts - 1, 5)))
    else:
        delay = float(_scheduled_retry_delay_seconds())
    retry_after[key] = time.monotonic() + delay
    return delay


def _clear_signal_retry(
    retry_after: dict[str, float],
    failure_counts: dict[str, int],
    key: str,
) -> None:
    retry_after.pop(key, None)
    failure_counts.pop(key, None)


def _scheduled_retry_allowed(retry_after: dict[str, float], key: str) -> bool:
    return time.monotonic() >= float(retry_after.get(key, 0.0) or 0.0)


def _mark_scheduled_retry(retry_after: dict[str, float], key: str) -> None:
    retry_after[key] = time.monotonic() + float(_scheduled_retry_delay_seconds())


def _clear_scheduled_retry(retry_after: dict[str, float], key: str) -> None:
    retry_after.pop(key, None)


def _artifact_backfill_status_path() -> Path:
    configured = _env(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(ARTIFACT_BACKFILL_STATUS_PATH),
    )
    return _resolve_repo_path(configured) or Path(str(configured))


@contextmanager
def _artifact_backfill_status_guard() -> Any:
    """Serialize receipt read/modify/write across bot and maintenance workers."""

    path = _artifact_backfill_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    with _ARTIFACT_BACKFILL_STATUS_LOCK:
        with lock_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_artifact_backfill_status() -> dict[str, Any]:
    path = _artifact_backfill_status_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {"schema_version": 1, "jobs": {}}
    if not isinstance(payload, dict):
        return {"schema_version": 1, "jobs": {}}
    if not isinstance(payload.get("jobs"), dict):
        payload["jobs"] = {}
    return payload


def _write_artifact_backfill_status(payload: dict[str, Any]) -> None:
    path = _artifact_backfill_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
    )
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _artifact_backfill_retry_delay_seconds(attempt: int) -> float:
    base = max(
        60.0,
        _env_float("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_BASE_SECONDS", 900.0),
    )
    maximum = max(
        base,
        _env_float("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_MAX_SECONDS", 21600.0),
    )
    return min(maximum, base * (2 ** min(max(0, int(attempt) - 1), 8)))


def _artifact_backfill_retry_allowed(
    key: str,
    *,
    now: datetime | None = None,
) -> bool:
    observed = now or datetime.now().astimezone()
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        job = (payload.get("jobs") or {}).get(str(key))
    if not isinstance(job, dict):
        return True
    if job.get("status") == "ready":
        return False
    if job.get("status") == "running" and job.get("run_id") == _BOT_RUN_ID:
        return False
    raw_next_retry = job.get("next_retry_at")
    if not raw_next_retry:
        return True
    try:
        next_retry = datetime.fromisoformat(str(raw_next_retry).replace("Z", "+00:00"))
    except ValueError:
        return True
    if next_retry.tzinfo is None:
        next_retry = next_retry.replace(tzinfo=observed.tzinfo)
    return observed >= next_retry.astimezone(observed.tzinfo)


def _begin_artifact_backfill(key: str, market: str) -> dict[str, Any]:
    observed = datetime.now().astimezone()
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        jobs = payload.setdefault("jobs", {})
        previous = jobs.get(str(key))
        attempt = int(previous.get("attempt") or 0) + 1 if isinstance(previous, dict) else 1
        job = {
            "key": str(key),
            "market": str(market),
            "status": "running",
            "attempt": attempt,
            "run_id": _BOT_RUN_ID,
            "started_at": observed.isoformat(timespec="seconds"),
            "next_retry_at": None,
            "error_type": None,
            "error_message": None,
        }
        jobs[str(key)] = job
        if len(jobs) > 64:
            for stale_key in list(jobs)[:-64]:
                jobs.pop(stale_key, None)
        payload["schema_version"] = 1
        payload["updated_at"] = observed.isoformat(timespec="seconds")
        _write_artifact_backfill_status(payload)
    return dict(job)


def _finish_artifact_backfill(
    key: str,
    market: str,
    *,
    status: str,
    exc: Exception | None = None,
) -> dict[str, Any]:
    observed = datetime.now().astimezone()
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        jobs = payload.setdefault("jobs", {})
        previous = jobs.get(str(key))
        job = dict(previous) if isinstance(previous, dict) else {}
        attempt = max(1, int(job.get("attempt") or 1))
        job.update(
            {
                "key": str(key),
                "market": str(market),
                "status": str(status),
                "attempt": attempt,
                "run_id": _BOT_RUN_ID,
                "completed_at": observed.isoformat(timespec="seconds"),
            }
        )
        if status == "ready":
            job.pop("retry_delay_seconds", None)
            job.update(
                {
                    "next_retry_at": None,
                    "error_type": None,
                    "error_message": None,
                }
            )
        else:
            delay = _artifact_backfill_retry_delay_seconds(attempt)
            job.update(
                {
                    "next_retry_at": (
                        observed + timedelta(seconds=delay)
                    ).isoformat(timespec="seconds"),
                    "retry_delay_seconds": delay,
                    "error_type": type(exc).__name__ if exc is not None else "Error",
                    "error_message": str(exc)[:2000] if exc is not None else "unknown",
                }
            )
        jobs[str(key)] = job
        payload["schema_version"] = 1
        payload["updated_at"] = observed.isoformat(timespec="seconds")
        _write_artifact_backfill_status(payload)
    return dict(job)


def _artifact_backfill_health_summary() -> dict[str, Any]:
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
    jobs = payload.get("jobs") or {}
    latest_by_market: dict[str, dict[str, Any]] = {}
    for row in jobs.values():
        if not isinstance(row, dict):
            continue
        market = str(row.get("market") or "unknown")
        previous = latest_by_market.get(market)
        row_at = str(row.get("completed_at") or row.get("started_at") or "")
        previous_at = (
            str(previous.get("completed_at") or previous.get("started_at") or "")
            if isinstance(previous, dict)
            else ""
        )
        if previous is None or row_at >= previous_at:
            latest_by_market[market] = row
    current = list(latest_by_market.values())
    failures = [row for row in current if row.get("status") == "failed"]
    running = [row for row in current if row.get("status") == "running"]
    return {
        "status": "degraded" if failures else "running" if running else "ready",
        "failed_count": len(failures),
        "running_count": len(running),
        "market_count": len(current),
        "updated_at": payload.get("updated_at"),
    }


def _signal_now_job_retry_delay_seconds(attempt: int) -> float:
    base = max(
        15.0,
        _env_float("STOCKAGENT_SIGNAL_NOW_RETRY_BASE_SECONDS", 60.0),
    )
    maximum = max(
        base,
        _env_float("STOCKAGENT_SIGNAL_NOW_RETRY_MAX_SECONDS", 900.0),
    )
    return min(maximum, base * (2 ** min(max(0, int(attempt) - 1), 6)))


def _signal_now_job_data_dates(status: MarketRuntimeStatus) -> tuple[str | None, str | None]:
    data = getattr(status, "data", None)
    expected = _date_key(getattr(data, "expected_latest_date", None))
    actual = _date_key(
        getattr(data, "last_data_date", None)
        or getattr(data, "panel_date", None)
    )
    return expected, actual


def _register_signal_now_job(
    key: str,
    *,
    user_id: int,
    cfg: LiveMarketConfig,
    runtime_status: MarketRuntimeStatus,
    requested_price_source: str,
    top_n: int,
    min_abs_delta: float,
    debug: bool,
    force_refresh: bool,
    mode: str,
) -> dict[str, Any]:
    """Persist an interactive job before any background task can be lost."""

    observed = datetime.now().astimezone()
    expected, actual = _signal_now_job_data_dates(runtime_status)
    waiting_source = not bool(getattr(runtime_status.data, "fresh", False))
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        jobs = payload.setdefault("signal_now_jobs", {})
        previous = jobs.get(str(key))
        previous = dict(previous) if isinstance(previous, dict) else {}
        user_ids = {
            int(value)
            for value in previous.get("user_ids", [])
            if str(value).isdigit() and int(value) > 0
        }
        if int(user_id) > 0:
            user_ids.add(int(user_id))
        job = {
            **previous,
            "key": str(key),
            "market": str(cfg.market),
            "target_date": expected or actual,
            "actual_data_date": actual,
            "requested_price_source": str(requested_price_source or "auto"),
            "top_n": int(top_n),
            "min_abs_delta": float(min_abs_delta),
            "debug": bool(debug),
            "force_refresh": bool(force_refresh),
            "mode": _normalize_signal_now_mode(mode),
            "user_ids": sorted(user_ids),
            "status": "waiting_source" if waiting_source else "queued",
            "run_id": _BOT_RUN_ID,
            "updated_at": observed.isoformat(timespec="seconds"),
            "next_retry_at": None,
            "error_type": None,
            "error_message": None,
            "waiting_reason": (
                str(getattr(runtime_status.data, "reason", None) or "source_not_fresh")
                if waiting_source
                else None
            ),
        }
        job.setdefault("created_at", observed.isoformat(timespec="seconds"))
        job.setdefault("attempt", 0)
        jobs[str(key)] = job
        if len(jobs) > 64:
            ordered = sorted(
                jobs,
                key=lambda item: str(
                    (jobs.get(item) or {}).get("updated_at")
                    or (jobs.get(item) or {}).get("created_at")
                    or ""
                ),
            )
            for stale_key in ordered[: len(jobs) - 64]:
                jobs.pop(stale_key, None)
        payload["schema_version"] = max(2, int(payload.get("schema_version") or 1))
        payload["updated_at"] = observed.isoformat(timespec="seconds")
        _write_artifact_backfill_status(payload)
    return dict(job)


def _update_signal_now_job(
    key: str,
    *,
    status: str,
    runtime_status: MarketRuntimeStatus | None = None,
    exc: Exception | None = None,
    signal_id: str | None = None,
    delivered_user_ids: set[int] | None = None,
) -> dict[str, Any]:
    observed = datetime.now().astimezone()
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        jobs = payload.setdefault("signal_now_jobs", {})
        previous = jobs.get(str(key))
        job = dict(previous) if isinstance(previous, dict) else {"key": str(key)}
        attempt = int(job.get("attempt") or 0)
        if status == "running" or (
            status == "failed" and job.get("status") != "running"
        ):
            attempt += 1
        job.update(
            {
                "status": str(status),
                "attempt": attempt,
                "run_id": _BOT_RUN_ID,
                "updated_at": observed.isoformat(timespec="seconds"),
            }
        )
        if runtime_status is not None:
            expected, actual = _signal_now_job_data_dates(runtime_status)
            job.update(
                {
                    "target_date": expected or job.get("target_date"),
                    "actual_data_date": actual,
                }
            )
        if status == "waiting_source":
            waiting_reason = (
                getattr(runtime_status.data, "reason", None)
                if runtime_status is not None
                else None
            )
            job.update(
                {
                    "next_retry_at": None,
                    "waiting_reason": str(
                        waiting_reason or exc or "source_not_fresh"
                    ),
                    "error_type": None,
                    "error_message": None,
                }
            )
        elif status in {"queued", "running"}:
            job.update(
                {
                    "next_retry_at": None,
                    "waiting_reason": None,
                    "error_type": None,
                    "error_message": None,
                }
            )
        elif status == "failed":
            delay = _signal_now_job_retry_delay_seconds(max(1, attempt))
            job.update(
                {
                    "next_retry_at": (
                        observed + timedelta(seconds=delay)
                    ).isoformat(timespec="seconds"),
                    "retry_delay_seconds": delay,
                    "waiting_reason": None,
                    "error_type": type(exc).__name__ if exc is not None else "Error",
                    "error_message": str(exc)[:2000] if exc is not None else "unknown",
                }
            )
        elif status == "ready":
            job.update(
                {
                    "completed_at": observed.isoformat(timespec="seconds"),
                    "next_retry_at": None,
                    "waiting_reason": None,
                    "error_type": None,
                    "error_message": None,
                    "signal_id": signal_id,
                    "delivered_user_ids": sorted(delivered_user_ids or set()),
                }
            )
        jobs[str(key)] = job
        payload["schema_version"] = max(2, int(payload.get("schema_version") or 1))
        payload["updated_at"] = observed.isoformat(timespec="seconds")
        _write_artifact_backfill_status(payload)
    return dict(job)


def _signal_now_job_waiters(key: str) -> set[int]:
    waiters = set(bot._signal_now_background_waiters.get(str(key), set()))
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        row = (payload.get("signal_now_jobs") or {}).get(str(key))
    if isinstance(row, dict):
        waiters.update(
            int(value)
            for value in row.get("user_ids", [])
            if str(value).isdigit() and int(value) > 0
        )
    return waiters


def _signal_now_resumable_jobs(*, now: datetime | None = None) -> list[dict[str, Any]]:
    observed = now or datetime.now().astimezone()
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        rows = list((payload.get("signal_now_jobs") or {}).values())
    pending: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        if row.get("status") not in {"queued", "waiting_source", "running", "failed"}:
            continue
        raw_retry = row.get("next_retry_at")
        if raw_retry:
            try:
                retry_at = datetime.fromisoformat(str(raw_retry).replace("Z", "+00:00"))
            except ValueError:
                retry_at = observed
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=observed.tzinfo)
            if observed < retry_at.astimezone(observed.tzinfo):
                continue
        pending.append(row)
    return sorted(
        pending,
        key=lambda row: str(row.get("created_at") or row.get("updated_at") or ""),
    )


def _signal_now_job_health_summary() -> dict[str, Any]:
    with _artifact_backfill_status_guard():
        payload = _load_artifact_backfill_status()
        rows = [
            row
            for row in (payload.get("signal_now_jobs") or {}).values()
            if isinstance(row, dict)
        ]
    active = [
        row
        for row in rows
        if row.get("status") in {"queued", "waiting_source", "running", "failed"}
    ]
    counts = {
        name: sum(row.get("status") == name for row in active)
        for name in ("queued", "waiting_source", "running", "failed")
    }
    state = (
        "degraded"
        if counts["failed"]
        else "running"
        if counts["running"] or counts["queued"]
        else "waiting_source"
        if counts["waiting_source"]
        else "ready"
    )
    return {
        "status": state,
        "active_count": len(active),
        **{f"{name}_count": count for name, count in counts.items()},
    }


def _interactive_signal_work_pending() -> bool:
    """Give persisted/manual signal work priority over historical maintenance."""

    if any(
        task is not None and not task.done()
        for task in bot._signal_now_background_tasks.values()
    ):
        return True
    return bool(_signal_now_resumable_jobs())


def _signal_now_source_is_pending(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus | None,
    exc: Exception,
) -> bool:
    if status is not None and not bool(getattr(status.data, "fresh", False)):
        return True
    text = str(exc).lower()
    if any(
        token in text
        for token in (
            "publication_pending",
            "publication pending",
            "exact-session day-trade eligibility unavailable",
            "publication watcher receipt is not current",
            "older than expected",
        )
    ):
        return True
    if not bool(getattr(cfg, "day_trade_simulation_enabled", False)):
        return False
    receipt_path = (
        ROOT
        / "artifacts"
        / "data_refresh"
        / "tw_public"
        / "opening_activation"
        / "latest.json"
    )
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return False
    failures = receipt.get("failures")
    return bool(
        receipt.get("status") == "failed"
        and isinstance(failures, list)
        and any(
            str(item).startswith(("publication:", "eligibility:"))
            for item in failures
        )
    )


def _resolve_pre_signal_command(command: tuple[str, ...] | list[str]) -> list[str]:
    resolved = [str(item) for item in command]
    if resolved and resolved[0] == PYTHON_EXECUTABLE_SENTINEL:
        resolved[0] = sys.executable
    return resolved


def _pre_signal_success_ttl_seconds() -> float:
    # A shared daily refresh can take longer than inference for one market.  Keep
    # the result long enough for every deployment using the same command to
    # reuse it instead of rebuilding the full TW panel once per market.
    return max(0.0, _env_float("STOCKAGENT_PRE_SIGNAL_SUCCESS_TTL_SECONDS", 3600.0))


def _pre_signal_failure_ttl_seconds() -> float:
    # Publication lag and other fail-closed data results are shared by all
    # deployments using the same command.  Retrying once per market only adds
    # load; retry the shared source at a bounded cadence instead.
    return max(0.0, _env_float("STOCKAGENT_PRE_SIGNAL_FAILURE_TTL_SECONDS", 900.0))


def _recent_pre_signal_success(command: list[str]) -> bool:
    ttl = _pre_signal_success_ttl_seconds()
    if ttl <= 0:
        return False
    key = tuple(command)
    now = time.monotonic()
    with _PRE_SIGNAL_SUCCESS_LOCK:
        completed_at = _PRE_SIGNAL_SUCCESS_AT.get(key)
        if completed_at is None or now - completed_at > ttl:
            _PRE_SIGNAL_SUCCESS_AT.pop(key, None)
            return False
        return True


def _remember_pre_signal_success(command: list[str]) -> None:
    key = tuple(command)
    now = time.monotonic()
    with _PRE_SIGNAL_SUCCESS_LOCK:
        _PRE_SIGNAL_SUCCESS_AT[key] = now
        _PRE_SIGNAL_FAILURE_AT.pop(key, None)
        stale_before = now - max(60.0, _pre_signal_success_ttl_seconds() * 4.0)
        for cached_key, completed_at in list(_PRE_SIGNAL_SUCCESS_AT.items()):
            if completed_at < stale_before:
                _PRE_SIGNAL_SUCCESS_AT.pop(cached_key, None)


def _recent_pre_signal_failure(command: list[str]) -> str | None:
    ttl = _pre_signal_failure_ttl_seconds()
    if ttl <= 0:
        return None
    key = tuple(command)
    now = time.monotonic()
    with _PRE_SIGNAL_SUCCESS_LOCK:
        cached = _PRE_SIGNAL_FAILURE_AT.get(key)
        if cached is None:
            return None
        completed_at, message = cached
        if now - completed_at > ttl:
            _PRE_SIGNAL_FAILURE_AT.pop(key, None)
            return None
        return message


def _remember_pre_signal_failure(command: list[str], message: str) -> None:
    key = tuple(command)
    now = time.monotonic()
    with _PRE_SIGNAL_SUCCESS_LOCK:
        _PRE_SIGNAL_FAILURE_AT[key] = (now, str(message))
        _PRE_SIGNAL_SUCCESS_AT.pop(key, None)
        stale_before = now - max(60.0, _pre_signal_failure_ttl_seconds() * 4.0)
        for cached_key, (completed_at, _) in list(_PRE_SIGNAL_FAILURE_AT.items()):
            if completed_at < stale_before:
                _PRE_SIGNAL_FAILURE_AT.pop(cached_key, None)


def _recent_pre_signal_artifact_failure(
    cfg: LiveMarketConfig, command: list[str]
) -> str | None:
    failure = first_download_failure(
        command=command,
        market=cfg.market,
        market_type=getattr(cfg, "market_type", None),
        resolve_path=_resolve_repo_path,
    )
    if failure is None:
        return None
    path, reason = failure
    try:
        age_seconds = max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        age_seconds = 0.0
    if age_seconds > _pre_signal_failure_ttl_seconds():
        return None
    return (
        f"`{cfg.market}` recent shared data update is unusable ({reason}); "
        f"summary=`{_display_path(path)}`"
    )


def _tw_data_layer_lock_path(command: list[str]) -> Path | None:
    command_names = {Path(str(item)).name for item in command}
    if command_names.intersection(
        {
            "activate_tw_public_opening_data.py",
            "refresh_tw_public_live_snapshot.py",
        }
    ):
        configured = command_option(command, "--live-root")
        live_root = Path(configured or "/srv/stockagent-live/data_tw_public")
        if not live_root.is_absolute():
            live_root = ROOT / live_root
        return live_root.parent / ".locks" / "tw-public-refresh.lock"
    if "download_tw_official_data.py" not in command_names:
        return None
    configured = command_option(command, "--lock-file")
    path = Path(configured) if configured else Path("artifacts/data_locks/tw_official_data.lock")
    return path if path.is_absolute() else ROOT / path


def _wait_for_existing_tw_data_update(command: list[str], *, timeout_seconds: int) -> bool:
    lock_path = _tw_data_layer_lock_path(command)
    if lock_path is None:
        return False
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                f"[pre-signal] existing TW data update owns lock={_display_path(lock_path)}; waiting",
                flush=True,
            )
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            return False

        deadline = time.monotonic() + max(1, int(timeout_seconds))
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise BotUserError(
                        f"TW data update is still running after {timeout_seconds}s; "
                        f"lock=`{_display_path(lock_path)}`"
                    )
                time.sleep(1.0)
                continue
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            print("[pre-signal] existing TW data update completed; reusing its outputs", flush=True)
            return True
    finally:
        handle.close()


def _pre_signal_run_lock(command: list[str]) -> threading.Lock:
    key = tuple(command)
    with _PRE_SIGNAL_RUN_LOCKS_LOCK:
        lock = _PRE_SIGNAL_RUN_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PRE_SIGNAL_RUN_LOCKS[key] = lock
        return lock


def _run_pre_signal_command(
    cfg: LiveMarketConfig,
    *,
    bypass_cache: bool = False,
) -> None:
    """Run one shared updater at a time and recheck its cache inside the lock."""

    if not cfg.pre_signal_command:
        return
    command = _resolve_pre_signal_command(cfg.pre_signal_command)
    with _pre_signal_run_lock(command):
        _run_pre_signal_command_serialized(cfg, bypass_cache=bypass_cache)


def _run_pre_signal_command_serialized(
    cfg: LiveMarketConfig,
    *,
    bypass_cache: bool = False,
) -> None:
    if not cfg.pre_signal_command:
        return
    command = _resolve_pre_signal_command(cfg.pre_signal_command)
    started = datetime.now().astimezone().isoformat(timespec="seconds")
    timeout_seconds = max(1, int(cfg.pre_signal_timeout_seconds))
    if not bypass_cache and _recent_pre_signal_success(command):
        print(
            f"[pre-signal:{cfg.market}] reuse recent shared data update "
            f"ttl={_pre_signal_success_ttl_seconds():.0f}s",
            flush=True,
        )
        return
    recent_failure = None if bypass_cache else _recent_pre_signal_failure(command)
    if recent_failure is None and not bypass_cache:
        recent_failure = _recent_pre_signal_artifact_failure(cfg, command)
        if recent_failure is not None:
            _remember_pre_signal_failure(command, recent_failure)
    if recent_failure is not None:
        print(
            f"[pre-signal:{cfg.market}] reuse recent shared data failure "
            f"ttl={_pre_signal_failure_ttl_seconds():.0f}s",
            flush=True,
        )
        raise BotUserError(recent_failure)
    if _wait_for_existing_tw_data_update(command, timeout_seconds=timeout_seconds):
        log_path = ROOT / "artifacts" / "discord_bot" / "pre_signal_commands.log"
        try:
            _validate_pre_signal_download_artifacts(cfg, command, log_path)
        except BotUserError as exc:
            _remember_pre_signal_failure(command, str(exc))
            raise
        _remember_pre_signal_success(command)
        clear_live_panel_memory_cache()
        return
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    stdout_tail_chunks: list[bytes] = []

    def remember_tail(chunk: bytes) -> None:
        stdout_tail_chunks.append(chunk)
        while sum(len(item) for item in stdout_tail_chunks) > 4000:
            stdout_tail_chunks.pop(0)

    print(f"[pre-signal:{cfg.market}] start command={' '.join(command)} timeout={timeout_seconds}s", flush=True)
    proc = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
    )

    def stream_output() -> None:
        assert proc.stdout is not None
        try:
            while True:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    break
                remember_tail(chunk)
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
        except Exception as exc:
            _log_exception(f"pre_signal_stream:{cfg.market}", exc)

    stream_thread = threading.Thread(target=stream_output, name=f"pre-signal-{cfg.market}", daemon=True)
    stream_thread.start()
    try:
        returncode = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        stream_thread.join(timeout=5)
        _log_exception(f"pre_signal_command:{cfg.market}", exc)
        message = (
            f"`{cfg.market}` pre-signal data update timed out after "
            f"{cfg.pre_signal_timeout_seconds}s"
        )
        _remember_pre_signal_failure(command, message)
        raise BotUserError(message)
    stream_thread.join(timeout=5)
    stdout_tail = b"".join(stdout_tail_chunks)[-4000:].decode("utf-8", errors="replace")
    log_path = ROOT / "artifacts" / "discord_bot" / "pre_signal_commands.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "timestamp": started,
                    "market": cfg.market,
                    "command": command,
                    "returncode": returncode,
                    "stdout_tail": stdout_tail,
                    "stderr_tail": "",
                },
                ensure_ascii=False,
            )
            + "\n"
        )
    print(f"[pre-signal:{cfg.market}] done returncode={returncode} log={_display_path(log_path)}", flush=True)
    if returncode != 0:
        detail = _pre_signal_failure_detail(cfg, command, stdout_tail)
        message = (
            f"`{cfg.market}` pre-signal data update failed rc={returncode}; "
            f"{detail} log=`{_display_path(log_path)}`"
        )
        _remember_pre_signal_failure(command, message)
        raise BotUserError(message)
    try:
        _validate_pre_signal_download_artifacts(cfg, command, log_path)
    except BotUserError as exc:
        _remember_pre_signal_failure(command, str(exc))
        raise
    _remember_pre_signal_success(command)
    clear_live_panel_memory_cache()


def _completed_session_signal_path(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus,
) -> bool:
    """Use official completed-session close data outside the live auction."""

    return bool(
        getattr(cfg, "day_trade_simulation_enabled", False)
        and getattr(cfg, "completed_session_command", ())
        and not bool(getattr(status, "market_open", False))
    )


def _run_completed_session_command(
    cfg: LiveMarketConfig,
    *,
    bypass_cache: bool = False,
) -> None:
    command = tuple(getattr(cfg, "completed_session_command", ()) or ())
    if not command:
        return
    completed_cfg = replace(
        cfg,
        pre_signal_command=command,
        pre_signal_timeout_seconds=max(
            1,
            int(getattr(cfg, "completed_session_timeout_seconds", 900) or 900),
        ),
    )
    _run_pre_signal_command(completed_cfg, bypass_cache=bypass_cache)


def _pre_signal_failure_detail(cfg: LiveMarketConfig, command: list[str], stdout_tail: str) -> str:
    asset = command_asset(command)
    output_root = command_option(command, "--output-root")
    if asset and output_root:
        root = _resolve_repo_path(output_root) or Path(output_root)
        for name in ("download_report.csv", "repair_report.csv"):
            path = root / asset / name
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8", newline="") as handle:
                    for row in csv.DictReader(handle):
                        status = str(row.get("status") or "").strip().lower()
                        message = str(row.get("message") or "").strip()
                        if status == "failed" and message:
                            return f"report=`{_display_path(path)}` reason=`{message[:220]}`;"
            except Exception:
                continue
    failure = first_download_failure(
        command=command,
        market=cfg.market,
        market_type=getattr(cfg, "market_type", None),
        resolve_path=_resolve_repo_path,
    )
    if failure is not None:
        path, reason = failure
        return f"summary=`{_display_path(path)}` reason=`{reason}`;"
    tail = " ".join(str(stdout_tail or "").split())[-220:]
    return f"tail=`{tail}`;" if tail else ""


def _validate_pre_signal_download_artifacts(cfg: LiveMarketConfig, command: list[str], log_path: Path) -> None:
    failure = first_download_failure(
        command=command,
        market=cfg.market,
        market_type=getattr(cfg, "market_type", None),
        resolve_path=_resolve_repo_path,
    )
    if failure is None:
        return
    path, reason = failure
    raise BotUserError(
        f"`{cfg.market}` data update did not produce usable data ({reason}); "
        f"summary=`{_display_path(path)}` log=`{_display_path(log_path)}`"
    )


def _date_key(value: Any) -> str | None:
    text = str(value or "").replace("T", " ").strip()
    if not text or text.lower() in {"none", "null", "nat", "n/a"}:
        return None
    return text[:10] if len(text) >= 10 else text


def _completed_session_receipt_ready(status: MarketRuntimeStatus) -> bool:
    """Require derived close data to acknowledge the newest accepted close phase."""

    data = getattr(status, "data", None)
    expected = _date_key(
        getattr(data, "expected_latest_date", None)
        or getattr(data, "last_data_date", None)
        or getattr(data, "panel_date", None)
    )
    if not expected:
        return False
    receipt_path = Path(
        _env(
            "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT",
            "artifacts/data_refresh/tw_public/completed_session/latest.json",
        )
    )
    publication_root = Path(
        _env(
            "STOCKAGENT_TW_PUBLICATION_RECEIPT_ROOT",
            "artifacts/data_refresh/tw_public/publications",
        )
    )
    if not receipt_path.is_absolute():
        receipt_path = ROOT / receipt_path
    if not publication_root.is_absolute():
        publication_root = ROOT / publication_root
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(receipt, dict):
        return False
    newest_phase: str | None = None
    newest_completed_at: str | None = None
    for phase in ("close_final", "close_revision", "close_initial"):
        try:
            publication = json.loads(
                (publication_root / phase / "latest.json").read_text(
                    encoding="utf-8"
                )
            )
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(publication, dict):
            continue
        summary = publication.get("download_summary")
        if (
            publication.get("status") == "ok"
            and str(publication.get("started_at_taipei") or "")[:10] == expected
            and isinstance(summary, dict)
            and str(summary.get("end_date") or "")[:10] == expected
            and summary.get("daily_close_ready") is True
            and int(summary.get("blocking_failed_count") or 0) == 0
        ):
            newest_phase = phase
            newest_completed_at = str(publication.get("completed_at_taipei") or "")
            break
    after = receipt.get("after")
    dates = after.get("dates") if isinstance(after, dict) else None
    return bool(
        newest_phase
        and receipt.get("status") == "ok"
        and receipt.get("expected_date") == expected
        and receipt.get("source_publication_phase") == newest_phase
        and str(receipt.get("source_publication_completed_at_taipei") or "")
        == newest_completed_at
        and isinstance(after, dict)
        and after.get("current") is True
        and isinstance(dates, dict)
        and dates.get("stock_panel") == expected
        and dates.get("public_features") == expected
    )


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=ZoneInfo("UTC"))


def _hhmm_minutes(value: Any) -> int | None:
    text = str(value or "").strip()
    match = re.match(r"^(\d{1,2}):(\d{2})", text)
    if not match:
        return None
    return int(match.group(1)) * 60 + int(match.group(2))


def _should_use_realtime_quote_after_open(cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> bool:
    try:
        now = datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei"))
    except Exception:
        now = datetime.now()
    configured_open = _hhmm_minutes(getattr(cfg, "open_time", None))
    open_minutes = configured_open if configured_open is not None else (9 * 60)
    if now.hour * 60 + now.minute < open_minutes:
        return False
    data = getattr(status, "data", None)
    latest = _date_key(getattr(data, "last_data_date", None) or getattr(data, "panel_date", None))
    today = now.date().isoformat()
    return bool(latest and latest < today)


def _realtime_price_source_for_market(cfg: LiveMarketConfig) -> str | None:
    market_type = str(getattr(cfg, "market_type", "") or "").strip().lower()
    if market_type in {"tw", "taiwan"}:
        return "shioaji"
    if market_type in {"us", "usa", "stock", "stocks", "equity", "equities"}:
        return "yahoo"
    return None


def _auto_signal_price_source(cfg: LiveMarketConfig, status: MarketRuntimeStatus, requested: str | None) -> str | None:
    text = str(requested or "").strip().lower()
    if text and text != "auto":
        return text
    market_type = str(getattr(cfg, "market_type", "") or "").strip().lower()
    frequency = str(getattr(cfg, "history_frequency", "daily") or "daily").strip().lower()
    if market_type in {"crypto", "forex", "fx"} or frequency in {"bar", "intraday", "1m", "15m"}:
        if not status.market_open:
            return None
        return "panel"
    if market_type in {"tw", "taiwan"}:
        if bool(getattr(cfg, "day_trade_simulation_enabled", False)):
            # The model needs one official same-session opening observation,
            # not a full-universe executable book.  The ``tw`` provider owns a
            # receipt-backed single-flight MIS snapshot shared by all models.
            # The independent paper engine remains the sole Shioaji client on
            # the execution path and observes a causally later best Bid/Ask for
            # the union of actual order candidates.
            return "tw" if (
                bool(getattr(status, "market_open", False))
                or _should_use_realtime_quote_after_open(cfg, status)
            ) else None
        return "shioaji" if (bool(getattr(status, "market_open", False)) or _should_use_realtime_quote_after_open(cfg, status)) else None
    realtime_source = _realtime_price_source_for_market(cfg)
    if realtime_source and _should_use_realtime_quote_after_open(cfg, status):
        return realtime_source
    if not status.market_open:
        return None
    return "yahoo"


def _prepare_realtime_signal_sync(
    cfg: LiveMarketConfig,
    *,
    requested_price_source: str | None = "auto",
    force_refresh: bool = False,
    completed_session: bool = False,
) -> tuple[str | None, MarketRuntimeStatus, bool]:
    status = _ensure_signal_ready(cfg)
    should_refresh = bool(force_refresh or _market_schedule_interval_minutes(cfg) is not None)
    if should_refresh:
        if completed_session:
            _run_completed_session_command(cfg, bypass_cache=bool(force_refresh))
        else:
            _run_pre_signal_command(cfg)
        _clear_runtime_status_cache()
        status = _runtime_status(cfg)
        if not status.data.fresh:
            _require_fresh_data_for_artifact_generation(cfg, status)
    return _auto_signal_price_source(cfg, status, requested_price_source), status, should_refresh


async def market_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    query = str(current or "").strip().lower()
    choices: list[app_commands.Choice[str]] = []
    for key, cfg in sorted(_market_configs().items()):
        if not _market_enabled(cfg):
            continue
        label = f"{key} - {cfg.label}"
        if query and query not in key.lower() and query not in cfg.label.lower():
            continue
        choices.append(app_commands.Choice(name=label[:100], value=key))
    return choices[:25]


def _signal_kwargs(
    *,
    market: str | None = None,
    price_source: str | None = None,
    top_n: int | None = None,
    min_abs_delta: float | None = None,
    signal_id: str | None = None,
    scheduled: bool = False,
    progress_callback: Any | None = None,
    progress_label: str | None = None,
    include_unconstrained_raw_scores: bool = False,
    prepared_status: MarketRuntimeStatus | None = None,
) -> dict:
    cfg = _effective_market_config(_resolve_market(market))
    status = prepared_status or _ensure_signal_ready(cfg, scheduled=scheduled)
    configured_backfill = max(0, int(getattr(cfg, "previous_signal_backfill_limit", 32)))
    backfill_limit = max(
        0,
        _env_int("STOCKAGENT_SIGNAL_BACKFILL_LIMIT", configured_backfill)
        or configured_backfill,
    )
    overrides = {
        "price_source": price_source if price_source and price_source != "auto" else None,
        "top_n": top_n,
        "min_abs_delta": min_abs_delta,
        "signal_id": signal_id,
        "market_notice": _market_notice(status),
        "previous_signal_backfill_limit": backfill_limit,
        "progress_callback": progress_callback,
        "progress_label": progress_label,
        "include_unconstrained_raw_scores": bool(include_unconstrained_raw_scores),
    }
    return cfg.signal_kwargs(**overrides)


async def _send_command_error(interaction: discord.Interaction, prefix: str, exc: Exception) -> None:
    if isinstance(exc, BotUserError):
        _log_exception(prefix, exc)
        try:
            await interaction.followup.send(str(exc))
        except discord.HTTPException as send_exc:
            _log_exception(f"{prefix}:error_response", send_exc)
        return
    _log_exception(prefix, exc)
    try:
        await interaction.followup.send(
            f"{prefix} failed: `{type(exc).__name__}`。詳細 traceback 已寫入 `{ERROR_LOG_PATH}`。"
        )
    except discord.HTTPException as send_exc:
        _log_exception(f"{prefix}:error_response", send_exc)


class StockAgentBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(
            self,
            allowed_installs=app_commands.AppInstallationType(guild=True, user=True),
            allowed_contexts=app_commands.AppCommandContext(
                guild=True,
                dm_channel=True,
                private_channel=True,
            ),
        )
        self.tz = ZoneInfo(_env("STOCKAGENT_TZ", "Asia/Taipei") or "Asia/Taipei")
        self.signal_time = _env("STOCKAGENT_SIGNAL_TIME", "13:15") or "13:15"
        self.channel_id = _env_int("DISCORD_CHANNEL_ID")
        self._last_scheduled_keys: set[str] = set()
        self._last_preopen_prepare_keys: set[str] = set()
        self._last_daily_summary_keys: set[str] = set()
        self._last_artifact_backfill_keys: set[str] = set()
        self._scheduled_retry_after: dict[str, float] = {}
        self._scheduled_failure_counts: dict[str, int] = {}
        self._scheduled_error_notice_keys: set[str] = set()
        self._preopen_retry_after: dict[str, float] = {}
        self._preopen_failure_counts: dict[str, int] = {}
        self._daily_summary_retry_after: dict[str, float] = {}
        self._artifact_backfill_retry_after: dict[str, float] = {}
        self._model_deployment_last_check: dict[str, float] = {}
        self._signal_now_background_tasks: dict[str, asyncio.Task[None]] = {}
        self._signal_now_background_waiters: dict[str, set[int]] = {}
        self._opening_attempt_started_monotonic: float | None = None
        self._opening_attempt_last_progress_monotonic: float | None = None
        self._opening_attempt_progress_message: str | None = None
        self._opening_attempt_market: str | None = None
        self._opening_attempt_hot = False

    async def setup_hook(self) -> None:
        # Strategy recording is the primary responsibility of this process.
        # Start its catch-up loop before Discord command synchronization so a
        # missed market schedule is recovered immediately after login.
        _rotate_error_log_if_needed()
        scheduled_signal.start()
        service_heartbeat.start()
        notify_systemd(
            "READY=1\nSTATUS=Discord scheduler ready; opening execution is independent of notifications"
        )
        try:
            synced = await asyncio.wait_for(
                self.tree.sync(),
                timeout=max(
                    2.0,
                    _env_float("STOCKAGENT_DISCORD_COMMAND_SYNC_TIMEOUT_SECONDS", 10.0),
                ),
            )
        except Exception as exc:
            # Global command registration is administrative I/O, not part of
            # signal generation. A slow Discord REST call must not prevent the
            # gateway or the already-started local scheduler from progressing.
            _log_exception("discord_command_sync", exc)
            print(
                "global app command sync deferred; local signal scheduler remains active "
                f"error={type(exc).__name__}",
                flush=True,
            )
        else:
            print(
                f"synced {len(synced)} global app commands "
                "installs=guild,user contexts=guild,dm,private_channel",
                flush=True,
            )
        signal_now_job_resumer.start()
        preopen_prepare.start()
        daily_summary.start()
        # Full-history maintenance runs in a separate systemd oneshot/cgroup.
        # Keeping it out of this process protects Gateway heartbeats and
        # interactive commands from post-close CPU, memory, and OOM failures.
        model_auto_deployment.start()

    async def on_ready(self) -> None:
        print(f"logged in as {self.user} signal_time={self.signal_time} channel_id={self.channel_id}", flush=True)


bot = StockAgentBot()


def _touch_opening_attempt_progress(message: str) -> None:
    """Refresh the no-progress watchdog without extending a genuinely idle task."""

    if bot._opening_attempt_started_monotonic is None:
        return
    bot._opening_attempt_last_progress_monotonic = time.monotonic()
    bot._opening_attempt_progress_message = str(message or "progress")


def _opening_attempt_timeout_seconds(*, hot: bool) -> float:
    """Return a watchdog deadline longer than any bounded opening I/O stage.

    A hot opening attempt can legitimately spend several explicitly bounded
    Shioaji Snapshot batches plus three 8-second MIS fallback waves between
    progress callbacks.  Killing the process inside that interval destroys the
    proven panel/model caches and turns a transient quote delay into a
    multi-minute cold restart.  The environment may lengthen this deadline,
    but it may not make it shorter than the causal work it supervises.
    """

    floor = 60.0 if hot else 180.0
    name = (
        "STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS"
        if hot
        else "STOCKAGENT_OPENING_COLD_ATTEMPT_TIMEOUT_SECONDS"
    )
    return max(floor, _env_float(name, floor))


class _ConsoleProgress:
    def __init__(
        self,
        *,
        prefix: str = "discord",
        event_callback: Any | None = None,
    ) -> None:
        self.prefix = str(prefix or "discord")
        self.started_at = time.perf_counter()
        self.event_callback = event_callback

    def __call__(self, event: dict[str, Any]) -> None:
        label = str(event.get("label") or self.prefix)
        message = str(event.get("message") or "")
        _touch_opening_attempt_progress(f"{label}: {message}")
        try:
            step = int(event.get("step") or 0)
            total = max(1, int(event.get("total") or 1))
        except Exception:
            step = 0
            total = 1
        step = min(max(step, 0), total)
        width = 28
        filled = int(round(width * step / total))
        bar = "#" * filled + "-" * (width - filled)
        pct = 100.0 * step / total
        elapsed = time.perf_counter() - self.started_at
        print(
            f"[signal-progress] {label} [{bar}] {step:02d}/{total:02d} {pct:6.2f}% "
            f"{elapsed:7.1f}s {message}",
            flush=True,
        )
        if self.event_callback is not None:
            try:
                self.event_callback(dict(event))
            except Exception as exc:
                print(
                    f"[signal-progress-state] status=failed "
                    f"error={type(exc).__name__}: {exc}",
                    flush=True,
                )


def _run_market_signal_sync(**kwargs):
    if _env_bool("STOCKAGENT_BOT_PROGRESS", True) and kwargs.get("progress_callback") is None:
        market = str(kwargs.get("market") or _default_market()).strip() or _default_market()
        label = str(kwargs.get("progress_label") or f"discord:{market}").strip()
        kwargs["progress_callback"] = _ConsoleProgress(prefix=label)
        kwargs["progress_label"] = label
    with _MODEL_INFERENCE_LOCK:
        return generate_live_signal(**_signal_kwargs(**kwargs))


async def _run_market_signal(**kwargs):
    return await asyncio.to_thread(_run_market_signal_sync, **kwargs)


def _write_preopen_readiness(
    cfg: LiveMarketConfig,
    *,
    status: str,
    started_at: str,
    elapsed_seconds: float,
    summary: dict[str, Any] | None = None,
    error: str | None = None,
    step: int | None = None,
    total: int | None = None,
    message: str | None = None,
) -> None:
    path = _preopen_readiness_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _PREOPEN_READINESS_LOCK:
        try:
            payload = (
                json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            )
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        markets = payload.get("markets")
        if not isinstance(markets, dict):
            markets = {}
        if payload.get("run_id") != _BOT_RUN_ID:
            session_date = datetime.now(
                ZoneInfo(cfg.timezone or "Asia/Taipei")
            ).date().isoformat()
            markets = {
                str(market): row
                for market, row in markets.items()
                if isinstance(row, dict)
                and row.get("status") == "ready"
                and _summary_date_matches(
                    row.get("completed_at"), session_date
                )
            }
        terminal = str(status) in {"ready", "failed"}
        completed_at = (
            datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei")).isoformat(
                timespec="seconds"
            )
            if terminal
            else None
        )
        markets[cfg.market] = {
            "status": str(status),
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_seconds": round(float(elapsed_seconds), 3),
            "step": int(step) if step is not None else None,
            "total": int(total) if total is not None else None,
            "message": message,
            "panel_date": (summary or {}).get("panel_date"),
            "fold_id": (summary or {}).get("fold_id"),
            "checkpoint_fingerprint": (summary or {}).get("checkpoint_fingerprint"),
            "symbol_count": (summary or {}).get("symbol_count"),
            "live_latency": (summary or {}).get("live_latency"),
            "preopen_price_limits": (summary or {}).get("preopen_price_limits"),
            "same_session_eligibility": (summary or {}).get(
                "same_session_eligibility"
            ),
            "opening_source_prewarm": (summary or {}).get(
                "opening_source_prewarm"
            ),
            "tw_mis_fallback_prewarm": (summary or {}).get(
                "tw_mis_fallback_prewarm"
            ),
            "error": error,
        }
        payload = {
            "schema_version": 2,
            "run_id": _BOT_RUN_ID,
            "run_started_at": _BOT_RUN_STARTED_AT,
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "markets": markets,
        }
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)


def _write_preopen_final_arm(
    cfg: LiveMarketConfig,
    *,
    status: str,
    started_at: str,
    elapsed_seconds: float,
    summary: dict[str, Any] | None = None,
    attempts: int = 1,
    error: str | None = None,
) -> None:
    path = _preopen_readiness_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _PREOPEN_READINESS_LOCK:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        markets = payload.get("markets")
        if not isinstance(markets, dict):
            markets = {}
        row = markets.get(cfg.market)
        row = dict(row) if isinstance(row, dict) else {}
        terminal = str(status) in {"ready", "failed"}
        completed_at = (
            datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei")).isoformat(
                timespec="seconds"
            )
            if terminal
            else None
        )
        row["final_arm"] = {
            "status": str(status),
            "run_id": _BOT_RUN_ID,
            "started_at": started_at,
            "completed_at": completed_at,
            "elapsed_seconds": round(float(elapsed_seconds), 3),
            "attempts": max(1, int(attempts)),
            "live_latency": (summary or {}).get("live_latency"),
            "opening_source_prewarm": (summary or {}).get(
                "opening_source_prewarm"
            ),
            "tw_mis_fallback_prewarm": (summary or {}).get(
                "tw_mis_fallback_prewarm"
            ),
            "error": error,
        }
        markets[cfg.market] = row
        payload.update(
            {
                "schema_version": max(3, int(payload.get("schema_version") or 0)),
                "run_id": _BOT_RUN_ID,
                "run_started_at": _BOT_RUN_STARTED_AT,
                "updated_at": datetime.now().astimezone().isoformat(
                    timespec="seconds"
                ),
                "markets": markets,
            }
        )
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)


def _prewarm_market_signal_sync(cfg: LiveMarketConfig) -> LiveSignalResult:
    """Single-flight one market/session prewarm across scheduler task loops."""

    timezone_name = cfg.timezone or "Asia/Taipei"
    session_date = datetime.now(ZoneInfo(timezone_name)).date().isoformat()
    key = (str(cfg.market), session_date)
    with _PREWARM_RUN_LOCKS_LOCK:
        lock = _PREWARM_RUN_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PREWARM_RUN_LOCKS[key] = lock
    with lock:
        cached = _PREWARM_RESULTS.get(key)
        if cached is not None:
            return cached
        result = _prewarm_market_signal_serialized(cfg)
        _PREWARM_RESULTS[key] = result
        with _PREWARM_RUN_LOCKS_LOCK:
            for stale_key in list(_PREWARM_RUN_LOCKS):
                if stale_key[1] != session_date:
                    _PREWARM_RUN_LOCKS.pop(stale_key, None)
                    _PREWARM_RESULTS.pop(stale_key, None)
        return result


def _prewarm_market_signal_serialized(cfg: LiveMarketConfig) -> LiveSignalResult:
    started = time.perf_counter()
    started_at = datetime.now(
        ZoneInfo(cfg.timezone or "Asia/Taipei")
    ).isoformat(timespec="seconds")
    progress_total = 24

    def update_progress(step: int, message: str) -> None:
        _touch_opening_attempt_progress(f"preopen:{cfg.market}: {message}")
        _write_preopen_readiness(
            cfg,
            status="running",
            started_at=started_at,
            elapsed_seconds=time.perf_counter() - started,
            step=step,
            total=progress_total,
            message=message,
        )

    try:
        update_progress(1, "refresh public snapshot")
        _run_pre_signal_command(cfg)
        update_progress(2, "public snapshot ready")
        _clear_runtime_status_cache()
        status = _runtime_status(cfg)
        if not status.data.fresh:
            _require_fresh_data_for_artifact_generation(cfg, status)
        # Previous live-weight history is reporting state for tw_day_trade;
        # reconcile it before the opening gate, never on the 09:00 hot path.
        _sync_latest_live_weights_to_market_artifact(cfg)
        update_progress(3, "runtime data fresh")
        experiment = load_config(cfg.config_path)
        observed = datetime.now(
            ZoneInfo(getattr(cfg, "timezone", None) or "Asia/Taipei")
        )
        rule_data_dir = resolve_day_trade_rule_data_dir(
            cfg.day_trade_rule_data_dir,
            parquet_root=Path(experiment.data.parquet_root),
            repo_root=ROOT,
        )
        eligibility_coverage = require_exact_session_eligibility(
            rule_data_dir=rule_data_dir,
            parquet_root=Path(experiment.data.parquet_root),
            trading_date=observed.date(),
        )
        same_session_eligibility = {
            "target_date": observed.date().isoformat(),
            "rule_data_dir": str(rule_data_dir),
            "venues": eligibility_coverage,
        }
        update_progress(4, "same-session eligibility ready")
        # Prime the official whole-market opening source here. The independent
        # execution process separately owns and prewarms the sole Shioaji
        # session used for the later candidate-only best Bid/Ask observation.
        opening_source_prewarm = _warm_or_reuse_tw_mis_opening_receipt(
            parquet_root=experiment.data.parquet_root,
            session_date=observed.date().isoformat(),
        )
        opening_source_prewarm["run_id"] = _BOT_RUN_ID
        update_progress(5, "TW opening source checked")

        def signal_progress(event: dict[str, Any]) -> None:
            raw_step = max(0, int(event.get("step") or 0))
            update_progress(
                min(progress_total - 1, raw_step + 5),
                str(event.get("message") or "signal generation"),
            )

        kwargs = _signal_kwargs(
            market=cfg.market,
            price_source="panel",
            scheduled=True,
            progress_callback=_ConsoleProgress(
                prefix=f"preopen:{cfg.market}",
                event_callback=signal_progress,
            ),
            progress_label=f"preopen:{cfg.market}",
        )
        kwargs.update(
            write=False,
            ensure_previous_signal=False,
            previous_signal_backfill_limit=0,
        )
        with _MODEL_INFERENCE_LOCK:
            result = generate_live_signal(**kwargs)
        symbols = _day_trade_opening_quote_symbols(result.weights_rows)
        result.summary["opening_source_prewarm"] = opening_source_prewarm
        # Compatibility alias for readers deployed before MIS became the
        # primary model-observation source.  It carries identical truthful
        # evidence and can be removed after all readers migrate.
        result.summary["tw_mis_fallback_prewarm"] = opening_source_prewarm
        update_progress(23, "opening source and model cache ready")
        fallback = _day_trade_opening_fallback_prices(
            result.weights_rows, symbols
        )
        result.summary["preopen_price_limits"] = prepare_tw_price_limit_snapshot(
            symbols,
            fallback,
            parquet_root=experiment.data.parquet_root,
            trading_date=datetime.now(
                ZoneInfo(cfg.timezone or "Asia/Taipei")
            ).date().isoformat(),
        )
        result.summary["same_session_eligibility"] = same_session_eligibility
        update_progress(progress_total, "price limits ready")
        _write_preopen_readiness(
            cfg,
            status="ready",
            started_at=started_at,
            elapsed_seconds=time.perf_counter() - started,
            summary=result.summary,
            step=progress_total,
            total=progress_total,
            message="ready",
        )
        return result
    except Exception as exc:
        _write_preopen_readiness(
            cfg,
            status="failed",
            started_at=started_at,
            elapsed_seconds=time.perf_counter() - started,
            error=f"{type(exc).__name__}: {exc}",
            step=progress_total,
            total=progress_total,
            message="failed",
        )
        raise


def _final_arm_market_signal_sync(cfg: LiveMarketConfig) -> LiveSignalResult:
    """Prove the panel/checkpoint/GPU model hot immediately before 09:00."""

    timezone_name = cfg.timezone or "Asia/Taipei"
    observed = datetime.now(ZoneInfo(timezone_name))
    session_date = observed.date().isoformat()
    if not _preopen_market_ready_for_session(cfg, session_date):
        return _prewarm_market_signal_sync(cfg)
    started = time.perf_counter()
    started_at = observed.isoformat(timespec="seconds")
    attempts = 0
    try:
        experiment = load_config(cfg.config_path)
        opening_source_prewarm = _warm_or_reuse_tw_mis_opening_receipt(
            parquet_root=experiment.data.parquet_root,
            session_date=session_date,
            force_probe=True,
        )
        opening_source_prewarm["run_id"] = _BOT_RUN_ID
        _sync_latest_live_weights_to_market_artifact(cfg)

        def run_once() -> LiveSignalResult:
            nonlocal attempts
            attempts += 1
            kwargs = _signal_kwargs(
                market=cfg.market,
                price_source="panel",
                scheduled=True,
                progress_label=f"final-arm:{cfg.market}",
            )
            kwargs.update(
                write=False,
                ensure_previous_signal=False,
                previous_signal_backfill_limit=0,
            )
            with _MODEL_INFERENCE_LOCK:
                return generate_live_signal(**kwargs)

        result = run_once()
        latency = dict(result.summary.get("live_latency") or {})
        cache_keys = (
            "panel_cache_hit",
            "checkpoint_cache_hit",
            "model_cache_hit",
        )
        if not all(latency.get(key) is True for key in cache_keys):
            # A cold first pass is allowed before the deadline; the second pass
            # is the evidence that 09:00 will not pay that cost again.
            result = run_once()
            latency = dict(result.summary.get("live_latency") or {})
        missing = [key for key in cache_keys if latency.get(key) is not True]
        if missing:
            raise RuntimeError(f"preopen final arm cache proof failed: {missing}")
        result.summary["opening_source_prewarm"] = opening_source_prewarm
        result.summary["tw_mis_fallback_prewarm"] = opening_source_prewarm
        _write_preopen_final_arm(
            cfg,
            status="ready",
            started_at=started_at,
            elapsed_seconds=time.perf_counter() - started,
            summary=result.summary,
            attempts=attempts,
        )
        return result
    except Exception as exc:
        _write_preopen_final_arm(
            cfg,
            status="failed",
            started_at=started_at,
            elapsed_seconds=time.perf_counter() - started,
            attempts=max(1, attempts),
            error=f"{type(exc).__name__}: {exc}",
        )
        raise


def _split_content_pages(content: str, *, max_chars: int = 1850) -> list[str]:
    text = str(content or "")
    if len(text) <= max_chars:
        return [text or "(empty)"]
    pages: list[str] = []
    current: list[str] = []
    current_len = 0
    for raw_line in text.splitlines():
        line = raw_line if len(raw_line) <= max_chars else raw_line[: max_chars - 3] + "..."
        extra = len(line) + (1 if current else 0)
        if current and current_len + extra > max_chars:
            pages.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line) + (1 if current_len else 0)
    if current:
        pages.append("\n".join(current))
    return pages or ["(empty)"]


async def _send_long_response(interaction: discord.Interaction, content: str) -> None:
    await _send_paginated_response(interaction, _split_content_pages(content))


async def _send_signal_response(interaction: discord.Interaction, content: str, signal_id: str, market: str) -> None:
    view = SignalReviewView(signal_id=signal_id, market=market)
    if len(content) <= 1900:
        await interaction.followup.send(content, view=view)
        return
    await interaction.followup.send(content[:1900], view=view)


def _symbol_label(row: dict) -> str:
    symbol = str(row.get("symbol", "") or "").strip()
    name = str(row.get("name", "") or "").strip()
    if name:
        return f"`{symbol}` {name}"
    return f"`{symbol}`"


def _position_status_label(row: dict[str, Any]) -> str:
    status = str(row.get("position_status") or "").strip().lower()
    if status == "locked_untradable":
        return "不可交易，已視為前一可交易日清算"
    if status == "untradable":
        return "不可交易"
    if status == "model_flattened_by_constraints":
        return "模型權重被交易限制歸零"
    constraint = str(row.get("constraint") or "").strip().lower()
    if constraint == "not_tradable":
        return "不可交易"
    if constraint == "buy_blocked":
        return "買進受限"
    if constraint == "sell_blocked":
        return "賣出受限"
    return ""


def _float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _pct(value: Any, digits: int = 2) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number * 100:.{digits}f}%"


def _signed_pct(value: Any, digits: int = 2) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number * 100:+.{digits}f}%"


def _signed_pct_zero_plain(value: Any, digits: int = 2) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    if abs(number) < 0.5 * (10 ** (-(digits + 2))):
        return f"{0.0:.{digits}f}%"
    return f"{number * 100:+.{digits}f}%"


def _num(value: Any, digits: int = 3) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:.{digits}f}"


def _signed_num(value: Any, digits: int = 3) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:+.{digits}f}"


def _money(value: Any, digits: int = 0) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:,.{digits}f}"


def _signed_money(value: Any, digits: int = 0) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:+,.{digits}f}"


def _price(value: Any) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:.2f}"


def _page_size(
    value: int | None,
    *,
    min_rows: int = MIN_DISCORD_ROWS,
    default: int = 20,
    max_rows: int = 40,
) -> int:
    try:
        number = int(value or default)
    except Exception:
        number = default
    return max(int(min_rows), min(int(max_rows), number))


def _top_n(value: int | None) -> int:
    try:
        number = int(value or 20)
    except Exception:
        number = 20
    return max(MIN_DISCORD_ROWS, number)


def _append_investment_warning(lines: list[str]) -> list[str]:
    if INVESTMENT_WARNING not in lines:
        lines.extend(["", INVESTMENT_WARNING])
    return lines


def _limit_rows(rows: list[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    try:
        count = int(limit or 0)
    except Exception:
        count = 0
    if count <= 0:
        return rows
    return rows[:count]


def _row_abs(row: dict[str, Any], key: str) -> float:
    number = _float_or_none(row.get(key))
    return abs(number) if number is not None else 0.0


def _row_raw_score(row: dict[str, Any]) -> Any:
    raw_score = _float_or_none(row.get("raw_score"))
    return raw_score if raw_score is not None else row.get("score")


def _normalize_signal_now_mode(value: str | None) -> str:
    normalized = str(value or "signal").strip().lower().replace("-", "_")
    if normalized in {"signal", "trade", "trading", "交易訊號"}:
        return "signal"
    if normalized in {"raw", "raw_score", "raw_scores", "original", "原始分數"}:
        return "raw_scores"
    raise BotUserError("mode 必須是 signal 或 raw_scores。")


def _row_position_weight(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key in row:
            value = _float_or_none(row.get(key))
            if value is not None:
                return value
    return None


def _position_adjusted_return(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    precomputed = _float_or_none(row.get("stock_return"))
    if precomputed is not None:
        return precomputed
    raw_return = _float_or_none(row.get("price_return"))
    if raw_return is None:
        return None
    weight = _row_position_weight(row, keys)
    if weight is None:
        return None
    if abs(weight) < 1e-12:
        return 0.0
    return raw_return if weight > 0.0 else -raw_return


def _portfolio_return_contribution(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    precomputed = _float_or_none(row.get("portfolio_contribution"))
    if precomputed is not None:
        return precomputed
    raw_return = _float_or_none(row.get("price_return"))
    if raw_return is None:
        return None
    weight = _row_position_weight(row, keys)
    if weight is None:
        return None
    return weight * raw_return


def _return_pnl_line(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    return _kv_line(
        ("stock_ret", _signed_pct_zero_plain(_position_adjusted_return(row, keys))),
        ("pnl_contrib", _signed_pct_zero_plain(_portfolio_return_contribution(row, keys))),
    )


def _resolve_history_capital_args(
    cfg: LiveMarketConfig,
    *,
    initial_capital: float | None = None,
    current_capital: float | None = None,
) -> tuple[float | None, float | None]:
    current = positive_float_or_none(current_capital) or _market_current_capital(cfg)
    initial = positive_float_or_none(initial_capital) or _market_initial_capital(cfg)
    return initial, current


def _resolve_current_capital(
    cfg: LiveMarketConfig,
    *,
    current_capital: float | None = None,
) -> float | None:
    return positive_float_or_none(current_capital) or _market_current_capital(cfg) or _market_initial_capital(cfg)


def _performance_window_label(cfg: LiveMarketConfig, recent: dict[str, Any]) -> str:
    configured_window = getattr(cfg, "benchmark_window_days", 32)
    raw_window = recent.get("window_days") or configured_window
    try:
        window = int(raw_window)
    except Exception:
        window = int(configured_window)
    frequency = str(getattr(cfg, "history_frequency", "daily") or "").strip().lower()
    if frequency in {
        "bar", "bars", "intraday", "1m", "1min", "1minute", "1minutes",
        "15m", "15min", "15minute", "15minutes",
    }:
        try:
            market_cfg = _load_experiment_config_cached(str(_resolve_repo_path(cfg.config_path) or Path(cfg.config_path)))
            trading_frequency = str(getattr(market_cfg.trading, "frequency", "") or "").strip()
        except Exception:
            trading_frequency = ""
        suffix = f"根{trading_frequency}" if trading_frequency else "根K"
        return f"過去{window}{suffix}"
    return f"過去{window}天"


def _rewrite_signal_artifacts(result: Any) -> None:
    output_dir = result.output_dir or result.summary.get("output_dir")
    if not output_dir:
        return
    path = _resolve_repo_path(str(output_dir)) or Path(str(output_dir))
    if not path.exists():
        return
    (path / "summary.json").write_text(
        json.dumps(result.summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (path / "discord_message.md").write_text(result.message, encoding="utf-8")


def _enrich_signal_performance_for_discord(
    cfg: LiveMarketConfig,
    result: Any,
    *,
    max_rows: int,
    current_capital: float | None = None,
    debug: bool = False,
) -> Any:
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    summary = result.summary
    if capital is not None:
        summary["display_capital"] = float(capital)
        portfolio_return = _float_or_none(summary.get("portfolio_simple_return"))
        benchmark_return = _float_or_none(summary.get("benchmark_simple_return"))
        if portfolio_return is not None:
            summary["portfolio_pnl_value"] = portfolio_return * float(capital)
        if benchmark_return is not None:
            summary["benchmark_pnl_value"] = benchmark_return * float(capital)
        if portfolio_return is not None and benchmark_return is not None:
            summary["excess_pnl_value"] = (portfolio_return - benchmark_return) * float(capital)

    _refresh_summary_recent_performance_from_history(cfg, summary, capital=capital)

    recent = summary.get("recent_performance")
    if isinstance(recent, dict):
        recent["window_label"] = _performance_window_label(cfg, recent)
        if capital is not None:
            for source_key, target_key in (
                ("strategy_return", "strategy_pnl_value"),
                ("benchmark_return", "benchmark_pnl_value"),
                ("excess_return", "excess_pnl_value"),
            ):
                value = _float_or_none(recent.get(source_key))
                if value is not None:
                    recent[target_key] = value * float(capital)

    result.message = format_signal_message(summary, max_rows=max_rows, debug=debug)
    try:
        _rewrite_signal_artifacts(result)
    except Exception as exc:
        _log_exception(f"rewrite_signal_artifacts:{cfg.market}", exc)
    return result


def _refresh_summary_recent_performance_from_history(
    cfg: LiveMarketConfig,
    summary: dict[str, Any],
    *,
    capital: float | None = None,
) -> None:
    raw_recent = summary.get("recent_performance")
    if isinstance(raw_recent, dict):
        window = _float_or_none(raw_recent.get("window_days"))
    else:
        window = None
    if window is None:
        window = _float_or_none(getattr(cfg, "benchmark_window_days", None))
    try:
        days = int(window or 0)
    except Exception:
        days = 0
    if days <= 0:
        return
    try:
        recent_fast = _recent_performance_from_returns(cfg, days, capital=capital)
    except MarketUnsupportedError:
        recent_fast = None
    except Exception as exc:
        _log_exception(f"recent_performance_history:{cfg.market}", exc)
        recent_fast = None
    if recent_fast is None:
        try:
            history = _load_portfolio_history_for_market(
                cfg,
                days,
                0,
                0.0,
                None,
                capital,
            )
        except MarketUnsupportedError:
            return
        except Exception as exc:
            _log_exception(f"recent_performance_history_fallback:{cfg.market}", exc)
            return
        recent_fast = {
            "window_days": int(history.days),
            "strategy_return": history.period_return,
            "benchmark_return": history.benchmark_return,
            "excess_return": (
                None
                if history.period_return is None or history.benchmark_return is None
                else float(history.period_return) - float(history.benchmark_return)
            ),
            "source": "portfolio_history_with_live_signals",
            "start_date": history.start_date,
            "end_date": history.end_date,
        }
        history_rows = sorted(
            list(getattr(history, "rows", []) or []),
            key=lambda row: _history_sort_dt(row.get("date")),
        )
        recent_fast.update(
            _risk_adjusted_metrics_from_simple_returns(
                [_float_or_none(row.get("portfolio_return")) for row in history_rows]
            )
        )
        if capital is not None:
            for source_key, target_key in (
                ("strategy_return", "strategy_pnl_value"),
                ("benchmark_return", "benchmark_pnl_value"),
                ("excess_return", "excess_pnl_value"),
            ):
                value = _float_or_none(recent_fast.get(source_key))
                if value is not None:
                    recent_fast[target_key] = value * float(capital)
    recent: dict[str, Any] = dict(raw_recent) if isinstance(raw_recent, dict) else {}
    recent.update(recent_fast)
    summary["recent_performance"] = recent


def _returns_artifact_path(fold_dir: Path) -> Path | None:
    for name in (
        "integer_share_daily_portfolio_returns.parquet",
        "integer_share_daily_portfolio_returns.csv",
        "daily_portfolio_returns.parquet",
        "daily_portfolio_returns.csv",
    ):
        path = fold_dir / name
        if path.exists():
            return path
    return None


def _history_sort_dt(value: Any) -> datetime:
    return _history_datetime(value) or datetime.min


def _compound_return_values(values: list[float | None]) -> float | None:
    total = 1.0
    seen = False
    for value in values:
        number = _float_or_none(value)
        if number is None:
            continue
        total *= 1.0 + number
        seen = True
    return total - 1.0 if seen else None


def _risk_adjusted_metrics_from_simple_returns(
    values: list[float | None],
    *,
    annualization_periods: int = 252,
) -> dict[str, Any]:
    """Match the project's canonical log-return risk metric definitions."""
    simple = np.asarray(
        [number for value in values if (number := _float_or_none(value)) is not None],
        dtype=np.float64,
    )
    periods = max(1, int(annualization_periods))
    if simple.size == 0:
        return {}
    log_returns = np.log1p(np.clip(simple, -0.999999, None))
    average = float(log_returns.mean())
    volatility = float(log_returns.std(ddof=0))
    downside = np.minimum(log_returns, 0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside))))
    cumulative_log = np.cumsum(log_returns)
    cumulative_with_initial = np.concatenate((np.zeros(1, dtype=np.float64), cumulative_log))
    running_peak = np.maximum.accumulate(cumulative_with_initial)[1:]
    drawdowns = np.expm1(np.clip(cumulative_log - running_peak, -745.0, 0.0))
    max_drawdown = float(drawdowns.min(initial=0.0))
    annualized_return = float(np.expm1(np.clip(average * periods, -745.0, 709.0)))
    return {
        "sharpe": float(average / volatility * math.sqrt(periods)) if volatility > 0.0 else 0.0,
        "sortino": (
            float(average / downside_deviation * math.sqrt(periods))
            if downside_deviation > 0.0
            else 0.0
        ),
        "max_drawdown": max_drawdown,
        "calmar": annualized_return / abs(max_drawdown) if max_drawdown < 0.0 else 0.0,
        "annualized_return": annualized_return,
        "risk_observations": int(simple.size),
        "risk_annualization_periods": periods,
        "risk_return_basis": "net_log_return",
    }


def _recent_performance_from_returns(
    cfg: LiveMarketConfig,
    periods: int,
    *,
    capital: float | None = None,
) -> dict[str, Any] | None:
    import polars as pl

    try:
        limit = max(1, int(periods))
    except Exception:
        limit = 32
    fold_dir = _market_fold_dir(cfg)
    path = _returns_artifact_path(fold_dir)
    rows: list[dict[str, Any]] = []
    source_paths: list[Path] = []
    if path is not None:
        source_paths.append(path)
        columns = ["date", "portfolio_return", "benchmark_return"]
        if path.suffix == ".parquet":
            frame = pl.scan_parquet(path).select([pl.col(name) for name in columns if name]).tail(limit * 2).collect()
        else:
            frame = pl.read_csv(path, columns=columns, infer_schema_length=10000).tail(limit * 2)
        for row in frame.select(columns).to_dicts():
            date_key = _date_key(row.get("date"))
            if not date_key:
                continue
            rows.append(
                {
                    "date": date_key,
                    "portfolio_return": _float_or_none(row.get("portfolio_return")),
                    "benchmark_return": _float_or_none(row.get("benchmark_return")),
                    "source": "returns_artifact",
                }
            )

    live_by_date: dict[str, tuple[Path, dict[str, Any]]] = {}
    for summary_path, summary in _recent_market_signal_metrics(cfg, max_summaries=max(limit * 4, 64)):
        if bool(summary.get("execution_preview_only")):
            continue
        date_key = _date_key(
            summary.get("panel_data_date")
            or summary.get("weights_date")
            or summary.get("panel_date")
            or summary.get("asof_date")
        )
        if not date_key:
            continue
        current = live_by_date.get(date_key)
        if current is None or summary_path.stat().st_mtime >= current[0].stat().st_mtime:
            live_by_date[date_key] = (summary_path, summary)

    settled_by_date = {str(row["date"]): row for row in rows}
    cursor = max(settled_by_date, default=None)
    for date_key, (summary_path, summary) in sorted(live_by_date.items()):
        if date_key in settled_by_date:
            continue
        previous_key = _date_key(
            summary.get("previous_weights_data_date")
            or summary.get("previous_weights_date")
            or summary.get("drift_base_data_date")
            or summary.get("drift_base_date")
        )
        if cursor is not None and previous_key != cursor:
            continue
        source_paths.append(summary_path)
        settled_by_date[date_key] = {
            "date": date_key,
            "portfolio_return": _float_or_none(summary.get("portfolio_simple_return")),
            "benchmark_return": _float_or_none(summary.get("benchmark_simple_return")),
            "source": "live_signal_summary",
        }
        cursor = date_key

    selected = sorted(
        settled_by_date.values(),
        key=lambda row: _history_sort_dt(row.get("date")),
    )[-limit:]
    if not selected:
        return None
    strategy = _compound_return_values([_float_or_none(row.get("portfolio_return")) for row in selected])
    benchmark = _compound_return_values([_float_or_none(row.get("benchmark_return")) for row in selected])
    excess = None if strategy is None or benchmark is None else strategy - benchmark
    out: dict[str, Any] = {
        "window_days": len(selected),
        "strategy_return": strategy,
        "benchmark_return": benchmark,
        "excess_return": excess,
        "source": "returns_artifact_with_live_signals",
        "start_date": str(selected[0].get("date")),
        "end_date": str(selected[-1].get("date")),
    }
    out.update(
        _risk_adjusted_metrics_from_simple_returns(
            [_float_or_none(row.get("portfolio_return")) for row in selected]
        )
    )
    if source_paths:
        out["source_path"] = str(source_paths[0])
    if capital is not None:
        for source_key, target_key in (
            ("strategy_return", "strategy_pnl_value"),
            ("benchmark_return", "benchmark_pnl_value"),
            ("excess_return", "excess_pnl_value"),
        ):
            value = _float_or_none(out.get(source_key))
            if value is not None:
                out[target_key] = value * float(capital)
    return out


def _annotate_weight_rows_with_capital(rows: list[dict[str, Any]], capital: float | None) -> list[dict[str, Any]]:
    amount = positive_float_or_none(capital)
    if amount is None:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        for source_key, target_key in (
            ("target_weight", "target_value"),
            ("current_weight", "current_value"),
            ("delta_weight", "delta_value"),
        ):
            number = _float_or_none(enriched.get(source_key))
            if number is not None:
                enriched[target_key] = number * amount
        out.append(enriched)
    return out


def _capital_context_text(*, capital: Any = None, initial_capital: Any = None, current_capital: Any = None) -> str:
    parts: list[str] = []
    amount = positive_float_or_none(capital)
    initial = positive_float_or_none(initial_capital)
    current = positive_float_or_none(current_capital)
    if amount is not None:
        parts.append(f"capital={_money(amount)}")
    if current is not None:
        parts.append(f"current={_money(current)}")
    if initial is not None:
        parts.append(f"initial={_money(initial)}")
    return " ".join(parts) if parts else "artifact capital"


def _summary_with_capital_context(
    cfg: LiveMarketConfig,
    summary: dict[str, Any],
    *,
    current_capital: float | None = None,
) -> dict[str, Any]:
    out = dict(summary)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    if capital is not None:
        out["display_capital"] = float(capital)
        portfolio_return = _float_or_none(out.get("portfolio_simple_return"))
        benchmark_return = _float_or_none(out.get("benchmark_simple_return"))
        if portfolio_return is not None:
            out["portfolio_pnl_value"] = portfolio_return * float(capital)
        if benchmark_return is not None:
            out["benchmark_pnl_value"] = benchmark_return * float(capital)
        if portfolio_return is not None and benchmark_return is not None:
            out["excess_pnl_value"] = (portfolio_return - benchmark_return) * float(capital)
    _refresh_summary_recent_performance_from_history(cfg, out, capital=capital)
    recent = out.get("recent_performance")
    if isinstance(recent, dict):
        recent = dict(recent)
        recent["window_label"] = _performance_window_label(cfg, recent)
        if capital is not None:
            for source_key, target_key in (
                ("strategy_return", "strategy_pnl_value"),
                ("benchmark_return", "benchmark_pnl_value"),
                ("excess_return", "excess_pnl_value"),
            ):
                value = _float_or_none(recent.get(source_key))
                if value is not None:
                    recent[target_key] = value * float(capital)
        out["recent_performance"] = recent
    return out


def _config_trading_limits(cfg: LiveMarketConfig) -> tuple[float | None, float | None]:
    try:
        market_cfg = _load_experiment_config_cached(str(_resolve_repo_path(cfg.config_path) or Path(cfg.config_path)))
    except Exception:
        return None, None
    # Canonical train/eval/live execution normalizes realised gross exposure to
    # 1.0. reporting_leverage is a separate plot-only scenario multiplier and
    # must not weaken live exposure sanity checks.
    gross = 1.0
    turnover = _float_or_none(getattr(market_cfg.trading, "max_turnover_ratio", None))
    # Canonical execution uses zero as the sentinel for "turnover cap disabled".
    # Do not reinterpret that sentinel as a literal 0% live-sanity ceiling.
    if turnover is not None and turnover <= 0.0:
        turnover = None
    return gross, turnover


def _signal_sanity_issues(cfg: LiveMarketConfig, summary: dict[str, Any]) -> list[tuple[str, str]]:
    issues: list[tuple[str, str]] = []

    def add(severity: str, text: str) -> None:
        issues.append((severity, text))

    for key, label in (
        ("asof_date", "signal time"),
        ("panel_date", "panel time"),
    ):
        if not str(summary.get(key) or "").strip():
            add("block", f"missing {label}")

    for key, label, warn_abs, block_abs in (
        ("portfolio_simple_return", "portfolio return", 0.20, 0.50),
        ("benchmark_simple_return", "baseline return", 0.20, 0.50),
    ):
        raw = summary.get(key)
        value = _float_or_none(raw)
        if raw is not None and value is None:
            add("block", f"{label} is not finite")
            continue
        if value is None:
            continue
        if abs(value) > block_abs:
            add("block", f"{label} {_signed_pct(value)} exceeds {_pct(block_abs)}")
        elif abs(value) > warn_abs:
            add("warn", f"{label} {_signed_pct(value)} is unusually large")

    recent = summary.get("recent_performance")
    if isinstance(recent, dict):
        for key, label in (
            ("strategy_return", "recent strategy return"),
            ("benchmark_return", "recent baseline return"),
        ):
            raw = recent.get(key)
            value = _float_or_none(raw)
            if raw is not None and value is None:
                add("block", f"{label} is not finite")
                continue
            if value is None:
                continue
            if abs(value) > 5.0:
                add("block", f"{label} {_signed_pct(value)} is implausible")
            elif abs(value) > 1.0:
                add("warn", f"{label} {_signed_pct(value)} is unusually large")

    gross_limit, turnover_limit = _config_trading_limits(cfg)
    risk = summary.get("target_risk") if isinstance(summary.get("target_risk"), dict) else {}
    gross = _float_or_none(risk.get("gross"))
    if gross is not None and gross_limit is not None:
        if gross > gross_limit * 1.25:
            add("block", f"gross exposure {_pct(gross)} exceeds configured limit {_pct(gross_limit)}")
        elif gross > gross_limit * 1.05:
            add("warn", f"gross exposure {_pct(gross)} is near configured limit {_pct(gross_limit)}")
    top_abs = _float_or_none(risk.get("top_abs_weight"))
    if top_abs is not None:
        if top_abs > 0.80:
            add("block", f"top position {_pct(top_abs)} is too concentrated")
        elif top_abs > 0.25:
            add("warn", f"top position {_pct(top_abs)} is concentrated")

    turnover = _float_or_none(summary.get("turnover"))
    if turnover is not None and turnover_limit is not None:
        if turnover > turnover_limit * 1.20:
            add("block", f"turnover {_pct(turnover)} exceeds configured limit {_pct(turnover_limit)}")
        elif turnover > turnover_limit * 0.80:
            add("warn", f"turnover {_pct(turnover)} is high")
    cost = _float_or_none(summary.get("estimated_trade_cost"))
    if cost is not None:
        if cost > 0.20:
            add("block", f"estimated fees {_pct(cost)} are implausibly high")
        elif cost > 0.05:
            add("warn", f"estimated fees {_pct(cost)} are high")

    for item in summary.get("risk_warnings", []) if isinstance(summary.get("risk_warnings"), list) else []:
        text = str(item or "").strip()
        if text:
            add("warn", text)
    return issues


def _signal_sanity_level(issues: list[tuple[str, str]]) -> str:
    if any(severity == "block" for severity, _ in issues):
        return "BLOCK"
    if issues:
        return "WARN"
    return "OK"


def _signal_sanity_line(issues: list[tuple[str, str]]) -> str:
    level = _signal_sanity_level(issues)
    if level == "OK":
        return "sanity=`OK`"
    shown = " | ".join(text for _, text in issues[:4])
    return f"sanity=`{level}` issues=`{shown}`"


def _signal_sanity_message(cfg: LiveMarketConfig, summary: dict[str, Any], issues: list[tuple[str, str]]) -> str:
    lines = [
        f"**signal sanity gate** {cfg.label}",
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
        ),
        _kv_line(("sanity", _signal_sanity_level(issues))),
        "訊號可能異常，但仍會提供完整訊號；請人工確認資料連續性、報酬與風險後再使用。",
        "issues:",
    ]
    lines.extend(f"- {severity}: {text}" for severity, text in issues)
    _append_investment_warning(lines)
    return "\n".join(lines)


def _prepend_sanity_notice(content: str, cfg: LiveMarketConfig, summary: dict[str, Any]) -> str:
    issues = _signal_sanity_issues(cfg, summary)
    if not issues:
        return content
    lines = [
        f"**sanity {_signal_sanity_level(issues)}**",
        _signal_sanity_line(issues),
        "notice: 訊號可能異常，但以下仍提供完整訊號；請人工確認資料連續性、報酬與風險。",
        "",
        content,
    ]
    return "\n".join(lines)


def _shorten(text: Any, max_chars: int = 220) -> str:
    value = str(text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[: max(0, max_chars - 3)].rstrip() + "..."


def _kv_line(*pairs: tuple[str, Any]) -> str:
    return "  " + _kv_inline(*pairs)


def _kv_inline(*pairs: tuple[str, Any]) -> str:
    return "  ".join(f"`{key}={value}`" for key, value in pairs)


def _cfg_display_timezone(cfg: LiveMarketConfig) -> str:
    return str(getattr(cfg, "display_timezone", None) or DEFAULT_DISPLAY_TIMEZONE)


def _display_cfg_time(cfg: LiveMarketConfig, value: Any) -> str:
    return format_display_time(
        value,
        source_timezone=getattr(cfg, "timezone", None),
        display_timezone=_cfg_display_timezone(cfg),
    )


def _display_summary_time(summary: dict[str, Any], value: Any) -> str:
    return format_display_time(
        value,
        source_timezone=summary.get("data_timezone") or summary.get("timezone"),
        display_timezone=summary.get("display_timezone") or DEFAULT_DISPLAY_TIMEZONE,
    )


def _display_tz_text(cfg: LiveMarketConfig) -> str:
    return display_timezone_label(_cfg_display_timezone(cfg))


def _line_pages(
    *,
    title: str,
    rows: list[dict[str, Any]],
    formatter,
    page_size: int,
    header_lines: list[str] | None = None,
    min_page_size: int = MIN_DISCORD_ROWS,
    default_page_size: int = 20,
) -> list[str]:
    size = _page_size(page_size, min_rows=min_page_size, default=default_page_size)
    total = len(rows)
    if total == 0:
        return [f"**{title}**\n(no rows)\n\n{INVESTMENT_WARNING}"]
    max_chars = 1850
    blocks = [formatter(row) for row in rows]

    def render_page(page_index: int, page_count: int, start: int, chunk: list[str]) -> str:
        lines = [
            f"**{title}**",
            f"`page {page_index}/{page_count}`  `rows {start + 1}-{start + len(chunk)}/{total}`",
        ]
        if header_lines:
            lines.extend(header_lines)
        for block in chunk:
            lines.append("")
            lines.append(block)
        _append_investment_warning(lines)
        return "\n".join(lines)

    groups: list[tuple[int, list[str]]] = []
    start = 0
    current: list[str] = []
    for index, block in enumerate(blocks):
        candidate = current + [block]
        candidate_text = render_page(999, 999, start, candidate)
        if current and (len(candidate) > size or len(candidate_text) > max_chars):
            groups.append((start, current))
            start = index
            current = [block]
        else:
            current = candidate
    if current:
        groups.append((start, current))

    pages: list[str] = []
    page_count = len(groups)
    for page_index, (start, chunk) in enumerate(groups, start=1):
        pages.extend(_split_content_pages(render_page(page_index, page_count, start, chunk), max_chars=max_chars))
    return pages


def _discord_page_kwargs(page: str) -> dict[str, Any]:
    """Use an embed for a single logical page that exceeds message content limits."""

    content = str(page or "(empty)")
    if len(content) <= 1900:
        return {"content": content, "embed": None}
    if len(content) <= 4000:
        return {"content": None, "embed": discord.Embed(description=content)}
    raise RuntimeError(f"Discord page exceeds embed limit ({len(content)}>4000)")


async def _send_paginated_response(interaction: discord.Interaction, pages: list[str]) -> None:
    clean_pages = [page if page else "(empty)" for page in pages] or ["(empty)"]
    view = PagedTextView(clean_pages) if len(clean_pages) > 1 else None
    kwargs = _discord_page_kwargs(clean_pages[0])
    if view is not None:
        kwargs["view"] = view
    await interaction.followup.send(**kwargs)


async def _send_channel_pages(channel: Any, pages: list[str], *, timeout: float | None = 24 * 60 * 60) -> None:
    clean_pages = [page if page else "(empty)" for page in pages] or ["(empty)"]
    view = PagedTextView(clean_pages, timeout=timeout) if len(clean_pages) > 1 else None
    kwargs = _discord_page_kwargs(clean_pages[0])
    if view is not None:
        kwargs["view"] = view
    await channel.send(**kwargs)


def _active_position_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            max(_row_abs(row, "current_weight"), _row_abs(row, "target_weight"), _row_abs(row, "delta_weight")),
            _row_abs(row, "delta_weight"),
            _row_abs(row, "score"),
        ),
        reverse=True,
    )
    return [
        row
        for row in sorted_rows
        if _row_abs(row, "current_weight") >= MIN_DISPLAY_ABS_WEIGHT
        or _row_abs(row, "target_weight") >= MIN_DISPLAY_ABS_WEIGHT
        or _row_abs(row, "delta_weight") >= MIN_DISPLAY_ABS_WEIGHT
    ]


def _scheduled_detail_page_groups(
    cfg: LiveMarketConfig,
    result: Any,
    *,
    title_prefix: str = "scheduled",
    include_decisions: bool = False,
    positions_only: bool = False,
    max_rows: int | None = None,
    debug: bool = False,
) -> list[list[str]]:
    capital = _resolve_current_capital(cfg)
    summary = result.summary
    output_dir = summary.get("output_dir") or result.output_dir
    output_text = _display_path(Path(output_dir)) if output_dir else "n/a"
    header_pairs = [
        ("market", summary.get("market", cfg.market)),
        ("signal", summary.get("signal_id", "n/a")),
        ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
        ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
        ("price", summary.get("price_source", "n/a")),
    ]
    if summary.get("price_timestamp"):
        header_pairs.append(("price_time", _display_summary_time(summary, summary.get("price_timestamp"))))
    common_header = [
        _kv_line(*header_pairs),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "欄位: `raw_score`=模型未置中原始分數；`score`=置中後排序分數。",
    ]
    if debug:
        common_header.extend(
            [
                _kv_line(
                    ("signal", summary.get("signal_id", "n/a")),
                    ("display_tz", summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"output: `{output_text}`",
            ]
        )

    position_rows = _annotate_weight_rows_with_capital(_active_position_rows(list(result.weights_rows)), capital)
    rebalance_rows = _annotate_weight_rows_with_capital(list(result.rebalance_rows), capital)
    decision_rows = _sort_decision_rows(
        _filter_decision_rows(
            list(getattr(result, "decision_rows", [])),
            action="actionable",
            actionable_only=True,
        ),
        "delta",
    )
    # /signal_now day-trade positions are the complete actionable portfolio.
    # Keep every active row and let PagedTextView expose later pages; top_n is
    # still respected by the non-day-trade/scheduled detail templates.
    position_rows = _limit_rows(position_rows, None if positions_only else max_rows)
    rebalance_rows = _limit_rows(rebalance_rows, max_rows)
    decision_rows = _limit_rows(decision_rows, max_rows)

    position_header = [
        *common_header,
        _kv_line(("rows", len(position_rows)), ("sort", "abs now/target/delta")),
    ]
    if debug:
        position_header.append(f"full: `{summary.get('positions_markdown_path', summary.get('weights_path', 'n/a'))}`")
    rebalance_header = [
        *common_header,
        _kv_line(("rows", len(rebalance_rows)), ("threshold", cfg.min_abs_delta), ("sort", "abs delta")),
    ]
    if debug:
        rebalance_header.append(f"full: `{summary.get('rebalance_markdown_path', summary.get('rebalance_path', 'n/a'))}`")
    decision_full_path = (
        summary.get("decision_report_path")
        or summary.get("decision_explanation_markdown_path")
        or summary.get("decision_explanation_path")
        or "n/a"
    )
    decision_header = [
        *common_header,
        _kv_line(("rows", len(decision_rows)), ("filter", "actionable"), ("sort", "abs delta")),
    ]
    if debug:
        decision_header.append(f"full: `{decision_full_path}`")

    groups = [
        _line_pages(
            title=f"{title_prefix} current / target positions",
            rows=position_rows,
            formatter=_position_line,
            page_size=20,
            header_lines=position_header,
        )
    ]
    if not positions_only:
        groups.append(
            _line_pages(
                title=f"{title_prefix} rebalance",
                rows=rebalance_rows,
                formatter=_rebalance_line,
                page_size=20,
                header_lines=rebalance_header,
            )
        )
    if include_decisions and not positions_only:
        groups.append(
            _line_pages(
                title=f"{title_prefix} decision explanations",
                rows=decision_rows,
                formatter=_decision_block,
                page_size=10,
                header_lines=decision_header,
            )
        )
    return groups


def _raw_score_line(row: dict[str, Any]) -> str:
    return "\n".join(
        [
            _symbol_label(row),
            _kv_line(
                ("raw_score", _num(_row_raw_score(row), 6)),
                ("abs_rank", row.get("abs_raw_score_rank", "n/a")),
                ("px", _price(row.get("current_price"))),
            ),
            _kv_line(
                ("alive", row.get("alive", "n/a")),
                ("tradable", row.get("tradable", "n/a")),
                ("can_buy", row.get("can_buy", "n/a")),
                ("can_sell", row.get("can_sell", "n/a")),
            ),
        ]
    )


def _raw_score_pages(
    cfg: LiveMarketConfig,
    result: Any,
    *,
    title_prefix: str = "signal_now",
    debug: bool = False,
) -> list[str]:
    summary = result.summary
    source_rows = [dict(row) for row in list(getattr(result, "weights_rows", []))]
    rows = sorted(
        source_rows,
        key=lambda row: (
            _row_abs(row, "raw_score"),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["abs_raw_score_rank"] = index
    contract = summary.get("score_contract") if isinstance(summary.get("score_contract"), dict) else {}
    header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("signal", summary.get("signal_id", "n/a")),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
        ),
        _kv_line(
            ("rows", len(rows)),
            ("scope", contract.get("raw_score_scope", "n/a")),
            ("filter", "none"),
            ("sort", "abs(raw_score)"),
        ),
        "此模式對完整 checkpoint 股票 universe 使用全 True 模型 mask；交易資格、漲跌停與買賣限制只標示，不過濾也不改寫 raw_score。",
    ]
    if debug:
        header.append(
            f"full: `{summary.get('positions_markdown_path', summary.get('weights_path', 'n/a'))}`"
        )
    return _line_pages(
        title=f"{title_prefix} unconstrained raw scores",
        rows=rows,
        formatter=_raw_score_line,
        page_size=12,
        header_lines=header,
        min_page_size=1,
        default_page_size=12,
    )


def _signal_now_detail_page_groups(
    cfg: LiveMarketConfig,
    result: Any,
    *,
    mode: str,
    top_n: int | None = None,
    debug: bool = False,
) -> list[list[str]]:
    if _normalize_signal_now_mode(mode) == "raw_scores":
        return [_raw_score_pages(cfg, result, title_prefix="raw_score_now", debug=debug)]
    execution_mode = str(result.summary.get("execution_mode") or "").strip().lower()
    market_id = str(result.summary.get("market") or cfg.market or "").strip().lower()
    is_day_trade = execution_mode == "tw_day_trade" or market_id.startswith("tw_day_trade")
    return _scheduled_detail_page_groups(
        cfg,
        result,
        title_prefix="signal_now",
        include_decisions=not is_day_trade,
        positions_only=is_day_trade,
        max_rows=top_n,
        debug=debug,
    )


def _status_line(key: str, cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> str:
    checkpoint = status.checkpoint
    fold = checkpoint.fold_id if checkpoint is not None and checkpoint.fold_id is not None else "none"
    mtime = checkpoint.mtime if checkpoint is not None else "none"
    test_years = ",".join(str(x) for x in checkpoint.test_years) if checkpoint is not None and checkpoint.test_years else "n/a"
    best_metric = checkpoint.best_metric if checkpoint is not None and checkpoint.best_metric else "n/a"
    enabled = "enabled" if status.enabled else "disabled"
    return (
        f"`{key}` {cfg.label} status=`{status.status}` {enabled} "
        f"data=`{_display_cfg_time(cfg, status.data.last_data_date or 'n/a')}` "
        f"panel=`{_display_cfg_time(cfg, status.data.panel_date or 'n/a')}` "
        f"benchmark=`{_display_cfg_time(cfg, status.data.benchmark_date or 'n/a')}` "
        f"expected=`{_display_cfg_time(cfg, status.data.expected_latest_date or 'n/a')}` "
        f"fold=`{fold}` ckpt_mtime=`{mtime}` test=`{test_years}` metric=`{best_metric}`"
    )


def _health_lines(market: str = "") -> list[str]:
    configs = _market_configs()
    if market:
        cfg = _resolve_market(market)
        status = _runtime_status_for_display(cfg)
        return [
            "**stockAgent bot health**",
            f"markets=`{', '.join(sorted(configs))}` default=`{_default_market()}`",
            _status_line(cfg.market, cfg, status),
            f"config=`{status.config_path}` config_hash=`{status.config_fingerprint or 'n/a'}`",
            f"output_dir=`{status.output_dir or 'config default'}` live_output_dir=`{cfg.live_output_dir or 'auto'}`",
            f"market_open=`{status.market_open}` reason=`{status.market_open_reason or 'ok'}`",
            f"schedule_time=`{_market_schedule_time(cfg)}` interval=`{_market_schedule_interval_minutes(cfg) or 'off'}` "
            f"delay_s=`{_market_schedule_delay_seconds(cfg)}` summary_time=`{_market_summary_time(cfg) or 'off'}` "
            f"data_tz=`{cfg.timezone}` display_tz=`{_display_tz_text(cfg)}`",
            f"capital initial=`{_money(_market_initial_capital(cfg))}` current=`{_money(_market_current_capital(cfg))}`",
        ]
    lines = [
        "**stockAgent bot health**",
        f"markets=`{', '.join(sorted(configs))}` default=`{_default_market()}`",
    ]
    for key, cfg in sorted(configs.items()):
        lines.append(_status_line(key, cfg, _runtime_status_for_display(cfg)))
    return lines


def _markets_lines() -> list[str]:
    lines = ["**stockAgent markets**"]
    for key, cfg in sorted(_market_configs().items()):
        fold = cfg.fold_id if cfg.fold_id is not None else "latest"
        runtime = _runtime_status_for_display(cfg)
        lines.append(
            f"`{key}` {cfg.label} status=`{runtime.status}` enabled=`{runtime.enabled}` "
            f"data=`{_display_cfg_time(cfg, runtime.data.last_data_date or 'n/a')}` schedule=`{_market_schedule_time(cfg)}` "
            f"interval=`{_market_schedule_interval_minutes(cfg) or 'off'}` "
            f"display_tz=`{_display_tz_text(cfg)}` config=`{cfg.config_path}` "
            f"output=`{cfg.output_dir or 'config default'}` fold=`{fold}`"
        )
    return lines


def _find_signal_summary(signal_id: str) -> tuple[Path, dict[str, Any]] | None:
    target = str(signal_id).strip()
    if not target:
        return None
    roots: list[Path] = []
    for cfg in _market_configs().values():
        cfg = _effective_market_config(cfg)
        if cfg.live_output_dir:
            roots.append(_resolve_repo_path(cfg.live_output_dir) or Path(cfg.live_output_dir))
    roots.append(ROOT / "artifacts" / "live_signals")
    seen: set[Path] = set()
    for root in roots:
        if root in seen or not root.exists():
            continue
        seen.add(root)
        for path in sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            quick_signal_id = path.parent.name if path.parent.name == target else _summary_signal_id_fast(path)
            if quick_signal_id != target:
                continue
            try:
                summary = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(summary, dict):
                return path, summary
    return None


def _summary_signal_id_fast(path: Path) -> str | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            text = handle.read(64 * 1024)
    except Exception:
        return None
    value = _summary_scalar_from_text(text, "signal_id")
    return str(value) if value is not None else None


def _latest_market_signal(cfg: LiveMarketConfig) -> tuple[Path, dict[str, Any]] | None:
    cfg = _effective_market_config(cfg)
    root = _resolve_repo_path(cfg.live_output_dir)
    if root is None or not root.exists():
        return None
    pointer_path = root / "latest_signal.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        if not isinstance(pointer, dict):
            raise ValueError("latest signal pointer is not an object")
        summary_path = _resolve_repo_path(pointer.get("summary_path"))
        if (
            pointer.get("artifact_complete") is not False
            and summary_path is not None
            and summary_path.is_file()
        ):
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(summary, dict):
                return summary_path, summary
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        pass
    for path in sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(summary, dict):
            return path, summary
    return None


def _market_has_generated_signal_for_session(
    cfg: LiveMarketConfig, session_date: str
) -> bool:
    latest = _latest_market_signal(cfg)
    if latest is None:
        return False
    _summary_path, summary = latest
    return _summary_date_matches(summary.get("generated_at"), session_date)


def _day_trade_schedule_state(
    cfg: LiveMarketConfig, session_date: str
) -> str:
    """Reconcile the scheduler with the paper engine, not only artifacts."""

    latest = _latest_market_signal(cfg)
    if latest is None:
        return "retry"
    _summary_path, summary = latest
    if not _summary_date_matches(summary.get("generated_at"), session_date):
        return "retry"
    receipt = load_service_sync(_day_trade_state_dir())
    raw_mode = mode_from_service_sync(receipt, str(cfg.market))
    if raw_mode is None:
        # Backward-compatible bootstrap while an older engine is being
        # replaced.  The normal hot path reads only the compact commit receipt.
        try:
            state = json.loads(
                (_day_trade_state_dir() / "state.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError, TypeError):
            return "retry"
        raw_mode = (state.get("modes") or {}).get(str(cfg.market))
    if not isinstance(raw_mode, dict):
        return "retry"
    positions = raw_mode.get("positions") or {}
    legacy_open = isinstance(positions, dict) and any(
        int(position.get("signed_shares") or 0) != 0
        for position in positions.values()
        if isinstance(position, dict)
    )
    if int(raw_mode.get("open_position_count") or 0) > 0 or legacy_open:
        return "blocked_open_position"
    if (
        str(raw_mode.get("session_date") or "") == session_date
        and _summary_date_matches(
            raw_mode.get("entry_completed_at"), session_date
        )
    ):
        return "completed"
    published_at = _parse_time(
        summary.get("artifact_published_at")
        or summary.get("signal_ready_at")
        or summary.get("generated_at")
    )
    if published_at is not None:
        observed = datetime.now(
            ZoneInfo(getattr(cfg, "timezone", None) or "Asia/Taipei")
        )
        age_seconds = max(
            0.0,
            (observed - published_at.astimezone(observed.tzinfo)).total_seconds(),
        )
        if age_seconds < _day_trade_confirmation_timeout_seconds():
            return "pending_confirmation"
    return "retry"


def _write_discord_service_status() -> dict[str, Any]:
    """Publish the bot's acknowledgement of the engine commit revision."""

    configs = _market_configs()
    day_trade_markets = sorted(
        market
        for market, cfg in configs.items()
        if _market_enabled(cfg)
        and bool(getattr(cfg, "day_trade_simulation_enabled", False))
    )
    engine = load_service_sync(_day_trade_state_dir()) or {}
    modes = engine.get("modes") or {}
    payload = {
        "schema_version": 1,
        "service": "stockagent-discord-bot",
        "run_id": _BOT_RUN_ID,
        "run_started_at": _BOT_RUN_STARTED_AT,
        "updated_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "discord_connected": bool(bot.is_ready()),
        "core_health": "ready" if bot.is_ready() else "waiting",
        "background_maintenance": _artifact_backfill_health_summary(),
        "interactive_signal_jobs": _signal_now_job_health_summary(),
        "day_trade_markets": day_trade_markets,
        "engine_run_id": engine.get("engine_run_id"),
        "engine_state_revision": int(engine.get("state_revision") or 0),
        "engine_published_at": engine.get("published_at"),
        "mode_signal_ids": {
            market: (modes.get(market) or {}).get("signal_id")
            for market in day_trade_markets
            if isinstance(modes.get(market), dict)
        },
        "simulation_only": True,
        "production_order_possible": False,
    }
    path = _discord_service_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".json.tmp.{os.getpid()}")
    with _SERVICE_STATUS_LOCK:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    return payload


def _summary_date_matches(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    return str(left).replace("T", " ").strip()[:10] == str(right).replace("T", " ").strip()[:10]


def _signal_now_open_cache_seconds() -> float:
    return max(0.0, _env_float("STOCKAGENT_SIGNAL_NOW_OPEN_CACHE_SECONDS", 60.0))


def _summary_age_seconds(summary: dict[str, Any], cfg: LiveMarketConfig) -> float | None:
    raw = summary.get("generated_at") or summary.get("asof_date")
    text = str(raw or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        parsed = None
    if parsed is not None and parsed.tzinfo is not None:
        return max(0.0, (datetime.now(parsed.tzinfo) - parsed).total_seconds())
    dt = _history_datetime(raw)
    if dt is None:
        return None
    try:
        now = datetime.now(ZoneInfo(cfg.display_timezone or cfg.timezone or "Asia/Taipei")).replace(tzinfo=None)
    except Exception:
        now = datetime.now()
    return max(0.0, (now - dt).total_seconds())


def _can_reuse_latest_signal_now(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus,
    summary: dict[str, Any],
    *,
    requested_price_source: str,
) -> tuple[bool, str | None]:
    deployment_ok, deployment_reason = _summary_matches_market_deployment(cfg, status, summary)
    if not deployment_ok:
        return False, deployment_reason
    requested = str(requested_price_source or "auto").strip().lower()
    summary_price = str(summary.get("price_source") or "").strip().lower()
    summary_date = _summary_data_date_key(summary)
    if status.market_open:
        market_type = str(getattr(cfg, "market_type", "") or "").strip().lower()
        frequency = str(getattr(cfg, "history_frequency", "daily") or "daily").strip().lower()
        if market_type in {"crypto", "forex", "fx"} or frequency in {"bar", "intraday", "1m", "15m"}:
            if requested not in {"", "auto", "panel"}:
                return False, None
            latest_data_date = getattr(status.data, "last_data_date", None) or getattr(status.data, "panel_date", None)
            if not _summary_date_matches(summary_date, latest_data_date):
                return False, None
            if summary_price and not (summary_price.startswith("panel") or summary_price in {"close", "panel_close"}):
                return False, None
            ttl = _signal_now_open_cache_seconds()
            age = _summary_age_seconds(summary, cfg)
            if ttl <= 0 or age is None or age > ttl:
                return False, None
            return True, f"cached_open_panel_age={age:.0f}s"
        if market_type in {"tw", "taiwan"}:
            if requested not in {"", "auto", "tw", "twse", "tpex", "mis", "tw_mis"}:
                return False, None
            if not (summary_price.startswith("twse_tpex") or summary_price in {"tw", "tw_mis", "mis"}):
                return False, None
            if int(summary.get("price_available_count") or 0) <= 0:
                return False, None
            execution_mode = str(summary.get("execution_mode") or "").strip().lower()
            if execution_mode == "tw_day_trade":
                if not bool(summary.get("live_session_open_feature_applied")):
                    return False, "day_trade_live_open_feature_missing"
                if not _summary_date_matches(
                    summary.get("panel_date"),
                    summary.get("price_timestamp"),
                ):
                    return False, "day_trade_decision_price_date_mismatch"
            ttl = _signal_now_open_cache_seconds()
            age = _summary_age_seconds(summary, cfg)
            if ttl <= 0 or age is None or age > ttl:
                return False, None
            return True, f"cached_open_tw_mis_age={age:.0f}s"
        if requested not in {"", "auto", "yahoo"}:
            return False, None
        if not summary_price.startswith("yahoo"):
            return False, None
        ttl = _signal_now_open_cache_seconds()
        age = _summary_age_seconds(summary, cfg)
        if ttl <= 0 or age is None or age > ttl:
            return False, None
        return True, f"cached_open_yahoo_age={age:.0f}s"

    realtime_source = _realtime_price_source_for_market(cfg)
    if realtime_source and _should_use_realtime_quote_after_open(cfg, status):
        allowed = {"", "auto", realtime_source}
        if realtime_source == "tw":
            allowed |= {"twse", "tpex", "mis", "tw_mis"}
        if requested not in allowed:
            return False, None
        if realtime_source == "tw":
            source_ok = summary_price.startswith("twse_tpex") or summary_price in {"tw", "tw_mis", "mis"}
        else:
            source_ok = summary_price.startswith(realtime_source)
        if not source_ok:
            return False, None
        if int(summary.get("price_available_count") or 0) <= 0:
            return False, None
        ttl = _signal_now_open_cache_seconds()
        age = _summary_age_seconds(summary, cfg)
        if ttl <= 0 or age is None or age > ttl:
            return False, None
        return True, f"cached_{realtime_source}_after_close_age={age:.0f}s"

    if requested not in {"", "auto", "panel"}:
        return False, None
    if not status.data.fresh:
        return False, None
    latest_data_date = getattr(status.data, "last_data_date", None) or getattr(status.data, "panel_date", None)
    if not _summary_date_matches(summary_date, latest_data_date):
        return False, None
    if summary_price and not (summary_price.startswith("panel") or summary_price in {"close", "panel_close"}):
        return False, None
    return True, "cached_latest_close"


def _summary_matches_market_deployment(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus,
    summary: dict[str, Any],
) -> tuple[bool, str | None]:
    cfg = _effective_market_config(cfg)
    expected_fold = getattr(cfg, "fold_id", None)
    if expected_fold is not None:
        try:
            if int(summary.get("fold_id")) != int(expected_fold):
                return False, "deployment_fold_changed"
        except (TypeError, ValueError):
            return False, "deployment_fold_missing"

    checkpoint = getattr(status, "checkpoint", None)
    expected_checkpoint = str(getattr(checkpoint, "fingerprint", "") or "").strip()
    if expected_checkpoint and str(summary.get("checkpoint_fingerprint") or "").strip() != expected_checkpoint:
        return False, "deployment_checkpoint_changed"

    expected_config = str(getattr(status, "config_fingerprint", "") or "").strip()
    if expected_config and str(summary.get("config_fingerprint") or "").strip() != expected_config:
        return False, "deployment_config_changed"
    return True, None


def _latest_signal_result_from_artifacts(
    cfg: LiveMarketConfig,
    summary_path: Path,
    summary: dict[str, Any],
    *,
    top_n: int,
    current_capital: float | None = None,
    debug: bool = False,
):
    enriched = _summary_with_capital_context(cfg, dict(summary), current_capital=current_capital)
    message = _latest_signal_message(cfg, summary_path, enriched, top_n=top_n, current_capital=current_capital, debug=debug)
    return SimpleNamespace(
        summary=enriched,
        weights_rows=_latest_artifact_rows(enriched, summary_path, "weights_path", "top_positions"),
        rebalance_rows=_latest_artifact_rows(enriched, summary_path, "rebalance_path", "rebalance"),
        decision_rows=_latest_artifact_rows(enriched, summary_path, "decision_explanation_path", "decision_explanations"),
        message=message,
        output_dir=str(summary_path.parent),
    )


def _signal_now_cached_result(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus,
    *,
    requested_price_source: str,
    top_n: int,
    require_unconstrained_raw_scores: bool = False,
    debug: bool = False,
):
    latest = _latest_market_signal(cfg)
    if latest is None:
        return None
    summary_path, summary = latest
    if not _summary_has_raw_score_contract(
        summary,
        require_unconstrained=require_unconstrained_raw_scores,
    ):
        return None
    reusable, reason = _can_reuse_latest_signal_now(
        cfg,
        status,
        summary,
        requested_price_source=requested_price_source,
    )
    if not reusable:
        return None
    result = _latest_signal_result_from_artifacts(cfg, summary_path, summary, top_n=top_n, debug=debug)
    result.summary["signal_now_cache"] = reason
    return summary_path, result, reason


def _summary_has_raw_score_contract(
    summary: dict[str, Any],
    *,
    require_unconstrained: bool = False,
) -> bool:
    contract = summary.get("score_contract")
    if not isinstance(contract, dict):
        return False
    try:
        valid = int(contract.get("schema_version", 0)) >= 1
    except (TypeError, ValueError):
        return False
    if not valid:
        return False
    if require_unconstrained:
        return (
            int(contract.get("schema_version", 0)) >= 2
            and str(contract.get("raw_score_scope") or "")
            == "all_checkpoint_symbols_unmasked"
        )
    return True


def _signal_now_should_refresh_data(status: MarketRuntimeStatus, *, refresh_data: bool) -> bool:
    return bool(refresh_data or not bool(getattr(status.data, "fresh", False)))


def _market_signals(cfg: LiveMarketConfig) -> list[tuple[Path, dict[str, Any]]]:
    cfg = _effective_market_config(cfg)
    root = _resolve_repo_path(cfg.live_output_dir)
    if root is None or not root.exists():
        return []
    signals: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(summary, dict):
            signals.append((path, summary))
    return signals


def _recent_market_signals(cfg: LiveMarketConfig, *, max_summaries: int) -> list[tuple[Path, dict[str, Any]]]:
    cfg = _effective_market_config(cfg)
    root = _resolve_repo_path(cfg.live_output_dir)
    if root is None or not root.exists():
        return []
    try:
        limit = max(1, int(max_summaries))
    except Exception:
        limit = 128
    paths = sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]
    signals: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(summary, dict):
            signals.append((path, summary))
    return list(reversed(signals))


_SUMMARY_SCALAR_PATTERN_CACHE: dict[str, re.Pattern[str]] = {}


def _summary_scalar_from_text(text: str, key: str) -> Any:
    pattern = _SUMMARY_SCALAR_PATTERN_CACHE.get(key)
    if pattern is None:
        pattern = re.compile(rf'"{re.escape(key)}"\s*:\s*(".*?"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|null)')
        _SUMMARY_SCALAR_PATTERN_CACHE[key] = pattern
    match = pattern.search(text)
    if match is None:
        return None
    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def _read_summary_metric_fields(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            text = handle.read(256 * 1024)
    except Exception:
        return {}
    keys = (
        "panel_data_date",
        "weights_date",
        "panel_date",
        "asof_date",
        "previous_weights_data_date",
        "previous_weights_date",
        "drift_base_data_date",
        "drift_base_date",
        "portfolio_simple_return",
        "benchmark_simple_return",
        "execution_preview_only",
        "price_source",
        "price_timestamp",
    )
    return {key: _summary_scalar_from_text(text, key) for key in keys}


def _recent_market_signal_metrics(cfg: LiveMarketConfig, *, max_summaries: int) -> list[tuple[Path, dict[str, Any]]]:
    cfg = _effective_market_config(cfg)
    root = _resolve_repo_path(cfg.live_output_dir)
    if root is None or not root.exists():
        return []
    try:
        limit = max(1, int(max_summaries))
    except Exception:
        limit = 128
    paths = sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]
    return [(path, _read_summary_metric_fields(path)) for path in reversed(paths)]


def _sync_latest_live_weights_to_market_artifact(cfg: LiveMarketConfig) -> str | None:
    latest = _latest_market_signal(cfg)
    if latest is None:
        return None
    summary_path, summary = latest
    if bool(summary.get("execution_preview_only")):
        return None
    status = _runtime_status_for_display(cfg)
    deployment_ok, reason = _summary_matches_market_deployment(cfg, status, summary)
    if not deployment_ok:
        print(
            f"[live-weights:{cfg.market}] skip artifact from another deployment: {reason}",
            flush=True,
        )
        return None
    weights_path = _summary_artifact_path(summary, "weights_path", summary_path)
    if weights_path is None or not weights_path.exists():
        return None
    try:
        rows = _read_parquet_rows(weights_path)
        return write_live_weights_history(_market_fold_dir(cfg), summary, rows)
    except Exception as exc:
        _log_exception(f"sync_live_weights:{cfg.market}", exc)
        return None


def _summary_data_date_key(summary: dict[str, Any]) -> str | None:
    for key in ("weights_date", "panel_data_date", "panel_date", "asof_date"):
        raw = summary.get(key)
        if raw:
            text = str(raw).replace("T", " ").strip()
            return text[:10] if len(text) >= 10 else text
    return None


def _market_has_live_signal_for_date(cfg: LiveMarketConfig, date_text: str | None) -> bool:
    if not date_text:
        return False
    target = str(date_text).replace("T", " ").strip()[:10]
    if not target:
        return False
    for _, summary in _recent_market_signal_metrics(cfg, max_summaries=128):
        if _summary_data_date_key(summary) == target:
            return True
    return False


def _market_has_panel_close_signal_for_date(
    cfg: LiveMarketConfig,
    date_text: str | None,
) -> bool:
    if not date_text:
        return False
    target = str(date_text).replace("T", " ").strip()[:10]
    for _, summary in _recent_market_signal_metrics(cfg, max_summaries=128):
        source = str(summary.get("price_source") or "").strip().lower()
        if (
            _summary_data_date_key(summary) == target
            and (source == "panel" or source.startswith("panel_"))
        ):
            return True
    return False


def _formal_history_latest_date(cfg: LiveMarketConfig) -> str | None:
    path = _returns_artifact_path(_market_fold_dir(cfg))
    if path is None or not path.exists():
        return None
    try:
        import polars as pl

        frame = pl.scan_parquet(path) if path.suffix == ".parquet" else pl.scan_csv(path)
        value = frame.select(pl.col("date").max()).collect().item()
    except Exception:
        return None
    parsed = _history_datetime(value)
    return parsed.date().isoformat() if parsed is not None else None


def _previous_source_session_date(
    cfg: LiveMarketConfig,
    target_date: str | None,
) -> str | None:
    target_key = _date_key(target_date)
    if not target_key:
        return None
    try:
        train_config = load_config(_resolve_repo_path(cfg.config_path) or cfg.config_path)
        parquet_root = _resolve_repo_path(train_config.data.parquet_root)
        benchmark = str(train_config.data.benchmark_name or "").strip()
        if parquet_root is None or not benchmark:
            return None
        path = parquet_root / f"{benchmark}_features.parquet"
        if not path.is_file():
            return None

        import polars as pl

        dates = (
            pl.scan_parquet(path)
            .select(pl.col("date").cast(pl.Date, strict=False).alias("date"))
            .filter(pl.col("date") < pl.lit(target_key).str.to_date())
            .select(pl.col("date").max())
            .collect()
            .item()
        )
    except Exception:
        return None
    parsed = _history_datetime(dates)
    return parsed.date().isoformat() if parsed is not None else None


def _artifact_backfill_is_current(
    cfg: LiveMarketConfig,
    status: MarketRuntimeStatus,
    execution_mode: str,
) -> bool:
    if not bool(getattr(status.data, "fresh", False)):
        return False
    target_date = (
        status.data.expected_latest_date
        or status.data.last_data_date
        or status.data.panel_date
    )
    target_key = _date_key(target_date)
    if not target_key:
        return False
    if execution_mode == "naive":
        required_formal = _previous_source_session_date(cfg, target_key)
        if not required_formal:
            return False
        formal_latest = _formal_history_latest_date(cfg)
        formal_ready = bool(
            required_formal
            and formal_latest
            and formal_latest >= required_formal
        )
        return formal_ready and _market_has_panel_close_signal_for_date(
            cfg,
            target_key,
        )
    if execution_mode == "tw_day_trade":
        formal_latest = _formal_history_latest_date(cfg)
        return bool(formal_latest and formal_latest >= target_key)
    return _market_has_live_signal_for_date(cfg, target_key)


def _formal_history_timeout_seconds(cfg: LiveMarketConfig) -> int:
    """Keep heavy fold inference independent from short activation commands."""

    configured = getattr(cfg, "formal_history_timeout_seconds", None)
    if configured is None:
        configured = max(
            900,
            int(getattr(cfg, "pre_signal_timeout_seconds", 900) or 900),
        )
    return max(60, int(configured))


def _run_formal_history_backfill(cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> bool:
    target_date = (
        getattr(status.data, "expected_latest_date", None)
        or status.data.last_data_date
        or status.data.panel_date
    )
    target_key = str(target_date or "").replace("T", " ")[:10]
    latest_key = _formal_history_latest_date(cfg)
    if target_key and latest_key and latest_key >= target_key:
        return False
    fold_id = cfg.fold_id
    if fold_id is None:
        checkpoint = _market_model_checkpoint(cfg)
        if checkpoint is not None:
            match = re.fullmatch(r"fold_(\d+)", checkpoint.parent.name)
            if match:
                fold_id = int(match.group(1))
    if fold_id is None:
        raise BotUserError(
            f"`{cfg.market}` formal history backfill cannot resolve fold_id "
            "from config or checkpoint path"
        )

    config_path = _resolve_repo_path(cfg.config_path) or Path(cfg.config_path)
    output_dir = _resolve_repo_path(cfg.output_dir) if cfg.output_dir else None
    command = [
        sys.executable,
        str(ROOT / "train.py"),
        "--config",
        str(config_path),
        "--mode",
        "infer",
        "--start-fold",
        str(int(fold_id)),
        "--max-folds",
        "1",
        "--multi-gpu-strategy",
        "none",
        "--no-explain-after-each-fold",
        "--no-postprocess-benchmark-after-fold",
    ]
    if output_dir is not None:
        command.extend(["--output-dir", str(output_dir)])
    print(
        f"[formal-history] market={cfg.market} latest={latest_key or 'none'} "
        f"target={target_key or 'unknown'} "
        f"timeout_seconds={_formal_history_timeout_seconds(cfg)} "
        f"command={' '.join(command)}",
        flush=True,
    )
    timeout_seconds = _formal_history_timeout_seconds(cfg)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise BotUserError(
            f"`{cfg.market}` formal history inference timed out after "
            f"{timeout_seconds}s"
        ) from exc
    if completed.returncode != 0:
        raise BotUserError(
            f"`{cfg.market}` formal history inference failed rc={completed.returncode}"
        )
    refreshed = _formal_history_latest_date(cfg)
    execution_mode = _market_execution_mode(cfg)
    # Naive row t is evaluated with close[t] -> close[t+1], so a source panel
    # ending on target day normally yields canonical tables through the prior
    # trading row. The target day's close signal is generated immediately
    # afterwards and is the only live row allowed to extend formal history.
    if execution_mode == "naive" and refreshed is None:
        raise BotUserError(f"`{cfg.market}` formal history produced no dated rows")
    if (
        execution_mode != "naive"
        and target_key
        and (not refreshed or refreshed < target_key)
    ):
        raise BotUserError(
            f"`{cfg.market}` formal history remains stale after inference: "
            f"latest={refreshed or 'none'} target={target_key}"
        )
    print(f"[formal-history] completed market={cfg.market} latest={refreshed}", flush=True)
    return True


# Compatibility name for focused callers/tests that predate Naive formal backfill.
_run_day_trade_settlement_backfill = _run_formal_history_backfill


def _run_artifact_backfill_sync(cfg: LiveMarketConfig) -> LiveSignalResult | None:
    cfg = _effective_market_config(cfg)
    status = _ensure_signal_ready(cfg)
    execution_mode = _market_execution_mode(cfg)
    if _artifact_backfill_is_current(cfg, status, execution_mode):
        _sync_latest_live_weights_to_market_artifact(cfg)
        return None
    if execution_mode in {"naive", "tw_day_trade"}:
        if not status.data.fresh:
            try:
                _run_pre_signal_command(cfg)
            except BotUserError:
                # Some auxiliary official datasets may be temporarily unavailable
                # even after the canonical stock OHLCV has already advanced.
                _clear_runtime_status_cache()
                refreshed_status = _runtime_status(cfg)
                if not refreshed_status.data.fresh:
                    raise
            _clear_runtime_status_cache()
            status = _runtime_status(cfg)
        _require_fresh_data_for_artifact_generation(cfg, status)
        target_date = (
            status.data.expected_latest_date
            or status.data.last_data_date
            or status.data.panel_date
        )
        if _artifact_backfill_is_current(cfg, status, execution_mode):
            _sync_latest_live_weights_to_market_artifact(cfg)
            return None
        _run_formal_history_backfill(cfg, status)
        if execution_mode == "naive":
            if _artifact_backfill_is_current(cfg, status, execution_mode):
                _sync_latest_live_weights_to_market_artifact(cfg)
                return None
            progress_label = f"backfill:{cfg.market}:panel-close"
            progress_callback = (
                _ConsoleProgress(prefix=progress_label)
                if _env_bool("STOCKAGENT_BOT_PROGRESS", True)
                else None
            )
            result = generate_live_signal(
                **cfg.signal_kwargs(
                    price_source="panel",
                    market_notice=_market_notice(status),
                    progress_callback=progress_callback,
                    progress_label=progress_label,
                )
            )
            _sync_latest_live_weights_to_market_artifact(cfg)
            return result
        return None
    resolved_price_source = _auto_signal_price_source(cfg, status, "auto")
    if resolved_price_source:
        target_date = datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei")).date().isoformat()
        if _market_has_live_signal_for_date(cfg, target_date):
            _sync_latest_live_weights_to_market_artifact(cfg)
            return None
        progress_label = f"backfill:{cfg.market}:close"
        progress_callback = _ConsoleProgress(prefix=progress_label) if _env_bool("STOCKAGENT_BOT_PROGRESS", True) else None
        result = generate_live_signal(
            **cfg.signal_kwargs(
                price_source=resolved_price_source,
                market_notice=_market_notice(status),
                progress_callback=progress_callback,
                progress_label=progress_label,
            )
        )
        _sync_latest_live_weights_to_market_artifact(cfg)
        return result

    if not status.data.fresh:
        _run_pre_signal_command(cfg)
        _clear_runtime_status_cache()
        status = _runtime_status(cfg)
    _require_fresh_data_for_artifact_generation(cfg, status)
    target_date = status.data.expected_latest_date or status.data.last_data_date or status.data.panel_date
    if _market_has_live_signal_for_date(cfg, target_date):
        _sync_latest_live_weights_to_market_artifact(cfg)
        return None
    progress_label = f"backfill:{cfg.market}"
    progress_callback = _ConsoleProgress(prefix=progress_label) if _env_bool("STOCKAGENT_BOT_PROGRESS", True) else None
    result = generate_live_signal(
        **cfg.signal_kwargs(
            price_source="panel",
            market_notice=_market_notice(status),
            progress_callback=progress_callback,
            progress_label=progress_label,
        )
    )
    _sync_latest_live_weights_to_market_artifact(cfg)
    return result


def _summary_artifact_path(summary: dict[str, Any], key: str, summary_path: Path | None = None) -> Path | None:
    raw = summary.get(key)
    if raw:
        path = _resolve_repo_path(str(raw))
        if path is not None:
            return path
    if summary_path is not None:
        fallback_names = {
            "decision_explanation_path": "decision_explanations.parquet",
            "weights_path": "target_weights.parquet",
            "rebalance_path": "rebalance.parquet",
            "decision_report_path": "decision_report.md",
            "decision_explanation_markdown_path": "decision_explanations.md",
        }
        name = fallback_names.get(key)
        if name:
            return summary_path.parent / name
    return None


def _reconcile_artifact_backfill_if_current(
    cfg: LiveMarketConfig,
    *,
    key: str,
    market: str,
) -> bool:
    """Clear a stale failure immediately when final artifacts already prove recovery."""

    try:
        effective = _effective_market_config(cfg)
        status = _ensure_signal_ready(effective)
        execution_mode = _market_execution_mode(effective)
        if not _artifact_backfill_is_current(effective, status, execution_mode):
            return False
        _sync_latest_live_weights_to_market_artifact(effective)
    except Exception:
        return False
    bot._last_artifact_backfill_keys.add(key)
    _finish_artifact_backfill(key, market, status="ready")
    print(
        f"[artifact-backfill] market={market} status=reconciled_current_artifact",
        flush=True,
    )
    return True


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    import polars as pl

    if not path.exists():
        raise FileNotFoundError(path)
    return pl.read_parquet(path).to_dicts()


def _latest_signal_or_raise(cfg: LiveMarketConfig) -> tuple[Path, dict[str, Any]]:
    latest = _latest_market_signal(cfg)
    if latest is None:
        raise BotUserError(f"`{cfg.market}` 尚無 live signal，請先跑 `/signal_now market:{cfg.market}`。")
    return latest


def _latest_artifact_rows(
    summary: dict[str, Any],
    summary_path: Path,
    key: str,
    fallback_key: str,
) -> list[dict[str, Any]]:
    path = _summary_artifact_path(summary, key, summary_path)
    if path is not None and path.exists():
        return _read_parquet_rows(path)
    fallback = summary.get(fallback_key)
    return list(fallback) if isinstance(fallback, list) else []


def _row_weight_value(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = _float_or_none(row.get(key))
        if value is not None:
            return value
    return 0.0


def _row_matches_watchlist(row: dict[str, Any], watchlist: list[str]) -> bool:
    if not watchlist:
        return True
    symbol = _normalize_watch_symbol(row.get("symbol"))
    name = str(row.get("name") or "").strip().lower()
    for item in watchlist:
        needle = _normalize_watch_symbol(item)
        if not needle:
            continue
        if needle == symbol or needle in symbol or symbol in needle:
            return True
        if needle.lower() and needle.lower() in name:
            return True
    return False


def _filter_watchlist_rows(rows: list[dict[str, Any]], watchlist: list[str]) -> list[dict[str, Any]]:
    if not watchlist:
        return rows
    return [row for row in rows if _row_matches_watchlist(row, watchlist)]


def _signal_action_mix(summary: dict[str, Any], rows: list[dict[str, Any]] | None = None) -> str:
    data = rows
    if data is None:
        raw = summary.get("rebalance")
        data = list(raw) if isinstance(raw, list) else []
    return _action_count_text(data)


def _latest_signal_message(
    cfg: LiveMarketConfig,
    summary_path: Path,
    summary: dict[str, Any],
    *,
    top_n: int = 8,
    current_capital: float | None = None,
    debug: bool = False,
) -> str:
    del summary_path
    enriched = _summary_with_capital_context(cfg, summary, current_capital=current_capital)
    message = format_signal_message(enriched, max_rows=max(0, int(top_n)), debug=debug)
    return _prepend_sanity_notice(message, cfg, enriched)


def _latest_changes_pages(
    cfg: LiveMarketConfig,
    summary_path: Path,
    summary: dict[str, Any],
    *,
    action: str = "actionable",
    limit: int = 0,
    page_size: int = 20,
    current_capital: float | None = None,
    watchlist: list[str] | None = None,
    debug: bool = False,
) -> list[str]:
    rows = _latest_artifact_rows(summary, summary_path, "rebalance_path", "rebalance")
    rows = _filter_decision_rows(rows, action=action, actionable_only=str(action or "").lower() in {"", "actionable"})
    rows = _filter_watchlist_rows(rows, watchlist or [])
    rows = _sort_decision_rows(rows, "delta")
    rows = _limit_rows(rows, limit)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    rows = _annotate_weight_rows_with_capital(rows, capital)
    issues = _signal_sanity_issues(cfg, summary)
    header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("rows", len(rows)),
        ),
        _kv_line(("action_mix", _signal_action_mix(summary, rows)), ("watch", ",".join(watchlist or []) or "off")),
        _kv_line(("sanity", _signal_sanity_level(issues))),
        f"capital: `{_capital_context_text(capital=capital)}`",
    ]
    if issues:
        header.append("issues: `" + " | ".join(text for _, text in issues[:3]) + "`")
    if debug:
        header.extend(
            [
                _kv_line(
                    ("signal", summary.get("signal_id", summary_path.parent.name)),
                    ("display_tz", summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"summary: `{_display_path(summary_path)}`",
            ]
        )
    return _line_pages(
        title=f"{cfg.label} latest changes",
        rows=rows,
        formatter=_rebalance_line,
        page_size=page_size,
        header_lines=header,
    )


def _performance_message(
    cfg: LiveMarketConfig,
    summary_path: Path,
    summary: dict[str, Any],
    *,
    days: int = 32,
    current_capital: float | None = None,
    debug: bool = False,
) -> str:
    del summary_path
    enriched = _summary_with_capital_context(cfg, summary, current_capital=current_capital)
    portfolio_return = _float_or_none(enriched.get("portfolio_simple_return"))
    benchmark_return = _float_or_none(enriched.get("benchmark_simple_return"))
    excess_return = None if portfolio_return is None or benchmark_return is None else portfolio_return - benchmark_return
    recent = enriched.get("recent_performance") if isinstance(enriched.get("recent_performance"), dict) else {}
    issues = _signal_sanity_issues(cfg, enriched)
    lines = [
        f"**performance** {cfg.label}",
        _kv_line(
            ("asof", _display_summary_time(enriched, enriched.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(enriched, enriched.get("panel_date", "n/a"))),
            ("sanity", _signal_sanity_level(issues)),
        ),
        "",
        "**上個訊號到現在**",
        _kv_line(
            ("strategy", _signed_pct(portfolio_return)),
            ("baseline", _signed_pct(benchmark_return)),
            ("excess", _signed_pct(excess_return)),
            ("turnover", _pct(enriched.get("turnover"))),
        ),
    ]
    if _float_or_none(enriched.get("portfolio_pnl_value")) is not None:
        lines.append(
            _kv_line(
                ("capital", _money(enriched.get("display_capital"))),
                ("pnl", _signed_money(enriched.get("portfolio_pnl_value"))),
                ("baseline_pnl", _signed_money(enriched.get("benchmark_pnl_value"))),
                ("excess_pnl", _signed_money(enriched.get("excess_pnl_value"))),
            )
        )
    if recent:
        recent_label = recent.get("window_label") or f"過去{recent.get('window_days', 'n')}期"
        lines.extend(
            [
                "",
                f"**{recent_label}**",
                _kv_line(
                    ("strategy", _signed_pct(recent.get("strategy_return"))),
                    ("baseline", _signed_pct(recent.get("benchmark_return"))),
                    ("excess", _signed_pct(recent.get("excess_return"))),
                ),
            ]
        )
        if _float_or_none(recent.get("strategy_pnl_value")) is not None:
            lines.append(
                _kv_line(
                    ("pnl", _signed_money(recent.get("strategy_pnl_value"))),
                    ("baseline_pnl", _signed_money(recent.get("benchmark_pnl_value"))),
                    ("excess_pnl", _signed_money(recent.get("excess_pnl_value"))),
                )
            )
    try:
        window = _recent_performance_from_returns(
            cfg,
            int(days or 0),
            capital=_resolve_current_capital(cfg, current_capital=current_capital),
        ) if int(days or 0) > 0 else None
    except Exception as exc:
        window = None
        if debug:
            lines.append(f"history_load_error: `{type(exc).__name__}`")
    if window is not None:
        lines.extend(
            [
                "",
                f"**artifact history {window.get('window_days', 'n')} periods**",
                _kv_line(
                    ("period", f"{window.get('start_date')}..{window.get('end_date')}"),
                    ("strategy", _signed_pct(window.get("strategy_return"))),
                    ("baseline", _signed_pct(window.get("benchmark_return"))),
                    ("profit", _signed_money(window.get("strategy_pnl_value"))),
                ),
            ]
        )
    if issues:
        lines.extend(["", "sanity issues:"])
        lines.extend(f"- {severity}: {text}" for severity, text in issues[:6])
    _append_investment_warning(lines)
    return "\n".join(lines)


def _risk_message(
    cfg: LiveMarketConfig,
    summary_path: Path,
    summary: dict[str, Any],
    *,
    top_n: int = 10,
    debug: bool = False,
) -> str:
    risk = summary.get("target_risk") if isinstance(summary.get("target_risk"), dict) else {}
    issues = _signal_sanity_issues(cfg, summary)
    rows = _latest_artifact_rows(summary, summary_path, "weights_path", "top_positions")
    rows = sorted(rows, key=lambda row: abs(_row_weight_value(row, "target_weight", "weight")), reverse=True)
    rows = [
        row
        for row in rows
        if is_display_position_row(row)
    ]
    gross_limit, turnover_limit = _config_trading_limits(cfg)
    lines = [
        f"**risk** {cfg.label}",
        _kv_line(
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("sanity", _signal_sanity_level(issues)),
        ),
        _kv_line(
            ("gross", _pct(risk.get("gross"))),
            ("limit", _pct(gross_limit)),
            ("long", _pct(risk.get("long_gross"))),
            ("short", _pct(risk.get("short_gross"))),
            ("net", _signed_pct(risk.get("net"))),
        ),
        _kv_line(
            ("top", _pct(risk.get("top_abs_weight"))),
            ("HHI", _num(risk.get("hhi"), 3)),
            ("turnover", _pct(summary.get("turnover"))),
            ("turnover_limit", _pct(turnover_limit)),
            ("fees", _pct(summary.get("estimated_trade_cost"), 3)),
        ),
    ]
    if issues:
        lines.extend(["", "**sanity issues**"])
        lines.extend(f"- {severity}: {text}" for severity, text in issues[:8])
    if rows:
        lines.extend(["", "**largest positions**"])
        for index, row in enumerate(rows[: max(1, int(top_n))], start=1):
            weight = _row_weight_value(row, "target_weight", "weight")
            lines.append(
                f"{index}. {_symbol_label(row)} "
                + _kv_inline(("weight", _signed_pct(weight)), ("px", _price(row.get("current_price"))))
            )
    if debug:
        lines.extend(["", f"summary: `{_display_path(summary_path)}`"])
    _append_investment_warning(lines)
    return "\n".join(lines)


def _guide_message() -> str:
    markets = ", ".join(
        f"`{key}`"
        for key, cfg in sorted(_market_configs().items())
        if _market_enabled(cfg)
    )
    lines = [
        "**stockAgent guide**",
        f"markets: {markets or '`n/a`'}",
        "",
        "**台股模式**",
        "`tw` 舊版 Naive 權重模式。",
        "`tw_cash` 現股/T+2 模式；需有相符 checkpoint 才能推論。",
        "`tw_day_trade_multi_basis` Multi-Basis 現股當沖（初始 1,000 萬）；使用 raw-feature lookback-32 fold 11。",
        "`tw_day_trade_100m` 現股當沖（初始 1 億）；使用獨立模型與資金基準。",
        "`tw_day_trade_multi_basis_projection_l1_gelu` Multi-Basis Projection-L1 GELU 現股當沖（初始 1,000 萬）。",
        "",
        "**日常看盤**",
        "`/latest market:<市場>` 最新訊號，不重跑模型。",
        "`/changes market:<市場>` 今日/最新調倉。",
        "`/positions market:<市場>` 目前與目標持倉。",
        "`/performance market:<市場>` 策略 vs baseline 績效。",
        "`/risk market:<市場>` 曝險、集中度、換手與 sanity。",
        "",
        "**個人化提醒**",
        "`/watch action:add market:<市場> symbol:<代號>` 加入關注。",
        "`/watch action:list market:<市場>` 查看 watchlist。",
        "`/subscribe action:add market:<市場> watchlist_only:true` 排程時只 DM 你的 watchlist 調倉。",
        "`/changes market:<市場> watchlist_only:true` 手動查看 watchlist 調倉。",
        "",
        "**進階查詢**",
        "`/stock_history symbol:<代號> market:<市場>` 單一標的交易與調整紀錄。",
        "`/portfolio_history market:<市場>` 每期持倉變化與損益。",
        "`/explain_signal market:<市場> symbol:<代號>` 決策解釋。",
        "",
        "**管理/重算**",
        "`/signal_now market:<市場>` 立即更新資料並重新推論。",
        "`/raw_score_now market:<市場>` 顯示 checkpoint 全股票 raw_score；交易限制只標示、不過濾。",
        "`/set_capital`、`/set_schedule`、`/set_market_enabled` 需要 trader/admin 權限。",
    ]
    return "\n".join(lines)


def _subscription_summary_lines(user_id: Any) -> list[str]:
    subscriptions = _user_subscriptions(user_id)
    lines = ["**subscriptions**"]
    if not subscriptions:
        lines.append("(empty)")
        return lines
    for market, settings in sorted(subscriptions.items()):
        watch_mode = "watchlist_only" if settings.get("watchlist_only", True) else "all_changes"
        watchlist = ", ".join(_user_watchlist(user_id, market)) or "(empty watchlist)"
        lines.append(f"`{market}` mode=`{watch_mode}` watch=`{watchlist}`")
    return lines


def _subscription_alert_pages(
    cfg: LiveMarketConfig,
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    user_id: Any,
    settings: dict[str, Any],
) -> list[str]:
    watchlist_only = bool(settings.get("watchlist_only", True))
    watchlist = _user_watchlist(user_id, cfg.market) if watchlist_only else []
    if watchlist_only and not watchlist:
        return []
    filtered = _filter_decision_rows(rows, action="actionable", actionable_only=True)
    filtered = _filter_watchlist_rows(filtered, watchlist)
    filtered = _sort_decision_rows(filtered, "delta")
    filtered = _limit_rows(filtered, 20)
    if not filtered:
        return []
    issues = _signal_sanity_issues(cfg, summary)
    header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("rows", len(filtered)),
        ),
        _kv_line(
            ("mode", "watchlist_only" if watchlist_only else "all_changes"),
            ("watch", ",".join(watchlist) if watchlist else "off"),
            ("sanity", _signal_sanity_level(issues)),
        ),
    ]
    if issues:
        header.append("issues: `" + " | ".join(text for _, text in issues[:3]) + "`")
    return _line_pages(
        title=f"{cfg.label} personal alert",
        rows=filtered,
        formatter=_rebalance_line,
        page_size=10,
        header_lines=header,
    )


async def _send_subscription_notifications(cfg: LiveMarketConfig, result: Any) -> None:
    subscribers = _subscribed_users_for_market(cfg.market)
    if not subscribers:
        return
    rows = list(getattr(result, "rebalance_rows", []))
    for user_id, settings in subscribers:
        try:
            pages = _subscription_alert_pages(
                cfg,
                result.summary,
                rows,
                user_id=user_id,
                settings=settings,
            )
            if not pages:
                continue
            user = await bot.fetch_user(int(user_id))
            await _send_channel_pages(user, pages, timeout=24 * 60 * 60)
        except Exception as exc:
            _log_exception(f"subscription_notify:{cfg.market}:{user_id}", exc)


def _signal_now_background_key(
    cfg: LiveMarketConfig,
    *,
    target_date: str | None,
    requested_price_source: str,
    top_n: int,
    min_abs_delta: float,
    debug: bool,
    force_refresh: bool = False,
    mode: str = "signal",
) -> str:
    target = _date_key(target_date) or "latest"
    source = str(requested_price_source or "auto").strip().lower() or "auto"
    normalized_mode = _normalize_signal_now_mode(mode)
    return (
        f"{target}:{cfg.market}:{source}:{int(top_n)}:"
        f"{float(min_abs_delta):.8g}:{int(bool(debug))}:"
        f"{int(bool(force_refresh))}:{normalized_mode}"
    )


async def _send_signal_now_background_failure(user_ids: set[int], cfg: LiveMarketConfig, exc: Exception) -> None:
    _log_exception(f"signal_now_background:{cfg.market}", exc)
    detail = f"\n原因：{str(exc)[:1200]}" if isinstance(exc, BotUserError) else ""
    text = (
        f"`{cfg.market}` 背景訊號工作暫時失敗: `{type(exc).__name__}`；"
        "系統已保留工作並會自動退避重試。\n"
        f"詳細 traceback 已寫入 `{ERROR_LOG_PATH}`。{detail}"
    )
    for user_id in sorted(user_ids):
        try:
            user = await bot.fetch_user(int(user_id))
            await user.send(text)
        except Exception as send_exc:
            _log_exception(f"signal_now_background_failure_dm:{cfg.market}:{user_id}", send_exc)


async def _run_signal_now_background_refresh(
    key: str,
    *,
    market: str,
    requested_price_source: str,
    top_n: int,
    min_abs_delta: float,
    debug: bool,
    force_refresh: bool = False,
    mode: str = "signal",
) -> None:
    cfg = _resolve_market(market)
    normalized_mode = _normalize_signal_now_mode(mode)
    include_raw_universe = normalized_mode == "raw_scores"
    command_name = "raw_score_now" if include_raw_universe else "signal_now"
    running_job: dict[str, Any] = {}
    try:
        initial_status = await asyncio.to_thread(_ensure_signal_ready_cached, cfg)
        completed_session = _completed_session_signal_path(cfg, initial_status)
        if (
            not bool(getattr(initial_status.data, "fresh", False))
            and not completed_session
        ):
            _update_signal_now_job(
                key,
                status="waiting_source",
                runtime_status=initial_status,
            )
            # The activation command can only validate/switch an accepted
            # release. It cannot make an unpublished official session exist.
            # Waiting here prevents stale inference and a guaranteed failure.
            return

        running_job = _update_signal_now_job(
            key,
            status="running",
            runtime_status=initial_status,
        )

        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        session_open, session_reason = await asyncio.to_thread(
            _scheduled_market_session_day,
            cfg,
            now,
        )
        if (
            _market_schedule_interval_minutes(cfg) is None
            and not session_open
            and not completed_session
        ):
            waiters = _signal_now_job_waiters(key)
            notice = (
                f"`{cfg.market}` 目前為休市時段，依週交易排程不啟動開盤資料更新或即時推論。\n"
                f"calendar=`{session_reason}`；工作保留到下一個有效交易日。"
            )
            for user_id in sorted(waiters):
                try:
                    user = await bot.fetch_user(int(user_id))
                    await user.send(notice[:1900])
                except Exception as send_exc:
                    _log_exception(
                        f"signal_now_closed_session_dm:{cfg.market}:{user_id}",
                        send_exc,
                    )
            _update_signal_now_job(
                key,
                status="waiting_source",
                runtime_status=initial_status,
                exc=BotUserError(str(session_reason)),
            )
            return

        cached = None
        if not force_refresh:
            cached = await asyncio.to_thread(
                _signal_now_cached_result,
                cfg,
                initial_status,
                requested_price_source=requested_price_source,
                top_n=top_n,
                require_unconstrained_raw_scores=include_raw_universe,
                debug=debug,
            )
        if cached is not None:
            _summary_path, result, _cache_reason = cached
            status = initial_status
            resolved_price_source = str(result.summary.get("price_source") or "artifact")
            auto_refreshed = False
        else:
            resolved_price_source, status, auto_refreshed = await asyncio.to_thread(
                _prepare_realtime_signal_sync,
                cfg,
                requested_price_source=requested_price_source,
                force_refresh=True,
                completed_session=completed_session,
            )
            await asyncio.to_thread(_sync_latest_live_weights_to_market_artifact, cfg)
            result = await _run_market_signal(
                market=cfg.market,
                price_source=resolved_price_source,
                top_n=top_n,
                min_abs_delta=min_abs_delta,
                progress_label=f"{command_name}:bg:{cfg.market}",
                include_unconstrained_raw_scores=include_raw_universe,
            )
        result = _enrich_signal_performance_for_discord(cfg, result, max_rows=0, debug=debug)
        sanity_issues = _signal_sanity_issues(cfg, result.summary)
        if sanity_issues:
            result.message = _prepend_sanity_notice(result.message, cfg, result.summary)
        waiters = _signal_now_job_waiters(key)
        if not waiters:
            _update_signal_now_job(
                key,
                status="ready",
                runtime_status=status,
                signal_id=str(result.summary.get("signal_id") or ""),
            )
            return
        header = (
            f"`{cfg.market}` 背景更新完成，以下是最新 {command_name}。\n"
            f"auto_refreshed=`{bool(auto_refreshed)}` "
            f"price_source=`{resolved_price_source or 'config'}` "
            f"valuation=`{'official_close' if completed_session else 'live_open'}`"
        )
        delivered: set[int] = set()
        for user_id in sorted(waiters):
            try:
                user = await bot.fetch_user(int(user_id))
                content = f"{header}\n\n{result.message}"
                await user.send(
                    content if len(content) <= 1900 else content[:1900],
                    view=SignalReviewView(
                        signal_id=str(result.summary.get("signal_id")),
                        market=str(result.summary.get("market") or cfg.market),
                    ),
                )
                for pages in _signal_now_detail_page_groups(
                    cfg,
                    result,
                    mode=normalized_mode,
                    top_n=top_n,
                    debug=debug,
                ):
                    await _send_channel_pages(user, pages, timeout=24 * 60 * 60)
                delivered.add(int(user_id))
            except Exception as send_exc:
                _log_exception(f"signal_now_background_dm:{cfg.market}:{user_id}", send_exc)
        if delivered != waiters:
            raise BotUserError(
                f"signal generated but Discord DM delivery remains pending for "
                f"{len(waiters - delivered)} user(s)"
            )
        _update_signal_now_job(
            key,
            status="ready",
            runtime_status=status,
            signal_id=str(result.summary.get("signal_id") or ""),
            delivered_user_ids=delivered,
        )
    except Exception as exc:
        refreshed_status = None
        try:
            _clear_runtime_status_cache()
            refreshed_status = await asyncio.to_thread(_ensure_signal_ready_cached, cfg)
        except Exception:
            refreshed_status = None
        source_pending = _signal_now_source_is_pending(
            cfg,
            refreshed_status,
            exc,
        )
        if source_pending:
            _update_signal_now_job(
                key,
                status="waiting_source",
                runtime_status=refreshed_status,
                exc=exc,
            )
        else:
            failed_job = _update_signal_now_job(
                key,
                status="failed",
                runtime_status=refreshed_status,
                exc=exc,
            )
            waiters = _signal_now_job_waiters(key)
            if int(failed_job.get("attempt") or running_job.get("attempt") or 1) <= 1:
                await _send_signal_now_background_failure(waiters, cfg, exc)
    finally:
        bot._signal_now_background_tasks.pop(key, None)
        bot._signal_now_background_waiters.pop(key, None)


def _enqueue_signal_now_background_refresh(
    *,
    user_id: int,
    cfg: LiveMarketConfig,
    runtime_status: MarketRuntimeStatus,
    requested_price_source: str,
    top_n: int,
    min_abs_delta: float,
    debug: bool,
    force_refresh: bool = False,
    mode: str = "signal",
) -> tuple[str, bool]:
    target_date, actual_date = _signal_now_job_data_dates(runtime_status)
    key = _signal_now_background_key(
        cfg,
        target_date=target_date or actual_date,
        requested_price_source=requested_price_source,
        top_n=top_n,
        min_abs_delta=min_abs_delta,
        debug=debug,
        force_refresh=force_refresh,
        mode=mode,
    )
    _register_signal_now_job(
        key,
        user_id=user_id,
        cfg=cfg,
        runtime_status=runtime_status,
        requested_price_source=requested_price_source,
        top_n=top_n,
        min_abs_delta=min_abs_delta,
        debug=debug,
        force_refresh=force_refresh,
        mode=mode,
    )
    bot._signal_now_background_waiters.setdefault(key, set()).add(int(user_id))
    task = bot._signal_now_background_tasks.get(key)
    if task is not None and not task.done():
        return key, False
    bot._signal_now_background_tasks[key] = asyncio.create_task(
        _run_signal_now_background_refresh(
            key,
            market=cfg.market,
            requested_price_source=requested_price_source,
            top_n=top_n,
            min_abs_delta=min_abs_delta,
            debug=debug,
            force_refresh=force_refresh,
            mode=mode,
        )
    )
    return key, True


async def _resume_signal_now_jobs_once() -> None:
    """Resume durable interactive jobs only after their canonical source is ready."""

    if _opening_critical_work_pending():
        return
    if any(
        task is not None and not task.done()
        for task in bot._signal_now_background_tasks.values()
    ):
        return
    for row in _signal_now_resumable_jobs():
        key = str(row.get("key") or "")
        market = str(row.get("market") or "")
        if not key or not market:
            continue
        try:
            cfg = _resolve_market(market)
            status = await asyncio.to_thread(_ensure_signal_ready_cached, cfg)
        except Exception as exc:
            failed_job = _update_signal_now_job(key, status="failed", exc=exc)
            if int(failed_job.get("attempt") or 0) <= 1:
                await _send_signal_now_background_failure(
                    _signal_now_job_waiters(key),
                    SimpleNamespace(market=market),
                    exc,
                )
            return
        if not bool(getattr(status.data, "fresh", False)):
            expected, actual = _signal_now_job_data_dates(status)
            completed_session = _completed_session_signal_path(cfg, status)
            if (
                row.get("status") != "waiting_source"
                or row.get("target_date") != expected
                or row.get("actual_data_date") != actual
                or row.get("waiting_reason") != str(status.data.reason or "source_not_fresh")
            ):
                _update_signal_now_job(
                    key,
                    status="waiting_source",
                    runtime_status=status,
                )
            if not completed_session:
                continue
        user_ids = {
            int(value)
            for value in row.get("user_ids", [])
            if str(value).isdigit() and int(value) > 0
        }
        bot._signal_now_background_waiters.setdefault(key, set()).update(user_ids)
        bot._signal_now_background_tasks[key] = asyncio.create_task(
            _run_signal_now_background_refresh(
                key,
                market=market,
                requested_price_source=str(row.get("requested_price_source") or "auto"),
                top_n=max(MIN_DISCORD_ROWS, int(row.get("top_n") or MIN_DISCORD_ROWS)),
                min_abs_delta=float(row.get("min_abs_delta") or 0.0),
                debug=bool(row.get("debug")),
                force_refresh=bool(row.get("force_refresh")),
                mode=str(row.get("mode") or "signal"),
            )
        )
        # Serialize interactive full-universe work. The next persisted job is
        # picked up on a later loop after this one releases the model lock.
        return


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _market_symbol_names(cfg: LiveMarketConfig) -> dict[str, str]:
    try:
        parquet_root = _market_price_root(cfg)
        if parquet_root is None:
            return {}
        return _symbol_name_map_cached(str(parquet_root))
    except Exception:
        return {}


def _market_price_root(cfg: LiveMarketConfig) -> Path | None:
    try:
        config_path = _resolve_repo_path(cfg.config_path) or Path(cfg.config_path)
        config = _load_experiment_config_cached(str(config_path))
        parquet_root = Path(config.data.parquet_root)
        if not parquet_root.is_absolute():
            parquet_root = ROOT / parquet_root
        return parquet_root
    except Exception:
        return None


def _market_execution_mode(cfg: LiveMarketConfig) -> str:
    try:
        config_path = _resolve_repo_path(cfg.config_path) or Path(cfg.config_path)
        return str(_load_experiment_config_cached(str(config_path)).trading.execution_mode)
    except Exception:
        return "naive"


@lru_cache(maxsize=16)
def _load_experiment_config_cached(config_path: str):
    return load_config(Path(config_path))


@lru_cache(maxsize=16)
def _symbol_name_map_cached(parquet_root: str) -> dict[str, str]:
    return load_symbol_name_map(Path(parquet_root))


def _annotate_history_rows_with_display_time(cfg: LiveMarketConfig, rows: list[dict[str, Any]]) -> None:
    execution_mode = _market_execution_mode(cfg)
    frequency = str(getattr(cfg, "history_frequency", "daily") or "daily").strip().lower()
    day_trade_open_records = execution_mode == "tw_day_trade" and frequency not in {
        "bar",
        "intraday",
        "1m",
        "1min",
        "15m",
        "15min",
        "interval",
    }
    open_time = str(getattr(cfg, "open_time", None) or "09:00").strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", open_time):
        open_time += ":00"
    for row in rows:
        if isinstance(row, dict):
            if row.get("display_date"):
                continue
            value = row.get("date", "n/a")
            if day_trade_open_records:
                day = _date_key(value)
                if day:
                    value = f"{day} {open_time}"
            row["display_date"] = _display_cfg_time(cfg, value)


def _load_stock_history_for_market(
    cfg: LiveMarketConfig,
    symbol: str,
    limit: int,
    changes_only: bool,
    initial_capital: float | None,
    current_capital: float | None,
) -> StockHistoryResult:
    initial, current = _resolve_history_capital_args(
        cfg,
        initial_capital=initial_capital,
        current_capital=current_capital,
    )
    result = load_stock_history(
        _market_fold_dir(cfg),
        symbol,
        limit=limit,
        changes_only=changes_only,
        initial_capital=initial,
        current_capital=current,
        symbol_names=_market_symbol_names(cfg),
        frequency=cfg.history_frequency,
        price_root=_market_price_root(cfg),
        execution_mode=_market_execution_mode(cfg),
    )
    _annotate_history_rows_with_display_time(cfg, result.rows)
    return result


def _history_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip().replace("T", " ")
    if not text or text.lower() in {"none", "null", "nat", "n/a"}:
        return None
    candidates = [text]
    if len(text) >= 19:
        candidates.append(text[:19])
    if len(text) >= 10:
        candidates.append(text[:10])
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
        except Exception:
            continue
        if dt.tzinfo is not None:
            dt = dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        return dt
    return None


def _compound_history_return(rows: list[dict[str, Any]], key: str) -> float | None:
    total = 1.0
    seen = False
    for row in reversed(rows):
        value = _float_or_none(row.get(key))
        if value is None:
            continue
        total *= 1.0 + value
        seen = True
    return total - 1.0 if seen else None


def _refresh_portfolio_history_window(result: PortfolioHistoryResult) -> None:
    rows = list(result.rows)
    cumulative = 1.0
    for row in reversed(rows):
        value = _float_or_none(row.get("portfolio_return"))
        if value is None:
            row["cumulative_return"] = None
        else:
            cumulative *= 1.0 + value
            row["cumulative_return"] = cumulative - 1.0
    result.rows = rows
    result.days = len(rows)
    result.start_date = str(rows[-1].get("date")) if rows else None
    result.end_date = str(rows[0].get("date")) if rows else None
    result.period_return = _compound_history_return(rows, "portfolio_return")
    result.benchmark_return = _compound_history_return(rows, "benchmark_return")
    result.profit_value = sum(float(row.get("profit_value") or 0.0) for row in rows)


def _live_signal_change_row(
    row: dict[str, Any],
    *,
    capital: float | None,
    signal_target: bool = False,
    execution_mode: str = "naive",
) -> dict[str, Any]:
    current_weight = _float_or_none(row.get("current_weight")) or 0.0
    target_weight = _float_or_none(row.get("target_weight")) or 0.0
    delta_weight = _float_or_none(row.get("delta_weight"))
    if delta_weight is None:
        delta_weight = target_weight - current_weight
    current_value = current_weight * capital if capital is not None else None
    target_value = target_weight * capital if capital is not None else None
    delta_value = delta_weight * capital if capital is not None else None
    raw_price_return = None if signal_target else _float_or_none(row.get("price_return"))
    stock_return = _float_or_none(row.get("stock_return"))
    if stock_return is None:
        stock_return = _position_adjusted_return(
            {"current_weight": current_weight, "price_return": raw_price_return},
            ("current_weight",),
        )
    portfolio_contribution = _float_or_none(row.get("portfolio_contribution"))
    if portfolio_contribution is None:
        portfolio_contribution = _portfolio_return_contribution(
            {"current_weight": current_weight, "price_return": raw_price_return},
            ("current_weight",),
        )
    session_open_price = bool(
        signal_target or str(execution_mode).strip().lower() == "tw_day_trade"
    )
    price = (
        row.get("open_price")
        if session_open_price
        else row.get("trade_price", row.get("current_price"))
    )
    if signal_target:
        stock_return = None
        portfolio_contribution = None
    return {
        "symbol": str(row.get("symbol") or ""),
        "name": str(row.get("name") or ""),
        "action": str(row.get("action") or "HOLD"),
        "price": price,
        "entry_price": price if session_open_price else row.get("entry_price"),
        "entry_price_source": "saved_session_open" if session_open_price else None,
        "price_contract": "session_open_target" if session_open_price else "mark_to_mark",
        "intraday_price_included": False if session_open_price else None,
        "price_return": raw_price_return,
        "stock_return": stock_return,
        "portfolio_contribution": portfolio_contribution,
        "market_value": target_value,
        "prev_market_value": current_value,
        "market_value_delta": delta_value,
        "current_weight": current_weight,
        "target_weight": target_weight,
        "holding_ratio": target_weight,
        "prev_holding_ratio": current_weight,
        "holding_ratio_delta": delta_weight,
        "is_live_signal": True,
        "position_source": "signal_target" if signal_target else "executed_history",
        "execution_mode": execution_mode,
    }


def _portfolio_history_accepts_live_signal(summary: dict[str, Any]) -> bool:
    execution_mode = str(summary.get("execution_mode") or "").strip().lower()
    contract = summary.get("signal_price_contract")
    if execution_mode == "tw_day_trade":
        # This applies to every historical/live day-trade artifact, not only
        # preview rows. Old artifacts without an explicit opening-auction
        # contract must never enter portfolio history.
        return (
            bool(summary.get("live_session_open_feature_applied"))
            and isinstance(contract, dict)
            and str(contract.get("model_observation") or "") == "session_open"
            and str(contract.get("history_effective_price") or "") == "session_open"
            and contract.get("intraday_prices_allowed_in_portfolio_history") is False
        )
    return not bool(summary.get("execution_preview_only"))


def _prepend_latest_signal_row_to_portfolio_history(
    result: PortfolioHistoryResult,
    *,
    summary_path: Path,
    summary: dict[str, Any],
    max_rows: int,
) -> bool:
    if not _portfolio_history_accepts_live_signal(summary):
        return False
    signal_target = bool(summary.get("execution_preview_only"))
    day_trade_signal = str(summary.get("execution_mode") or "").strip().lower() == "tw_day_trade"
    signal_date = str(
        summary.get("portfolio_history_effective_date")
        or summary.get("weights_date")
        or summary.get("panel_data_date")
        or summary.get("panel_date")
        or summary.get("asof_date")
        or ""
    ).strip()
    if not signal_date:
        return False
    signal_dt = _history_datetime(signal_date)
    end_dt = _history_datetime(result.end_date)
    if signal_dt is None or (
        end_dt is not None and signal_dt.date() <= end_dt.date()
    ):
        return False

    capital = _float_or_none(summary.get("display_capital"))
    result_capital = getattr(result, "capital", None)
    if capital is None and result_capital is not None:
        capital = _float_or_none(getattr(result_capital, "capital", None))
    portfolio_return = _float_or_none(summary.get("portfolio_simple_return"))
    benchmark_return = (
        None
        if signal_target
        else _float_or_none(summary.get("benchmark_simple_return"))
    )
    profit_value = _float_or_none(summary.get("portfolio_pnl_value"))
    if profit_value is None and capital is not None and portfolio_return is not None:
        profit_value = capital * portfolio_return

    rebalance_path = _summary_artifact_path(summary, "rebalance_path", summary_path)
    weights_path = _summary_artifact_path(summary, "weights_path", summary_path)
    rebalance_rows: list[dict[str, Any]] = []
    weight_rows: list[dict[str, Any]] = []
    if rebalance_path is not None and rebalance_path.exists():
        rebalance_rows = _read_parquet_rows(rebalance_path)
    if weights_path is not None and weights_path.exists():
        weight_rows = _read_parquet_rows(weights_path)

    min_abs_change = _float_or_none(getattr(result, "min_abs_change", None))
    if min_abs_change is None:
        min_abs_change = MIN_DISPLAY_ABS_WEIGHT
    min_abs_change = max(0.0, min_abs_change)
    complete_weight_rows = any(
        {"action", "current_weight", "target_weight"}.issubset(raw)
        and ("delta_weight" in raw or "abs_delta_weight" in raw)
        for raw in weight_rows
    )
    change_source_rows = weight_rows if complete_weight_rows else rebalance_rows
    change_counts: dict[str, int] = {}
    changes_all: list[dict[str, Any]] = []
    for raw in change_source_rows:
        action = str(raw.get("action") or "HOLD").upper()
        if action == "HOLD":
            continue
        delta_weight = _float_or_none(raw.get("delta_weight"))
        if delta_weight is None:
            current_weight = _float_or_none(raw.get("current_weight")) or 0.0
            target_weight = _float_or_none(raw.get("target_weight")) or 0.0
            delta_weight = target_weight - current_weight
        if abs(delta_weight) + 1e-12 < min_abs_change:
            continue
        change_counts[action] = change_counts.get(action, 0) + 1
        changes_all.append(
            _live_signal_change_row(
                raw,
                capital=capital,
                signal_target=signal_target,
                execution_mode=str(summary.get("execution_mode") or "naive"),
            )
        )
    changes_all.sort(key=lambda row: abs(float(row.get("holding_ratio_delta") or 0.0)), reverse=True)

    eps = 1e-9
    target_weights = [_float_or_none(row.get("target_weight")) or 0.0 for row in weight_rows]
    if target_weights:
        position_count = sum(1 for value in target_weights if abs(value) > eps)
        long_count = sum(1 for value in target_weights if value > eps)
        short_count = sum(1 for value in target_weights if value < -eps)
    else:
        top_positions = summary.get("top_positions") if isinstance(summary.get("top_positions"), list) else []
        position_count = len(top_positions)
        long_count = sum(1 for row in top_positions if (_float_or_none(row.get("weight")) or 0.0) > eps)
        short_count = sum(1 for row in top_positions if (_float_or_none(row.get("weight")) or 0.0) < -eps)

    target_risk = summary.get("target_risk") if isinstance(summary.get("target_risk"), dict) else {}
    gross = _float_or_none(target_risk.get("gross"))
    if gross is None:
        gross = _float_or_none(summary.get("target_gross"))
    long_gross = _float_or_none(target_risk.get("long_gross"))
    short_gross = _float_or_none(target_risk.get("short_gross"))
    net = _float_or_none(target_risk.get("net"))
    row = {
        "date": signal_date,
        "display_date": _display_summary_time(
            summary,
            summary.get("portfolio_history_effective_date")
            or summary.get("panel_date")
            or summary.get("asof_date")
            or signal_date,
        ),
        "portfolio_return": portfolio_return,
        "benchmark_return": benchmark_return,
        "turnover": _float_or_none(summary.get("turnover")),
        "profit_value": profit_value,
        "nav": capital,
        "open_nav": capital if signal_target else None,
        "close_nav": None,
        "gross_ratio": gross,
        "net_ratio": net,
        "cash_ratio": max(0.0, 1.0 - gross) if gross is not None else None,
        "long_ratio": long_gross,
        "short_ratio": short_gross,
        "position_count": position_count,
        "long_count": long_count,
        "short_count": short_count,
        "changes": changes_all[: max(0, int(result.top_changes))],
        "change_counts": change_counts,
        "change_count": sum(change_counts.values()),
        "source": "live_signal_open_target" if signal_target else "latest_live_signal",
        "position_source": "signal_target" if signal_target else "executed_history",
        "execution_constraints_complete": summary.get("execution_constraints_complete"),
        "execution_constraints_notice": summary.get("execution_constraints_notice"),
        "price_source": summary.get("price_source"),
        "price_timestamp": summary.get("price_timestamp"),
        "signal_generated_at": summary.get("asof_date"),
        "execution_mode": str(getattr(result, "execution_mode", "naive")),
        "price_contract": "session_open_target" if day_trade_signal else "mark_to_mark",
        "intraday_price_included": False if day_trade_signal else None,
    }

    rows = [row, *result.rows]
    try:
        limit = int(max_rows)
    except Exception:
        limit = int(result.days or 0)
    if limit > 0:
        rows = rows[:limit]
    result.rows = rows
    source_paths = list(result.source_paths)
    for path in (summary_path, weights_path, rebalance_path):
        if path is not None and path.exists() and path not in source_paths:
            source_paths.append(path)
    result.source_paths = tuple(source_paths)
    _refresh_portfolio_history_window(result)
    return True


def _include_live_signals_in_portfolio_history(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
    *,
    max_rows: int,
) -> None:
    def signal_date_key(summary: dict[str, Any]) -> str:
        return str(
            summary.get("weights_date")
            or summary.get("panel_data_date")
            or summary.get("panel_date")
            or summary.get("asof_date")
            or ""
        ).strip()

    def signal_day_key(summary: dict[str, Any]) -> str:
        return _date_key(signal_date_key(summary)) or ""

    def previous_day_key(summary: dict[str, Any]) -> str:
        return (
            _date_key(
                summary.get("previous_weights_data_date")
                or summary.get("previous_weights_date")
                or summary.get("drift_base_data_date")
                or summary.get("drift_base_date")
            )
            or ""
        )

    def signal_sort_key(item: tuple[Path, dict[str, Any]]) -> tuple[datetime, str]:
        path, summary = item
        dt = _history_datetime(signal_date_key(summary))
        return dt or datetime.min, str(path)

    def should_replace_selected(
        current: tuple[Path, dict[str, Any]],
        candidate: tuple[Path, dict[str, Any]],
    ) -> bool:
        current_path, current_summary = current
        candidate_path, candidate_summary = candidate
        current_preview = bool(current_summary.get("execution_preview_only"))
        candidate_preview = bool(candidate_summary.get("execution_preview_only"))
        if current_preview != candidate_preview:
            return not candidate_preview
        if not candidate_preview:
            return candidate_path.stat().st_mtime >= current_path.stat().st_mtime
        current_coverage = int(current_summary.get("opening_price_available_count") or 0)
        candidate_coverage = int(candidate_summary.get("opening_price_available_count") or 0)
        if candidate_coverage != current_coverage:
            return candidate_coverage > current_coverage
        current_time = _history_datetime(current_summary.get("asof_date")) or datetime.max
        candidate_time = _history_datetime(candidate_summary.get("asof_date")) or datetime.max
        return candidate_time < current_time

    latest_by_date: dict[str, tuple[Path, dict[str, Any]]] = {}
    history_ceiling: str | None = None
    try:
        import polars as pl

        live_weights_path = _market_fold_dir(cfg) / "live_signal_weights.parquet"
        if live_weights_path.exists():
            latest_weight_date = (
                pl.scan_parquet(live_weights_path)
                .select(pl.col("date").max())
                .collect()
                .item()
            )
            history_ceiling = _date_key(latest_weight_date)
    except Exception:
        history_ceiling = None
    scan_limit = max(max_rows * 4, max_rows + 16, 64)
    recent_signals = _recent_market_signals(cfg, max_summaries=scan_limit)
    if not recent_signals:
        recent_signals = _market_signals(cfg)
    def collect(signals: list[tuple[Path, dict[str, Any]]]) -> None:
        for summary_path, summary in signals:
            if not _portfolio_history_accepts_live_signal(summary):
                continue
            key = signal_day_key(summary)
            if not key:
                continue
            if (
                history_ceiling is not None
                and key > history_ceiling
            ):
                continue
            current = latest_by_date.get(key)
            candidate = (summary_path, summary)
            if current is None or should_replace_selected(current, candidate):
                latest_by_date[key] = (summary_path, summary)

    collect(recent_signals)
    if not latest_by_date:
        collect(_market_signals(cfg))

    cursor = _date_key(result.end_date)
    for summary_path, summary in sorted(latest_by_date.values(), key=signal_sort_key):
        signal_day = signal_day_key(summary)
        if cursor is not None and previous_day_key(summary) != cursor:
            continue
        history_summary = dict(summary)
        if bool(summary.get("execution_preview_only")):
            open_time = str(getattr(cfg, "open_time", None) or "09:00").strip()
            if len(open_time.split(":")) == 2:
                open_time += ":00"
            history_summary["portfolio_history_effective_date"] = (
                f"{signal_day} {open_time}" if signal_day else signal_date_key(summary)
            )
        inserted = _prepend_latest_signal_row_to_portfolio_history(
            result,
            summary_path=summary_path,
            summary=history_summary,
            max_rows=max_rows,
        )
        if inserted:
            cursor = signal_day


def _include_latest_signal_in_portfolio_history(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
    *,
    max_rows: int,
) -> None:
    _include_live_signals_in_portfolio_history(cfg, result, max_rows=max_rows)


def _load_portfolio_history_for_market(
    cfg: LiveMarketConfig,
    days: int,
    top_changes: int,
    min_abs_change: float,
    initial_capital: float | None,
    current_capital: float | None,
) -> PortfolioHistoryResult:
    initial, current = _resolve_history_capital_args(
        cfg,
        initial_capital=initial_capital,
        current_capital=current_capital,
    )
    result = load_portfolio_history(
        _market_fold_dir(cfg),
        days=days,
        top_changes=top_changes,
        min_abs_change=min_abs_change,
        initial_capital=initial,
        current_capital=current,
        symbol_names=_market_symbol_names(cfg),
        frequency=cfg.history_frequency,
        price_root=_market_price_root(cfg),
        execution_mode=_market_execution_mode(cfg),
    )
    _include_latest_signal_in_portfolio_history(cfg, result, max_rows=days)
    _annotate_history_rows_with_display_time(cfg, result.rows)
    _validate_day_trade_portfolio_history_result(cfg, result)
    return result


def _validate_day_trade_portfolio_history_result(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
) -> None:
    """Fail closed before Discord renders an incorrect day-trade history page."""

    if _market_execution_mode(cfg) != "tw_day_trade":
        return
    open_time = str(getattr(cfg, "open_time", None) or "09:00").strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", open_time):
        open_time += ":00"
    seen_days: set[str] = set()
    for row in result.rows:
        day = _date_key(row.get("date"))
        if not day:
            raise RuntimeError("day-trade portfolio history contains a row without a date")
        if day in seen_days:
            raise RuntimeError(f"day-trade portfolio history contains duplicate day {day}")
        seen_days.add(day)
        display_date = str(row.get("display_date") or "")
        if not display_date.startswith(day) or not display_date.endswith(open_time):
            raise RuntimeError(
                f"day-trade portfolio history {day} must render at session open {open_time}; "
                f"got {display_date!r}"
            )

        signal_target = row.get("position_source") == "signal_target"
        expected_contract = "session_open_target" if signal_target else "session_open_to_close"
        if row.get("price_contract") != expected_contract:
            raise RuntimeError(
                f"day-trade portfolio history {day} has invalid price contract "
                f"{row.get('price_contract')!r}"
            )
        if row.get("intraday_price_included") is not False:
            raise RuntimeError(f"day-trade portfolio history {day} includes an intraday price")
        if signal_target:
            if any(row.get(key) is not None for key in ("portfolio_return", "benchmark_return", "profit_value")):
                raise RuntimeError(
                    f"pending day-trade opening target {day} must not contain return/benchmark/PnL"
                )
        else:
            if row.get("source") != "integer_share_backtest":
                raise RuntimeError(
                    f"executed day-trade history {day} must use integer_share_backtest, "
                    f"got {row.get('source')!r}"
                )
            open_nav = _float_or_none(row.get("open_nav"))
            close_nav = _float_or_none(row.get("close_nav"))
            portfolio_return = _float_or_none(row.get("portfolio_return"))
            profit_value = _float_or_none(row.get("profit_value"))
            if open_nav is None or open_nav <= 0.0 or close_nav is None:
                raise RuntimeError(f"executed day-trade history {day} has invalid open/close NAV")
            if portfolio_return is not None:
                expected_close = open_nav * (1.0 + portfolio_return)
                expected_profit = open_nav * portfolio_return
                if not math.isclose(close_nav, expected_close, rel_tol=1e-10, abs_tol=0.02):
                    raise RuntimeError(f"executed day-trade history {day} close NAV does not match return")
                if profit_value is None or not math.isclose(
                    profit_value,
                    expected_profit,
                    rel_tol=1e-10,
                    abs_tol=0.02,
                ):
                    raise RuntimeError(f"executed day-trade history {day} PnL does not match return")

        changes = row.get("changes") if isinstance(row.get("changes"), list) else []
        for change in changes:
            if not isinstance(change, dict):
                raise RuntimeError(f"day-trade portfolio history {day} contains an invalid change row")
            if change.get("price_contract") != expected_contract:
                raise RuntimeError(
                    f"day-trade portfolio history {day} change {change.get('symbol')} has invalid price contract"
                )
            if change.get("intraday_price_included") is not False:
                raise RuntimeError(
                    f"day-trade portfolio history {day} change {change.get('symbol')} includes an intraday price"
                )
            entry_price = _float_or_none(change.get("entry_price", change.get("price")))
            if entry_price is None or entry_price <= 0.0:
                raise RuntimeError(
                    f"day-trade portfolio history {day} change {change.get('symbol')} has no opening price"
                )
            if not signal_target:
                exit_price = _float_or_none(change.get("exit_price"))
                if exit_price is None or exit_price <= 0.0:
                    raise RuntimeError(
                        f"executed day-trade history {day} change {change.get('symbol')} has no closing price"
                    )


def _stock_history_header_lines(cfg: LiveMarketConfig, result: StockHistoryResult, *, debug: bool = False) -> list[str]:
    label = result.symbol + (f" {result.name}" if result.name else "")
    mode_pairs: list[tuple[str, Any]] = [
        ("freq", cfg.history_frequency),
        ("changes_only", result.changes_only),
    ]
    if result.capital and result.capital.capital is not None:
        mode_pairs.append(("capital", _money(result.capital.capital)))
    header = [
        _kv_line(
            ("market", cfg.market),
            ("symbol", label),
            ("requested", result.requested_symbol),
            ("rows", len(result.rows)),
        ),
        _kv_line(*mode_pairs),
        "說明: stock_ret=個股方向報酬；pnl_contrib=對整體組合報酬貢獻。",
    ]
    if _market_execution_mode(cfg) == "tw_day_trade":
        header.append("當沖契約: 每列只記開盤部位；損益以同日 open->close 結算，收盤後持倉歸零。")
    if debug:
        source_text = _shorten(", ".join(_display_path(path) for path in result.source_paths), 700)
        header.extend(
            [
                _kv_line(
                    ("fold", _display_path(result.fold_dir)),
                    ("fallback_all_rows", result.fell_back_to_all_rows),
                    ("display_tz", _display_tz_text(cfg)),
                ),
                _kv_line(
                    ("capital_mode", result.capital.mode if result.capital else "artifact"),
                    ("capital", _money(result.capital.capital) if result.capital else "n/a"),
                    ("ref", _display_cfg_time(cfg, result.capital.reference_date) if result.capital else "n/a"),
                ),
                f"sources: `{source_text}`",
                "欄位: hold=實際持倉比例；actual=整數股回測權重；model=模型目標權重；Δ=相對上一筆交易日變化。",
            ]
        )
    return header


def _portfolio_history_header_lines(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
    *,
    debug: bool = False,
) -> list[str]:
    start_display = (
        str(result.rows[-1].get("display_date") or result.rows[-1].get("date"))
        if result.rows
        else _display_cfg_time(cfg, result.start_date or "n/a")
    )
    end_display = (
        str(result.rows[0].get("display_date") or result.rows[0].get("date"))
        if result.rows
        else _display_cfg_time(cfg, result.end_date or "n/a")
    )
    profit_pairs: list[tuple[str, Any]] = [("profit", _signed_money(result.profit_value))]
    if result.capital.capital is not None:
        profit_pairs.append(("capital", _money(result.capital.capital)))
    header = [
        _kv_line(
            ("market", cfg.market),
            ("periods", result.days),
            ("freq", result.frequency),
            ("top_changes", result.top_changes),
        ),
        _kv_line(
            ("period", f"{start_display}..{end_display}"),
            ("ret", _signed_pct(result.period_return)),
            ("benchmark", _signed_pct(result.benchmark_return)),
        ),
        _kv_line(*profit_pairs),
        "說明: stock_ret=個股方向報酬；pnl_contrib=對整體組合報酬貢獻。",
    ]
    if str(getattr(result, "execution_mode", "naive")) == "tw_day_trade":
        header.append(
            "當沖契約: 每列記錄當日開盤成交；ret/pnl 依同日 open->close（含費用）結算。"
        )
    if any(row.get("position_source") == "signal_target" for row in result.rows):
        header.append(
            "今日 `source=signal_target` 是已觀察開盤價後的模型目標；尚未有整數成交與收盤損益，不計入期間報酬。"
        )
    if debug:
        source_text = _shorten(", ".join(_display_path(path) for path in result.source_paths), 700)
        header.extend(
            [
                _kv_line(("fold", _display_path(result.fold_dir)), ("display_tz", _display_tz_text(cfg))),
                _kv_line(
                    ("capital_mode", result.capital.mode),
                    ("capital", _money(result.capital.capital)),
                    ("ref", _display_cfg_time(cfg, result.capital.reference_date or "n/a")),
                ),
                f"sources: `{source_text}`",
                (
                    "欄位: pnl=當日開盤 NAV x 同日 open->close 報酬；"
                    "cum=本查詢期間累積報酬；top=開盤部位變動最大的標的。"
                    if str(getattr(result, "execution_mode", "naive")) == "tw_day_trade"
                    else "欄位: pnl≈前一期 NAV x 本期報酬估算；cum=本查詢期間累積報酬；"
                    "top=本期絕對持倉比例變動最大的標的。"
                ),
            ]
        )
    return header


def _position_line(row: dict[str, Any]) -> str:
    action = str(row.get("action") or "").strip().upper()
    label = _symbol_label(row)
    if action and action != "HOLD":
        label = f"{label} **{action}**"
    lines = [
        label,
        _kv_line(
            ("now", _pct(row.get("current_weight"))),
            ("target", _pct(row.get("target_weight"))),
            ("delta", _signed_pct(row.get("delta_weight"))),
        ),
    ]
    if _float_or_none(row.get("target_value")) is not None:
        lines.append(
            _kv_line(
                ("target_value", _money(row.get("target_value"))),
                ("current_value", _money(row.get("current_value"))),
                ("delta_value", _signed_money(row.get("delta_value"))),
            )
        )
    lines.append(
        _kv_line(
            ("px", _price(row.get("current_price"))),
            ("raw_score", _num(_row_raw_score(row), 4)),
        )
    )
    status = _position_status_label(row)
    if status:
        lines.append(f"狀態: `{status}`")
    lines.append(_return_pnl_line(row, ("current_weight", "holding_ratio", "target_weight")))
    return "\n".join(lines)


def _rebalance_line(row: dict[str, Any]) -> str:
    delta = _float_or_none(row.get("delta_weight")) or 0.0
    side = str(row.get("action") or ("BUY" if delta > 0 else "SELL"))
    lines = [
        f"{_symbol_label(row)} **{side}**",
        _kv_line(
            ("delta", _signed_pct(delta)),
            ("px", _price(row.get("trade_price", row.get("current_price")))),
        ),
        _kv_line(
            ("now", _pct(row.get("current_weight"))),
            ("target", _pct(row.get("target_weight"))),
            ("raw_score", _num(_row_raw_score(row), 4)),
        ),
        _return_pnl_line(row, ("current_weight", "holding_ratio", "target_weight")),
    ]
    status = _position_status_label(row)
    if status:
        lines.append(f"狀態: `{status}`")
    if _float_or_none(row.get("delta_value")) is not None:
        lines.append(
            _kv_line(
                ("delta_value", _signed_money(row.get("delta_value"))),
                ("current_value", _money(row.get("current_value"))),
                ("target_value", _money(row.get("target_value"))),
            )
        )
    return "\n".join(lines)


def _stock_history_block(row: dict[str, Any]) -> str:
    shares = int(_float_or_none(row.get("shares")) or 0)
    prev_shares = int(_float_or_none(row.get("prev_shares")) or 0)
    share_delta = int(_float_or_none(row.get("share_delta")) or 0)
    price_pairs: list[tuple[str, Any]] = []
    if str(row.get("execution_mode") or "").lower() == "tw_day_trade":
        price_pairs.extend(
            [
                ("open", _price(row.get("entry_price", row.get("price")))),
                ("close", _price(row.get("exit_price"))),
            ]
        )
    else:
        price_pairs.append(("px", _price(row.get("price"))))
    lines = [
            f"`{row.get('display_date', row.get('date', 'n/a'))}` **{row.get('action', 'HOLD')}**",
            _kv_line(
                ("shares", f"{prev_shares}->{shares}"),
                ("delta", f"{share_delta:+d}"),
                *price_pairs,
            ),
            _kv_line(
                ("hold", _pct(row.get("holding_ratio"))),
                ("delta", _signed_pct(row.get("holding_ratio_delta"))),
                ("actual", _pct(row.get("actual_weight"))),
            ),
            _kv_line(
                ("model", _pct(row.get("model_weight"))),
                ("model_delta", _signed_pct(row.get("model_weight_delta"))),
                ("mv", _money(row.get("market_value"))),
                ("delta_mv", _signed_money(row.get("market_value_delta"))),
            ),
            _kv_line(
                ("portfolio", _signed_pct(row.get("portfolio_return"))),
                ("benchmark", _signed_pct(row.get("benchmark_return"))),
                ("turnover", _pct(row.get("turnover"))),
            ),
            _return_pnl_line(row, ("prev_holding_ratio", "holding_ratio", "current_weight", "target_weight")),
        ]
    if row.get("position_source") == "signal_target":
        lines.insert(1, "`source=signal_target` 尚未有同日整數成交/持倉紀錄")
    return "\n".join(lines)


def _portfolio_change_counts(row: dict[str, Any]) -> str:
    counts = row.get("change_counts")
    if not isinstance(counts, dict) or not counts:
        return "none"
    parts = [f"{key}={counts[key]}" for key in sorted(counts)]
    return " ".join(parts)


def _portfolio_change_line(row: dict[str, Any]) -> str:
    label = _symbol_label(row)
    pnl_weight_keys = (
        ("current_weight", "holding_ratio", "target_weight")
        if row.get("is_live_signal")
        else ("prev_holding_ratio", "holding_ratio", "current_weight", "target_weight")
    )
    parts = [
        f"{label} {row.get('action', 'HOLD')}",
        f"Δhold={_signed_pct(row.get('holding_ratio_delta'))}",
        f"hold={_pct(row.get('holding_ratio'))}",
        f"stock_ret={_signed_pct_zero_plain(_position_adjusted_return(row, pnl_weight_keys))}",
        f"pnl_contrib={_signed_pct_zero_plain(_portfolio_return_contribution(row, pnl_weight_keys))}",
    ]
    if _float_or_none(row.get("market_value")) is not None:
        parts.append(f"value={_money(row.get('market_value'))}")
    if _float_or_none(row.get("market_value_delta")) is not None:
        parts.append(f"Δvalue={_signed_money(row.get('market_value_delta'))}")
    if row.get("shares") is not None or row.get("share_delta") is not None:
        parts.append(f"shares={int(_float_or_none(row.get('shares')) or 0)}")
        parts.append(f"Δsh={int(_float_or_none(row.get('share_delta')) or 0):+d}")
    if str(row.get("execution_mode") or "").lower() == "tw_day_trade":
        parts.append(f"open={_price(row.get('entry_price', row.get('price')))}")
        parts.append(f"close={_price(row.get('exit_price'))}")
    else:
        parts.append(f"px={_price(row.get('price'))}")
    return " ".join(parts)


def _portfolio_change_block(row: dict[str, Any], index: int) -> str:
    label = _symbol_label(row)
    action = str(row.get("action") or "HOLD").strip().upper() or "HOLD"
    pnl_weight_keys = (
        ("current_weight", "holding_ratio", "target_weight")
        if row.get("is_live_signal")
        else ("prev_holding_ratio", "holding_ratio", "current_weight", "target_weight")
    )
    day_trade = str(row.get("execution_mode") or "").lower() == "tw_day_trade"
    position_pairs: list[tuple[str, Any]] = []
    # A day-trade history row always starts flat. Its holding, value, and shares
    # therefore already equal their respective deltas; displaying both copies
    # wastes most of a one-day Discord page without adding information.
    if not day_trade:
        position_pairs.append(("Δhold", _signed_pct(row.get("holding_ratio_delta"))))
    position_pairs.extend(
        [
            ("hold", _pct(row.get("holding_ratio"))),
            ("stock_ret", _signed_pct_zero_plain(_position_adjusted_return(row, pnl_weight_keys))),
            ("pnl_contrib", _signed_pct_zero_plain(_portfolio_return_contribution(row, pnl_weight_keys))),
        ]
    )
    if day_trade:
        lines = [f"    {index}. {label} **{action}**  " + _kv_inline(*position_pairs)]
    else:
        lines = [
            f"    {index}. {label} **{action}**",
            "       " + _kv_inline(*position_pairs),
        ]
    value_pairs: list[tuple[str, Any]] = []
    if _float_or_none(row.get("market_value")) is not None:
        value_pairs.append(("value", _money(row.get("market_value"))))
    if not day_trade and _float_or_none(row.get("market_value_delta")) is not None:
        value_pairs.append(("Δvalue", _signed_money(row.get("market_value_delta"))))
    if row.get("shares") is not None or row.get("share_delta") is not None:
        value_pairs.append(("shares", int(_float_or_none(row.get("shares")) or 0)))
        if not day_trade:
            value_pairs.append(("Δsh", f"{int(_float_or_none(row.get('share_delta')) or 0):+d}"))
    if day_trade:
        value_pairs.append(("open", _price(row.get("entry_price", row.get("price")))))
        value_pairs.append(("close", _price(row.get("exit_price"))))
    else:
        value_pairs.append(("px", _price(row.get("price"))))
    lines.append("       " + _kv_inline(*value_pairs))
    return "\n".join(lines)


def _portfolio_history_block(row: dict[str, Any]) -> str:
    changes = row.get("changes")
    change_rows = changes if isinstance(changes, list) else []
    day_trade = str(row.get("execution_mode") or "").strip().lower() == "tw_day_trade"
    signal_target = row.get("position_source") == "signal_target"
    position_pairs: list[tuple[str, Any]] = [
        ("pos", row.get("position_count", "n/a")),
        ("long", row.get("long_count", "n/a")),
        ("short", row.get("short_count", "n/a")),
    ]
    if not day_trade:
        position_pairs.append(("changes", row.get("change_count", 0)))
    lines = [
        f"`{row.get('display_date', row.get('date', 'n/a'))}`",
        _kv_line(
            ("ret", _signed_pct(row.get("portfolio_return"))),
            ("bench", _signed_pct(row.get("benchmark_return"))),
            ("pnl", _signed_money(row.get("profit_value"))),
        ),
        (
            _kv_line(
                ("cum", _signed_pct(row.get("cumulative_return"))),
                ("target_turnover" if signal_target else "turnover", _pct(row.get("turnover"))),
                ("open_nav", _money(row.get("open_nav", row.get("nav")))),
                ("close_nav", _money(row.get("close_nav"))),
            )
            if day_trade
            else _kv_line(
                ("cum", _signed_pct(row.get("cumulative_return"))),
                ("turnover", _pct(row.get("turnover"))),
                ("nav", _money(row.get("nav"))),
            )
        ),
        _kv_line(
            (
                "target_gross" if signal_target else "open_gross" if day_trade else "gross",
                _pct(row.get("gross_ratio")),
            ),
            (
                "target_net" if signal_target else "open_net" if day_trade else "net",
                _signed_pct(row.get("net_ratio")),
            ),
            (
                "target_cash" if signal_target else "open_cash" if day_trade else "cash",
                _pct(row.get("cash_ratio")),
            ),
        ),
        _kv_line(*position_pairs),
        f"  change_mix: `{_portfolio_change_counts(row)}`",
    ]
    if signal_target:
        lines.insert(
            1,
            "`source=signal_target` 已觀察當日開盤價的模型目標；尚未有同日整數成交或收盤損益。",
        )
        if row.get("execution_constraints_complete") is False:
            notice = _shorten(row.get("execution_constraints_notice"), 260)
            lines.insert(2, f"  限制: `{notice or '同日官方當沖資格尚未完整套用'}`")
    requested_gross = _float_or_none(row.get("requested_gross_ratio"))
    if requested_gross is not None:
        executed_gross = _float_or_none(row.get("gross_ratio")) or 0.0
        lines.insert(
            4,
            _kv_line(
                ("model_gross", _pct(requested_gross)),
                ("open_executed", _pct(executed_gross)),
                ("fill", _pct(row.get("execution_fill_ratio"))),
                ("model_pos", row.get("requested_position_count", "n/a")),
            ),
        )
        if day_trade and requested_gross > 1e-12 and executed_gross <= 1e-12:
            lines.insert(
                5,
                "  未成交: 開盤目標受到前一交易日成交量、整張股數、交易遮罩與可用資金限制。",
            )
    displayed_changes = change_rows
    if displayed_changes:
        total_changes = int(row.get("change_count") or len(displayed_changes))
        lines.append(f"  top changes: `{len(displayed_changes)}/{total_changes}`")
        for index, item in enumerate(displayed_changes, start=1):
            if isinstance(item, dict):
                lines.append(_portfolio_change_block(item, index))
    else:
        lines.append("  top changes: none")
    return "\n".join(lines)


def _portfolio_history_pages(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
    *,
    debug: bool = False,
) -> list[str]:
    """Render exactly one history period per Discord page without splitting it."""

    rows = list(result.rows)
    if not rows:
        return [f"**portfolio history**\n(no rows)\n\n{INVESTMENT_WARNING}"]
    _validate_day_trade_portfolio_history_result(cfg, result)
    max_chars = 4000
    page_count = len(rows)
    pages: list[str] = []
    for page_index, source_row in enumerate(rows, start=1):
        source_changes = (
            [dict(item) for item in source_row.get("changes", []) if isinstance(item, dict)]
            if isinstance(source_row.get("changes"), list)
            else []
        )

        row = dict(source_row)
        row["changes"] = source_changes
        lines = [
            f"**portfolio history · {cfg.market}**  `page={page_index}/{page_count}`",
            "",
            _portfolio_history_block(row),
        ]
        if debug:
            lines.append(
                _kv_line(
                    ("fold", _display_path(result.fold_dir)),
                    ("sources", len(result.source_paths)),
                    ("display_tz", _display_tz_text(cfg)),
                )
            )
        _append_investment_warning(lines)
        page = "\n".join(lines)
        if len(page) > max_chars:
            day = source_row.get("display_date", source_row.get("date", "n/a"))
            raise RuntimeError(
                f"portfolio history day {day} cannot fit all {len(source_changes)} top changes "
                f"in one Discord page ({len(page)}>{max_chars})"
            )
        pages.append(page)
    if len(pages) != len(rows):
        raise RuntimeError("portfolio history pagination must produce exactly one page per period")
    return pages


def _decision_line(row: dict[str, Any]) -> str:
    constraint = str(row.get("constraint") or "")
    constraint_text = f" constraint=`{constraint}`" if constraint else ""
    status = _position_status_label(row)
    status_text = f" status=`{status}`" if status else ""
    return (
        f"{_symbol_label(row)} **{row.get('action', 'HOLD')}** "
        f"delta=`{_signed_pct(row.get('delta_weight'))}` "
        f"target=`{_pct(row.get('target_weight'))}` "
        f"score=`{_num(row.get('score'), 3)}` "
        f"rank=`{row.get('abs_score_rank', 'n/a')}` "
        f"px=`{_price(row.get('trade_price', row.get('current_price')))}` "
        f"stock_ret=`{_signed_pct_zero_plain(_position_adjusted_return(row, ('current_weight', 'holding_ratio', 'target_weight')))}` "
        f"pnl_contrib=`{_signed_pct_zero_plain(_portfolio_return_contribution(row, ('current_weight', 'holding_ratio', 'target_weight')))}` "
        f"{constraint_text} "
        f"{status_text} "
        f"reason=`{_shorten(row.get('decision_reason', ''), 100)}`"
    )


def _decision_block(row: dict[str, Any]) -> str:
    constraint = str(row.get("constraint") or "").strip()
    constraint_text = constraint if constraint else "none"
    return "\n".join(
        [
            f"{_symbol_label(row)} **{row.get('action', 'HOLD')}**",
            _kv_line(
                ("now", _pct(row.get("current_weight"))),
                ("model", _pct(row.get("model_weight"))),
                ("target", _pct(row.get("target_weight"))),
                ("delta", _signed_pct(row.get("delta_weight"))),
            ),
            _kv_line(
                ("px", _price(row.get("trade_price", row.get("current_price")))),
                ("raw_score", _num(_row_raw_score(row), 4)),
                ("score", _num(row.get("score"), 4)),
                ("rank", row.get("abs_score_rank", "n/a")),
            ),
            _return_pnl_line(row, ("current_weight", "holding_ratio", "target_weight")),
            _kv_line(
                ("target_rank", row.get("abs_target_rank", "n/a")),
                ("gate", _num(row.get("stock_market_gate"), 3)),
                ("market_delta", _num(row.get("market_delta_norm"), 3)),
            ),
            _kv_line(
                ("tradable", row.get("tradable")),
                ("can_buy", row.get("can_buy")),
                ("can_sell", row.get("can_sell")),
                ("constraint", constraint_text),
            ),
            f"  status: `{_position_status_label(row) or '一般'}`",
            f"  reason: `{_shorten(row.get('decision_reason', ''), 220)}`",
        ]
    )


def _action_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        action = str(row.get("action") or "UNKNOWN").upper()
        counts[action] = counts.get(action, 0) + 1
    return counts


def _action_count_text(rows: list[dict[str, Any]]) -> str:
    counts = _action_counts(rows)
    parts = [f"{action}={counts.get(action, 0)}" for action in ("BUY", "SELL", "REDUCE", "EXIT", "HOLD")]
    extra = sorted(key for key in counts if key not in {"BUY", "SELL", "REDUCE", "EXIT", "HOLD"})
    parts.extend(f"{key}={counts[key]}" for key in extra)
    return " ".join(parts)


def _filter_decision_rows(
    rows: list[dict[str, Any]],
    *,
    symbol: str = "",
    action: str = "actionable",
    actionable_only: bool = True,
) -> list[dict[str, Any]]:
    symbol_query = str(symbol or "").strip().lower()
    action_query = str(action or ("actionable" if actionable_only else "all")).strip().lower()
    if not action_query:
        action_query = "actionable" if actionable_only else "all"
    filtered = list(rows)
    if symbol_query:
        filtered = [
            row
            for row in filtered
            if symbol_query in str(row.get("symbol") or "").lower()
            or symbol_query in str(row.get("name") or "").lower()
        ]
    if action_query in {"actionable", "trade", "trades"}:
        filtered = [row for row in filtered if str(row.get("action") or "").upper() != "HOLD"]
    elif action_query not in {"all", "any", "*"}:
        wanted = action_query.upper()
        filtered = [row for row in filtered if str(row.get("action") or "").upper() == wanted]
    return filtered


def _sort_decision_rows(rows: list[dict[str, Any]], sort_by: str) -> list[dict[str, Any]]:
    key = str(sort_by or "delta").strip().lower()
    if key in {"score", "abs_score"}:
        sort_key = lambda row: (_row_abs(row, "score"), _row_abs(row, "delta_weight"), _row_abs(row, "target_weight"))
    elif key in {"target", "weight", "abs_target"}:
        sort_key = lambda row: (_row_abs(row, "target_weight"), _row_abs(row, "delta_weight"), _row_abs(row, "score"))
    elif key in {"return", "price_return", "ret"}:
        sort_key = lambda row: (_row_abs(row, "price_return"), _row_abs(row, "delta_weight"), _row_abs(row, "score"))
    elif key in {"rank", "score_rank"}:
        def sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
            rank = _float_or_none(row.get("abs_score_rank"))
            rank_score = -rank if rank is not None else float("-inf")
            return (rank_score, _row_abs(row, "delta_weight"), _row_abs(row, "target_weight"))
    else:
        sort_key = lambda row: (_row_abs(row, "delta_weight"), _row_abs(row, "target_weight"), _row_abs(row, "score"))
    return sorted(rows, key=sort_key, reverse=True)


def _driver_line(row: dict[str, Any], *, feature: bool = False) -> str:
    if feature:
        return f"{row.get('feature')}: {_num(row.get('weighted_abs_value'), 4)}"
    label = str(row.get("symbol") or "")
    name = str(row.get("name") or "").strip()
    if name:
        label += f" {name}"
    return f"{label}: score={_num(row.get('score'), 4)} target={_pct(row.get('target_weight'))}"


def _decision_overview_page(
    *,
    summary: dict[str, Any],
    summary_path: Path,
    explain_path: Path,
    rows_all: list[dict[str, Any]],
    rows_filtered: list[dict[str, Any]],
    symbol: str,
    action: str,
    sort_by: str,
    debug: bool = False,
) -> str:
    explanation = summary.get("model_explanation") if isinstance(summary.get("model_explanation"), dict) else {}
    features = explanation.get("top_feature_drivers") if isinstance(explanation.get("top_feature_drivers"), list) else []
    scores = explanation.get("top_score_drivers") if isinstance(explanation.get("top_score_drivers"), list) else []
    report_path = summary.get("decision_report_path") or summary.get("decision_explanation_markdown_path") or str(explain_path)
    lines = [
        f"**decision explanation**",
        f"`{summary.get('signal_id', summary_path.parent.name)}`",
        _kv_line(
            ("market", summary.get("market", "n/a")),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
        ),
        _kv_line(
            ("rows", f"{len(rows_filtered)}/{len(rows_all)}"),
            ("symbol", symbol or "all"),
            ("action", action or "actionable"),
            ("sort", sort_by or "delta"),
        ),
        "",
        "**action mix**",
        _kv_line(("all", _action_count_text(rows_all))),
        _kv_line(("filtered", _action_count_text(rows_filtered))),
        "",
        "**model context**",
        _kv_line(("confidence", _num(explanation.get("confidence_proxy_score_std"), 4))),
    ]
    if debug:
        lines.extend(
            [
                _kv_line(
                    ("fold", summary.get("fold_id", "n/a")),
                    ("display_tz", summary.get("display_timezone_label") or display_timezone_label(summary.get("display_timezone"))),
                ),
                _kv_line(("source", _shorten(explanation.get("source", "score/weight decision table"), 80))),
            ]
        )
    if features:
        lines.append("  feature drivers:")
        lines.extend(f"    {index}. {_driver_line(row, feature=True)}" for index, row in enumerate(features[:5], start=1) if isinstance(row, dict))
    if scores:
        lines.append("  score drivers:")
        lines.extend(f"    {index}. {_driver_line(row)}" for index, row in enumerate(scores[:5], start=1) if isinstance(row, dict))
    lines.extend(
        [
            "",
            "**欄位說明**",
            "score=模型排序分數；model=交易約束前權重；target=最終目標權重；delta=要調整的權重。",
            "gate/market_delta=Transformer 市場脈絡影響；constraint=買賣/交易限制。",
        ]
    )
    if debug:
        lines.extend(
            [
                "",
                "**files**",
                f"report: `{report_path}`",
                f"table: `{explain_path}`",
            ]
        )
    _append_investment_warning(lines)
    return "\n".join(lines)


def _daily_summary_message(cfg: LiveMarketConfig, *, debug: bool = False) -> str:
    status = _runtime_status_for_display(cfg)
    latest = _latest_market_signal(cfg)
    lines = [
        f"**daily summary** {cfg.label}",
        _kv_line(("status", status.status), ("generated", "yes" if latest else "no")),
        _kv_line(
            ("data", _display_cfg_time(cfg, status.data.last_data_date or "n/a")),
            ("panel", _display_cfg_time(cfg, status.data.panel_date or "n/a")),
            ("benchmark", _display_cfg_time(cfg, status.data.benchmark_date or "n/a")),
        ),
    ]
    if debug:
        lines.append(_kv_line(("display_tz", _display_tz_text(cfg))))
    if not status.data.fresh:
        lines.append(f"warning: 資料過期，目前不建議使用。 `{status.data.reason or 'stale'}`")
    notice = _market_notice(status)
    if notice:
        lines.append(f"notice: {notice}")
    if latest is not None:
        path, summary = latest
        summary = _summary_with_capital_context(cfg, summary)
        portfolio_return = _float_or_none(summary.get("portfolio_simple_return"))
        baseline_return = _float_or_none(summary.get("benchmark_simple_return"))
        excess_return = None if portfolio_return is None or baseline_return is None else portfolio_return - baseline_return
        lines.extend(
            [
                "",
                "**latest signal**",
                _kv_line(
                    ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
                    ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
                ),
                _kv_line(
                    ("portfolio", _signed_pct(portfolio_return)),
                    ("baseline", _signed_pct(baseline_return)),
                    ("excess", _signed_pct(excess_return)),
                    ("turnover", _pct(summary.get("turnover"))),
                ),
            ]
        )
        if debug:
            lines.extend(
                [
                    _kv_line(
                        ("signal", summary.get("signal_id", path.parent.name)),
                        ("fold", summary.get("fold_id", "n/a")),
                    ),
                    f"artifact: `{path}`",
                ]
            )
        if _float_or_none(summary.get("portfolio_pnl_value")) is not None:
            lines.append(
                _kv_line(
                    ("capital", _money(summary.get("display_capital"))),
                    ("pnl", _signed_money(summary.get("portfolio_pnl_value"))),
                    ("baseline_pnl", _signed_money(summary.get("benchmark_pnl_value"))),
                    ("excess_pnl", _signed_money(summary.get("excess_pnl_value"))),
                )
            )
        recent = summary.get("recent_performance") if isinstance(summary.get("recent_performance"), dict) else {}
        if recent:
            lines.append(
                _kv_line(
                    ("period", recent.get("window_label") or f"過去{recent.get('window_days', 'n')}期"),
                    ("strategy", _signed_pct(recent.get("strategy_return"))),
                    ("baseline", _signed_pct(recent.get("benchmark_return"))),
                    ("excess", _signed_pct(recent.get("excess_return"))),
                )
            )
            if _float_or_none(recent.get("strategy_pnl_value")) is not None:
                lines.append(
                    _kv_line(
                        ("pnl", _signed_money(recent.get("strategy_pnl_value"))),
                        ("baseline_pnl", _signed_money(recent.get("benchmark_pnl_value"))),
                        ("excess_pnl", _signed_money(recent.get("excess_pnl_value"))),
                    )
                )
        warnings = summary.get("risk_warnings") if isinstance(summary.get("risk_warnings"), list) else []
        if warnings:
            lines.append("risk warning: " + " | ".join(str(item) for item in warnings[:3]))
        top = summary.get("top_positions") if isinstance(summary.get("top_positions"), list) else []
        top = [
            row
            for row in top
            if isinstance(row, dict)
            and is_display_position_row(row)
        ]
        if top:
            lines.append("top positions:")
            lines.extend(
                f"  {index}. {_symbol_label(row)} `{_pct(row.get('weight'))}`"
                for index, row in enumerate(top[:5], start=1)
            )
    _append_investment_warning(lines)
    return "\n".join(lines)


class PagedTextView(discord.ui.View):
    def __init__(self, pages: list[str], *, timeout: float | None = 30 * 60) -> None:
        super().__init__(timeout=timeout)
        self.pages = pages
        self.index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        last = len(self.pages) - 1
        for item in self.children:
            if not isinstance(item, discord.ui.Button):
                continue
            if item.custom_id in {"page_first", "page_prev"}:
                item.disabled = self.index <= 0
            elif item.custom_id in {"page_next", "page_last"}:
                item.disabled = self.index >= last

    async def _show(self, interaction: discord.Interaction) -> None:
        self._sync_buttons()
        try:
            await interaction.response.edit_message(
                **_discord_page_kwargs(self.pages[self.index]),
                view=self,
            )
        except discord.NotFound:
            return
        except discord.HTTPException as exc:
            if getattr(exc, "status", None) == 404:
                return
            raise

    @discord.ui.button(label="<<", style=discord.ButtonStyle.secondary, custom_id="page_first")
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        self.index = 0
        await self._show(interaction)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, custom_id="page_prev")
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        self.index = max(0, self.index - 1)
        await self._show(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, custom_id="page_next")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        self.index = min(len(self.pages) - 1, self.index + 1)
        await self._show(interaction)

    @discord.ui.button(label=">>", style=discord.ButtonStyle.secondary, custom_id="page_last")
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        self.index = len(self.pages) - 1
        await self._show(interaction)


class SignalReviewView(discord.ui.View):
    def __init__(self, *, signal_id: str, market: str) -> None:
        super().__init__(timeout=24 * 60 * 60)
        self.signal_id = signal_id
        self.market = market

    async def _handle(self, interaction: discord.Interaction, action: str, *, restricted: bool = False) -> None:
        cfg = _resolve_market(self.market)
        try:
            if restricted:
                _require_trader_permission(interaction, cfg)
        except BotUserError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return
        _record_audit_event(self.signal_id, action, interaction, market=self.market)
        await interaction.response.send_message(f"`{self.signal_id}` 已記錄 `{action}`。", ephemeral=True)

    @discord.ui.button(label="acknowledge", style=discord.ButtonStyle.primary)
    async def acknowledge(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self._handle(interaction, "acknowledge")

    @discord.ui.button(label="skip today", style=discord.ButtonStyle.secondary)
    async def skip_today(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self._handle(interaction, "skip_today", restricted=True)

    @discord.ui.button(label="mark reviewed", style=discord.ButtonStyle.success)
    async def mark_reviewed(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        del button
        await self._handle(interaction, "mark_reviewed")


@bot.tree.command(name="ask", description="詢問 Otto Suwen")
@app_commands.allowed_installs(guilds=True, users=True)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.describe(question="你的問題")
async def ask(interaction: discord.Interaction, question: str) -> None:
    await interaction.response.send_message(f"你的問題：{question}")


@bot.tree.command(name="guide", description="Show the investor-friendly stockAgent command guide.")
async def guide(interaction: discord.Interaction) -> None:
    await interaction.response.defer(thinking=True, ephemeral=True)
    await _send_long_response(interaction, _guide_message())


@bot.tree.command(name="subscribe", description="Manage personal scheduled signal alerts.")
@app_commands.describe(
    action="add/remove/list/clear",
    market="Market id. Omit with list, or with clear to clear all.",
    watchlist_only="Only DM changes that match your watchlist. Recommended.",
)
@app_commands.choices(
    action=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="remove", value="remove"),
        app_commands.Choice(name="list", value="list"),
        app_commands.Choice(name="clear", value="clear"),
    ]
)
@app_commands.autocomplete(market=market_autocomplete)
async def subscribe(
    interaction: discord.Interaction,
    action: str,
    market: str = "",
    watchlist_only: bool = True,
) -> None:
    user_id = getattr(interaction.user, "id", None)
    action_value = str(action or "").strip().lower()
    try:
        market_text = str(market or "").strip()
        if action_value == "add":
            cfg = _resolve_market(market_text)
            subscriptions = _set_user_subscription(user_id, cfg.market, watchlist_only=watchlist_only)
            verb = f"`{cfg.market}` subscribed"
        elif action_value in {"remove", "delete", "del"}:
            cfg = _resolve_market(market_text)
            subscriptions = _remove_user_subscription(user_id, cfg.market)
            verb = f"`{cfg.market}` unsubscribed"
        elif action_value == "clear":
            if market_text:
                cfg = _resolve_market(market_text)
                subscriptions = _remove_user_subscription(user_id, cfg.market)
                verb = f"`{cfg.market}` unsubscribed"
            else:
                subscriptions = _clear_user_subscriptions(user_id)
                verb = "all subscriptions cleared"
        elif action_value == "list":
            subscriptions = _user_subscriptions(user_id)
            verb = "current subscriptions"
        else:
            raise BotUserError("action 必須是 add/remove/list/clear。")
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    _record_audit_event(
        f"subscribe:{_user_state_key(user_id)}",
        f"subscribe_{action_value}",
        interaction,
        market=str(market or ""),
        watchlist_only=watchlist_only,
        subscriptions=subscriptions,
    )
    lines = [f"**subscribe** {verb}", *_subscription_summary_lines(user_id)]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@bot.tree.command(name="latest", description="Show the latest saved signal without rerunning inference.")
@app_commands.describe(
    market="Market id",
    top_n="Rows to show in top positions and changes.",
    current_capital="Current account capital used to estimate PnL.",
    debug="Show signal ids, fingerprints, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def latest(
    interaction: discord.Interaction,
    market: str = "",
    top_n: int = 8,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        message = await asyncio.to_thread(
            lambda: _latest_signal_message(
                cfg,
                *_latest_signal_or_raise(cfg),
                top_n=max(0, int(top_n or 0)),
                current_capital=current_capital,
                debug=debug,
            )
        )
    except Exception as exc:
        await _send_command_error(interaction, "latest", exc)
        return
    await _send_long_response(interaction, message)


@bot.tree.command(name="changes", description="Show latest actionable rebalance changes.")
@app_commands.describe(
    market="Market id",
    action="actionable/all/BUY/SELL/REDUCE/EXIT/HOLD.",
    limit="Max rows to show. 0 means all matching rows.",
    page_size="Rows per page, clamped to 10-40.",
    watchlist_only="Only show symbols in your watchlist for this market.",
    current_capital="Current account capital used to estimate trade amounts.",
    debug="Show signal id, display timezone, and artifact path.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def changes(
    interaction: discord.Interaction,
    market: str = "",
    action: str = "actionable",
    limit: int = 0,
    page_size: int = 20,
    watchlist_only: bool = False,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        watchlist = _user_watchlist(getattr(interaction.user, "id", None), cfg.market) if watchlist_only else []
        if watchlist_only and not watchlist:
            await interaction.followup.send(f"`{cfg.market}` 你的 watchlist 是空的，先用 `/watch action:add symbol:<代號>` 加入。")
            return
        pages = await asyncio.to_thread(
            lambda: _latest_changes_pages(
                cfg,
                *_latest_signal_or_raise(cfg),
                action=action,
                limit=limit,
                page_size=page_size,
                current_capital=current_capital,
                watchlist=watchlist,
                debug=debug,
            )
        )
    except Exception as exc:
        await _send_command_error(interaction, "changes", exc)
        return
    await _send_paginated_response(interaction, pages)


@bot.tree.command(name="performance", description="Show strategy performance versus baseline.")
@app_commands.describe(
    market="Market id",
    days="Artifact history window to compound. Default 32 periods.",
    current_capital="Current account capital used to estimate PnL.",
    debug="Show history load/debug details.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def performance(
    interaction: discord.Interaction,
    market: str = "",
    days: int = 32,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        summary_path, summary = _latest_signal_or_raise(cfg)
        message = await asyncio.to_thread(
            _performance_message,
            cfg,
            summary_path,
            summary,
            days=days,
            current_capital=current_capital,
            debug=debug,
        )
    except Exception as exc:
        await _send_command_error(interaction, "performance", exc)
        return
    await _send_long_response(interaction, message)


@bot.tree.command(name="risk", description="Show latest portfolio risk and concentration.")
@app_commands.describe(
    market="Market id",
    top_n="Largest positions to show.",
    debug="Show artifact path.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def risk(
    interaction: discord.Interaction,
    market: str = "",
    top_n: int = 10,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        summary_path, summary = _latest_signal_or_raise(cfg)
        message = await asyncio.to_thread(
            _risk_message,
            cfg,
            summary_path,
            summary,
            top_n=top_n,
            debug=debug,
        )
    except Exception as exc:
        await _send_command_error(interaction, "risk", exc)
        return
    await _send_long_response(interaction, message)


@bot.tree.command(name="watch", description="Manage your per-market symbol watchlist.")
@app_commands.describe(
    market="Market id",
    action="add/update/remove/delete/list/clear/enable/disable",
    symbol="Symbol to add, update, or remove. For update, this is the old symbol.",
    new_symbol="New symbol for action:update.",
    alerts="Enable personal watchlist DM alerts for this market.",
)
@app_commands.choices(
    action=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="update", value="update"),
        app_commands.Choice(name="remove", value="remove"),
        app_commands.Choice(name="delete", value="delete"),
        app_commands.Choice(name="list", value="list"),
        app_commands.Choice(name="clear", value="clear"),
        app_commands.Choice(name="enable alerts", value="enable"),
        app_commands.Choice(name="disable alerts", value="disable"),
    ]
)
@app_commands.autocomplete(market=market_autocomplete)
async def watchlist_command(
    interaction: discord.Interaction,
    action: str,
    market: str = "",
    symbol: str = "",
    new_symbol: str = "",
    alerts: bool = True,
) -> None:
    cfg = _resolve_market(market)
    user_id = getattr(interaction.user, "id", None)
    action_value = str(action).strip().lower()
    try:
        if action_value == "add":
            normalized = _normalize_watch_symbol(symbol)
            if not normalized:
                raise BotUserError("請提供要加入 watchlist 的 symbol。")
            items = _add_user_watch_symbol(user_id, cfg.market, normalized)
            if alerts:
                _set_user_subscription(user_id, cfg.market, watchlist_only=True)
            verb = "加入"
        elif action_value in {"update", "replace", "modify", "set"}:
            items = _replace_user_watch_symbol(user_id, cfg.market, symbol, new_symbol)
            if alerts:
                _set_user_subscription(user_id, cfg.market, watchlist_only=True)
            verb = "更新"
        elif action_value in {"remove", "delete", "del"}:
            normalized = _normalize_watch_symbol(symbol)
            if not normalized:
                raise BotUserError("請提供要移除的 symbol。")
            items = _remove_user_watch_symbol(user_id, cfg.market, normalized)
            verb = "移除"
        elif action_value == "clear":
            items = _clear_user_watchlist(user_id, cfg.market)
            verb = "清空"
        elif action_value == "list":
            items = _user_watchlist(user_id, cfg.market)
            verb = "目前"
        elif action_value in {"enable", "on", "subscribe"}:
            items = _user_watchlist(user_id, cfg.market)
            _set_user_subscription(user_id, cfg.market, watchlist_only=True)
            verb = "啟用提醒"
        elif action_value in {"disable", "off", "unsubscribe"}:
            items = _user_watchlist(user_id, cfg.market)
            _remove_user_subscription(user_id, cfg.market)
            verb = "停用提醒"
        else:
            raise BotUserError("action 必須是 add/update/remove/delete/list/clear/enable/disable。")
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    _record_audit_event(
        f"watch:{cfg.market}",
        f"watch_{action_value}",
        interaction,
        market=cfg.market,
        symbol=_normalize_watch_symbol(symbol),
        new_symbol=_normalize_watch_symbol(new_symbol),
        watchlist=items,
        alerts=cfg.market in _user_subscriptions(user_id),
    )
    content = ", ".join(f"`{item}`" for item in items) if items else "(empty)"
    alert_text = "on" if cfg.market in _user_subscriptions(user_id) else "off"
    await interaction.response.send_message(
        f"`{cfg.market}` watchlist 已{verb}: {content}\nalerts=`{alert_text}` mode=`watchlist_only`",
        ephemeral=True,
    )


async def _handle_signal_now_command(
    interaction: discord.Interaction,
    *,
    market: str,
    mode: str,
    price_source: str,
    top_n: int,
    min_abs_delta: float,
    refresh_data: bool,
    debug: bool,
) -> None:
    normalized_mode = _normalize_signal_now_mode(mode)
    include_raw_universe = normalized_mode == "raw_scores"
    command_name = "raw_score_now" if include_raw_universe else "signal_now"
    await interaction.response.defer(thinking=True)
    try:
        shown_rows = _top_n(top_n)
        cfg = _resolve_market(market)
        status = await asyncio.to_thread(_ensure_signal_ready_cached, cfg)
        completed_session = _completed_session_signal_path(cfg, status)
        completed_session_ready = bool(
            not completed_session or _completed_session_receipt_ready(status)
        )
        cached = None
        if not refresh_data and completed_session_ready:
            cached = await asyncio.to_thread(
                _signal_now_cached_result,
                cfg,
                status,
                requested_price_source=price_source,
                top_n=shown_rows,
                require_unconstrained_raw_scores=include_raw_universe,
                debug=debug,
            )
        if cached is not None:
            summary_path, result, cache_reason = cached
            _record_audit_event(
                str(result.summary.get("signal_id")),
                "cached",
                interaction,
                market=str(result.summary.get("market") or market or _default_market()),
                output_dir=result.output_dir,
                market_open=bool(status.market_open),
                auto_refreshed=False,
                requested_price_source=price_source,
                resolved_price_source=str(result.summary.get("price_source") or "artifact"),
                cache=cache_reason,
                mode=normalized_mode,
                summary=str(summary_path),
                sanity=_signal_sanity_level(_signal_sanity_issues(cfg, result.summary)),
            )
            await _send_signal_response(
                interaction,
                result.message,
                str(result.summary.get("signal_id")),
                str(result.summary.get("market") or market or _default_market()),
            )
            for pages in _signal_now_detail_page_groups(
                cfg,
                result,
                mode=normalized_mode,
                top_n=shown_rows,
                debug=debug,
            ):
                await _send_paginated_response(interaction, pages)
            return
        should_refresh_data = bool(
            _signal_now_should_refresh_data(status, refresh_data=refresh_data)
            or not completed_session_ready
        )
        if should_refresh_data:
            user_id = int(getattr(interaction.user, "id", 0) or 0)
            key, started = _enqueue_signal_now_background_refresh(
                user_id=user_id,
                cfg=cfg,
                runtime_status=status,
                requested_price_source=price_source,
                top_n=shown_rows,
                min_abs_delta=min_abs_delta,
                debug=debug,
                force_refresh=bool(refresh_data),
                mode=normalized_mode,
            )
            expected, actual = _signal_now_job_data_dates(status)
            if (
                not bool(getattr(status.data, "fresh", False))
                or (completed_session and not completed_session_ready)
            ):
                if completed_session:
                    verb = "已開始" if started else "已合併至既有"
                    await interaction.followup.send(
                        f"`{cfg.market}` {verb}最新已完成交易日的官方收盤驗收、"
                        "衍生層原子重建與推論；不會要求下一交易日資格或 MIS 開盤行情。\n"
                        f"latest=`{actual or 'n/a'}` target_close=`{expected or 'n/a'}` "
                        "price_anchor=`official_close`；若官方 close receipt 尚未通過，"
                        "工作會保留為 `waiting_source`，通過後自動推論並 DM。\n"
                        f"command=`/{command_name}` job=`{key}`"
                    )
                else:
                    verb = "已登記" if started else "已合併至既有"
                    await interaction.followup.send(
                        f"`{cfg.market}` 資料尚未通過 freshness gate，{verb}可恢復的來源等待工作；"
                        "不會重算舊資料，也不會把 activation 誤當下載。\n"
                        f"latest=`{actual or 'n/a'}` expected=`{expected or 'n/a'}` "
                        f"status=`waiting_source`；官方資料通過後會自動推論並 DM。\n"
                        f"command=`/{command_name}` job=`{key}`"
                    )
            else:
                verb = "已開始" if started else "已加入既有"
                await interaction.followup.send(
                    f"`{cfg.market}` refresh_data=true，{verb}背景驗證與推論；完成後會 DM 結果。\n"
                    f"command=`/{command_name}` job=`{key}`"
                )
            return
        resolved_price_source, status, auto_refreshed = await asyncio.to_thread(
            _prepare_realtime_signal_sync,
            cfg,
            requested_price_source=price_source,
            force_refresh=should_refresh_data,
        )
        await asyncio.to_thread(_sync_latest_live_weights_to_market_artifact, cfg)
        result = await _run_market_signal(
            market=market,
            price_source=resolved_price_source,
            top_n=shown_rows,
            min_abs_delta=min_abs_delta,
            progress_label=f"{command_name}:{cfg.market}",
            include_unconstrained_raw_scores=include_raw_universe,
        )
        result = _enrich_signal_performance_for_discord(cfg, result, max_rows=0, debug=debug)
    except Exception as exc:
        error_prefix = "raw score" if include_raw_universe else "live signal"
        await _send_command_error(interaction, error_prefix, exc)
        return
    sanity_issues = _signal_sanity_issues(cfg, result.summary)
    if sanity_issues:
        result.message = _prepend_sanity_notice(result.message, cfg, result.summary)
    _record_audit_event(
        str(result.summary.get("signal_id")),
        "generated",
        interaction,
        market=str(result.summary.get("market") or market or _default_market()),
        output_dir=result.output_dir,
        market_open=bool(status.market_open),
        auto_refreshed=bool(auto_refreshed),
        requested_price_source=price_source,
        resolved_price_source=resolved_price_source or "config",
        mode=normalized_mode,
        sanity=_signal_sanity_level(sanity_issues),
    )
    await _send_signal_response(
        interaction,
        result.message,
        str(result.summary.get("signal_id")),
        str(result.summary.get("market") or market or _default_market()),
    )
    for pages in _signal_now_detail_page_groups(
        cfg,
        result,
        mode=normalized_mode,
        top_n=shown_rows,
        debug=debug,
    ):
        await _send_paginated_response(interaction, pages)


@bot.tree.command(name="signal_now", description="Run stockAgent live signal now.")
@app_commands.describe(
    market="Market id",
    price_source="auto/panel/csv/yahoo/tw/shioaji",
    top_n="Summary driver rows, minimum 10; day-trade positions show every active row with paging",
    min_abs_delta="Minimum absolute weight delta",
    refresh_data="Run the market pre-signal data updater before generating. Default false for fast query.",
    debug="Show signal ids, fingerprints, output folders, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def signal_now(
    interaction: discord.Interaction,
    market: str = "",
    price_source: str = "auto",
    top_n: int = 20,
    min_abs_delta: float = 0.001,
    refresh_data: bool = False,
    debug: bool = False,
) -> None:
    await _handle_signal_now_command(
        interaction,
        market=market,
        mode="signal",
        price_source=price_source,
        top_n=top_n,
        min_abs_delta=min_abs_delta,
        refresh_data=refresh_data,
        debug=debug,
    )


@bot.tree.command(
    name="raw_score_now",
    description="Show unfiltered model raw scores for the complete checkpoint universe.",
)
@app_commands.describe(
    market="Market id",
    price_source="auto/panel/csv/yahoo/tw/shioaji",
    refresh_data="Run the market pre-signal data updater first. Default false for fast query.",
    debug="Show signal ids, fingerprints, output folders, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def raw_score_now(
    interaction: discord.Interaction,
    market: str = "",
    price_source: str = "auto",
    refresh_data: bool = False,
    debug: bool = False,
) -> None:
    await _handle_signal_now_command(
        interaction,
        market=market,
        mode="raw_scores",
        price_source=price_source,
        top_n=20,
        min_abs_delta=0.001,
        refresh_data=refresh_data,
        debug=debug,
    )


@bot.tree.command(name="positions", description="Show target position weights.")
@app_commands.describe(
    market="Market id",
    limit="Max rows to show. 0 means all non-zero rows.",
    page_size="Rows per page, clamped to 10-40.",
    include_zero="Include zero-weight universe rows.",
    current_capital="Current account capital used to estimate position amounts.",
    debug="Show signal ids, display timezone, sort internals, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def positions(
    interaction: discord.Interaction,
    market: str = "",
    limit: int = 0,
    page_size: int = 20,
    include_zero: bool = False,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        summary_path, summary = _latest_signal_or_raise(cfg)
        rows = await asyncio.to_thread(
            _latest_artifact_rows,
            summary,
            summary_path,
            "weights_path",
            "top_positions",
        )
    except Exception as exc:
        await _send_command_error(interaction, "positions", exc)
        return
    rows = sorted(
        rows,
        key=lambda row: (_row_abs(row, "target_weight"), _row_abs(row, "delta_weight"), _row_abs(row, "score")),
        reverse=True,
    )
    if not include_zero:
        rows = [
            row
            for row in rows
            if is_display_position_row(row)
        ]
    rows = _limit_rows(rows, limit)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    rows = _annotate_weight_rows_with_capital(rows, capital)
    header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("rows", len(rows)),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "sort: absolute target weight, then delta and score",
    ]
    if debug:
        header.extend(
            [
                _kv_line(
                    ("signal", summary.get("signal_id", summary_path.parent.name)),
                    ("display_tz", summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"full: `{summary.get('positions_markdown_path', summary.get('weights_path', 'n/a'))}`",
            ]
        )
    await _send_paginated_response(
        interaction,
        _line_pages(
            title="target positions",
            rows=rows,
            formatter=_position_line,
            page_size=page_size,
            header_lines=header,
        ),
    )


@bot.tree.command(name="rebalance", description="Show rebalance deltas.")
@app_commands.describe(
    market="Market id",
    threshold="Minimum absolute weight delta",
    limit="Max rows to show. 0 means all rows above threshold.",
    page_size="Rows per page, clamped to 10-40.",
    current_capital="Current account capital used to estimate trade amounts.",
    debug="Show signal ids, display timezone, sort internals, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def rebalance(
    interaction: discord.Interaction,
    market: str = "",
    threshold: float = 0.001,
    limit: int = 0,
    page_size: int = 20,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        _require_trader_permission(interaction, cfg)
        summary_path, summary = _latest_signal_or_raise(cfg)
        rows = await asyncio.to_thread(
            _latest_artifact_rows,
            summary,
            summary_path,
            "rebalance_path",
            "rebalance",
        )
    except Exception as exc:
        await _send_command_error(interaction, "rebalance", exc)
        return
    rows = [row for row in rows if _row_abs(row, "delta_weight") >= max(0.0, float(threshold or 0.0))]
    rows = _sort_decision_rows(rows, "delta")
    rows = _limit_rows(rows, limit)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    rows = _annotate_weight_rows_with_capital(rows, capital)
    header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("threshold", threshold),
            ("rows", len(rows)),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "sort: absolute rebalance delta",
    ]
    if debug:
        header.extend(
            [
                _kv_line(
                    ("signal", summary.get("signal_id", summary_path.parent.name)),
                    ("display_tz", summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"full: `{summary.get('rebalance_markdown_path', summary.get('rebalance_path', 'n/a'))}`",
            ]
        )
    await _send_paginated_response(
        interaction,
        _line_pages(
            title="rebalance",
            rows=rows,
            formatter=_rebalance_line,
            page_size=page_size,
            header_lines=header,
        ),
    )


@bot.tree.command(name="health", description="Show bot configuration.")
@app_commands.describe(market="Market id")
@app_commands.autocomplete(market=market_autocomplete)
async def health(interaction: discord.Interaction, market: str = "") -> None:
    await interaction.response.defer(thinking=True)
    try:
        lines = await asyncio.to_thread(_health_lines, market)
    except Exception as exc:
        await _send_command_error(interaction, "health", exc)
        return
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="markets", description="List configured stockAgent markets.")
async def markets(interaction: discord.Interaction) -> None:
    await interaction.response.defer(thinking=True)
    try:
        lines = await asyncio.to_thread(_markets_lines)
    except Exception as exc:
        await _send_command_error(interaction, "markets", exc)
        return
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="signal", description="Show a saved live signal by signal_id.")
@app_commands.describe(
    signal_id="signal_id from /signal_now",
    debug="Show fingerprints and artifact paths.",
)
async def signal(interaction: discord.Interaction, signal_id: str, debug: bool = False) -> None:
    await interaction.response.defer(thinking=True)
    found = _find_signal_summary(signal_id)
    if found is None:
        await interaction.followup.send(f"找不到 signal_id=`{signal_id}`。")
        return
    path, summary = found
    risk = summary.get("target_risk", {}) if isinstance(summary.get("target_risk"), dict) else {}
    lines = [
        f"**signal** `{summary.get('signal_id', signal_id)}`",
        f"market=`{summary.get('market', 'n/a')}` "
        f"asof=`{_display_summary_time(summary, summary.get('asof_date', 'n/a'))}` "
        f"panel=`{_display_summary_time(summary, summary.get('panel_date', 'n/a'))}`",
        f"risk gross=`{_pct(risk.get('gross'))}` top=`{_pct(risk.get('top_abs_weight'))}` turnover=`{_pct(summary.get('turnover'))}`",
    ]
    if debug:
        lines.extend(
            [
                f"display_tz=`{summary.get('display_timezone_label') or display_timezone_label(summary.get('display_timezone'))}`",
                f"fold=`{summary.get('fold_id', 'n/a')}` checkpoint=`{summary.get('checkpoint_fingerprint', 'n/a')}` config=`{summary.get('config_fingerprint', 'n/a')}`",
                f"summary=`{path}`",
                f"weights=`{summary.get('weights_path', 'n/a')}`",
                f"rebalance=`{summary.get('rebalance_path', 'n/a')}`",
                f"explain=`{summary.get('decision_explanation_path', 'n/a')}`",
            ]
        )
    _append_investment_warning(lines)
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="explain_signal", description="Show paged daily decision explanations.")
@app_commands.describe(
    market="Market id. Used when signal_id is empty.",
    signal_id="Optional signal_id from /signal_now. Empty means latest market signal.",
    symbol="Optional symbol/code or name filter.",
    action="all/actionable/BUY/SELL/REDUCE/EXIT/HOLD.",
    sort_by="delta/score/target/return/rank.",
    detail="compact or full. full is easier to read.",
    limit="Max rows to show. 0 means all decision rows.",
    page_size="Rows per page, clamped to 10-40.",
    actionable_only="Hide HOLD rows.",
    attach_file="Upload the full markdown decision report.",
    debug="Show fold, source, display timezone, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def explain_signal(
    interaction: discord.Interaction,
    market: str = "",
    signal_id: str = "",
    symbol: str = "",
    action: str = "actionable",
    sort_by: str = "delta",
    detail: str = "full",
    limit: int = 0,
    page_size: int = 10,
    actionable_only: bool = True,
    attach_file: bool = False,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        if str(signal_id or "").strip():
            found = _find_signal_summary(signal_id)
            if found is None:
                await interaction.followup.send(f"找不到 signal_id=`{signal_id}`。")
                return
            summary_path, summary = found
        else:
            cfg = _resolve_market(market)
            latest = _latest_market_signal(cfg)
            if latest is None:
                await interaction.followup.send(f"`{cfg.market}` 尚無 live signal，請先跑 `/signal_now market:{cfg.market}`。")
                return
            summary_path, summary = latest
        explain_path = _summary_artifact_path(summary, "decision_explanation_path", summary_path)
        if explain_path is None or not explain_path.exists():
            await interaction.followup.send(
                "這筆 signal 沒有逐檔決策解釋檔；請重新跑一次 `/signal_now` 產生新版 artifact。"
            )
            return
        rows = _read_parquet_rows(explain_path)
    except Exception as exc:
        await _send_command_error(interaction, "explain_signal", exc)
        return

    rows_all = _sort_decision_rows(rows, sort_by)
    rows_filtered = _filter_decision_rows(
        rows_all,
        symbol=symbol,
        action=action,
        actionable_only=actionable_only,
    )
    rows_filtered = _sort_decision_rows(rows_filtered, sort_by)
    rows_visible = _limit_rows(rows_filtered, limit)
    formatter = _decision_block if str(detail or "").strip().lower() not in {"compact", "line", "short"} else _decision_line
    overview = _decision_overview_page(
        summary=summary,
        summary_path=summary_path,
        explain_path=explain_path,
        rows_all=rows_all,
        rows_filtered=rows_filtered,
        symbol=symbol,
        action=action,
        sort_by=sort_by,
        debug=debug,
    )
    pages = [overview]
    pages.extend(
        _line_pages(
            title="decision rows",
            rows=rows_visible,
            formatter=formatter,
            page_size=page_size,
        )
    )
    await _send_paginated_response(
        interaction,
        pages,
    )
    if attach_file:
        report_path = _summary_artifact_path(summary, "decision_report_path", summary_path)
        if report_path is None or not report_path.exists():
            report_path = _summary_artifact_path(summary, "decision_explanation_markdown_path", summary_path)
        if report_path is not None and report_path.exists():
            await interaction.followup.send(file=discord.File(str(report_path), filename=report_path.name))


@bot.tree.command(name="stock_history", description="Show recent per-symbol trades and adjustments.")
@app_commands.describe(
    market="Market id.",
    symbol="Stock code/ticker, e.g. 2330 or 2330.TW.",
    limit="Max periods/bars to show. Default 32. 0 means all rows.",
    page_size="Rows per page, clamped to 10-40.",
    changes_only="Only show trade/adjustment rows. If false, show recent state rows.",
    initial_capital="Scale fold values from the first fold NAV.",
    current_capital="Scale fold values from the latest fold NAV. Overrides initial_capital.",
    debug="Show fold paths, source files, timezone, and capital-basis internals.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def stock_history_command(
    interaction: discord.Interaction,
    symbol: str,
    market: str = "",
    limit: int = 32,
    page_size: int = 10,
    changes_only: bool = True,
    initial_capital: float = 0.0,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        result = await asyncio.to_thread(
            _load_stock_history_for_market,
            cfg,
            symbol,
            limit,
            changes_only,
            initial_capital,
            current_capital,
        )
    except Exception as exc:
        await _send_command_error(interaction, "stock_history", exc)
        return

    label = result.symbol + (f" {result.name}" if result.name else "")
    pages = _line_pages(
        title=f"stock history {label}",
        rows=result.rows,
        formatter=_stock_history_block,
        page_size=page_size,
        header_lines=_stock_history_header_lines(cfg, result, debug=debug),
    )
    await _send_paginated_response(interaction, pages)


@bot.tree.command(name="portfolio_history", description="Show recent PnL and holding changes.")
@app_commands.describe(
    market="Market id.",
    days="Periods to show. Daily markets use days; crypto uses 1m bars. Default 32. 0 means all.",
    top_changes="Top holding changes per period (0-20; all requested rows are shown).",
    min_abs_change="Hide weight-only changes below this absolute ratio.",
    initial_capital="Scale fold values from the first fold NAV.",
    current_capital="Scale fold values from the latest fold NAV. Overrides initial_capital.",
    debug="Show fold paths, source files, timezone, and capital-basis internals.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def portfolio_history_command(
    interaction: discord.Interaction,
    market: str = "",
    days: int = 32,
    top_changes: app_commands.Range[int, 0, 20] = 5,
    min_abs_change: float = MIN_DISPLAY_ABS_WEIGHT,
    initial_capital: float = 0.0,
    current_capital: float = 0.0,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        result = await asyncio.to_thread(
            _load_portfolio_history_for_market,
            cfg,
            days,
            top_changes,
            min_abs_change,
            initial_capital,
            current_capital,
        )
    except Exception as exc:
        await _send_command_error(interaction, "portfolio_history", exc)
        return

    try:
        pages = _portfolio_history_pages(cfg, result, debug=debug)
        await _send_paginated_response(interaction, pages)
    except Exception as exc:
        await _send_command_error(interaction, "portfolio_history", exc)


@bot.tree.command(name="set_market_enabled", description="Enable or disable a market in the Discord bot.")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.describe(market="Market id", enabled="true/false")
@app_commands.autocomplete(market=market_autocomplete)
async def set_market_enabled(interaction: discord.Interaction, market: str, enabled: bool) -> None:
    cfg = _resolve_market(market)
    try:
        _require_trader_permission(interaction, cfg)
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    _set_market_state(cfg.market, enabled=bool(enabled))
    _record_audit_event(f"market:{cfg.market}", "set_market_enabled", interaction, market=cfg.market, enabled=bool(enabled))
    await interaction.response.send_message(f"`{cfg.market}` enabled=`{bool(enabled)}`")


@bot.tree.command(name="set_schedule", description="Set a market scheduled signal time.")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.describe(market="Market id", schedule_time="HH:MM in the market timezone")
@app_commands.autocomplete(market=market_autocomplete)
async def set_schedule(interaction: discord.Interaction, market: str, schedule_time: str) -> None:
    cfg = _resolve_market(market)
    try:
        _require_trader_permission(interaction, cfg)
        normalized = _validate_hhmm(schedule_time)
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    _set_market_state(cfg.market, schedule_time=normalized)
    _record_audit_event(f"market:{cfg.market}", "set_schedule", interaction, market=cfg.market, schedule_time=normalized)
    await interaction.response.send_message(f"`{cfg.market}` schedule_time=`{normalized}` tz=`{cfg.timezone}`")


@bot.tree.command(name="set_capital", description="Set default capital for market amount estimates.")
@app_commands.allowed_installs(guilds=True, users=False)
@app_commands.allowed_contexts(guilds=True, dms=False, private_channels=False)
@app_commands.describe(
    market="Market id",
    initial_capital="Fold initial capital. Use 0 to clear.",
    current_capital="Current account capital. Use 0 to clear. Overrides initial_capital.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def set_capital(
    interaction: discord.Interaction,
    market: str,
    initial_capital: float = 0.0,
    current_capital: float = 0.0,
) -> None:
    cfg = _resolve_market(market)
    try:
        _require_trader_permission(interaction, cfg)
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    initial = positive_float_or_none(initial_capital)
    current = positive_float_or_none(current_capital)
    _set_market_state(cfg.market, initial_capital=initial, current_capital=current)
    _record_audit_event(
        f"market:{cfg.market}",
        "set_capital",
        interaction,
        market=cfg.market,
        initial_capital=initial,
        current_capital=current,
    )
    await interaction.response.send_message(
        f"`{cfg.market}` capital initial=`{_money(initial)}` current=`{_money(current)}` "
        "current 會優先用於金額估算。"
    )


@bot.tree.command(name="daily_summary", description="Show today's market summary.")
@app_commands.describe(
    market="Market id",
    debug="Show display timezone, signal id, fold, and artifact path.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def daily_summary_command(interaction: discord.Interaction, market: str = "", debug: bool = False) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        message = await asyncio.to_thread(_daily_summary_message, cfg, debug=debug)
    except Exception as exc:
        await _send_command_error(interaction, "daily_summary", exc)
        return
    await _send_long_response(interaction, message)


def _auto_deploy_smoke_test(
    cfg: LiveMarketConfig,
    deployment: ModelDeployment,
) -> dict[str, Any]:
    candidate_cfg = replace(
        cfg,
        config_path=deployment.config_path,
        output_dir=deployment.output_dir,
        fold_id=deployment.fold_id,
        checkpoint_path=deployment.checkpoint_path,
        weights_path=deployment.weights_path,
    )
    with _MODEL_INFERENCE_LOCK:
        result = generate_live_signal(
            **candidate_cfg.signal_kwargs(
                write=False,
                ensure_previous_signal=False,
                top_n=max(MIN_DISCORD_ROWS, min(int(candidate_cfg.top_n), 20)),
                progress_callback=_ConsoleProgress(prefix=f"model-fit:{cfg.market}"),
                progress_label=f"model-fit:{cfg.market}",
            )
        )
    if int(result.summary.get("fold_id", -1)) != deployment.fold_id:
        raise RuntimeError("smoke signal resolved a different fold")
    if Path(str(result.summary.get("checkpoint_path"))).resolve() != Path(
        deployment.checkpoint_path
    ).resolve():
        raise RuntimeError("smoke signal resolved a different checkpoint")
    return result.summary


def _run_model_auto_deploy_sync(cfg: LiveMarketConfig) -> tuple[str, ModelDeployment | None]:
    if not cfg.model_deployment_manifest:
        raise ValueError(f"{cfg.market} model auto-deploy requires model_deployment_manifest")
    return attempt_model_deployment(
        market=cfg.market,
        candidate_roots=cfg.model_candidate_output_dirs,
        candidate_configs=cfg.model_candidate_config_paths,
        manifest_path=cfg.model_deployment_manifest,
        root=ROOT,
        smoke_test=lambda deployment: _auto_deploy_smoke_test(cfg, deployment),
    )


@tasks.loop(seconds=10)
async def model_auto_deployment() -> None:
    now = time.monotonic()
    for cfg in _market_configs().values():
        if not cfg.model_auto_deploy or not _market_enabled(cfg):
            continue
        interval = max(10, int(cfg.model_auto_deploy_interval_seconds))
        last_check = bot._model_deployment_last_check.get(cfg.market, 0.0)
        if now - last_check < interval:
            continue
        bot._model_deployment_last_check[cfg.market] = now
        try:
            status, deployment = await asyncio.to_thread(_run_model_auto_deploy_sync, cfg)
            if status == "promoted" and deployment is not None:
                _clear_runtime_status_cache()
                print(
                    f"[model-deploy] market={cfg.market} status=promoted "
                    f"root={deployment.output_dir} fold={deployment.fold_id} "
                    f"checkpoint={deployment.checkpoint_path}",
                    flush=True,
                )
            elif status not in {"already_active", "known_failed"}:
                print(f"[model-deploy] market={cfg.market} status={status}", flush=True)
        except Exception as exc:
            _log_exception(f"model_auto_deploy:{cfg.market}", exc)
            print(
                f"[model-deploy] market={cfg.market} status=failed "
                f"error={type(exc).__name__}: {exc}",
                flush=True,
            )


@tasks.loop(seconds=15)
async def signal_now_job_resumer() -> None:
    await _resume_signal_now_jobs_once()


@tasks.loop(seconds=1)
async def service_heartbeat() -> None:
    """Keep a compact, source-backed Discord/engine synchronization receipt."""

    notify_systemd("WATCHDOG=1")
    attempt_started = bot._opening_attempt_started_monotonic
    if attempt_started is not None:
        now_monotonic = time.monotonic()
        elapsed = max(0.0, now_monotonic - attempt_started)
        last_progress = (
            bot._opening_attempt_last_progress_monotonic or attempt_started
        )
        idle_elapsed = max(0.0, now_monotonic - last_progress)
        timeout_seconds = _opening_attempt_timeout_seconds(
            hot=bot._opening_attempt_hot,
        )
        if idle_elapsed > timeout_seconds:
            market = bot._opening_attempt_market or "unknown"
            progress = bot._opening_attempt_progress_message or "none"
            print(
                f"[scheduled-watchdog] market={market} elapsed={elapsed:.3f}s "
                f"idle={idle_elapsed:.3f}s last_progress={progress!r} "
                f"timeout={timeout_seconds:.3f}s action=exit_for_systemd_restart",
                flush=True,
            )
            notify_systemd(
                f"STATUS=Opening attempt stalled for {idle_elapsed:.1f}s "
                f"({market}, {progress}); restarting"
            )
            os._exit(70)
    try:
        payload = await asyncio.to_thread(_write_discord_service_status)
    except Exception as exc:
        _log_exception("service_heartbeat", exc)
        return
    if attempt_started is None:
        interactive = payload.get("interactive_signal_jobs") or {}
        maintenance = payload.get("background_maintenance") or {}
        notify_systemd(
            "STATUS=Discord Gateway "
            f"{payload.get('core_health', 'unknown')}; "
            f"interactive={interactive.get('status', 'unknown')}; "
            f"maintenance={maintenance.get('status', 'unknown')}; "
            f"engine_revision={payload.get('engine_state_revision', 0)}"
        )


@tasks.loop(seconds=1)
async def preopen_prepare() -> None:
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        key = _preopen_prepare_key(cfg, now)
        if key is None or key in bot._last_preopen_prepare_keys:
            continue
        if not _scheduled_retry_allowed(bot._preopen_retry_after, key):
            continue
        try:
            final_arm = key.endswith(":preopen-final-arm")
            result = await asyncio.to_thread(
                _final_arm_market_signal_sync
                if final_arm
                else _prewarm_market_signal_sync,
                cfg,
            )
            bot._last_preopen_prepare_keys.add(key)
            _clear_signal_retry(
                bot._preopen_retry_after,
                bot._preopen_failure_counts,
                key,
            )
            print(
                f"[preopen] market={market} "
                f"phase={'final_arm' if final_arm else 'prepare'} status=ready "
                f"panel={result.summary.get('panel_date')} "
                f"latency={result.summary.get('live_latency')}",
                flush=True,
            )
        except Exception as exc:
            _log_exception(f"preopen_prepare:{market}", exc)
            retry_delay = _mark_signal_retry(
                bot._preopen_retry_after,
                bot._preopen_failure_counts,
                key,
                day_trade=bool(
                    getattr(cfg, "day_trade_simulation_enabled", False)
                ),
            )
            print(
                f"[preopen] market={market} status=failed "
                f"retry_seconds={retry_delay:.3f} "
                f"error={type(exc).__name__}: {exc}",
                flush=True,
            )


@tasks.loop(seconds=0.1)
async def scheduled_signal() -> None:
    # Compute and publish every due strategy before doing any Discord network
    # I/O.  The strategy ledger is the primary output; chat is an asynchronous
    # observer and must never serialize the three 09:00 models.
    deliveries: list[tuple[LiveMarketConfig, LiveSignalResult, str]] = []
    error_messages: list[str] = []
    scheduled_markets = list(_scheduled_markets())
    scheduled_markets.sort(
        key=lambda market: (
            0
            if bool(
                getattr(_resolve_market(market), "day_trade_simulation_enabled", False)
            )
            else 1,
            -_preopen_market_symbol_count(_resolve_market(market)),
            market,
        )
    )
    for market in scheduled_markets:
        cfg = _resolve_market(market)
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        session_open, _session_reason = _scheduled_market_session_day(cfg, now)
        if not session_open:
            # Defense in depth: no future change to schedule-key construction
            # may reach refresh, Snapshot, inference, or simulation on a
            # closed/unknown session.
            continue
        key = _scheduled_signal_key(cfg, now)
        if key is None:
            continue
        if key in bot._last_scheduled_keys:
            continue
        day_trade_simulation = bool(
            getattr(cfg, "day_trade_simulation_enabled", False)
        )
        if day_trade_simulation:
            execution_state = _day_trade_schedule_state(
                cfg, now.date().isoformat()
            )
            if execution_state == "blocked_open_position":
                continue
            if execution_state == "pending_confirmation":
                continue
        else:
            execution_state = (
                "completed"
                if _market_has_generated_signal_for_session(
                    cfg, now.date().isoformat()
                )
                else "retry"
            )
        if execution_state == "completed":
            bot._last_scheduled_keys.add(key)
            _clear_signal_retry(
                bot._scheduled_retry_after,
                bot._scheduled_failure_counts,
                key,
            )
            bot._scheduled_error_notice_keys.discard(key)
            print(
                f"[scheduled] market={market} status=already_recorded_after_restart "
                f"session={now.date().isoformat()}",
                flush=True,
            )
            continue
        if not _scheduled_retry_allowed(bot._scheduled_retry_after, key):
            continue
        if day_trade_simulation:
            attempt_started = time.monotonic()
            bot._opening_attempt_started_monotonic = attempt_started
            bot._opening_attempt_last_progress_monotonic = attempt_started
            bot._opening_attempt_progress_message = "scheduled attempt started"
            bot._opening_attempt_market = market
            bot._opening_attempt_hot = _preopen_market_final_armed_for_session(
                cfg, now.date().isoformat()
            )
            notify_systemd(f"STATUS=09:00 opening signal in progress: {market}")
        try:
            if _scheduled_signal_requires_preopen_catch_up(cfg, now):
                # A restart may have missed both the pre-open preparation and
                # the exact scheduled signal minute.  Reuse the complete
                # pre-open contract so same-session eligibility and price
                # limits exist before recording the strategy signal.
                await asyncio.to_thread(_prewarm_market_signal_sync, cfg)
            resolved_price_source, prepared_status, _ = await asyncio.to_thread(
                _prepare_realtime_signal_sync,
                cfg,
                requested_price_source="auto",
                force_refresh=False,
            )
            result = await _run_market_signal(
                market=market,
                scheduled=True,
                price_source=resolved_price_source,
                prepared_status=prepared_status,
                progress_label=f"scheduled:{market}",
            )
        except BotUserError as exc:
            if not isinstance(exc, MarketClosedError):
                retry_delay = _mark_signal_retry(
                    bot._scheduled_retry_after,
                    bot._scheduled_failure_counts,
                    key,
                    day_trade=day_trade_simulation,
                )
                if key not in bot._scheduled_error_notice_keys:
                    error_messages.append(str(exc))
                    bot._scheduled_error_notice_keys.add(key)
                print(
                    f"[scheduled] market={market} status=retry "
                    f"delay_seconds={retry_delay:.3f} error={type(exc).__name__}",
                    flush=True,
                )
            continue
        except Exception as exc:
            _log_exception(f"scheduled_signal:{market}", exc)
            retry_delay = _mark_signal_retry(
                bot._scheduled_retry_after,
                bot._scheduled_failure_counts,
                key,
                day_trade=day_trade_simulation,
            )
            if key not in bot._scheduled_error_notice_keys:
                error_messages.append(
                    f"`{market}` scheduled signal failed: `{type(exc).__name__}`"
                )
                bot._scheduled_error_notice_keys.add(key)
            print(
                f"[scheduled] market={market} status=retry "
                f"delay_seconds={retry_delay:.3f} error={type(exc).__name__}",
                flush=True,
            )
            continue
        finally:
            if day_trade_simulation:
                bot._opening_attempt_started_monotonic = None
                bot._opening_attempt_last_progress_monotonic = None
                bot._opening_attempt_progress_message = None
                bot._opening_attempt_market = None
                bot._opening_attempt_hot = False
        if day_trade_simulation:
            bot._scheduled_retry_after[key] = (
                time.monotonic() + _day_trade_confirmation_delay_seconds()
            )
            print(
                f"[scheduled] market={market} status=awaiting_engine_confirmation "
                f"session={now.date().isoformat()}",
                flush=True,
            )
        else:
            bot._last_scheduled_keys.add(key)
            _clear_signal_retry(
                bot._scheduled_retry_after,
                bot._scheduled_failure_counts,
                key,
            )
        bot._scheduled_error_notice_keys.discard(key)
        deliveries.append((cfg, result, key))

    if not deliveries and not error_messages:
        return
    channel = await _scheduled_broadcast_channel()
    if channel is not None:
        for message in error_messages:
            try:
                await channel.send(message)
            except Exception as exc:
                _log_exception("scheduled_signal:error_notification", exc)
    for cfg, result, _key in deliveries:
        try:
            result = await asyncio.to_thread(
                _enrich_signal_performance_for_discord,
                cfg,
                result,
                max_rows=0,
            )
            if _signal_sanity_issues(cfg, result.summary):
                result.message = _prepend_sanity_notice(
                    result.message,
                    cfg,
                    result.summary,
                )
            if channel is not None:
                await channel.send(
                    result.message,
                    view=SignalReviewView(
                        signal_id=str(result.summary.get("signal_id")),
                        market=str(result.summary.get("market") or cfg.market),
                    ),
                )
            await _send_subscription_notifications(cfg, result)
            if channel is not None:
                for pages in _scheduled_detail_page_groups(cfg, result):
                    await _send_channel_pages(channel, pages)
        except Exception as exc:
            # Notification/rendering failures occur after the atomic execution
            # pointer was published and must never kill the scheduler task.
            _log_exception(f"scheduled_signal:post_publish:{cfg.market}", exc)


@tasks.loop(minutes=1)
async def daily_summary() -> None:
    if not _public_broadcasts_enabled() or bot.channel_id is None:
        return
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        summary_time = _market_summary_time(cfg)
        if not summary_time:
            continue
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        session_open, _session_reason = _scheduled_market_session_day(cfg, now)
        if not session_open:
            continue
        if now.strftime("%H:%M") != summary_time:
            continue
        today = now.strftime("%Y-%m-%d")
        key = f"{today}:{market}"
        if key in bot._last_daily_summary_keys:
            continue
        if not _scheduled_retry_allowed(bot._daily_summary_retry_after, key):
            continue
        try:
            message = await asyncio.to_thread(_daily_summary_message, cfg)
            await channel.send(message[:1900])
            bot._last_daily_summary_keys.add(key)
            _clear_scheduled_retry(bot._daily_summary_retry_after, key)
        except MarketUnsupportedError as exc:
            await channel.send(str(exc))
            _mark_scheduled_retry(bot._daily_summary_retry_after, key)
            continue
        except Exception as exc:
            _log_exception(f"daily_summary:{market}", exc)
            await channel.send(f"`{market}` daily summary failed: `{type(exc).__name__}`")
            _mark_scheduled_retry(bot._daily_summary_retry_after, key)


@tasks.loop(minutes=1)
async def artifact_backfill() -> None:
    if _opening_critical_work_pending():
        print(
            "[artifact-backfill] deferred: opening-critical day-trade work pending",
            flush=True,
        )
        return
    if _interactive_signal_work_pending():
        print(
            "[artifact-backfill] deferred: interactive signal work pending",
            flush=True,
        )
        return
    channel = None
    if bot.channel_id is not None and _public_broadcasts_enabled():
        try:
            channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
        except Exception:
            channel = None
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        # Computing the key resolves runtime freshness, which may scan thousands
        # of parquet files.  Keep that blocking filesystem work off Discord's
        # event loop so Gateway heartbeats cannot be starved by maintenance.
        key = await asyncio.to_thread(_artifact_backfill_key, cfg, now)
        if key is None:
            continue
        if not _market_has_model(cfg):
            continue
        if key in bot._last_artifact_backfill_keys:
            continue
        if not _artifact_backfill_retry_allowed(key):
            await asyncio.to_thread(
                _reconcile_artifact_backfill_if_current,
                cfg,
                key=key,
                market=market,
            )
            continue
        job = _begin_artifact_backfill(key, market)
        try:
            result = await asyncio.to_thread(_run_artifact_backfill_sync, cfg)
            bot._last_artifact_backfill_keys.add(key)
            _finish_artifact_backfill(key, market, status="ready")
            if result is not None:
                print(
                    f"[artifact-backfill] {market} signal={result.summary.get('signal_id')} "
                    f"panel={result.summary.get('panel_date')} output={result.output_dir}",
                    flush=True,
                )
        except BotUserError as exc:
            failed_job = _finish_artifact_backfill(
                key,
                market,
                status="failed",
                exc=exc,
            )
            _log_exception(f"artifact_backfill:{market}", exc)
            if (
                channel is not None
                and not isinstance(exc, MarketClosedError)
                and int(job.get("attempt") or 1) == 1
            ):
                await channel.send(f"`{market}` artifact backfill failed: {exc}")
            print(
                f"[artifact-backfill] market={market} status=failed "
                f"attempt={failed_job.get('attempt')} "
                f"next_retry_at={failed_job.get('next_retry_at')} "
                f"error={type(exc).__name__}",
                flush=True,
            )
        except Exception as exc:
            failed_job = _finish_artifact_backfill(
                key,
                market,
                status="failed",
                exc=exc,
            )
            _log_exception(f"artifact_backfill:{market}", exc)
            if channel is not None and int(job.get("attempt") or 1) == 1:
                await channel.send(f"`{market}` artifact backfill failed: `{type(exc).__name__}`")
            print(
                f"[artifact-backfill] market={market} status=failed "
                f"attempt={failed_job.get('attempt')} "
                f"next_retry_at={failed_job.get('next_retry_at')} "
                f"error={type(exc).__name__}",
                flush=True,
            )


async def _scheduled_broadcast_channel() -> Any | None:
    if bot.channel_id is None or not _public_broadcasts_enabled():
        return None
    try:
        return bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    except Exception as exc:
        # Discord delivery must not delay or cancel the strategy ledger.
        _log_exception("scheduled_signal:broadcast_channel", exc)
        return None


@preopen_prepare.before_loop
async def before_preopen_prepare() -> None:
    await bot.wait_until_ready()


@scheduled_signal.before_loop
async def before_scheduled_signal() -> None:
    # discord.ext tasks keep the phase established by their first iteration.
    # Align that phase to the wall-clock second so the 09:00 gate does not pay
    # an arbitrary 0-999 ms service-start offset.
    delay = 1.0 - (time.time() % 1.0)
    if delay < 0.999:
        await asyncio.sleep(delay)


@daily_summary.before_loop
async def before_daily_summary() -> None:
    await bot.wait_until_ready()


@artifact_backfill.before_loop
async def before_artifact_backfill() -> None:
    await bot.wait_until_ready()


@model_auto_deployment.before_loop
async def before_model_auto_deployment() -> None:
    await bot.wait_until_ready()


@signal_now_job_resumer.before_loop
async def before_signal_now_job_resumer() -> None:
    await bot.wait_until_ready()


RELOAD_CHILD_ENV = "STOCKAGENT_DISCORD_BOT_CHILD"
WATCH_ROOTS_DEFAULT = (
    "services/discord_bot",
    "stockagent/live",
    "stockagent/models",
    "stockagent/training/trainer.py",
    "configs/markets",
    "scripts/live_signal.py",
)
WATCH_EXTENSIONS = {".py", ".yaml", ".yml"}
WATCH_FILENAMES = {".env"}
WATCH_SKIP_DIRS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules"}


def _watch_delay_seconds() -> float:
    raw = _env("STOCKAGENT_BOT_RESTART_DELAY_SECONDS", "0") or "0"
    return max(0.0, float(raw))


def _watch_crash_delay_seconds() -> float:
    raw = _env("STOCKAGENT_BOT_CRASH_RESTART_DELAY_SECONDS", "10") or "10"
    return max(0.0, float(raw))


def _watch_poll_seconds() -> float:
    raw = _env("STOCKAGENT_BOT_RELOAD_POLL_SECONDS", "0.2") or "0.2"
    return max(0.05, float(raw))


def _watch_roots() -> list[Path]:
    raw = _env("STOCKAGENT_BOT_WATCH_PATHS")
    items = [item.strip() for item in raw.split(",") if item.strip()] if raw else list(WATCH_ROOTS_DEFAULT)
    roots: list[Path] = []
    for item in items:
        path = Path(item)
        roots.append(path if path.is_absolute() else ROOT / path)
    return roots


def _watch_file_included(path: Path) -> bool:
    return path.suffix in WATCH_EXTENSIONS or path.name in WATCH_FILENAMES


def _iter_watch_files() -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for root in _watch_roots():
        if root.is_file():
            candidates = [root]
        elif root.is_dir():
            candidates = []
            for path in root.rglob("*"):
                if any(part in WATCH_SKIP_DIRS for part in path.parts):
                    continue
                if path.is_file():
                    candidates.append(path)
        else:
            candidates = []
        for path in candidates:
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path
            if resolved in seen or not _watch_file_included(path):
                continue
            seen.add(resolved)
            files.append(path)
    return sorted(files)


def _watch_snapshot() -> dict[str, tuple[int, int]]:
    snapshot: dict[str, tuple[int, int]] = {}
    for path in _iter_watch_files():
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        snapshot[str(path)] = (int(stat.st_mtime_ns), int(stat.st_size))
    return snapshot


def _changed_watch_files(
    before: dict[str, tuple[int, int]],
    after: dict[str, tuple[int, int]],
) -> list[str]:
    changed: list[str] = []
    for path, state in after.items():
        if before.get(path) != state:
            changed.append(path)
    for path in before:
        if path not in after:
            changed.append(path)
    return sorted(changed)


def _start_bot_child() -> subprocess.Popen:
    env = os.environ.copy()
    env[RELOAD_CHILD_ENV] = "1"
    env.setdefault("PYTHONUNBUFFERED", "1")
    command = [sys.executable, str(Path(__file__).resolve())]
    print(f"[bot-reload] starting child: {' '.join(command)}", flush=True)
    return subprocess.Popen(command, cwd=str(ROOT), env=env)


def _stop_bot_child(process: subprocess.Popen, *, timeout: float = 15.0) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5.0)


def run_with_reloader() -> None:
    delay = _watch_delay_seconds()
    crash_delay = _watch_crash_delay_seconds()
    poll = _watch_poll_seconds()
    stop_requested = False
    child = _start_bot_child()
    snapshot = _watch_snapshot()
    pending_deadline: float | None = None
    pending_changes: set[str] = set()

    def request_stop(signum, frame) -> None:
        del signum, frame
        nonlocal stop_requested
        stop_requested = True

    previous_sigint = signal_module.getsignal(signal_module.SIGINT)
    previous_sigterm = signal_module.getsignal(signal_module.SIGTERM)
    signal_module.signal(signal_module.SIGINT, request_stop)
    signal_module.signal(signal_module.SIGTERM, request_stop)
    print(
        "[bot-reload] enabled "
        f"delay={delay:.2f}s crash_delay={crash_delay:.1f}s poll={poll:.2f}s "
        f"paths={', '.join(str(path) for path in _watch_roots())}",
        flush=True,
    )
    try:
        while not stop_requested:
            exit_code = child.poll()
            if exit_code is not None:
                print(f"[bot-reload] child exited code={exit_code}; restarting in {crash_delay:.1f}s", flush=True)
                time.sleep(crash_delay)
                if stop_requested:
                    break
                child = _start_bot_child()
                snapshot = _watch_snapshot()
                pending_deadline = None
                pending_changes.clear()
                continue

            new_snapshot = _watch_snapshot()
            changed = _changed_watch_files(snapshot, new_snapshot)
            if changed:
                snapshot = new_snapshot
                pending_changes.update(changed)
                pending_deadline = time.monotonic() + delay
                preview = ", ".join(Path(path).name for path in sorted(pending_changes)[:5])
                if len(pending_changes) > 5:
                    preview += f", +{len(pending_changes) - 5} more"
                if delay <= 0.0:
                    print(f"[bot-reload] file update detected: {preview}; restarting now", flush=True)
                else:
                    print(f"[bot-reload] file update detected: {preview}; restart in {delay:.2f}s", flush=True)

            if pending_deadline is not None and time.monotonic() >= pending_deadline:
                print("[bot-reload] restarting child after file updates", flush=True)
                _stop_bot_child(child)
                child = _start_bot_child()
                snapshot = _watch_snapshot()
                pending_deadline = None
                pending_changes.clear()

            time.sleep(poll)
    finally:
        _stop_bot_child(child)
        signal_module.signal(signal_module.SIGINT, previous_sigint)
        signal_module.signal(signal_module.SIGTERM, previous_sigterm)


def main() -> None:
    token = _env("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is required")
    bot.run(token)


if __name__ == "__main__":
    if _env_bool("STOCKAGENT_BOT_RELOAD", True) and os.environ.get(RELOAD_CHILD_ENV) != "1":
        run_with_reloader()
    else:
        main()
