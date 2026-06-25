from __future__ import annotations

import asyncio
import json
import math
import os
import signal as signal_module
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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

from stockagent.live.market_config import LiveMarketConfig, load_market_configs
from stockagent.live.market_status import MarketRuntimeStatus, runtime_status
from stockagent.config import load_config
from stockagent.live.capital import positive_float_or_none
from stockagent.live.quote_provider import load_symbol_name_map
from stockagent.live.portfolio_history import PortfolioHistoryResult, load_portfolio_history
from stockagent.live.report_formatter import INVESTMENT_WARNING, format_signal_message
from stockagent.live.signal_engine import generate_live_signal, write_live_weights_history
from stockagent.live.stock_history import StockHistoryResult, load_stock_history
from stockagent.live.time_display import DEFAULT_DISPLAY_TIMEZONE, display_timezone_label, format_display_time


MIN_DISCORD_ROWS = 10
STATE_PATH = ROOT / "artifacts" / "discord_bot" / "state.json"
ERROR_LOG_PATH = ROOT / "artifacts" / "discord_bot" / "errors.log"
AUDIT_LOG_PATH = ROOT / "artifacts" / "discord_bot" / "audit_events.jsonl"


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


def _clear_user_watchlist(user_id: Any, market: str) -> list[str]:
    return _set_user_watchlist(user_id, market, [])


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


def _market_initial_capital(cfg: LiveMarketConfig) -> float | None:
    entry = _market_state(cfg.market)
    return positive_float_or_none(entry.get("initial_capital")) or positive_float_or_none(cfg.initial_capital)


def _market_current_capital(cfg: LiveMarketConfig) -> float | None:
    entry = _market_state(cfg.market)
    return positive_float_or_none(entry.get("current_capital")) or positive_float_or_none(cfg.current_capital)


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


def _log_exception(context: str, exc: Exception) -> None:
    ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {context}",
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        "",
    ]
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
    configs = load_market_configs(_markets_dir())
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


def _market_model_checkpoint(cfg: LiveMarketConfig) -> Path | None:
    explicit = _resolve_repo_path(cfg.checkpoint_path)
    if explicit is not None:
        return explicit if explicit.exists() else None
    if cfg.fold_id is not None and cfg.output_dir:
        path = _resolve_repo_path(cfg.output_dir)
        if path is None:
            return None
        checkpoint = path / f"fold_{int(cfg.fold_id):02d}" / "checkpoint_best.pt"
        return checkpoint if checkpoint.exists() else None
    return _latest_checkpoint(cfg.output_dir)


def _market_fold_dir(cfg: LiveMarketConfig) -> Path:
    checkpoint = _market_model_checkpoint(cfg)
    if checkpoint is None:
        raise MarketUnsupportedError(cfg)
    return checkpoint.parent


def _market_has_model(cfg: LiveMarketConfig) -> bool:
    return _market_model_checkpoint(cfg) is not None


def _unsupported_message(cfg: LiveMarketConfig) -> str:
    if cfg.unsupported_message:
        return cfg.unsupported_message
    return f"**{cfg.label}** 目前不支援：尚未上線可用模型。之後模型上線後就會支援。"


def _runtime_status(cfg: LiveMarketConfig) -> MarketRuntimeStatus:
    return runtime_status(cfg, root=ROOT, enabled_override=_market_enabled(cfg))


def _ensure_signal_ready(cfg: LiveMarketConfig, *, scheduled: bool = False) -> MarketRuntimeStatus:
    del scheduled
    status = _runtime_status(cfg)
    if not status.enabled:
        raise MarketDisabledError(cfg)
    if status.checkpoint is None:
        raise MarketUnsupportedError(cfg)
    non_trading_day = (not status.market_open) and "not a trading day" in (status.market_open_reason or "")
    if not status.data.fresh and not non_trading_day:
        raise DataStaleError(cfg, status)
    return status


def _market_notice(status: MarketRuntimeStatus) -> str | None:
    if status.market_open:
        return None
    data_date = _display_cfg_time(status.cfg, status.data.panel_date or status.data.last_data_date or "n/a")
    reason = status.market_open_reason or "market closed"
    if "not a trading day" in reason:
        freshness = f"資料提醒：{status.data.reason}。" if status.data.reason else ""
        return f"今天沒有開盤，使用最後可用資料 `{data_date}`（{_display_tz_text(status.cfg)}）產生訊號。{freshness}"
    return f"目前非交易時間，使用最後可用資料 `{data_date}`（{_display_tz_text(status.cfg)}）產生訊號。"


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
    raw = _env("STOCKAGENT_SCHEDULED_MARKETS")
    if raw:
        items = [item.strip() for item in raw.split(",") if item.strip()]
        if any(item.lower() in {"all", "*"} for item in items):
            return sorted(_market_configs())
        return items
    return [_default_market()]


def _scheduled_signal_key(cfg: LiveMarketConfig, now: datetime) -> str | None:
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
    if now.strftime("%H:%M") != _market_schedule_time(cfg):
        return None
    return f"{now.strftime('%Y-%m-%d')}:{cfg.market}"


def _run_pre_signal_command(cfg: LiveMarketConfig) -> None:
    if not cfg.pre_signal_command:
        return
    command = [str(item) for item in cfg.pre_signal_command]
    started = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=max(1, int(cfg.pre_signal_timeout_seconds)),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        _log_exception(f"pre_signal_command:{cfg.market}", exc)
        raise BotUserError(f"`{cfg.market}` pre-signal data update timed out after {cfg.pre_signal_timeout_seconds}s")
    log_path = ROOT / "artifacts" / "discord_bot" / "pre_signal_commands.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "timestamp": started,
                    "market": cfg.market,
                    "command": command,
                    "returncode": result.returncode,
                    "stdout_tail": result.stdout[-4000:],
                    "stderr_tail": result.stderr[-4000:],
                },
                ensure_ascii=False,
            )
            + "\n"
        )
    if result.returncode != 0:
        raise BotUserError(
            f"`{cfg.market}` pre-signal data update failed rc={result.returncode}; log=`{_display_path(log_path)}`"
        )


def _auto_signal_price_source(cfg: LiveMarketConfig, status: MarketRuntimeStatus, requested: str | None) -> str | None:
    text = str(requested or "").strip().lower()
    if text and text != "auto":
        return text
    if not status.market_open:
        return None
    if cfg.pre_signal_command:
        return "panel"
    return "yahoo"


def _prepare_realtime_signal_sync(
    cfg: LiveMarketConfig,
    *,
    requested_price_source: str | None = "auto",
    force_refresh: bool = False,
) -> tuple[str | None, MarketRuntimeStatus, bool]:
    status = _runtime_status(cfg)
    should_refresh = bool(force_refresh or status.market_open)
    if should_refresh:
        _run_pre_signal_command(cfg)
        status = _runtime_status(cfg)
    return _auto_signal_price_source(cfg, status, requested_price_source), status, should_refresh


async def market_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    del interaction
    query = str(current or "").strip().lower()
    choices: list[app_commands.Choice[str]] = []
    for key, cfg in sorted(_market_configs().items()):
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
) -> dict:
    cfg = _resolve_market(market)
    status = _ensure_signal_ready(cfg, scheduled=scheduled)
    overrides = {
        "price_source": price_source if price_source and price_source != "auto" else None,
        "top_n": top_n,
        "min_abs_delta": min_abs_delta,
        "signal_id": signal_id,
        "market_notice": _market_notice(status),
    }
    return cfg.signal_kwargs(**overrides)


async def _send_command_error(interaction: discord.Interaction, prefix: str, exc: Exception) -> None:
    if isinstance(exc, BotUserError):
        _log_exception(prefix, exc)
        await interaction.followup.send(str(exc))
        return
    _log_exception(prefix, exc)
    await interaction.followup.send(
        f"{prefix} failed: `{type(exc).__name__}`。詳細 traceback 已寫入 `{ERROR_LOG_PATH}`。"
    )


class StockAgentBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.tz = ZoneInfo(_env("STOCKAGENT_TZ", "Asia/Taipei") or "Asia/Taipei")
        self.signal_time = _env("STOCKAGENT_SIGNAL_TIME", "13:15") or "13:15"
        self.channel_id = _env_int("DISCORD_CHANNEL_ID")
        self._last_scheduled_keys: set[str] = set()
        self._last_daily_summary_keys: set[str] = set()
        self._synced_guild_id: int | None = None

    async def setup_hook(self) -> None:
        await self.tree.sync()
        scheduled_signal.start()
        daily_summary.start()

    async def on_ready(self) -> None:
        print(f"logged in as {self.user} signal_time={self.signal_time} channel_id={self.channel_id}", flush=True)
        if self.channel_id is None or self._synced_guild_id is not None:
            return
        channel = bot.get_channel(self.channel_id) or await bot.fetch_channel(self.channel_id)
        guild = getattr(channel, "guild", None)
        if guild is None:
            return
        self.tree.copy_global_to(guild=guild)
        synced = await self.tree.sync(guild=guild)
        self._synced_guild_id = int(guild.id)
        print(f"synced {len(synced)} app commands to guild={guild.id}", flush=True)


bot = StockAgentBot()


def _run_market_signal_sync(**kwargs):
    return generate_live_signal(**_signal_kwargs(**kwargs))


async def _run_market_signal(**kwargs):
    return await asyncio.to_thread(_run_market_signal_sync, **kwargs)


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
    raw_window = recent.get("window_days") or cfg.benchmark_window_days
    try:
        window = int(raw_window)
    except Exception:
        window = int(cfg.benchmark_window_days)
    frequency = str(cfg.history_frequency or "").strip().lower()
    if frequency in {"bar", "bars", "intraday", "15m", "15min", "15minute", "15minutes"}:
        try:
            market_cfg = load_config(_resolve_repo_path(cfg.config_path) or Path(cfg.config_path))
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
        market_cfg = load_config(_resolve_repo_path(cfg.config_path) or Path(cfg.config_path))
    except Exception:
        return None, None
    gross = _float_or_none(getattr(market_cfg.trading, "gross_leverage", None))
    turnover = _float_or_none(getattr(market_cfg.trading, "max_turnover_ratio", None))
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
        "訊號異常，已暫停自動公開播報；請人工確認資料連續性、報酬與風險後再使用。",
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


async def _send_paginated_response(interaction: discord.Interaction, pages: list[str]) -> None:
    clean_pages = [page if page else "(empty)" for page in pages] or ["(empty)"]
    view = PagedTextView(clean_pages) if len(clean_pages) > 1 else None
    if view is None:
        await interaction.followup.send(clean_pages[0])
    else:
        await interaction.followup.send(clean_pages[0], view=view)


async def _send_channel_pages(channel: Any, pages: list[str], *, timeout: float | None = 24 * 60 * 60) -> None:
    clean_pages = [page if page else "(empty)" for page in pages] or ["(empty)"]
    view = PagedTextView(clean_pages, timeout=timeout) if len(clean_pages) > 1 else None
    if view is None:
        await channel.send(clean_pages[0])
    else:
        await channel.send(clean_pages[0], view=view)


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
        if _row_abs(row, "current_weight") > 1e-9
        or _row_abs(row, "target_weight") > 1e-9
        or _row_abs(row, "delta_weight") > 1e-9
    ]


def _scheduled_detail_page_groups(
    cfg: LiveMarketConfig,
    result: Any,
    *,
    title_prefix: str = "scheduled",
    include_decisions: bool = False,
    debug: bool = False,
) -> list[list[str]]:
    capital = _resolve_current_capital(cfg)
    summary = result.summary
    output_dir = summary.get("output_dir") or result.output_dir
    output_text = _display_path(Path(output_dir)) if output_dir else "n/a"
    common_header = [
        _kv_line(
            ("market", summary.get("market", cfg.market)),
            ("signal", summary.get("signal_id", "n/a")),
            ("asof", _display_summary_time(summary, summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(summary, summary.get("panel_date", "n/a"))),
            ("price", summary.get("price_source", "n/a")),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
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
        ),
        _line_pages(
            title=f"{title_prefix} rebalance",
            rows=rebalance_rows,
            formatter=_rebalance_line,
            page_size=20,
            header_lines=rebalance_header,
        ),
    ]
    if include_decisions:
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
        status = _runtime_status(cfg)
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
        lines.append(_status_line(key, cfg, _runtime_status(cfg)))
    return lines


def _markets_lines() -> list[str]:
    lines = ["**stockAgent markets**"]
    for key, cfg in sorted(_market_configs().items()):
        fold = cfg.fold_id if cfg.fold_id is not None else "latest"
        runtime = _runtime_status(cfg)
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
        if cfg.live_output_dir:
            roots.append(_resolve_repo_path(cfg.live_output_dir) or Path(cfg.live_output_dir))
    roots.append(ROOT / "artifacts" / "live_signals")
    seen: set[Path] = set()
    for root in roots:
        if root in seen or not root.exists():
            continue
        seen.add(root)
        for path in sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            try:
                summary = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(summary, dict):
                continue
            if str(summary.get("signal_id") or "") == target or path.parent.name == target:
                return path, summary
    return None


def _latest_market_signal(cfg: LiveMarketConfig) -> tuple[Path, dict[str, Any]] | None:
    root = _resolve_repo_path(cfg.live_output_dir)
    if root is None or not root.exists():
        return None
    for path in sorted(root.glob("**/summary.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(summary, dict):
            return path, summary
    return None


def _sync_latest_live_weights_to_market_artifact(cfg: LiveMarketConfig) -> str | None:
    latest = _latest_market_signal(cfg)
    if latest is None:
        return None
    summary_path, summary = latest
    weights_path = _summary_artifact_path(summary, "weights_path", summary_path)
    if weights_path is None or not weights_path.exists():
        return None
    try:
        rows = _read_parquet_rows(weights_path)
        return write_live_weights_history(_market_fold_dir(cfg), summary, rows)
    except Exception as exc:
        _log_exception(f"sync_live_weights:{cfg.market}", exc)
        return None


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
        window = _load_portfolio_history_for_market(cfg, days, 0, 0.0, None, current_capital) if int(days or 0) > 0 else None
    except Exception as exc:
        window = None
        if debug:
            lines.append(f"history_load_error: `{type(exc).__name__}`")
    if window is not None:
        lines.extend(
            [
                "",
                f"**artifact history {window.days} periods**",
                _kv_line(
                    ("period", f"{window.start_date}..{window.end_date}"),
                    ("strategy", _signed_pct(window.period_return)),
                    ("baseline", _signed_pct(window.benchmark_return)),
                    ("profit", _signed_money(window.profit_value)),
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
        return load_symbol_name_map(parquet_root)
    except Exception:
        return {}


def _market_price_root(cfg: LiveMarketConfig) -> Path | None:
    try:
        config_path = _resolve_repo_path(cfg.config_path) or Path(cfg.config_path)
        config = load_config(config_path)
        parquet_root = Path(config.data.parquet_root)
        if not parquet_root.is_absolute():
            parquet_root = ROOT / parquet_root
        return parquet_root
    except Exception:
        return None


def _annotate_history_rows_with_display_time(cfg: LiveMarketConfig, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if isinstance(row, dict):
            if row.get("display_date"):
                continue
            row["display_date"] = _display_cfg_time(cfg, row.get("date", "n/a"))


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


def _live_signal_change_row(row: dict[str, Any], *, capital: float | None) -> dict[str, Any]:
    current_weight = _float_or_none(row.get("current_weight")) or 0.0
    target_weight = _float_or_none(row.get("target_weight")) or 0.0
    delta_weight = _float_or_none(row.get("delta_weight"))
    if delta_weight is None:
        delta_weight = target_weight - current_weight
    current_value = current_weight * capital if capital is not None else None
    target_value = target_weight * capital if capital is not None else None
    delta_value = delta_weight * capital if capital is not None else None
    raw_price_return = _float_or_none(row.get("price_return"))
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
    return {
        "symbol": str(row.get("symbol") or ""),
        "name": str(row.get("name") or ""),
        "action": str(row.get("action") or "HOLD"),
        "price": row.get("trade_price", row.get("current_price")),
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
    }


def _prepend_latest_signal_row_to_portfolio_history(
    result: PortfolioHistoryResult,
    *,
    summary_path: Path,
    summary: dict[str, Any],
    max_rows: int,
) -> bool:
    signal_date = str(
        summary.get("panel_data_date")
        or summary.get("weights_date")
        or summary.get("panel_date")
        or summary.get("asof_date")
        or ""
    ).strip()
    if not signal_date:
        return False
    signal_dt = _history_datetime(signal_date)
    end_dt = _history_datetime(result.end_date)
    if signal_dt is None or (end_dt is not None and signal_dt <= end_dt):
        return False

    capital = _float_or_none(summary.get("display_capital"))
    result_capital = getattr(result, "capital", None)
    if capital is None and result_capital is not None:
        capital = _float_or_none(getattr(result_capital, "capital", None))
    portfolio_return = _float_or_none(summary.get("portfolio_simple_return"))
    benchmark_return = _float_or_none(summary.get("benchmark_simple_return"))
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

    change_counts: dict[str, int] = {}
    changes_all: list[dict[str, Any]] = []
    for raw in rebalance_rows:
        action = str(raw.get("action") or "HOLD").upper()
        if action == "HOLD":
            continue
        change_counts[action] = change_counts.get(action, 0) + 1
        changes_all.append(_live_signal_change_row(raw, capital=capital))
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
        "display_date": _display_summary_time(summary, summary.get("panel_date") or summary.get("asof_date") or signal_date),
        "portfolio_return": portfolio_return,
        "benchmark_return": benchmark_return,
        "turnover": _float_or_none(summary.get("turnover")),
        "profit_value": profit_value,
        "nav": capital,
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
        "source": "latest_live_signal",
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


def _include_latest_signal_in_portfolio_history(
    cfg: LiveMarketConfig,
    result: PortfolioHistoryResult,
    *,
    max_rows: int,
) -> None:
    latest = _latest_market_signal(cfg)
    if latest is None:
        return
    summary_path, summary = latest
    _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=summary_path,
        summary=summary,
        max_rows=max_rows,
    )


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
    )
    _include_latest_signal_in_portfolio_history(cfg, result, max_rows=days)
    _annotate_history_rows_with_display_time(cfg, result.rows)
    return result


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
                "欄位: pnl≈前一期 NAV x 本期報酬估算；cum=本查詢期間累積報酬；top=本期絕對持倉比例變動最大的標的。",
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
            ("score", _num(row.get("score"), 3)),
        )
    )
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
        ),
        _return_pnl_line(row, ("current_weight", "holding_ratio", "target_weight")),
    ]
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
    return "\n".join(
        [
            f"`{row.get('display_date', row.get('date', 'n/a'))}` **{row.get('action', 'HOLD')}**",
            _kv_line(
                ("shares", f"{prev_shares}->{shares}"),
                ("delta", f"{share_delta:+d}"),
                ("px", _price(row.get("price"))),
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
    )


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
    lines = [
        f"    {index}. {label} **{action}**",
        "       "
        + _kv_inline(
            ("Δhold", _signed_pct(row.get("holding_ratio_delta"))),
            ("hold", _pct(row.get("holding_ratio"))),
            ("stock_ret", _signed_pct_zero_plain(_position_adjusted_return(row, pnl_weight_keys))),
            ("pnl_contrib", _signed_pct_zero_plain(_portfolio_return_contribution(row, pnl_weight_keys))),
        ),
    ]
    value_pairs: list[tuple[str, Any]] = []
    if _float_or_none(row.get("market_value")) is not None:
        value_pairs.append(("value", _money(row.get("market_value"))))
    if _float_or_none(row.get("market_value_delta")) is not None:
        value_pairs.append(("Δvalue", _signed_money(row.get("market_value_delta"))))
    if row.get("shares") is not None or row.get("share_delta") is not None:
        value_pairs.append(("shares", int(_float_or_none(row.get("shares")) or 0)))
        value_pairs.append(("Δsh", f"{int(_float_or_none(row.get('share_delta')) or 0):+d}"))
    value_pairs.append(("px", _price(row.get("price"))))
    lines.append("       " + _kv_inline(*value_pairs))
    return "\n".join(lines)


def _portfolio_history_block(row: dict[str, Any]) -> str:
    changes = row.get("changes")
    change_rows = changes if isinstance(changes, list) else []
    lines = [
        f"`{row.get('display_date', row.get('date', 'n/a'))}`",
        _kv_line(
            ("ret", _signed_pct(row.get("portfolio_return"))),
            ("bench", _signed_pct(row.get("benchmark_return"))),
            ("pnl", _signed_money(row.get("profit_value"))),
        ),
        _kv_line(
            ("cum", _signed_pct(row.get("cumulative_return"))),
            ("turnover", _pct(row.get("turnover"))),
            ("nav", _money(row.get("nav"))),
        ),
        _kv_line(
            ("gross", _pct(row.get("gross_ratio"))),
            ("net", _signed_pct(row.get("net_ratio"))),
            ("cash", _pct(row.get("cash_ratio"))),
        ),
        _kv_line(
            ("pos", row.get("position_count", "n/a")),
            ("long", row.get("long_count", "n/a")),
            ("short", row.get("short_count", "n/a")),
            ("changes", row.get("change_count", 0)),
        ),
        f"  change_mix: `{_portfolio_change_counts(row)}`",
    ]
    if change_rows:
        lines.append("  top changes:")
        for index, item in enumerate(change_rows[:8], start=1):
            if isinstance(item, dict):
                lines.append(_portfolio_change_block(item, index))
    else:
        lines.append("  top changes: none")
    return "\n".join(lines)


def _decision_line(row: dict[str, Any]) -> str:
    constraint = str(row.get("constraint") or "")
    constraint_text = f" constraint=`{constraint}`" if constraint else ""
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
    status = _runtime_status(cfg)
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
        if top:
            lines.append("top positions:")
            lines.extend(
                f"  {index}. {_symbol_label(row)} `{_pct(row.get('weight'))}`"
                for index, row in enumerate(top[:5], start=1)
                if isinstance(row, dict)
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
            await interaction.response.edit_message(content=self.pages[self.index], view=self)
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
        summary_path, summary = _latest_signal_or_raise(cfg)
        message = _latest_signal_message(
            cfg,
            summary_path,
            summary,
            top_n=max(0, int(top_n or 0)),
            current_capital=current_capital,
            debug=debug,
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
        summary_path, summary = _latest_signal_or_raise(cfg)
        pages = _latest_changes_pages(
            cfg,
            summary_path,
            summary,
            action=action,
            limit=limit,
            page_size=page_size,
            current_capital=current_capital,
            watchlist=watchlist,
            debug=debug,
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
    action="add/remove/list/clear",
    symbol="Symbol to add or remove.",
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
async def watchlist_command(
    interaction: discord.Interaction,
    action: str,
    market: str = "",
    symbol: str = "",
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
            verb = "加入"
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
        else:
            raise BotUserError("action 必須是 add/remove/list/clear。")
    except Exception as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    _record_audit_event(
        f"watch:{cfg.market}",
        f"watch_{action_value}",
        interaction,
        market=cfg.market,
        symbol=_normalize_watch_symbol(symbol),
        watchlist=items,
    )
    content = ", ".join(f"`{item}`" for item in items) if items else "(empty)"
    await interaction.response.send_message(f"`{cfg.market}` watchlist 已{verb}: {content}", ephemeral=True)


@bot.tree.command(name="signal_now", description="Run stockAgent live signal now.")
@app_commands.describe(
    market="Market id",
    price_source="auto/panel/csv/yahoo",
    top_n="Rows to show, minimum 10",
    min_abs_delta="Minimum absolute weight delta",
    refresh_data="Run the market pre-signal data updater before generating.",
    allow_unsafe="Show the signal even when sanity gate returns BLOCK.",
    debug="Show signal ids, fingerprints, output folders, and artifact paths.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def signal_now(
    interaction: discord.Interaction,
    market: str = "",
    price_source: str = "auto",
    top_n: int = 20,
    min_abs_delta: float = 0.001,
    refresh_data: bool = True,
    allow_unsafe: bool = False,
    debug: bool = False,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        resolved_price_source, status, auto_refreshed = await asyncio.to_thread(
            _prepare_realtime_signal_sync,
            cfg,
            requested_price_source=price_source,
            force_refresh=refresh_data,
        )
        await asyncio.to_thread(_sync_latest_live_weights_to_market_artifact, cfg)
        result = await _run_market_signal(
            market=market,
            price_source=resolved_price_source,
            top_n=_top_n(top_n),
            min_abs_delta=min_abs_delta,
        )
        result = _enrich_signal_performance_for_discord(cfg, result, max_rows=0, debug=debug)
    except Exception as exc:
        await _send_command_error(interaction, "live signal", exc)
        return
    sanity_issues = _signal_sanity_issues(cfg, result.summary)
    if _signal_sanity_level(sanity_issues) == "BLOCK" and not allow_unsafe:
        _record_audit_event(
            str(result.summary.get("signal_id")),
            "sanity_blocked",
            interaction,
            market=str(result.summary.get("market") or market or _default_market()),
            issues=[text for _, text in sanity_issues],
            output_dir=result.output_dir,
        )
        await interaction.followup.send(_signal_sanity_message(cfg, result.summary, sanity_issues))
        return
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
        sanity=_signal_sanity_level(sanity_issues),
    )
    await _send_signal_response(
        interaction,
        result.message,
        str(result.summary.get("signal_id")),
        str(result.summary.get("market") or market or _default_market()),
    )
    for pages in _scheduled_detail_page_groups(
        cfg,
        result,
        title_prefix="signal_now",
        include_decisions=True,
        debug=debug,
    ):
        await _send_paginated_response(interaction, pages)


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
        result = await _run_market_signal(market=market, top_n=_page_size(page_size))
    except Exception as exc:
        await _send_command_error(interaction, "positions", exc)
        return
    rows = sorted(
        result.weights_rows,
        key=lambda row: (_row_abs(row, "target_weight"), _row_abs(row, "delta_weight"), _row_abs(row, "score")),
        reverse=True,
    )
    if not include_zero:
        rows = [
            row
            for row in rows
            if _row_abs(row, "target_weight") > 1e-9
            or _row_abs(row, "current_weight") > 1e-9
            or _row_abs(row, "delta_weight") > 1e-9
        ]
    rows = _limit_rows(rows, limit)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    rows = _annotate_weight_rows_with_capital(rows, capital)
    header = [
        _kv_line(
            ("market", result.summary.get("market", "n/a")),
            ("asof", _display_summary_time(result.summary, result.summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(result.summary, result.summary.get("panel_date", "n/a"))),
            ("rows", len(rows)),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "sort: absolute target weight, then delta and score",
    ]
    if debug:
        header.extend(
            [
                _kv_line(
                    ("signal", result.summary.get("signal_id", "n/a")),
                    ("display_tz", result.summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"full: `{result.summary.get('positions_markdown_path', result.summary.get('weights_path', 'n/a'))}`",
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
        result = await _run_market_signal(market=market, top_n=_page_size(page_size), min_abs_delta=threshold)
    except Exception as exc:
        await _send_command_error(interaction, "rebalance", exc)
        return
    rows = _limit_rows(result.rebalance_rows, limit)
    capital = _resolve_current_capital(cfg, current_capital=current_capital)
    rows = _annotate_weight_rows_with_capital(rows, capital)
    header = [
        _kv_line(
            ("market", result.summary.get("market", "n/a")),
            ("asof", _display_summary_time(result.summary, result.summary.get("asof_date", "n/a"))),
            ("panel", _display_summary_time(result.summary, result.summary.get("panel_date", "n/a"))),
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
                    ("signal", result.summary.get("signal_id", "n/a")),
                    ("display_tz", result.summary.get("display_timezone_label") or _display_tz_text(cfg)),
                ),
                f"full: `{result.summary.get('rebalance_markdown_path', result.summary.get('rebalance_path', 'n/a'))}`",
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
    days="Periods to show. Daily markets use days; crypto can use 15m bars. Default 32. 0 means all.",
    top_changes="Top holding changes per period.",
    page_size="Periods per page. Default 1 means one day/bar per page.",
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
    top_changes: int = 5,
    page_size: int = 1,
    min_abs_change: float = 0.0,
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

    pages = _line_pages(
        title="portfolio history",
        rows=result.rows,
        formatter=_portfolio_history_block,
        page_size=page_size,
        header_lines=_portfolio_history_header_lines(cfg, result, debug=debug),
        min_page_size=1,
        default_page_size=1,
    )
    await _send_paginated_response(interaction, pages)


@bot.tree.command(name="set_market_enabled", description="Enable or disable a market in the Discord bot.")
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


@tasks.loop(seconds=10)
async def scheduled_signal() -> None:
    if bot.channel_id is None:
        return
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        key = _scheduled_signal_key(cfg, now)
        if key is None:
            continue
        if key in bot._last_scheduled_keys:
            continue
        bot._last_scheduled_keys.add(key)
        try:
            resolved_price_source, _, _ = await asyncio.to_thread(
                _prepare_realtime_signal_sync,
                cfg,
                requested_price_source="auto",
                force_refresh=False,
            )
            await asyncio.to_thread(_sync_latest_live_weights_to_market_artifact, cfg)
            result = await _run_market_signal(
                market=market,
                scheduled=True,
                price_source=resolved_price_source,
            )
            result = _enrich_signal_performance_for_discord(cfg, result, max_rows=0)
            sanity_issues = _signal_sanity_issues(cfg, result.summary)
            if _signal_sanity_level(sanity_issues) == "BLOCK":
                await channel.send(_signal_sanity_message(cfg, result.summary, sanity_issues))
                continue
            if sanity_issues:
                result.message = _prepend_sanity_notice(result.message, cfg, result.summary)
        except BotUserError as exc:
            if not isinstance(exc, MarketClosedError):
                await channel.send(str(exc))
            continue
        except Exception as exc:
            _log_exception(f"scheduled_signal:{market}", exc)
            await channel.send(f"`{market}` scheduled signal failed: `{type(exc).__name__}`")
            continue
        await channel.send(result.message, view=SignalReviewView(
            signal_id=str(result.summary.get("signal_id")),
            market=str(result.summary.get("market") or market),
        ))
        for pages in _scheduled_detail_page_groups(cfg, result):
            await _send_channel_pages(channel, pages)


@tasks.loop(minutes=1)
async def daily_summary() -> None:
    if bot.channel_id is None:
        return
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        summary_time = _market_summary_time(cfg)
        if not summary_time:
            continue
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        if now.strftime("%H:%M") != summary_time:
            continue
        today = now.strftime("%Y-%m-%d")
        key = f"{today}:{market}"
        if key in bot._last_daily_summary_keys:
            continue
        bot._last_daily_summary_keys.add(key)
        try:
            message = await asyncio.to_thread(_daily_summary_message, cfg)
            await channel.send(message[:1900])
        except MarketUnsupportedError as exc:
            await channel.send(str(exc))
            continue
        except Exception as exc:
            _log_exception(f"daily_summary:{market}", exc)
            await channel.send(f"`{market}` daily summary failed: `{type(exc).__name__}`")


@scheduled_signal.before_loop
async def before_scheduled_signal() -> None:
    await bot.wait_until_ready()


@daily_summary.before_loop
async def before_daily_summary() -> None:
    await bot.wait_until_ready()


RELOAD_CHILD_ENV = "STOCKAGENT_DISCORD_BOT_CHILD"
WATCH_ROOTS_DEFAULT = (
    "services/discord_bot",
    "stockagent/live",
    "configs/markets",
    "scripts/live_signal.py",
)
WATCH_EXTENSIONS = {".py", ".yaml", ".yml"}
WATCH_FILENAMES = {".env"}
WATCH_SKIP_DIRS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "node_modules"}


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _watch_delay_seconds() -> float:
    raw = _env("STOCKAGENT_BOT_RESTART_DELAY_SECONDS", "10") or "10"
    return max(0.0, float(raw))


def _watch_poll_seconds() -> float:
    raw = _env("STOCKAGENT_BOT_RELOAD_POLL_SECONDS", "1") or "1"
    return max(0.2, float(raw))


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
        f"delay={delay:.1f}s poll={poll:.1f}s paths={', '.join(str(path) for path in _watch_roots())}",
        flush=True,
    )
    try:
        while not stop_requested:
            exit_code = child.poll()
            if exit_code is not None:
                print(f"[bot-reload] child exited code={exit_code}; restarting in {delay:.1f}s", flush=True)
                time.sleep(delay)
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
                print(f"[bot-reload] file update detected: {preview}; restart in {delay:.1f}s", flush=True)

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
