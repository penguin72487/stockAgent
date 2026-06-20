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
from datetime import datetime
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
            os.environ.setdefault(key, value)


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
from stockagent.live.signal_engine import generate_live_signal
from stockagent.live.stock_history import StockHistoryResult, load_stock_history


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
            f"**{cfg.label}** latest=`{status.data.last_data_date or 'n/a'}` "
            f"expected=`{status.data.expected_latest_date or 'n/a'}` reason=`{detail}`"
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
        return {"markets": {}}
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"markets": {}}
    if not isinstance(raw, dict):
        return {"markets": {}}
    raw.setdefault("markets", {})
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


def _market_enabled(cfg: LiveMarketConfig) -> bool:
    entry = _market_state(cfg.market)
    if "enabled" in entry:
        return bool(entry["enabled"])
    return bool(cfg.enabled)


def _market_schedule_time(cfg: LiveMarketConfig) -> str:
    entry = _market_state(cfg.market)
    value = entry.get("schedule_time") or cfg.schedule_time or bot.signal_time
    return str(value)


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
    data_date = status.data.panel_date or status.data.last_data_date or "n/a"
    reason = status.market_open_reason or "market closed"
    if "not a trading day" in reason:
        freshness = f"資料提醒：{status.data.reason}。" if status.data.reason else ""
        return f"今天沒有開盤，使用最後可用資料 `{data_date}` 產生訊號。{freshness}"
    return f"目前非交易時間，使用最後可用資料 `{data_date}` 產生訊號。"


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
        return [item.strip() for item in raw.split(",") if item.strip()]
    return [_default_market()]


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


def _page_size(value: int | None) -> int:
    try:
        number = int(value or 20)
    except Exception:
        number = 20
    return max(5, min(40, number))


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


def _shorten(text: Any, max_chars: int = 220) -> str:
    value = str(text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[: max(0, max_chars - 3)].rstrip() + "..."


def _kv_line(*pairs: tuple[str, Any]) -> str:
    return "  " + "  ".join(f"`{key}={value}`" for key, value in pairs)


def _line_pages(
    *,
    title: str,
    rows: list[dict[str, Any]],
    formatter,
    page_size: int,
    header_lines: list[str] | None = None,
) -> list[str]:
    size = _page_size(page_size)
    total = len(rows)
    if total == 0:
        return [f"**{title}**\n(no rows)"]
    pages: list[str] = []
    page_count = (total + size - 1) // size
    for page_index, start in enumerate(range(0, total, size), start=1):
        chunk = rows[start : start + size]
        lines = [
            f"**{title}**",
            f"`page {page_index}/{page_count}`  `rows {start + 1}-{start + len(chunk)}/{total}`",
        ]
        if header_lines:
            lines.extend(header_lines)
        for row in chunk:
            lines.append("")
            lines.append(formatter(row))
        pages.extend(_split_content_pages("\n".join(lines)))
    return pages


async def _send_paginated_response(interaction: discord.Interaction, pages: list[str]) -> None:
    clean_pages = [page if page else "(empty)" for page in pages] or ["(empty)"]
    view = PagedTextView(clean_pages) if len(clean_pages) > 1 else None
    if view is None:
        await interaction.followup.send(clean_pages[0])
    else:
        await interaction.followup.send(clean_pages[0], view=view)


def _status_line(key: str, cfg: LiveMarketConfig, status: MarketRuntimeStatus) -> str:
    checkpoint = status.checkpoint
    fold = checkpoint.fold_id if checkpoint is not None and checkpoint.fold_id is not None else "none"
    mtime = checkpoint.mtime if checkpoint is not None else "none"
    test_years = ",".join(str(x) for x in checkpoint.test_years) if checkpoint is not None and checkpoint.test_years else "n/a"
    best_metric = checkpoint.best_metric if checkpoint is not None and checkpoint.best_metric else "n/a"
    enabled = "enabled" if status.enabled else "disabled"
    return (
        f"`{key}` {cfg.label} status=`{status.status}` {enabled} "
        f"data=`{status.data.last_data_date or 'n/a'}` panel=`{status.data.panel_date or 'n/a'}` "
        f"benchmark=`{status.data.benchmark_date or 'n/a'}` expected=`{status.data.expected_latest_date or 'n/a'}` "
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
            f"schedule_time=`{_market_schedule_time(cfg)}` summary_time=`{_market_summary_time(cfg) or 'off'}` tz=`{cfg.timezone}`",
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
            f"data=`{runtime.data.last_data_date or 'n/a'}` schedule=`{_market_schedule_time(cfg)}` "
            f"config=`{cfg.config_path}` output=`{cfg.output_dir or 'config default'}` fold=`{fold}`"
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


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _market_symbol_names(cfg: LiveMarketConfig) -> dict[str, str]:
    try:
        config_path = _resolve_repo_path(cfg.config_path) or Path(cfg.config_path)
        config = load_config(config_path)
        parquet_root = Path(config.data.parquet_root)
        if not parquet_root.is_absolute():
            parquet_root = ROOT / parquet_root
        return load_symbol_name_map(parquet_root)
    except Exception:
        return {}


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
    return load_stock_history(
        _market_fold_dir(cfg),
        symbol,
        limit=limit,
        changes_only=changes_only,
        initial_capital=initial,
        current_capital=current,
        symbol_names=_market_symbol_names(cfg),
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
    return load_portfolio_history(
        _market_fold_dir(cfg),
        days=days,
        top_changes=top_changes,
        min_abs_change=min_abs_change,
        initial_capital=initial,
        current_capital=current,
        symbol_names=_market_symbol_names(cfg),
    )


def _position_line(row: dict[str, Any]) -> str:
    lines = [
        f"{_symbol_label(row)}",
        _kv_line(
            ("target", _pct(row.get("target_weight"))),
            ("now", _pct(row.get("current_weight"))),
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
            ("ret", _signed_pct(row.get("price_return"))),
            ("score", _num(row.get("score"), 3)),
        )
    )
    return "\n".join(lines)


def _rebalance_line(row: dict[str, Any]) -> str:
    delta = _float_or_none(row.get("delta_weight")) or 0.0
    side = str(row.get("action") or ("BUY" if delta > 0 else "SELL"))
    lines = [
        f"{_symbol_label(row)} **{side}**",
        _kv_line(
            ("delta", _signed_pct(delta)),
            ("px", _price(row.get("trade_price", row.get("current_price")))),
            ("ret", _signed_pct(row.get("price_return"))),
        ),
        _kv_line(
            ("now", _pct(row.get("current_weight"))),
            ("target", _pct(row.get("target_weight"))),
        ),
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
            f"`{row.get('date', 'n/a')}` **{row.get('action', 'HOLD')}**",
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
    return (
        f"{label} {row.get('action', 'HOLD')} "
        f"Δhold={_signed_pct(row.get('holding_ratio_delta'))} "
        f"hold={_pct(row.get('holding_ratio'))} "
        f"value={_money(row.get('market_value'))} "
        f"Δvalue={_signed_money(row.get('market_value_delta'))} "
        f"shares={int(_float_or_none(row.get('shares')) or 0)} "
        f"Δsh={int(_float_or_none(row.get('share_delta')) or 0):+d} "
        f"px={_price(row.get('price'))}"
    )


def _portfolio_history_block(row: dict[str, Any]) -> str:
    changes = row.get("changes")
    change_rows = changes if isinstance(changes, list) else []
    lines = [
        f"`{row.get('date', 'n/a')}`",
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
                lines.append(f"    {index}. {_portfolio_change_line(item)}")
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
        f"px=`{_price(row.get('trade_price', row.get('current_price')))}`"
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
                ("ret", _signed_pct(row.get("price_return"))),
                ("score", _num(row.get("score"), 4)),
                ("rank", row.get("abs_score_rank", "n/a")),
            ),
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
            ("asof", summary.get("asof_date", "n/a")),
            ("panel", summary.get("panel_date", "n/a")),
            ("fold", summary.get("fold_id", "n/a")),
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
        _kv_line(
            ("confidence", _num(explanation.get("confidence_proxy_score_std"), 4)),
            ("source", _shorten(explanation.get("source", "score/weight decision table"), 80)),
        ),
    ]
    if features:
        lines.append("  feature drivers:")
        lines.extend(f"    {index}. {_driver_line(row, feature=True)}" for index, row in enumerate(features[:5], start=1) if isinstance(row, dict))
    if scores:
        lines.append("  score drivers:")
        lines.extend(f"    {index}. {_driver_line(row)}" for index, row in enumerate(scores[:5], start=1) if isinstance(row, dict))
    lines.extend(
        [
            "",
            "**files**",
            f"report: `{report_path}`",
            f"table: `{explain_path}`",
            "",
            "**欄位說明**",
            "score=模型排序分數；model=交易約束前權重；target=最終目標權重；delta=要調整的權重。",
            "gate/market_delta=Transformer 市場脈絡影響；constraint=買賣/交易限制。",
        ]
    )
    return "\n".join(lines)


def _daily_summary_message(cfg: LiveMarketConfig) -> str:
    status = _runtime_status(cfg)
    latest = _latest_market_signal(cfg)
    lines = [
        f"**daily summary** {cfg.label}",
        _kv_line(("status", status.status), ("generated", "yes" if latest else "no")),
        _kv_line(
            ("data", status.data.last_data_date or "n/a"),
            ("panel", status.data.panel_date or "n/a"),
            ("benchmark", status.data.benchmark_date or "n/a"),
        ),
    ]
    if not status.data.fresh:
        lines.append(f"warning: 資料過期，目前不建議使用。 `{status.data.reason or 'stale'}`")
    notice = _market_notice(status)
    if notice:
        lines.append(f"notice: {notice}")
    if latest is not None:
        path, summary = latest
        lines.extend(
            [
                "",
                "**latest signal**",
                _kv_line(
                    ("signal", summary.get("signal_id", path.parent.name)),
                    ("asof", summary.get("asof_date", "n/a")),
                    ("fold", summary.get("fold_id", "n/a")),
                ),
                _kv_line(
                    ("portfolio", _signed_pct(summary.get("portfolio_simple_return"))),
                    ("benchmark", _signed_pct(summary.get("benchmark_simple_return"))),
                    ("turnover", _pct(summary.get("turnover"))),
                ),
                f"artifact: `{path}`",
            ]
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
    return "\n".join(lines)


class PagedTextView(discord.ui.View):
    def __init__(self, pages: list[str]) -> None:
        super().__init__(timeout=30 * 60)
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


@bot.tree.command(name="signal_now", description="Run stockAgent live signal now.")
@app_commands.describe(market="Market id", price_source="auto/panel/csv/yahoo", top_n="Rows to show", min_abs_delta="Minimum absolute weight delta")
@app_commands.autocomplete(market=market_autocomplete)
async def signal_now(
    interaction: discord.Interaction,
    market: str = "",
    price_source: str = "auto",
    top_n: int = 20,
    min_abs_delta: float = 0.001,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        result = await _run_market_signal(
            market=market,
            price_source=price_source,
            top_n=top_n,
            min_abs_delta=min_abs_delta,
        )
    except Exception as exc:
        await _send_command_error(interaction, "live signal", exc)
        return
    _record_audit_event(
        str(result.summary.get("signal_id")),
        "generated",
        interaction,
        market=str(result.summary.get("market") or market or _default_market()),
        output_dir=result.output_dir,
    )
    await _send_signal_response(
        interaction,
        result.message,
        str(result.summary.get("signal_id")),
        str(result.summary.get("market") or market or _default_market()),
    )


@bot.tree.command(name="positions", description="Show target position weights.")
@app_commands.describe(
    market="Market id",
    limit="Max rows to show. 0 means all non-zero rows.",
    page_size="Rows per page, clamped to 5-40.",
    include_zero="Include zero-weight universe rows.",
    current_capital="Current account capital used to estimate position amounts.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def positions(
    interaction: discord.Interaction,
    market: str = "",
    limit: int = 0,
    page_size: int = 20,
    include_zero: bool = False,
    current_capital: float = 0.0,
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
            ("signal", result.summary.get("signal_id", "n/a")),
            ("rows", len(rows)),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "sort: absolute target weight, then delta and score",
        f"full: `{result.summary.get('positions_markdown_path', result.summary.get('weights_path', 'n/a'))}`",
    ]
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
    page_size="Rows per page, clamped to 5-40.",
    current_capital="Current account capital used to estimate trade amounts.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def rebalance(
    interaction: discord.Interaction,
    market: str = "",
    threshold: float = 0.001,
    limit: int = 0,
    page_size: int = 20,
    current_capital: float = 0.0,
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
            ("signal", result.summary.get("signal_id", "n/a")),
            ("threshold", threshold),
            ("rows", len(rows)),
        ),
        f"capital: `{_capital_context_text(capital=capital)}`",
        "sort: absolute rebalance delta",
        f"full: `{result.summary.get('rebalance_markdown_path', result.summary.get('rebalance_path', 'n/a'))}`",
    ]
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
@app_commands.describe(signal_id="signal_id from /signal_now")
async def signal(interaction: discord.Interaction, signal_id: str) -> None:
    await interaction.response.defer(thinking=True)
    found = _find_signal_summary(signal_id)
    if found is None:
        await interaction.followup.send(f"找不到 signal_id=`{signal_id}`。")
        return
    path, summary = found
    risk = summary.get("target_risk", {}) if isinstance(summary.get("target_risk"), dict) else {}
    lines = [
        f"**signal** `{summary.get('signal_id', signal_id)}`",
        f"market=`{summary.get('market', 'n/a')}` asof=`{summary.get('asof_date', 'n/a')}` panel=`{summary.get('panel_date', 'n/a')}`",
        f"fold=`{summary.get('fold_id', 'n/a')}` checkpoint=`{summary.get('checkpoint_fingerprint', 'n/a')}` config=`{summary.get('config_fingerprint', 'n/a')}`",
        f"risk gross=`{_pct(risk.get('gross'))}` top=`{_pct(risk.get('top_abs_weight'))}` turnover=`{_pct(summary.get('turnover'))}`",
        f"summary=`{path}`",
        f"weights=`{summary.get('weights_path', 'n/a')}`",
        f"rebalance=`{summary.get('rebalance_path', 'n/a')}`",
        f"explain=`{summary.get('decision_explanation_path', 'n/a')}`",
    ]
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
    page_size="Rows per page, clamped to 5-40.",
    actionable_only="Hide HOLD rows.",
    attach_file="Upload the full markdown decision report.",
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
    page_size: int = 8,
    actionable_only: bool = True,
    attach_file: bool = False,
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
    limit="Max rows to show. Default 32. 0 means all rows.",
    page_size="Rows per page, clamped to 5-40.",
    changes_only="Only show trade/adjustment rows. If false, show recent daily state rows.",
    initial_capital="Scale fold values from the first fold NAV.",
    current_capital="Scale fold values from the latest fold NAV. Overrides initial_capital.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def stock_history_command(
    interaction: discord.Interaction,
    symbol: str,
    market: str = "",
    limit: int = 32,
    page_size: int = 8,
    changes_only: bool = True,
    initial_capital: float = 0.0,
    current_capital: float = 0.0,
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
    source_text = _shorten(", ".join(_display_path(path) for path in result.source_paths), 700)
    header = [
        _kv_line(
            ("market", cfg.market),
            ("fold", _display_path(result.fold_dir)),
            ("requested", result.requested_symbol),
        ),
        _kv_line(
            ("changes_only", result.changes_only),
            ("fallback_all_rows", result.fell_back_to_all_rows),
            ("rows", len(result.rows)),
        ),
        _kv_line(
            ("capital_mode", result.capital.mode if result.capital else "artifact"),
            ("capital", _money(result.capital.capital) if result.capital else "n/a"),
            ("ref", result.capital.reference_date if result.capital else "n/a"),
        ),
        f"sources: `{source_text}`",
        "欄位: hold=實際持倉比例；actual=整數股回測權重；model=模型目標權重；Δ=相對上一筆交易日變化。",
    ]
    pages = _line_pages(
        title=f"stock history {label}",
        rows=result.rows,
        formatter=_stock_history_block,
        page_size=page_size,
        header_lines=header,
    )
    await _send_paginated_response(interaction, pages)


@bot.tree.command(name="portfolio_history", description="Show recent daily PnL and holding changes.")
@app_commands.describe(
    market="Market id.",
    days="Trading days to show. Default 32. 0 means all days.",
    top_changes="Top holding changes per day.",
    page_size="Days per page, clamped to 5-40.",
    min_abs_change="Hide weight-only changes below this absolute ratio.",
    initial_capital="Scale fold values from the first fold NAV.",
    current_capital="Scale fold values from the latest fold NAV. Overrides initial_capital.",
)
@app_commands.autocomplete(market=market_autocomplete)
async def portfolio_history_command(
    interaction: discord.Interaction,
    market: str = "",
    days: int = 32,
    top_changes: int = 5,
    page_size: int = 5,
    min_abs_change: float = 0.0,
    initial_capital: float = 0.0,
    current_capital: float = 0.0,
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

    source_text = _shorten(", ".join(_display_path(path) for path in result.source_paths), 700)
    header = [
        _kv_line(
            ("market", cfg.market),
            ("fold", _display_path(result.fold_dir)),
            ("days", result.days),
        ),
        _kv_line(
            ("period", f"{result.start_date or 'n/a'}..{result.end_date or 'n/a'}"),
            ("ret", _signed_pct(result.period_return)),
            ("benchmark", _signed_pct(result.benchmark_return)),
        ),
        _kv_line(("profit", _signed_money(result.profit_value)), ("top_changes", result.top_changes)),
        _kv_line(
            ("capital_mode", result.capital.mode),
            ("capital", _money(result.capital.capital)),
            ("ref", result.capital.reference_date or "n/a"),
        ),
        f"sources: `{source_text}`",
        "欄位: pnl≈前一日 NAV x 當日報酬估算；cum=本查詢期間累積報酬；top=當天絕對持倉比例變動最大的股票。",
    ]
    pages = _line_pages(
        title="portfolio history",
        rows=result.rows,
        formatter=_portfolio_history_block,
        page_size=page_size,
        header_lines=header,
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
@app_commands.describe(market="Market id")
@app_commands.autocomplete(market=market_autocomplete)
async def daily_summary_command(interaction: discord.Interaction, market: str = "") -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        message = await asyncio.to_thread(_daily_summary_message, cfg)
    except Exception as exc:
        await _send_command_error(interaction, "daily_summary", exc)
        return
    await _send_long_response(interaction, message)


@tasks.loop(minutes=1)
async def scheduled_signal() -> None:
    if bot.channel_id is None:
        return
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    for market in _scheduled_markets():
        cfg = _resolve_market(market)
        now = datetime.now(ZoneInfo(cfg.timezone or bot.tz.key))
        if now.strftime("%H:%M") != _market_schedule_time(cfg):
            continue
        today = now.strftime("%Y-%m-%d")
        key = f"{today}:{market}"
        if key in bot._last_scheduled_keys:
            continue
        bot._last_scheduled_keys.add(key)
        try:
            result = await _run_market_signal(market=market, scheduled=True)
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
