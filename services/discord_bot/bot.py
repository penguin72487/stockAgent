from __future__ import annotations

import asyncio
import json
import os
import sys
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
from stockagent.live.signal_engine import generate_live_signal


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


async def _run_signal(**kwargs):
    return await asyncio.to_thread(generate_live_signal, **kwargs)


async def _send_long_response(interaction: discord.Interaction, content: str) -> None:
    if len(content) <= 1900:
        await interaction.followup.send(content)
        return
    await interaction.followup.send(content[:1900])


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


def _pct(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
    except Exception:
        return "n/a"
    return f"{number * 100:.{digits}f}%"


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


def _daily_summary_message(cfg: LiveMarketConfig) -> str:
    status = _runtime_status(cfg)
    latest = _latest_market_signal(cfg)
    lines = [
        f"**daily summary** {cfg.label}",
        f"status=`{status.status}` generated=`{'yes' if latest else 'no'}`",
        (
            f"data=`{status.data.last_data_date or 'n/a'}` "
            f"panel=`{status.data.panel_date or 'n/a'}` "
            f"benchmark=`{status.data.benchmark_date or 'n/a'}`"
        ),
    ]
    if not status.data.fresh:
        lines.append(f"warning: 資料過期，目前不建議使用。 `{status.data.reason or 'stale'}`")
    notice = _market_notice(status)
    if notice:
        lines.append(f"notice: {notice}")
    if latest is not None:
        path, summary = latest
        lines.append(f"signal=`{summary.get('signal_id', path.parent.name)}` artifact=`{path}`")
        warnings = summary.get("risk_warnings") if isinstance(summary.get("risk_warnings"), list) else []
        if warnings:
            lines.append("risk_warning: " + " | ".join(str(item) for item in warnings[:3]))
        top = summary.get("top_positions") if isinstance(summary.get("top_positions"), list) else []
        if top:
            lines.append(
                "top positions: "
                + ", ".join(f"{row.get('symbol')}:{_pct(row.get('weight'))}" for row in top[:5] if isinstance(row, dict))
            )
    return "\n".join(lines)


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
        result = await _run_signal(
            **_signal_kwargs(market=market, price_source=price_source, top_n=top_n, min_abs_delta=min_abs_delta)
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
@app_commands.describe(market="Market id", top_n="Rows to show")
@app_commands.autocomplete(market=market_autocomplete)
async def positions(interaction: discord.Interaction, market: str = "", top_n: int = 20) -> None:
    await interaction.response.defer(thinking=True)
    try:
        result = await _run_signal(**_signal_kwargs(market=market, top_n=top_n))
    except Exception as exc:
        await _send_command_error(interaction, "positions", exc)
        return
    rows = result.summary.get("top_positions", [])[:top_n]
    lines = ["**target positions**"]
    for row in rows:
        lines.append(f"{_symbol_label(row)} {float(row['weight']) * 100:.2f}% px={float(row['current_price']):.2f}")
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="rebalance", description="Show rebalance deltas.")
@app_commands.describe(market="Market id", threshold="Minimum absolute weight delta", top_n="Rows to show")
@app_commands.autocomplete(market=market_autocomplete)
async def rebalance(interaction: discord.Interaction, market: str = "", threshold: float = 0.001, top_n: int = 20) -> None:
    await interaction.response.defer(thinking=True)
    try:
        cfg = _resolve_market(market)
        _require_trader_permission(interaction, cfg)
        result = await _run_signal(**_signal_kwargs(market=market, top_n=top_n, min_abs_delta=threshold))
    except Exception as exc:
        await _send_command_error(interaction, "rebalance", exc)
        return
    rows = result.summary.get("rebalance", [])[:top_n]
    lines = ["**rebalance**"]
    for row in rows:
        delta = float(row["delta_weight"])
        side = str(row.get("action") or ("BUY" if delta > 0 else "SELL"))
        trade_price = float(row.get("trade_price", row.get("current_price", float("nan"))))
        lines.append(
            f"{_symbol_label(row)} {side} delta={delta * 100:.2f}% "
            f"px={trade_price:.2f} "
            f"now={float(row['current_weight']) * 100:.2f}% target={float(row['target_weight']) * 100:.2f}%"
        )
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="health", description="Show bot configuration.")
@app_commands.describe(market="Market id")
@app_commands.autocomplete(market=market_autocomplete)
async def health(interaction: discord.Interaction, market: str = "") -> None:
    await interaction.response.defer(thinking=True)
    configs = _market_configs()
    if market:
        cfg = _resolve_market(market)
        status = _runtime_status(cfg)
        lines = [
            "**stockAgent bot health**",
            f"markets=`{', '.join(sorted(configs))}` default=`{_default_market()}`",
            _status_line(cfg.market, cfg, status),
            f"config=`{status.config_path}` config_hash=`{status.config_fingerprint or 'n/a'}`",
            f"output_dir=`{status.output_dir or 'config default'}` live_output_dir=`{cfg.live_output_dir or 'auto'}`",
            f"market_open=`{status.market_open}` reason=`{status.market_open_reason or 'ok'}`",
            f"schedule_time=`{_market_schedule_time(cfg)}` summary_time=`{_market_summary_time(cfg) or 'off'}` tz=`{cfg.timezone}`",
        ]
    else:
        lines = [
            "**stockAgent bot health**",
            f"markets=`{', '.join(sorted(configs))}` default=`{_default_market()}`",
        ]
        for key, cfg in sorted(configs.items()):
            lines.append(_status_line(key, cfg, _runtime_status(cfg)))
    await interaction.followup.send("\n".join(lines)[:1900])


@bot.tree.command(name="markets", description="List configured stockAgent markets.")
async def markets(interaction: discord.Interaction) -> None:
    await interaction.response.defer(thinking=True)
    lines = ["**stockAgent markets**"]
    for key, cfg in sorted(_market_configs().items()):
        fold = cfg.fold_id if cfg.fold_id is not None else "latest"
        runtime = _runtime_status(cfg)
        lines.append(
            f"`{key}` {cfg.label} status=`{runtime.status}` enabled=`{runtime.enabled}` "
            f"data=`{runtime.data.last_data_date or 'n/a'}` schedule=`{_market_schedule_time(cfg)}` "
            f"config=`{cfg.config_path}` output=`{cfg.output_dir or 'config default'}` fold=`{fold}`"
        )
    await interaction.followup.send("\n".join(lines)[:1900])


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
    ]
    await interaction.followup.send("\n".join(lines)[:1900])


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


@bot.tree.command(name="daily_summary", description="Show today's market summary.")
@app_commands.describe(market="Market id")
@app_commands.autocomplete(market=market_autocomplete)
async def daily_summary_command(interaction: discord.Interaction, market: str = "") -> None:
    await interaction.response.defer(thinking=True)
    cfg = _resolve_market(market)
    await interaction.followup.send(_daily_summary_message(cfg)[:1900])


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
            result = await _run_signal(**_signal_kwargs(market=market, scheduled=True))
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
            await channel.send(_daily_summary_message(cfg)[:1900])
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


def main() -> None:
    token = _env("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is required")
    bot.run(token)


if __name__ == "__main__":
    main()
