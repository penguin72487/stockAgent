from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
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
from stockagent.live.signal_engine import generate_live_signal


class MarketUnsupportedError(RuntimeError):
    def __init__(self, cfg: LiveMarketConfig) -> None:
        self.cfg = cfg
        super().__init__(_unsupported_message(cfg))


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
) -> dict:
    cfg = _resolve_market(market)
    if not _market_has_model(cfg):
        raise MarketUnsupportedError(cfg)
    overrides = {
        "price_source": price_source if price_source and price_source != "auto" else None,
        "top_n": top_n,
        "min_abs_delta": min_abs_delta,
    }
    return cfg.signal_kwargs(**overrides)


async def _send_command_error(interaction: discord.Interaction, prefix: str, exc: Exception) -> None:
    if isinstance(exc, MarketUnsupportedError):
        await interaction.followup.send(str(exc))
        return
    await interaction.followup.send(f"{prefix} failed: `{type(exc).__name__}: {str(exc)[:1500]}`")


class StockAgentBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.tz = ZoneInfo(_env("STOCKAGENT_TZ", "Asia/Taipei") or "Asia/Taipei")
        self.signal_time = _env("STOCKAGENT_SIGNAL_TIME", "13:15") or "13:15"
        self.channel_id = _env_int("DISCORD_CHANNEL_ID")
        self._last_scheduled_keys: set[str] = set()
        self._synced_guild_id: int | None = None

    async def setup_hook(self) -> None:
        await self.tree.sync()
        scheduled_signal.start()

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


def _symbol_label(row: dict) -> str:
    symbol = str(row.get("symbol", "") or "").strip()
    name = str(row.get("name", "") or "").strip()
    if name:
        return f"`{symbol}` {name}"
    return f"`{symbol}`"


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
    await _send_long_response(interaction, result.message)


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
        result = await _run_signal(**_signal_kwargs(market=market, top_n=top_n, min_abs_delta=threshold))
    except Exception as exc:
        await _send_command_error(interaction, "rebalance", exc)
        return
    rows = result.summary.get("rebalance", [])[:top_n]
    lines = ["**rebalance**"]
    for row in rows:
        delta = float(row["delta_weight"])
        side = "BUY" if delta > 0 else "SELL"
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
    configs = _market_configs()
    cfg = _resolve_market(market)
    checkpoint = _market_model_checkpoint(cfg)
    await interaction.response.send_message(
        "\n".join(
            [
                "**stockAgent bot health**",
                f"markets=`{', '.join(sorted(configs))}` default=`{_default_market()}`",
                f"active=`{cfg.market}` label=`{cfg.label}`",
                f"model=`{'ready' if checkpoint is not None else '目前不支援'}`",
                f"checkpoint=`{checkpoint if checkpoint is not None else 'none'}`",
                f"config=`{cfg.config_path}`",
                f"output_dir=`{cfg.output_dir or 'config default'}`",
                f"live_output_dir=`{cfg.live_output_dir or 'auto'}`",
                f"fold_id=`{cfg.fold_id if cfg.fold_id is not None else 'latest'}`",
                f"price_source=`{cfg.price_source}`",
                f"signal_time=`{bot.signal_time}` tz=`{bot.tz.key}`",
            ]
        )
    )


@bot.tree.command(name="markets", description="List configured stockAgent markets.")
async def markets(interaction: discord.Interaction) -> None:
    lines = ["**stockAgent markets**"]
    for key, cfg in sorted(_market_configs().items()):
        fold = cfg.fold_id if cfg.fold_id is not None else "latest"
        status = "ready" if _market_has_model(cfg) else "目前不支援"
        lines.append(
            f"`{key}` {cfg.label} model=`{status}` "
            f"config=`{cfg.config_path}` output=`{cfg.output_dir or 'config default'}` fold=`{fold}`"
        )
    await interaction.response.send_message("\n".join(lines))


@tasks.loop(minutes=1)
async def scheduled_signal() -> None:
    if bot.channel_id is None:
        return
    now = datetime.now(bot.tz)
    if now.strftime("%H:%M") != bot.signal_time:
        return
    today = now.strftime("%Y-%m-%d")
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    for market in _scheduled_markets():
        key = f"{today}:{market}"
        if key in bot._last_scheduled_keys:
            continue
        bot._last_scheduled_keys.add(key)
        try:
            result = await _run_signal(**_signal_kwargs(market=market))
        except MarketUnsupportedError as exc:
            await channel.send(str(exc))
            continue
        await channel.send(result.message)


@scheduled_signal.before_loop
async def before_scheduled_signal() -> None:
    await bot.wait_until_ready()


def main() -> None:
    token = _env("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is required")
    bot.run(token)


if __name__ == "__main__":
    main()
