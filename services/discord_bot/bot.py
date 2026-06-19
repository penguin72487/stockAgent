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

from stockagent.live.signal_engine import generate_live_signal


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


def _signal_kwargs(
    *,
    price_source: str | None = None,
    top_n: int | None = None,
    min_abs_delta: float | None = None,
) -> dict:
    fold_raw = _env("STOCKAGENT_FOLD_ID", "25")
    return {
        "config_path": _env("STOCKAGENT_CONFIG", "configs/experiment_baseline.yaml"),
        "output_dir": _env("STOCKAGENT_OUTPUT_DIR"),
        "fold_id": int(fold_raw) if fold_raw else None,
        "checkpoint_path": _env("STOCKAGENT_CHECKPOINT"),
        "weights_path": _env("STOCKAGENT_WEIGHTS_PATH"),
        "panel_date": _env("STOCKAGENT_PANEL_DATE", "latest"),
        "price_source": price_source or _env("STOCKAGENT_PRICE_SOURCE", "panel"),
        "prices_csv": _env("STOCKAGENT_PRICES_CSV"),
        "device": _env("STOCKAGENT_DEVICE"),
        "top_n": int(top_n if top_n is not None else _env_int("STOCKAGENT_TOP_N", 20)),
        "min_abs_delta": float(
            min_abs_delta if min_abs_delta is not None else _env_float("STOCKAGENT_MIN_ABS_DELTA", 0.001)
        ),
        "write": True,
    }


class StockAgentBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.tz = ZoneInfo(_env("STOCKAGENT_TZ", "Asia/Taipei") or "Asia/Taipei")
        self.signal_time = _env("STOCKAGENT_SIGNAL_TIME", "13:15") or "13:15"
        self.channel_id = _env_int("DISCORD_CHANNEL_ID")
        self._last_scheduled_date: str | None = None
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
@app_commands.describe(price_source="panel/csv/yahoo", top_n="Rows to show", min_abs_delta="Minimum absolute weight delta")
async def signal_now(
    interaction: discord.Interaction,
    price_source: str = "panel",
    top_n: int = 20,
    min_abs_delta: float = 0.001,
) -> None:
    await interaction.response.defer(thinking=True)
    try:
        result = await _run_signal(**_signal_kwargs(price_source=price_source, top_n=top_n, min_abs_delta=min_abs_delta))
    except Exception as exc:
        await interaction.followup.send(f"live signal failed: `{type(exc).__name__}: {str(exc)[:1500]}`")
        return
    await _send_long_response(interaction, result.message)


@bot.tree.command(name="positions", description="Show target position weights.")
@app_commands.describe(top_n="Rows to show")
async def positions(interaction: discord.Interaction, top_n: int = 20) -> None:
    await interaction.response.defer(thinking=True)
    try:
        result = await _run_signal(**_signal_kwargs(top_n=top_n))
    except Exception as exc:
        await interaction.followup.send(f"positions failed: `{type(exc).__name__}: {str(exc)[:1500]}`")
        return
    rows = result.summary.get("top_positions", [])[:top_n]
    lines = ["**target positions**"]
    for row in rows:
        lines.append(f"{_symbol_label(row)} {float(row['weight']) * 100:.2f}% px={float(row['current_price']):.2f}")
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="rebalance", description="Show rebalance deltas.")
@app_commands.describe(threshold="Minimum absolute weight delta", top_n="Rows to show")
async def rebalance(interaction: discord.Interaction, threshold: float = 0.001, top_n: int = 20) -> None:
    await interaction.response.defer(thinking=True)
    try:
        result = await _run_signal(**_signal_kwargs(top_n=top_n, min_abs_delta=threshold))
    except Exception as exc:
        await interaction.followup.send(f"rebalance failed: `{type(exc).__name__}: {str(exc)[:1500]}`")
        return
    rows = result.summary.get("rebalance", [])[:top_n]
    lines = ["**rebalance**"]
    for row in rows:
        delta = float(row["delta_weight"])
        side = "BUY" if delta > 0 else "SELL"
        lines.append(
            f"{_symbol_label(row)} {side} delta={delta * 100:.2f}% "
            f"now={float(row['current_weight']) * 100:.2f}% target={float(row['target_weight']) * 100:.2f}%"
        )
    await _send_long_response(interaction, "\n".join(lines))


@bot.tree.command(name="health", description="Show bot configuration.")
async def health(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(
        "\n".join(
            [
                "**stockAgent bot health**",
                f"config=`{_env('STOCKAGENT_CONFIG', 'configs/experiment_baseline.yaml')}`",
                f"output_dir=`{_env('STOCKAGENT_OUTPUT_DIR', 'config default')}`",
                f"fold_id=`{_env('STOCKAGENT_FOLD_ID', '25')}`",
                f"price_source=`{_env('STOCKAGENT_PRICE_SOURCE', 'panel')}`",
                f"signal_time=`{bot.signal_time}` tz=`{bot.tz.key}`",
            ]
        )
    )


@tasks.loop(minutes=1)
async def scheduled_signal() -> None:
    if bot.channel_id is None:
        return
    now = datetime.now(bot.tz)
    if now.strftime("%H:%M") != bot.signal_time:
        return
    today = now.strftime("%Y-%m-%d")
    if bot._last_scheduled_date == today:
        return
    bot._last_scheduled_date = today
    channel = bot.get_channel(bot.channel_id) or await bot.fetch_channel(bot.channel_id)
    result = await _run_signal(**_signal_kwargs())
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
