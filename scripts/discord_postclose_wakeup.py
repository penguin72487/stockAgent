"""CPU-only advisory wakeups for the existing completed-close cache owner.

File generations decide whether to wake a worker, NEVER whether a signal is
usable. The worker and every consumer retain the canonical source/hash gates.
No network, Torch import, source mutation or model execution belongs here.
"""
from __future__ import annotations

from dataclasses import asdict
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path

CONTRACT = "postclose_cache_advisory_wakeup_v1"
OWNER_UNIT = "stockagent-discord-postclose-cache.service"
# Ten-minute start budget, 90-second stop grace and controller/queue headroom.
RUNWAY_MINUTES = 13
MAX_RECEIPT_BYTES = 16 * 1024 * 1024


def absolute(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def read_json(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_RECEIPT_BYTES + 1)
    except FileNotFoundError:
        return {}
    if len(raw) > MAX_RECEIPT_BYTES:
        raise ValueError("postclose wakeup receipt exceeds bounded reader")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("postclose wakeup receipt must be an object")
    return value


def accepted_path(root: Path) -> Path:
    return absolute(root, os.environ.get("STOCKAGENT_POSTCLOSE_WAKEUP_STATUS_PATH") or
                    "artifacts/discord_bot/postclose_cache_wakeup_latest.json")


def lock_held(path: Path, *, producer_only: bool = False) -> bool:
    """Inspect a real kernel lock; never create locks or trust a PID timestamp."""
    try:
        with path.open("rb") as handle:
            try:
                mode = fcntl.LOCK_SH if producer_only else fcntl.LOCK_EX
                fcntl.flock(handle, mode | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle, fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    return False


def _generations(paths: list[str]) -> list:
    members = []
    for text in paths:
        path = Path(text)
        try:
            stat = path.stat()
        except FileNotFoundError:
            generation = None
        else:
            generation = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]
        members.append((str(path), generation))
    return members


def _environment() -> dict:
    return {key: os.environ.get(key, "") for key in (
        "STOCKAGENT_MARKETS_DIR", "STOCKAGENT_SCHEDULED_MARKETS", "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT")}


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     default=str).encode()).hexdigest()


def artifacts_unchanged(accepted: dict) -> bool:
    rows = accepted.get("artifact_generations")
    return (isinstance(rows, list) and 0 < len(rows) <= 1000
            and all(isinstance(row, (list, tuple)) and len(row) == 2
                    and row[1] is not None and isinstance(row[0], str)
                    and Path(row[0]).is_absolute() for row in rows)
            and _digest(rows) == _digest(_generations([row[0] for row in rows])))


def _manifest_paths(value, parent: Path):
    # An advisory superset, not factorized schema/integrity validation. Native
    # readers remain authoritative for allowed members and their payload hashes.
    if isinstance(value, dict):
        if isinstance(value.get("path"), str):
            yield absolute(parent, value["path"])
        for child in value.values():
            yield from _manifest_paths(child, parent)
    elif isinstance(value, list):
        for child in value:
            yield from _manifest_paths(child, parent)


def observe(root: Path, *, previous: dict | list[dict] | None = None) -> dict:
    """Observe configured TW close dependencies, including in-place stock writes.

    This deliberately over-invalidates on implementation/config changes. It is
    not a second source acceptance implementation. Factorized publication is
    hinted by its committed manifest; payload integrity stays with its reader.
    """
    # An unchanged, worker-generated observation plan requires only bounded
    # metadata reads. Directory generations fence added/deleted dependencies;
    # every listed file is still stat'ed, including in-place source repairs.
    # This is an idle-wakeup optimization, NEVER a model/source acceptance cache.
    for candidate in previous if isinstance(previous, list) else [previous]:
        if not candidate or candidate.get("contract") != CONTRACT:
            continue
        plan = candidate.get("observation_plan", {})
        paths = plan.get("paths")
        if (plan.get("root") == str(root) and plan.get("environment") == _environment()
                and isinstance(paths, list) and 0 < len(paths) <= 20000
                and all(isinstance(path, str) and Path(path).is_absolute() for path in paths)):
            current = _digest({"plan": plan, "members": _generations(paths)})
            if current == candidate.get("fingerprint"):
                return {key: candidate[key] for key in (
                    "contract", "advisory_only", "markets", "fingerprint", "member_count",
                    "reason", "observation_plan")}
    from stockagent.config import external_panel_data_kwargs, load_config
    from stockagent.live.market_config import load_market_configs
    completed = absolute(root, os.environ.get("STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT") or
                         "artifacts/data_refresh/tw_public/completed_session/latest.json")
    receipt = read_json(completed)
    result = {"contract": CONTRACT, "advisory_only": True, "markets": [],
              "fingerprint": None, "member_count": 0}
    if receipt.get("status") != "ok" or not receipt.get("after", {}).get("current"):
        return {**result, "reason": "accepted_close_receipt_required"}
    directory = absolute(root, os.environ.get("STOCKAGENT_MARKETS_DIR") or
                         "services/discord_bot/markets")
    configs = load_market_configs(directory)
    overrides = read_json(root / "artifacts/discord_bot/state.json").get("markets", {})
    if not isinstance(overrides, dict):
        raise ValueError("market-state overrides must be an object")
    selection = {item.strip() for item in os.environ.get("STOCKAGENT_SCHEDULED_MARKETS", "").split(",")
                 if item.strip()}
    all_markets = not selection or any(item.lower() in {"all", "*"} for item in selection)
    paths = {completed, root / "artifacts/discord_bot/state.json", directory, Path(__file__),
             root / "scripts/run_discord_artifact_maintenance.py",
             root / "services/discord_bot/bot.py", root / "stockagent/config.py",
             root / "stockagent/live/signal_engine.py",
             root / "stockagent/live/completed_inference_cache.py",
             *(root / "stockagent/training" / name for name in (
                 "runtime.py", "runtime_configuration.py", "inference_contract.py"))}
    # Same complete model/data code scope as the inference implementation fence;
    # stat hints only, no repeated payload hashing or loading model dependencies.
    for scope in ("stockagent/models", "stockagent/data"):
        code_paths = set((root / scope).rglob("*.py"))
        paths.update(code_paths)
        paths.update(path.parent for path in code_paths)
        paths.add(root / scope)
    paths.update(directory.glob("*.yaml"))
    paths.update(directory.glob("*.yml"))
    resolved = {}
    for name, cfg in sorted(configs.items()):
        override = overrides.get(name, {})
        enabled = override.get("enabled", cfg.enabled) if isinstance(override, dict) else cfg.enabled
        daily_tw = cfg.day_trade_simulation_enabled or (
            (cfg.market_type or "").lower() in {"tw", "taiwan"}
            and (cfg.history_frequency or "daily").lower() in {"daily", "day", "1d"})
        if not enabled or not daily_tw or not (all_markets or name in selection):
            continue
        dependencies = {}
        config = load_config(absolute(root, cfg.config_path), source_signatures=dependencies)
        paths.update(dependencies)
        resolved[name] = {"market": asdict(cfg), "inference": asdict(config)}
        result["markets"].append(name)
        for field in ("checkpoint_path", "weights_path", "model_deployment_manifest", "config_path"):
            if value := getattr(cfg, field):
                paths.add(absolute(root, value))
        if cfg.model_selection_path:
            paths.add(absolute(directory, cfg.model_selection_path))
        data_root = absolute(root, config.data.parquet_root).resolve()
        paths.update((data_root, data_root / "symbols.csv", data_root / "_hot_tail"))
        paths.update(data_root.glob("*_features.parquet"))
        paths.update((data_root / "_hot_tail").glob("*_features.parquet"))
        rule_root = absolute(root, cfg.day_trade_rule_data_dir) if cfg.day_trade_rule_data_dir else data_root.parent
        paths.update(rule_root / f"{venue}_day_trade_eligibility.parquet" for venue in ("twse", "tpex"))
        if external := external_panel_data_kwargs(config.data).get("external_feature_path"):
            target = absolute(root, external).resolve()
            paths.update((target, target.with_suffix(".summary.json")))
            for directory_root in (target.parent, target.parent.parent):
                for stem in ("tw_corporate_action_reference", "tw_corporate_action_entitlements"):
                    paths.update((directory_root / f"{stem}.parquet", directory_root / f"{stem}.summary.json"))
        if manifest := config.data.factorized_feature_manifest:
            link = absolute(root, manifest)
            target = link.resolve()
            paths.update((link, target, link.parent, target.parent))
            paths.update(_manifest_paths(read_json(target), target.parent))
    if not result["markets"]:
        return {**result, "reason": "no_enabled_tw_daily_markets"}
    if len(paths) > 20000:
        raise ValueError("postclose wakeup exceeds bounded dependency plan")
    plan = {"paths": [str(path) for path in sorted(paths)], "root": str(root),
            "environment": _environment(), "resolved_digest": _digest(resolved)}
    members = _generations(plan["paths"])
    result.update(fingerprint=_digest({"plan": plan, "members": members}), member_count=len(members),
                  reason="observed_close_dependencies", observation_plan=plan)
    return result


def acknowledge(root: Path, before: dict, bot) -> bool:
    """Only the canonical worker can acknowledge strictly reusable artifacts.

    A deferred/no-op exit is not success. Verify all summaries and weights with
    canonical readers, then fence the advisory generation across that readback.
    Consumers still independently validate source generations on every reuse.
    """
    if not before.get("fingerprint") or not before.get("markets"):
        return False
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

    artifacts = set()
    with ExitStack() as guards:
        for market in before["markets"]:
            cfg = bot._effective_market_config(bot._resolve_market(market))
            config = bot.load_config(cfg.config_path)
            guards.enter_context(live_tw_public_source_read_guard(config.data.parquet_root))
            status = bot._ensure_signal_ready(cfg)
            if (not bot._completed_session_signal_path(cfg, status)
                    or not bot._completed_session_receipt_ready(status) or not status.data.fresh):
                return False
            latest = bot._latest_market_signal(cfg)
            if latest is None:
                return False
            path, summary = latest
            reusable, _reason = bot._can_reuse_latest_signal_now(cfg, status, summary,
                                                               requested_price_source="panel")
            weights = bot._summary_artifact_path(summary, "weights_path", path)
            if not reusable or summary.get("postclose_fast_preview") or weights is None or not weights.is_file():
                return False
            artifacts.update((path, weights))
            if live_root := getattr(cfg, "live_output_dir", None):
                pointer = absolute(root, live_root) / "latest_signal.json"
                if pointer.is_file():
                    artifacts.add(pointer)
        after = observe(root)
        if after != before:
            return False
        from downloader.artifact_io import atomic_write_json

        atomic_write_json(accepted_path(root), {**after, "status": "canonical_artifacts_reusable",
                          "worker_verified_at": datetime.now(timezone.utc).isoformat(),
                          "artifact_generations": _generations([str(path) for path in sorted(artifacts)]),
                          "owner_unit": OWNER_UNIT})
    return True
