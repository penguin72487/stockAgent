from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PRODUCTIVE_STATUSES = {
    "updated",
    "repaired",
    "new_symbol_repaired",
    "schema_repaired",
    "current",
    "unchanged",
    "not_found",
    "not_found_skip",
    "delisted_skip",
    "delisted_no_history",
    "delisted_removed",
    "lagging_skip",
}

MARKET_ASSET_ALIASES = {
    "tw": "tw_stocks",
    "taiwan": "tw_stocks",
    "us": "us_stocks",
    "usa": "us_stocks",
    "fx": "forex",
}

SUMMARY_NAME_BY_MODE = {
    "download": "download_summary.json",
    "repair": "repair_summary.json",
    "incremental": "incremental_update_summary.json",
    "daily-update": "daily_update_summary.json",
}


def download_counts_failure_reason(counts: dict[str, Any]) -> str | None:
    failed = int(counts.get("failed", 0) or 0)
    if failed <= 0:
        return None
    productive = sum(int(counts.get(status, 0) or 0) for status in PRODUCTIVE_STATUSES)
    if productive <= 0:
        return f"{failed} failed, 0 productive"
    if failed > productive:
        return f"{failed} failed > {productive} productive"
    return None


def command_option(command: list[str], option: str) -> str | None:
    for idx, item in enumerate(command):
        if item == option and idx + 1 < len(command):
            value = str(command[idx + 1]).strip()
            return value or None
    return None


def command_asset(command: list[str]) -> str | None:
    asset = command_option(command, "--asset")
    if asset:
        return asset
    if any(str(item).endswith("download_alpaca_us_ohlcv.py") for item in command):
        return "us_stocks"
    return None


def command_summary_paths(command: list[str], *, resolve_path=None) -> list[Path]:
    def resolve(raw: str) -> Path:
        if resolve_path is None:
            return Path(raw)
        resolved = resolve_path(raw)
        return resolved if resolved is not None else Path(raw)

    output_root_raw = command_option(command, "--output-root")
    output_dir_raw = command_option(command, "--output-dir")
    mode = str(command_option(command, "--mode") or "").strip().lower()
    asset = command_asset(command)
    summary_name = SUMMARY_NAME_BY_MODE.get(mode, "download_summary.json")

    paths: list[Path] = []
    output_dir = resolve(output_dir_raw) if output_dir_raw else None
    output_root = resolve(output_root_raw) if output_root_raw else None
    if output_dir is not None:
        paths.append(output_dir / "download_summary.json")
    if output_root is not None and asset:
        paths.append(output_root / asset / "download_summary.json")
    if output_root is not None:
        paths.append(output_root / summary_name)
    return paths


def market_asset_keys(*, market: str | None, market_type: str | None, command: list[str]) -> set[str]:
    keys = {str(market or "").strip(), str(market_type or "").strip()}
    asset = command_asset(command)
    if asset:
        keys.add(asset)
    for key in list(keys):
        alias = MARKET_ASSET_ALIASES.get(key)
        if alias:
            keys.add(alias)
    return {key for key in keys if key}


def extract_counts_for_asset(payload: dict[str, Any], asset_keys: set[str]) -> dict[str, Any] | None:
    if "status_counts" in payload and isinstance(payload.get("status_counts"), dict):
        return payload.get("status_counts")
    for key in asset_keys:
        if key in payload and isinstance(payload.get(key), dict):
            return payload.get(key)
    if len(payload) == 1 and all(isinstance(value, dict) for value in payload.values()):
        only_key = str(next(iter(payload.keys())))
        if only_key in asset_keys:
            return next(iter(payload.values()))
    return None


def read_summary_payloads(paths: list[Path]) -> list[tuple[Path, dict[str, Any]]]:
    summaries: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            summaries.append((path, payload))
    return summaries


def first_download_failure(
    *,
    command: list[str],
    market: str | None = None,
    market_type: str | None = None,
    resolve_path=None,
) -> tuple[Path, str] | None:
    asset_keys = market_asset_keys(market=market, market_type=market_type, command=command)
    for path, payload in read_summary_payloads(command_summary_paths(command, resolve_path=resolve_path)):
        counts = extract_counts_for_asset(payload, asset_keys)
        if not isinstance(counts, dict):
            continue
        reason = download_counts_failure_reason(counts)
        if reason:
            return path, reason
    return None
