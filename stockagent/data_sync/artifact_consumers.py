"""Read-only service dependency gates shared by artifact storage operations."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from typing import Any, Iterable

import yaml

from stockagent.data_sync.desync_snapshots import SnapshotError


def _strings(value: Any, prefix: str = ""):
    if isinstance(value, (str, os.PathLike)):
        yield prefix, os.fspath(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from _strings(child, f"{prefix}.{key}" if prefix else str(key))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            yield from _strings(child, f"{prefix}[{index}]")


def is_repository_data_path(value: str) -> bool:
    return (Path(value).is_absolute() or value.startswith("artifacts/")
            or (value.startswith("data_") and "/" in value))


def service_artifact_paths(repo_root: Path) -> list[dict[str, str]]:
    """Resolve active Discord and independently enabled simulation consumers.

    Includes model-selection overrides/candidates and resolved experiment data,
    runner and initialization paths. A disabled Discord toggle does not stop an
    overnight engine. Missing or malformed referenced configs fail closed.
    """
    from stockagent.data_sync.node_roles import training_only_node
    if training_only_node():
        return []
    from stockagent.config import load_config
    from stockagent.live.market_config import (
        _apply_model_selection,
        _bool_value,
        _str_tuple,
    )

    repo_root = repo_root.resolve()
    state_path = repo_root / "artifacts/discord_bot/state.json"
    runtime_markets = {}
    if state_path.is_file():
        state = json.loads(state_path.read_text())
        if not isinstance(state, dict) or not isinstance(state.get("markets"), dict):
            raise SnapshotError("Discord runtime market state is invalid")
        runtime_markets = state["markets"]
    paths = []
    enabled_markets = []
    replay_state_roots: set[Path] = set()
    for config_path in sorted(
        (repo_root / "services/discord_bot/markets").glob("*.yaml")
    ):
        raw = yaml.safe_load(config_path.read_text())
        if not isinstance(raw, dict):
            raise SnapshotError(f"invalid service market config: {config_path}")
        market = str(raw.get("market") or raw.get("id") or config_path.stem)
        runtime = runtime_markets.get(market, {})
        if not isinstance(runtime, dict):
            raise SnapshotError(f"invalid Discord runtime state for {market}")
        if not (
            _bool_value(raw.get("enabled"), True)
            or runtime.get("enabled") is True
            or _bool_value(raw.get("overnight_simulation_enabled"), False)
        ):
            continue
        enabled_markets.append(market)
        # Protect both configured and selected assets, including fallback inputs.
        selected = _apply_model_selection(raw, market_config_path=config_path)
        for document in (raw, selected):
            value = document.get("day_trade_simulation_state_dir")
            if value:
                path = Path(str(value))
                replay_state_roots.add(path if path.is_absolute() else repo_root / path)
        documents = [
            (str(config_path.relative_to(repo_root)), raw),
            (f"{market}:model-selection", selected),
        ]
        experiments = {
            item.get("config_path") or item.get("config") for item in (raw, selected)
        } - {None, ""}
        experiments.update(_str_tuple(selected.get("model_candidate_config_paths")))
        for experiment in sorted(experiments):
            documents.append(
                (str(experiment), asdict(load_config(repo_root / experiment)))
            )
        for label, document in documents:
            for field, value in _strings(document):
                if not is_repository_data_path(value):
                    continue
                path = Path(value)
                if not path.is_absolute():
                    path = repo_root / path
                paths.append(
                    {
                        "market": market,
                        "document": label,
                        "field": field,
                        "configured_path": value,
                        "path": str(path.absolute()),
                        "resolved_path": str(path.resolve(strict=False)),
                    }
                )
    if enabled_markets:
        # The live panel disk cache is an implicit runtime dependency, even
        # when no experiment YAML names it. Resolve its canonical default.
        from stockagent.live.signal_engine import _live_panel_disk_cache_root

        path = _live_panel_disk_cache_root(repo_root=repo_root)
        paths.append({"market": "shared-live-panels", "document": "live.signal_engine",
                      "field": "live_panel_disk_cache_root", "configured_path": str(path),
                      "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        # The service supervisor sets this default before launching the web
        # process. A storage command need not inherit that process environment.
        value = os.environ.get("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", "").strip()
        path = Path(value) if value else repo_root / "artifacts/cache/tw_day_trade_dashboard_indexes"
        if not path.is_absolute():
            path = repo_root / path
        paths.append({"market": "shared-day-trade-dashboard", "document": "scripts/run_tw_day_trade_services.sh",
                      "field": "dashboard_index_cache_dir", "configured_path": str(path),
                      "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        # Minute maintenance reads implicit local fallbacks, including historical
        # chunks under data_repair. These are live inputs despite their location.
        from scripts.rebuild_tw_day_trade_minute_curves import (
            DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS,
        )

        for value in (*DEFAULT_LOCAL_MINUTE_ROOTS, *DEFAULT_LOCAL_MINUTE_CACHE_ROOTS):
            path = value if value.is_absolute() else repo_root / value
            paths.append({"market": "shared-day-trade-minute-curves",
                          "document": "scripts/rebuild_tw_day_trade_minute_curves.py",
                          "field": "local_minute_source", "configured_path": str(path),
                          "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        replay_state_roots.add(repo_root / "artifacts/live/tw_day_trade_simulation")
        for state_root in sorted(replay_state_roots):
            receipt_path = state_root / "rebuild_receipt.json"
            if not receipt_path.is_file():
                continue
            receipt = json.loads(receipt_path.read_text())
            if not isinstance(receipt, dict) or not isinstance(receipt.get("sessions", []), list):
                raise SnapshotError(f"invalid execution replay source receipt: {receipt_path}")
            for session in receipt.get("sessions") or ():
                if not isinstance(session, dict):
                    raise SnapshotError(f"invalid execution replay session: {receipt_path}")
                replay = session.get("intraday_replay") or {}
                if not replay:
                    continue
                if not isinstance(replay, dict) or not isinstance(replay.get("source_files"), list) or not replay["source_files"]:
                    raise SnapshotError(f"execution replay source pins are invalid: {receipt_path}")
                for item in replay["source_files"]:
                    value = item.get("path") if isinstance(item, dict) else None
                    digest = str(item.get("sha256") or "") if isinstance(item, dict) else ""
                    if not isinstance(value, str) or not value or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                        raise SnapshotError(f"invalid execution replay source pin: {receipt_path}")
                    path = Path(value)
                    if not path.is_absolute():
                        path = repo_root / path
                    paths.append({"market": "shared-day-trade-execution-replay",
                                  "document": str(receipt_path), "field": "intraday_replay.source_files",
                                  "configured_path": str(path), "path": str(path.absolute()),
                                  "resolved_path": str(path.resolve(strict=False))})
    return paths


def artifact_service_references(
    sources: Iterable[Path], repo_root: Path
) -> dict[str, list[str]]:
    sources = tuple(path.absolute() for path in sources)
    result = {str(path): [] for path in sources}
    source_views = [(source, {"path": str(source), "resolved_path": str(source.resolve(strict=False))})
                    for source in sources]
    seen = set()
    for row in service_artifact_paths(repo_root):
        identity = tuple(row[key] for key in ("document", "field", "configured_path", "path", "resolved_path"))
        if identity in seen:
            continue
        seen.add(identity)
        reference_views = {key: str(Path(row[key])) for key in ("path", "resolved_path")}
        for source, views in source_views:
            for key in ("path", "resolved_path"):
                reference = reference_views[key]
                resolved = views[key]
                if (
                    reference == resolved
                    or resolved.startswith(reference.rstrip("/") + "/")
                    or reference.startswith(resolved.rstrip("/") + "/")
                ):
                    result[str(source)].append(
                        f"{row['document']}:{row['field']}:{row['configured_path']}"
                    )
                    break
    return {path: sorted(set(refs)) for path, refs in result.items()}
