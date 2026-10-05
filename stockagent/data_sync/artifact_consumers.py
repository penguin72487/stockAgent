"""Read-only service dependency gates shared by artifact storage operations."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Any, Iterable

import yaml

from stockagent.data_sync.desync_snapshots import SnapshotError


def _strings(value: Any, prefix: str = ""):
    if isinstance(value, str):
        yield prefix, value
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from _strings(child, f"{prefix}.{key}" if prefix else str(key))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            yield from _strings(child, f"{prefix}[{index}]")


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
        # Protect both configured and selected assets, including fallback inputs.
        selected = _apply_model_selection(raw, market_config_path=config_path)
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
                if not (value.startswith("artifacts/") or Path(value).is_absolute()):
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
    return paths


def artifact_service_references(
    sources: Iterable[Path], repo_root: Path
) -> dict[str, list[str]]:
    sources = tuple(path.absolute() for path in sources)
    result = {str(path): [] for path in sources}
    for row in service_artifact_paths(repo_root):
        for source in sources:
            for key in ("path", "resolved_path"):
                reference = Path(row[key])
                resolved = (
                    source.resolve(strict=False) if key == "resolved_path" else source
                )
                if (
                    reference == resolved
                    or reference in resolved.parents
                    or resolved in reference.parents
                ):
                    result[str(source)].append(
                        f"{row['document']}:{row['field']}:{row['configured_path']}"
                    )
                    break
    return {path: sorted(set(refs)) for path, refs in result.items()}
