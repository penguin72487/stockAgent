from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

import pytest
import yaml

from stockagent.data_sync.artifact_consumers import (
    artifact_service_references,
    service_artifact_paths,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


def _market(repo: Path, **values) -> Path:
    path = repo / "services/discord_bot/markets/example.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"market": "example", **values}))
    return path


def _refs(repo: Path, name: str) -> list[str]:
    source = repo / "artifacts/markets" / name
    return artifact_service_references((source,), repo)[str(source)]


def test_disabled_discord_does_not_hide_independent_overnight_consumer(tmp_path):
    _market(
        tmp_path,
        enabled=False,
        overnight_simulation_enabled=True,
        output_dir="artifacts/markets/overnight",
    )
    assert _refs(tmp_path, "overnight")
    assert not _refs(tmp_path, "overnight-other")


def test_runtime_enabled_market_is_protected(tmp_path):
    _market(tmp_path, enabled=False, output_dir="artifacts/markets/runtime")
    state = tmp_path / "artifacts/discord_bot/state.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"markets": {"example": {"enabled": True}}}))
    assert _refs(tmp_path, "runtime")
    state.write_text(json.dumps({"markets": {"example": {"enabled": False}}}))
    assert not _refs(tmp_path, "runtime")


def test_market_default_enabled_matches_runtime_loader(tmp_path):
    _market(tmp_path, output_dir="artifacts/markets/default")
    assert _refs(tmp_path, "default")
    _market(tmp_path, enabled="true", output_dir="artifacts/markets/default")
    assert _refs(tmp_path, "default")


def test_model_selection_and_candidates_keep_original_and_selected_assets(tmp_path):
    market = _market(
        tmp_path,
        enabled=True,
        output_dir="artifacts/markets/fallback",
        model_selection_path="../models/example.yaml",
    )
    selection = market.parent.parent / "models/example.yaml"
    selection.parent.mkdir(parents=True)
    selection.write_text(
        yaml.safe_dump(
            {
                "mode": "auto",
                "output_dir": "artifacts/markets/selected",
                "checkpoint_path": "artifacts/markets/weights/fold.pt",
                "model_candidate_output_dirs": ["artifacts/markets/candidate"],
            }
        )
    )
    for name in ("fallback", "selected", "weights", "candidate"):
        assert _refs(tmp_path, name), name
    assert not _refs(tmp_path, "unrelated")


def test_experiment_inputs_and_initialization_are_protected(tmp_path, monkeypatch):
    @dataclass
    class Config:
        data: dict = field(
            default_factory=lambda: {"input": "artifacts/markets/input/data"}
        )
        runner: dict = field(
            default_factory=lambda: {"init_from": "artifacts/markets/init/model.pt"}
        )

    loaded = []

    def load(path):
        loaded.append(path)
        return Config()

    monkeypatch.setattr("stockagent.config.load_config", load)
    _market(
        tmp_path,
        enabled=True,
        config_path="configs/active.yaml",
        model_candidate_config_paths=["configs/candidate.yaml"],
    )
    assert _refs(tmp_path, "input") and _refs(tmp_path, "init")
    assert {path.name for path in loaded} == {"active.yaml", "candidate.yaml"}


def test_managed_alias_and_resolved_target_both_protected(tmp_path):
    target = tmp_path / "materialized/exact-release"
    target.mkdir(parents=True)
    alias = tmp_path / "artifacts/markets/alias"
    alias.parent.mkdir(parents=True)
    alias.symlink_to(target, target_is_directory=True)
    _market(tmp_path, enabled=True, checkpoint_path=str(target / "checkpoint.pt"))
    assert _refs(tmp_path, "alias")
    _market(tmp_path, enabled=True, output_dir="artifacts/markets/alias")
    assert artifact_service_references((target,), tmp_path)[str(target)]


def test_parent_path_keeps_descendants_but_not_siblings(tmp_path):
    _market(tmp_path, enabled=True, output_dir="artifacts/markets/parent")
    assert _refs(tmp_path, "parent/child")
    assert not _refs(tmp_path, "parent-other")


def test_malformed_runtime_or_referenced_config_fails_closed(tmp_path):
    _market(tmp_path, enabled=True, config_path="configs/missing.yaml")
    with pytest.raises(OSError):
        service_artifact_paths(tmp_path)
    state = tmp_path / "artifacts/discord_bot/state.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"markets": []}))
    with pytest.raises(SnapshotError, match="runtime market state"):
        service_artifact_paths(tmp_path)
