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


def test_live_signal_panel_default_is_protected_without_a_configured_cache_path(tmp_path, monkeypatch):
    monkeypatch.delenv("STOCKAGENT_LIVE_PANEL_CACHE_ROOT", raising=False)
    _market(tmp_path, enabled=True, output_dir="artifacts/markets/live-model")
    root = tmp_path / "artifacts/cache/live_signal_panels"
    refs = artifact_service_references((root,), tmp_path)[str(root)]
    assert any("live_panel_disk_cache_root" in value for value in refs)
    _market(tmp_path, enabled=False, output_dir="artifacts/markets/live-model")
    assert artifact_service_references((root,), tmp_path)[str(root)] == []


def test_live_signal_panel_environment_override_is_protected(tmp_path, monkeypatch):
    root = tmp_path / "private-live-cache"
    monkeypatch.setenv("STOCKAGENT_LIVE_PANEL_CACHE_ROOT", str(root))
    _market(tmp_path, enabled=True)
    assert artifact_service_references((root,), tmp_path)[str(root)]


def test_dashboard_supervisor_default_cache_is_protected_without_inherited_env(tmp_path, monkeypatch):
    monkeypatch.delenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", raising=False)
    _market(tmp_path, enabled=True)
    root = tmp_path / "artifacts/cache/tw_day_trade_dashboard_indexes"
    assert any("dashboard_index_cache_dir" in ref
               for ref in artifact_service_references((root,), tmp_path)[str(root)])
    _market(tmp_path, enabled=False)
    assert artifact_service_references((root,), tmp_path)[str(root)] == []


def test_dashboard_cache_override_resolves_against_repository(tmp_path, monkeypatch):
    _market(tmp_path, enabled=True)
    root = tmp_path / "private-dashboard-cache"
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", "private-dashboard-cache")
    assert artifact_service_references((root,), tmp_path)[str(root)]
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(root))
    assert artifact_service_references((root,), tmp_path)[str(root)]


def test_minute_maintenance_implicit_sources_are_protected(tmp_path):
    _market(tmp_path, enabled=True)
    root = tmp_path / "artifacts/data_repair/tw_day_trade_minute_curve"
    assert any("local_minute_source" in ref
               for ref in artifact_service_references((root,), tmp_path)[str(root)])
    sibling = root.with_name("tw_day_trade_minute_curve_old_unused")
    assert artifact_service_references((sibling,), tmp_path)[str(sibling)] == []


def test_service_protects_relative_data_view_input_paths(tmp_path):
    _market(tmp_path, enabled=True, input_root="data_tw_minute/research_dataset_developing_v5")
    root = tmp_path / "data_tw_minute/research_dataset_developing_v5"
    assert artifact_service_references((root,), tmp_path)[str(root)]
    sibling = root.with_name("research_dataset_schema2_volume_bug_20260807")
    assert artifact_service_references((sibling,), tmp_path)[str(sibling)] == []


def test_replay_pins_protect_nondefault_sources_even_when_temporarily_missing(tmp_path):
    state = tmp_path / "private-replay"
    _market(tmp_path, enabled=False, overnight_simulation_enabled=True,
            day_trade_simulation_state_dir=str(state))
    state.mkdir()
    root = tmp_path / "artifacts/data_repair/nondefault-originals"
    receipt = state / "rebuild_receipt.json"
    receipt.write_text(json.dumps({"sessions": [{"session_date": "2026-02-25",
        "intraday_replay": {"source_files": [{"path": str(root / "minute.parquet"), "sha256": "a" * 64}]}}]}))
    assert any("intraday_replay.source_files" in ref
               for ref in artifact_service_references((root,), tmp_path)[str(root)])
    receipt.write_text(json.dumps({"sessions": [{"intraday_replay": {"source_files": []}}]}))
    with pytest.raises(SnapshotError, match="source pins"):
        artifact_service_references((root,), tmp_path)


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


def test_resolved_experiment_path_objects_are_protected(tmp_path, monkeypatch):
    @dataclass
    class Config:
        data: dict = field(default_factory=lambda: {
            "parquet_root": Path("data_tw_minute/research_dataset_developing_v5")})
    monkeypatch.setattr("stockagent.config.load_config", lambda path: Config())
    _market(tmp_path, enabled=True, config_path="configs/active.yaml")
    root = tmp_path / "data_tw_minute/research_dataset_developing_v5"
    assert artifact_service_references((root,), tmp_path)[str(root)]


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
