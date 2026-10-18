from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from stockagent.data_sync.artifact_consumers import (
    artifact_service_references,
    completed_forward_consumer_pins,
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


def test_completed_forward_default_is_an_implicit_service_dependency(tmp_path, monkeypatch):
    monkeypatch.delenv("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT", raising=False)
    config = _market(tmp_path, enabled=True)
    root = tmp_path / "artifacts/cache/completed_live_inference"
    refs = artifact_service_references((root,), tmp_path)[str(root)]
    assert any("completed_inference_cache_root" in item for item in refs)
    config.write_text("market: example\nenabled: false\n")
    assert not artifact_service_references((root,), tmp_path)[str(root)]


@pytest.mark.parametrize("configured", ["cache/relative", "/srv/test-forward", "~/forward", "   "])
def test_completed_forward_dependency_preserves_original_writer_path_semantics(
    tmp_path, monkeypatch, configured,
):
    # Unlike the panel resolver, this writer deliberately does not strip or
    # expand its explicit override. Protect the actual path, not a nicer alias.
    monkeypatch.setenv("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT", configured)
    _market(tmp_path, enabled=False, overnight_simulation_enabled=True)
    root = Path(configured)
    if not root.is_absolute():
        root = tmp_path / root
    refs = artifact_service_references((root,), tmp_path)[str(root)]
    assert any("completed_inference_cache_root" in item for item in refs)


@pytest.mark.parametrize("configured", [None, "", "   ", "cache/relative", "/srv/test-panel", "~/panels"])
def test_panel_cache_resolver_preserves_writer_path_semantics(tmp_path, monkeypatch, configured):
    from stockagent.live.market_config import live_panel_disk_cache_root

    working = tmp_path / "worker"
    working.mkdir()
    monkeypatch.chdir(working)
    if configured is None:
        monkeypatch.delenv("STOCKAGENT_LIVE_PANEL_CACHE_ROOT", raising=False)
    else:
        monkeypatch.setenv("STOCKAGENT_LIVE_PANEL_CACHE_ROOT", configured)
    value = str(configured or "").strip()
    expected = (Path(value).expanduser() if value else
                tmp_path / "artifacts/cache/live_signal_panels")
    if not expected.is_absolute():
        expected = working / expected
    assert live_panel_disk_cache_root(repo_root=tmp_path) == expected


def test_complete_service_paths_do_not_import_models_or_providers(tmp_path):
    _market(tmp_path, enabled=True, output_dir="artifacts/markets/live-model")
    code = """
import sys
from pathlib import Path
from scripts.benchmark_artifact_consumer_discovery import MetadataOnlyImports
sys.meta_path.insert(0, MetadataOnlyImports({'scripts.rebuild_tw_day_trade_minute_curves',
    'scripts.rebuild_tw_day_trade_open_price_replay', 'stockagent.live.tw_day_trade_simulation'}))
from stockagent.data_sync.artifact_consumers import service_artifact_paths
paths = service_artifact_paths(Path(sys.argv[1]))
assert any(row['field'] == 'live_panel_disk_cache_root' for row in paths)
assert not any(name in sys.modules for name in ('torch', 'cupy', 'shioaji',
    'scripts.rebuild_tw_day_trade_minute_curves', 'scripts.rebuild_tw_day_trade_open_price_replay',
    'stockagent.live.tw_day_trade_simulation'))
"""
    subprocess.run([sys.executable, "-c", code, str(tmp_path)], check=True, timeout=30)


def test_metadata_import_guard_allows_availability_probe_but_not_model_import():
    code = """
import sys
from importlib.util import find_spec
from scripts.benchmark_artifact_consumer_discovery import MetadataOnlyImports
sys.meta_path.insert(0, MetadataOnlyImports())
assert find_spec('torch') is not None
assert 'torch' not in sys.modules
try:
    import torch
except AssertionError:
    pass
else:
    raise AssertionError('the compute guard allowed Torch to load')
assert 'torch' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)


def test_minute_declarations_alone_import_no_compute_data_or_provider_modules():
    code = """
import sys
from scripts.benchmark_artifact_consumer_discovery import MetadataOnlyImports
sys.meta_path.insert(0, MetadataOnlyImports({'numpy', 'polars', 'pandas'}))
from stockagent.live.minute_source_paths import DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS
assert len(DEFAULT_LOCAL_MINUTE_ROOTS) == 4 and len(DEFAULT_LOCAL_MINUTE_CACHE_ROOTS) == 1
assert not any(name in sys.modules for name in ('torch', 'cupy', 'shioaji', 'numpy', 'polars', 'pandas'))
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=30)


def _minute_path_reference_fixture(repo):
    from scripts import benchmark_artifact_consumer_discovery as benchmark

    curves = benchmark.MINUTE_REFERENCE_FILES
    declarations = (
        'DEFAULT_LOCAL_MINUTE_ROOTS = (Path("source"),)\n'
        'DEFAULT_LOCAL_MINUTE_CACHE_ROOTS = (Path("cache"),)\n'
    )
    values = {
        "scripts/rebuild_tw_day_trade_minute_curves.py":
            'from pathlib import Path\n' + declarations + 'def compute():\n    return 42\n',
        "scripts/rebuild_tw_day_trade_open_price_replay.py":
            'from pathlib import Path\nDEFAULT_MINUTE_DATA_ROOTS = (Path("source"),)\n'
            'def compute():\n    return 42\n',
        "stockagent/data_sync/artifact_consumers.py":
            'def paths():\n    from scripts.rebuild_tw_day_trade_minute_curves import '
            'DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS\n'
            '    return DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS\n',
        "scripts/recheck_tw_day_trade_0901_local_price_gaps.py":
            'def paths():\n    from scripts.rebuild_tw_day_trade_open_price_replay import DEFAULT_MINUTE_DATA_ROOTS\n'
            '    return DEFAULT_MINUTE_DATA_ROOTS\n',
    }
    assert set(values) == set(curves)
    for name, source in values.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return values, declarations


@pytest.mark.parametrize("defect", [None, "curve", "replay", "consumer", "recheck",
                                    "declaration", "extra_code", "reference"])
def test_minute_declaration_full_ast_proof_rejects_any_unrelated_change(tmp_path, monkeypatch, defect):
    from scripts import benchmark_artifact_consumer_discovery as benchmark

    values, declarations = _minute_path_reference_fixture(tmp_path)
    monkeypatch.setattr(benchmark, "ROOT", tmp_path)
    reference = tmp_path / "reference"
    result = benchmark.capture_minute_reference(reference)
    assert result["source_files"] == 4
    with pytest.raises(FileExistsError):
        benchmark.capture_minute_reference(reference)
    module = "stockagent.live.minute_source_paths"
    for name, source in values.items():
        if name.endswith("minute_curves.py"):
            source = source.replace(declarations, f"from {module} import DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS\n")
        elif name.endswith("open_price_replay.py"):
            source = source.replace('DEFAULT_MINUTE_DATA_ROOTS = (Path("source"),)',
                                    f"from {module} import DEFAULT_LOCAL_MINUTE_ROOTS as DEFAULT_MINUTE_DATA_ROOTS")
        elif name.endswith("artifact_consumers.py"):
            source = source.replace("scripts.rebuild_tw_day_trade_minute_curves", module)
        else:
            source = source.replace("scripts.rebuild_tw_day_trade_open_price_replay import DEFAULT_MINUTE_DATA_ROOTS",
                                    f"{module} import DEFAULT_LOCAL_MINUTE_ROOTS as DEFAULT_MINUTE_DATA_ROOTS")
        (tmp_path / name).write_text(source)
    shared = tmp_path / "stockagent/live/minute_source_paths.py"
    shared.parent.mkdir(parents=True)
    shared.write_text('"""Pure declarations."""\nfrom __future__ import annotations\nfrom pathlib import Path\n' + declarations)
    targets = {"curve": "scripts/rebuild_tw_day_trade_minute_curves.py",
               "replay": "scripts/rebuild_tw_day_trade_open_price_replay.py",
               "consumer": "stockagent/data_sync/artifact_consumers.py",
               "recheck": "scripts/recheck_tw_day_trade_0901_local_price_gaps.py"}
    if defect in targets:
        target = tmp_path / targets[defect]
        target.write_text(target.read_text() + "\nUNRELATED_CHANGE = True\n")
    elif defect == "declaration":
        shared.write_text(shared.read_text().replace('Path("source")', 'Path("foreign")'))
    elif defect == "extra_code":
        shared.write_text(shared.read_text() + "\nimport socket\n")
    elif defect == "reference":
        target = reference / "original_minute_curves.py"
        target.write_text(target.read_text() + "\nUNRELATED_CHANGE = True\n")
    if defect is None:
        benchmark.verify_minute_path_relocation(reference)
    else:
        with pytest.raises(ValueError):
            benchmark.verify_minute_path_relocation(reference)


def test_live_writer_compatibility_name_uses_the_same_canonical_resolver(monkeypatch):
    from stockagent.live import market_config, signal_engine

    monkeypatch.delenv("STOCKAGENT_LIVE_PANEL_CACHE_ROOT", raising=False)
    assert signal_engine._live_panel_disk_cache_root is market_config.live_panel_disk_cache_root
    assert market_config.live_panel_disk_cache_root() == (
        Path(market_config.__file__).resolve().parents[2] / "artifacts/cache/live_signal_panels"
    )


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


def _empty_minute_replay():
    return {
        "contract": "historical_1m_ohlcv_counterfactual_not_executable_bid_ask",
        "historical_best_bid_ask_claimed": False,
        "requested_symbols": 0, "resolved_symbols": 0,
        "ignored_zero_volume_rows": 0, "ignored_unverified_volume_rows": 0,
        "missing_symbols": [], "official_no_trade_carried_symbols": [],
        "source_counts": {}, "source_files": [],
    }


@pytest.mark.parametrize("contract", [
    "historical_1m_ohlcv_counterfactual_not_executable_bid_ask",
    "historical_1m_ohlcv_full_target_counterfactual_no_liquidity_claim_v1",
])
def test_explicit_zero_request_replay_keeps_other_sessions_and_implicit_inputs_protected(tmp_path, contract):
    _market(tmp_path, enabled=True)
    state = tmp_path / "artifacts/live/tw_day_trade_simulation"
    state.mkdir(parents=True)
    replay = _empty_minute_replay()
    replay["contract"] = contract
    pin = tmp_path / "data_tw_minute/unique-original"
    (state / "rebuild_receipt.json").write_text(json.dumps({"sessions": [
        {"intraday_replay": replay},
        {"intraday_replay": {"source_files": [{"path": str(pin), "sha256": "a"*64}]}},
    ]}))
    fallback = tmp_path / "artifacts/data_repair/tw_day_trade_minute_curve"
    unused = tmp_path / "unrelated-unused"
    refs = artifact_service_references((pin, fallback, unused), tmp_path)
    assert refs[str(pin)] and refs[str(fallback)] and not refs[str(unused)]


@pytest.mark.parametrize("field,value", [
    ("requested_symbols", 1), ("resolved_symbols", 1),
    ("requested_symbols", False), ("requested_symbols", 0.0),
    ("requested_symbols", "0"), ("requested_symbols", None),
    ("resolved_symbols", -1), ("contract", "unknown"), ("contract", []), ("contract", {}),
    ("historical_best_bid_ask_claimed", True),
    ("ignored_zero_volume_rows", 1), ("ignored_unverified_volume_rows", 1),
    ("missing_symbols", ["2330"]), ("official_no_trade_carried_symbols", ["2330"]),
    ("source_counts", {"minute": 1}), ("source_files", None),
])
def test_empty_source_pins_without_complete_zero_work_evidence_fail_closed(tmp_path, field, value):
    _market(tmp_path, enabled=True)
    state = tmp_path / "artifacts/live/tw_day_trade_simulation"
    state.mkdir(parents=True)
    replay = _empty_minute_replay()
    replay[field] = value
    (state / "rebuild_receipt.json").write_text(json.dumps({"sessions": [{"intraday_replay": replay}]}))
    with pytest.raises(SnapshotError, match="source pins"):
        artifact_service_references((tmp_path / "unrelated",), tmp_path)


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

    def load(path, *, source_signatures=None):
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
    monkeypatch.setattr("stockagent.config.load_config", lambda path, **kwargs: Config())
    _market(tmp_path, enabled=True, config_path="configs/active.yaml")
    root = tmp_path / "data_tw_minute/research_dataset_developing_v5"
    assert artifact_service_references((root,), tmp_path)[str(root)]


def test_inherited_config_source_is_a_service_dependency(tmp_path, monkeypatch):
    from stockagent.config import _load_raw_config
    @dataclass
    class Config:
        data: dict = field(default_factory=dict)
    archived = tmp_path / "artifacts/markets/inherited-code/configs"
    archived.mkdir(parents=True)
    (archived / "base.yaml").write_text("{}\n")
    current = tmp_path / "configs/active.yaml"
    current.parent.mkdir()
    current.write_text(yaml.safe_dump({"base_config": str(archived / "base.yaml")}))
    def load(path, *, source_signatures=None):
        _load_raw_config(path, source_signatures=source_signatures)
        return Config()
    monkeypatch.setattr("stockagent.config.load_config", load)
    _market(tmp_path, enabled=True, config_path="configs/active.yaml")
    assert any("config_source" in r for r in _refs(tmp_path, "inherited-code"))


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


def _current_forward_consumer(repo, **options):
    config = _market(repo, enabled=True, market_type="tw", day_trade_simulation_enabled=True,
                     live_output_dir="artifacts/live_signals", model_scoped_live_output=True,
                     output_dir="artifacts/markets/selected", fold_id=11, **options)
    root = repo / "artifacts/live_signals/selected/fold_11"
    root.mkdir(parents=True)
    summary = root / "signal-1/summary.json"
    summary.parent.mkdir()
    summary.write_text(json.dumps({"market": "example", "signal_id": "signal-1", "live_latency": {
        "completed_forward_cache": {"contract": "completed_tw_panel_forward_cpu_v3", "key": "a" * 64}}}))
    pointer = root / "latest_signal.json"
    pointer.write_text(json.dumps({"schema_version": 2, "market": "example", "signal_id": "signal-1",
                                   "summary_path": str(summary.relative_to(repo))}))
    return config, pointer, summary


def test_current_forward_pins_follow_canonical_selected_fold_not_historical_walk(tmp_path):
    config, pointer, summary = _current_forward_consumer(tmp_path)
    selection = config.parent.parent / "models/example.yaml"
    selection.parent.mkdir()
    selection.write_text("output_dir: artifacts/markets/selected\nfold_id: 11\n")
    raw = yaml.safe_load(config.read_text())
    raw.update(output_dir="artifacts/markets/old", fold_id=1, model_selection_path="../models/example.yaml")
    config.write_text(yaml.safe_dump(raw))
    # Historical bodies are deliberately not valid JSON; the census must not
    # enumerate or load them, nor derive current pins from directory names.
    (summary.parent / "unrelated.json").write_text("not JSON")
    result = completed_forward_consumer_pins(tmp_path)
    assert result["complete_for_scope"] and result["errors"] == []
    assert result["keys"] == ["a" * 64]
    assert result["consumers"][0]["pointer_path"] == str(pointer)
    assert {row["path"] for row in result["controls"]} == {
        str(config), str(pointer), str(summary), str(selection)}
    assert not result["all_fleet_consumers_exhaustive"]


def test_current_forward_pins_include_the_producers_unconstrained_output(tmp_path):
    _, _, summary = _current_forward_consumer(tmp_path)
    value = json.loads(summary.read_text())
    forward = value["live_latency"]["completed_forward_cache"]
    forward["unconstrained"] = {"contract": forward["contract"], "key": "b" * 64}
    summary.write_text(json.dumps(value))
    result = completed_forward_consumer_pins(tmp_path)
    assert result["complete_for_scope"] and not result["errors"]
    assert result["keys"] == ["a" * 64, "b" * 64]
    assert [(item["output_kind"], item["key"]) for item in result["consumers"]] == [
        ("masked", "a" * 64), ("unconstrained", "b" * 64)]


@pytest.mark.parametrize("defect", [None, [], {}, {"key": "b" * 64},
                                    {"contract": "unknown", "key": "b" * 64},
                                    {"contract": "completed_tw_panel_forward_cpu_v3", "key": "../foreign"},
                                    {"contract": "completed_tw_panel_forward_cpu_v3", "key": "b" * 64,
                                     "unconstrained": {"key": "c" * 64}}])
def test_unknown_unconstrained_dependency_retains_the_known_main_key(tmp_path, defect):
    _, _, summary = _current_forward_consumer(tmp_path)
    value = json.loads(summary.read_text())
    value["live_latency"]["completed_forward_cache"]["unconstrained"] = defect
    summary.write_text(json.dumps(value))
    result = completed_forward_consumer_pins(tmp_path)
    assert not result["complete_for_scope"] and result["errors"]
    assert "a" * 64 in result["keys"]


def test_identical_masked_and_unconstrained_keys_are_a_single_pin(tmp_path):
    _, _, summary = _current_forward_consumer(tmp_path)
    value = json.loads(summary.read_text())
    forward = value["live_latency"]["completed_forward_cache"]
    forward["unconstrained"] = {"contract": forward["contract"], "key": forward["key"]}
    summary.write_text(json.dumps(value))
    result = completed_forward_consumer_pins(tmp_path)
    assert result["complete_for_scope"] and result["keys"] == ["a" * 64]
    assert len(result["consumers"]) == 2


@pytest.mark.parametrize("kind", ["runtime", "overnight"])
def test_current_forward_pins_include_independently_enabled_consumers(tmp_path, kind):
    config, _, _ = _current_forward_consumer(tmp_path)
    raw = yaml.safe_load(config.read_text())
    raw["enabled"] = False
    if kind == "overnight":
        raw["overnight_simulation_enabled"] = True
    else:
        state = tmp_path / "artifacts/discord_bot/state.json"
        state.parent.mkdir()
        state.write_text(json.dumps({"markets": {"example": {"enabled": True}}}))
    config.write_text(yaml.safe_dump(raw))
    assert completed_forward_consumer_pins(tmp_path)["keys"] == ["a" * 64]
    raw["overnight_simulation_enabled"] = False
    config.write_text(yaml.safe_dump(raw))
    if kind == "runtime":
        state.write_text(json.dumps({"markets": {"example": {"enabled": False}}}))
    result = completed_forward_consumer_pins(tmp_path)
    assert result["complete_for_scope"] and result["keys"] == []


@pytest.mark.parametrize("defect", ["missing", "symlink", "fifo", "oversized", "duplicate_json",
                                    "pointer_market", "pointer_schema", "summary_market", "signal_id",
                                    "contract", "key", "escape", "mode", "malformed_state",
                                    "unknown_runtime", "duplicate_market", "missing_selection"])
def test_current_forward_census_is_incomplete_for_unverifiable_control_metadata(tmp_path, defect):
    config, pointer, summary = _current_forward_consumer(tmp_path)
    target = summary
    if defect == "missing":
        summary.unlink()
    elif defect == "symlink":
        outside = tmp_path / "outside.json"
        summary.rename(outside)
        summary.symlink_to(outside)
    elif defect == "fifo":
        summary.unlink()
        os.mkfifo(summary)
    elif defect == "oversized":
        with summary.open("wb") as stream:
            stream.truncate(64 * 1024**2 + 1)
    elif defect == "duplicate_json":
        summary.write_text('{"market":"example","market":"example"}')
    elif defect in {"pointer_market", "pointer_schema", "escape"}:
        value = json.loads(pointer.read_text())
        field, content = {"pointer_market": ("market", "foreign"), "pointer_schema": ("schema_version", True),
                          "escape": ("summary_path", "artifacts/other/summary.json")}[defect]
        value[field] = content
        pointer.write_text(json.dumps(value))
    elif defect in {"summary_market", "signal_id", "contract", "key"}:
        value = json.loads(summary.read_text())
        if defect in {"contract", "key"}:
            value["live_latency"]["completed_forward_cache"][defect] = "unknown"
        else:
            value["market" if defect == "summary_market" else "signal_id"] = "foreign"
        summary.write_text(json.dumps(value))
    elif defect == "mode":
        summary.chmod(0o666)
    elif defect in {"malformed_state", "unknown_runtime"}:
        state = tmp_path / "artifacts/discord_bot/state.json"
        state.parent.mkdir()
        state.write_text(json.dumps({"markets": [] if defect == "malformed_state" else
                                    {"unknown": {"enabled": True}}}))
    elif defect == "duplicate_market":
        config.with_suffix(".yml").write_bytes(config.read_bytes())
    else:
        raw = yaml.safe_load(config.read_text())
        raw["model_selection_path"] = "missing.yaml"
        config.write_text(yaml.safe_dump(raw))
    result = completed_forward_consumer_pins(tmp_path)
    assert not result["complete_for_scope"] and result["errors"]
    assert target.exists() or target.is_symlink() or defect == "missing"


@pytest.mark.parametrize("change", ["pointer", "new_state", "new_config"])
def test_current_forward_census_rechecks_generations_and_market_namespace(tmp_path, monkeypatch, change):
    from stockagent.data_sync import artifact_consumers as consumers
    _, pointer, summary = _current_forward_consumer(tmp_path)
    original, changed = consumers._control_bytes, []
    def read(path, **kwargs):
        result = original(path, **kwargs)
        if path == summary and not changed:
            changed.append(True)
            if change == "pointer":
                pointer.write_bytes(pointer.read_bytes() + b"\n")
            elif change == "new_state":
                state = tmp_path / "artifacts/discord_bot/state.json"
                state.parent.mkdir()
                state.write_text('{"markets":{}}')
            else:
                pointer.parent.joinpath("unused").mkdir()
                config = tmp_path / "services/discord_bot/markets/other.yml"
                config.write_text("market: other\nenabled: false\n")
        return result
    monkeypatch.setattr(consumers, "_control_bytes", read)
    result = completed_forward_consumer_pins(tmp_path)
    assert result["keys"] == ["a" * 64]  # Retain the already observed key, even on a race.
    assert not result["complete_for_scope"] and result["errors"]


def test_service_yaml_and_yml_are_both_dependency_inputs(tmp_path):
    config = _market(tmp_path, output_dir="artifacts/markets/selected")
    config.rename(config.with_suffix(".yml"))
    assert _refs(tmp_path, "selected")


def test_current_forward_census_does_not_import_numeric_or_inference_runtime(tmp_path):
    _current_forward_consumer(tmp_path)
    process = subprocess.run([sys.executable, "-c", (
        "import sys; from pathlib import Path; "
        "from stockagent.data_sync.artifact_consumers import completed_forward_consumer_pins; "
        "result = completed_forward_consumer_pins(Path(sys.argv[1])); "
        "assert result['complete_for_scope'], result['errors']; "
        "assert not any(k.split('.')[0] in {'torch','numpy','polars','pandas'} for k in sys.modules)"
    ), str(tmp_path)], capture_output=True, text=True, timeout=10)
    assert process.returncode == 0, process.stderr
