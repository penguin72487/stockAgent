"""Keep clean-checkout configuration and read-only inventory contracts."""

import hashlib
import json
from pathlib import Path

import pytest

from scripts.audit_project_architecture import _unit_fields
from stockagent.config import load_config
import stockagent.config as config_module

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "path", sorted((ROOT / "configs/markets").glob("*.yaml")), ids=lambda p: p.name
)
def test_market_configuration_loads_without_local_training_artifacts(
    path: Path, monkeypatch
) -> None:
    # Local artifacts are not distributed with a Git checkout. This also
    # catches missing base_config targets and strict schema regressions.
    original = config_module._load_raw_config

    def repository_config_only(config_path, *args, **kwargs):
        assert Path(config_path).resolve().is_relative_to(ROOT / "configs"), (
            f"market configuration inherits node-local/external artifact: {config_path}"
        )
        return original(config_path, *args, **kwargs)

    monkeypatch.setattr(config_module, "_load_raw_config", repository_config_only)
    load_config(path)


def test_unit_inventory_retains_repeated_calendars_without_environment(
    tmp_path: Path,
) -> None:
    path = tmp_path / "example.timer.in"
    path.write_text(
        "[Timer]\nOnCalendar=Mon 09:00\nOnCalendar=Tue 10:00\n"
        "Environment=SECRET=never-in-inventory\n",
        encoding="utf-8",
    )
    assert _unit_fields(path) == {"OnCalendar": ["Mon 09:00", "Tue 10:00"]}


def test_historical_configs_preserve_exact_sources_and_closed_current_abi():
    bundle = ROOT / "configs/historical/tw_futures_v17_20261003"
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["runnable_with_current_code"] is False
    for entry in manifest["files"]:
        path = bundle / entry["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        config_module._load_raw_config(path)
    for original in manifest["archived_market_paths"]:
        assert not (ROOT / original).exists()
        archived = bundle / "markets" / Path(original).name
        with pytest.raises(ValueError, match="Unknown or removed config key"):
            load_config(archived)


def test_deployment_configuration_loads_without_retired_experiments():
    for path in sorted((ROOT / "configs/deployments").glob("*.yaml")):
        load_config(path)


def test_cpu16_runtime_profile_preserves_complete_experiment_contract():
    from dataclasses import asdict

    name = "tw_day_trade_last_last_only_training_vastai1t_v8_twpublic_248d0869_full"
    base = asdict(load_config(ROOT / f"configs/deployments/{name}.yaml"))
    selected = asdict(load_config(ROOT / f"configs/deployments/{name}_cpu16.yaml"))
    assert selected["environment"]["cpu_threads"] == 16
    assert selected["runner"]["output_dir"] != base["runner"]["output_dir"]
    selected["environment"]["cpu_threads"] = base["environment"]["cpu_threads"]
    selected["runner"]["output_dir"] = base["runner"]["output_dir"]
    assert selected == base


def test_rejected_hotpath_experiments_keep_exact_inheritance_and_source_bytes():
    bundle = ROOT / "configs/historical/tw_day_trade_hotpath_20261003"
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["runnable_with_current_code"] is False
    for entry in manifest["files"]:
        path = bundle / entry["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        config_module._load_raw_config(path)
    for original in manifest["archived_deployment_paths"]:
        assert not (ROOT / original).exists()
        archived = bundle / "deployments" / Path(original).name
        with pytest.raises(ValueError, match="Unknown or removed config key"):
            load_config(archived)
