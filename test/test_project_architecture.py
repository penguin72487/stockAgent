"""Keep clean-checkout configuration and read-only inventory contracts."""

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
