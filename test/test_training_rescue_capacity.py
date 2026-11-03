"""Capacity is a deferred admission; original recovery failures stay failures."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import training_rescue as command
from stockagent import training_rescue as rescue


def test_physical_d_floor_defers_without_relaxing_admission(tmp_path, monkeypatch):
    cold = tmp_path / "d" / ".local-state" / "training-rescue" / "run"
    original_path = Path
    monkeypatch.setattr(rescue, "Path", lambda value: (
        tmp_path / "d" / ".local-state" / "training-rescue"
        if value == "/srv/stockagent-packed/.local-state/training-rescue" else original_path(value)))
    monkeypatch.setattr("stockagent.data_sync.cold_primary._check_d_primary_mount", lambda path: None)
    instance = object.__new__(rescue.Rescue)
    instance.cold = cold
    instance.policy = {"reserve_bytes": 32 * 1024**3}
    free = [instance.policy["reserve_bytes"] - 1]
    monkeypatch.setattr(rescue.shutil, "disk_usage", lambda path: SimpleNamespace(free=free[0]))
    with pytest.raises(rescue.RescueCapacityWait, match="D reserve reached"):
        instance.guard()
    assert not list(cold.iterdir())
    free[0] += 1
    instance.guard()
    assert instance.policy["reserve_bytes"] == 32 * 1024**3


@pytest.mark.parametrize("error", [rescue.RescueCapacityWait("D reserve reached"), BlockingIOError()])
def test_expected_wait_reaches_original_timer_with_temporary_exit(error, monkeypatch, capsys):
    def waiting():
        raise error
    monkeypatch.setattr(command, "main", waiting)
    assert command.cli() == 75
    output = capsys.readouterr()
    assert not output.err
    assert json.loads(output.out)["retry_later"] is True
    if isinstance(error, rescue.RescueCapacityWait):
        assert json.loads(output.out)["runtime_acceptance"] is False
        assert json.loads(output.out)["source_retirement"] is False


@pytest.mark.parametrize("error", [rescue.RescueError("runtime mismatch"), ValueError("wrong enrollment"),
                                  OSError("disk read failed")])
def test_recovery_integrity_and_io_errors_are_not_capacity_waits(error, monkeypatch, capsys):
    def failing():
        raise error
    monkeypatch.setattr(command, "main", failing)
    assert command.cli() == 65
    output = capsys.readouterr()
    assert not output.out
    assert json.loads(output.err)["state"] == "rescue_rejected"
