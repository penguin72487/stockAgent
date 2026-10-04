from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from scripts.packed_backup import backup_status


def test_backup_status_does_not_report_stale_up_to_date_after_cutover(
    tmp_path: Path,
) -> None:
    source = tmp_path / "packed"
    source.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    (state / "status.json").write_text(json.dumps({"state": "up_to_date"}))
    config = SimpleNamespace(source=source, state_dir=state)
    assert backup_status(config)["state"] == "up_to_date"
    (source / ".stockagent-d-primary").write_text("{}")
    result = backup_status(config)
    assert result["state"] == "retired_single_d_primary"
    assert result["backup_verified"] is False
    (source / ".stockagent-d-primary").unlink()
    (source / ".stockagent-d-mount-required").write_text("required")
    assert backup_status(config)["state"] == "unavailable"
