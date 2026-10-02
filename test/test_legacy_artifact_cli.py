from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

from scripts import manage_legacy_artifact_archives as cli
from stockagent.data_sync.packed_retention import RetentionConfig


@pytest.mark.parametrize("manual", [False, True])
def test_retirement_cli_uses_hot_retirement_peer_policy(tmp_path, monkeypatch, capsys, manual):
    dataset = "legacy-markets-unused"
    spec = SimpleNamespace(
        dataset=dataset, relative_root="markets/unused", stage_root=tmp_path / "stage"
    )
    cfg = RetentionConfig(
        sync_root=tmp_path / "packed",
        archive_root=tmp_path / "archive",
        materialized_root=tmp_path / "materialized",
        backup_config=tmp_path / "backup.json",
        state_dir=tmp_path / "retention",
        folder_id="packed",
        required_peer_names=("obsolete-cold-object-peer",),
    )
    monkeypatch.setattr(cli, "load_legacy_specs", lambda _: {dataset: spec})
    monkeypatch.setattr(cli.RetentionConfig, "load", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(cli, "load_retirement_peer_names", lambda *args, **kwargs: ())
    monkeypatch.setattr(cli, "_bridge_inactive", lambda _: True)

    def syncthing(local_cfg):
        assert local_cfg.required_peer_names == ()
        return {"ok": True}

    def plan(_spec, **options):
        assert options["materialized_root"] == cfg.materialized_root
        assert options["peer_probe"]() == {"ok": True}
        assert options["manual_immediate"] is manual
        return {"deleted": False, "apply_ready": False}

    monkeypatch.setattr(cli, "_syncthing", syncthing)
    monkeypatch.setattr(cli, "plan_legacy_retirement", plan)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "manage_legacy_artifact_archives.py",
            "retire-plan",
            dataset,
            "--sync-root",
            str(cfg.sync_root),
            *(["--manual-immediate"] if manual else []),
        ],
    )
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["deleted"] is False
    assert cfg.required_peer_names == ("obsolete-cold-object-peer",)
