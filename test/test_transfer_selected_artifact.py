import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import transfer_selected_artifact as entry


def test_remote_build_sends_repository_code_not_artifact_bytes(tmp_path, monkeypatch):
    config = json.loads(Path("configs/data_sync/artifact_ingress.json").read_text())
    config["state_root"] = str(tmp_path / "state")
    for name, value in {
        "IDENTITY_FILE": "/key",
        "SSH_PORT": "22",
        "SSH_TARGET": "user@host",
    }.items():
        monkeypatch.setenv("COLD_ARTIFACT_INGRESS_" + name, value)
    calls = []

    def execute(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"package_id":"verified"}')

    monkeypatch.setattr(entry.subprocess, "run", execute)
    assert entry.remote_control(config, action="build")["package_id"] == "verified"
    command, kwargs = calls[0]
    assert command[0] == "ssh"
    assert "def build_package" in kwargs["input"]
    assert "--config-json" in kwargs["input"]
    assert not any(token in command for token in ("scp", "rsync", "cat"))
    assert json.loads((tmp_path / "state/remote-build.json").read_text()) == {
        "package_id": "verified"
    }


def test_receive_waits_for_both_sides_without_opening_candidate(tmp_path, monkeypatch):
    config = json.loads(Path("configs/data_sync/artifact_ingress.json").read_text())
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr(
        entry.sys,
        "argv",
        ["transfer", "receive", "--config", str(config_path), "--package-id", "a" * 64],
    )
    monkeypatch.setattr(entry, "local_syncthing", lambda *_: {"ok": True})
    monkeypatch.setattr(
        entry, "remote_control", lambda *_args, **_kwargs: {"ok": False}
    )

    def must_not_receive(*_):
        pytest.fail("receive must wait for source-side hashing errors too")

    monkeypatch.setattr(entry, "receive_package", must_not_receive)
    with pytest.raises(SystemExit, match="not converged"):
        entry.main()
