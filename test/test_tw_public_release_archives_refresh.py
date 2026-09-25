from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import refresh_tw_public_release_archives as refresh
from scripts.refresh_tw_public_release_archives import (
    ARCHIVES, _command, _money_recent_pages, _source_update_lock,
)


def test_recent_scan_requires_completed_full_archive(tmp_path: Path) -> None:
    state_path = tmp_path / "state" / "cbc_money_release_vintages.json"
    state_path.parent.mkdir()
    assert _money_recent_pages(tmp_path, full_index=False) == 0
    state_path.write_text(json.dumps({"status": "running", "complete": False}))
    assert _money_recent_pages(tmp_path, full_index=False) == 0
    state_path.write_text(json.dumps({"status": "complete", "complete": True}))
    assert _money_recent_pages(tmp_path, full_index=False) == 2
    assert _money_recent_pages(tmp_path, full_index=True) == 0


def test_separate_hosts_are_rate_budgeted_independently(tmp_path: Path) -> None:
    dgbas = _command("dgbas_release_vintages", tmp_path, money_recent_pages=2)
    cbc_fx = _command("cbc_fx_reserve_release_vintages", tmp_path, money_recent_pages=2)
    cbc_money = _command("cbc_money_release_vintages", tmp_path, money_recent_pages=2)
    assert dgbas[dgbas.index("--request-interval") + 1] == "1.0"
    assert cbc_fx[cbc_fx.index("--request-interval") + 1] == "0.25"
    assert cbc_money[cbc_money.index("--recent-pages") + 1] == "2"


def test_refresh_respects_canonical_source_update_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(refresh.subprocess, "run", lambda *_args, **_kwargs: None)
    root = tmp_path / "data_tw_public"
    root.mkdir()
    with _source_update_lock(root):
        with pytest.raises(RuntimeError, match="remained busy"):
            with _source_update_lock(root, wait_seconds=0.01):
                pass


def test_refresh_waits_for_transient_source_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(refresh.subprocess, "run", lambda *_args, **_kwargs: None)
    original_flock = refresh.fcntl.flock
    attempts = 0

    def transient_busy(fd: int, operation: int) -> None:
        nonlocal attempts
        if operation & refresh.fcntl.LOCK_NB:
            attempts += 1
            if attempts <= 2:
                raise BlockingIOError("transient producer")
        original_flock(fd, operation)

    monkeypatch.setattr(refresh.fcntl, "flock", transient_busy)
    with _source_update_lock(tmp_path / "data_tw_public", wait_seconds=1):
        assert attempts == 3


def test_refresh_rechecks_opening_guard_after_lock_and_releases_on_reject(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    def reject_guard(_command: list[str], **_kwargs: object) -> None:
        raise refresh.subprocess.CalledProcessError(1, "opening guard")

    monkeypatch.setattr(refresh.subprocess, "run", reject_guard)
    root = tmp_path / "data_tw_public"
    with pytest.raises(refresh.subprocess.CalledProcessError):
        with _source_update_lock(root, wait_seconds=0):
            pytest.fail("source writes must not start after guard rejection")

    with (tmp_path / ".locks" / "tw-public-refresh.lock").open("a+") as handle:
        refresh.fcntl.flock(
            handle.fileno(), refresh.fcntl.LOCK_EX | refresh.fcntl.LOCK_NB
        )
        refresh.fcntl.flock(handle.fileno(), refresh.fcntl.LOCK_UN)


def test_service_waits_inside_producer_before_running_archive() -> None:
    service = Path("deploy/systemd/stockagent-tw-public-release-archives.service.in").read_text(
        encoding="utf-8"
    )
    assert "ExecCondition=/usr/bin/flock -n" not in service
    assert "--minimum-runway-minutes 45" in service
    assert "refresh_tw_public_release_archives.py" in service
    assert "ReadWritePaths=__REPO_ROOT__/artifacts /srv/stockagent-live/data_tw_public /srv/stockagent-live/.locks" in service


def test_archive_refresh_builds_research_after_last_mof_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(
        refresh.subprocess, "run",
        lambda command, **_kwargs: commands.append(command),
    )
    refresh._refresh(tmp_path, {name: [name] for name in ARCHIVES})
    assert [Path(command[1]).name for command in commands[-5:]] == [
        "download_tw_mof_release_archive.py",
        "build_tw_public_provisional_macro.py",
        "build_tw_public_research_features.py",
        "build_tw_public_research_taifex.py",
        "build_tw_public_research_all_features.py",
    ]
