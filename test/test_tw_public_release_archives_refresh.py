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


def test_refresh_respects_canonical_source_update_lock(tmp_path: Path) -> None:
    root = tmp_path / "data_tw_public"
    root.mkdir()
    with _source_update_lock(root):
        with pytest.raises(RuntimeError, match="already active"):
            with _source_update_lock(root):
                pass


def test_service_skips_busy_producer_without_missing_lock_write_access() -> None:
    service = Path("deploy/systemd/stockagent-tw-public-release-archives.service.in").read_text(
        encoding="utf-8"
    )
    assert "ExecCondition=/usr/bin/flock -n /srv/stockagent-live/.locks/tw-public-refresh.lock" in service
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
    assert commands[-3][1].endswith("download_tw_mof_release_archive.py")
    assert commands[-2][1].endswith("build_tw_public_provisional_macro.py")
    assert commands[-1][1].endswith("build_tw_public_research_features.py")
