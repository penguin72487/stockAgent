from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from downloader.download_tw_public_data import DEFAULT_DATASETS
from scripts import run_tw_public_0830_check
from scripts import watch_tw_public_publication_group as publication


TAIPEI = ZoneInfo("Asia/Taipei")


def test_preopen_sweep_covers_every_registered_official_dataset() -> None:
    phase = publication.PUBLICATION_PHASES["preopen_all"]
    selected = publication._select_specs(list(phase.selectors))
    assert {spec.name for spec in selected} == set(DEFAULT_DATASETS)
    assert len(selected) == 156


def test_auto_phase_accepts_two_minute_retry() -> None:
    observed = datetime(2026, 8, 17, 14, 52, tzinfo=TAIPEI)
    phase = publication.resolve_phase(observed, window_minutes=20.0)
    assert phase.name == "institutional_initial"


def test_auto_phase_catchup_uses_full_sweep() -> None:
    observed = datetime(2026, 8, 17, 12, 0, tzinfo=TAIPEI)
    phase = publication.resolve_phase(observed, window_minutes=20.0)
    assert phase.name == "preopen_all"


def test_close_command_requires_verified_publication(tmp_path: Path) -> None:
    args = type(
        "Args",
        (),
        {"workers": 8, "date_workers": 4, "timeout": 20, "retries": 2},
    )()
    phase = publication.PUBLICATION_PHASES["close_initial"]
    command = publication._download_command(
        live_root=tmp_path,
        phase=phase,
        args=args,
    )
    assert "--require-taiex-session-calendar" in command
    assert "--require-daily-close-publication" in command
    assert "--no-write-run-metadata" in command


def test_snapshot_fingerprint_ignores_download_clock_but_detects_content(
    tmp_path: Path,
) -> None:
    name = "twse_api_exchangereport_mi_margn"
    path = tmp_path / f"{name}.parquet"
    pl.DataFrame(
        {
            "symbol": ["2330", "2317"],
            "balance": ["10", "20"],
            "_downloaded_at_utc": ["2026-08-17T01:00:00Z"] * 2,
        }
    ).write_parquet(path)
    first = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    pl.DataFrame(
        {
            "symbol": ["2317", "2330"],
            "balance": ["20", "10"],
            "_downloaded_at_utc": ["2026-08-17T02:00:00Z"] * 2,
        }
    ).write_parquet(path)
    reordered = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    assert reordered == first

    pl.DataFrame(
        {
            "symbol": ["2317", "2330"],
            "balance": ["21", "10"],
            "_downloaded_at_utc": ["2026-08-17T03:00:00Z"] * 2,
        }
    ).write_parquet(path)
    changed = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    assert changed != first


def test_content_change_comparison_ignores_parquet_serialization_size() -> None:
    before = {
        "dataset": {
            "sha256": "semantic-fingerprint",
            "bytes": 100,
            "fingerprint_kind": "semantic_without_download_clock",
        }
    }
    after = {
        "dataset": {
            "sha256": "semantic-fingerprint",
            "bytes": 120,
            "fingerprint_kind": "semantic_without_download_clock",
        }
    }
    assert publication._changed_files(before, after) == []


def test_0830_command_forces_full_acceptance_refresh(tmp_path: Path) -> None:
    command = run_tw_public_0830_check.refresh_command(tmp_path / "tw.yaml")
    assert Path(command[1]).name == "refresh_tw_public_live_snapshot.py"
    assert "--force" in command


def test_0830_requires_both_exact_session_venues() -> None:
    snapshot = {
        "same_session_eligibility": {
            "trading_date": "2026-08-17",
            "venues": {
                "twse": {"covered": True, "target_date": "2026-08-17"},
                "tpex": {"covered": True, "target_date": "2026-08-17"},
            },
        }
    }
    assert run_tw_public_0830_check._same_session_accepted(
        snapshot,
        trading_date="2026-08-17",
    )
    snapshot["same_session_eligibility"]["venues"]["tpex"]["covered"] = False
    assert not run_tw_public_0830_check._same_session_accepted(
        snapshot,
        trading_date="2026-08-17",
    )


def test_systemd_timers_have_no_random_delay() -> None:
    publication_timer = (
        Path("deploy/systemd/stockagent-tw-public-publication-sweep.timer.in")
        .read_text(encoding="utf-8")
    )
    acceptance_timer = (
        Path("deploy/systemd/stockagent-tw-public-0830-check.timer.in")
        .read_text(encoding="utf-8")
    )
    assert "07:50:00 Asia/Taipei" in publication_timer
    assert "21:00:30 Asia/Taipei" in publication_timer
    assert "21:01:00 Asia/Taipei" in publication_timer
    assert "21:02:00 Asia/Taipei" in publication_timer
    assert "RandomizedDelaySec=0" in publication_timer
    assert "08:30:00 Asia/Taipei" in acceptance_timer
    assert "RandomizedDelaySec=0" in acceptance_timer


def test_service_wrappers_exec_resolved_python_not_shell_function() -> None:
    for name in (
        "run_tw_day_trade_eligibility_watcher.sh",
        "run_tw_public_publication_sweep.sh",
        "run_tw_public_0830_check.sh",
    ):
        body = (Path("scripts") / name).read_text(encoding="utf-8")
        assert 'python_bin="$(resolve_fintech_python)"' in body
        assert "exec run_fintech_python" not in body
