from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import refresh_tw_public_live_snapshot as refresh


def _summary() -> dict[str, object]:
    return {
        "end_date": "2026-08-11",
        "daily_close_ready": True,
        "coverage_complete": True,
        "blocking_failed_count": 0,
        "missing_dates_after": 0,
    }


def test_download_summary_gate_is_fail_closed() -> None:
    refresh._validate_download_summary(_summary(), expected_latest="2026-08-11")
    for key, value in (
        ("end_date", "2026-08-10"),
        ("daily_close_ready", False),
        ("coverage_complete", False),
        ("blocking_failed_count", 1),
        ("missing_dates_after", 1),
    ):
        payload = _summary()
        payload[key] = value
        with pytest.raises(RuntimeError, match="failed closed"):
            refresh._validate_download_summary(
                payload, expected_latest="2026-08-11"
            )


def test_downloader_command_keeps_every_mutable_output_in_live_tree(
    tmp_path: Path,
) -> None:
    live_root = tmp_path / "live" / "data_tw_public"
    command = refresh._downloader_command(
        python="python",
        downloader=tmp_path / "download.py",
        config_path=tmp_path / "config.yaml",
        live_root=live_root,
    )
    for option, expected in (
        ("--public-dir", live_root),
        ("--stock-root", live_root / "stocks"),
        (
            "--public-feature-path",
            live_root / "features" / "tw_public_stock_daily.parquet",
        ),
    ):
        assert Path(command[command.index(option) + 1]) == expected
    assert not any(
        value.startswith("data_tw_public/")
        for value in command
        if isinstance(value, str)
    )


def test_audit_command_reads_only_the_mutable_live_tree(tmp_path: Path) -> None:
    live_root = tmp_path / "live" / "data_tw_public"
    output_dir = tmp_path / "audit"
    command = refresh._audit_command(
        python="python",
        config_path=tmp_path / "config.yaml",
        live_root=live_root,
        output_dir=output_dir,
    )
    for option, expected in (
        ("--parquet-root", live_root / "stocks"),
        ("--public-dir", live_root),
        (
            "--public-feature-path",
            live_root / "features" / "tw_public_stock_daily.parquet",
        ),
        ("--output-dir", output_dir),
    ):
        assert Path(command[command.index(option) + 1]) == expected
    assert "--strict" in command
    assert "--require-live-selected-features" in command


def test_atomic_switch_and_daily_reuse_require_exact_materialized_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    old = tmp_path / "old"
    target = tmp_path / "target"
    old.mkdir()
    target.mkdir()
    link = tmp_path / "data_tw_public"
    link.symlink_to(old, target_is_directory=True)
    monkeypatch.setattr(refresh, "REPO_ROOT", tmp_path)

    refresh._switch_repo_symlink(target)
    assert link.is_symlink()
    assert link.resolve() == target

    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "status": "ok",
                "expected_latest_date": "2026-08-11",
                "materialized_path": str(target),
            }
        ),
        encoding="utf-8",
    )
    assert (
        refresh._can_reuse_today(
            receipt,
            expected_latest="2026-08-11",
            active_link=link,
        )
        is not None
    )
    assert (
        refresh._can_reuse_today(
            receipt,
            expected_latest="2026-08-12",
            active_link=link,
        )
        is None
    )
