from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

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


def test_same_session_rule_command_targets_today_without_overwriting_parent_metadata(
    tmp_path: Path,
) -> None:
    live_root = tmp_path / "live" / "data_tw_public"
    command = refresh._same_session_rule_command(
        python="python",
        live_root=live_root,
        trading_date="2026-08-13",
    )

    assert command[command.index("--datasets") + 1 : command.index("--end-date")] == [
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    ]
    assert command[command.index("--end-date") + 1] == "2026-08-13"
    assert command[command.index("--same-session-rule-date") + 1] == "2026-08-13"
    assert Path(command[command.index("--output-dir") + 1]) == live_root
    assert "--require-taiex-session-calendar" in command
    assert "--no-write-run-metadata" in command


def test_same_session_rule_refresh_reuses_already_exact_local_coverage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coverage = {
        "twse": {"covered": True, "target_date": "2026-08-20"},
        "tpex": {"covered": True, "target_date": "2026-08-20"},
    }
    monkeypatch.setattr(
        refresh,
        "require_exact_session_eligibility",
        lambda **_kwargs: coverage,
    )
    monkeypatch.setattr(
        refresh.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("network refresh should not run"),
    )

    result = refresh._refresh_same_session_rules(
        tmp_path,
        observed=datetime(2026, 8, 20, 8, 30, tzinfo=ZoneInfo("Asia/Taipei")),
    )

    assert result["venues"] == coverage
    assert result["refresh_attempted"] is False
    assert result["source"] == "existing_exact_session_coverage"


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
