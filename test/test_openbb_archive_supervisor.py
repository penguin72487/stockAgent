from __future__ import annotations

import fcntl
import os
from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR = REPO_ROOT / "scripts" / "run_openbb_archive_supervisor.sh"
USER_ENTRY = REPO_ROOT / "scripts" / "run_openbb_archive_until_complete.sh"
DOWNLOAD_ENTRY = REPO_ROOT / "scripts" / "run_openbb_archive_download.sh"


def _run(script: Path, *args: str, env: dict[str, str] | None = None):
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    return subprocess.run(
        ["bash", str(script), *args],
        cwd=REPO_ROOT,
        env=process_env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_archive_shell_entrypoints_have_valid_syntax() -> None:
    for script in (SUPERVISOR, USER_ENTRY, DOWNLOAD_ENTRY):
        result = subprocess.run(
            ["bash", "-n", str(script)],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


def test_supervisor_rejects_invalid_monitor_timeout_before_download() -> None:
    result = _run(
        SUPERVISOR,
        env={"OPENBB_FULL_MONITOR_TIMEOUT_SECONDS": "0"},
    )

    assert result.returncode == 2
    assert "OPENBB_FULL_MONITOR_TIMEOUT_SECONDS" in result.stderr


def test_supervisor_rejects_invalid_idle_monitor_interval_before_download() -> None:
    result = _run(
        SUPERVISOR,
        env={"OPENBB_IDLE_FULL_MONITOR_INTERVAL_SECONDS": "0"},
    )

    assert result.returncode == 2
    assert "OPENBB_IDLE_FULL_MONITOR_INTERVAL_SECONDS" in result.stderr


def test_user_entry_rejects_start_date_override() -> None:
    result = _run(USER_ENTRY, "--start-date", "2001-01-01")

    assert result.returncode == 2
    assert "fixed at 2000-01-01" in result.stderr


def test_user_entry_ignores_stale_or_recycled_pid_files(tmp_path) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir()
    (state_dir / "supervisor.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    result = _run(
        USER_ENTRY,
        env={
            "OPENBB_OUTPUT_DIR": str(tmp_path),
            "OPENBB_MIN_FREE_BYTES": "0",
            "OPENBB_PREFLIGHT_ONLY": "1",
        },
    )

    assert result.returncode == 0, result.stderr
    assert "preflight completed" in result.stdout


def test_user_entry_refuses_duplicate_kernel_lock_holder(tmp_path) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir()
    with (state_dir / "supervisor.lock").open("w") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run(
            USER_ENTRY,
            env={
                "OPENBB_OUTPUT_DIR": str(tmp_path),
                "OPENBB_MIN_FREE_BYTES": "0",
            },
        )

    assert result.returncode == 3
    assert "another supervisor already holds" in result.stdout


def test_user_entry_preflight_does_not_start_downloader(tmp_path) -> None:
    result = _run(
        USER_ENTRY,
        env={
            "OPENBB_OUTPUT_DIR": str(tmp_path),
            "OPENBB_MIN_FREE_BYTES": "0",
            "OPENBB_PREFLIGHT_ONLY": "1",
        },
    )

    assert result.returncode == 0, result.stderr
    assert "preflight completed; downloader was not started" in result.stdout
    assert not (tmp_path / "_state" / "supervisor.pid").exists()
    assert not (tmp_path / "_state" / "downloader.pid").exists()


def test_user_entry_rejects_conflicting_pinned_end_date(tmp_path) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir()
    (state_dir / "archive_end_date.txt").write_text("2026-07-18\n", encoding="utf-8")

    result = _run(
        USER_ENTRY,
        env={
            "OPENBB_OUTPUT_DIR": str(tmp_path),
            "OPENBB_ARCHIVE_END_DATE": "2026-07-19",
            "OPENBB_MIN_FREE_BYTES": "0",
        },
    )

    assert result.returncode == 2
    assert "already pinned to 2026-07-18" in result.stderr


def test_supervisor_rejects_conflicting_pinned_end_date(tmp_path) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir()
    (state_dir / "archive_end_date.txt").write_text("2026-07-18\n", encoding="utf-8")

    result = _run(
        SUPERVISOR,
        env={
            "OPENBB_OUTPUT_DIR": str(tmp_path),
            "OPENBB_ARCHIVE_END_DATE": "2026-07-19",
            "OPENBB_MIN_FREE_BYTES": "0",
        },
    )

    assert result.returncode == 2
    assert "already pinned to 2026-07-18" in result.stderr
    assert not (state_dir / "supervisor.pid").exists()
    assert not (state_dir / "downloader.pid").exists()


def test_download_entry_rejects_conflicting_pinned_end_date(tmp_path) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir()
    (state_dir / "archive_end_date.txt").write_text("2026-07-18\n", encoding="utf-8")

    result = _run(
        DOWNLOAD_ENTRY,
        "--output-dir",
        str(tmp_path),
        "--end-date",
        "2026-07-19",
    )

    assert result.returncode == 2
    assert "already pinned to 2026-07-18" in result.stderr
