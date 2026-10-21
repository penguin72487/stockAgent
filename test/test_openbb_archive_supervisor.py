from __future__ import annotations

import fcntl
import os
from pathlib import Path
import subprocess
import sys


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


def _monitor_functions() -> str:
    source = SUPERVISOR.read_text()
    return source[source.index('monitor_pid=""'):source.index("refresh_watchdog_state()")]


def test_background_full_monitor_does_not_block_or_duplicate_watchdog(tmp_path) -> None:
    # Execute the maintained shell functions against a bounded local fixture;
    # no real downloader, API, archive or systemd owner is started.
    script = tmp_path / "scripts" / "monitor_openbb_archive.py"
    script.parent.mkdir()
    script.write_text("import time\ntime.sleep(0.6)\n")
    command = _monitor_functions() + """
start_snapshot_monitor
first_pid="$monitor_pid"
if start_snapshot_monitor; then exit 11; fi
if poll_snapshot_monitor; then exit 12; fi
[[ "$monitor_pid" == "$first_pid" ]] || exit 13
printf 'watchdog-not-blocked\\n'
wait "$monitor_pid" || exit 14
poll_snapshot_monitor || exit 15
[[ -z "$monitor_pid" && "$snapshot_monitor_rc" == 0 ]] || exit 16
"""
    env = os.environ | {"python_bin": sys.executable, "output_dir": str(tmp_path),
                        "full_monitor_timeout": "5"}
    result = subprocess.run(["bash", "-euc", command], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "watchdog-not-blocked" in result.stdout


def test_background_monitor_timeout_is_reaped_and_terminal_check_is_fresh(tmp_path) -> None:
    script = tmp_path / "scripts" / "monitor_openbb_archive.py"
    script.parent.mkdir()
    script.write_text("from pathlib import Path\nimport sys,time\n"
                      "stamp=Path('invocations.txt')\n"
                      "stamp.write_text(stamp.read_text()+'x' if stamp.exists() else 'x')\n"
                      "time.sleep(2) if len(stamp.read_text()) == 1 else None\n")
    command = _monitor_functions() + """
start_snapshot_monitor
wait "$monitor_pid" || true
poll_snapshot_monitor || exit 11
[[ "$snapshot_monitor_rc" == 124 && -z "$monitor_pid" ]] || exit 12
# A post-exit gate must start a fresh invocation even after a background pass.
run_snapshot_monitor || exit 13
[[ -z "$monitor_pid" ]] || exit 14
"""
    env = os.environ | {"python_bin": sys.executable, "output_dir": str(tmp_path),
                        "full_monitor_timeout": "1"}
    result = subprocess.run(["bash", "-euc", command], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "invocations.txt").read_text() == "xx"


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
