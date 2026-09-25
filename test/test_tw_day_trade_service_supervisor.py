"""Process-level failure isolation for the paper engine and read-only UI."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPO_ROOT / "scripts/run_tw_day_trade_services.sh"


def _fixture(
    tmp_path: Path, *, engine_exits: bool = False,
    engine_exit_status: int | None = None,
    dashboard_always_exits: bool = False,
) -> tuple[Path, dict[str, str]]:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    launcher = scripts / LAUNCHER.name
    launcher.write_bytes(LAUNCHER.read_bytes())
    runtime = scripts / "runtime_env.sh"
    runtime.write_text('resolve_fintech_python() { printf "%s\\n" "$FAKE_PYTHON"; }\n')
    fake_python = scripts / "fake_python.sh"
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'if [[ "$1" == "scripts/run_tw_day_trade_simulation.py" ]]; then\n'
        '  printf "%s\\n" "$$" >> "$TEST_EVENTS/engine"\n'
        '  if [[ -n "${FAKE_ENGINE_EXIT_STATUS:-}" ]]; then '
        'exit "$FAKE_ENGINE_EXIT_STATUS"; fi\n'
        '  trap "exit 0" TERM INT\n'
        '  while true; do /usr/bin/sleep 0.1; done\n'
        'elif [[ "$1" == "scripts/serve_tw_day_trade_dashboard.py" ]]; then\n'
        '  printf "%s\\n" "$$" >> "$TEST_EVENTS/dashboard"\n'
        '  if [[ "${FAKE_DASHBOARD_ALWAYS_EXIT:-0}" == 1 || '
        '! -e "$TEST_EVENTS/dashboard_failed_once" ]]; then\n'
        '    touch "$TEST_EVENTS/dashboard_failed_once"\n'
        '    exit 7\n'
        '  fi\n'
        '  trap "exit 0" TERM INT\n'
        '  while true; do /usr/bin/sleep 0.1; done\n'
        'else exit 20; fi\n'
    )
    fake_python.chmod(0o755)
    (tmp_path / ".env").write_text(
        "SHIOAJI_API_KEY=fixture\nSHIOAJI_SECRET_KEY=fixture\n"
    )
    events = tmp_path / "events"
    events.mkdir()
    env = {
        **os.environ,
        "FAKE_PYTHON": str(fake_python),
        "FAKE_ENGINE_EXIT_STATUS": (
            str(engine_exit_status)
            if engine_exit_status is not None
            else "9" if engine_exits else ""
        ),
        "FAKE_DASHBOARD_ALWAYS_EXIT": "1" if dashboard_always_exits else "0",
        "SHIOAJI_ENV_FILE": str(tmp_path / ".env"),
        "STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR": str(tmp_path / "cache"),
        "TEST_EVENTS": str(events),
    }
    return launcher, env


def _await_lines(path: Path, count: int, *, timeout: float = 6) -> list[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lines = path.read_text().splitlines() if path.exists() else []
        if len(lines) >= count:
            return lines
        time.sleep(0.05)
    raise AssertionError(f"expected {count} process starts at {path}")


def _terminate_owned_group(process: subprocess.Popen[str]) -> tuple[str, str]:
    if process.poll() is None:
        process.terminate()
    try:
        return process.communicate(timeout=4)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        return process.communicate(timeout=4)


def test_dashboard_crash_retries_without_restarting_paper_engine(tmp_path: Path) -> None:
    launcher, env = _fixture(tmp_path)
    process = subprocess.Popen(
        ["bash", str(launcher)], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        _await_lines(tmp_path / "events/dashboard", 2)
        engine_starts = _await_lines(tmp_path / "events/engine", 1)
        assert len(engine_starts) == 1
        assert process.poll() is None
    finally:
        _stdout, stderr = _terminate_owned_group(process)
    assert "engine remains running; retry in 2s" in stderr


def test_paper_engine_exit_stops_supervisor_for_systemd_restart(tmp_path: Path) -> None:
    launcher, env = _fixture(tmp_path, engine_exits=True)
    process = subprocess.Popen(
        ["bash", str(launcher)], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        _stdout, stderr = process.communicate(timeout=4)
    finally:
        if process.poll() is None:
            _terminate_owned_group(process)
    assert process.returncode == 9
    assert "paper engine exited status=9" in stderr


def test_clean_paper_engine_exit_is_still_an_unexpected_failure(tmp_path: Path) -> None:
    launcher, env = _fixture(tmp_path, engine_exit_status=0)
    process = subprocess.Popen(
        ["bash", str(launcher)], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        _stdout, stderr = process.communicate(timeout=4)
    finally:
        if process.poll() is None:
            _terminate_owned_group(process)
    assert process.returncode == 1
    assert "paper engine exited status=0" in stderr


def test_repeated_dashboard_crashes_have_bounded_backoff(tmp_path: Path) -> None:
    launcher, env = _fixture(tmp_path, dashboard_always_exits=True)
    fake_sleep = tmp_path / "sleep"
    fake_sleep.write_text(
        '#!/usr/bin/env bash\n'
        'printf "%s\\n" "$1" >> "$TEST_EVENTS/retry_delays"\n'
        '/usr/bin/sleep 0.02\n'
    )
    fake_sleep.chmod(0o755)
    env["PATH"] = f"{tmp_path}:{env['PATH']}"
    process = subprocess.Popen(
        ["bash", str(launcher)], cwd=tmp_path, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        delays = _await_lines(tmp_path / "events/retry_delays", 7)
        assert delays[:7] == ["2", "4", "8", "16", "30", "30", "30"]
        assert len(_await_lines(tmp_path / "events/engine", 1)) == 1
        assert process.poll() is None
    finally:
        _terminate_owned_group(process)
