"""Redundant calendar writes must not acquire the model-source lock at opening."""
from datetime import datetime
import os
from pathlib import Path
import subprocess

import pytest

from scripts.check_outside_tw_opening_resource_window import calendar_refresh_admission, evaluate


@pytest.mark.parametrize("clock,allowed", [("08:19:59", True), ("08:20:00", False),
    ("08:59:59", False), ("09:00:00", False), ("09:09:59", False), ("09:10:00", True)])
def test_verified_calendar_yields_during_opening_without_changing_reader_safety(monkeypatch, clock, allowed):
    monkeypatch.setattr("stockagent.live.market_status.verified_tw_stock_session_day",
        lambda *a, **k: (True, "official TWSE schedule as-of 2026-10-20: ordinary weekday session"))
    window = evaluate(datetime.fromisoformat("2026-10-21T" + clock + "+08:00"),
        official_calendar_root=Path("test-calendar"))
    admitted = calendar_refresh_admission(window)
    assert admitted["allowed"] is allowed
    assert not admitted["calendar_bootstrap_required"]
    assert admitted["protected"] is window["protected"]


@pytest.mark.parametrize("verified,reason,bootstrap", [(False, "official calendar unavailable", True),
    (False, "official TWSE schedule as-of 2026-10-20: holiday", False)])
def test_unknown_calendar_can_bootstrap_but_verified_holiday_needs_no_opening_protection(monkeypatch, verified, reason, bootstrap):
    monkeypatch.setattr("stockagent.live.market_status.verified_tw_stock_session_day",
        lambda *a, **k: (verified, reason))
    window = evaluate(datetime.fromisoformat("2026-10-21T09:00:00+08:00"),
        official_calendar_root=Path("test-calendar"))
    admitted = calendar_refresh_admission(window)
    assert admitted["allowed"]
    assert admitted["calendar_bootstrap_required"] is bootstrap
    if bootstrap:
        assert not window["allowed"], "other maintenance must still protect an unknown weekday"


@pytest.mark.parametrize("first_rc,second_rc,collector_runs", [(1, 0, False), (0, 1, False),
    (0, 0, True), (2, 0, False), (0, 2, False)])
def test_calendar_launcher_rechecks_after_lock_wait_and_preserves_real_errors(tmp_path, first_rc, second_rc, collector_runs):
    # Execute the actual launcher with only its filesystem root and runtime
    # replaced. Never open the production source lock or call a live collector.
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    source = Path("scripts/run_tw_public_calendar_refresh.sh").read_text()
    (scripts / "run_tw_public_calendar_refresh.sh").write_text(source.replace("/srv/stockagent-live", str(tmp_path / "live")))
    (scripts / "runtime_env.sh").write_text('''run_fintech_python() {
  if [[ "$1" == scripts/check_outside_tw_opening_resource_window.py ]]; then
    if [[ ! -f "$TASK_CALENDAR_COUNT" ]]; then
      echo first > "$TASK_CALENDAR_COUNT"
      return "$TASK_CALENDAR_FIRST_RC"
    fi
    return "$TASK_CALENDAR_SECOND_RC"
  fi
  echo collected > "$TASK_CALENDAR_COLLECTED"
}
''')
    collected = tmp_path / "collected"
    env = {**os.environ, "TASK_CALENDAR_COUNT": str(tmp_path / "admissions"),
           "TASK_CALENDAR_COLLECTED": str(collected), "TASK_CALENDAR_FIRST_RC": str(first_rc),
           "TASK_CALENDAR_SECOND_RC": str(second_rc)}
    result = subprocess.run(["bash", str(scripts / "run_tw_public_calendar_refresh.sh")],
        env=env, text=True, capture_output=True, timeout=5)
    assert collected.exists() is collector_runs
    assert result.returncode == (2 if 2 in (first_rc, second_rc) else 0)
    if first_rc == 1:
        assert not (tmp_path / "live" / ".locks").exists(), "deferred maintenance cannot become a writer"
