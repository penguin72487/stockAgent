from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import subprocess

from scripts.check_finlab_refresh_due import refresh_due
from stockagent.data.finlab_acquisition_contract import WORKLOAD_CONTRACT_VERSION

NOW = datetime(2026, 10, 4, 3, tzinfo=UTC)  # Sunday, not the account reset slot.


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def setup(root, *, pending=False, retry=None, remaining=0):
    write(root / "artifacts/live/finlab/workload_latest.json", {
        "contract_version": WORKLOAD_CONTRACT_VERSION, "state": "available", "general_keys": 1,
        "generated_at_utc": NOW.isoformat(), "datasets": [{"key": "price:a", "needs_refresh": pending,
            "retry_at_utc": retry, "next_source_check_at_utc": (NOW+timedelta(hours=1)).isoformat()}]})
    write(root / "artifacts/live/finlab/quota_latest.json", {"observed_at_utc": NOW.isoformat(), "remaining_mb": remaining})


def test_minute_dispatch_does_not_start_a_heavy_idle_sweep(tmp_path):
    setup(tmp_path)
    assert refresh_due(tmp_path, now=NOW) == (False, "nothing_actionable_now")


def test_verified_sdk_due_update_bypasses_local_history_reserve(tmp_path):
    setup(tmp_path, pending=True, remaining=0)
    assert refresh_due(tmp_path, now=NOW) == (True, "general_work_due")


def test_retry_clock_admits_only_after_real_backoff(tmp_path):
    setup(tmp_path, pending=True, retry=(NOW+timedelta(minutes=1)).isoformat())
    assert not refresh_due(tmp_path, now=NOW)[0]
    assert refresh_due(tmp_path, now=NOW+timedelta(minutes=1)) == (True, "general_work_due")


def test_reset_does_not_wait_for_trading_day_or_old_quota_snapshot(tmp_path):
    setup(tmp_path)
    assert refresh_due(tmp_path, now=NOW.replace(hour=0))[1] == "reset_confirmation_window"


def test_uncertain_receipts_fall_through_to_recover_not_fake_idle(tmp_path):
    assert refresh_due(tmp_path, now=NOW) == (True, "workload_unverified_recovery")
    setup(tmp_path)
    assert refresh_due(tmp_path, now=NOW+timedelta(minutes=4))[0]


def test_tick_only_uses_proven_residual_capacity(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": True, "scheduled_reserve_mb": 500})
    assert refresh_due(tmp_path, now=NOW) == (True, "residual_tick_capacity")
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": False, "scheduled_reserve_mb": 500})
    assert not refresh_due(tmp_path, now=NOW)[0]


def test_exhausted_frontier_is_not_rescanned_every_minute(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": True})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "state": "frontiers_scanned; retries_due_next_cycle", "observed_at_utc": NOW.isoformat()})
    assert not refresh_due(tmp_path, now=NOW)[0]


def test_deployed_timer_contract_has_real_runtime_entrypoint():
    root = Path(__file__).resolve().parents[1]
    service = (root / "deploy/systemd/stockagent-finlab-local-refresh.service.in").read_text()
    timer = (root / "deploy/systemd/stockagent-finlab-local-refresh.timer.in").read_text()
    assert 'ExecCondition=/usr/bin/bash "@REPO_ROOT@/scripts/run_finlab_refresh_due.sh"' in service
    assert "OnUnitInactiveSec=1min" in timer
    subprocess.run(["bash", "-n", str(root / "scripts/run_finlab_refresh_due.sh")], check=True)
