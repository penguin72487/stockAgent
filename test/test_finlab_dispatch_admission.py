from datetime import UTC, datetime, timedelta
import fcntl
import json
from pathlib import Path
import os
import shutil
import sqlite3
import subprocess
from types import SimpleNamespace

import pytest

from scripts.check_finlab_refresh_due import refresh_due
from scripts import check_finlab_refresh_due as dispatch
from stockagent.data.finlab_acquisition_contract import WORKLOAD_CONTRACT_VERSION
from stockagent.data.finlab_acquisition_contract import intraday_progress, process_owner, process_owner_alive
from stockagent.data.finlab_quota_policy import load_policy

NOW = datetime(2026, 10, 4, 3, tzinfo=UTC)  # Sunday, not the account reset slot.


@pytest.mark.parametrize("free,allowed", [(0, False), (25 * 1024**3 - 1, False),
                                           (25 * 1024**3, True), (26 * 1024**3, True)])
def test_intraday_disk_preflight_uses_collector_floor_without_import_or_scan(tmp_path, monkeypatch, free, allowed):
    calls = []
    def usage(path):
        calls.append(path)
        return SimpleNamespace(free=free)
    monkeypatch.setattr(dispatch.shutil, "disk_usage", usage)
    monkeypatch.setattr(dispatch, "refresh_due", lambda *_a, **_kw: pytest.fail("disk preflight used SDK admission"))
    result = dispatch.intraday_disk_admission(tmp_path)
    assert result["allowed"] is allowed
    assert result["minimum_free_bytes"] == 25 * 1024**3
    assert result["provider_calls"] == 0 and calls == [tmp_path / "data_finlab"]


def test_intraday_disk_probe_failure_is_not_fake_completion(tmp_path, monkeypatch, capsys):
    def fail(_path):
        raise OSError("unavailable mount")
    monkeypatch.setattr(dispatch.shutil, "disk_usage", fail)
    assert dispatch.main(["--root", str(tmp_path), "--intraday-disk-only"]) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == "intraday_disk_probe_failed"


@pytest.mark.parametrize("lock_mode", [fcntl.LOCK_SH, fcntl.LOCK_EX])
def test_two_real_resource_blocks_skip_idle_sweep_but_resume_immediately(tmp_path, monkeypatch, lock_mode):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"minute_residual_allowed": True})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "state": "disk_reserve_reached", "acquisition_mode": "minute_first_tick_residual",
        "by_kind": {"tw_minute": {"remaining_partitions": 1}}})
    public = tmp_path / "data_tw_public"
    public.mkdir()
    lock = tmp_path / ".locks/tw-public-refresh.lock"
    lock.parent.mkdir()
    monkeypatch.setattr(dispatch.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1))
    with lock.open("a+") as owner:
        fcntl.flock(owner, lock_mode | fcntl.LOCK_NB)
        assert refresh_due(tmp_path, now=NOW) == (False, "intraday_disk_full_research_owner_busy")
        monkeypatch.setattr(dispatch.shutil, "disk_usage", lambda _path: SimpleNamespace(free=26 * 1024**3))
        assert refresh_due(tmp_path, now=NOW) == (True, "minute_first_residual_capacity")
        monkeypatch.setattr(dispatch.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1))
        setup(tmp_path, pending=True, remaining=1000)
        assert refresh_due(tmp_path, now=NOW) == (True, "general_work_due")
        setup(tmp_path, remaining=1000)
        assert refresh_due(tmp_path, now=NOW.replace(hour=0)) == (True, "reset_confirmation_window")
    assert dispatch.research_owner_busy(tmp_path) is False
    # Releasing the real owner permits the independent research branch even
    # while minutes remain disk-blocked. No stale progress file can hold it.
    assert refresh_due(tmp_path, now=NOW) == (True, "minute_first_residual_capacity")


def test_unknown_or_redirected_research_lock_is_not_skip_evidence(tmp_path):
    public = tmp_path / "data_tw_public"
    public.mkdir()
    assert dispatch.research_owner_busy(tmp_path) is None
    lock = tmp_path / ".locks/tw-public-refresh.lock"
    lock.parent.mkdir()
    elsewhere = tmp_path / "unrelated-lock"
    elsewhere.touch()
    lock.symlink_to(elsewhere)
    assert dispatch.research_owner_busy(tmp_path) is None


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


def test_shared_intraday_cooldown_skips_pool_but_not_due_general_data(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/intraday/source_backoff.json", {
        "retry_at_utc": (NOW+timedelta(minutes=15)).isoformat()})
    assert refresh_due(tmp_path, now=NOW) == (False, "intraday_source_cooldown")
    setup(tmp_path, pending=True, remaining=1000)
    assert refresh_due(tmp_path, now=NOW) == (True, "general_work_due")


def test_expired_or_invalid_intraday_cooldown_does_not_suppress_recovery(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": True})
    for at in ("invalid", (NOW-timedelta(seconds=1)).isoformat()):
        write(tmp_path / "data_finlab/intraday/source_backoff.json", {"retry_at_utc": at})
        assert refresh_due(tmp_path, now=NOW) == (True, "residual_tick_capacity")


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


def test_minute_work_is_admitted_even_when_tick_backlog_is_zero(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": True, "scheduled_reserve_mb": 50})
    write(tmp_path / "data_finlab/intraday/market_status.json", {"acquisition_mode": "minute_first_tick_residual",
        "by_kind": {"tw_tick": {"remaining_partitions": 0}, "tw_minute": {"remaining_partitions": 100}}})
    assert refresh_due(tmp_path, now=NOW) == (True, "minute_first_residual_capacity")


def test_minute_dispatch_uses_50mb_not_tick_scheduled_reserve(tmp_path):
    setup(tmp_path, remaining=60)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {
        "minute_residual_allowed": True, "supplemental_allowed": False, "scheduled_reserve_mb": 800})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "acquisition_mode": "minute_first_tick_residual", "by_kind": {"tw_minute": {"remaining_partitions": 1}}})
    assert refresh_due(tmp_path, now=NOW) == (True, "minute_first_residual_capacity")
    write(tmp_path / "artifacts/live/finlab/quota_latest.json", {"observed_at_utc": NOW.isoformat(), "remaining_mb": 50})
    assert refresh_due(tmp_path, now=NOW) == (False, "minute_quota_floor_reached")


def test_pre_reset_dispatch_drain_runs_even_on_closed_market_and_under_50mb(tmp_path):
    before_reset = NOW.replace(hour=23)
    setup(tmp_path, remaining=20)
    workload = tmp_path / "artifacts/live/finlab/workload_latest.json"
    value = json.loads(workload.read_text())
    value["generated_at_utc"] = before_reset.isoformat()
    value["datasets"][0]["next_source_check_at_utc"] = (before_reset+timedelta(hours=2)).isoformat()
    write(workload, value)
    write(tmp_path / "artifacts/live/finlab/quota_latest.json", {
        "observed_at_utc": before_reset.isoformat(), "remaining_mb": 20})
    write(tmp_path / "data_finlab/core_acquisition_status.json", {
        "minute_residual_allowed": True, "supplemental_allowed": True, "scheduled_reserve_mb": 800})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "acquisition_mode": "minute_first_tick_residual", "by_kind": {"tw_minute": {"remaining_partitions": 1}}})
    assert refresh_due(tmp_path, now=before_reset) == (True, "minute_pre_reset_drain")
    write(tmp_path / "artifacts/live/finlab/quota_latest.json", {
        "observed_at_utc": before_reset.isoformat(), "remaining_mb": 0})
    assert refresh_due(tmp_path, now=before_reset) == (False, "minute_quota_floor_reached")


def test_known_minute_source_wait_does_not_restart_heavy_sweep_every_minute(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": False,
        "minute_residual_allowed": True, "scheduled_reserve_mb": 50})
    write(tmp_path / "data_finlab/intraday/market_status.json", {"acquisition_mode": "minute_first_tick_residual",
        "minute_priority": {"pending_known_window_partitions": 0, "due_publication_rechecks": 0,
                            "next_source_check_at_utc": (NOW+timedelta(hours=2)).isoformat()}})
    assert refresh_due(tmp_path, now=NOW) == (False, "minute_source_wait_tick_general_debt")
    assert refresh_due(tmp_path, now=NOW+timedelta(minutes=1))[0] is False


def test_finite_futures_priority_wakes_owner_without_bypassing_gates(tmp_path):
    from downloader.provider_priority import ensure_schema, task_key
    from scripts.download_finlab_intraday import _partition_paths

    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {
        "supplemental_allowed": False, "minute_residual_allowed": True})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "acquisition_mode": "minute_first_tick_residual",
        "minute_priority": {"pending_known_window_partitions": 0, "due_publication_rechecks": 0,
                            "next_source_check_at_utc": (NOW + timedelta(hours=2)).isoformat()}})
    index = tmp_path / "data_finlab/intraday/market_index.sqlite3"
    item = {"dataset": "tw_futures_minute:TX_202609", "partition": "2026-09-30"}
    with sqlite3.connect(index) as con:
        ensure_schema(con)
        con.execute("INSERT INTO provider_query_intents VALUES (?,?,?,?)", (
            "request", task_key(item), (NOW - timedelta(minutes=1)).isoformat(),
            (NOW + timedelta(hours=1)).isoformat()))
    assert refresh_due(tmp_path, now=NOW) == (True, "futures_priority_due")

    # General updates and provider cooldown retain their original precedence.
    setup(tmp_path, pending=True, remaining=1000)
    assert refresh_due(tmp_path, now=NOW) == (True, "general_work_due")
    setup(tmp_path, remaining=1000)
    backoff = tmp_path / "data_finlab/intraday/source_backoff.json"
    write(backoff, {"retry_at_utc": (NOW + timedelta(minutes=1)).isoformat()})
    assert refresh_due(tmp_path, now=NOW) == (False, "intraday_source_cooldown")
    backoff.unlink()
    setup(tmp_path, remaining=0)
    assert refresh_due(tmp_path, now=NOW)[0] is False
    setup(tmp_path, remaining=1000)

    # A not-ready attempt waits for the owner's next real retry cycle.
    _, attempt = _partition_paths(tmp_path / "data_finlab", item["dataset"],
                                  datetime.fromisoformat(item["partition"]).date())
    write(attempt, {"status": "partition_not_ready", "attempted_at_utc": NOW.isoformat()})
    assert refresh_due(tmp_path, now=NOW) == (False, "minute_source_wait_tick_general_debt")


def test_finite_stock_tick_priority_wakes_only_with_room_and_nonactionable_general_work(tmp_path):
    from downloader.provider_priority import ensure_schema, task_key
    from scripts.download_finlab_intraday import _partition_paths

    setup(tmp_path, remaining=1000)
    core = tmp_path / 'data_finlab/core_acquisition_status.json'
    write(core, {'supplemental_allowed': False, 'minute_residual_allowed': True,
                 'actionable_pending': 0, 'scheduled_reserve_mb': 500})
    write(tmp_path / 'data_finlab/intraday/market_status.json', {
        'acquisition_mode': 'minute_first_tick_residual',
        'minute_priority': {'pending_known_window_partitions': 0, 'due_publication_rechecks': 0,
                            'next_source_check_at_utc': (NOW + timedelta(hours=2)).isoformat()}})
    index = tmp_path / 'data_finlab/intraday/market_index.sqlite3'
    item = {'dataset': 'tw_tick:5203', 'partition': '2011-10-03'}
    with sqlite3.connect(index) as con:
        ensure_schema(con)
        con.execute('INSERT INTO provider_query_intents VALUES (?,?,?,?)', (
            'request', task_key(item), (NOW-timedelta(minutes=1)).isoformat(),
            (NOW+timedelta(hours=1)).isoformat()))
    assert refresh_due(tmp_path, now=NOW) == (True, 'stock_tick_priority_due')
    write(core, {'supplemental_allowed': False, 'minute_residual_allowed': True,
                 'actionable_pending': 1, 'scheduled_reserve_mb': 500})
    assert refresh_due(tmp_path, now=NOW) == (False, 'minute_source_wait_tick_general_debt')
    write(core, {'supplemental_allowed': False, 'minute_residual_allowed': True,
                 'actionable_pending': 0, 'scheduled_reserve_mb': 500})
    _, attempt = _partition_paths(tmp_path / 'data_finlab', item['dataset'],
                                  datetime.fromisoformat(item['partition']).date())
    write(attempt, {'status': 'partition_not_ready', 'attempted_at_utc': NOW.isoformat()})
    assert refresh_due(tmp_path, now=NOW) == (False, 'minute_source_wait_tick_general_debt')


def test_stock_scope_wait_is_local_bounded_and_invalidated_by_source_or_cycle(tmp_path, monkeypatch):
    from downloader.provider_priority import ensure_schema, task_key
    from scripts.download_finlab_intraday import _priority_scope_path
    from datetime import date

    index = tmp_path / 'data_finlab/intraday/market_index.sqlite3'
    index.parent.mkdir(parents=True)
    item = {'dataset': 'tw_tick:5203', 'partition': '2011-10-03'}
    with sqlite3.connect(index) as con:
        ensure_schema(con)
        con.execute('INSERT INTO provider_query_intents VALUES (?,?,?,?)', (
            'request', task_key(item), (NOW-timedelta(minutes=1)).isoformat(),
            (NOW+timedelta(days=2)).isoformat()))
    identity = ['scope-version-1']
    monkeypatch.setattr(dispatch, 'intraday_source_identity', lambda root: identity[0])
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is True
    scope_path = _priority_scope_path(tmp_path / 'data_finlab', item['dataset'], date(2011, 10, 3))
    scope = {'contract': 'finlab_stock_priority_local_scope_v1', 'dataset': item['dataset'],
             'trade_date': item['partition'], 'checked_at_utc': NOW.isoformat(),
             'source_identity': identity[0], 'reason': 'symbol_ineligible',
             'provider_calls': 0, 'source_no_trade_claim': False}
    write(scope_path, scope)
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is False
    identity[0] = 'scope-version-2'
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is True
    identity[0] = None
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is True
    identity[0] = 'scope-version-1'
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW + timedelta(days=1), stock_kind='tw_tick') is True
    scope['checked_at_utc'] = (NOW + timedelta(minutes=1)).isoformat()
    write(scope_path, scope)
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is True
    write(scope_path, [])
    assert dispatch.actionable_futures_priority(tmp_path, now=NOW, stock_kind='tw_tick') is True


def test_unknown_futures_index_does_not_claim_idle(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"minute_residual_allowed": True})
    index = tmp_path / "data_finlab/intraday/market_index.sqlite3"
    index.parent.mkdir(parents=True, exist_ok=True)
    index.write_text("not a sqlite database")
    assert refresh_due(tmp_path, now=NOW) == (True, "futures_priority_unverified_recovery")


def test_exhausted_frontier_is_not_rescanned_every_minute(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {"supplemental_allowed": True})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "state": "frontiers_scanned; retries_due_next_cycle", "observed_at_utc": NOW.isoformat()})
    assert not refresh_due(tmp_path, now=NOW)[0]


def source_wait_setup(root, monkeypatch):
    setup(root, remaining=1000)
    write(root / "data_finlab/core_acquisition_status.json", {
        "minute_residual_allowed": True, "supplemental_allowed": True})
    wait = {"actionable_known_window_partitions": 0, "due_publication_rechecks": 0,
            "next_source_check_at_utc": (NOW + timedelta(hours=2)).isoformat()}
    value = {"acquisition_mode": "minute_first_tick_residual", "state": "waiting_older_publication",
        "active_kind": "tw_tick", "observed_at_utc": NOW.isoformat(),
        "quota_policy": {"configuration_fingerprint": load_policy(root / "configs/finlab_quota_policy.json").fingerprint},
        "minute_priority": wait, "tick_priority": dict(wait), "dispatch_inputs_sha256": "current-inputs"}
    write(root / "data_finlab/intraday/market_status.json", value)
    write(root / "data_finlab/catalog/discovery.json", {"keys": ["price:a"], "observed_at_utc": NOW.isoformat()})
    monkeypatch.setattr(dispatch, "intraday_source_identity", lambda *_a, **_kw: "current-inputs")
    return value


def test_both_sources_wait_without_restarting_full_inventory_and_resume_at_real_deadline(tmp_path, monkeypatch):
    value = source_wait_setup(tmp_path, monkeypatch)
    assert refresh_due(tmp_path, now=NOW) == (False, "intraday_waiting_source_publication")
    assert refresh_due(tmp_path, now=NOW + timedelta(minutes=1))[0] is False
    # Independent general releases and reset never wait for intraday supply.
    setup(tmp_path, pending=True, remaining=1000)
    assert refresh_due(tmp_path, now=NOW) == (True, "general_work_due")
    setup(tmp_path, remaining=1000)
    value["tick_priority"]["next_source_check_at_utc"] = NOW.isoformat()
    write(tmp_path / "data_finlab/intraday/market_status.json", value)
    assert refresh_due(tmp_path, now=NOW) == (True, "residual_tick_capacity")
    assert refresh_due(tmp_path, now=NOW.replace(hour=0))[1] == "reset_confirmation_window"


@pytest.mark.parametrize("change", ["inputs", "unknown_inputs", "policy", "pending", "due", "stale_cycle",
                                    "future", "running", "minute_run", "missing_priority"])
def test_unknown_changed_or_actionable_tick_evidence_never_suppresses_recovery(tmp_path, monkeypatch, change):
    value = source_wait_setup(tmp_path, monkeypatch)
    if change == "inputs": value["dispatch_inputs_sha256"] = "old-inputs"
    elif change == "unknown_inputs": monkeypatch.setattr(dispatch, "intraday_source_identity", lambda *_a: None)
    elif change == "policy": value["quota_policy"] = None
    elif change == "pending": value["tick_priority"]["actionable_known_window_partitions"] = 1
    elif change == "due": value["tick_priority"]["due_publication_rechecks"] = 1
    elif change == "stale_cycle": value["observed_at_utc"] = (NOW - timedelta(days=1)).isoformat()
    elif change == "future": value["observed_at_utc"] = (NOW + timedelta(seconds=1)).isoformat()
    elif change == "running": value["state"] = "running"
    elif change == "minute_run": value["active_kind"] = "tw_minute"
    else: value.pop("tick_priority")
    write(tmp_path / "data_finlab/intraday/market_status.json", value)
    assert refresh_due(tmp_path, now=NOW) == (True, "residual_tick_capacity")


def test_idle_intraday_still_checks_hourly_provider_catalog(tmp_path, monkeypatch):
    source_wait_setup(tmp_path, monkeypatch)
    write(tmp_path / "data_finlab/catalog/discovery.json", {
        "keys": ["price:a"], "observed_at_utc": (NOW-timedelta(hours=1)).isoformat()})
    assert refresh_due(tmp_path, now=NOW) == (True, "catalog_refresh_due")


def test_dispatch_source_identity_detects_replacements_new_symbols_and_missing_inputs(tmp_path):
    public = tmp_path / "data_tw_public"
    paths = [public / name for name in ("stocks/symbols.csv", "twse_market_index.parquet",
        "twse_listed_company_basic.parquet", "tpex_basic_company.parquet", "twse_delisted_company.parquet",
        "tpex_delisted_company.parquet", "stocks/2330_features.parquet")]
    paths += [tmp_path / name for name in ("scripts/download_finlab_market_intraday.py",
        "scripts/check_finlab_refresh_due.py", "stockagent/data/finlab_quota_policy.py")]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("metadata fixture, not valid Parquet")
    first = dispatch.intraday_source_identity(tmp_path)
    assert first and dispatch.intraday_source_identity(tmp_path) == first
    new = public / "stocks/new_features.parquet"
    new.write_text("new source")
    assert dispatch.intraday_source_identity(tmp_path) != first
    before = dispatch.intraday_source_identity(tmp_path)
    staged = new.with_suffix(".tmp")
    staged.write_bytes(new.read_bytes())
    staged.replace(new)
    assert dispatch.intraday_source_identity(tmp_path) != before
    paths[0].unlink()
    assert dispatch.intraday_source_identity(tmp_path) is None


def test_deployed_timer_contract_has_real_runtime_entrypoint():
    root = Path(__file__).resolve().parents[1]
    service = (root / "deploy/systemd/stockagent-finlab-local-refresh.service.in").read_text()
    timer = (root / "deploy/systemd/stockagent-finlab-local-refresh.timer.in").read_text()
    assert 'ExecCondition=/usr/bin/bash "@REPO_ROOT@/scripts/run_finlab_refresh_due.sh"' in service
    assert "OnUnitInactiveSec=1min" in timer
    assert "OnActiveSec=1min" in timer  # A previous boot's deactivation cannot seed retries.
    subprocess.run(["bash", "-n", str(root / "scripts/run_finlab_refresh_due.sh")], check=True)


def test_timer_only_installer_does_not_replace_or_restart_workers(tmp_path):
    """Exercise the real installer with privileged mutations replaced by logs."""
    root = Path(__file__).resolve().parents[1]
    isolated = tmp_path / "repo"
    scripts = isolated / "scripts"
    templates = isolated / "deploy/systemd"
    scripts.mkdir(parents=True)
    templates.mkdir(parents=True)
    for name in ("install_registered_data_refresh_services.sh", "run_finlab_refresh.sh",
                 "run_finlab_refresh_frozen.sh"):
        shutil.copyfile(root / "scripts" / name, scripts / name)
    timer = "stockagent-finlab-local-refresh.timer"
    shutil.copyfile(root / "deploy/systemd" / f"{timer}.in", templates / f"{timer}.in")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    command_log = tmp_path / "commands.log"
    for command in ("systemctl", "systemd-analyze", "install"):
        fake = bin_dir / command
        fake.write_text(
            '#!/bin/bash\n'
            'printf "%s %s\\n" "${0##*/}" "$*" >> "$FINLAB_TEST_COMMAND_LOG"\n'
            'if [[ "${0##*/}" == "systemctl" && "${1:-}" == "show" ]]; then echo waiting; fi\n'
        )
        fake.chmod(0o755)
    result = subprocess.run(
        ["bash", str(scripts / "install_registered_data_refresh_services.sh"), "finlab-timer-only"],
        env={**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
             "FINLAB_TEST_COMMAND_LOG": str(command_log)},
        text=True, capture_output=True,
    )
    # The installer intentionally retains its existing root requirement.
    if os.geteuid() != 0:
        assert result.returncode == 2 and not command_log.exists()
        return
    assert result.returncode == 0, result.stderr
    commands = command_log.read_text().splitlines()
    assert [row for row in commands if row.startswith("systemctl ")] == [
        "systemctl daemon-reload", f"systemctl enable {timer}",
        f"systemctl restart {timer}", f"systemctl show {timer} -p SubState --value",
    ]
    installed = [row for row in commands if row.startswith("install ")]
    assert len(installed) == 1 and timer in installed[0]
    assert ".service" not in installed[0] and "quota-snapshot" not in "\n".join(commands)
    assert "worker and other units left untouched" in result.stdout


def test_dead_owner_and_overrun_are_not_fake_running_or_eta(tmp_path):
    progress = {"contract_version": 1, "state": "running", "owner": process_owner(),
                "observed_at_utc": (NOW-timedelta(seconds=10)).isoformat(), "active_key": "tw_tick:2330",
                "estimated_batch_finish_at_utc": (NOW-timedelta(seconds=1)).isoformat()}
    write(tmp_path / "intraday/active_run.json", progress)
    live = intraday_progress(tmp_path, now=NOW)
    assert live["owner_alive"] and live["state"] == "running" and live["batch_eta_state"] == "sample_overrun"
    assert live["estimated_batch_finish_at_utc"] is None
    write(tmp_path / "intraday/active_run.json", {**progress, "owner": {**progress["owner"], "start_ticks": "wrong-pid-generation"}})
    dead = intraday_progress(tmp_path, now=NOW)
    assert dead["state"] == "interrupted" and not dead["owner_alive"] and dead["active_key"] is None


def test_proc_subset_hides_boot_uuid_not_the_live_pid_generation(monkeypatch):
    owner = process_owner()
    real_read = Path.read_text

    def sandbox_read(path, *args, **kwargs):
        if str(path) == "/proc/sys/kernel/random/boot_id":
            raise FileNotFoundError("ProcSubset=pid")
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", sandbox_read)
    assert process_owner_alive(owner, started_at_utc=NOW.isoformat(), now=NOW)
    assert not process_owner_alive(owner, now=NOW)  # no boot/run proof
    assert not process_owner_alive(owner, started_at_utc=(NOW-timedelta(days=365)).isoformat(), now=NOW)
    assert not process_owner_alive(owner, started_at_utc=(NOW+timedelta(seconds=1)).isoformat(), now=NOW)
    assert not process_owner_alive({**owner, "start_ticks": "another-generation"},
                                   started_at_utc=NOW.isoformat(), now=NOW)


def test_checkpoint_during_projection_is_not_discarded_as_future(tmp_path):
    progress = {"contract_version": 1, "state": "running", "owner": process_owner(),
                "run_started_at_utc": (NOW-timedelta(minutes=1)).isoformat(),
                "observed_at_utc": (NOW+timedelta(seconds=3)).isoformat()}
    write(tmp_path / "intraday/active_run.json", progress)
    assert intraday_progress(tmp_path, now=NOW)["owner_alive"]
    assert intraday_progress(tmp_path, now=NOW)["age_seconds"] == 0
    write(tmp_path / "intraday/active_run.json", {**progress, "observed_at_utc": (NOW+timedelta(seconds=6)).isoformat()})
    assert intraday_progress(tmp_path, now=NOW) == {}
