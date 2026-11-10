from __future__ import annotations

from datetime import datetime, timedelta
import fcntl
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import run_discord_artifact_maintenance as runner
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending

_REAL_WAIT_FOR_TW_PUBLIC_REFRESH = runner._wait_for_tw_public_refresh


@pytest.fixture
def isolated_worker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    status_path = tmp_path / "artifact_backfill_status.json"
    emissions: list[dict] = []
    bot = runner._load_discord_bot()
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH", str(status_path))
    # Retry lifecycle tests do not inherit the node's live deployment policy.
    # The enabled-year admission contract has its own explicit fixtures/tests.
    monkeypatch.setenv("STOCKAGENT_FORMAL_HISTORY_SCOPE_POLICY", str(tmp_path / "absent-scope.json"))
    monkeypatch.setattr(bot, "_rotate_error_log_if_needed", lambda: None)
    monkeypatch.setattr(bot, "_opening_critical_work_pending", lambda: False)
    monkeypatch.setattr(bot, "_interactive_signal_work_pending", lambda: False)
    monkeypatch.setattr(runner, "_wait_for_tw_public_refresh", lambda **_kw: True)
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", lambda _, **_kw: (0, 0, 0))
    monkeypatch.setattr(bot, "_artifact_maintenance_markets", lambda: ["market"])
    monkeypatch.setattr(
        bot,
        "_resolve_market",
        lambda market: SimpleNamespace(
            market=market, timezone="Asia/Taipei", day_trade_simulation_enabled=False
        ),
    )
    monkeypatch.setattr(bot, "_artifact_backfill_key", lambda cfg, _: cfg.market)
    monkeypatch.setattr(bot, "_market_has_model", lambda _: True)
    monkeypatch.setattr(bot, "_reconcile_artifact_backfill_if_current", lambda *a, **kw: False)
    monkeypatch.setattr(
        bot,
        "_run_artifact_backfill_sync",
        lambda _: pytest.fail("deferred work must not bypass retry backoff"),
    )
    monkeypatch.setattr(runner, "_emit", lambda **row: emissions.append(row))
    return status_path, emissions


def _pending_job(state: str) -> dict:
    return {
        "key": "market",
        "market": "market",
        "status": state,
        "run_id": runner.discord_bot._BOT_RUN_ID,
        "attempt": 4,
        "next_retry_at": (
            datetime.now().astimezone() + timedelta(hours=1)
        ).isoformat(),
        "error_type": "FileNotFoundError" if state == "failed" else None,
        "error_message": "retained source is missing" if state == "failed" else None,
    }


@pytest.mark.parametrize("cache_only", [True, False])
def test_source_writer_race_waits_without_failure_or_false_completion(
    isolated_worker, monkeypatch, tmp_path, cache_only,
):
    status_path, emissions = isolated_worker
    def updating(_markets, **_kwargs):
        raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "tw-public-refresh.lock")
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", updating)
    assert runner.run_once(signal_cache_only=cache_only) == 0
    status = json.loads(status_path.read_text())
    worker = status["signal_cache_maintenance_run" if cache_only else "maintenance_run"]
    assert worker["status"] == "waiting_source"
    assert worker["reason"] == "tw_public_refresh_in_progress"
    assert emissions[-1]["status"] == "waiting_source"
    assert emissions[-1]["producer_lock_held"] and emissions[-1]["retryable"]
    assert not any(row.get("status") in {"signal_cache_complete", "complete", "degraded"} for row in emissions)


@pytest.mark.parametrize("cache_only", [False, True])
def test_only_the_full_isolated_worker_can_request_cold_source_warming(isolated_worker, monkeypatch, cache_only):
    status_path, _emissions = isolated_worker
    # Full maintenance has a subsequent historical-backfill phase. Keep it
    # under its real future retry deadline while measuring warming ownership.
    status_path.write_text(json.dumps({"schema_version": 2,
                                      "jobs": {"market": _pending_job("failed")}}))
    observed = []
    def caches(_markets, *, allow_source_warming):
        observed.append(allow_source_warming)
        return 0, 0, 0
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", caches)
    assert runner.run_once(signal_cache_only=cache_only) == 0
    assert observed == [not cache_only]


def test_capacity_wait_preserves_error_evidence_without_false_inference_failure(isolated_worker, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, emissions = isolated_worker
    def unavailable(_):
        raise CapacityDeferred("physical_backing_reserve", physical_free_bytes=10, required_bytes=100)
    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", unavailable)
    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    job = status["jobs"]["market"]
    assert job["status"] == "waiting_source" and job["error_type"] == "CapacityDeferred"
    assert job["error_details"]["reason"] == "physical_backing_reserve"
    assert job["error_details"]["allocation_admitted"] is False
    assert job.get("failure_attempt", 0) == 0 and job["next_retry_at"]
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert not any(row.get("status") in {"ready", "failed", "complete"} for row in emissions)


def test_capacity_retries_back_off_without_incrementing_model_failures(isolated_worker, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, _ = isolated_worker
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: "same-source")
    monkeypatch.setattr(runner, "_backing_capacity_hint", lambda _required: ("blocked", None))
    calls = []

    def unavailable(_):
        calls.append(1)
        raise CapacityDeferred("physical_backing_reserve", physical_free_bytes=10,
                               required_bytes_including_reserve=100 + len(calls))

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", unavailable)
    for expected in (60, 120, 240, 480, 900, 900):
        assert runner.run_once() == 0
        payload = json.loads(status_path.read_text())
        job = payload["jobs"]["market"]
        assert job["status"] == "waiting_source" and job.get("failure_attempt", 0) == 0
        assert job["retry_delay_seconds"] == expected
        assert job["capacity_wait_attempt"] == len(calls)
        assert job["capacity_workspace_was_blocked"] is True
        assert job["error_details"]["physical_free_bytes"] == 10
        job["next_retry_at"] = (datetime.now().astimezone() - timedelta(seconds=1)).isoformat()
        status_path.write_text(json.dumps(payload))
    assert len(calls) == 6


@pytest.mark.parametrize("trigger", ["capacity_recovered", "source_changed"])
def test_capacity_wait_can_recover_early_only_through_canonical_worker(isolated_worker, monkeypatch, trigger):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, _ = isolated_worker
    source = {"hint": "first"}
    capacity = {"value": ("blocked", None)}
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: source["hint"])
    monkeypatch.setattr(runner, "_backing_capacity_hint", lambda _required: capacity["value"])
    calls = []

    def infer(_):
        calls.append(1)
        if len(calls) == 1:
            raise CapacityDeferred("physical_backing_reserve", required_bytes_including_reserve=100)
        return None

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", infer)
    assert runner.run_once() == 0
    job = json.loads(status_path.read_text())["jobs"]["market"]
    assert job["capacity_wait_contract"] == runner.CAPACITY_WAIT_CONTRACT
    assert datetime.fromisoformat(job["next_retry_at"]) > datetime.now().astimezone()
    # Still-blocked or unknown capacity with an unchanged source does not run inference.
    capacity["value"] = ("unknown", None)
    assert runner.run_once() == 0
    assert len(calls) == 1
    if trigger == "capacity_recovered":
        capacity["value"] = ("available", 100)
    else:
        source["hint"] = "new-source"
    assert runner.run_once() == 0
    assert len(calls) == 2
    assert json.loads(status_path.read_text())["jobs"]["market"]["status"] == "ready"


def test_capacity_early_hint_never_overrides_opening_priority(isolated_worker, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, _ = isolated_worker
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: "same")
    capacity = {"value": ("blocked", None)}
    monkeypatch.setattr(runner, "_backing_capacity_hint", lambda _: capacity["value"])
    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", lambda _: (
        (_ for _ in ()).throw(CapacityDeferred("physical_backing_reserve", required_bytes_including_reserve=100))))
    assert runner.run_once() == 0
    before = json.loads(status_path.read_text())["jobs"]["market"]
    capacity["value"] = ("available", 100)
    monkeypatch.setattr(runner.discord_bot, "_opening_critical_work_pending", lambda: True)
    assert runner.run_once() == 0
    assert json.loads(status_path.read_text())["jobs"]["market"] == before


def test_capacity_recovery_observes_one_sample_for_distinct_thresholds(monkeypatch):
    now = datetime.now().astimezone()
    deadline = (now + timedelta(minutes=10)).isoformat()
    jobs = {}
    for key, required in (("a", 100), ("b", 150), ("c", 200)):
        jobs[key] = {"key": key, "status": "waiting_source", "error_type": "CapacityDeferred",
                     "next_retry_at": deadline, "capacity_wait_contract": runner.CAPACITY_WAIT_CONTRACT,
                     "capacity_workspace_was_blocked": True, "capacity_source_hint": "same",
                     "error_details": {"reason": "physical_backing_reserve",
                                       "required_bytes_including_reserve": required}}
    calls = []
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: "same")
    monkeypatch.setattr(runner, "_backing_capacity_hint",
                        lambda required: (calls.append(required) or ("available", 150)))
    assert set(runner._capacity_recovered_jobs(jobs, observed=now)) == {"a", "b"}
    assert calls == [100]


def test_capacity_wait_survives_an_unavailable_source_hint(isolated_worker, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, _ = isolated_worker
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: None)
    capacity = {"value": ("blocked", None)}
    monkeypatch.setattr(runner, "_backing_capacity_hint", lambda _: capacity["value"])
    calls = []

    def infer(_):
        calls.append(1)
        if len(calls) == 1:
            raise CapacityDeferred("physical_backing_reserve", required_bytes_including_reserve=100)
        return None

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", infer)
    assert runner.run_once() == 0
    assert json.loads(status_path.read_text())["jobs"]["market"]["capacity_source_hint"] is None
    capacity["value"] = ("available", 100)
    assert runner.run_once() == 0
    assert len(calls) == 2


def test_capacity_schedule_does_not_overwrite_a_changed_job(isolated_worker, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

    status_path, _ = isolated_worker
    bot = runner.discord_bot
    monkeypatch.setattr(runner, "_capacity_source_hint", lambda: "same")
    bot._begin_artifact_backfill("market", "market")
    waiting = bot._finish_artifact_backfill("market", "market", status="waiting_source",
        exc=CapacityDeferred("physical_backing_reserve", required_bytes_including_reserve=100))

    def changed(_required):
        bot._finish_artifact_backfill("market", "market", status="ready")
        return "blocked", None

    monkeypatch.setattr(runner, "_backing_capacity_hint", changed)
    assert runner._schedule_capacity_wait("market", waiting, {}) == waiting
    assert json.loads(status_path.read_text())["jobs"]["market"]["status"] == "ready"


@pytest.mark.parametrize("state", ["failed", "ready", "running"])
def test_unreconciled_skip_is_not_worker_completion(isolated_worker, state: str) -> None:
    status_path, emissions = isolated_worker
    job = _pending_job(state)
    status_path.write_text(json.dumps({"schema_version": 2, "jobs": {"market": job}}))

    assert runner.run_once() == 0

    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"] == job
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert status["maintenance_run"]["reason"] == "artifact_retry_deferred"
    assert status["maintenance_run"]["attempted"] == 0
    assert status["maintenance_run"]["deferred"] == 1
    assert emissions[-1]["retry_deferred"] == 1
    assert any(row.get("reason") == "artifact_retry_deferred" for row in emissions)
    expected_health = "degraded" if state == "failed" else "running" if state == "running" else "waiting_source"
    assert runner.discord_bot._artifact_backfill_health_summary()["status"] == expected_health


def test_proven_recovery_can_complete_without_retry(isolated_worker, monkeypatch) -> None:
    status_path, emissions = isolated_worker
    status_path.write_text(json.dumps({"jobs": {"market": _pending_job("failed")}}))

    def reconciled(_cfg, *, key, market):
        runner.discord_bot._finish_artifact_backfill(key, market, status="ready")
        return True

    monkeypatch.setattr(
        runner.discord_bot, "_reconcile_artifact_backfill_if_current", reconciled
    )

    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"]["status"] == "ready"
    assert status["maintenance_run"]["status"] == "complete"
    assert status["maintenance_run"]["deferred"] == 0
    assert emissions[-1]["retry_deferred"] == 0


def test_obsolete_ready_hint_is_repaired_by_the_same_serial_owner(isolated_worker, monkeypatch):
    status_path, emissions = isolated_worker
    status_path.write_text(json.dumps({"jobs": {"market": _pending_job("ready")}}))
    calls = []

    def invalidate(_cfg, *, key, market):
        assert runner.discord_bot._invalidate_ready_artifact_backfill(key, market)
        return False

    monkeypatch.setattr(runner.discord_bot, "_reconcile_artifact_backfill_if_current", invalidate)
    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", lambda cfg: calls.append(cfg.market))
    assert runner.run_once() == 0
    assert calls == ["market"]
    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"]["status"] == "ready"
    assert status["jobs"]["market"]["attempt"] == 5
    assert status["jobs"]["market"]["prior_ready_receipt"]["status"] == "ready"
    assert status["jobs"]["market"]["prior_ready_receipt"]["attempt"] == 4
    assert status["jobs"]["market"]["ready_invalidated_by_run_id"] == runner.discord_bot._BOT_RUN_ID
    assert status["maintenance_run"]["attempted"] == 1
    assert emissions[-1]["retry_deferred"] == 0


def test_other_markets_continue_without_hiding_deferred_work(
    isolated_worker, monkeypatch, tmp_path
) -> None:
    status_path, emissions = isolated_worker
    pending = _pending_job("failed")
    status_path.write_text(json.dumps({"jobs": {"market": pending}}))
    monkeypatch.setattr(
        runner.discord_bot, "_artifact_maintenance_markets", lambda: ["market", "other"]
    )
    calls: list[str] = []

    def inferred(cfg):
        calls.append(cfg.market)
        return SimpleNamespace(summary={"panel_date": "2026-09-30"}, output_dir=tmp_path)

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", inferred)

    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert calls == ["other"]
    assert status["jobs"]["market"] == pending
    assert status["jobs"]["other"]["status"] == "ready"
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert status["maintenance_run"]["attempted"] == 1
    assert emissions[-1]["deferred"] == 1


def test_history_source_race_waits_and_other_mode_still_completes(isolated_worker, monkeypatch, tmp_path):
    status_path, emissions = isolated_worker
    monkeypatch.setattr(runner.discord_bot, "_artifact_maintenance_markets", lambda: ["market", "other"])

    def inferred(cfg):
        if cfg.market == "market":
            raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock", observed_in_child=True)
        return SimpleNamespace(summary={"panel_date": "2026-10-29"}, output_dir=tmp_path)

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", inferred)
    assert runner.run_once() == 0
    payload = json.loads(status_path.read_text())
    waiting = payload["jobs"]["market"]
    assert waiting["status"] == "waiting_source"
    assert waiting["retry_delay_seconds"] == 60.0
    assert waiting.get("failure_attempt", 0) == 0
    assert waiting["error_details"]["current_lock_state"] == "unverified"
    assert payload["jobs"]["other"]["status"] == "ready"
    assert payload["maintenance_run"]["status"] == "waiting_source"
    assert payload["maintenance_run"]["reason"] == "tw_public_refresh_observed_in_child"
    assert emissions[-1]["lock_deferred"] == 1
    assert not any(row.get("status") == "failed" for row in emissions)


def test_source_wait_does_not_increase_failure_backoff(isolated_worker, monkeypatch, tmp_path):
    status_path, _ = isolated_worker
    bot = runner.discord_bot
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_BASE_SECONDS", "60")
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_MAX_SECONDS", "120")
    bot._begin_artifact_backfill("market", "market")
    first = bot._finish_artifact_backfill("market", "market", status="failed", exc=ValueError("bad data"))
    assert first["failure_attempt"] == 1
    for _ in range(3):
        bot._begin_artifact_backfill("market", "market")
        waiting = bot._finish_artifact_backfill("market", "market", status="waiting_source",
            exc=TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock"))
        assert waiting["failure_attempt"] == 1
        assert waiting["last_failure"]["error_message"] == "bad data"
    bot._begin_artifact_backfill("market", "market")
    second = bot._finish_artifact_backfill("market", "market", status="failed", exc=ValueError("still bad"))
    assert second["failure_attempt"] == 2 and second["retry_delay_seconds"] == 120.0
    assert json.loads(status_path.read_text())["jobs"]["market"]["status"] == "failed"


def test_running_factorized_source_does_not_spend_attempts_or_block_other_markets(
    isolated_worker, monkeypatch, tmp_path,
) -> None:
    status_path, emissions = isolated_worker
    pending = _pending_job("failed")
    status_path.write_text(json.dumps({"jobs": {"market": pending}}))
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner.discord_bot, "_artifact_maintenance_markets", lambda: ["market", "other"])
    monkeypatch.setattr(runner.discord_bot, "_resolve_market", lambda market: SimpleNamespace(
        market=market, timezone="Asia/Taipei", day_trade_simulation_enabled=False,
        completed_session_command=(("scripts/update_tw_day_trade_factorized_live.py", "--phase", "completed")
                                   if market == "market" else ())))
    calls = []
    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync",
                        lambda cfg: calls.append(cfg.market))
    lock = runner._factorized_source_updater_receipt().with_name("prepare.lock")
    lock.parent.mkdir(parents=True)
    with lock.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert calls == ["other"]
    assert status["jobs"]["market"] == pending
    assert status["jobs"]["other"]["status"] == "ready"
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert status["maintenance_run"]["attempted"] == 1
    assert status["maintenance_run"]["deferred"] == 1
    assert any(row.get("reason") == "factorized_projection_in_progress" for row in emissions)
    assert status["maintenance_run"]["reason"] == "factorized_projection_in_progress"
    assert emissions[-1]["source_deferred"] == 1


@pytest.mark.parametrize("cache_only", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_close_cache_outcome_is_not_hidden_by_history_retry_or_completion(
    isolated_worker, monkeypatch, cache_only, failed,
):
    status_path, emissions = isolated_worker
    pending = _pending_job("failed")
    status_path.write_text(json.dumps({"jobs": {"market": pending}}))
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches",
                        lambda _, **_kw: (2, int(failed), 1))

    assert runner.run_once(signal_cache_only=cache_only) == int(failed)

    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"] == pending  # No extra artifact attempt.
    worker = status["signal_cache_maintenance_run" if cache_only else "maintenance_run"]
    assert worker["status"] == ("degraded" if failed else "waiting_source")
    assert worker["failures"] == int(failed)
    assert worker["attempted"] == 2
    assert worker["deferred"] == (1 if cache_only else 2)
    if failed:
        assert worker["reason"] == "postclose_cache_failed"
    assert "complete" not in emissions[-1]["status"]


def test_independent_cache_worker_cannot_overwrite_running_history_receipt(isolated_worker, monkeypatch):
    status_path, _ = isolated_worker
    history = {"status": "running", "run_id": "history-owner", "updated_at": "2026-10-29T15:47:03+08:00"}
    status_path.write_text(json.dumps({"jobs": {}, "maintenance_run": history}))
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", lambda _, **_kw: (2, 0, 0))
    assert runner.run_once(signal_cache_only=True) == 0
    status = json.loads(status_path.read_text())
    assert status["maintenance_run"] == history
    assert status["signal_cache_maintenance_run"]["status"] == "complete"
    assert runner.discord_bot._artifact_backfill_health_summary()["status"] == "running"


def test_empty_cache_scope_is_not_a_successful_cache_build(isolated_worker):
    status_path, emissions = isolated_worker
    assert runner.run_once(signal_cache_only=True) == 0
    status = json.loads(status_path.read_text())
    assert status["signal_cache_maintenance_run"]["status"] == "not_applicable"
    assert emissions[-1]["status"] == "signal_cache_not_applicable"


def test_cache_failure_remains_visible_even_when_history_completed(isolated_worker, monkeypatch):
    status_path, _ = isolated_worker
    history = {"status": "complete", "run_id": "history-owner", "updated_at": "2026-10-29T15:47:03+08:00"}
    status_path.write_text(json.dumps({"jobs": {}, "maintenance_run": history}))
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", lambda _, **_kw: (2, 1, 0))
    assert runner.run_once(signal_cache_only=True) == 1
    status = json.loads(status_path.read_text())
    assert status["maintenance_run"] == history
    health = runner.discord_bot._artifact_backfill_health_summary()
    assert health["status"] == "degraded"
    assert health["cache_worker_status"] == "degraded"


def test_full_worker_can_recover_an_earlier_cache_failure(isolated_worker, monkeypatch):
    status_path, _ = isolated_worker
    status_path.write_text(json.dumps({"jobs": {}, "signal_cache_maintenance_run": {
        "status": "degraded", "run_id": "old-cache-owner", "reason": "postclose_cache_failed"}}))
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", lambda _, **_kw: (2, 0, 0))
    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", lambda _: None)
    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert status["signal_cache_maintenance_run"]["status"] == "complete"
    assert status["maintenance_run"]["status"] == "complete"
    assert runner.discord_bot._artifact_backfill_health_summary()["status"] == "ready"


def test_cache_source_wait_records_its_own_progress_without_touching_history(isolated_worker, monkeypatch):
    status_path, _ = isolated_worker
    history = {"status": "running", "run_id": "history-owner"}
    status_path.write_text(json.dumps({"jobs": {}, "maintenance_run": history}))
    states = iter([True, False])
    monkeypatch.setattr(runner.discord_bot, "_tw_public_refresh_in_progress", lambda: next(states))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)
    transitions = []

    def record(status, **kwargs):
        transitions.append(status)
        runner._record_signal_cache_maintenance_run(status, **kwargs)

    assert _REAL_WAIT_FOR_TW_PUBLIC_REFRESH(record_maintenance_run=False, record_worker=record)
    status = json.loads(status_path.read_text())
    assert transitions == ["waiting_source", "running"]
    assert status["maintenance_run"] == history
    assert status["signal_cache_maintenance_run"]["status"] == "running"


def test_cli_retains_global_code_failure_without_spending_artifact_attempts(
    isolated_worker, monkeypatch,
):
    from stockagent.live.completed_inference_cache import LoadedImplementationChanged

    status_path, emissions = isolated_worker
    pending = _pending_job("failed")
    status_path.write_text(json.dumps({"jobs": {"market": pending}}))
    def changed(_markets, **_kwargs):
        raise LoadedImplementationChanged()
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", changed)
    monkeypatch.setattr(runner.discord_bot, "_log_exception", lambda *_a: None)
    monkeypatch.setattr("sys.argv", ["run_discord_artifact_maintenance.py"])
    with pytest.raises(LoadedImplementationChanged):
        runner.main()
    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"] == pending
    assert status["maintenance_run"]["status"] == "degraded"
    assert status["maintenance_run"]["reason"] == "unhandled_LoadedImplementationChanged"
    assert status["maintenance_run"]["failures"] == 1
    assert emissions[-1]["status"] == "degraded"


@pytest.mark.parametrize("state", ["failed", "running", "waiting_source"])
def test_closed_day_resumes_only_existing_accepted_close_job(
    isolated_worker, monkeypatch, state,
):
    status_path, emissions = isolated_worker
    bot = runner.discord_bot
    key = "2026-10-16:market:artifact_backfill"
    job = dict(_pending_job(state), key=key, run_id="previous_worker",
               next_retry_at="2026-10-16T23:00:00+08:00")
    status_path.write_text(json.dumps({"jobs": {key: job}}))
    cfg = SimpleNamespace(market="market", timezone="Asia/Taipei",
                          day_trade_simulation_enabled=True)
    status = SimpleNamespace(data=SimpleNamespace(fresh=True))
    monkeypatch.setattr(bot, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(bot, "_ensure_signal_ready_cached", lambda _: status)
    monkeypatch.setattr(bot, "_completed_session_receipt_ready", lambda _: True)
    monkeypatch.setattr(bot, "_completed_session_target_date", lambda _: "2026-10-16")
    monkeypatch.setattr(bot, "_artifact_backfill_key", lambda *_: None)
    calls = []
    monkeypatch.setattr(bot, "_run_artifact_backfill_sync", lambda cfg: calls.append(cfg.market))

    class Sunday(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 18, 15, 0, tzinfo=tz)

    monkeypatch.setattr(runner, "datetime", Sunday)
    assert runner.run_once() == 0
    observed = json.loads(status_path.read_text())
    assert calls == ["market"]
    assert set(observed["jobs"]) == {key}  # No Sunday key or obsolete Friday replacement.
    assert observed["jobs"][key]["status"] == "ready"
    assert observed["jobs"][key]["attempt"] == job["attempt"] + 1
    assert any(row.get("status") == "resuming_completed_session" for row in emissions)


@pytest.mark.parametrize("defect", [
    "absent", "old_session", "wrong_market", "wrong_key", "ready", "future_session", "stale",
])
def test_completed_session_retry_never_creates_or_relabels_a_job(
    isolated_worker, monkeypatch, defect,
):
    status_path, _emissions = isolated_worker
    bot = runner.discord_bot
    cfg = SimpleNamespace(market="market", day_trade_simulation_enabled=True)
    status = SimpleNamespace(data=SimpleNamespace(fresh=defect != "stale"))
    target = "2026-10-19" if defect == "future_session" else "2026-10-16"
    monkeypatch.setattr(bot, "_completed_session_target_date", lambda _: target)
    key = f"{target}:market:artifact_backfill"
    job = dict(_pending_job("failed"), key=key)
    if defect == "wrong_market":
        job["market"] = "other"
    elif defect == "wrong_key":
        job["key"] = "2026-10-15:market:artifact_backfill"
    elif defect == "ready":
        job["status"] = "ready"
    stored_key = "2026-10-15:market:artifact_backfill" if defect == "old_session" else key
    payload = {"jobs": {} if defect == "absent" else {stored_key: job}}
    status_path.write_text(json.dumps(payload))
    before = status_path.read_bytes()
    now = datetime(2026, 10, 18, 15, tzinfo=ZoneInfo("Asia/Taipei"))
    assert runner._completed_session_retry_key(cfg, status, now=now) is None
    assert status_path.read_bytes() == before


@pytest.mark.parametrize("gate", ["source_stale", "receipt_unaccepted", "opening", "interactive"])
def test_closed_day_retry_retains_all_canonical_priority_and_close_gates(
    isolated_worker, monkeypatch, gate,
):
    status_path, _emissions = isolated_worker
    bot = runner.discord_bot
    key = "2026-10-16:market:artifact_backfill"
    job = dict(_pending_job("failed"), key=key, next_retry_at="2026-10-16T23:00:00+08:00")
    status_path.write_text(json.dumps({"jobs": {key: job}}))
    monkeypatch.setattr(bot, "_resolve_market", lambda _: SimpleNamespace(
        market="market", timezone="Asia/Taipei", day_trade_simulation_enabled=True))
    monkeypatch.setattr(bot, "_ensure_signal_ready_cached", lambda _: SimpleNamespace(
        data=SimpleNamespace(fresh=gate != "source_stale", last_data_date="2026-10-16",
                             expected_latest_date="2026-10-16")))
    monkeypatch.setattr(bot, "_completed_session_receipt_ready", lambda _: gate != "receipt_unaccepted")
    monkeypatch.setattr(bot, "_opening_critical_work_pending", lambda: gate == "opening")
    monkeypatch.setattr(bot, "_interactive_signal_work_pending", lambda: gate == "interactive")
    monkeypatch.setattr(runner, "_completed_session_retry_key", lambda *_a, **_k: pytest.fail("not admitted"))
    assert runner.run_once() == 0
    assert json.loads(status_path.read_text())["jobs"][key] == job


def test_closed_day_retry_preserves_failed_job_backoff(isolated_worker, monkeypatch):
    status_path, _emissions = isolated_worker
    bot = runner.discord_bot
    key = "2026-10-16:market:artifact_backfill"
    job = dict(_pending_job("failed"), key=key,
               next_retry_at=(datetime.now().astimezone() + timedelta(days=1)).isoformat())
    status_path.write_text(json.dumps({"jobs": {key: job}}))
    monkeypatch.setattr(bot, "_resolve_market", lambda _: SimpleNamespace(
        market="market", timezone="Asia/Taipei", day_trade_simulation_enabled=True))
    monkeypatch.setattr(bot, "_ensure_signal_ready_cached", lambda _: SimpleNamespace(data=SimpleNamespace(fresh=True)))
    monkeypatch.setattr(bot, "_completed_session_receipt_ready", lambda _: True)
    monkeypatch.setattr(bot, "_completed_session_target_date", lambda _: "2026-10-16")
    monkeypatch.setattr(bot, "_artifact_backfill_key", lambda *_: None)
    assert runner.run_once() == 0
    payload = json.loads(status_path.read_text())
    assert payload["jobs"][key] == job
    assert payload["maintenance_run"]["reason"] == "artifact_retry_deferred"
