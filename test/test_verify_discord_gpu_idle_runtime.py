from copy import deepcopy
from types import SimpleNamespace

import pytest

from scripts import verify_discord_gpu_idle_runtime as verifier


def status():
    return {"interactive_signal_jobs": {"active_count": 0, "running_count": 0},
            "background_maintenance": {"running_count": 0, "status": "degraded"},
            "startup_inference_warmup": {"status": "ready"},
            "scheduled_day_trade_markets": ["cash"], "engine_state_revision": 30,
            "engine_run_id": "original execution engine", "mode_signal_ids": {"cash": "original signal"}}


def engine():
    return {"engine_run_id": "original execution engine", "state_revision": 30,
            "modes": {"cash": {"signal_id": "original signal"}}}


@pytest.mark.parametrize("kind", ["interactive", "maintenance", "warmup", "opening", "source", "prepare"])
def test_idle_admission_rejects_actual_work_and_prepare_window(monkeypatch, kind):
    value = status()
    monkeypatch.setattr(verifier.BOT, "_opening_critical_work_pending", lambda: kind == "opening")
    monkeypatch.setattr(verifier.BOT, "_interactive_signal_work_pending", lambda: False)
    monkeypatch.setattr(verifier.BOT, "_tw_public_refresh_in_progress", lambda: kind == "source")
    monkeypatch.setattr(verifier.BOT, "_startup_inference_warmup_must_defer", lambda: kind != "prepare")
    monkeypatch.setattr(verifier.BOT, "_offhours_gpu_cache_release_allowed", lambda: True)
    if kind == "interactive":
        value["interactive_signal_jobs"]["active_count"] = 1
    elif kind == "maintenance":
        value["background_maintenance"]["running_count"] = 1
    elif kind == "warmup":
        value["startup_inference_warmup"]["status"] = "running"
    with pytest.raises(ValueError):
        verifier.pending_work(value)


def test_degraded_old_formal_history_does_not_block_idle_gateway_admission(monkeypatch):
    for name in ("_opening_critical_work_pending", "_interactive_signal_work_pending", "_tw_public_refresh_in_progress"):
        monkeypatch.setattr(verifier.BOT, name, lambda: False)
    monkeypatch.setattr(verifier.BOT, "_startup_inference_warmup_must_defer", lambda: True)
    monkeypatch.setattr(verifier.BOT, "_offhours_gpu_cache_release_allowed", lambda: True)
    assert verifier.pending_work(status()) is None


@pytest.mark.parametrize("kind", ["engine_generation", "revision", "signal", "missing", "scope"])
def test_engine_ack_requires_current_revision_exact_signals_and_all_modes(monkeypatch, kind):
    value, receipt = status(), engine()
    if kind == "engine_generation":
        value["engine_run_id"] = "different owner"
    elif kind == "revision":
        value["engine_state_revision"] = 29
    elif kind == "signal":
        value["mode_signal_ids"]["cash"] = "different signal"
    elif kind == "scope":
        value["scheduled_day_trade_markets"] = []
    elif kind == "missing":
        receipt["modes"] = {}
    monkeypatch.setattr(verifier, "load_service_sync", lambda _root: deepcopy(receipt))
    with pytest.raises(ValueError):
        verifier.engine_acknowledgements(value, ["cash"])


def test_current_engine_ack_is_readonly_and_not_a_fill_claim(monkeypatch):
    receipt = engine()
    monkeypatch.setattr(verifier, "load_service_sync", lambda _root: deepcopy(receipt))
    assert verifier.engine_acknowledgements(status(), ["cash"]) == [
        {"market": "cash", "signal_id": "original signal", "lag": 0},
    ]
    assert receipt == engine()


@pytest.mark.parametrize("text,accepted", [
    ("synced 20 global app commands installs=guild,user\n", True),
    ("global app command sync deferred; local signal scheduler remains active error=TimeoutError\n", True),
    ("commands failed\n", False), ("command sync failed\n", False),
])
def test_commands_need_success_or_explicit_contractual_defer(monkeypatch, text, accepted):
    monkeypatch.setattr(verifier.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=text))
    assert verifier.command_sync_since_restart()["commands_sync_or_deferred_observed"] is accepted


@pytest.fixture
def runtime(monkeypatch):
    value = status()
    value["startup_inference_warmup"] = {"status": "deferred"}
    owner = {"InvocationID": "before", "MainPID": "12"}
    admission = {"contract": "live_implementation_process_admission_v1", "code_sha256": "current",
                 "requires_fresh_process_on_change": True}
    monkeypatch.setattr(verifier, "owner", lambda: deepcopy(owner))
    monkeypatch.setattr(verifier, "identity", lambda unit: {"InvocationID": unit})
    monkeypatch.setattr(verifier, "sha", lambda path: str(path))
    monkeypatch.setattr(verifier, "status_for_owner", lambda: (deepcopy(value), SimpleNamespace(name="12")))
    monkeypatch.setattr(verifier, "pending_work", lambda status: None)
    monkeypatch.setattr(verifier.cache, "verify", lambda: {"markets": [
        {"market": "cash", "cache_key": "same", "summary_path": "same.json"}]})
    monkeypatch.setattr(verifier, "engine_acknowledgements", lambda *args: [{"lag": 0}])
    monkeypatch.setattr(verifier, "gpu_handles", lambda process: [])
    monkeypatch.setattr(verifier, "gpu_snapshot", lambda: {"gpu": "not probed in unit tests"})
    monkeypatch.setattr(verifier.BOT, "_scheduled_markets", lambda: [])
    monkeypatch.setattr(verifier, "current_code_admission", lambda: deepcopy(admission))
    monkeypatch.setattr(verifier, "command_sync_since_restart", lambda: {
        "commands_sync_or_deferred_observed": True, "watchdog_or_fatal_observed": False,
        "new_warm_ready_log_observed": False})
    return value, owner, admission


def test_code_adoption_is_distinct_from_gpu_numerical_acceptance(runtime, monkeypatch):
    value, owner, admission = runtime
    monkeypatch.setattr(verifier.cache, "matched_benchmarks", lambda *args: pytest.fail("wrong scope"))
    baseline = verifier.verify(None, None, code_admission_only=True)
    assert baseline["contract"] == "discord_native_loaded_code_admission_v1"
    assert baseline["full_numerical_oracle_proven"] is False
    owner["InvocationID"] = "after"
    value["live_implementation_admission"] = admission
    result = verifier.verify(None, None, baseline, code_admission_only=True)
    assert result["state"] == "accepted_native_discord_loaded_code_admission"
    assert result["signals"] == baseline["signals"]


@pytest.mark.parametrize("admission", [None, {}, {"code_sha256": "old"}])
def test_native_changed_owner_needs_its_actual_current_code_admission(runtime, admission):
    value, owner, _expected = runtime
    baseline = verifier.verify(None, None, code_admission_only=True)
    owner["InvocationID"] = "after"
    value["live_implementation_admission"] = admission
    with pytest.raises(ValueError, match="implementation_not_current"):
        verifier.verify(None, None, baseline, code_admission_only=True)


def test_original_numerical_acceptance_still_requires_independent_benchmarks(runtime, monkeypatch):
    calls = []
    monkeypatch.setattr(verifier.cache, "matched_benchmarks", lambda *args: calls.append(args) or ["proof"])
    with pytest.raises(ValueError, match="separate modes"):
        verifier.verify(None, None)
    with pytest.raises(ValueError, match="separate modes"):
        verifier.verify("bench", "control", code_admission_only=True)
    result = verifier.verify("bench", "control")
    assert result["full_numerical_oracle_proven"] is True and len(calls) == 1
    with pytest.raises(ValueError, match="control_contract_mismatch"):
        verifier.verify(None, None, result, code_admission_only=True)
