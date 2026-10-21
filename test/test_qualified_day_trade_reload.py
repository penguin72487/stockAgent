"""Independent owner updates must not accept stale evidence or change accounts."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from zoneinfo import ZoneInfo

import pytest

from scripts import reload_tw_day_trade_services_if_flat as reload

NOW = datetime(2026, 10, 15, 10, 15, tzinfo=ZoneInfo("Asia/Taipei"))


@pytest.fixture(autouse=True)
def no_host_deployment_state(monkeypatch):
    from stockagent.live import service_code_release
    monkeypatch.setattr(service_code_release, "configured_release", lambda _: None)


def receipts():
    state = {"production_order_possible": False, "enabled_markets": ["tw_day_trade_100m"],
             "modes": {"tw_day_trade_100m": {
                 "signal_id": "immutable", "positions": {"2330": {"signed_shares": 1000}},
                 "pending_entry_orders": {"0050": {"remaining": 1000}}}}}
    bot = {"updated_at": NOW.isoformat(), "production_order_possible": False,
           "engine_published_at": NOW.isoformat(), "engine_state_revision": 100,
           "discord_connected": True, "core_health": "ready", "run_id": "old-bot",
           "engine_run_id": "unchanged-engine", "mode_signal_ids": {"tw_day_trade_100m": "immutable"},
           "opening_input_dispatch": {"inflight_mode_count": 0, "scheduled_delivery_pending": False,
                                      "input_stages": {}},
           "interactive_signal_jobs": {"active_count": 0, "running_count": 0, "queued_count": 0,
                                       "waiting_source_count": 0},
           "background_maintenance": {"status": "degraded", "running_count": 0},
           "startup_inference_warmup": {"status": "ready", "run_id": "old-bot",
                                        "model_cache_resident_ready": True, "ready_count": 1, "market_count": 1,
                                        "markets": [{"market": "tw_day_trade_100m", "status": "ready"}]}}
    sync = {"published_at": NOW.isoformat(), "engine_run_id": "unchanged-engine",
            "production_order_possible": False, "ledger_integrity_ready": True, "state_revision": 100,
            "modes": {"tw_day_trade_100m": {"signal_id": "immutable"}}}
    return state, bot, sync


def test_discord_can_reload_with_positions_and_pending_engine_orders():
    state, bot, sync = receipts()
    original = deepcopy((state, bot, sync))
    plan = reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)
    assert plan["services"] == ["stockagent-discord-bot.service"]
    assert plan["nonflat_count"] == plan["pending_orders"] == 1
    assert (state, bot, sync) == original


def test_cold_idle_discord_can_be_repaired_but_is_not_accepted_as_warm():
    state, bot, sync = receipts()
    bot["startup_inference_warmup"]["status"] = "pending"
    plan = reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)
    bot.update(run_id="new-bot", live_implementation_admission={"code_sha256": "qualified"})
    assert not reload.native_ready(bot, sync, plan, now=NOW, expected_code="qualified", expected_entrypoint="bot")


@pytest.mark.parametrize("field,value", [
    ("inflight_mode_count", 1), ("inflight_mode_count", None),
    ("scheduled_delivery_pending", True), ("scheduled_delivery_pending", None),
    ("input_stages", {"still": {"phase": "panel"}}),
])
def test_opening_work_prevents_cutover(field, value):
    state, bot, sync = receipts()
    bot["opening_input_dispatch"][field] = value
    with pytest.raises(ValueError, match="work|pending"):
        reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)


@pytest.mark.parametrize("field", ["active_count", "running_count", "queued_count", "waiting_source_count"])
def test_every_interactive_job_state_prevents_cutover(field):
    state, bot, sync = receipts()
    bot["interactive_signal_jobs"][field] = 1
    with pytest.raises(ValueError, match="work"):
        reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)


@pytest.mark.parametrize("change", ["stale_bot", "future_sync", "missing_mode", "malformed_mode",
                                    "unknown_shares", "pending_commit", "divergence", "wrong_signal",
                                    "broker_enabled", "warming", "maintenance_running",
                                    "missing_identity", "ledger_unaccepted", "sync_broker_enabled"])
def test_ambiguous_or_unsafe_evidence_fails_closed(change):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    if change == "stale_bot":
        bot["updated_at"] = (NOW - timedelta(seconds=11)).isoformat()
    elif change == "future_sync":
        sync["published_at"] = (NOW + timedelta(seconds=1)).isoformat()
    elif change == "missing_mode":
        state["enabled_markets"].append("not-present")
    elif change == "malformed_mode":
        state["modes"]["tw_day_trade_100m"] = None
    elif change == "unknown_shares":
        mode["positions"]["2330"]["signed_shares"] = .5
    elif change == "pending_commit":
        mode["entry_retry_pending_commit"] = True
    elif change == "divergence":
        mode["ledger_state_divergence"] = {"difference": 1}
    elif change == "wrong_signal":
        sync["modes"]["tw_day_trade_100m"]["signal_id"] = "another"
    elif change == "broker_enabled":
        state["production_order_possible"] = True
    elif change == "warming":
        bot["startup_inference_warmup"]["status"] = "running"
    elif change == "maintenance_running":
        bot["background_maintenance"]["running_count"] = 1
    elif change == "missing_identity":
        bot.pop("run_id")
    elif change == "ledger_unaccepted":
        sync["ledger_integrity_ready"] = False
    elif change == "sync_broker_enabled":
        sync["production_order_possible"] = True
    with pytest.raises(ValueError):
        reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)


def test_engine_still_requires_offsession_flat_and_no_pending_orders():
    state, bot, sync = receipts()
    with pytest.raises(ValueError, match="off-session"):
        reload.reload_plan(state, bot, sync, now=NOW, discord_only=False)
    closed = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = closed.isoformat()
    with pytest.raises(ValueError, match="nonflat"):
        reload.reload_plan(state, bot, sync, now=closed, discord_only=False)
    mode = state["modes"]["tw_day_trade_100m"]
    mode["positions"]["2330"]["signed_shares"] = 0
    with pytest.raises(ValueError, match="pending orders"):
        reload.reload_plan(state, bot, sync, now=closed, discord_only=False)
    mode["pending_entry_orders"].clear()
    assert len(reload.reload_plan(state, bot, sync, now=closed, discord_only=False)["services"]) == 2


@pytest.mark.parametrize("change", ["in_session", "position", "pending_entry", "pending_reduction"])
def test_engine_only_retains_every_flat_and_clock_guard(change):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    mode["positions"]["2330"]["signed_shares"] = 0
    mode["pending_entry_orders"].clear()
    now = NOW.replace(hour=14)
    if change == "in_session":
        now = NOW
    elif change == "position":
        mode["positions"]["2330"]["signed_shares"] = -1000
    else:
        field = "pending_entry_orders" if change == "pending_entry" else "pending_reduction_orders"
        mode[field] = {"2330": {"remaining": 1000}}
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
    with pytest.raises(ValueError, match="off-session|nonflat|pending orders"):
        reload.reload_plan(state, bot, sync, now=now, engine_only=True)


@pytest.mark.parametrize("change", [None, "old_engine", "new_bot", "wrong_ack", "old_code",
                                    "wrong_signal", "broker_enabled", "ledger_unaccepted"])
def test_engine_only_accepts_new_engine_not_new_bot_or_fake_gpu_residency(change):
    state, bot, sync = receipts()
    state["modes"]["tw_day_trade_100m"]["positions"].clear()
    state["modes"]["tw_day_trade_100m"]["pending_entry_orders"].clear()
    now = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
    bot["startup_inference_warmup"]["model_cache_resident_ready"] = False
    plan = reload.reload_plan(state, bot, sync, now=now, engine_only=True)
    assert plan["services"] == [reload.SERVICES[1]]
    bot["live_implementation_admission"] = {"code_sha256": "qualified", "entrypoint_sha256": "bot"}
    bot["engine_run_id"] = sync["engine_run_id"] = "new-engine"
    if change == "old_engine":
        bot["engine_run_id"] = sync["engine_run_id"] = plan["engine_run_id"]
    elif change == "new_bot":
        bot["run_id"] = "different-bot"
    elif change == "wrong_ack":
        bot["engine_state_revision"] -= 1
    elif change == "old_code":
        bot["live_implementation_admission"]["code_sha256"] = "old"
    elif change == "wrong_signal":
        sync["modes"]["tw_day_trade_100m"]["signal_id"] = "changed"
    elif change == "broker_enabled":
        sync["production_order_possible"] = True
    elif change == "ledger_unaccepted":
        sync["ledger_integrity_ready"] = False
    assert reload.native_ready(bot, sync, plan, now=now, expected_code="qualified", expected_entrypoint="bot") is (change is None)
    assert bot["startup_inference_warmup"]["model_cache_resident_ready"] is False


def test_selecting_both_independent_owners_is_not_an_engine_only_bypass():
    with pytest.raises(ValueError, match="one independent owner"):
        reload.reload_plan(*receipts(), now=NOW, discord_only=True, engine_only=True)
    with pytest.raises(SystemExit):
        from unittest.mock import patch
        with patch("sys.argv", ["reload", "--engine-only", "--discord-only", "--apply"]):
            reload.main()


@pytest.mark.parametrize("change", [None, "stale_heartbeat", "future_heartbeat", "missing_commit",
                                    "wrong_ack_commit", "heartbeat_before_commit", "malformed_heartbeat"])
def test_idle_commit_age_is_not_heartbeat_age_and_unknown_liveness_fails_closed(change):
    state, bot, sync = receipts()
    state["modes"]["tw_day_trade_100m"]["positions"].clear()
    state["modes"]["tw_day_trade_100m"]["pending_entry_orders"].clear()
    now = NOW.replace(hour=14)
    bot["updated_at"] = sync["heartbeat_at"] = now.isoformat()
    bot["engine_published_at"] = sync["published_at"] = (now - timedelta(minutes=1)).isoformat()
    if change == "stale_heartbeat":
        sync["heartbeat_at"] = (now - timedelta(seconds=11)).isoformat()
    elif change == "future_heartbeat":
        sync["heartbeat_at"] = (now + timedelta(seconds=1)).isoformat()
    elif change == "missing_commit":
        sync.pop("published_at")
    elif change == "wrong_ack_commit":
        bot["engine_published_at"] = (now - timedelta(minutes=2)).isoformat()
    elif change == "heartbeat_before_commit":
        sync["published_at"] = bot["engine_published_at"] = (now + timedelta(seconds=1)).isoformat()
    elif change == "malformed_heartbeat":
        sync["heartbeat_at"] = None
    if change is not None:
        with pytest.raises(ValueError):
            reload.reload_plan(state, bot, sync, now=now, engine_only=True)
        return
    plan = reload.reload_plan(state, bot, sync, now=now, engine_only=True)
    bot["engine_run_id"] = sync["engine_run_id"] = "new-engine"
    bot["live_implementation_admission"] = {"code_sha256": "qualified", "entrypoint_sha256": "bot"}
    assert reload.native_ready(bot, sync, plan, now=now, expected_code="qualified", expected_entrypoint="bot")


@pytest.mark.parametrize("field", ["pending_entry_orders", "pending_reduction_orders"])
@pytest.mark.parametrize("change", [None, "remaining", "unknown_quantity", "bool_quantity",
                                    "unsized", "missing_sizing", "working", "cancelled", "malformed"])
def test_preserved_filled_rows_are_not_active_orders_but_ambiguous_rows_block(field, change):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    mode["positions"].clear()
    mode["pending_entry_orders"].clear()
    order = {"status": "filled", "remaining_shares": 0, "sizing_pending": False}
    mode[field] = {"2330": order}
    now = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
    if change == "remaining":
        order["remaining_shares"] = 1000
    elif change == "unknown_quantity":
        order.pop("remaining_shares")
    elif change == "bool_quantity":
        order["remaining_shares"] = False
    elif change == "unsized":
        order["sizing_pending"] = True
    elif change == "missing_sizing":
        order.pop("sizing_pending")
    elif change in {"working", "cancelled"}:
        order["status"] = change
    elif change == "malformed":
        mode[field]["2330"] = None
    before = deepcopy(state)
    if change is None:
        plan = reload.reload_plan(state, bot, sync, now=now, engine_only=True)
        assert plan["pending_orders"] == 0 and plan["retained_terminal_orders"] == 1
    else:
        with pytest.raises(ValueError, match="pending orders"):
            reload.reload_plan(state, bot, sync, now=now, engine_only=True)
    assert state == before


def test_opening_protection_applies_even_to_discord_only():
    state, bot, sync = receipts()
    opening = NOW.replace(hour=9, minute=0)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = opening.isoformat()
    with pytest.raises(ValueError, match="opening protection"):
        reload.reload_plan(state, bot, sync, now=opening, discord_only=True)


@pytest.mark.parametrize("change", [None, "old_run", "old_code", "engine_restarted", "wrong_signal", "warming",
                                    "missing_run", "old_revision", "ledger_unaccepted", "old_entrypoint",
                                    "old_warmup", "evicted_model", "missing_warm_mode"])
def test_native_acceptance_requires_new_code_gateway_warmup_and_unchanged_engine(change):
    state, bot, sync = receipts()
    plan = reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)
    bot.update(run_id="new-bot", live_implementation_admission={"code_sha256": "qualified", "entrypoint_sha256": "bot"})
    bot["startup_inference_warmup"]["run_id"] = "new-bot"
    if change == "old_run":
        bot["run_id"] = "old-bot"
    elif change == "old_code":
        bot["live_implementation_admission"]["code_sha256"] = "old"
    elif change == "engine_restarted":
        bot["engine_run_id"] = sync["engine_run_id"] = "different-engine"
    elif change == "wrong_signal":
        bot["mode_signal_ids"]["tw_day_trade_100m"] = "new-signal"
    elif change == "warming":
        bot["startup_inference_warmup"]["status"] = "running"
    elif change == "missing_run":
        bot.pop("run_id")
    elif change == "old_revision":
        bot["engine_state_revision"] = 99
    elif change == "ledger_unaccepted":
        sync["ledger_integrity_ready"] = False
    elif change == "old_entrypoint":
        bot["live_implementation_admission"]["entrypoint_sha256"] = "old"
    elif change == "old_warmup":
        bot["startup_inference_warmup"]["run_id"] = "old-bot"
    elif change == "evicted_model":
        bot["startup_inference_warmup"]["model_cache_resident_ready"] = False
    elif change == "missing_warm_mode":
        bot["startup_inference_warmup"]["markets"].clear()
    assert reload.native_ready(bot, sync, plan, now=NOW, expected_code="qualified", expected_entrypoint="bot") is (change is None)


def test_cli_default_is_plan_not_restart(tmp_path, monkeypatch):
    from contextlib import nullcontext
    state, bot, sync = receipts()
    # Use a fixed injected clock, not a future-dated production receipt.
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    paths = [tmp_path / name for name in ("state.json", "bot.json", "sync.json")]
    for path, value in zip(paths, (state, bot, sync)):
        path.write_text(json.dumps(value))
    monkeypatch.setattr(reload, "datetime", Clock)
    for field, path in zip(("STATE_PATH", "BOT_STATUS", "SYNC_PATH"), paths):
        monkeypatch.setattr(reload, field, path)
    monkeypatch.setattr(reload, "control_lease", lambda **_: nullcontext())
    monkeypatch.setattr(reload, "owner", lambda _: {"ActiveState": "active"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *a, **kw: pytest.fail("plan restarted a service"))
    output = tmp_path / "plan.json"
    monkeypatch.setattr("sys.argv", ["reload", "--discord-only", "--output", str(output)])
    assert reload.main() == 0
    result = json.loads(output.read_bytes())
    assert result["status"] == "planned" and not result["restart_requested"]


@pytest.mark.parametrize("engine_only,code_only", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("pinned", [False, True])
@pytest.mark.parametrize("failure", [None, "prefix_changed", "protected_changed", "not_ready", "accounting_changed",
                                    "preexisting_old_code", "preexisting_old_entrypoint", "malformed_admission",
                                    "reported_current_code", "journal_fatal", "journal_pending", "slow_prefix_check",
                                    "code_changed_during_start", "pinned_changed_during_start", "pin_install_failed"])
def test_apply_restarts_only_selected_owner_and_requires_independent_acceptance(tmp_path, monkeypatch, failure, engine_only, code_only, pinned):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from scripts import audit_tw_day_trade_restart as audit
    from stockagent.live import completed_inference_cache as cache, market_config, tw_public_source_guard
    import stockagent.config as config_module
    from stockagent.live import service_code_release

    state, bot, sync = receipts()
    if pinned:
        bot["run_id"] = bot["startup_inference_warmup"]["run_id"] = "100-old-bot"
    now = NOW.replace(hour=14) if engine_only else NOW
    if engine_only:
        state["modes"]["tw_day_trade_100m"]["positions"].clear()
        state["modes"]["tw_day_trade_100m"]["pending_entry_orders"].clear()
        bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
        bot["startup_inference_warmup"]["model_cache_resident_ready"] = False
    class Clock(datetime):
        offset_seconds = 0
        @classmethod
        def now(cls, tz=None):
            return now + timedelta(seconds=cls.offset_seconds)
    paths = [tmp_path / name for name in ("state.json", "bot.json", "sync.json")]
    for path, value in zip(paths, (state, bot, sync)):
        path.write_text(json.dumps(value))
    for field, path in zip(("STATE_PATH", "BOT_STATUS", "SYNC_PATH"), paths):
        monkeypatch.setattr(reload, field, path)
    for name in ("events.jsonl", "orders.jsonl", "fills.jsonl"):
        (tmp_path / name).write_text("immutable ledger prefix\n")
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload, "control_lease", lambda **_: nullcontext())
    code_changed = [False]
    monkeypatch.setattr(cache, "implementation_identity", lambda: [{"qualified": "changed" if code_changed[0] else "implementation"}])
    expected = cache.identity_digest({"code": cache.implementation_identity()})
    frozen_changed, installs = [False], []
    if pinned:
        pinned_code = cache.implementation_identity()
        pinned_entry = cache.file_identity(reload.REPO_ROOT / "services/discord_bot/bot.py")
        path = tmp_path / "release.json"
        monkeypatch.setattr(service_code_release, "DROP_IN", tmp_path / "code.conf")
        monkeypatch.setattr(service_code_release, "configured_release", lambda _: path)
        monkeypatch.setattr(service_code_release, "verify_frozen_source", lambda *_: {"receipt": str(path)})
        monkeypatch.setattr(service_code_release, "namespace_identity", lambda *_: {
            "code": pinned_code, "entrypoint": pinned_entry, "readonly_verified": True})
        monkeypatch.setattr(service_code_release, "frozen_generation", lambda *_: (frozen_changed[0],))
        monkeypatch.setattr(service_code_release, "verify_owner_namespace", lambda *_: {"readonly_mounts_verified": True})
        def install(*args):
            installs.append(args)
            if failure == "pin_install_failed":
                raise ValueError("invalid unit preview")
        monkeypatch.setattr(service_code_release, "install_drop_in", install)
    bot["live_implementation_admission"] = {"code_sha256": expected,
        "entrypoint_sha256": cache.file_identity(reload.REPO_ROOT / "services/discord_bot/bot.py")["sha256"]}
    if code_only:
        bot["live_implementation_admission"]["code_sha256"] = "old"
        bot["startup_inference_warmup"].update(status="degraded", ready_count=0,
            model_cache_resident_ready=False, markets=[{"market": "tw_day_trade_100m", "status": "failed",
                "error_type": "LoadedImplementationChanged", "retryable": True}])
        from scripts import verify_discord_gpu_idle_runtime as idle
        monkeypatch.setattr(idle, "command_sync_since_restart", lambda: {
            "commands_sync_or_deferred_observed": failure != "journal_pending",
            "watchdog_or_fatal_observed": failure == "journal_fatal"})
        if failure == "reported_current_code":
            bot["live_implementation_admission"]["code_sha256"] = expected
    preflight_mismatch = failure in {"preexisting_old_code", "preexisting_old_entrypoint", "malformed_admission"}
    if failure == "preexisting_old_code":
        bot["live_implementation_admission"]["code_sha256"] = "old"
    elif failure == "preexisting_old_entrypoint":
        bot["live_implementation_admission"]["entrypoint_sha256"] = "old"
    elif failure == "malformed_admission":
        bot["live_implementation_admission"] = "not a verified admission"
    paths[1].write_text(json.dumps(bot))
    monkeypatch.setattr(market_config, "load_market_config", lambda _: SimpleNamespace(config_path="qualified.yaml"))
    monkeypatch.setattr(config_module, "load_config", lambda _: SimpleNamespace(data=SimpleNamespace(parquet_root="source")))
    monkeypatch.setattr(tw_public_source_guard, "live_tw_public_source_read_guard", lambda _: nullcontext())
    restarts = []
    selected = reload.SERVICES[int(engine_only)]

    def owner(service):
        changed = bool(restarts) and (service == selected or failure == "protected_changed")
        return {"ActiveState": "active", "MainPID": "200" if changed else "100",
                "InvocationID": "new" if changed else "original"}

    def restart(command, **kwargs):
        assert command == ["systemctl", "restart", selected]
        restarts.append(command)
        if failure == "code_changed_during_start":
            code_changed[0] = True
        if failure == "pinned_changed_during_start":
            frozen_changed[0] = True
        bot["live_implementation_admission"] = {"code_sha256": expected,
            "entrypoint_sha256": cache.file_identity(reload.REPO_ROOT / "services/discord_bot/bot.py")["sha256"]}
        if failure != "not_ready":
            if engine_only:
                bot["engine_run_id"] = sync["engine_run_id"] = "new-engine"
                paths[2].write_text(json.dumps(sync))
            else:
                bot["run_id"] = bot["startup_inference_warmup"]["run_id"] = "201-new-bot"
                if code_only:
                    bot["startup_inference_warmup"].update(status="pending", markets=[])
            paths[1].write_text(json.dumps(bot))
        if failure == "accounting_changed":
            state["modes"]["tw_day_trade_100m"]["cumulative_realized_net_pnl_twd"] = 123
            paths[0].write_text(json.dumps(state))
        # Native append progress is allowed; rewriting a prior prefix is not.
        with (tmp_path / "events.jsonl").open("a") as handle:
            handle.write("native engine continued\n")
        if failure == "prefix_changed":
            (tmp_path / "fills.jsonl").write_text("corrupted ledger\n")

    monkeypatch.setattr(reload, "owner", owner)
    monkeypatch.setattr(reload.subprocess, "run", restart)
    monkeypatch.setattr(reload, "read_service_ack_pair", lambda *_a, **_kw: (
        reload.read_object(paths[2]), reload.read_object(paths[1]), {"matched": True}))
    if failure == "slow_prefix_check":
        verify_prefix = audit.verify_prefix
        def slow_prefix(*args, **kwargs):
            result = verify_prefix(*args, **kwargs)
            Clock.offset_seconds = 20
            bot["updated_at"] = sync["heartbeat_at"] = (now+timedelta(seconds=20)).isoformat()
            paths[1].write_text(json.dumps(bot))
            paths[2].write_text(json.dumps(sync))
            return result
        monkeypatch.setattr(audit, "verify_prefix", slow_prefix)
    ticks = iter((0., 2.))
    monkeypatch.setattr(reload.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "applied.json"
    monkeypatch.setattr("sys.argv", ["reload", "--engine-only" if engine_only else "--discord-only", "--apply", "--timeout-seconds", "1",
                                    "--output", str(output), *(["--code-admission-only"] if code_only else [])])
    accepted = failure is None or ((failure == "accounting_changed" or preflight_mismatch) and not engine_only)
    accepted = accepted or (not code_only and failure in {"reported_current_code", "journal_fatal", "journal_pending"})
    accepted = accepted or failure == "slow_prefix_check"
    accepted = accepted or (pinned and failure == "code_changed_during_start")
    accepted = accepted or (not pinned and failure == "pinned_changed_during_start")
    accepted = accepted or (failure == "pin_install_failed" and (not pinned or engine_only))
    assert reload.main() == (0 if accepted else 75)
    result = json.loads(output.read_bytes())
    if engine_only and preflight_mismatch or code_only and failure == "reported_current_code":
        assert not restarts and not result["restart_requested"]
        assert result["status"] == "blocked" and ("code admission" in result["error"] or "code drift" in result["error"])
        return
    if pinned and not engine_only and failure == "pin_install_failed":
        assert not restarts and not result["restart_requested"]
        assert result["status"] == "blocked" and "unit preview" in result["error"]
        return
    assert len(restarts) == 1 and result["restart_requested"]
    assert result["status"] == (("accepted_code_admission_only" if code_only else "accepted")
                                if accepted else "failed_after_restart")
    if (failure == "code_changed_during_start" and not pinned
            or failure == "pinned_changed_during_start" and pinned):
        assert result["error_type"] == "RuntimeError" and "code changed during startup" in result["error"]
    if accepted:
        if failure == "slow_prefix_check":
            assert result["engine_heartbeat_at"] == (now+timedelta(seconds=20)).isoformat()
        assert result["gpu_warmup_verified"] is not (code_only or engine_only)
        if code_only:
            assert result["gpu_warmup_verified"] is False
            assert result["acceptance_scope"] == "loaded_code_gateway_ack_not_gpu_readiness"
        assert result["engine_restarted"] is engine_only
        if pinned:
            assert result["native_code_namespace"]["readonly_mounts_verified"]
            assert len(installs) == int(not engine_only)
        assert result["inventory_restored_exactly"] is engine_only
        assert result["protected_before"] == result["protected_after"]
        if engine_only:
            assert reload.SERVICES[0] in result["protected_after"]
            assert result["new_discord_run_id"] == ("100-old-bot" if pinned else "old-bot")
            assert result["native_warmup"]["model_cache_resident_ready"] is False
        assert audit.verify_prefix(tmp_path, {"inputs": result["ledger_inputs"]}) == result["ledger_prefixes"]


def code_drift_receipts():
    state, bot, sync = receipts()
    bot["startup_inference_warmup"].update(status="degraded", ready_count=0,
        model_cache_resident_ready=False, markets=[{"market": "tw_day_trade_100m", "status": "failed",
            "error_type": "LoadedImplementationChanged", "retryable": True}])
    return state, bot, sync


@pytest.mark.parametrize("change", [None, "old_run", "unknown_error", "not_retryable", "missing_mode",
    "duplicate_mode", "wrong_mode", "unknown_status", "not_degraded", "resident", "ready_count",
    "bool_ready_count", "bool_market_count", "nonlist_rows", "malformed_market", "duplicate_enabled"])
def test_code_drift_recovery_is_explicit_complete_and_same_run(change):
    state, bot, sync = code_drift_receipts()
    w = bot["startup_inference_warmup"]
    if change == "old_run": w["run_id"] = "another"
    elif change == "unknown_error": w["markets"][0]["error_type"] = "OutOfMemoryError"
    elif change == "not_retryable": w["markets"][0]["retryable"] = False
    elif change == "missing_mode": w["markets"].clear()
    elif change == "duplicate_mode": w["markets"] *= 2
    elif change == "wrong_mode": w["markets"][0]["market"] = "different"
    elif change == "unknown_status": w["markets"][0]["status"] = "running"
    elif change == "not_degraded": w["status"] = "running"
    elif change == "resident": w["model_cache_resident_ready"] = True
    elif change == "ready_count": w["ready_count"] = 1
    elif change == "bool_ready_count": w["ready_count"] = False
    elif change == "bool_market_count": w["market_count"] = True
    elif change == "nonlist_rows": w["markets"] = {}
    elif change == "malformed_market": w["markets"][0]["market"] = []
    elif change == "duplicate_enabled":
        state["enabled_markets"] *= 2
        w["markets"] *= 2
        w["market_count"] = 2
    with pytest.raises(ValueError, match="warmup"):
        reload.reload_plan(state, bot, sync, now=NOW, discord_only=True)
    if change is None:
        plan = reload.reload_plan(state, bot, sync, now=NOW, discord_only=True, code_admission_only=True)
        assert plan["code_admission_only"] and plan["services"] == [reload.SERVICES[0]]
    else:
        with pytest.raises(ValueError, match="warmup"):
            reload.reload_plan(state, bot, sync, now=NOW, discord_only=True, code_admission_only=True)


@pytest.mark.parametrize("change", ["busy", "opening", "stale", "wrong_ack", "broker"])
def test_code_recovery_does_not_bypass_existing_guards(change):
    state, bot, sync = code_drift_receipts()
    now = NOW
    if change == "busy": bot["interactive_signal_jobs"]["queued_count"] = 1
    elif change == "opening":
        now = NOW.replace(hour=9, minute=0)
        bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
    elif change == "stale": bot["updated_at"] = (NOW-timedelta(seconds=11)).isoformat()
    elif change == "wrong_ack": sync["modes"]["tw_day_trade_100m"]["signal_id"] = "wrong"
    elif change == "broker": sync["production_order_possible"] = True
    with pytest.raises(ValueError):
        reload.reload_plan(state, bot, sync, now=now, discord_only=True, code_admission_only=True)


@pytest.mark.parametrize("status", ["pending", "running", "disabled", "ready", "degraded", "unknown"])
def test_code_admission_never_claims_deferred_gpu_residency(status):
    state, bot, sync = receipts()
    plan = reload.reload_plan(state, bot, sync, now=NOW, discord_only=True, code_admission_only=True)
    bot.update(run_id="fresh", live_implementation_admission={"code_sha256": "qualified", "entrypoint_sha256": "bot"})
    bot["startup_inference_warmup"].update(run_id="fresh", status=status, model_cache_resident_ready=False)
    assert reload.native_ready(bot, sync, plan, now=NOW, expected_code="qualified", expected_entrypoint="bot") is (
        status in {"pending", "running", "disabled", "ready"})
    assert not reload.native_ready(bot, sync, {**plan, "code_admission_only": False}, now=NOW,
                                   expected_code="qualified", expected_entrypoint="bot")
    bot["live_implementation_admission"]["code_sha256"] = "old"
    assert not reload.native_ready(bot, sync, plan, now=NOW, expected_code="qualified", expected_entrypoint="bot")


def test_code_admission_cannot_select_an_engine(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="independent Discord"):
        reload.reload_plan(*receipts(), now=NOW, code_admission_only=True)
    monkeypatch.setattr("sys.argv", ["reload", "--engine-only", "--code-admission-only", "--apply"])
    with pytest.raises(SystemExit): reload.main()


@pytest.mark.parametrize("matched", [True, False])
def test_plan_uses_original_coherent_pair_not_three_independent_reads(tmp_path, monkeypatch, matched):
    from contextlib import nullcontext
    state, bot, sync = code_drift_receipts()
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload, "control_lease", lambda **_: nullcontext())
    monkeypatch.setattr(reload, "read_object", lambda path: state if path == reload.STATE_PATH
                        else pytest.fail("split native-pair read"))
    observations = []
    def pair(*args, **kwargs):
        observations.append((args, kwargs))
        return sync, bot, {"matched": matched}
    monkeypatch.setattr(reload, "read_service_ack_pair", pair)
    monkeypatch.setattr(reload, "owner", lambda _: {"ActiveState": "active"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *a, **kw: pytest.fail("plan changed a service"))
    output = tmp_path / "plan.json"
    monkeypatch.setattr("sys.argv", ["reload", "--discord-only", "--code-admission-only", "--output", str(output)])
    assert reload.main() == (0 if matched else 75)
    assert len(observations) == 1 and observations[0][1]["timeout_seconds"] == 5.
    assert json.loads(output.read_text())["restart_requested"] is False
