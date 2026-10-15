"""Discord-only updates must not interrupt execution or accept stale evidence."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from zoneinfo import ZoneInfo

import pytest

from scripts import reload_tw_day_trade_services_if_flat as reload

NOW = datetime(2026, 10, 15, 10, 15, tzinfo=ZoneInfo("Asia/Taipei"))


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


@pytest.mark.parametrize("failure", [None, "prefix_changed", "protected_changed", "not_ready"])
def test_apply_restarts_only_selected_owner_and_requires_independent_acceptance(tmp_path, monkeypatch, failure):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from scripts import audit_tw_day_trade_restart as audit
    from stockagent.live import completed_inference_cache as cache, market_config, tw_public_source_guard
    import stockagent.config as config_module

    state, bot, sync = receipts()
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    paths = [tmp_path / name for name in ("state.json", "bot.json", "sync.json")]
    for path, value in zip(paths, (state, bot, sync)):
        path.write_text(json.dumps(value))
    for field, path in zip(("STATE_PATH", "BOT_STATUS", "SYNC_PATH"), paths):
        monkeypatch.setattr(reload, field, path)
    for name in ("events.jsonl", "orders.jsonl", "fills.jsonl"):
        (tmp_path / name).write_text("immutable ledger prefix\n")
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload, "control_lease", lambda **_: nullcontext())
    monkeypatch.setattr(cache, "implementation_identity", lambda: [{"qualified": "implementation"}])
    expected = cache.identity_digest({"code": cache.implementation_identity()})
    monkeypatch.setattr(market_config, "load_market_config", lambda _: SimpleNamespace(config_path="qualified.yaml"))
    monkeypatch.setattr(config_module, "load_config", lambda _: SimpleNamespace(data=SimpleNamespace(parquet_root="source")))
    monkeypatch.setattr(tw_public_source_guard, "live_tw_public_source_read_guard", lambda _: nullcontext())
    restarts = []

    def owner(service):
        changed = bool(restarts) and (service == reload.SERVICES[0] or failure == "protected_changed")
        return {"ActiveState": "active", "MainPID": "200" if changed else "100",
                "InvocationID": "new" if changed else "original"}

    def restart(command, **kwargs):
        assert command == ["systemctl", "restart", reload.SERVICES[0]]
        restarts.append(command)
        if failure != "not_ready":
            bot.update(run_id="new-bot", live_implementation_admission={"code_sha256": expected,
                "entrypoint_sha256": cache.file_identity(reload.REPO_ROOT / "services/discord_bot/bot.py")["sha256"]})
            bot["startup_inference_warmup"]["run_id"] = "new-bot"
            paths[1].write_text(json.dumps(bot))
        # Native append progress is allowed; rewriting a prior prefix is not.
        with (tmp_path / "events.jsonl").open("a") as handle:
            handle.write("native engine continued\n")
        if failure == "prefix_changed":
            (tmp_path / "fills.jsonl").write_text("corrupted ledger\n")

    monkeypatch.setattr(reload, "owner", owner)
    monkeypatch.setattr(reload.subprocess, "run", restart)
    monkeypatch.setattr(reload, "read_service_ack_pair", lambda *_a, **_kw: (
        reload.read_object(paths[2]), reload.read_object(paths[1]), {"matched": True}))
    ticks = iter((0., 2.))
    monkeypatch.setattr(reload.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "applied.json"
    monkeypatch.setattr("sys.argv", ["reload", "--discord-only", "--apply", "--timeout-seconds", "1",
                                    "--output", str(output)])
    assert reload.main() == (0 if failure is None else 75)
    result = json.loads(output.read_bytes())
    assert len(restarts) == 1 and result["restart_requested"]
    assert result["status"] == ("accepted" if failure is None else "failed_after_restart")
    if failure is None:
        assert not result["engine_restarted"]
        assert result["protected_before"] == result["protected_after"]
        assert audit.verify_prefix(tmp_path, {"inputs": result["ledger_inputs"]}) == result["ledger_prefixes"]
