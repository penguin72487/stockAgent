"""Independent owner updates must not accept stale evidence or change accounts."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import reload_tw_day_trade_services_if_flat as reload

NOW = datetime(2026, 10, 15, 10, 15, tzinfo=ZoneInfo("Asia/Taipei"))


@pytest.mark.parametrize("changed,expected", [
    ({"scripts/serve_tw_day_trade_dashboard.py"}, True),
    (set(reload.READ_MODEL_MEMBERS), True),
    ({"stockagent/live/quote_provider.py"}, False),
    ({"scripts/run_tw_day_trade_simulation.py"}, False),
    ({"stockagent/live/tw_day_trade_simulation.py"}, False),
    (set(), False),
    (set(reload.READ_MODEL_MEMBERS) | {"stockagent/models/factory.py"}, False),
])
def test_read_model_restart_requires_exact_verified_nonexecution_delta(tmp_path, monkeypatch, changed, expected):
    from stockagent.live import service_code_release
    members = set(reload.READ_MODEL_MEMBERS) | {"stockagent/live/quote_provider.py",
        "scripts/run_tw_day_trade_simulation.py", "stockagent/live/tw_day_trade_simulation.py",
        "stockagent/models/factory.py"}
    before = {name: "a" * 64 for name in members}
    after = {name: ("b" if name in changed else "a") * 64 for name in members}
    paths = tmp_path / "previous.json", tmp_path / "candidate.json"
    for path, files in zip(paths, (before, after)):
        path.write_text(json.dumps({"code": {"files": files}}))
    verified = []
    monkeypatch.setattr(service_code_release, "verify_frozen_source", lambda path, _: verified.append(path))
    proof = reload.engine_read_model_scope(*paths)
    assert proof["eligible"] is expected
    assert verified == list(paths)
    assert reload.engine_read_model_scope(None, paths[1])["eligible"] is False
    if expected:
        assert proof["all_signal_history_bytes_verified"] is False
        assert set(proof["financial_journals"]) == {"events.jsonl", "orders.jsonl", "fills.jsonl"}
    after["new-unreviewed-file.py"] = "c" * 64
    paths[1].write_text(json.dumps({"code": {"files": after}}))
    assert reload.engine_read_model_scope(*paths)["eligible"] is False


@pytest.mark.parametrize("journal", ["signals.jsonl", "marks.jsonl", "benchmark_marks.jsonl"])
@pytest.mark.parametrize("damage", ["rewrite", "append", "replace"])
def test_ui_only_generation_fence_rejects_every_projection_change(tmp_path, journal, damage):
    path = tmp_path / journal
    path.write_text("original bytes\n")
    proof = reload.journal_generation_fence(tmp_path, (journal,))
    reload.verify_journal_generation_fence(tmp_path, proof)
    if damage == "rewrite":
        path.write_text("modified bytes\n")
    elif damage == "append":
        with path.open("a") as handle:
            handle.write("new row\n")
    else:
        replacement = tmp_path / "replacement"
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    with pytest.raises(RuntimeError, match="protected journal generation"):
        reload.verify_journal_generation_fence(tmp_path, proof)


def test_ui_only_generation_fence_also_protects_immutable_head(tmp_path):
    from stockagent.live.paper_journal import HEADS, publish_snapshot
    path = tmp_path / "signals.jsonl"
    path.write_text('{"signal":"original"}\n')
    publish_snapshot(path, operation="seed")
    proof = reload.journal_generation_fence(tmp_path, (path.name,))
    assert proof["immutable_layout"]
    with (tmp_path / HEADS / (path.name + ".json")).open("a") as handle:
        handle.write(" ")
    with pytest.raises(RuntimeError, match="protected journal generation"):
        reload.verify_journal_generation_fence(tmp_path, proof)


@pytest.fixture(autouse=True)
def no_host_deployment_state(monkeypatch):
    from stockagent.live import service_code_release
    monkeypatch.setattr(service_code_release, "configured_release", lambda _, **_kw: None)


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


def test_candidate_engine_mode_probe_accepts_only_real_exact_mode_specs(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from stockagent.live import service_code_release

    calls = []
    def probe(release, root, arguments, *, timeout):
        calls.append((release, root, arguments, timeout))
        payload = {"modes": [{"market": "a", "capital": 2_000_000.0},
                             {"market": "b", "capital": 10_000_000.0}], "errors": {}}
        return SimpleNamespace(returncode=0, stdout="STOCKAGENT_ENGINE_MODE_PROBE=" + json.dumps(payload), stderr="")
    monkeypatch.setattr(service_code_release, "run_in_namespace", probe)
    result = reload.verify_engine_release_mode_specs(tmp_path / "release.json", repo_root=tmp_path,
        expected_capitals={"b": 10_000_000, "a": 2_000_000})
    assert result["status"] == "passed"
    assert result["ledger_opened"] is result["native_readiness_claimed"] is False
    assert calls[0][2][-2] == str(tmp_path / "services/discord_bot/markets")
    assert calls[0][3] == 90


@pytest.mark.parametrize("payload", [
    {"modes": [], "errors": {"a": "AttributeError: incompatible TradingConfig"}},
    {"modes": [{"market": "a", "capital": 1_000_000}], "errors": {}},
    {"modes": [{"market": "a", "capital": 2_000_000}, {"market": "a", "capital": 2_000_000}], "errors": {}},
    {"modes": [{"market": "a", "capital": True}], "errors": {}},
    {"modes": [{"market": "a", "capital": 2_000_000}], "errors": {"b": "unavailable"}},
])
def test_candidate_engine_mode_probe_rejects_schema_or_mode_mismatch(monkeypatch, tmp_path, payload):
    from types import SimpleNamespace
    from stockagent.live import service_code_release
    monkeypatch.setattr(service_code_release, "run_in_namespace", lambda *_args, **_kw:
        SimpleNamespace(returncode=0, stdout="STOCKAGENT_ENGINE_MODE_PROBE=" + json.dumps(payload), stderr=""))
    with pytest.raises(ValueError, match="candidate engine"):
        reload.verify_engine_release_mode_specs(tmp_path / "release.json", repo_root=tmp_path,
                                                expected_capitals={"a": 2_000_000})


def test_candidate_engine_mode_probe_rejects_process_failure(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from stockagent.live import service_code_release
    monkeypatch.setattr(service_code_release, "run_in_namespace", lambda *_args, **_kw:
        SimpleNamespace(returncode=1, stdout="", stderr="module mismatch"))
    with pytest.raises(ValueError, match="before restart.*module mismatch"):
        reload.verify_engine_release_mode_specs(tmp_path / "release.json", repo_root=tmp_path,
                                                expected_capitals={"a": 2_000_000})


def test_ack_preflight_waits_for_a_real_fresh_heartbeat_without_extending_ttl(monkeypatch):
    _state, bot, sync = receipts()
    stale = {**sync, "heartbeat_at": (NOW - timedelta(seconds=11)).isoformat()}
    current = {**sync, "heartbeat_at": NOW.isoformat()}
    observed, sleeps = iter((stale, current)), []
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload, "read_service_ack_pair", lambda *_a, **_kw:
                        (next(observed), bot, {"matched": True}))
    monkeypatch.setattr(reload.time, "sleep", sleeps.append)
    actual, _bot, _ack = reload.read_fresh_ack_pair()
    assert actual["heartbeat_at"] == NOW.isoformat()
    assert len(sleeps) == 1
    with pytest.raises(ValueError, match="stale or future receipt"):
        reload.fresh_native_pair(bot, stale, NOW)


def test_ack_preflight_does_not_accept_a_persistently_stale_pair(monkeypatch):
    _state, bot, sync = receipts()
    sync["heartbeat_at"] = (NOW - timedelta(seconds=11)).isoformat()
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    ticks = iter((0., .001, .003))
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(reload, "read_service_ack_pair", lambda *_a, **_kw:
                        (sync, bot, {"matched": True}))
    with pytest.raises(ValueError, match="stale or future receipt"):
        reload.read_fresh_ack_pair(timeout_seconds=.002)


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
                                    "unknown_shares", "pending_commit", "research_pending", "divergence", "wrong_signal",
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
    elif change == "research_pending":
        mode["research_terminal_pending_commit"] = {}
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


@pytest.mark.parametrize("damage", [None, "missing_event", "wrong_quantity", "open", "not_completed", "pending_total"])
def test_previous_terminal_order_inventory_requires_the_existing_full_event_proof(tmp_path, damage):
    from test_tw_day_trade_flat_opening_repair import _settled_terminal_orders
    state, bot, sync = receipts()
    mode, event = _settled_terminal_orders()
    state["enabled_markets"] = ["m"]
    state["modes"] = {"m": mode}
    bot["mode_signal_ids"] = {"m": mode["signal_id"]}
    sync["modes"] = {"m": {"signal_id": mode["signal_id"]}}
    observed = datetime(2026, 10, 8, 14, tzinfo=ZoneInfo("Asia/Taipei"))
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = observed.isoformat()
    events = tmp_path / "events.jsonl"
    if damage == "wrong_quantity": event["cancelled"][1]["shares"] += 1000
    if damage == "open": mode["positions"] = {"p": {"signed_shares": 1000}}
    if damage == "not_completed": mode["execution_evidence_complete"] = False
    if damage == "pending_total": mode["pending_reduction_shares"] = 1000
    events.write_text("" if damage == "missing_event" else json.dumps(event) + "\n")
    if damage:
        with pytest.raises(ValueError, match="nonflat|pending orders"):
            reload.reload_plan(state, bot, sync, now=observed, engine_only=True, events_path=events)
    else:
        plan = reload.reload_plan(state, bot, sync, now=observed, engine_only=True, events_path=events)
        assert plan["pending_orders"] == plan["nonflat_count"] == 0
        assert plan["retained_terminal_orders"] == 3


@pytest.mark.parametrize("damage", [None, "missing_event", "wrong_quantity", "future_settlement",
    "open", "pending_total", "no_execution_proof", "unconsumed_signal", "naive_clock",
    "filled_reduction_progress", "filled_reduction_sequence", "filled_reduction_signal",
    "late_reduction_signal", "price_guard_reason", "price_guard_contract", "price_guard_clock"])
def test_same_day_terminal_orders_need_settlement_and_exact_evidence(tmp_path, damage):
    from test_tw_day_trade_flat_opening_repair import _settled_terminal_orders
    from stockagent.live.tw_day_trade_simulation import ENTRY_PRICE_GUARD_CONTRACT
    state, bot, sync = receipts()
    mode, event = _settled_terminal_orders()
    mode["engine_status"] = "session_flat_after_exit"
    signal = mode["signal_id"]
    mode["pending_reduction_orders"]["1101"] = {
        "status": "filled", "remaining_shares": 0, "requested_shares": 1000,
        "sequence": 1, "signal_at": mode["signal_at"],
    }
    mode["signal_reductions"] = {"1101": {"requested_shares": 1000, "filled_shares": 1000}}
    mode["pending_entry_orders"]["2318"] = {
        "status": "cancelled_entry_price_guard", "sizing_pending": False,
        "remaining_shares": 1000, "entry_price_guard_contract": ENTRY_PRICE_GUARD_CONTRACT,
        "last_wait_reason": "entry_price_at_or_beyond_take_profit", "rejected_entry_price": 110.,
        "cancelled_at": "2026-10-07T13:19:15+08:00", "last_quote_at": "2026-10-07T13:19:14+08:00",
        "position": {"market": "m", "symbol": "2318", "session_date": "2026-10-07",
                     "signal_id": signal, "signal_at": mode["signal_at"], "side": "long",
                     "take_profit_price": 110., "stop_trigger_price": 90.},
    }
    state["enabled_markets"], state["modes"] = ["m"], {"m": mode}
    bot["mode_signal_ids"], sync["modes"] = {"m": signal}, {"m": {"signal_id": signal}}
    observed = datetime(2026, 10, 7, 14, tzinfo=ZoneInfo("Asia/Taipei"))
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = observed.isoformat()
    events = tmp_path / "events.jsonl"
    if damage == "wrong_quantity": event["cancelled"][1]["shares"] += 1000
    if damage == "future_settlement": mode["closing_auction_settled_at"] = "2026-10-07T14:01:00+08:00"
    if damage == "open": mode["positions"] = {"p": {"signed_shares": 1000}}
    if damage == "pending_total": mode["pending_reduction_shares"] = 1000
    if damage == "no_execution_proof": mode["execution_evidence_complete"] = False
    if damage == "unconsumed_signal": mode["processed_signal_ids"] = []
    if damage == "naive_clock": mode["closing_auction_settled_at"] = "2026-10-07T13:30:01"
    if damage == "filled_reduction_progress": mode["signal_reductions"]["1101"]["filled_shares"] = 0
    if damage == "filled_reduction_sequence": mode["pending_reduction_orders"]["1101"]["sequence"] = 0
    if damage == "filled_reduction_signal": mode["pending_reduction_orders"]["1101"]["signal_at"] = "2026-10-06T13:19:00+08:00"
    if damage == "late_reduction_signal":
        mode["signal_at"] = "2026-10-07T13:21:00+08:00"
        for order in mode["pending_reduction_orders"].values(): order["signal_at"] = mode["signal_at"]
    guard = mode["pending_entry_orders"]["2318"]
    if damage == "price_guard_reason": guard["last_wait_reason"] = "invented"
    if damage == "price_guard_contract": guard["entry_price_guard_contract"] = "unknown"
    if damage == "price_guard_clock": guard["last_quote_at"] = "2026-10-07T14:01:00+08:00"
    events.write_text("" if damage == "missing_event" else json.dumps(event) + "\n")
    before = deepcopy((state, bot, sync))
    if damage:
        with pytest.raises(ValueError, match="nonflat|pending orders"):
            reload.reload_plan(state, bot, sync, now=observed, engine_only=True, events_path=events)
    else:
        plan = reload.reload_plan(state, bot, sync, now=observed, engine_only=True, events_path=events)
        assert plan["pending_orders"] == plan["nonflat_count"] == 0
        assert plan["retained_terminal_orders"] == 5
    assert (state, bot, sync) == before


@pytest.mark.parametrize("damage", ["missing_event", "no_execution_proof", "pending_reductions"])
def test_journal_backed_cutover_cannot_bypass_failed_mode_proof_for_entry_only(tmp_path, damage):
    from test_tw_day_trade_flat_opening_repair import _settled_terminal_orders
    state, bot, sync = receipts()
    mode, event = _settled_terminal_orders()
    mode["engine_status"] = "session_flat_after_exit"
    mode["pending_reduction_orders"] = {}
    mode["pending_entry_orders"].pop("2317")
    state["enabled_markets"], state["modes"] = ["m"], {"m": mode}
    bot["mode_signal_ids"] = {"m": mode["signal_id"]}
    sync["modes"] = {"m": {"signal_id": mode["signal_id"]}}
    observed = datetime(2026, 10, 7, 14, tzinfo=ZoneInfo("Asia/Taipei"))
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = observed.isoformat()
    if damage == "no_execution_proof": mode["execution_evidence_complete"] = False
    if damage == "pending_reductions": mode["pending_reduction_shares"] = 1000
    events = tmp_path / "events.jsonl"
    events.write_text("" if damage == "missing_event" else json.dumps(event) + "\n")
    with pytest.raises(ValueError, match="pending orders"):
        reload.reload_plan(state, bot, sync, now=observed, engine_only=True, events_path=events)


@pytest.mark.parametrize("change", [None, "working", "unknown_contract", "wrong_reason", "inside_price",
                                    "wrong_session", "wrong_signal", "missing_clock", "future_clock", "unsized", "open_position"])
def test_only_proven_entry_bracket_cancellations_are_terminal(change):
    from stockagent.live.tw_day_trade_simulation import ENTRY_PRICE_GUARD_CONTRACT
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    mode["positions"].clear()
    mode["session_date"] = NOW.date().isoformat()
    order = {
        "status": "cancelled_entry_price_guard", "sizing_pending": False,
        "remaining_shares": 2000, "entry_price_guard_contract": ENTRY_PRICE_GUARD_CONTRACT,
        "last_wait_reason": "entry_price_at_or_beyond_take_profit", "rejected_entry_price": 1100.,
        "cancelled_at": NOW.isoformat(), "last_quote_at": NOW.isoformat(),
        "position": {"signal_id": "immutable", "session_date": NOW.date().isoformat(),
                     "side": "long", "take_profit_price": 1100., "stop_trigger_price": 900.},
    }
    mode["pending_entry_orders"] = {"2330": order}
    closed = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = closed.isoformat()
    if change == "working":
        order["status"] = "working"
    elif change == "unknown_contract":
        order["entry_price_guard_contract"] = "unknown"
    elif change == "wrong_reason":
        order["last_wait_reason"] = "marketable_depth_unavailable"
    elif change == "inside_price":
        order["rejected_entry_price"] = 1000.
    elif change == "wrong_session":
        order["position"]["session_date"] = "2026-10-14"
    elif change == "wrong_signal":
        order["position"]["signal_id"] = "other"
    elif change == "missing_clock":
        order.pop("cancelled_at")
    elif change == "future_clock":
        order["cancelled_at"] = (closed + timedelta(seconds=1)).isoformat()
    elif change == "unsized":
        order["sizing_pending"] = True
    elif change == "open_position":
        mode["positions"] = {"2330": {"signed_shares": 1000}}
    if change is None:
        plan = reload.reload_plan(state, bot, sync, now=closed, engine_only=True)
        assert plan["pending_orders"] == plan["nonflat_count"] == 0
    else:
        with pytest.raises(ValueError, match="nonflat|pending orders"):
            reload.reload_plan(state, bot, sync, now=closed, engine_only=True)


@pytest.mark.parametrize("change", [None, "working", "other_status", "wrong_signal", "wrong_session",
                                    "wrong_market", "wrong_symbol", "missing_settlement", "early_settlement",
                                    "future_settlement", "naive_settlement", "pending_total", "unknown_size",
                                    "negative_remaining", "open_position", "prior_consumed_signal",
                                    "malformed_consumed_ids", "consumed_wrong_session"])
def test_only_settled_exit_priority_entry_cancellation_is_terminal(change):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    mode.update(positions={}, session_date=NOW.date().isoformat(),
                engine_status="session_flat_after_exit", pending_entry_shares=0,
                closing_auction_settled_at=NOW.replace(hour=13, minute=30).isoformat())
    order = {"status": "cancelled_exit_priority", "sizing_pending": False,
             "remaining_shares": 2000,
             "position": {"signal_id": "immutable", "session_date": NOW.date().isoformat(),
                          "market": "tw_day_trade_100m", "symbol": "2330"}}
    mode["pending_entry_orders"] = {"2330": order}
    closed = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = closed.isoformat()
    if change == "working":
        order["status"] = "working"
    elif change == "other_status":
        mode["engine_status"] = "entry_pending"
    elif change == "wrong_signal":
        order["position"]["signal_id"] = "other"
    elif change == "wrong_session":
        order["position"]["session_date"] = "2026-10-14"
    elif change == "wrong_market":
        order["position"]["market"] = "other"
    elif change == "wrong_symbol":
        order["position"]["symbol"] = "0050"
    elif change == "missing_settlement":
        mode.pop("closing_auction_settled_at")
    elif change == "early_settlement":
        mode["closing_auction_settled_at"] = NOW.replace(hour=13, minute=29).isoformat()
    elif change == "future_settlement":
        mode["closing_auction_settled_at"] = (closed + timedelta(seconds=1)).isoformat()
    elif change == "naive_settlement":
        mode["closing_auction_settled_at"] = "2026-10-15T13:30:00"
    elif change == "pending_total":
        mode["pending_entry_shares"] = 2000
    elif change == "unknown_size":
        order["sizing_pending"] = None
    elif change == "negative_remaining":
        order["remaining_shares"] = -1
    elif change == "open_position":
        mode["positions"] = {"2330": {"signed_shares": 1000}}
    elif change == "prior_consumed_signal":
        mode["processed_signal_ids"] = ["immutable", "post-close-cache"]
        mode["signal_id"] = bot["mode_signal_ids"]["tw_day_trade_100m"] = "post-close-cache"
        sync["modes"]["tw_day_trade_100m"]["signal_id"] = "post-close-cache"
    elif change == "malformed_consumed_ids":
        order["position"]["signal_id"] = "prior"
        mode["processed_signal_ids"] = {"prior": True}
    elif change == "consumed_wrong_session":
        order["position"]["signal_id"] = "prior"
        order["position"]["session_date"] = "2026-10-14"
        mode["processed_signal_ids"] = ["prior"]
    if change in (None, "prior_consumed_signal"):
        plan = reload.reload_plan(state, bot, sync, now=closed, engine_only=True)
        assert plan["pending_orders"] == plan["nonflat_count"] == 0
        assert plan["retained_terminal_orders"] == 1
    else:
        with pytest.raises(ValueError, match="nonflat|pending orders"):
            reload.reload_plan(state, bot, sync, now=closed, engine_only=True)


@pytest.mark.parametrize("change", [None, "same_day", "wrong_status", "missing_evidence",
    "missing_pending_reductions", "pending_reductions", "pending_auction", "unresolved_exit",
    "missing_position_quantity", "open_position", "missing_settlement", "wrong_session",
    "unconsumed_signal", "matching_but_unconsumed"])
def test_previous_session_cancelled_entry_survives_overnight_readiness_status(change):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    prior = NOW.date().isoformat()
    mode.update(positions={"cohort": {"signed_shares": 0}}, session_date=prior,
                engine_status="waiting_09_00_signal", pending_entry_shares=0,
                pending_reduction_shares=0, closing_auction_pending_count=0,
                unresolved_exit_count=0, execution_evidence_complete=True,
                closing_auction_settled_at=NOW.replace(hour=13, minute=30).isoformat(),
                processed_signal_ids=["immutable"])
    order = {"status": "cancelled_exit_priority", "sizing_pending": False,
             "remaining_shares": 2000,
             "position": {"signal_id": "immutable", "session_date": prior,
                          "market": "tw_day_trade_100m", "symbol": "2330"}}
    mode["pending_entry_orders"] = {"2330": order}
    mode["pending_reduction_orders"] = {}
    observed = (NOW + timedelta(days=1)).replace(hour=0, minute=15)
    if change == "same_day":
        observed = NOW.replace(hour=14)
    elif change == "wrong_status":
        mode["engine_status"] = "waiting_signal"
    elif change == "missing_evidence":
        mode["execution_evidence_complete"] = False
    elif change == "missing_pending_reductions":
        mode.pop("pending_reduction_shares")
    elif change == "pending_reductions":
        mode["pending_reduction_shares"] = 1000
    elif change == "pending_auction":
        mode["closing_auction_pending_count"] = 1
    elif change == "unresolved_exit":
        mode["unresolved_exit_count"] = 1
    elif change == "missing_position_quantity":
        mode["positions"]["cohort"] = {}
    elif change == "open_position":
        mode["positions"]["cohort"]["signed_shares"] = 1000
    elif change == "missing_settlement":
        mode.pop("closing_auction_settled_at")
    elif change == "wrong_session":
        order["position"]["session_date"] = "2026-10-14"
    elif change == "unconsumed_signal":
        order["position"]["signal_id"] = "unknown"
    elif change == "matching_but_unconsumed":
        mode["processed_signal_ids"] = []
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = observed.isoformat()
    if change is None:
        plan = reload.reload_plan(state, bot, sync, now=observed, engine_only=True)
        assert plan["pending_orders"] == plan["nonflat_count"] == 0
        assert plan["retained_terminal_orders"] == 1
    else:
        with pytest.raises(ValueError, match="nonflat|pending orders|unknown position quantity"):
            reload.reload_plan(state, bot, sync, now=observed, engine_only=True)


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


@pytest.mark.parametrize("status,same_run", [
    ("pending", True), ("pending", False), ("running", True),
    ("degraded", True), ("unknown", True),
])
def test_idle_engine_recovery_does_not_require_scheduled_bot_gpu_warmup(status, same_run):
    state, bot, sync = receipts()
    mode = state["modes"]["tw_day_trade_100m"]
    mode["positions"].clear()
    mode["pending_entry_orders"].clear()
    now = NOW.replace(hour=14)
    bot["updated_at"] = bot["engine_published_at"] = sync["published_at"] = now.isoformat()
    bot["startup_inference_warmup"].update(status=status,
        run_id=bot["run_id"] if same_run else "different-run", model_cache_resident_ready=False)
    original = deepcopy((state, bot, sync))
    if status == "pending" and same_run:
        plan = reload.reload_plan(state, bot, sync, now=now, engine_only=True)
        assert plan["services"] == [reload.SERVICES[1]]
        assert plan["previous_discord_run_id"] == bot["run_id"]
        # A joint restart replaces the bot; it has no unchanged-owner exemption.
        with pytest.raises(ValueError, match="warmup"):
            reload.reload_plan(state, bot, sync, now=now)
    else:
        with pytest.raises(ValueError, match="warmup"):
            reload.reload_plan(state, bot, sync, now=now, engine_only=True)
    assert (state, bot, sync) == original


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


@pytest.mark.parametrize("wait", ["-1", "121", "nan", "inf"])
def test_control_wait_rejects_invalid_bounds_before_any_lock(wait, monkeypatch):
    monkeypatch.setattr(reload, "control_lease", lambda **_: pytest.fail("invalid wait must not acquire a lease"))
    monkeypatch.setattr("sys.argv", ["reload", "--control-wait-seconds", wait])
    with pytest.raises(SystemExit):
        reload.main()


def test_control_wait_uses_existing_lease_and_never_restarts_when_busy(tmp_path, monkeypatch):
    calls = []
    def busy(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("paper service control is held by another coordinator")
    monkeypatch.setattr(reload, "control_lease", busy)
    monkeypatch.setattr(reload, "read_service_ack_pair", lambda *a, **kw: pytest.fail("busy lease must not inspect or restart"))
    output = tmp_path / "busy.json"
    monkeypatch.setattr("sys.argv", ["reload", "--control-wait-seconds", "2.5", "--apply", "--output", str(output)])
    assert reload.main() == 75
    assert calls == [{"owner": "qualified-paper-service-reload", "timeout_seconds": 2.5}]
    result = json.loads(output.read_bytes())
    assert result["status"] == "blocked" and not result["restart_requested"]
    assert result["control_wait_ms"] >= 0 and result["broker_orders"] == 0


@pytest.mark.parametrize("engine_only,code_only,engine_pinned", [
    (False, False, False), (True, False, False), (False, True, False), (True, False, True)])
@pytest.mark.parametrize("pinned", [False, True])
@pytest.mark.parametrize("failure", [None, "prefix_changed", "protected_changed", "not_ready", "accounting_changed",
                                    "preexisting_old_code", "preexisting_old_entrypoint", "malformed_admission",
                                    "reported_current_code", "journal_fatal", "journal_pending", "slow_prefix_check",
                                    "heartbeat_boundary_after_prefix",
                                    "code_changed_during_start", "pinned_changed_during_start", "pin_install_failed",
                                    "source_update_pending", "engine_workspace_changed"])
def test_apply_restarts_only_selected_owner_and_requires_independent_acceptance(tmp_path, monkeypatch, failure, engine_only, code_only, pinned, engine_pinned, read_model_only=False):
    from contextlib import contextmanager, nullcontext
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
    from stockagent.live.paper_journal import FILENAMES
    for name in FILENAMES:
        (tmp_path / name).write_text("immutable ledger prefix\n")
    monkeypatch.setattr(reload, "datetime", Clock)
    monkeypatch.setattr(reload, "control_lease", lambda **_: nullcontext())
    code_changed = [False]
    engine_workspace_changed = [False]
    original_identity = cache.file_identity
    def file_identity(path):
        result = original_identity(path)
        if engine_workspace_changed[0] and str(path) == str(reload.REPO_ROOT / service_code_release.ENGINE_CODE_FILES[0]):
            return {**result, "sha256": "changed mutable workspace, not the pinned engine"}
        return result
    monkeypatch.setattr(cache, "file_identity", file_identity)
    monkeypatch.setattr(cache, "implementation_identity", lambda: [{"qualified": "changed" if code_changed[0] else "implementation"}])
    expected = cache.identity_digest({"code": cache.implementation_identity()})
    frozen_changed, installs = [False], []
    if pinned or engine_pinned:
        pinned_code = cache.implementation_identity()
        pinned_entry = cache.file_identity(reload.REPO_ROOT / "services/discord_bot/bot.py")
        path = tmp_path / "release.json"
        engine_path = tmp_path / "engine-release.json"
        if read_model_only:
            state["modes"]["tw_day_trade_100m"]["initial_capital_twd"] = 100_000_000.0
            paths[0].write_text(json.dumps(state))
            monkeypatch.setattr(reload, "engine_read_model_scope", lambda *a, **k:
                {"eligible": True, "previous_release": str(engine_path),
                 "all_signal_history_bytes_verified": False})
            monkeypatch.setattr(reload, "verify_engine_release_mode_specs",
                                lambda *a, **k: {"status": "passed"})
        monkeypatch.setattr(service_code_release, "DROP_IN", tmp_path / "code.conf")
        monkeypatch.setattr(service_code_release, "ENGINE_DROP_IN", tmp_path / "engine-code.conf")
        monkeypatch.setattr(service_code_release, "configured_release",
                            lambda _, **kw: (engine_path if engine_pinned else None) if kw.get("role") == "engine"
                            else path if pinned else None)
        monkeypatch.setattr(service_code_release, "verify_frozen_source", lambda value, *_: {"receipt": str(value)})
        def namespace(*_a, **kw):
            if kw.get("role") == "engine":
                return {"engine_code": [cache.file_identity(reload.REPO_ROOT / name)
                                        for name in service_code_release.ENGINE_CODE_FILES]}
            return {"code": pinned_code, "entrypoint": pinned_entry, "readonly_verified": True}
        monkeypatch.setattr(service_code_release, "namespace_identity", namespace)
        monkeypatch.setattr(service_code_release, "frozen_generation", lambda *_: (frozen_changed[0],))
        monkeypatch.setattr(service_code_release, "verify_owner_namespace", lambda *_a, **_kw: {"readonly_mounts_verified": True})
        monkeypatch.setattr(service_code_release, "native_owner_pid", lambda *_a, **_kw: 200)
        def install(*args, **kw):
            installs.append((args, kw))
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
    source_held, prefix_calls = [False], []
    source_exempt = bool(pinned and code_only and not engine_only)
    @contextmanager
    def source_guard(_):
        if failure == "source_update_pending":
            raise tw_public_source_guard.TWPublicSourceUpdatePending(Path("source"), Path("source.lock"))
        source_held[0] = True
        try:
            yield
        finally:
            source_held[0] = False
    monkeypatch.setattr(tw_public_source_guard, "live_tw_public_source_read_guard", source_guard)
    real_prefix = audit.prefix_sha256
    def prefix(path, size):
        if len(prefix_calls) < (6 if engine_pinned and not read_model_only else 3):
            assert source_held[0] or source_exempt, "data-reading reload must admit the source before scanning"
        prefix_calls.append((path, size))
        return real_prefix(path, size)
    monkeypatch.setattr(audit, "prefix_sha256", prefix)
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
        if failure == "engine_workspace_changed":
            engine_workspace_changed[0] = True
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
        if failure == "signal_generation_changed":
            (tmp_path / "signals.jsonl").write_text("changed un-hashed signals\n")

    monkeypatch.setattr(reload, "owner", owner)
    monkeypatch.setattr(reload.subprocess, "run", restart)
    prefix_completed = [False]
    stale_final_samples = []
    def pair(*_args, **_kwargs):
        observed = reload.read_object(paths[2]), reload.read_object(paths[1]), {"matched": True}
        if failure == "heartbeat_boundary_after_prefix" and prefix_completed[0] and not stale_final_samples:
            stale_final_samples.append(observed)
            # The exact ACK may be old while the closed owner is between
            # heartbeats. Only the NEXT actual fresh sample may be accepted.
            bot["updated_at"] = sync["heartbeat_at"] = (now+timedelta(seconds=20)).isoformat()
            paths[1].write_text(json.dumps(bot))
            paths[2].write_text(json.dumps(sync))
        return observed
    monkeypatch.setattr(reload, "read_service_ack_pair", pair)
    if failure in {"slow_prefix_check", "heartbeat_boundary_after_prefix"}:
        verify_prefix = audit.verify_prefix
        def slow_prefix(*args, **kwargs):
            result = verify_prefix(*args, **kwargs)
            Clock.offset_seconds = 20
            prefix_completed[0] = True
            if failure == "slow_prefix_check":
                bot["updated_at"] = sync["heartbeat_at"] = (now+timedelta(seconds=20)).isoformat()
                paths[1].write_text(json.dumps(bot))
                paths[2].write_text(json.dumps(sync))
            return result
        monkeypatch.setattr(audit, "verify_prefix", slow_prefix)
    ticks = iter((0., 2.))
    monkeypatch.setattr(reload.time, "monotonic", lambda: next(ticks))
    output = tmp_path / "applied.json"
    plan_event_paths = []
    real_reload_plan = reload.reload_plan
    def inspect_plan(*args, **kwargs):
        plan_event_paths.append(kwargs.get("events_path"))
        return real_reload_plan(*args, **kwargs)
    monkeypatch.setattr(reload, "reload_plan", inspect_plan)
    monkeypatch.setattr("sys.argv", ["reload", "--engine-only" if engine_only else "--discord-only", "--apply", "--timeout-seconds", "1",
                                    "--output", str(output), *(["--code-admission-only"] if code_only else []),
                                    *(["--engine-source-release", str(engine_path)] if read_model_only else [])])
    accepted = failure is None or ((failure == "accounting_changed" or preflight_mismatch) and not engine_only)
    accepted = accepted or (not code_only and failure in {"reported_current_code", "journal_fatal", "journal_pending"})
    accepted = accepted or failure in {"slow_prefix_check", "heartbeat_boundary_after_prefix"}
    accepted = accepted or (failure == "engine_workspace_changed" and engine_pinned)
    accepted = accepted or (pinned and failure == "code_changed_during_start")
    accepted = accepted or (not pinned and not engine_pinned and failure == "pinned_changed_during_start")
    accepted = accepted or (failure == "pin_install_failed" and not engine_pinned and (not pinned or engine_only))
    accepted = accepted or (failure == "source_update_pending" and source_exempt)
    assert reload.main() == (0 if accepted else 75)
    result = json.loads(output.read_bytes())
    assert result["control_wait_ms"] >= 0
    if failure == "source_update_pending" and not source_exempt:
        assert not prefix_calls and not restarts and not result["restart_requested"]
        assert result["status"] == "blocked" and result["error_type"] == "TWPublicSourceUpdatePending"
        return
    assert result["source_read_admission_required"] is not source_exempt
    assert result["data_or_gpu_readiness_claim"] is not code_only
    if engine_only and preflight_mismatch or code_only and failure == "reported_current_code":
        assert not restarts and not result["restart_requested"]
        assert result["status"] == "blocked" and ("code admission" in result["error"] or "code drift" in result["error"])
        return
    if (pinned and not engine_only or engine_pinned) and failure == "pin_install_failed":
        assert not restarts and not result["restart_requested"]
        assert result["status"] == "blocked" and "unit preview" in result["error"]
        return
    assert len(restarts) == 1 and result["restart_requested"]
    assert plan_event_paths[:2] == [tmp_path / "events.jsonl", tmp_path / "events.jsonl"]
    assert result["status"] == (("accepted_code_admission_only" if code_only else "accepted")
                                if accepted else "failed_after_restart")
    if (failure == "code_changed_during_start" and not pinned
            or failure == "pinned_changed_during_start" and (pinned or engine_pinned)):
        assert result["error_type"] == "RuntimeError" and "code changed during startup" in result["error"]
    if accepted:
        if failure in {"slow_prefix_check", "heartbeat_boundary_after_prefix"}:
            assert result["engine_heartbeat_at"] == (now+timedelta(seconds=20)).isoformat()
        if failure == "heartbeat_boundary_after_prefix":
            assert len(stale_final_samples) == 1
        assert result["gpu_warmup_verified"] is not (code_only or engine_only)
        if code_only:
            assert result["gpu_warmup_verified"] is False
            assert result["acceptance_scope"] == "loaded_code_gateway_ack_not_gpu_readiness"
        assert result["engine_restarted"] is engine_only
        if engine_only:
            assert result["acceptance_scope"] == "engine_restart_inventory_code_gateway_ack_not_gpu_readiness"
        if pinned:
            assert result["native_code_namespace"]["readonly_mounts_verified"]
        assert len(installs) == int(pinned and not engine_only) + int(engine_pinned)
        if engine_pinned:
            assert result["native_engine_code_namespace"]["readonly_mounts_verified"]
            assert result["engine_code_drop_in_installed"] is True
            assert set(result["ledger_inputs"]) == (set(reload.FINANCIAL_JOURNALS) if read_model_only else set(FILENAMES))
            if read_model_only:
                assert set(result["protected_journal_generations"]["files"]) == set(FILENAMES) - set(reload.FINANCIAL_JOURNALS)
            assert installs[-1][1] == {"role": "engine"}
        assert result["inventory_restored_exactly"] is engine_only
        assert result["protected_before"] == result["protected_after"]
        if engine_only:
            assert reload.SERVICES[0] in result["protected_after"]
            assert result["new_discord_run_id"] == ("100-old-bot" if pinned else "old-bot")
            assert result["native_warmup"]["model_cache_resident_ready"] is False
        assert audit.verify_prefix(tmp_path, {"inputs": result["ledger_inputs"]}) == result["ledger_prefixes"]


@pytest.mark.parametrize("failure", [None, "prefix_changed", "signal_generation_changed",
                                   "protected_changed", "accounting_changed", "source_update_pending"])
def test_ui_only_reload_retains_financial_prefix_state_and_owner_gates(tmp_path, monkeypatch, failure):
    test_apply_restarts_only_selected_owner_and_requires_independent_acceptance(
        tmp_path, monkeypatch, failure, engine_only=True, code_only=False,
        pinned=True, engine_pinned=True, read_model_only=True)


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


@pytest.mark.parametrize("flags", [[], ["--discord-only"], ["--discord-only", "--code-admission-only"]])
def test_engine_source_selection_requires_independent_engine_owner(monkeypatch, flags):
    monkeypatch.setattr("sys.argv", ["reload", *flags, "--engine-source-release", "release.json", "--apply"])
    monkeypatch.setattr(reload, "control_lease", lambda **_: pytest.fail("invalid owner selection acquired a lease"))
    with pytest.raises(SystemExit):
        reload.main()


@pytest.mark.parametrize("pinned", [False, True])
def test_engine_startup_proves_pinned_owner_before_opening_ledger(tmp_path, monkeypatch, pinned):
    from scripts import run_tw_day_trade_simulation as runner
    from stockagent.live import service_code_release
    calls = []
    monkeypatch.delenv(service_code_release.ENVIRONMENT, raising=False)
    if pinned:
        monkeypatch.setenv(service_code_release.ENVIRONMENT, str(tmp_path / "release.json"))
    def check(*args, **kw):
        calls.append((args, kw))
        raise ValueError("unaccepted native pin")
    monkeypatch.setattr(service_code_release, "verify_startup_namespace", check)
    def lock(_):
        calls.append("ledger")
        raise RuntimeError("legacy ledger boundary reached")
    monkeypatch.setattr(runner, "_acquire_engine_lock", lock)
    with pytest.raises(ValueError if pinned else RuntimeError, match="unaccepted native pin|legacy ledger"):
        runner.main(["--once"])
    if pinned:
        assert calls == [((runner.REPO_ROOT,), {"role": "engine"})]
    else:
        assert calls == ["ledger"]


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
    assert len(observations) == 1 and 0 < observations[0][1]["timeout_seconds"] <= 5.
    assert json.loads(output.read_text())["restart_requested"] is False
