from __future__ import annotations

from datetime import datetime, timedelta
import fcntl
import json
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from scripts import watch_tw_public_source_events as events
from scripts import run_tw_public_0830_check as acceptance
from stockagent.data.tw_public_source_receipts import DERIVED_INPUT_RECEIPTS


NOW = datetime(2026, 10, 14, 18, 5, tzinfo=events.TAIPEI)


def dependency_manifests(root: Path) -> None:
    for relative, keys in DERIVED_INPUT_RECEIPTS.values():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            keys[0]: [{"path": "tpex_basic_company.parquet", "sha256": "abc", "size": 1}],
        }), encoding="utf-8")


def queue(state, *, reason="derived_source_content_changed", observed=NOW):
    events._queue_completed_session_reconcile(
        state, datasets=["tpex_basic_company"], reason=reason, observed=observed,
    )


def drain(state, root, monkeypatch, *, observed=NOW, status="ok"):
    calls = []
    monkeypatch.setattr(events, "_latest_completed_taiex_session", lambda *_a, **_kw: "2026-10-14")

    def finalizer(request, **kwargs):
        # Crash recovery work must be durable before a real subprocess starts.
        persisted = json.loads((kwargs["state_root"] / "latest.json").read_text())
        assert persisted["completed_session_pending"]
        calls.append(request)
        return {"status": status, "expected_date": "2026-10-14"}

    monkeypatch.setattr(events, "_finalize_completed_session_close_event", finalizer)
    result = events._drain_completed_session_reconcile(
        state, live_root=root, state_root=root / "events", heartbeat_seconds=30,
        observed=observed,
    )
    return result, calls


def test_dependency_router_and_validator_share_keys(tmp_path, monkeypatch):
    dependency_manifests(tmp_path)
    assert events._derived_event_dependencies(tmp_path) == {"tpex_basic_company"}
    captured = []
    monkeypatch.setattr(acceptance, "_derived_data_dates", lambda *_: {})
    monkeypatch.setattr(acceptance, "_receipt_dependency_errors", lambda path, **kw: (captured.append((path, kw["keys"])) or []))
    acceptance._derived_data_status(tmp_path, expected_latest="2026-10-14", session_date="2026-10-14")
    assert captured == [(tmp_path / relative, keys) for relative, keys in DERIVED_INPUT_RECEIPTS.values()]


@pytest.mark.parametrize("changed,accepted,expected", [
    ([{"dataset": "tpex_basic_company"}], ["tpex_basic_company"], True),
    ([{"dataset": "unrelated_table"}], ["unrelated_table"], False),
    ([{"dataset": "tpex_basic_company"}], [], False),
    ([], ["tpex_basic_company"], False),
])
def test_only_real_accepted_dependency_changes_queue(tmp_path, changed, accepted, expected):
    dependency_manifests(tmp_path)
    state = {}
    events._queue_completed_source_refresh(state, {
        "accepted_datasets": accepted, "content_changed_datasets": changed,
    }, live_root=tmp_path, observed=NOW)
    assert bool(state.get("completed_session_pending")) is expected


@pytest.mark.parametrize("damage", ["missing", "malformed", "oversize", "no_inputs"])
def test_unreadable_routing_metadata_defers_to_canonical_validator(tmp_path, damage):
    dependency_manifests(tmp_path)
    path = tmp_path / DERIVED_INPUT_RECEIPTS["stock_panel"][0]
    if damage == "missing":
        path.unlink()
    else:
        path.write_text({
            "malformed": "{", "oversize": " " * (events.DEPENDENCY_SUMMARY_MAX_BYTES + 1),
            "no_inputs": "{}",
        }[damage])
    assert events._derived_event_dependencies(tmp_path) is None
    state = {}
    events._queue_completed_source_refresh(state, {
        "accepted_datasets": ["some_new_table"],
        "content_changed_datasets": [{"dataset": "some_new_table"}],
    }, live_root=tmp_path, observed=NOW)
    assert state["completed_session_pending"]["datasets"] == ["some_new_table"]


def test_real_source_writer_routes_conservatively_without_blocking(tmp_path):
    root = tmp_path / "live"
    dependency_manifests(root)
    (root / "twse_daily_ohlcv.parquet").touch()
    lock = tmp_path / ".locks" / "tw-public-refresh.lock"
    lock.parent.mkdir()
    with lock.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX)
        assert events._derived_event_dependencies(root) is None


def test_changed_batch_is_coalesced_to_one_finalizer(tmp_path, monkeypatch):
    state = {}
    queue(state)
    events._queue_completed_session_reconcile(state, datasets=["tpex_attention_stock"], reason="derived_source_content_changed", observed=NOW)
    result, calls = drain(state, tmp_path, monkeypatch)
    assert len(calls) == 1
    assert result["triggered_datasets"] == ["tpex_attention_stock", "tpex_basic_company"]
    assert "completed_session_pending" not in state
    again, _ = drain(state, tmp_path, monkeypatch)
    assert again is None


@pytest.mark.parametrize("offset,reason,expected", [
    (0, "derived_source_content_changed", "coalescing"),
    (60, "derived_source_content_changed", "ok"),
    (0, "official_close_accepted", "ok"),
])
def test_known_burst_wait_is_bounded_and_never_delays_official_close(tmp_path, monkeypatch, offset, reason, expected):
    dependency_manifests(tmp_path)
    state = {"datasets": {"tpex_basic_company": {"observed_version": "new", "applied_version": "old"}}}
    queue(state, reason=reason)
    result, calls = drain(state, tmp_path, monkeypatch, observed=NOW + timedelta(seconds=offset))
    assert result["status"] == expected
    assert len(calls) == int(expected == "ok")


def test_failed_download_does_not_block_known_good_derived_update(tmp_path, monkeypatch):
    dependency_manifests(tmp_path)
    state = {"datasets": {"tpex_basic_company": {
        "observed_version": "new", "applied_version": "old",
        "next_download_retry_at_taipei": (NOW + timedelta(seconds=60)).isoformat(),
    }}}
    queue(state)
    result, calls = drain(state, tmp_path, monkeypatch)
    assert result["status"] == "ok" and len(calls) == 1


def test_retry_lane_survives_download_ack_and_state_reload(tmp_path, monkeypatch):
    state = {"datasets": {"tpex_basic_company": {"observed_version": "v2", "applied_version": "v2"}}}
    queue(state)
    result, calls = drain(state, tmp_path, monkeypatch, status="failed")
    assert result["status"] == "failed" and len(calls) == 1
    pending = state["completed_session_pending"]
    assert pending["failure_count"] == 1
    retry_at = events._parse_timestamp(pending["next_retry_at_taipei"])
    events._atomic_json(tmp_path / "events" / "latest.json", state)
    restored = events._read_json(tmp_path / "events" / "latest.json")
    deferred, calls = drain(restored, tmp_path, monkeypatch, observed=retry_at - timedelta(seconds=1))
    assert deferred["status"] == "retry_deferred" and not calls
    recovered, calls = drain(restored, tmp_path, monkeypatch, observed=retry_at)
    assert recovered["status"] == "ok" and len(calls) == 1
    assert "completed_session_pending" not in restored


def test_retry_delay_stops_at_five_minutes(tmp_path, monkeypatch):
    state = {}
    queue(state)
    state["completed_session_pending"]["failure_count"] = 20
    drain(state, tmp_path, monkeypatch, status="failed")
    retry = events._parse_timestamp(state["completed_session_pending"]["next_retry_at_taipei"])
    finished = events._parse_timestamp(state["completed_session_finalize"]["completed_at_taipei"])
    assert (retry - finished).total_seconds() == 300


def test_pending_reconcile_never_runs_in_opening_window(tmp_path, monkeypatch):
    state = {}
    queue(state)
    result, calls = drain(state, tmp_path, monkeypatch, observed=NOW.replace(hour=9))
    assert result["status"] == "waiting_close_window" and not calls
    assert state["completed_session_pending"]


def test_actual_new_close_does_not_inherit_not_ready_backoff(tmp_path, monkeypatch):
    state = {}
    queue(state)
    state["completed_session_pending"]["next_retry_at_taipei"] = (NOW + timedelta(minutes=5)).isoformat()
    queue(state, reason="official_close_accepted")
    result, calls = drain(state, tmp_path, monkeypatch)
    assert result["status"] == "ok" and len(calls) == 1


def test_unchanged_source_refresh_performs_no_manifest_or_source_reads(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("an unchanged source refresh must not scan inputs")

    monkeypatch.setattr(events, "_derived_event_dependencies", forbidden)
    state = {}
    events._queue_completed_source_refresh(state, {
        "accepted_datasets": ["tpex_basic_company"], "content_changed_datasets": [],
    }, live_root=tmp_path, observed=NOW)
    assert not state


@pytest.mark.parametrize("probe_only", [False, True])
def test_main_routes_source_update_through_same_serial_owner(tmp_path, monkeypatch, probe_only):
    dependency_manifests(tmp_path)
    state = {"schema_version": 1, "datasets": {"tpex_basic_company": {
        "observed_version": "v2", "applied_version": "v1",
    }}}
    args = SimpleNamespace(
        live_root=tmp_path, state_root=tmp_path / "events", once=True,
        no_bootstrap_refresh=False, probe_only=probe_only,
        workers=1, date_workers=1, probe_workers=1, timeout=20, retries=2,
        heartbeat_seconds=30, fast_interval_seconds=60,
        close_probe_interval_seconds=5, medium_interval_seconds=300,
        slow_interval_seconds=300, max_refresh_datasets_per_cycle=12,
    )
    monkeypatch.setattr(events, "parse_args", lambda: args)
    monkeypatch.setattr(events, "_STOP", threading.Event())
    monkeypatch.setattr(events.signal, "signal", lambda *_: None)
    monkeypatch.setattr(events, "notify_systemd", lambda *_: None)
    monkeypatch.setattr(events, "_load_state", lambda *_: state)
    monkeypatch.setattr(events, "SharedRateLimiter", lambda *_a, **_kw: None)
    monkeypatch.setattr(events, "_due_specs", lambda *_: [events.DEFAULT_DATASETS["tpex_basic_company"]])
    monkeypatch.setattr(events, "_prioritize_unpublished_close_event", lambda due, **_kw: due)
    monkeypatch.setattr(events, "_probe_specs", lambda *_a, **_kw: [])
    monkeypatch.setattr(events, "_apply_probe_results", lambda *_a, **_kw: [])
    monkeypatch.setattr(events, "active_opening_revision_freeze", lambda *_a, **_kw: None)
    monkeypatch.setattr(events, "_publish_event_close_receipt_if_ready", lambda **_kw: {"status": "not_applicable"})
    monkeypatch.setattr(events, "_latest_completed_taiex_session", lambda *_a, **_kw: "2026-10-14")
    calls = []

    def refresh(*_a, **_kw):
        calls.append("download")
        state["datasets"]["tpex_basic_company"]["applied_version"] = "v2"
        return {
            "status": "ok", "accepted_datasets": ["tpex_basic_company"],
            "content_changed_datasets": [{"dataset": "tpex_basic_company"}],
            "failed_dataset_count": 0, "completed_at_taipei": NOW.isoformat(),
        }

    def finalize(request, **kwargs):
        calls.append("finalize")
        persisted = events._read_json(kwargs["state_root"] / "latest.json")
        assert persisted["completed_session_pending"]["datasets"] == ["tpex_basic_company"]
        assert persisted["datasets"]["tpex_basic_company"]["applied_version"] == "v2"
        return {"status": "ok", "expected_date": "2026-10-14"}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(events, "datetime", Clock)
    monkeypatch.setattr(events, "_refresh_pending", refresh)
    monkeypatch.setattr(events, "_finalize_completed_session_close_event", finalize)
    events.main()
    assert calls == ([] if probe_only else ["download", "finalize"])
    assert "completed_session_pending" not in state


@pytest.mark.parametrize("fresh,status,current,exit_code,expected", [
    (True, "ok", True, 0, "ok"),
    (False, "ok", True, 0, "failed"),
    (True, "ok", False, 0, "failed"),
    (True, "failed", True, 0, "failed"),
    (True, "ok", True, 1, "failed"),
])
def test_reused_close_cannot_skip_current_generation_or_use_old_receipt(tmp_path, monkeypatch, fresh, status, current, exit_code, expected):
    receipt = tmp_path / "completed_session" / "latest.json"
    receipt.parent.mkdir()
    payload = {"status": "ok", "expected_date": "2026-10-14", "after": {"current": True}}
    events._atomic_json(receipt, payload)
    calls = []

    def launch(command, cwd):
        calls.append(command)
        assert Path(command[command.index("--receipt") + 1]) == receipt
        if fresh:
            events._atomic_json(receipt, {**payload, "status": status, "after": {"current": current}})
        return SimpleNamespace(pid=1, poll=lambda: exit_code, returncode=exit_code)

    monkeypatch.setattr(events.subprocess, "Popen", launch)
    result = events._finalize_completed_session_close_event(
        {"status": "ok", "expected_date": "2026-10-14", "reused": True},
        live_root=tmp_path / "live", state_root=tmp_path / "events", heartbeat_seconds=30,
    )
    assert len(calls) == 1
    assert result["status"] == expected
