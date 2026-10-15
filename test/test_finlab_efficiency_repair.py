"""Offline contracts for the 2026-10-11 account-owner efficiency repair."""
from datetime import UTC, date, datetime, timedelta
import json
import signal
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from finlab.exceptions import DataError
import pytest

from scripts import download_finlab_history as history
from scripts import download_finlab_intraday as intraday
from scripts import download_finlab_market_intraday as market
from scripts.audit_finlab_runtime import efficiency_history
from scripts.check_finlab_refresh_due import refresh_due
from stockagent.data.finlab_quota_policy import publication_recheck_delay, QuotaPolicy
from stockagent.data.finlab_acquisition_contract import last_acquisition_success, source_check_due
from test_finlab_pipeline import universe, run, fake_range, frame
from test_finlab_dispatch_admission import setup, write, NOW


def test_general_cache_release_preserves_files_and_source_generation(tmp_path):
    class FileStorage:
        def __init__(self):
            self._cache = {"price:x": object()}
            self._created = {"price:x": "source-generation"}

        def clear(self):
            pytest.fail("SDK clear() is destructive")

    original = tmp_path / "source.pkl"
    original.write_bytes(b"immutable-generation")
    storage = FileStorage()
    context = SimpleNamespace(_storage=storage, _finalized_cache={"price:x": object()},
                              _loaded_datasets={"price:x"})
    with patch("finlab.data._default_context", context):
        assert history.release_sdk_frames()
    assert not storage._cache and not context._finalized_cache and not context._loaded_datasets
    assert storage._created == {"price:x": "source-generation"}
    assert original.read_bytes() == b"immutable-generation"
    with patch("finlab.data._default_context", SimpleNamespace()):
        assert not history.release_sdk_frames()


def test_deadline_escapes_sdk_exception_retry_and_restores_signal():
    original = signal.getsignal(signal.SIGALRM)
    with pytest.raises(history.DatasetFetchTimeout), history.dataset_deadline(30):
        try:
            signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
        except Exception:
            pytest.fail("SDK Exception handlers must not swallow collector deadlines")
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
    assert signal.getsignal(signal.SIGALRM) == original
    assert history.classify_provider_error(history.DatasetFetchTimeout()) == "timed_out"


def test_deadline_never_replaces_an_outer_alarm():
    signal.setitimer(signal.ITIMER_REAL, 30)
    try:
        with pytest.raises(RuntimeError, match="existing timer"), history.dataset_deadline(1):
            pytest.fail("must reject outer alarm")
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 20
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


@pytest.mark.parametrize("reusable", [True, False])
def test_batch_reuses_only_a_supported_adapter_and_stops_bounded_timeouts(tmp_path, reusable):
    with patch.object(history, "credential_available", return_value=True), \
         patch.object(history, "sync_selection", return_value=["price:x", "price:y", "price:z"]), \
         patch.object(history, "record_core_acquisition_status", return_value={
             "required_complete": False, "required_pending": 3, "required_blocked": 0}), \
         patch.object(history, "quota_room_mb", return_value=(1000, 5000)), \
         patch.object(history, "release_sdk_frames", return_value=reusable), \
         patch.object(history, "fetch_one", side_effect=history.DatasetFetchTimeout()) as fetch:
        result = history.sync_catalog([], {}, tmp_path, limit=8, refresh_days=1,
            min_quota_remaining_mb=50, retry_unavailable=False, maximum_consecutive_timeouts=2)
    assert fetch.call_count == (2 if reusable else 1)
    assert result["timed_out"] == fetch.call_count and result["state"] == "partial"
    assert result["sdk_frame_releases"] == (2 if reusable else 0)


def test_sync_catalog_reuses_fresh_discovery_not_stale_or_invented_entitlement(tmp_path):
    path = tmp_path / "catalog/discovery.json"
    path.parent.mkdir()
    with patch.object(history, "provider_catalog", return_value=["fresh:key"]) as native:
        path.write_text(json.dumps({"observed_at_utc": datetime.now(UTC).isoformat(), "keys": ["cached:key"]}))
        assert history.sync_provider_catalog(tmp_path, now=datetime.now(UTC)) == ["cached:key"]
        assert native.call_count == 0
        path.write_text(json.dumps({"observed_at_utc": (datetime.now(UTC)-timedelta(hours=5)).isoformat(),
                                    "keys": ["cached:key"]}))
        assert history.sync_provider_catalog(tmp_path, now=datetime.now(UTC)) == ["fresh:key"]
        assert native.call_count == 1


def test_unresolved_provider_errors_retry_three_times_not_once_a_day(tmp_path):
    key, day = "tw_minute:2330", date(2026, 6, 1)
    for attempt in range(1, 4):
        assert intraday.record_failed_partition(tmp_path, key, day, DataError("fixture unavailable")) == "provider_error"
        path = intraday._partition_paths(tmp_path, key, day)[1]
        record = json.loads(path.read_text())
        assert record["cycle_attempts"] == attempt and not record["message_retained"]
        checked = datetime.fromisoformat(record["attempted_at_utc"])
        retry = datetime.fromisoformat(record["retry_at_utc"])
        assert intraday._checked_this_cycle(path, "attempted_at_utc", checked)
        assert intraday._checked_this_cycle(path, "attempted_at_utc", retry) is (attempt >= 3)
    assert not intraday._checked_this_cycle(path, "attempted_at_utc", retry+timedelta(days=1))


def test_reset_tail_has_bounded_publication_checks_not_an_all_day_poll_increase():
    policy = QuotaPolicy()
    assert publication_recheck_delay(now=NOW, policy=policy) == timedelta(hours=2)
    tail = NOW.replace(hour=23)
    assert publication_recheck_delay(now=tail, policy=policy) == timedelta(minutes=10)
    assert publication_recheck_delay(now=tail+timedelta(hours=1), policy=policy) == timedelta(hours=2)
    assert publication_recheck_delay(now=tail, policy=QuotaPolicy(drain_before_reset_minutes=0)) == timedelta(hours=2)


def test_local_recovery_completion_supersedes_failure_but_never_proves_source_freshness():
    stale = NOW-timedelta(days=3)
    receipt = {"source_checked_at_utc": stale.isoformat(), "source_check_mode": "upstream_forced",
               "acquisition_completed_at_utc": NOW.isoformat()}
    assert last_acquisition_success(receipt) == NOW
    assert source_check_due(receipt, now=NOW)  # Do not advance the observation/publication clock.


@pytest.mark.parametrize("message", ["request_conflict: fixture", "fixture temporary transport failure"])
def test_general_intraday_errors_share_one_validated_range(universe, message):
    root, _, days = universe
    db = market._open_index(root / "intraday/market_index.sqlite3")
    for day in days:
        intraday.record_failed_partition(root, "tw_minute:2317", day, DataError(message))
        path = intraday._partition_paths(root, "tw_minute:2317", day)[1]
        record = json.loads(path.read_text())
        before = datetime.now(UTC)-timedelta(minutes=3)
        record.update(attempted_at_utc=before.isoformat(), retry_at_utc=before.isoformat())
        path.write_text(json.dumps(record))
        market._record(db, "tw_minute", "2317", day, "provider_error")
        db.execute("UPDATE partitions SET checked_at_utc=? WHERE kind='tw_minute' AND symbol='2317' AND trade_date=?",
                   (before.isoformat(), str(day)))
    db.commit()
    db.close()
    calls = []

    def fetch(root, key, targets, **kwargs):
        calls.append((key, targets))
        return fake_range(root, key, targets, **kwargs)

    assert run(universe, limit=5, queue_depth=4, fetch=fetch)["successes_this_run"] == 5
    assert calls == [("tw_minute:2317", days)]


def test_known_hole_behind_a_committed_frontier_is_repaired_once(universe):
    root, _, days = universe
    run(universe, limit=9, queue_depth=4)
    day = days[1]
    path = intraday._partition_paths(root, "tw_minute:2330", day)[0]
    assert path.is_file()
    path.unlink()  # Deliberately damage this disposable fixture, not a source tree.
    with sqlite3.connect(root / "intraday/market_index.sqlite3") as db:
        db.execute("DELETE FROM partitions WHERE kind='tw_minute' AND symbol='2330' AND trade_date=?", (str(day),))
    cursor_path = root / "intraday/minute_cursor.json"
    cursor = json.loads(cursor_path.read_text())
    cursor.update(newest=10, oldest=10, recent_queue=[])
    cursor_path.write_text(json.dumps(cursor))
    calls = []

    def fetch(root, key, targets, **kwargs):
        calls.extend((key, value) for value in targets)
        return fake_range(root, key, targets, **kwargs)

    assert run(universe, limit=8, queue_depth=4, fetch=fetch)["successes_this_run"] == 1
    assert calls == [("tw_minute:2330", day)]
    assert run(universe, queue_depth=4, fetch=fetch)["successes_this_run"] == 0
    assert len(calls) == 1


def test_due_publication_pool_is_not_artificially_capped_at_sixteen(universe, monkeypatch):
    root, public, days = universe
    prototype = market.load_universe(public, start=days[0])[0]
    symbols = ["2330", *[str(4300+value) for value in range(20)]]
    items = [{**prototype, "symbol": symbol} for symbol in symbols]
    monkeypatch.setattr(market, "load_universe", lambda *a, **kw: items)
    db = market._open_index(root / "intraday/market_index.sqlite3")
    for symbol in symbols:
        for day in days[:-1]:
            receipt = intraday.persist_partition_frame(root, "tw_minute:"+symbol, day, frame(symbol, [day]))
            market._record_source(db, root, "tw_minute", symbol, day, receipt)
        day = days[-1]
        intraday.record_failed_partition(root, "tw_minute:"+symbol, day, DataError("not_ready: fixture"))
        path = intraday._partition_paths(root, "tw_minute:"+symbol, day)[1]
        record = json.loads(path.read_text())
        before = datetime.now(UTC)-timedelta(days=2)
        record["attempted_at_utc"] = before.isoformat()
        path.write_text(json.dumps(record))
        market._record(db, "tw_minute", symbol, day, "partition_not_ready")
        db.execute("UPDATE partitions SET checked_at_utc=? WHERE kind='tw_minute' AND symbol=? AND trade_date=?",
                   (before.isoformat(), symbol, str(day)))
    db.commit()
    db.close()
    calls = []

    def fetch(root, key, targets, **kwargs):
        calls.append(key)
        return fake_range(root, key, targets, **kwargs)

    result = run(universe, workers=8, limit=21, queue_depth=4, fetch=fetch)
    assert result["successes_this_run"] == 21 and len(set(calls)) == 21


def test_summary_reports_blocked_debt_separately_from_actionable_work(universe):
    root, public, days = universe
    run(universe, limit=9, queue_depth=4)
    day = days[1]
    intraday._partition_paths(root, "tw_minute:2330", day)[0].unlink()
    for _ in range(3):
        intraday.record_failed_partition(root, "tw_minute:2330", day, DataError("fixture provider failure"))
    db = market._open_index(root / "intraday/market_index.sqlite3")
    market._record(db, "tw_minute", "2330", day, "provider_error")
    items = market.load_universe(public, start=days[0])
    result = market._summarize(db, items, days, started=datetime.now(UTC), attempts=0,
        successes=0, state="waiting_older_publication", source_root=root)
    db.close()
    priority = result["minute_priority"]
    assert priority["pending_known_window_partitions"] == priority["blocked_known_window_partitions"] == 1
    assert priority["actionable_known_window_partitions"] == 0 and priority["tick_residual_allowed"]
    assert datetime.fromisoformat(priority["next_source_check_at_utc"]) > datetime.now(UTC)


def test_blocked_minute_failures_do_not_fake_completion_or_starve_residual_tick(tmp_path):
    setup(tmp_path, remaining=1000)
    write(tmp_path / "data_finlab/core_acquisition_status.json", {
        "supplemental_allowed": True, "minute_residual_allowed": True})
    write(tmp_path / "data_finlab/intraday/market_status.json", {
        "observed_at_utc": NOW.isoformat(), "acquisition_mode": "minute_first_tick_residual",
        "minute_priority": {"pending_known_window_partitions": 5, "actionable_known_window_partitions": 0,
            "due_publication_rechecks": 0, "next_source_check_at_utc": (NOW+timedelta(hours=1)).isoformat()},
        "by_kind": {"tw_minute": {"remaining_partitions": 5}, "tw_tick": {"remaining_partitions": 100}}})
    assert refresh_due(tmp_path, now=NOW) == (True, "residual_tick_capacity")


def test_efficiency_audit_uses_taipei_day_all_immutable_batches_and_no_api(tmp_path):
    root = tmp_path / "data_finlab/intraday/batch_receipts"
    root.mkdir(parents=True)
    for number in range(270):
        (root / f"{number}.json").write_text(json.dumps({
            "started_at_utc": "2026-10-09T16:00:00+00:00", "finished_at_utc": "2026-10-09T16:01:00+00:00",
            "state": "batch_limit", "new_receipted_partitions": 1, "attempted": 1}))
    (root / "crossing.json").write_text(json.dumps({"started_at_utc": "2026-10-09T15:59:00+00:00",
        "finished_at_utc": "2026-10-09T16:01:00+00:00"}))
    (root / "naive.json").write_text(json.dumps({"started_at_utc": "2026-10-10T12:00:00",
        "finished_at_utc": "2026-10-10T12:01:00"}))
    with patch("finlab.data.get", side_effect=AssertionError("must stay offline")):
        result = efficiency_history(tmp_path, date(2026, 10, 10), through=datetime(2026, 10, 11, tzinfo=UTC),
                                    include_service_journal=False)
    assert result["intraday"]["batches"] == 270 and result["intraday"]["new_receipted_partitions"] == 270
    assert result["intraday"]["active_owner_union_seconds"] == 60  # Not 270 overlapping worker minutes.
    assert len(result["boundary_crossing_receipts"]) == 1 and result["invalid_receipts"] == ["naive.json"]
    assert result["window_start_utc"] == "2026-10-09T16:00:00+00:00"
    assert result["window_end_utc"] == "2026-10-10T16:00:00+00:00" and result["provider_calls"] == 0
    with pytest.raises(ValueError, match="aware"):
        efficiency_history(tmp_path, date(2026, 10, 10), through=datetime(2026, 10, 11))


def test_efficiency_quota_cycles_do_not_include_a_nonintersecting_future_day(tmp_path):
    path = tmp_path / "artifacts/live/finlab/quota_history.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE quota_samples(observed_at_utc TEXT, used_mb REAL, limit_mb REAL)")
        db.executemany("INSERT INTO quota_samples VALUES(?,?,?)", [
            ("2026-10-09T23:59:00+00:00", 4000, 5000), ("2026-10-10T23:59:00+00:00", 2000, 5000),
            ("2026-10-11T00:01:00+00:00", 10, 5000)])
    result = efficiency_history(tmp_path, date(2026, 10, 10), through=datetime(2026, 10, 11, 1, tzinfo=UTC),
                                include_service_journal=False)
    assert len(result["quota_cycles"]) == 2
    assert result["quota_cycles"][-1]["quota_used_ratio"] == 0.4
