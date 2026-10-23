from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import importlib
import json
import os
import stat
import struct
import sys
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from stockagent.live import opening_price_acquisition as acquisition, quote_provider as quotes


TAIPEI = ZoneInfo("Asia/Taipei")


@pytest.fixture
def owner(monkeypatch, tmp_path):
    monkeypatch.setattr(acquisition, "_FINMIND_SNAPSHOT_OWNER", acquisition._FinMindSnapshotOwner())
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REVISION", 0)
    fixed = datetime.now(TAIPEI).replace(hour=9, minute=0, second=0, microsecond=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz or TAIPEI)

    monkeypatch.setattr(acquisition, "datetime", Clock)
    monkeypatch.setattr(quotes, "datetime", Clock)
    result = acquisition.OpeningPriceFanIn()
    yield result
    result.close()


def snapshot(values, *, source="twse_tpex:mis", stale=False, simtrade=False):
    now = acquisition.datetime.now(TAIPEI)
    stamp = int((now - timedelta(days=1) if stale else now).timestamp() * 1000)
    values = np.asarray(values, dtype=np.float64)
    return quotes.PriceSnapshot(prices=np.where(np.isfinite(values), values, 99.0), open_prices=values, source=source, available_mask=np.ones(len(values), dtype=bool), available_count=len(values), requested_count=len(values), timestamps_ms=np.full(len(values), int(now.timestamp() * 1000), dtype=np.int64), exchange_timestamps_ms=np.full(len(values), stamp, dtype=np.int64), simtrade_flags=np.full(len(values), 1 if simtrade else 0, dtype=np.int8))


def fetch(owner, tmp_path, symbols, providers, **kwargs):
    fallback = np.full(len(symbols), 99.0)
    return owner.fetch(symbols, fallback, parquet_root=tmp_path, providers=providers, minimum_coverage=kwargs.pop("minimum_coverage", 0.9), cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=tmp_path, allow_network=False), **kwargs)


def test_wait_idle_includes_final_durable_source_journal(owner, tmp_path, monkeypatch):
    finishing, release = threading.Event(), threading.Event()
    original = acquisition._record_source_event

    def blocked_finished(key, provider, event, details):
        if event == "finished":
            finishing.set()
            assert release.wait(3)
        return original(key, provider, event, details)

    monkeypatch.setattr(acquisition, "_record_source_event", blocked_finished)
    try:
        result = fetch(owner, tmp_path, ["A"], {"source": lambda *_: snapshot([100.])})
        assert result.transport_timing["opening_quorum_ready"]
        assert finishing.wait(2)
        assert not owner.wait_idle(timeout_seconds=.02), "network completion is not durable job completion"
    finally:
        release.set()
    assert owner.wait_idle(timeout_seconds=2)
    assert all(not state.jobs for state in owner._sessions.values())


def test_fan_in_rechecks_expired_absence_from_a_custom_cache_reader(owner, tmp_path):
    stamp = int(acquisition.datetime.now(TAIPEI).timestamp() * 1000)
    value = snapshot([np.nan], source="opening:shared_opening_snapshot")
    value.no_open_print_timestamps_ms = np.array([stamp - 15001])
    calls = []

    def source(*_args):
        calls.append(True)
        return snapshot([np.nan])

    result = owner.fetch(["A"], np.array([99.]), parquet_root=tmp_path, providers={"source": source},
        minimum_coverage=1., cache_reader=lambda: value, timeout_seconds=.1)
    assert owner.wait_idle(timeout_seconds=2)
    assert calls == [True]
    assert not result.transport_timing["opening_quorum_ready"]
    assert result.transport_timing["opening_resolved_count"] == 0


def test_bulk_observation_journal_is_lossless_strict_and_keeps_extra_fields(owner, tmp_path):
    from stockagent.live.opening_price_journal import observation_values, pack_observation_details
    stamp = int(acquisition.datetime.now(TAIPEI).timestamp() * 1000)
    rows = [{"symbol": f"code-{i}", "state": "valid_open", "source": "試驗:source",
        "open_price": np.nextafter(100., np.inf).item(), "available": True,
        "market_timestamp_ms": 2 ** 53 + 1 + i, "received_timestamp_ms": stamp,
        "simtrade": None} for i in range(128)]
    details = {"job_id": "full", "observations": rows}
    original = json.dumps(details, allow_nan=False)
    key = (str(tmp_path), acquisition.datetime.now(TAIPEI).date().isoformat())
    assert acquisition._record_source_event(key, "source", "observations_delta", details)
    path = quotes._tw_mis_opening_receipt_path(key[1]).with_suffix(".acquisition.jsonl")
    event = json.loads(path.read_text())
    assert event["schema_version"] == 2 and len(event["observations"]) == 128
    assert list(observation_values(event)) == list(observation_values(details))
    assert list(observation_values(event, ("symbol", "state"))) == [(row["symbol"], row["state"]) for row in rows]
    assert json.dumps(details, allow_nan=False) == original
    extra = {**details, "observations": [{**row, "extra_proof": "must-survive"} for row in rows]}
    assert pack_observation_details(extra) is extra
    assert pack_observation_details({**details, "observations": rows[:127]})["observations"] == rows[:127]
    size = path.stat().st_size
    bad = {**details, "observations": [{**row, "open_price": float("nan")} for row in rows]}
    assert not acquisition._record_source_event(key, "source", "observations_delta", bad)
    assert path.stat().st_size == size, "strict invalid events must not leave a partial journal row"


def test_bulk_v2_keeps_original_absence_clock_and_v1_consumers():
    from stockagent.live.opening_price_journal import observation_values, pack_observation_details
    row = {"symbol": "ZERO", "state": "observed_no_first_print", "source": "twse_tpex:mis",
        "open_price": None, "available": True, "market_timestamp_ms": 1,
        "received_timestamp_ms": 200, "simtrade": None, "no_first_print_observed_timestamp_ms": 100}
    event = pack_observation_details({"observations": [row] * 128})
    assert event["observation_encoding"].endswith("v2")
    assert list(observation_values(event))[0] == ("ZERO", "observed_no_first_print", "twse_tpex:mis", None, True, 1, 200, None)
    assert list(observation_values(event, ("symbol",), optional_fields=("no_first_print_observed_timestamp_ms",)))[0] == ("ZERO", 100)
    old = {key: value for key, value in row.items() if key != "no_first_print_observed_timestamp_ms"}
    legacy = pack_observation_details({"observations": [old] * 128})
    assert list(observation_values(legacy, ("symbol",), optional_fields=("no_first_print_observed_timestamp_ms",)))[0] == ("ZERO", None)


def test_unchanged_no_first_state_records_only_real_proof_refreshes(owner, tmp_path):
    stamp = int(acquisition.datetime.now(TAIPEI).timestamp() * 1000)
    def provider(names, values, progress):
        value = snapshot([np.nan])
        value.volumes = np.zeros(1)
        value.no_open_print_timestamps_ms = np.array([stamp])
        progress(names, value)
        refresh = snapshot([np.nan])
        refresh.volumes = np.zeros(1)
        refresh.no_open_print_timestamps_ms = np.array([stamp + 1])
        progress(names, refresh)
        return refresh
    fetch(owner, tmp_path, ["ZERO"], {"source": provider})
    assert owner.wait_idle(timeout_seconds=2)
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).with_suffix(".acquisition.jsonl")
    events = [json.loads(line) for line in path.read_text().splitlines()]
    rows = [row for event in events if event["event"] == "observations_delta" for row in event["observations"]]
    assert [row["no_first_print_observed_timestamp_ms"] for row in rows] == [stamp, stamp + 1]


@pytest.mark.parametrize("corrupt", ["unknown_encoding", "wrong_columns", "wrong_count", "short_row", "long_row"])
def test_bulk_observation_reader_rejects_unknown_or_truncated_evidence(corrupt):
    from stockagent.live.opening_price_journal import OBSERVATION_FIELDS, observation_values, pack_observation_details
    row = dict(zip(OBSERVATION_FIELDS, ("A", "valid_open", "fixture", 100., True, 1, 2, None)))
    event = pack_observation_details({"observations": [row] * 128})
    if corrupt == "unknown_encoding":
        event["observation_encoding"] = "unknown"
    elif corrupt == "wrong_columns":
        event["observation_columns"] = list(reversed(OBSERVATION_FIELDS))
    elif corrupt == "wrong_count":
        event["observation_row_count"] -= 1
    elif corrupt == "short_row":
        event["observations"][0].pop()
    else:
        event["observations"][0].append("extra")
    with pytest.raises(ValueError):
        list(observation_values(event))


@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize("result_job", ["source:1", "source:2"])
def test_merged_journal_references_only_durable_matching_source_details(durable, result_job):
    transport = {"mis_requests": [{"elapsed_ms": 12.5, "error_type": "ReadTimeout"}]}
    completed = {"job_id": "source:1", "transport_timing": transport,
                 "state": "failed", "http_status": 403, "valid_open_count": 0}
    metric = {"state": "failed", "http_status": 403, "attempts": 2,
              "last_result_job_id": result_job, "transport_timing": transport,
              "last_completed_job": completed}
    original = json.dumps(metric, sort_keys=True)
    compact = acquisition._journal_provider_metrics(
        {"source": metric}, {"source": "source:1"} if durable else {})["source"]
    assert json.dumps(metric, sort_keys=True) == original
    assert compact["state"] == "failed" and compact["http_status"] == 403
    if not durable:
        assert compact == metric
    else:
        ref = {"event": "finished", "job_id": "source:1", "owner_pid": os.getpid()}
        assert compact["completed_job_ref"] == ref
        assert "last_completed_job" not in compact
        if result_job == "source:1":
            assert compact["transport_timing_ref"] == ref
            assert "transport_timing" not in compact
        else:
            assert compact["transport_timing"] == transport
            assert "transport_timing_ref" not in compact


@pytest.mark.parametrize("durable", [False, True])
def test_provider_journal_compaction_preserves_dto_and_complete_failure_evidence(
        owner, tmp_path, monkeypatch, durable):
    original = acquisition._record_source_event

    def record(key, provider, event, details):
        success = original(key, provider, event, details)
        return success and (durable or event != "finished")

    def partial(symbols, fallback, progress):
        result = snapshot([100., np.nan])
        result.transport_timing = {"mis_requests": [{"elapsed_ms": 4., "error_type": "ReadTimeout"}]}
        return result

    monkeypatch.setattr(acquisition, "_record_source_event", record)
    fetch(owner, tmp_path, ["A", "B"], {"source": partial}, minimum_coverage=1.)
    # Drain the finite test jobs, including their final fsync. A read-only
    # no-provider pass below must not create a replacement source request.
    owner._pool.shutdown(wait=True)
    result = fetch(owner, tmp_path, ["A", "B"], {}, minimum_coverage=1.)
    metric = result.transport_timing["opening_providers"]["source"]
    assert metric["last_completed_job"]["transport_timing"]["mis_requests"][0]["error_type"] == "ReadTimeout"
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    events = [json.loads(line) for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
    merged = [row for row in events if row["provider"] == "merged"][-1]
    summary = merged["providers"]["source"]
    finished = [row for row in events if row["event"] == "finished" and row["provider"] == "source"]
    assert len(finished) == 1
    assert finished[0]["transport_timing"] == metric["transport_timing"]
    assert merged["provider_metric_layout"] == "inline_until_durable_finished_reference_v1"
    if durable:
        assert summary["completed_job_ref"]["job_id"] == finished[0]["job_id"]
        assert summary["transport_timing_ref"] == summary["completed_job_ref"]
        assert "last_completed_job" not in summary and "transport_timing" not in summary
    else:
        assert summary == metric
    assert result.transport_timing["opening_resolved_count"] == 1
    assert not result.transport_timing["opening_quorum_ready"]


def test_every_provider_starts_and_slow_loser_does_not_delay_quorum(owner, tmp_path):
    barrier, release = threading.Barrier(3), threading.Event()
    starts = []

    def fast(symbols, fallback, progress):
        starts.append(("fast", symbols))
        barrier.wait(timeout=2)
        return snapshot([100.0] * 10)

    def slow(symbols, fallback, progress):
        starts.append(("slow", symbols))
        barrier.wait(timeout=2)
        release.wait(timeout=2)
        return snapshot([100.0] * 10, source="shioaji:slow")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fetch, owner, tmp_path, list("ABCDEFGHIJ"), {"fast": fast, "slow": slow})
        barrier.wait(timeout=2)
        result = future.result(timeout=1)
        assert result.transport_timing["opening_background_pending"]
        assert result.transport_timing["opening_quorum_ready"]
        assert {name for name, _ in starts} == {"fast", "slow"}
        assert all(len(symbols) == 10 for _, symbols in starts)
        release.set()


def test_provider_request_overlaps_slow_dispatch_journal_without_losing_order(owner, tmp_path, monkeypatch):
    journal_entered, request_started, release = (threading.Event() for _ in range(3))
    original = acquisition._record_source_event
    recorded = []

    def journal(key, provider, event, details):
        if event == "started":
            journal_entered.set()
            assert release.wait(5)
        result = original(key, provider, event, details)
        recorded.append(event)
        return result

    def provider(names, values, progress):
        request_started.set()
        return snapshot([100.])

    monkeypatch.setattr(acquisition, "_record_source_event", journal)
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(fetch, owner, tmp_path, ["A"], {"source": provider})
        try:
            assert journal_entered.wait(2)
            assert request_started.wait(1), "metadata fsync must not delay the external request"
        finally:
            release.set()
        assert quotes.tw_session_open_observed_mask(result.result(timeout=2)).all()
    owner.close()
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    events = [json.loads(line) for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
    source_events = [row for row in events if row["provider"] == "source"]
    assert source_events[0]["event"] == "started" and source_events[-1]["event"] == "finished"
    assert recorded.index("started") < recorded.index("finished")
    metric = source_events[-1]
    assert metric["dispatch_telemetry_recorded"] is True
    assert metric["dispatch_telemetry_wait_ms"] >= 0
    assert metric["dispatch_journal_policy"] == "request_overlaps_start_receipt_join_before_progress_v1"
    assert source_events[0]["observed_at_taipei"] == metric["last_started_at_taipei"]
    assert json.loads(path.read_text())["valid_open_count"] == 1


def test_failed_dispatch_telemetry_does_not_cancel_provider_or_erase_failure(owner, tmp_path, monkeypatch):
    original = acquisition._record_source_event

    def journal(key, provider, event, details):
        return False if event == "started" else original(key, provider, event, details)

    monkeypatch.setattr(acquisition, "_record_source_event", journal)
    result = fetch(owner, tmp_path, ["A"], {"source": lambda *args: snapshot([100.])})
    assert quotes.tw_session_open_observed_mask(result).all()
    # Quorum return can precede source completion. Closing here would fence
    # its unfinished generation, deliberately cancelling the source flush.
    assert owner.wait_idle(timeout_seconds=1)
    owner.close()
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    events = [json.loads(line) for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
    metric = next(row for row in events if row["provider"] == "source" and row["event"] == "finished")
    assert metric["dispatch_telemetry_recorded"] is False
    assert metric["input_flush"]["captured_row_count"] == 1


@pytest.mark.parametrize("empty_response", [False, True])
def test_no_observation_source_does_not_flush_but_consumer_retains_complete_scope(
        owner, tmp_path, monkeypatch, empty_response):
    original = quotes.persist_tw_opening_snapshot_cache
    flush_threads = []

    def flush(**kwargs):
        flush_threads.append(threading.current_thread().name)
        return original(**kwargs)

    def unavailable(names, fallback, progress):
        if empty_response:
            return quotes.PriceSnapshot(prices=fallback.copy(), source="empty:fixture",
                available_mask=np.zeros(len(names), dtype=bool), available_count=0)
        raise PermissionError("fixture; must not be recorded")

    monkeypatch.setattr(quotes, "persist_tw_opening_snapshot_cache", flush)
    result = fetch(owner, tmp_path, list("AB"), {"unavailable": unavailable})
    owner.close()
    assert flush_threads == [threading.current_thread().name]
    assert not result.transport_timing["opening_quorum_ready"]
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    receipt = json.loads(path.read_bytes())
    assert receipt["requested_symbols"] == list("AB") and receipt["rows"] == {}
    finished = next(row for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()
                    if (row := json.loads(line))["event"] == "finished")
    assert finished["input_flush_policy"] == "no_source_observations_consumer_retains_scope_barrier_v1"
    assert "input_flush" not in finished
    assert finished["state"] == ("waiting_source" if empty_response else "failed")


def test_partial_source_flushes_accepted_inputs_even_after_failure(owner, tmp_path, monkeypatch):
    original = quotes.persist_tw_opening_snapshot_cache
    flush_threads = []

    def flush(**kwargs):
        flush_threads.append(threading.current_thread().name)
        return original(**kwargs)

    def partial(names, fallback, progress):
        value = snapshot([100., np.nan])
        value.available_mask[1], value.available_count = False, 1
        progress(names, value)
        raise ConnectionError("fixture; accepted partial input must survive")

    monkeypatch.setattr(quotes, "persist_tw_opening_snapshot_cache", flush)
    result = fetch(owner, tmp_path, list("AB"), {"partial": partial}, minimum_coverage=1.)
    owner._pool.shutdown(wait=True)
    assert any(name.startswith("tw-opening-source") for name in flush_threads)
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    receipt = json.loads(path.read_bytes())
    assert receipt["requested_symbols"] == list("AB") and receipt["valid_open_count"] == 1
    assert receipt["rows"]["A"]["open_prices"] == 100.
    assert not result.transport_timing["opening_quorum_ready"]
    finished = next(row for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()
                    if (row := json.loads(line))["event"] == "finished")
    assert finished["state"] == "failed" and finished["input_observation_received"]
    assert finished["input_flush"]["captured_row_count"] == 1
    assert "input_flush_policy" not in finished


def test_repeated_full_scope_jobs_retain_independent_coverage_without_duplicate_prices(owner, tmp_path):
    def provider(names, values, progress):
        return snapshot([100.] * len(names))

    fetch(owner, tmp_path, list("AB"), {"source": provider})
    assert owner.wait_idle(timeout_seconds=2)
    owner.close()
    second = acquisition.OpeningPriceFanIn()
    try:
        fetch(second, tmp_path, list("AB"), {"source": provider}, force_refresh=True)
        assert second.wait_idle(timeout_seconds=2)
    finally:
        second.close()
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).with_suffix(".acquisition.jsonl")
    events = [json.loads(line) for line in path.read_text().splitlines()]
    progress = [row for row in events if row["event"] == "coverage_progress"]
    assert len(progress) == 2
    assert len({row["job_id"] for row in progress}) == 2
    assert all(row["valid_open_count"] == row["resolved_count"] == row["response_count"] == 2 for row in progress)
    assert all(row["elapsed_ms"] >= 0 and row["measurement_clock"] == "provider_job_perf_counter_v1" for row in progress)
    # A fresh owner legitimately restates the full raw observations; the
    # per-request job clock remains distinct even within the same process.
    assert len([row for row in events if row["event"] == "observations_delta"]) == 2


def test_partial_batches_merge_before_either_source_finishes(owner, tmp_path):
    barrier, release = threading.Barrier(2), threading.Event()

    def first(symbols, fallback, progress):
        barrier.wait(timeout=2)
        progress(symbols[:5], snapshot([100.0] * 5))
        release.wait(timeout=2)
        return snapshot([100.0] * 5 + [np.nan] * 5)

    def second(symbols, fallback, progress):
        barrier.wait(timeout=2)
        progress(symbols[5:9], snapshot([101.0] * 4, source="shioaji:partial"))
        release.wait(timeout=2)
        return snapshot([np.nan] * 5 + [101.0] * 4 + [np.nan], source="shioaji:partial")

    started = time.perf_counter()
    result = fetch(owner, tmp_path, list("ABCDEFGHIJ"), {"one": first, "two": second})
    elapsed = time.perf_counter() - started
    release.set()
    assert elapsed < 1
    assert quotes.tw_session_open_observed_mask(result).sum() == 9
    np.testing.assert_array_equal(result.open_prices[:9], [100.0] * 5 + [101.0] * 4)
    assert np.isnan(result.open_prices[-1])
    receipt = json.loads(quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).read_text())
    assert receipt["requested_count"] == 10


def test_quorum_retains_flush_measurements_in_transport_and_daily_evidence(owner, tmp_path):
    result = fetch(owner, tmp_path, ["A"], {"source": lambda *a: snapshot([100.])})
    flush = result.transport_timing["opening_durable_flush"]
    assert flush["contract"] == "captured_opening_input_revision_durable_flush_v1"
    # The provider's final barrier may win before the caller's quorum barrier.
    assert type(flush["receipt_written"]) is bool
    assert flush["captured_row_count"] == flush["requested_symbol_count"] == 1
    assert flush["captured_revision"] > 0
    assert all(flush[name] >= 0 for name in (
        "total_ms", "writer_queue_ms", "snapshot_capture_ms", "scope_check_ms",
        "durable_write_ms", "commit_ack_ms",
    ))
    written = [flush] if flush["receipt_written"] else []
    assert result.transport_timing["opening_telemetry_recorded"]
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    events = [json.loads(line) for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
    merged = [event for event in events if event["provider"] == "merged"]
    assert len(merged) == 1 and merged[0]["input_flush"] == flush
    assert owner.wait_idle(timeout_seconds=1)

    # A hot quorum is still checked against its durable revision, but does not
    # invent a second write or reuse the previous call's write duration.
    cached = fetch(owner, tmp_path, ["A"], {})
    hot_flush = cached.transport_timing["opening_durable_flush"]
    assert cached.transport_timing["opening_cache_ready"]
    assert not hot_flush["receipt_written"]
    assert hot_flush["captured_revision"] == flush["captured_revision"]
    assert hot_flush["captured_row_count"] == 1
    assert hot_flush["durable_write_ms"] == 0.0
    assert hot_flush["receipt_stages"] == {}, "a hot barrier must not replay a previous write's timings"
    owner.close()
    events = [json.loads(line) for line in path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
    finished = [event for event in events if event["provider"] == "source" and event["event"] == "finished"]
    assert len(finished) == 1
    assert finished[0]["input_flush"]["contract"] == flush["contract"]
    written += [row["input_flush"] for row in finished if row["input_flush"]["receipt_written"]]
    assert written
    for item in written:
        stages = item["receipt_stages"]
        assert stages["contract"] == "opening_receipt_write_stages_v1"
        assert stages["row_count"] == stages["reused_numeric_row_count"] == 1
        assert stages["defensively_converted_row_count"] == 0 and stages["encoded_bytes"] > 0
        assert all(stages[name] >= 0 for name in (
            "total_ms", "row_projection_ms", "previous_receipt_read_ms", "official_scope_and_archive_ms",
            "payload_assembly_ms", "json_encode_ms", "atomic_write_fsync_ms", "metadata_publish_ms"))
    assert finished[0]["source_and_persistence_ms"] >= finished[0]["last_total_ms"]


def test_provider_failure_isolated_and_error_text_never_written(owner, tmp_path):
    def failed(*args):
        raise ConnectionError("api-key=VERY-SECRET-KEY")

    result = fetch(owner, tmp_path, ["A"], {"failed": failed, "good": lambda *a: snapshot([100.0])})
    owner.close()
    # Already returned metrics may have the failure pending; the final owner
    # record retains a safe exception type, never the secret-bearing message.
    assert "VERY-SECRET-KEY" not in json.dumps(result.transport_timing)
    assert quotes.tw_session_open_observed_mask(result).all()


@pytest.mark.parametrize("failure", ["exception", "empty"])
def test_source_failure_has_local_backoff_and_success_resets_it(owner, tmp_path, failure):
    calls = []

    def provider(symbols, fallback, progress):
        calls.append(tuple(symbols))
        if failure == "exception":
            raise ConnectionError("secret=never-write")
        result = snapshot([np.nan] * len(symbols))
        result.available_count = 0
        result.available_mask[:] = False
        return result

    first = fetch(owner, tmp_path, ["A"], {"failed": provider})
    assert owner.wait_idle(timeout_seconds=1)
    metric = first.transport_timing["opening_providers"]["failed"]
    assert metric["consecutive_source_failures"] == 1
    assert metric["retry_delay_seconds"] == 0.5
    fetch(owner, tmp_path, ["A"], {"failed": provider})
    assert len(calls) == 1
    result = fetch(owner, tmp_path, ["A"], {"failed": provider, "winner": lambda *a: snapshot([100.0])})
    assert quotes.tw_session_open_observed_mask(result).all()
    session = next(iter(owner._sessions.values()))
    session.metrics["failed"]["retry_after_monotonic"] = 0.0
    recovered = fetch(owner, tmp_path, ["B"], {"failed": lambda *a: snapshot([101.0])})
    assert owner.wait_idle(timeout_seconds=1)
    assert quotes.tw_session_open_observed_mask(recovered).all()
    assert session.metrics["failed"]["consecutive_source_failures"] == 0
    assert "never-write" not in json.dumps(session.metrics)


@pytest.mark.parametrize("invalid", ["stale", "trial", "empty"])
def test_unproven_open_is_not_quorum(owner, tmp_path, invalid):
    result = fetch(owner, tmp_path, ["A"], {"source": lambda *a: snapshot([np.nan if invalid == "empty" else 100.0], stale=invalid == "stale", simtrade=invalid == "trial")})
    assert not result.transport_timing["opening_quorum_ready"]
    assert not quotes.tw_session_open_observed_mask(result).any()


def test_observed_no_print_is_explicit_and_never_sized_price(owner, tmp_path):
    def source(*args):
        result = snapshot([100.0, np.nan])
        result.volumes = np.array([1.0, 0.0])
        return result

    result = fetch(owner, tmp_path, ["A", "B"], {"official": source}, minimum_coverage=1)
    assert result.transport_timing["opening_quorum_ready"]
    assert quotes.tw_session_no_open_print_mask(result).tolist() == [False, True]
    assert np.isnan(result.open_prices[1])
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official"]
    assert metric["no_print_count"] == 1
    assert metric["consecutive_source_no_progress"] == 0


def test_missing_volume_cannot_reuse_cached_zero_as_a_new_no_print_proof(owner, tmp_path):
    first = snapshot([np.nan])
    first.volumes = np.array([0.0])
    quotes.seed_tw_opening_snapshot_cache(["A"], first, parquet_root=tmp_path)
    assert quotes.tw_session_no_open_print_mask(first).tolist() == [True]
    unknown = snapshot([np.nan])
    assert quotes.tw_session_no_open_print_mask(unknown).tolist() == [False]
    quotes.seed_tw_opening_snapshot_cache(["A"], unknown, parquet_root=tmp_path)
    cached = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, allow_network=False)
    assert quotes.tw_session_no_open_print_mask(cached).tolist() == [False]
    assert np.isnan(cached.open_prices[0])


@pytest.mark.parametrize("invalid", ["stale", "trial", "broker"])
def test_only_fresh_raw_mis_zero_volume_is_no_print_evidence(owner, invalid):
    value = snapshot([np.nan], source="shioaji:stock_snapshot" if invalid == "broker" else "twse_tpex:mis",
                     stale=invalid == "stale", simtrade=invalid == "trial")
    value.volumes = np.array([0.0])
    assert not quotes.tw_session_no_open_print_mask(value).any()


def test_failed_mis_transport_is_not_complete_and_does_not_cancel_winners(owner, tmp_path):
    def failed(*args):
        result = snapshot([np.nan])
        result.available_count = 0
        result.available_mask[:] = False
        result.transport_timing = {"mis_failed_request_count": 1, "mis_failure_types": ["ConnectionError"]}
        return result
    result = fetch(owner, tmp_path, ["A"], {"official_mis": failed})
    metrics = result.transport_timing["opening_providers"]["official_mis"]
    assert metrics["state"] == "failed"
    assert metrics["retry_after_monotonic"] > time.monotonic()
    assert not result.transport_timing["opening_quorum_ready"]
    recovered = fetch(owner, tmp_path, ["A"], {"official_mis": failed, "healthy": lambda *args: snapshot([100.0])})
    assert recovered.transport_timing["opening_quorum_ready"]
    assert recovered.open_prices[0] == 100.0


def test_stale_source_responses_are_waiting_not_ready(owner, tmp_path):
    result = fetch(owner, tmp_path, ["A"], {"broker": lambda *args: snapshot([100.0], stale=True)})
    assert result.transport_timing["opening_providers"]["broker"]["state"] == "waiting_source"
    assert not result.transport_timing["opening_quorum_ready"]


@pytest.mark.parametrize("invalid", ["stale", "trial", "missing_open"])
def test_responding_source_without_inputs_backs_off_and_other_source_recovers(owner, tmp_path, invalid):
    calls = []

    def unchanged(symbols, fallback, progress):
        calls.append(tuple(symbols))
        return snapshot(
            [np.nan if invalid == "missing_open" else 100.0] * len(symbols),
            source="shioaji:unchanged", stale=invalid == "stale", simtrade=invalid == "trial",
        )

    result = fetch(owner, tmp_path, ["A"], {"unchanged": unchanged})
    assert owner.wait_idle(timeout_seconds=1)
    session = next(iter(owner._sessions.values()))
    metric = session.metrics["unchanged"]
    assert metric["state"] == "waiting_source"
    assert metric["response_count"] == 1
    assert metric["consecutive_source_no_progress"] == 1
    assert metric["retry_delay_seconds"] == 0.5
    assert metric["retry_reason"] == "no_same_session_opening_inputs"
    assert not result.transport_timing["opening_quorum_ready"]
    fetch(owner, tmp_path, ["A"], {"unchanged": unchanged})
    assert calls == [("A",)]
    session.metrics["unchanged"]["retry_after_monotonic"] = 0.0
    fetch(owner, tmp_path, ["A"], {"unchanged": unchanged})
    assert owner.wait_idle(timeout_seconds=1)
    assert metric["consecutive_source_no_progress"] == 2
    assert metric["retry_delay_seconds"] == 1.0
    winner = fetch(owner, tmp_path, ["A"], {"unchanged": unchanged, "winner": lambda *a: snapshot([102.0])})
    assert winner.open_prices.tolist() == [102.0]
    metric["retry_after_monotonic"] = 0.0
    recovered = fetch(owner, tmp_path, ["B"], {"unchanged": lambda *a: snapshot([103.0])})
    assert owner.wait_idle(timeout_seconds=1)
    assert recovered.open_prices.tolist() == [103.0]
    assert metric["consecutive_source_no_progress"] == 0
    assert "retry_reason" not in metric
    # Repairing B cannot delete A's accepted opening observation.
    preserved = fetch(owner, tmp_path, ["A"], {})
    assert preserved.open_prices.tolist() == [102.0]


def test_mis_rate_limit_metadata_uses_permission_backoff(owner, tmp_path):
    def limited(*args):
        result = snapshot([np.nan])
        result.available_count = 0
        result.available_mask[:] = False
        result.transport_timing = {"mis_failed_request_count": 1, "mis_http_error_statuses": [429]}
        return result

    result = fetch(owner, tmp_path, ["A"], {"official_mis": limited})
    metric = result.transport_timing["opening_providers"]["official_mis"]
    assert metric["state"] == "failed"
    assert metric["retry_delay_seconds"] == 60.0


def test_partial_mis_peer_refusal_keeps_inputs_and_uses_endpoint_backoff(owner, tmp_path):
    def mis(symbols, fallback, progress):
        result = snapshot([101., np.nan])
        result.available_count = 1
        result.available_mask[:] = [True, False]
        result.transport_timing = {
            "mis_failed_request_count": 4, "mis_dispatch_stopped": True,
            "mis_error_chain_types": ["ConnectionError", "RemoteDisconnected"],
        }
        return result

    result = fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    session = next(iter(owner._sessions.values()))
    metric = session.metrics["official_mis"]
    assert metric["state"] == "degraded"
    assert metric["retry_delay_seconds"] == 15.
    assert metric["retry_reason"] == "source_endpoint_unavailable"
    # A healthy independent source can resolve B during MIS's quiet interval.
    result = fetch(owner, tmp_path, ["A", "B"], {
        "official_mis": lambda *_: pytest.fail("refused MIS source must wait"),
        "winner": lambda names, *_: snapshot([102.] * len(names)),
    })
    assert result.open_prices.tolist() == [101., 102.]


def _mis_transport_outage(size, *, status=None):
    result = snapshot([np.nan] * size)
    result.available_count = 0
    result.available_mask[:] = False
    result.transport_timing = {"mis_failed_request_count": 4,
        "mis_dispatch_stopped": True, "mis_http_error_statuses": [status] if status else []}
    return result


def test_mis_half_open_probe_restores_only_unresolved_delta_without_full_sweep_burst(owner, tmp_path):
    calls = []
    def mis(names, *_args):
        calls.append(list(names))
        return _mis_transport_outage(len(names)) if len(calls) == 1 else snapshot([101.] * len(names))
    fetch(owner, tmp_path, ["A", "B", "C"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official_mis"]
    assert metric["retry_delay_seconds"] == 15.
    assert metric["transport_probe_delay_seconds"] == .5
    metric["transport_probe_after_monotonic"] = 0.
    fetch(owner, tmp_path, ["A", "B", "C"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    assert metric["last_completed_job"]["transport_recovery_probe"]
    assert metric["last_completed_job"]["transport_health_recovered"]
    result = fetch(owner, tmp_path, ["A", "B", "C"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [["A", "B", "C"], ["A"], ["B", "C"]]
    assert result.open_prices.tolist() == [101., 101., 101.]
    assert metric["consecutive_source_failures"] == 0
    assert "transport_probe_after_monotonic" not in metric


def test_mis_failed_half_open_probe_exponentially_backs_off_without_expanding_scope(owner, tmp_path):
    calls = []
    def mis(names, *_args):
        calls.append(list(names))
        return _mis_transport_outage(len(names))
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official_mis"]
    metric["transport_probe_after_monotonic"] = 0.
    result = fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [["A", "B"], ["A"]]
    assert metric["transport_probe_delay_seconds"] == 1.
    assert metric["retry_delay_seconds"] == 15.
    assert not quotes.tw_session_open_observed_mask(result).any()
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert calls == [["A", "B"], ["A"]]


@pytest.mark.parametrize("status", [401, 402, 403, 429])
def test_mis_half_open_never_bypasses_permission_or_quota_refusal(owner, tmp_path, status):
    fetch(owner, tmp_path, ["A", "B"], {
        "official_mis": lambda names, *_: _mis_transport_outage(len(names), status=status)})
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official_mis"]
    assert metric["retry_delay_seconds"] == 60.
    assert "transport_probe_after_monotonic" not in metric
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": lambda *_: pytest.fail("refusal cooldown")})


def test_mis_half_open_old_row_is_health_only_not_new_opening_input(owner, tmp_path):
    calls = []
    def mis(names, *_args):
        calls.append(list(names))
        return _mis_transport_outage(len(names)) if len(calls) == 1 else snapshot([101.] * len(names), stale=True)
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official_mis"]
    metric["transport_probe_after_monotonic"] = 0.
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    assert metric["last_completed_job"]["transport_health_recovered"]
    result = fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis})
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [["A", "B"], ["A"], ["A", "B"]]
    assert not quotes.tw_session_open_observed_mask(result).any()
    assert not quotes.tw_session_no_open_print_mask(result).any()
    assert "transport_probe_after_monotonic" not in metric


def test_mis_half_open_is_single_flight_across_consumers(owner, tmp_path):
    fetch(owner, tmp_path, ["A", "B"], {"official_mis": lambda names, *_: _mis_transport_outage(len(names))})
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["official_mis"]
    metric["transport_probe_after_monotonic"] = 0.
    entered, release = threading.Event(), threading.Event()
    calls = []
    def mis(names, *_args):
        calls.append(list(names))
        entered.set()
        assert release.wait(2)
        return snapshot([101.] * len(names))
    with ThreadPoolExecutor(max_workers=5) as pool:
        future = pool.submit(fetch, owner, tmp_path, ["A", "B"], {"official_mis": mis})
        assert entered.wait(1)
        try:
            others = [pool.submit(fetch, owner, tmp_path, ["B", "A"], {"official_mis": mis}, timeout_seconds=.01) for _ in range(4)]
            for other in others:
                other.result(timeout=1)
            assert calls == [["A"]]
        finally:
            release.set()
        future.result(timeout=1)


def test_mis_half_open_and_resumed_sweep_progress_inside_one_bounded_wait(owner, tmp_path):
    calls = []
    def mis(names, *_args):
        calls.append(list(names))
        return _mis_transport_outage(len(names)) if len(calls) == 1 else snapshot([101.] * len(names))
    result = fetch(owner, tmp_path, ["A", "B"], {"official_mis": mis},
        poll_opening_snapshots=True, timeout_seconds=2)
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [["A", "B"], ["A"], ["B"]]
    assert quotes.tw_session_open_observed_mask(result).all()


def test_overlapping_models_only_request_universe_delta(owner, tmp_path):
    calls = []
    release, first_started = threading.Event(), threading.Event()

    def source(symbols, fallback, progress):
        calls.append(symbols)
        first_started.set()
        release.wait(timeout=2)
        return snapshot([100.0] * len(symbols))

    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(fetch, owner, tmp_path, ["A", "B"], {"source": source})
        assert first_started.wait(timeout=2)
        rest = [pool.submit(fetch, owner, tmp_path, names, {"source": source}) for names in (["B", "A"], ["B", "C"], ["A", "C"])]
        deadline = time.monotonic() + 1
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.001)
        release.set()
        results = [future.result(timeout=2) for future in [first, *rest]]
    assert sorted(calls) == [["A", "B"], ["C"]]
    assert all(quotes.tw_session_open_observed_mask(row).all() for row in results)


def test_completed_delta_is_revalidated_inside_dispatch_lock(owner, tmp_path):
    calls = []

    def provider(symbols, *_args):
        calls.append(symbols)
        return snapshot([100.] * len(symbols))

    fetch(owner, tmp_path, ["A"], {"source": provider})
    assert owner.wait_idle(timeout_seconds=2)
    first_read = True

    def reader():
        nonlocal first_read
        observed = quotes.fetch_tw_mis_opening_snapshot(["A", "B"], np.full(2, 99.),
            parquet_root=tmp_path, allow_network=False)
        if first_read:
            first_read = False
            # Deterministically complete B after the optimistic read, just as
            # another worker can do while this caller waits for the condition.
            quotes.seed_tw_opening_snapshot_cache(["B"], snapshot([101.]),
                parquet_root=tmp_path, persist=False)
        return observed

    result = owner.fetch(["A", "B"], np.full(2, 99.), parquet_root=tmp_path,
        providers={"source": provider}, minimum_coverage=1., cache_reader=reader)
    assert owner.wait_idle(timeout_seconds=2)
    assert calls == [["A"]], "a completed delta must not be requested twice"
    assert result.open_prices.tolist() == [100., 101.]


def test_overlapping_full_and_delta_receipts_keep_job_counts_and_unique_provider_coverage(owner, tmp_path):
    entered, release = threading.Event(), threading.Event()

    def source(symbols, fallback, progress):
        if len(symbols) == 3:
            entered.set()
            assert release.wait(timeout=2)
        return snapshot([100.] * len(symbols), source="shioaji:fixture")

    with ThreadPoolExecutor(max_workers=2) as pool:
        full = pool.submit(fetch, owner, tmp_path, list("ABC"), {"same": source})
        assert entered.wait(timeout=1)
        delta = fetch(owner, tmp_path, ["D"], {"same": source})
        assert delta.open_prices.tolist() == [100.]
        release.set()
        assert full.result(timeout=1).open_prices.tolist() == [100., 100., 100.]
    owner.close()
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).with_suffix(".acquisition.jsonl")
    events = [json.loads(line) for line in path.read_text().splitlines()]
    finished = [row for row in events if row["provider"] == "same" and row["event"] == "finished"]
    assert sorted(row["requested_count"] for row in finished) == [1, 3]
    assert sorted(row["response_count"] for row in finished) == [1, 3]
    assert len({row["job_id"] for row in finished}) == 2
    aggregate = next(iter(owner._sessions.values())).metrics["same"]
    assert aggregate["unique_requested_count"] == 4
    assert aggregate["response_count"] == aggregate["valid_open_count"] == 4
    observed = [row for row in events if row["event"] == "observations_delta"]
    assert {item["symbol"] for row in observed for item in row["observations"]} == set("ABCD")


def test_quality_transitions_are_receipted_without_erasing_success_on_partial_progress(owner, tmp_path):
    def source(symbols, fallback, progress):
        progress(symbols, snapshot([100., 101.], stale=True))
        progress([symbols[0]], snapshot([100.]))
        # An incomplete final response may not erase A's earlier valid proof.
        result = snapshot([np.nan, 101.])
        result.available_mask[0] = False
        result.available_count = 1
        return result

    result = fetch(owner, tmp_path, ["A", "B"], {"source": source}, minimum_coverage=1)
    owner.close()
    assert result.open_prices.tolist() == [100., 101.]
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).with_suffix(".acquisition.jsonl")
    events = [json.loads(line) for line in path.read_text().splitlines()]
    changes = [item for row in events if row["event"] == "observations_delta" for item in row["observations"]]
    assert {item["state"] for item in changes} == {"stale_or_preopen_time", "valid_open"}
    aggregate = next(iter(owner._sessions.values())).metrics["source"]
    assert aggregate["valid_open_count"] == 2
    assert aggregate["last_valid_open_count"] == 1


def test_conflict_preserves_first_price_and_both_proofs(owner, tmp_path):
    first = snapshot([100.0])
    second = snapshot([101.0], source="shioaji:second")
    quotes.seed_tw_opening_snapshot_cache(["A"], first, parquet_root=tmp_path)
    quotes.seed_tw_opening_snapshot_cache(["A"], second, parquet_root=tmp_path)
    cached = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, allow_network=False)
    assert cached.open_prices[0] == 100.0
    receipt = json.loads(quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).read_text())
    assert receipt["rows"]["A"]["open_source"] == "twse_tpex:mis"
    assert receipt["rows"]["A"]["opening_conflicts"][0]["price"] == 101.0
    quotes.seed_tw_opening_snapshot_cache(["A"], second, parquet_root=tmp_path)
    assert len(quotes._TW_MIS_OPENING_CACHE["A"][1]["opening_conflicts"]) == 1


def test_identical_progress_and_existing_scope_do_not_rewrite_receipt(owner, tmp_path, monkeypatch):
    value = snapshot([100.])
    writes = []
    original = quotes._persist_tw_mis_opening_receipt

    def persist(**kwargs):
        writes.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", persist)
    quotes.seed_tw_opening_snapshot_cache(["A"], value, parquet_root=tmp_path)
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    raw, stat = path.read_bytes(), path.stat()
    for _ in range(5):
        result = quotes.seed_tw_opening_snapshot_cache(["A"], value, parquet_root=tmp_path)
        assert result["accepted_count"] == 1
        quotes.seed_tw_opening_snapshot_cache(["A"], quotes.PriceSnapshot(
            prices=np.array([99.]), source="opening_request_scope",
            available_mask=np.zeros(1, dtype=bool)), parquet_root=tmp_path)
    assert len(writes) == 1
    assert path.read_bytes() == raw
    assert path.stat().st_mtime_ns == stat.st_mtime_ns
    assert not quotes._TW_MIS_OPENING_DIRTY


def test_new_scope_is_durable_even_if_provider_has_no_rows(owner, tmp_path):
    for names in [["A"], ["A", "B"]]:
        quotes.seed_tw_opening_snapshot_cache(names, quotes.PriceSnapshot(
            prices=np.full(len(names), 99.), source="opening_request_scope",
            available_mask=np.zeros(len(names), dtype=bool)), parquet_root=tmp_path)
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    receipt = json.loads(path.read_text())
    assert receipt["requested_symbols"] == ["A", "B"]
    assert receipt["rows"] == {}


def test_new_receipt_clock_is_not_hidden_by_write_deduplication(owner, tmp_path):
    first = snapshot([100.])
    quotes.seed_tw_opening_snapshot_cache(["A"], first, parquet_root=tmp_path)
    later = snapshot([100.])
    later.timestamps_ms += 5
    later.exchange_timestamps_ms += 5
    quotes.seed_tw_opening_snapshot_cache(["A"], later, parquet_root=tmp_path, persist=False)
    assert quotes._TW_MIS_OPENING_DIRTY
    quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path,
        session_date=acquisition.datetime.now(TAIPEI).date().isoformat())
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    row = json.loads(path.read_text())["rows"]["A"]
    assert row["timestamp_ms"] == int(later.timestamps_ms[0])
    assert row["open_timestamp_ms"] == int(first.exchange_timestamps_ms[0])


def test_source_receipt_syncs_file_then_directory_before_return(owner, tmp_path, monkeypatch):
    events = []
    sync, replace = os.fsync, os.replace

    def synced(fd):
        events.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        sync(fd)

    def replaced(source, dest):
        events.append("replace")
        replace(source, dest)

    monkeypatch.setattr(os, "fsync", synced)
    monkeypatch.setattr(os, "replace", replaced)
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path)
    assert events == ["file", "replace", "directory"]
    assert not quotes._TW_MIS_OPENING_DIRTY


@pytest.mark.parametrize("failure", ["file", "directory"])
def test_failed_source_durability_remains_dirty_and_can_recover(owner, tmp_path, monkeypatch, failure):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path)
    path = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat())
    raw = path.read_bytes()
    sync = os.fsync

    def failed(fd):
        kind = "directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
        if kind == failure:
            raise OSError("injected storage barrier failure")
        sync(fd)

    with monkeypatch.context() as patcher:
        patcher.setattr(os, "fsync", failed)
        with pytest.raises(OSError, match="storage barrier"):
            quotes.seed_tw_opening_snapshot_cache(["A", "B"], snapshot([100., 101.]), parquet_root=tmp_path)
    assert quotes._TW_MIS_OPENING_DIRTY
    if failure == "file":
        assert path.read_bytes() == raw
    quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path,
        session_date=acquisition.datetime.now(TAIPEI).date().isoformat())
    assert not quotes._TW_MIS_OPENING_DIRTY
    assert set(json.loads(path.read_text())["rows"]) == {"A", "B"}


def test_source_flush_does_not_hold_the_quote_merge_or_read_lock(owner, tmp_path, monkeypatch):
    """A slow fsync cannot stall an independent provider's accepted facts."""
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path, persist=False)
    entered, release = threading.Event(), threading.Event()
    original = quotes._persist_tw_mis_opening_receipt

    def slow_flush(**kwargs):
        entered.set()
        assert release.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", slow_flush)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    timings = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        flushing = pool.submit(quotes.persist_tw_opening_snapshot_cache,
                               parquet_root=tmp_path, session_date=day, timings=timings)
        assert entered.wait(2)
        merge = pool.submit(quotes.seed_tw_opening_snapshot_cache, ["B"], snapshot([101.]),
                            parquet_root=tmp_path, persist=False)
        read = pool.submit(quotes.fetch_tw_mis_opening_snapshot, ["A"], np.array([99.]),
                           parquet_root=tmp_path, allow_network=False)
        try:
            assert merge.result(timeout=0.5)["accepted_count"] == 1
            assert read.result(timeout=0.5).open_prices.tolist() == [100.]
            assert not flushing.done(), "non-durable input must not release the quorum"
        finally:
            release.set()
        path = flushing.result(timeout=2)
    # A flush owns a frozen input set. A later merge is dirty, not erased by
    # the older writer or falsely acknowledged as already durable.
    assert set(json.loads(path.read_text())["rows"]) == {"A"}
    assert timings["newer_input_pending"]
    assert timings["captured_row_count"] == 1
    assert quotes._TW_MIS_OPENING_DIRTY
    quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day, timings=timings)
    assert set(json.loads(path.read_text())["rows"]) == {"A", "B"}
    assert not timings["newer_input_pending"]
    assert timings["captured_row_count"] == 2
    assert not quotes._TW_MIS_OPENING_DIRTY


def test_durable_projection_reuses_normalized_values_but_retains_legacy_validation(owner, tmp_path, monkeypatch):
    value = snapshot([100.12345678901235, np.nan])
    value.volumes = np.array([0., 1000.])
    quotes.seed_tw_opening_snapshot_cache(["A", "B"], value, parquet_root=tmp_path, persist=False)
    original = quotes._snapshot_field_or_none
    conversions = []

    def convert(field, value):
        conversions.append((field, value))
        return original(field, value)

    monkeypatch.setattr(quotes, "_snapshot_field_or_none", convert)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    path = quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day)
    assert not conversions, "a durable barrier must not parse normalized numeric columns again"
    wire = json.loads(path.read_bytes())
    assert wire["rows"]["A"]["open_prices"] == value.open_prices[0]
    assert wire["rows"]["A"]["volumes"] == 0.
    assert wire["rows"]["B"]["open_prices"] is None
    assert wire["valid_open_count"] == 1 and wire["missing_open_symbols"] == ["B"]
    # Marker stays private. A restored receipt validates every column once,
    # then subsequent snapshots/flushes may reuse those exact parsed numbers.
    restored = quotes._load_tw_mis_opening_receipt(parquet_root=str(tmp_path.resolve()), session_date=day)
    assert len(conversions) == 2 * len(quotes._PRICE_SNAPSHOT_ARRAY_FIELDS)
    assert all(type(row) is quotes._NormalizedOpeningRow for _, row in restored.values())
    conversions.clear()
    # An unmarked mapping remains defensive, including zero-vs-unknown units
    # and malformed/off-session values. It cannot gain trust through a merge.
    stamp, raw = quotes._TW_MIS_OPENING_CACHE["A"]
    legacy = {**raw, "price": "1,234.5", "volumes": True, "bid_volumes": "0", "open_prices": "100.25"}
    quotes._TW_MIS_OPENING_CACHE["A"] = (stamp, legacy)
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.25]), parquet_root=tmp_path, persist=False)
    assert type(quotes._TW_MIS_OPENING_CACHE["A"][1]) is dict
    quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day)
    assert len(conversions) == len(quotes._PRICE_SNAPSHOT_ARRAY_FIELDS)
    wire = json.loads(path.read_bytes())
    assert wire["rows"]["A"]["volumes"] is None
    assert wire["rows"]["A"]["bid_volumes"] == 0.
    assert wire["rows"]["A"]["open_prices"] == 100.25


def test_source_flushes_serialize_without_losing_later_facts(owner, tmp_path, monkeypatch):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path, persist=False)
    entered, release, merged = threading.Event(), threading.Event(), threading.Event()
    original = quotes._persist_tw_mis_opening_receipt
    writes, in_writer = [], []

    def writer(**kwargs):
        assert not in_writer
        in_writer.append(True)
        try:
            writes.append(set(kwargs["rows"]))
            if len(writes) == 1:
                entered.set()
                assert release.wait(5)
            return original(**kwargs)
        finally:
            in_writer.pop()

    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", writer)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()

    def later_provider():
        quotes.seed_tw_opening_snapshot_cache(["B"], snapshot([101.]),
                                             parquet_root=tmp_path, persist=False)
        merged.set()
        return quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(quotes.persist_tw_opening_snapshot_cache,
                            parquet_root=tmp_path, session_date=day)
        assert entered.wait(2)
        second = pool.submit(later_provider)
        try:
            assert merged.wait(0.5)
            assert not second.done()
        finally:
            release.set()
        first.result(timeout=2)
        path = second.result(timeout=2)
    assert writes == [{"A"}, {"A", "B"}]
    assert set(json.loads(path.read_text())["rows"]) == {"A", "B"}
    assert not quotes._TW_MIS_OPENING_DIRTY


def test_receipt_metadata_reuses_only_exact_bytes_and_never_supplies_price_rows(owner, tmp_path, monkeypatch):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    path = quotes._tw_mis_opening_receipt_path(day)
    original = quotes.private_json.loads
    decoded = []

    def loads(body):
        decoded.append(body)
        return original(body)

    monkeypatch.setattr(quotes.private_json, "loads", loads)
    quotes._opening_receipt_scope_fingerprint.cache_clear()

    def fingerprint():
        info = path.stat()
        return quotes._opening_receipt_scope_fingerprint(str(path.resolve()), info.st_mtime_ns,
            info.st_size, info.st_ctime_ns, info.st_ino, info.st_dev)

    assert fingerprint() is None and not decoded
    # Rebuild prices still uses the complete validated receipt, even when its
    # metadata is known. Header knowledge cannot replace restore/data evidence.
    restored = quotes._load_tw_mis_opening_receipt(parquet_root=str(tmp_path.resolve()), session_date=day)
    assert restored["A"][1]["open_prices"] == 100. and len(decoded) == 1
    metadata = json.loads(path.read_bytes())
    metadata["official_trading_universe"] = {"fingerprint": "version_A"}
    path.write_text(json.dumps(metadata))
    assert fingerprint() == "version_A" and len(decoded) == 2
    old_info = path.stat()
    replacement = path.with_suffix(".replacement")
    replacement.write_text(path.read_text().replace("version_A", "version_B"))
    assert replacement.stat().st_size == old_info.st_size
    os.utime(replacement, ns=(old_info.st_atime_ns, old_info.st_mtime_ns))
    os.replace(replacement, path)
    # Old mtime+size keys would return version_A for this different inode.
    assert fingerprint() == "version_B" and len(decoded) == 3
    # A matching wire body at a DIFFERENT path cannot borrow writer metadata.
    other = tmp_path / "other.json"
    known_body = quotes._TW_OPENING_RECEIPT_METADATA[1]
    other.write_bytes(known_body)
    quotes._opening_receipt_metadata(other, known_body)
    assert len(decoded) == 4


@pytest.mark.parametrize("native_available", [True, False])
def test_scoped_receipt_encoder_keeps_finite_bits_and_strict_metadata(owner, tmp_path, monkeypatch, native_available):
    try:
        importlib.import_module("msgspec")
    except ImportError:
        if native_available:
            pytest.skip("optional native encoder unavailable")
    if not native_available:
        monkeypatch.setitem(sys.modules, "msgspec", None)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    numbers = [np.nextafter(0., 1.), 100.12345678901235, np.finfo(np.float64).max]
    value = snapshot(numbers)
    value.volumes = np.array([0., -0., float(2 ** 53 + 2)])
    quotes.seed_tw_opening_snapshot_cache(list("ABC"), value, parquet_root=tmp_path, persist=False)
    timing = {}
    path = quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day, timings=timing)
    payload = json.loads(path.read_bytes())
    assert timing["receipt_stages"]["json_encoder"] == (
        "schema_checked_native_json_v1" if native_available else "strict_stdlib_json")
    for index, symbol in enumerate("ABC"):
        for field, wanted in (("open_prices", numbers[index]), ("volumes", value.volumes[index])):
            assert struct.pack("!d", payload["rows"][symbol][field]) == struct.pack("!d", wanted)
    # The native fast path must preserve numeric types too, not merely Python
    # numeric equality. Unknown metadata keeps the exact legacy/error path.
    payload["official_trading_universe"] = {"fingerprint": "test", "nested": [2 ** 53 + 1, True, None, -0.]}
    for normalized in (True, False):
        wire, _ = quotes._encode_tw_opening_receipt(payload, normalized_rows=normalized)
        actual = json.loads(wire)
        assert actual == payload
        assert type(actual["official_trading_universe"]["nested"][0]) is int
    before = path.read_bytes()
    for invalid in (float("nan"), float("inf"), -float("inf"), b"not-a-json-string", datetime.now(TAIPEI)):
        payload["rows"]["A"]["opening_conflicts"] = [{"value": invalid}]
        with pytest.raises((ValueError, TypeError)):
            quotes._encode_tw_opening_receipt(payload, normalized_rows=True)
        assert path.read_bytes() == before
    payload["rows"]["A"]["opening_conflicts"] = [{"counter": 2 ** 128}]
    wire, policy = quotes._encode_tw_opening_receipt(payload, normalized_rows=True)
    assert policy == "strict_stdlib_json" and json.loads(wire) == payload
    payload["rows"]["A"]["opening_conflicts"] = []
    payload["source_artifact"] = "\ud800"
    with pytest.raises(UnicodeEncodeError):
        quotes._encode_tw_opening_receipt(payload, normalized_rows=True)
    loop = []
    loop.append(loop)
    payload["source_artifact"] = loop
    with pytest.raises(ValueError, match="Circular"):
        quotes._encode_tw_opening_receipt(payload, normalized_rows=True)


@pytest.mark.parametrize("return_to_original_root", [False, True])
def test_source_flush_rejects_replaced_cache_generation(owner, tmp_path, monkeypatch, return_to_original_root):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path, persist=False)
    entered, release = threading.Event(), threading.Event()
    original = quotes._persist_tw_mis_opening_receipt

    def slow_flush(**kwargs):
        entered.set()
        assert release.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", slow_flush)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    with ThreadPoolExecutor(max_workers=2) as pool:
        flushing = pool.submit(quotes.persist_tw_opening_snapshot_cache,
                               parquet_root=tmp_path, session_date=day)
        assert entered.wait(2)
        def replace_generation():
            result = quotes.seed_tw_opening_snapshot_cache(["B"], snapshot([101.]),
                parquet_root=tmp_path / "different-root", persist=False)
            if return_to_original_root:
                result = quotes.seed_tw_opening_snapshot_cache(["C"], snapshot([102.]),
                    parquet_root=tmp_path, persist=False)
            return result

        replacement = pool.submit(replace_generation)
        try:
            assert replacement.result(timeout=0.5)["accepted_count"] == 1
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="generation changed"):
            flushing.result(timeout=2)
    assert quotes._TW_MIS_OPENING_DIRTY
    root = tmp_path if return_to_original_root else tmp_path / "different-root"
    path = quotes.persist_tw_opening_snapshot_cache(parquet_root=root, session_date=day)
    receipt = json.loads(path.read_text())
    assert receipt["parquet_root"] == str(root.resolve())
    assert set(receipt["rows"]) == ({"C"} if return_to_original_root else {"B"})


def test_source_flush_failure_keeps_concurrent_facts_and_retries(owner, tmp_path, monkeypatch):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path, persist=False)
    day = acquisition.datetime.now(TAIPEI).date().isoformat()
    timings = {}

    def failing_writer(**kwargs):
        quotes.seed_tw_opening_snapshot_cache(["B"], snapshot([101.]), parquet_root=tmp_path, persist=False)
        raise OSError("injected durable flush failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(quotes, "_persist_tw_mis_opening_receipt", failing_writer)
        with pytest.raises(OSError, match="flush failure"):
            quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day, timings=timings)
    assert timings == {}, "failed storage must not emit a successful flush measurement"
    assert quotes._TW_MIS_OPENING_DIRTY
    path = quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=day)
    assert set(json.loads(path.read_text())["rows"]) == {"A", "B"}
    assert not quotes._TW_MIS_OPENING_DIRTY


def test_source_flush_does_not_release_yesterdays_quorum_after_midnight(owner, tmp_path, monkeypatch):
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.]), parquet_root=tmp_path, persist=False)
    now = acquisition.datetime.now(TAIPEI)
    original = quotes._persist_tw_mis_opening_receipt

    class NextDay(datetime):
        @classmethod
        def now(cls, tz=None):
            return (now + timedelta(days=1)).astimezone(tz)

    def writer(**kwargs):
        result = original(**kwargs)
        monkeypatch.setattr(quotes, "datetime", NextDay)
        return result

    monkeypatch.setattr(quotes, "_persist_tw_mis_opening_receipt", writer)
    with pytest.raises(RuntimeError, match="generation changed"):
        quotes.persist_tw_opening_snapshot_cache(parquet_root=tmp_path, session_date=now.date().isoformat())
    assert quotes._TW_MIS_OPENING_DIRTY


def test_float64_fast_conversion_matches_text_contract_bit_for_bit():
    values = np.random.default_rng(726).lognormal(0., 100., 1000)
    values = np.r_[values, np.nextafter(0., 1.), np.finfo(np.float64).max, 100.12345678901235]
    for value in values:
        expected = float(str(value))
        assert quotes._float_or_none(value).hex() == expected.hex()
        assert quotes._quantity_or_none(value).hex() == expected.hex()
    for value in (np.float64(np.nan), np.float64(np.inf), np.float64(-1.)):
        assert quotes._float_or_none(value) is None
        assert quotes._quantity_or_none(value) is None
    assert quotes._quantity_or_none(np.float64(0.)) == 0.
    assert quotes._float_or_none(np.float32(100.1)) == float(str(np.float32(100.1)))
    assert quotes._float_or_none(np.float32(100.1)) != float(np.float32(100.1))
    assert quotes._quantity_or_none(np.bool_(True)) is None
    assert quotes._float_or_none(True) is None


def test_preopen_and_closed_generation_do_not_query(owner, monkeypatch, tmp_path):
    class Preopen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(TAIPEI).replace(hour=8, minute=59)

    monkeypatch.setattr(acquisition, "datetime", Preopen)
    with pytest.raises(RuntimeError, match="before 09:00"):
        fetch(owner, tmp_path, ["A"], {"source": lambda *a: pytest.fail("preopen provider must not run")})


def test_late_yesterday_callback_cannot_rewind_cache(owner, tmp_path):
    today = acquisition.datetime.now(TAIPEI).date().isoformat()
    quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([100.0]), parquet_root=tmp_path)
    yesterday = (acquisition.datetime.now(TAIPEI) - timedelta(days=1)).date().isoformat()
    result = quotes.seed_tw_opening_snapshot_cache(["A"], snapshot([90.0]), parquet_root=tmp_path, session_date=yesterday)
    assert result["accepted_count"] == 0
    assert quotes._TW_MIS_OPENING_CACHE_KEY[1] == today
    assert quotes._TW_MIS_OPENING_CACHE["A"][1]["open_prices"] == 100.0


def test_finmind_adapter_uses_dated_open_not_close_or_books(owner, monkeypatch):
    now = acquisition.datetime.now(TAIPEI)
    payload = {"status": 200, "data": [
        {"stock_id": "A", "date": now.isoformat(), "open": 100, "close": 105, "total_volume": 1},
        {"stock_id": "B", "date": (now - timedelta(days=1)).isoformat(), "open": 90, "total_volume": 1},
        {"stock_id": "C", "date": now.isoformat(), "open": 101, "total_volume": 0},
    ]}
    class Session:
        def get(self, url, **kwargs):
            assert kwargs["params"] == {"data_id": ""}
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)
    monkeypatch.setattr(acquisition, "_http_session", Session)
    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    result = acquisition.fetch_finmind_opening_prices(["A", "B", "C"], np.full(3, 99.0), lambda *a: None)
    np.testing.assert_array_equal(result.open_prices, [100.0, np.nan, np.nan])
    assert result.bid_prices is None and result.simtrade_flags is None


def test_fugle_adapter_validates_date_market_and_microsecond_clock(owner, monkeypatch):
    now = acquisition.datetime.now(TAIPEI)
    payload = {"date": now.date().isoformat(), "market": "TSE", "data": [{"type": "EQUITY", "symbol": "A", "openPrice": 100, "tradeVolume": 1, "lastUpdated": int(now.timestamp() * 1e6)}]}
    monkeypatch.setenv("FUGLE_API_KEY", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=lambda *a, **k: SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)))
    result = acquisition.fugle_market_provider("TSE")(["A"], np.array([99.0]), lambda *a: None)
    assert result.open_prices.tolist() == [100.0]
    assert result.exchange_timestamps_ms.tolist() == [int(now.timestamp() * 1000)]
    assert result.transport_timing["row_validation_projection_ms"] >= 0
    assert result.transport_timing["http_headers_ms"] >= 0
    payload["date"] = (now - timedelta(days=1)).date().isoformat()
    with pytest.raises(ValueError, match="date/market"):
        acquisition.fugle_market_provider("TSE")(["A"], np.array([99.0]), lambda *a: None)


def test_fugle_permission_failure_is_closed_timed_and_does_not_block_other_sources(owner, tmp_path, monkeypatch):
    closed = []
    def denied():
        raise acquisition.requests.HTTPError("private-error-must-not-be-persisted", response=response)
    response = SimpleNamespace(status_code=403, raise_for_status=denied, close=lambda: closed.append(True))
    def request(_url, **kwargs):
        assert kwargs["params"] == {"type": "ALLBUT0999"}
        return response
    monkeypatch.setenv("FUGLE_API_KEY", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=request))
    result = fetch(owner, tmp_path, ["A"], {
        "fugle_tse": acquisition.fugle_market_provider("TSE"),
        "healthy": lambda *args: snapshot([100.]),
    })
    assert result.transport_timing["opening_quorum_ready"] and owner.wait_idle(timeout_seconds=2)
    assert closed == [True]
    key = (str(tmp_path.resolve()), acquisition.datetime.now(TAIPEI).date().isoformat())
    status = owner._sessions[key].metrics["fugle_tse"]
    assert status["http_status"] == 403 and status["retry_delay_seconds"] == 60.
    failure = status["last_completed_job"]["transport_timing"]
    assert failure["http_failure_phase"] == "http_status" and failure["http_request_to_failure_ms"] >= 0
    journal = quotes._tw_mis_opening_receipt_path(acquisition.datetime.now(TAIPEI).date().isoformat()).with_suffix(".acquisition.jsonl").read_text()
    assert "private-test-token" not in journal and "private-error-must-not-be-persisted" not in journal


def test_shioaji_wall_clock_is_localized_not_shifted(owner):
    now = acquisition.datetime.now(TAIPEI)
    row = SimpleNamespace(code="A", ts=int(np.datetime64(now.replace(tzinfo=None), "ns").astype(np.int64)), open=100, close=100)
    values = quotes._shioaji_snapshot_values(row, SimpleNamespace(reference=99), received_ms=int(now.timestamp() * 1000))
    assert values["exchange_ms"] == int(now.timestamp() * 1000)
    before = now.replace(hour=8, minute=59)
    row.ts = int(np.datetime64(before.replace(tzinfo=None), "ns").astype(np.int64))
    result = quotes._shioaji_opening_progress_snapshot(["A"], np.array([99]), [(row, int(now.timestamp() * 1000))], {"A": SimpleNamespace(reference=99)})
    assert not quotes.tw_session_open_observed_mask(result).any()


def test_broker_worker_does_not_block_engine_poll(owner, monkeypatch, tmp_path):
    release, started = threading.Event(), threading.Event()
    inbox = quotes.day_trade_quote_broker_request_dir(tmp_path)
    (inbox / "test.json").write_text("{}")
    def slow(**kwargs):
        started.set()
        release.wait(timeout=2)
        return [{"available_count": 1}]
    monkeypatch.setattr(quotes, "serve_shared_day_trade_quote_requests", slow)
    broker = quotes.SharedDayTradeQuoteBroker(tmp_path)
    try:
        assert broker.poll() == []
        assert started.wait(timeout=2)
        measured = time.perf_counter()
        assert broker.poll() == []
        assert time.perf_counter() - measured < 0.05
    finally:
        release.set()
        broker.close()


def test_autonomous_broker_serves_while_engine_is_busy_and_requires_session(owner, monkeypatch, tmp_path):
    inbox = quotes.day_trade_quote_broker_request_dir(tmp_path)
    path = inbox / "pending.json"
    path.write_text("{}")
    served = threading.Event()

    def serve(**kwargs):
        if not path.exists():
            return []  # Canonical server never serves an already-drained inbox.
        path.unlink()
        served.set()
        return [{"available_count": 1, "snapshot": {"large": "body"}}]

    monkeypatch.setattr(quotes, "serve_shared_day_trade_quote_requests", serve)
    broker = quotes.SharedDayTradeQuoteBroker(tmp_path, autonomous=True)
    try:
        assert not served.wait(0.08)  # No verified session means no quote I/O.
        broker.authorize("1900-01-01")
        assert not served.wait(0.08)  # Yesterday cannot authorize a new session.
        broker.authorize(acquisition.datetime.now(TAIPEI).date().isoformat())
        assert served.wait(2)  # No engine poll is needed to dispatch the request.
        deadline = time.monotonic() + 2
        results = []
        while not results and time.monotonic() < deadline:
            results = broker.poll()
            time.sleep(0.001)
        assert results == [{"available_count": 1}]
    finally:
        broker.close()


def test_autonomous_broker_protects_opening_purpose_and_releases_deferred_inbox(owner, monkeypatch, tmp_path):
    inbox = quotes.day_trade_quote_broker_request_dir(tmp_path)
    path = inbox / "pending.json"
    path.write_text("{}")
    clock = [datetime(2026, 10, 15, 9, 0, tzinfo=TAIPEI)]

    class Clock:
        @staticmethod
        def now(_zone):
            return clock[0]

    monkeypatch.setattr(quotes, "datetime", Clock)
    protected, ordinary = threading.Event(), threading.Event()

    def serve(**kwargs):
        if kwargs["allowed_purposes"] == {"opening_signal"}:
            protected.set()
            return []  # Interactive inbox remains unchanged during hot gate.
        path.unlink()
        ordinary.set()
        return [{"available_count": 1}]

    monkeypatch.setattr(quotes, "serve_shared_day_trade_quote_requests", serve)
    broker = quotes.SharedDayTradeQuoteBroker(tmp_path, autonomous=True)
    try:
        broker.authorize("2026-10-15")
        assert protected.wait(2)
        assert not ordinary.is_set()
        clock[0] = clock[0].replace(second=15)
        assert ordinary.wait(2)  # Policy change must invalidate the inbox scan.
    finally:
        broker.close()


@pytest.mark.parametrize("session", ["2026-10-08", "2026-02-28", "2024-02-29", "invalid", "20261008"])
def test_vectorized_clock_matches_original_scalar_contract(session):
    center = datetime.fromisoformat("2026-10-08T00:00:00+08:00")
    random = np.random.default_rng(72487)
    stamps = np.concatenate((
        np.array([0, -1, 2**63 - 1, -(2**63)], dtype=np.int64),
        np.array([int((center + timedelta(hours=hour)).timestamp() * 1000) + edge
                  for hour in (0, 9, 24) for edge in (-1, 0, 1)], dtype=np.int64),
        random.integers(int(center.timestamp() * 1000) - 3 * 86400000,
                        int(center.timestamp() * 1000) + 3 * 86400000, size=500),
    ))

    def legacy(stamp, opening):
        if stamp <= 0:
            return False
        try:
            observed = datetime.fromtimestamp(int(stamp) / 1000, TAIPEI)
        except (OSError, OverflowError, ValueError):
            return False
        return observed.date().isoformat() == session and (not opening or observed.hour >= 9)

    expected = [legacy(stamp, True) for stamp in stamps]
    np.testing.assert_array_equal(quotes._tw_opening_timestamp_mask(stamps, session), expected)
    assert [quotes._same_taipei_open_timestamp(int(stamp), session) for stamp in stamps] == expected
    assert [quotes._same_taipei_session_timestamp(int(stamp), session) for stamp in stamps] == [legacy(stamp, False) for stamp in stamps]


@pytest.mark.parametrize("field", ["open_prices", "simtrade_flags", "volumes", "exchange_timestamps_ms"])
def test_malformed_source_arrays_do_not_partially_mutate_cache(owner, tmp_path, field):
    result = snapshot([100.0, 101.0])
    setattr(result, field, np.array([1]))
    with pytest.raises(ValueError, match="shape"):
        quotes.seed_tw_opening_snapshot_cache(["A", "B"], result, parquet_root=tmp_path)
    assert not quotes._TW_MIS_OPENING_CACHE
    assert not quotes._TW_MIS_OPENING_REQUESTED


def test_optional_provider_retry_obeys_its_published_cadence(owner, tmp_path):
    calls = []

    def provider(*args):
        calls.append(1)
        return snapshot([np.nan], source="finmind:opening")

    providers = {"finmind_full_market": provider}
    fetch(owner, tmp_path, ["A"], providers, resolve_no_prints=False)
    fetch(owner, tmp_path, ["A"], providers, resolve_no_prints=False, force_refresh=True)
    assert calls == [1]


@pytest.mark.parametrize("second,expected", [(0, 1.), (10, 1.), (11, 10.), (30, 10.)])
def test_finmind_short_poll_is_bounded_to_the_opening_deadline(second, expected):
    observed = datetime(2026, 10, 16, 9, 0, second, tzinfo=TAIPEI)
    assert acquisition._snapshot_poll_interval("finmind_snapshot", observed) == expected
    assert acquisition._snapshot_poll_interval("shioaji", observed) is None


def test_finmind_retries_inside_one_wait_without_waiting_for_slow_provider(owner, tmp_path, monkeypatch):
    release, retried = threading.Event(), threading.Event()
    calls = []
    monkeypatch.setattr(acquisition, "_snapshot_poll_interval", lambda name, now: .03 if name.startswith("finmind") else None)

    def finmind(symbols, fallback, progress):
        calls.append(time.monotonic())
        result = snapshot([100. if len(calls) > 1 else np.nan] * len(symbols))
        result.transport_timing = {"response_schema_valid": True}
        if len(calls) > 1:
            retried.set()
        return result

    def slow(*args):
        assert release.wait(2)
        raise ConnectionError("loser does not own FinMind's retry clock")

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(fetch, owner, tmp_path, ["A", "B"],
                {"finmind_snapshot": finmind, "slow": slow}, poll_opening_snapshots=True)
            assert retried.wait(1)
            result = future.result(timeout=1)
            assert not release.is_set()
            assert result.transport_timing["opening_snapshot_poll_owner"] == "bounded_shared_fan_in"
            assert quotes.tw_session_open_observed_mask(result).all()
            assert len(calls) == 2 and calls[1] - calls[0] >= .025
    finally:
        release.set()


def test_finmind_one_wait_retries_only_unknown_and_stops_at_quorum(owner, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(acquisition, "_snapshot_poll_interval", lambda *a: .01)

    def provider(symbols, fallback, progress):
        calls.append(symbols)
        result = snapshot([100., np.nan] if len(calls) == 1 else [101.])
        result.transport_timing = {"response_schema_valid": True}
        return result

    result = fetch(owner, tmp_path, ["A", "B"], {"finmind_snapshot": provider},
        minimum_coverage=1., poll_opening_snapshots=True)
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [["A", "B"], ["B"]]
    np.testing.assert_array_equal(result.open_prices, [100., 101.])


@pytest.mark.parametrize("failure", ["quota", "permission"])
def test_finmind_inside_wait_never_shortens_error_backoff(owner, tmp_path, monkeypatch, failure):
    calls = []
    monkeypatch.setattr(acquisition, "_snapshot_poll_interval", lambda *a: .001)

    def provider(*args):
        calls.append(1)
        if failure == "permission":
            raise PermissionError("private credential")
        import requests
        raise requests.HTTPError(response=SimpleNamespace(status_code=429))

    result = fetch(owner, tmp_path, ["A"], {"finmind_snapshot": provider},
        poll_opening_snapshots=True, timeout_seconds=.04)
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [1]
    assert not result.transport_timing["opening_quorum_ready"]
    assert next(iter(owner._sessions.values())).metrics["finmind_snapshot"]["retry_delay_seconds"] == 60.


def test_finmind_inside_wait_is_bounded_and_closed_owner_wakes(owner, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(acquisition, "_snapshot_poll_interval", lambda *a: .03)

    def provider(*args):
        calls.append(1)
        result = snapshot([np.nan])
        result.transport_timing = {"response_schema_valid": True}
        return result

    result = fetch(owner, tmp_path, ["A"], {"finmind_snapshot": provider},
        poll_opening_snapshots=True, timeout_seconds=.08)
    assert not result.transport_timing["opening_quorum_ready"]
    assert 1 <= len(calls) <= 3
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fetch, owner, tmp_path, ["B"], {"finmind_snapshot": provider},
            poll_opening_snapshots=True)
        time.sleep(.01)
        owner.close()
        with pytest.raises(RuntimeError, match="superseded|closed"):
            future.result(timeout=1)


def test_finmind_valid_old_snapshot_uses_start_clock_not_failure_backoff(owner, tmp_path, monkeypatch):
    clock, calls = [100.], []
    monkeypatch.setattr(acquisition, "time", SimpleNamespace(
        monotonic=lambda: clock[0], perf_counter=time.perf_counter))

    def provider(symbols, fallback, progress):
        calls.append(clock[0])
        clock[0] += .8  # Transfer/decode time is inside the start cadence.
        result = snapshot([np.nan] * len(symbols))
        result.available_mask[:] = False
        result.available_count = 0
        result.transport_timing = {"response_schema_valid": True}
        return result

    providers = {"finmind_snapshot": provider}
    fetch(owner, tmp_path, ["A"], providers, resolve_no_prints=False)
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["finmind_snapshot"]
    assert metric["consecutive_source_failures"] == 0
    assert metric["consecutive_source_no_progress"] == 1
    assert metric["retry_after_monotonic"] == 101.
    assert metric["last_completed_job"]["poll_clock_basis"] == "request_start_not_completion"
    clock[0] = 100.99
    fetch(owner, tmp_path, ["A"], providers, resolve_no_prints=False)
    assert len(calls) == 1
    clock[0] = 101.
    fetch(owner, tmp_path, ["A"], providers, resolve_no_prints=False)
    assert owner.wait_idle(timeout_seconds=1)
    assert calls == [100., 101.]
    assert metric["consecutive_source_failures"] == 0


def test_finmind_disjoint_model_delta_cannot_start_another_full_market_request(owner, tmp_path):
    started, release = threading.Event(), threading.Event()
    calls = []

    def provider(symbols, fallback, progress):
        calls.append(tuple(symbols))
        started.set()
        assert release.wait(timeout=3)
        return snapshot([100.] * len(symbols))

    providers = {"finmind_snapshot": provider}
    try:
        fetch(owner, tmp_path, ["A"], providers, timeout_seconds=.01)
        assert started.wait(timeout=1)
        # B is not in A's per-symbol in-flight set. The endpoint still must
        # remain single-flight because data_id="" fetches the whole market.
        fetch(owner, tmp_path, ["B"], providers, timeout_seconds=.01)
        assert calls == [("A",)]
    finally:
        release.set()
    assert owner.wait_idle(timeout_seconds=1)


@pytest.mark.parametrize("status", [400, 401, 402, 403, 429])
def test_finmind_short_poll_never_overrides_permission_or_quota_backoff(owner, tmp_path, monkeypatch, status):
    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"status": status})

    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=get))
    providers = {"finmind_snapshot": acquisition.fetch_finmind_opening_prices}
    fetch(owner, tmp_path, ["A"], providers)
    assert owner.wait_idle(timeout_seconds=1)
    metric = next(iter(owner._sessions.values())).metrics["finmind_snapshot"]
    assert metric["retry_delay_seconds"] == 60.
    fetch(owner, tmp_path, ["B"], providers)
    assert calls == [1]


def test_finmind_diagnostics_explain_old_rows_without_publishing_old_prices(owner, monkeypatch):
    now = acquisition.datetime.now(TAIPEI)
    rows = [
        {"stock_id": "A", "date": now.isoformat(), "open": 100, "total_volume": 1},
        {"stock_id": "B", "date": (now - timedelta(days=1)).isoformat(), "open": 100, "total_volume": 1},
        {"stock_id": "C", "date": now.isoformat(), "open": 100, "total_volume": 0},
        {"stock_id": "D", "date": now.isoformat(), "open": 100, "total_volume": float("nan")},
        {"stock_id": "E", "date": now.isoformat(), "open": 0, "total_volume": 1},
        {"stock_id": "F", "date": now.isoformat(), "open": 100, "total_volume": 1, "isTrial": True},
        {"stock_id": "G", "date": (now + timedelta(seconds=5)).isoformat(), "open": 100, "total_volume": 1},
        {"stock_id": "H", "date": "malformed", "open": 100, "total_volume": 1},
        {"stock_id": "outside-request", "date": now.isoformat(), "open": 100, "total_volume": 1},
    ]
    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=lambda *a, **k:
        SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"status": 200, "data": rows})))
    result = acquisition.fetch_finmind_opening_prices(list("ABCDEFGH"), np.full(8, 99.), lambda *a: None)
    assert quotes.tw_session_open_observed_mask(result).tolist() == [True] + [False] * 7
    assert result.transport_timing["provider_row_quality"] == {
        "requested_rows": 8, "malformed_rows": 1, "trial_rows": 1,
        "no_positive_volume_rows": 2, "invalid_open_rows": 1,
        "other_session_rows": 1, "future_timestamp_rows": 1, "accepted_rows": 1}
    full = result.transport_timing["provider_snapshot_coverage"]
    assert full["valid_open_symbols"] == ["A", "outside-request"]
    assert full["rejected_symbols"]["other_session_rows"] == ["B"]


def test_finmind_full_view_and_connection_follow_owner_not_pool_thread(owner, monkeypatch):
    now = acquisition.datetime.now(TAIPEI)
    payload = {"status": 200, "data": [{"stock_id": symbol, "date": now.isoformat(),
        "open": 100 + i, "total_volume": 1} for i, symbol in enumerate("AB")]}
    calls, sessions, closed = [], [], []
    body = json.dumps(payload).encode()

    def make_session():
        sessions.append(threading.get_ident())
        def get(*args, **kwargs):
            assert kwargs["stream"] is True
            calls.append(threading.get_ident())
            return SimpleNamespace(content=body, raise_for_status=lambda: None,
                json=lambda: pytest.fail("real body must use the shared fast decoder"),
                close=lambda: closed.append(1))
        return SimpleNamespace(get=get)

    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", make_session)
    first = acquisition.fetch_finmind_opening_prices(["A"], np.array([99.]), lambda *a: None)
    with ThreadPoolExecutor(max_workers=1) as pool:
        second = pool.submit(acquisition.fetch_finmind_opening_prices,
            ["B"], np.array([99.]), lambda *a: None).result(timeout=1)
        third = pool.submit(acquisition.fetch_finmind_opening_prices,
            ["A", "B"], np.full(2, 99.), lambda *a: None, force_network=True).result(timeout=1)
    assert len(calls) == len(closed) == 2 and len(sessions) == 1
    assert calls[0] != calls[1]
    assert not first.transport_timing["provider_response_reused"]
    assert second.transport_timing["provider_response_reused"]
    assert not third.transport_timing["provider_response_reused"]
    assert first.transport_timing["provider_snapshot_coverage"]["snapshot_id"] == second.transport_timing["provider_snapshot_coverage"]["snapshot_id"]
    assert first.timestamps_ms[0] == second.timestamps_ms[0]
    np.testing.assert_array_equal(third.open_prices, [100., 101.])


def test_finmind_token_and_open_gate_invalidate_view_not_the_connection(owner, monkeypatch):
    clock = [acquisition.datetime.now(TAIPEI).replace(hour=8, minute=59, second=59)]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    calls, sessions = [], []
    def make_session():
        sessions.append(1)
        def get(*args, **kwargs):
            calls.append(kwargs["headers"]["Authorization"])
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"status": 200,
                "data": [{"stock_id": "A", "date": clock[0].isoformat(), "open": 100, "total_volume": 1}]})
        return SimpleNamespace(get=get)
    monkeypatch.setattr(acquisition, "datetime", Clock)
    monkeypatch.setattr(acquisition, "_http_session", make_session)
    monkeypatch.setenv("FINMIND_TOKEN", "one")
    before = acquisition.fetch_finmind_opening_prices(["A"], np.array([99.]), lambda *a: None)
    clock[0] = clock[0].replace(hour=9, minute=0, second=0)
    after = acquisition.fetch_finmind_opening_prices(["A"], np.array([99.]), lambda *a: None)
    monkeypatch.setenv("FINMIND_TOKEN", "two")
    rotated = acquisition.fetch_finmind_opening_prices(["A"], np.array([99.]), lambda *a: None)
    assert calls == ["Bearer one", "Bearer one", "Bearer two"] and sessions == [1]
    assert before.available_count == 0 and after.available_count == rotated.available_count == 1
    assert not rotated.transport_timing["provider_response_reused"]


def test_finmind_cooldown_cannot_be_bypassed_by_other_consumers_or_force(owner, monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"status": 429})
    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=get))
    for force in (False, False, True):
        with pytest.raises(PermissionError):
            acquisition.fetch_finmind_opening_prices(["A"], np.array([99.]), lambda *a: None, force_network=force)
    assert calls == [1]


def test_full_provider_snapshot_is_journalled_once_but_each_axis_is_projected(owner, tmp_path, monkeypatch):
    now = acquisition.datetime.now(TAIPEI)
    monkeypatch.setenv("FINMIND_TOKEN", "private-test-token")
    monkeypatch.setattr(acquisition, "_http_session", lambda: SimpleNamespace(get=lambda *a, **k:
        SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"status": 200, "data": [
            {"stock_id": symbol, "date": now.isoformat(), "open": 100, "total_volume": 1} for symbol in "AB"]})))
    providers = {"finmind_snapshot": acquisition.fetch_finmind_opening_prices}
    assert fetch(owner, tmp_path, ["A"], providers).available_count == 1
    assert owner.wait_idle(timeout_seconds=1)
    # Exercise a disjoint consumer after the owner cadence; the HTTP view is
    # still fresh and no prices are copied from A to B.
    session = next(iter(owner._sessions.values()))
    session.metrics["finmind_snapshot"].pop("retry_after_monotonic", None)
    assert fetch(owner, tmp_path, ["B"], providers).available_count == 1
    assert owner.wait_idle(timeout_seconds=1)
    journal = quotes._tw_mis_opening_receipt_path(now.date().isoformat()).with_suffix(".acquisition.jsonl")
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    snapshots = [row for row in events if row["event"] == "provider_snapshot"]
    assert len(snapshots) == 1 and snapshots[0]["coverage"]["valid_open_symbols"] == ["A", "B"]
    assert snapshots[0]["owner_pid"] == os.getpid()
    assert all("provider_snapshot_coverage" not in row.get("transport_timing", {})
               for row in events if row["event"] == "finished")


@pytest.mark.parametrize("override,workers", [(None, 4), ("16", 16)])
def test_mis_measured_default_is_source_local_and_never_shrinks_the_axis(owner, tmp_path, monkeypatch, override, workers):
    if override is None:
        monkeypatch.delenv("STOCKAGENT_TW_OPENING_PARALLEL_REQUESTS", raising=False)
    else:
        monkeypatch.setenv("STOCKAGENT_TW_OPENING_PARALLEL_REQUESTS", override)
    calls = []
    def mis(symbols, fallback, **kwargs):
        calls.append((symbols, kwargs))
        return snapshot([100.] * len(symbols))
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", mis)
    result = acquisition.configured_opening_providers(tmp_path)["official_mis"](list("AB"), np.full(2, 99.), lambda *a: None)
    assert result.requested_count == 2 and calls[0][0] == list("AB")
    assert calls[0][1]["max_parallel_requests"] == workers
    assert calls[0][1]["empty_chunk_retry_attempts"] == 0


def test_native_partial_callback_precedes_last_batch_and_releases_stock_lock(owner, monkeypatch):
    from contextlib import contextmanager
    first_progress, release_last = threading.Event(), threading.Event()
    symbols = [str(1000 + i) for i in range(501)]
    contracts = {symbol: SimpleNamespace(code=symbol, reference=99.0, limit_up=110.0, limit_down=90.0) for symbol in symbols}
    now = acquisition.datetime.now(TAIPEI)
    stamp = int(np.datetime64(now.replace(tzinfo=None), "ns").astype(np.int64))

    def rows(batch):
        return [SimpleNamespace(code=contract.code, ts=stamp, open=100.0, close=100.0, total_volume=1) for contract in batch]

    class Api:
        def __init__(self):
            self.contracts = SimpleNamespace(get=contracts.get)
            self.callbacks = []

        def snapshots(self, batch, *, timeout, cb):
            assert timeout == 0
            if len(batch) == 500:
                cb(rows(batch))
            else:
                self.callbacks.append(lambda: cb(rows(batch)))
            return []

    @contextmanager
    def query(*args, **kwargs):
        yield lambda *args, **kwargs: None

    api = Api()
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_CACHE", {})
    monkeypatch.setattr(quotes, "shioaji_query", query)
    monkeypatch.setattr(quotes, "_load_prepared_tw_price_limits", lambda *args, **kwargs: ({}, None))

    def progress(requested, result):
        assert requested == symbols
        if result.available_count == 500:
            first_progress.set()
            assert release_last.wait(timeout=2)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(quotes._fetch_shioaji_stock_snapshots_once, symbols, np.full(501, 99.0), cache_ttl_seconds=0, api=api, on_progress=progress)
        try:
            assert first_progress.wait(timeout=1), "first synchronous callback must not wait for the slow final batch"
            assert not future.done()
            acquired = quotes._SHIOAJI_STOCK_LOCK.acquire(timeout=0.1)
            assert acquired, "native network wait/progress must not block execution quote access"
            quotes._SHIOAJI_STOCK_LOCK.release()
            api.callbacks[0]()
        finally:
            release_last.set()
        assert future.result(timeout=2).available_count == 501


def test_mode_spec_resolution_does_not_create_execution_broker(monkeypatch, tmp_path):
    from scripts import run_tw_day_trade_simulation as service
    monkeypatch.setattr(service, "SharedDayTradeQuoteBroker", lambda *args: pytest.fail("only the main execution owner may instantiate the broker"))
    specs, configs, errors = service._mode_specs(tmp_path)
    assert not specs and not configs and not errors


@pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
def test_native_timestamp_units_preserve_taipei_wall_clock(owner, unit):
    now = acquisition.datetime.now(TAIPEI)
    row = SimpleNamespace(code="A", ts=int(np.datetime64(now.replace(tzinfo=None), unit).astype(np.int64)), open=100, close=100)
    result = quotes._shioaji_snapshot_values(row, SimpleNamespace(reference=99), received_ms=int(now.timestamp() * 1000))
    assert result["exchange_ms"] == int(now.timestamp() * 1000)


@pytest.mark.parametrize("response_format", ["json_v1", "msgpack_v1"])
def test_shared_ipc_delivers_cumulative_partial_before_final(owner, monkeypatch, tmp_path, response_format):
    if response_format == "msgpack_v1":
        pytest.importorskip("msgspec")
    monkeypatch.setattr(quotes, "_quote_ipc_response_format", lambda: response_format)
    started, partial_observed, release = threading.Event(), threading.Event(), threading.Event()
    received_counts = []

    def native(symbols, fallback, *, on_progress, **kwargs):
        on_progress(symbols, snapshot([100.0, np.nan], source="shioaji:partial"))
        on_progress(symbols, snapshot([100.0, 101.0], source="shioaji:partial"))
        started.set()
        assert release.wait(timeout=2)
        return snapshot([100.0, 101.0], source="shioaji:final")

    def progress(symbols, result):
        received_counts.append(result.available_count)
        # Response availability may include NaN open placeholders; verify
        # actual opening proof rather than trusting a vendor response count.
        if quotes.tw_session_open_observed_mask(result).sum() == 2:
            partial_observed.set()

    monkeypatch.setattr(quotes, "fetch_shioaji_stock_snapshots", native)
    inbox = quotes.day_trade_quote_broker_request_dir(tmp_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        client = pool.submit(quotes.fetch_shared_day_trade_stock_snapshots, ["A", "B"], np.full(2, 99.0), state_dir=tmp_path, purpose="opening_signal", on_progress=progress, timeout_seconds=2)
        deadline = time.monotonic() + 1
        while not list(inbox.glob("*.json")) and time.monotonic() < deadline:
            time.sleep(0.001)
        server = pool.submit(quotes.serve_shared_day_trade_quote_requests, state_dir=tmp_path)
        try:
            assert started.wait(timeout=1)
            assert partial_observed.wait(timeout=1)
            assert not client.done(), "partial input availability is not provider completion"
        finally:
            release.set()
        assert client.result(timeout=1).open_prices.tolist() == [100.0, 101.0]
        assert len(server.result(timeout=1)) == 1
    assert received_counts


def test_transport_prewarm_is_independent_of_mis_and_never_seeds_prices(owner, tmp_path):
    barrier, release = threading.Barrier(2), threading.Event()

    def slow_mis():
        barrier.wait(timeout=2)
        release.wait(timeout=2)
        raise ConnectionError("official MIS offline")

    def healthy_broker():
        barrier.wait(timeout=2)
        return {"ready": True}

    try:
        result = owner.warm_transports(parquet_root=tmp_path, session_date="2026-10-08", probes={"official_mis": slow_mis, "shioaji_shared": healthy_broker})
        assert result["source"] == "parallel_opening_sources"
        assert result["providers"]["shioaji_shared"]["ready"]
        assert result["providers"]["official_mis"]["state"] == "running"
        assert not quotes._TW_MIS_OPENING_CACHE and not quotes._TW_MIS_OPENING_REQUESTED
        # The four final-arm passes borrow the same already-proven transport.
        repeated = owner.warm_transports(parquet_root=tmp_path, session_date="2026-10-08", probes={"official_mis": lambda: pytest.fail("must not duplicate"), "shioaji_shared": lambda: pytest.fail("must not duplicate")}, force=True)
        assert repeated["ready"]
    finally:
        release.set()


def test_all_failed_transport_probes_fail_closed(owner, tmp_path):
    with pytest.raises(RuntimeError, match="no configured opening source"):
        owner.warm_transports(parquet_root=tmp_path, session_date="2026-10-08", probes={"bad": lambda: {"ready": False}})
    assert not quotes._TW_MIS_OPENING_CACHE


def test_transport_prewarm_parses_venue_map_without_seeding_prices(owner, tmp_path, monkeypatch):
    import csv

    path = tmp_path / "symbols.csv"
    path.write_text("code,market\n2330,twse\n", encoding="utf-8")
    reader, parses = csv.DictReader, []

    def counted(*args, **kwargs):
        parses.append(1)
        return reader(*args, **kwargs)

    monkeypatch.setattr(csv, "DictReader", counted)
    result = owner.warm_transports(parquet_root=tmp_path, session_date="2026-10-08",
        probes={"test_transport": lambda: {"ready": True}})
    assert result["local_preparation"]["venue_mapping_count"] == 1
    assert result["local_preparation"]["venue_mapping_ms"] >= 0
    assert quotes.load_symbol_yahoo_map(tmp_path) == {"2330": "2330.TW"}
    assert parses == [1]
    assert not quotes._TW_MIS_OPENING_CACHE and not quotes._TW_MIS_OPENING_REQUESTED
    path.write_text("code,market\n2330,tpex\n", encoding="utf-8")
    assert quotes.load_symbol_yahoo_map(tmp_path) == {"2330": "2330.TWO"}
    assert parses == [1, 1]


def test_opening_mis_does_not_wait_for_optional_bootstrap(owner, monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", lambda *args, **kwargs: pytest.fail("opening quote API sweep must not wait for optional prewarm"))
    monkeypatch.setattr(quotes, "load_symbol_yahoo_map", lambda *args: {})
    result = quotes.fetch_tw_mis_last_prices(["A"], np.array([99.0]), parquet_root=tmp_path, on_progress=lambda *args: None)
    assert result.available_count == 0
    assert not quotes.tw_session_open_observed_mask(result).any()
