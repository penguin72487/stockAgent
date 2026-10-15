from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import os
import stat
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
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
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
    payload["date"] = (now - timedelta(days=1)).date().isoformat()
    with pytest.raises(ValueError, match="date/market"):
        acquisition.fugle_market_provider("TSE")(["A"], np.array([99.0]), lambda *a: None)


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
        path.unlink(missing_ok=True)
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


def test_shared_ipc_delivers_cumulative_partial_before_final(owner, monkeypatch, tmp_path):
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


def test_opening_mis_does_not_wait_for_optional_bootstrap(owner, monkeypatch, tmp_path):
    monkeypatch.setattr(quotes, "warm_tw_mis_quote_client", lambda *args, **kwargs: pytest.fail("opening quote API sweep must not wait for optional prewarm"))
    monkeypatch.setattr(quotes, "load_symbol_yahoo_map", lambda *args: {})
    result = quotes.fetch_tw_mis_last_prices(["A"], np.array([99.0]), parquet_root=tmp_path, on_progress=lambda *args: None)
    assert result.available_count == 0
    assert not quotes.tw_session_open_observed_mask(result).any()
