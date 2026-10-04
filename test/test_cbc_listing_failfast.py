"""Invalid archive discovery must not drain the entire pending request queue."""

from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from downloader import download_tw_cbc_fx_release_archive as fx
from downloader import download_tw_cbc_money_release_archive as money


def listing(page, *, total_rows=181, total_pages=4, raw_rows=None, size=60):
    count = min(size, total_rows - (page - 1) * size) if raw_rows is None else raw_rows
    return (
        "".join(f'<li><time>2026-01-01</time><a href="/tw/cp-302-{page}-{i}.html">'
                '一般新聞</a></li>' for i in range(max(0, count)))
        + f'<div class="total">共{total_rows}筆資料，第{page}/{total_pages}頁</div>'
        + f'<select id="PageSize"><option value="{size}" selected>{size}</option></select>'
    ).encode()


@pytest.mark.parametrize("module", [fx, money])
@pytest.mark.parametrize("damage", ["short_first", "wrong_first", "wrong_size", "bad_total", "bad_count"])
def test_invalid_first_page_stops_before_pool_or_raw_publish(tmp_path, monkeypatch, module, damage):
    kwargs = {"total_rows": 60000, "total_pages": 1000}
    page = 1
    if damage == "short_first":
        kwargs["raw_rows"] = 1
    elif damage == "wrong_first":
        page = 2
    elif damage == "wrong_size":
        kwargs["size"] = 20
    elif damage == "bad_total":
        kwargs["total_rows"] = 59940
    else:
        kwargs["total_pages"] = 1001
    fetch = Mock(return_value=listing(page, **kwargs))
    save = Mock()
    pool = Mock(side_effect=AssertionError("invalid first page must never create a pool"))
    monkeypatch.setattr(module, "_fetch", fetch)
    monkeypatch.setattr(module, "_save_raw", save)
    monkeypatch.setattr(module, "ThreadPoolExecutor", pool)
    with pytest.raises(ValueError):
        module.collect(tmp_path, workers=1)
    assert fetch.call_count == 1
    save.assert_not_called()
    pool.assert_not_called()


def deferred_pool(monkeypatch, module):
    pools, tasks = [], {}

    def run(future):
        if future.done():
            return
        function, args, kwargs = tasks[future]
        try:
            future.set_result(function(*args, **kwargs))
        except Exception as error:
            future.set_exception(error)

    class Pool:
        def __init__(self, **kwargs):
            self.futures, self.shutdown_calls = [], []
            pools.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            # Real context-manager shutdown waits for any uncancelled queue.
            self.shutdown(wait=True)

        def submit(self, function, *args, **kwargs):
            future = Future()
            self.futures.append(future)
            tasks[future] = function, args, kwargs
            return future

        def shutdown(self, *, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))
            if cancel_futures:
                for future in self.futures:
                    future.cancel()
            if wait:
                for future in self.futures:
                    run(future)

    def completed(futures):
        for future in futures:
            run(future)
            if not future.cancelled():
                yield future

    monkeypatch.setattr(module, "ThreadPoolExecutor", Pool)
    monkeypatch.setattr(module, "as_completed", completed)
    return pools


@pytest.mark.parametrize("module", [fx, money])
@pytest.mark.parametrize("damage", [
    "short", "wrong_page", "changed_total", "transport", "source_block", "save_failure",
])
def test_later_listing_failure_cancels_queued_work_and_preserves_archive(
    tmp_path, monkeypatch, module, damage,
):
    pools = deferred_pool(monkeypatch, module)
    calls, saved_pages = [], []
    failure = (requests.ConnectionError("original transport failure") if damage == "transport"
               else module.SourceAccessBlocked("original source block") if damage == "source_block"
               else OSError("original raw save failure"))
    parquet = tmp_path / f"{module.OUTPUT_NAME}.parquet"
    parquet.write_bytes(b"unchanged prior archive; never decoded in invalid discovery")
    previous_bytes = parquet.read_bytes()

    def fetch(url, limiter):
        page = int(url.rsplit("-", 2)[1])
        calls.append(page)
        if page != 2:
            return listing(page)
        if damage in {"transport", "source_block"}:
            raise failure
        return listing(1 if damage == "wrong_page" else page,
                       total_rows=183 if damage == "changed_total" else 181,
                       raw_rows=1 if damage == "short" else None)

    def save(directory, prefix, body):
        page = int(prefix.rsplit("-", 1)[-1])
        if page == 2 and damage == "save_failure":
            raise failure
        saved_pages.append(page)
        return "a" * 64, str(directory / f"{prefix}.html")

    writer = Mock(side_effect=AssertionError("must never replace archive"))
    detail = Mock(side_effect=AssertionError("must never enter details"))
    monkeypatch.setattr(module, "_fetch", fetch)
    monkeypatch.setattr(module, "_save_raw", save)
    monkeypatch.setattr(module, "_collect_one", detail)
    monkeypatch.setattr(module, "write_release_rows_if_changed", writer)
    expected = ValueError if damage in {"short", "wrong_page", "changed_total"} else type(failure)
    with pytest.raises(expected) as caught:
        module.collect(tmp_path, workers=1)
    if expected is not ValueError:
        assert caught.value is failure
    assert calls == [1, 2]
    assert saved_pages == [1]
    assert pools[0].shutdown_calls == [(False, True), (True, False)]
    assert [future.cancelled() for future in pools[0].futures] == [False, True, True]
    assert parquet.read_bytes() == previous_bytes
    writer.assert_not_called()
    detail.assert_not_called()


@pytest.mark.parametrize("size", [20, 60])
def test_common_validator_retains_exact_full_and_last_page_layout(size):
    first = fx._parse_listing_page(listing(1, total_rows=size + 1, total_pages=2, size=size))
    last = fx._parse_listing_page(listing(2, total_rows=size + 1, total_pages=2, size=size))
    fx._validate_listing_page(first, 1, page_size=size)
    fx._validate_listing_page(last, 2, page_size=size, expected=first)
    with pytest.raises(ValueError, match="page identity"):
        fx._validate_listing_page(first, 2, page_size=size)


@pytest.mark.parametrize("module", [fx, money])
def test_progress_write_failure_also_cancels_remaining_discovery(tmp_path, monkeypatch, module):
    pools = deferred_pool(monkeypatch, module)
    requests_made = []
    failure = OSError("cannot persist discovery progress")

    def fetch(url, limiter):
        page = int(url.rsplit("-", 2)[1])
        requests_made.append(page)
        return listing(page, total_rows=2401, total_pages=41)

    def write_state(root, state):
        if state.get("completed_pages") == 20:
            raise failure

    monkeypatch.setattr(module, "_fetch", fetch)
    monkeypatch.setattr(module, "_write_state", write_state)
    monkeypatch.setattr(module, "_save_raw", lambda *args: ("a" * 64, "fixture raw"))
    with pytest.raises(OSError) as caught:
        module.collect(tmp_path, workers=1)
    assert caught.value is failure
    assert requests_made == list(range(1, 21))
    assert sum(future.cancelled() for future in pools[0].futures) == 21
    assert pools[0].shutdown_calls == [(False, True), (True, False)]


@pytest.mark.parametrize("module", [fx, money])
def test_partial_queue_submission_failure_cancels_already_pending_work(tmp_path, monkeypatch, module):
    pools = deferred_pool(monkeypatch, module)
    constructor = module.ThreadPoolExecutor
    failure = RuntimeError("cannot submit more work")

    def pool_factory(**kwargs):
        pool = constructor(**kwargs)
        submit = pool.submit

        def limited_submit(*args, **kwargs):
            if pool.futures:
                raise failure
            return submit(*args, **kwargs)

        pool.submit = limited_submit
        return pool

    fetch = Mock(return_value=listing(1))
    monkeypatch.setattr(module, "ThreadPoolExecutor", pool_factory)
    monkeypatch.setattr(module, "_fetch", fetch)
    with pytest.raises(RuntimeError) as caught:
        module.collect(tmp_path, workers=1)
    assert caught.value is failure
    assert fetch.call_count == 1
    assert len(pools[0].futures) == 1 and pools[0].futures[0].cancelled()
    assert pools[0].shutdown_calls == [(False, True), (True, False)]


@pytest.mark.parametrize("module", [fx, money])
def test_stage_timings_include_slow_parquet_proof(tmp_path, monkeypatch, module):
    title = "115年8月底外匯存底" if module is fx else "115年8月金融情況"
    body = (
        '<li><time>2026-09-01</time><a href="/tw/cp-302-1-ABC-1.html">'
        f'{title}</a></li><div class="total">共1筆資料，第1/1頁</div>'
        '<select id="PageSize"><option value="60" selected>60</option></select>'
    ).encode()
    monkeypatch.setattr(module, "_fetch", lambda *args: body)

    def detail(row, *args, **kwargs):
        if module is fx:
            return {**row, "metric": "fx_reserves_usd_100m", "value": 5001.0}
        return [{**row, "metric": name, "value_pct": 1.0}
                for name in ("m1b_yoy_pct", "m2_yoy_pct")]

    monkeypatch.setattr(module, "_collect_one", detail)
    clock = [0.0]

    def monotonic():
        clock[0] += 0.001
        return clock[0]

    original_write = module.write_release_rows_if_changed

    def slow_proof(*args, **kwargs):
        clock[0] += 2.0  # Inject elapsed time without sleeping or patching global time.
        return original_write(*args, **kwargs)

    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(module, "write_release_rows_if_changed", slow_proof)
    summary = module.collect(tmp_path, workers=1)
    assert summary["complete"] is True
    expected = {"listing_discovery", "detail_collection", "parquet_proof_write"}
    if module is money:
        expected.add("resume_verification")
    assert set(summary["stage_seconds"]) == expected
    assert summary["elapsed_scope"] == "through_parquet_proof_before_final_state_write"
    assert summary["elapsed_seconds"] >= summary["stage_seconds"]["parquet_proof_write"] >= 2
    assert all(0 <= duration <= summary["elapsed_seconds"] + 0.000001
               for duration in summary["stage_seconds"].values())
    assert Path(summary["parquet_path"]).is_file()
