"""Concurrent opening inputs retain exact source/panel and recovery semantics."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import numpy as np
import pytest

from stockagent.live import signal_engine as engine


@pytest.fixture(autouse=True)
def empty_caches():
    engine.clear_live_panel_memory_cache()
    yield
    engine.clear_live_panel_memory_cache()
    assert not engine._LIVE_PANEL_BUILDS


def test_six_cold_requesters_build_once_and_share_exact_panel():
    started, release = threading.Event(), threading.Event()
    calls, progress = [], [[] for _ in range(6)]
    panel = np.array([[np.nan, -0.0, 1.234567890123]], dtype=np.float64)

    def build(report):
        calls.append(1)
        report("load", 250, 2760)
        started.set()
        assert release.wait(3)
        report("materialize", 2760, 2760)
        return panel, False, "rebuilt"

    with ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(engine._shared_live_panel_build, "same", build,
                            lambda *event, i=i: progress[i].append(event)) for i in range(6)]
        assert started.wait(3)
        deadline = time.monotonic() + 3
        while not all(progress) and time.monotonic() < deadline:
            time.sleep(0.005)
        assert all(progress)
        assert all(row == [("load", 250, 2760)] for row in progress)
        release.set()
        results = [job.result(3) for job in jobs]
    assert len(calls) == 1
    assert all(row[0] is panel for row in results)
    assert all(row[0].tobytes() == panel.tobytes() for row in results)
    assert results[0][1:] == (False, "rebuilt")
    assert sum(row[2] == "shared-rebuilt" for row in results) == 5
    assert engine._shared_live_panel_build("same", lambda _: pytest.fail("hot rebuild"))[1:] == (True, "memory")


@pytest.mark.parametrize("error", [OSError("read failure"), TimeoutError("source deadline")])
def test_failure_fans_out_without_poisoning_next_attempt(error):
    started, release = threading.Event(), threading.Event()

    def fail(report):
        report("load", 1, 2)
        started.set()
        assert release.wait(3)
        raise error

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine._shared_live_panel_build, "broken", fail)
        assert started.wait(3)
        joined = threading.Event()
        second = pool.submit(engine._shared_live_panel_build, "broken", fail,
                             lambda *_: joined.set())
        assert joined.wait(3)
        release.set()
        for job in (first, second):
            with pytest.raises(type(error), match=str(error)) as caught:
                job.result(3)
            assert caught.value is error
    assert "broken" not in engine._LIVE_PANEL_CACHE
    assert engine._shared_live_panel_build("broken", lambda _: ("recovered", False, "rebuilt"))[0] == "recovered"


def test_different_revisions_build_independently():
    barrier = threading.Barrier(2)

    def build(report):
        barrier.wait(3)  # Fails if a global build lock serializes unrelated modes.
        return object(), False, "rebuilt"

    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(engine._shared_live_panel_build, key, build) for key in ("a", "b")]
        a, b = [job.result(3)[0] for job in jobs]
    assert a is not b


def test_eviction_fences_inflight_result_and_new_callers():
    started, release = threading.Event(), threading.Event()

    def build(report):
        started.set()
        assert release.wait(3)
        return "old", False, "rebuilt"

    with ThreadPoolExecutor(max_workers=1) as pool:
        old = pool.submit(engine._shared_live_panel_build, "same", build)
        assert started.wait(3)
        engine.clear_live_panel_memory_cache()
        assert engine._shared_live_panel_build("same", lambda _: ("new", False, "rebuilt"))[0] == "new"
        release.set()
        assert old.result(3)[0] == "old"
    assert engine._LIVE_PANEL_CACHE["same"] == "new"


def test_monitor_failure_does_not_change_valid_result():
    def bad_monitor(*_):
        raise RuntimeError("observer unavailable")

    def build(report):
        report("load", 1, 1)
        return "valid", False, "rebuilt"

    assert engine._shared_live_panel_build("source", build, bad_monitor)[0] == "valid"
