"""Concurrent opening inputs retain exact source/panel and recovery semantics."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
import json
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


def test_cache_namespaces_and_capacities_do_not_evict_each_other():
    for cache in (engine._LIVE_PANEL_CACHE, engine._LIVE_FACTORIZED_PANEL_CACHE,
                  engine._LIVE_ALIGNED_PANEL_CACHE):
        value = object()
        result = engine._shared_live_panel_build(
            "same", lambda _: (value, False, "built"), cache=cache, max_entries=1,
        )
        assert result[0] is value
        assert cache["same"] is value
    assert len({id(cache["same"]) for cache in (
        engine._LIVE_PANEL_CACHE, engine._LIVE_FACTORIZED_PANEL_CACHE,
        engine._LIVE_ALIGNED_PANEL_CACHE,
    )}) == 3


def test_alignment_warmup_and_prefetch_share_one_immutable_result(monkeypatch):
    started, release, joined = threading.Event(), threading.Event(), threading.Event()
    source, aligned, calls = object(), object(), []
    monkeypatch.setattr(engine, "checkpoint_manifest_symbols", lambda _: ["2330"])

    def align(*args, **kwargs):
        calls.append(1)
        started.set()
        assert release.wait(3)
        return aligned

    monkeypatch.setattr(engine, "align_panel_to_checkpoint_universe", align)
    kwargs = dict(checkpoint_key="exact", checkpoint_payload={}, state_dict={},
                  fold_dir=Path("fold_11"), context="same generation")
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine._cached_aligned_panel, source, **kwargs)
        assert started.wait(3)
        second = pool.submit(engine._cached_aligned_panel, source,
                             progress_callback=lambda *_: joined.set(), **kwargs)
        assert joined.wait(3)
        release.set()
        assert first.result(3)[0] is aligned
        assert second.result(3)[0] is aligned
    assert calls == [1]
    assert engine._cached_aligned_panel(source, **kwargs) == (aligned, True)


def factorized_example(tmp_path):
    from test_factorized_panel import example
    panel, path, dense = example(tmp_path)
    manifest = json.loads(path.read_bytes())
    manifest["paper_inference"] = {"contract": "factorized_paper_projection_lag1_v1",
                                   "status": "verified", "raw_source_publication": False}
    path.write_text(json.dumps(manifest))
    config = SimpleNamespace(data=SimpleNamespace(
        factorized_feature_manifest=str(path), factorized_host_cache_bytes=4096,
    ))
    return panel, path, dense, config


def test_factorized_warmup_and_prefetch_share_verified_blocks(tmp_path, monkeypatch):
    from stockagent.data import factorized_panel as source
    panel, path, dense, config = factorized_example(tmp_path)
    started, release, joined = threading.Event(), threading.Event(), threading.Event()
    original, calls, progress = source.attach_factorized_live_features, [], []

    def attach(*args, **kwargs):
        calls.append(1)
        started.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    monkeypatch.setattr(source, "attach_factorized_live_features", attach)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine._cached_factorized_live_panel, panel, config,
                            lambda *event: progress.append(event))
        assert started.wait(3)
        second = pool.submit(engine._cached_factorized_live_panel, panel, config,
                             lambda *_: joined.set())
        assert joined.wait(3)
        release.set()
        a, b = first.result(3), second.result(3)
    assert a is b
    assert calls == [1]
    assert a.features.source._cache is b.features.source._cache
    assert a.features[:].tobytes() == dense.tobytes()
    assert any(phase == "factorized_member_verify" and done == total and total > 0
               for phase, done, total in progress)
    assert engine._cached_factorized_live_panel(panel, config) is a
    assert calls == [1]


def test_changed_member_invalidates_verified_attachment(tmp_path):
    panel, path, _, config = factorized_example(tmp_path)
    engine._cached_factorized_live_panel(panel, config)
    manifest = json.loads(path.read_bytes())
    member = path.parent / manifest["common"]["path"]
    member.write_bytes(b"not the verified common member")
    with pytest.raises(ValueError, match="member identity mismatch"):
        engine._cached_factorized_live_panel(panel, config)


def test_changed_member_during_attachment_cannot_enter_cache(tmp_path, monkeypatch):
    import os
    from stockagent.data import factorized_panel as source
    panel, path, _, config = factorized_example(tmp_path)
    original = source.attach_factorized_live_features

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        member = path.parent / json.loads(path.read_bytes())["common"]["path"]
        before = member.stat()
        os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns + 1000))
        return result

    monkeypatch.setattr(source, "attach_factorized_live_features", changed)
    with pytest.raises(RuntimeError, match="members changed during live attachment"):
        engine._cached_factorized_live_panel(panel, config)
    assert not engine._LIVE_FACTORIZED_PANEL_CACHE
    monkeypatch.setattr(source, "attach_factorized_live_features", original)
    assert engine._cached_factorized_live_panel(panel, config).num_symbols == 3


@pytest.mark.parametrize("cache_name", ["_LIVE_ALIGNED_PANEL_CACHE", "_LIVE_FACTORIZED_PANEL_CACHE"])
def test_shared_stage_eviction_fences_late_cache_commit(cache_name):
    started, release = threading.Event(), threading.Event()
    cache = getattr(engine, cache_name)

    def build(_):
        started.set()
        assert release.wait(3)
        return "old", False, "built"

    with ThreadPoolExecutor(max_workers=1) as pool:
        job = pool.submit(engine._shared_live_panel_build, "source", build, cache=cache)
        assert started.wait(3)
        engine.clear_live_panel_memory_cache()
        result = engine._shared_live_panel_build(
            "source", lambda _: ("current", False, "built"), cache=cache,
        )
        assert result[0] == "current"
        release.set()
        assert job.result(3)[0] == "old"
    assert cache["source"] == "current"
