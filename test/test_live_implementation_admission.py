from __future__ import annotations

from functools import lru_cache
import json
from types import SimpleNamespace

import pytest

from stockagent.live import completed_inference_cache as cache
from stockagent.live import signal_engine


@pytest.fixture
def code_tree(monkeypatch):
    current = [{"path": "/admitted/model.py", "generation": [1, 2, 3, 4, 5],
                "sha256": "original"}]
    monkeypatch.setattr(cache, "implementation_identity", lambda: current)
    monkeypatch.setattr(cache, "_admitted_implementation_json", lru_cache(maxsize=1)(
        lambda: json.dumps(cache.implementation_identity(), sort_keys=True, allow_nan=False)))
    return current


def test_stable_admission_keeps_existing_cache_code_identity(code_tree):
    assert cache.loaded_implementation_identity() == cache.implementation_identity()
    assert cache.loaded_implementation_identity() == code_tree


def test_identity_consumers_cannot_mutate_process_admission(code_tree):
    returned = cache.loaded_implementation_identity()
    returned[0]["generation"][0] = 999
    returned[0]["sha256"] = "caller mutation"
    assert cache.loaded_implementation_identity() == code_tree


@pytest.mark.parametrize("change", ["generation", "hash", "added", "deleted"])
def test_changed_code_cannot_be_blessed_as_a_resident_model(code_tree, change):
    cache.loaded_implementation_identity()
    if change == "generation":
        code_tree[0]["generation"][-1] += 1
    elif change == "hash":
        code_tree[0]["sha256"] = "new disk code"
    elif change == "added":
        code_tree.append({"path": "/admitted/new_model.py", "generation": [1], "sha256": "new"})
    else:
        code_tree.clear()
    for _ in range(2):
        with pytest.raises(cache.LoadedImplementationChanged) as caught:
            cache.loaded_implementation_identity()
        assert caught.value.details["requires_fresh_process"] is True
        assert caught.value.details["retryable"] is False


@pytest.mark.parametrize("resident", [False, True])
def test_model_cache_hit_and_miss_refuse_changed_code_before_model_or_cuda(
    code_tree, monkeypatch, resident,
):
    cache.loaded_implementation_identity()
    code_tree[0]["sha256"] = "changed"
    monkeypatch.setattr(signal_engine, "_model_cache_key", lambda **kwargs: "fixed")
    monkeypatch.setattr(signal_engine, "_LIVE_MODEL_CACHE", {"fixed": object()} if resident else {})
    monkeypatch.setattr(signal_engine, "build_model", lambda **kwargs: pytest.fail("model built"))
    with pytest.raises(cache.LoadedImplementationChanged):
        signal_engine._cached_live_model(
            config=SimpleNamespace(), checkpoint_key="checkpoint", state_dict={},
            panel=SimpleNamespace(), runtime_device=SimpleNamespace(),
        )


def test_signal_refuses_changed_code_before_source_or_config_load(code_tree, monkeypatch):
    cache.loaded_implementation_identity()
    code_tree[0]["sha256"] = "changed"
    monkeypatch.setattr(signal_engine, "load_config", lambda *args: pytest.fail("config read"))
    with pytest.raises(cache.LoadedImplementationChanged):
        signal_engine.generate_live_signal(config_path="not-read.yaml")
