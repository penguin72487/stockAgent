"""Pool setup is scoped, early, observable and never a global import side effect."""
import os

import pytest

from stockagent.data.source_processing_runtime import THREAD_ENV_KEYS, configure_cpu_source_runtime


def test_explicit_pool_settings_and_cpu_only_source(monkeypatch):
    with monkeypatch.context() as patch:
        for key in (*THREAD_ENV_KEYS, "CUDA_VISIBLE_DEVICES"):
            patch.setenv(key, "old caller setting")
        patch.setattr(os, "sched_getaffinity", lambda _pid: {0, 1, 2, 3})
        profile = configure_cpu_source_runtime(2)
        assert all(os.environ[key] == "2" for key in THREAD_ENV_KEYS)
        assert os.environ["CUDA_VISIBLE_DEVICES"] == ""
        assert profile["effective_threads"] == 2 and profile["process_cpu_quota"] is False


def test_single_visible_cpu_clamps_requested_pool(monkeypatch):
    for key in (*THREAD_ENV_KEYS, "CUDA_VISIBLE_DEVICES"):
        monkeypatch.setenv(key, "old")
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {0})
    assert configure_cpu_source_runtime(2)["effective_threads"] == 1
    assert all(os.environ[key] == "1" for key in THREAD_ENV_KEYS)


@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "2"])
def test_invalid_setting_cannot_change_the_callers_environment(monkeypatch, invalid):
    monkeypatch.setenv("POLARS_MAX_THREADS", "unchanged")
    before = dict(os.environ)
    with pytest.raises(ValueError, match="positive integer"):
        configure_cpu_source_runtime(invalid)
    assert dict(os.environ) == before


def test_task_specific_override(monkeypatch):
    for key in (*THREAD_ENV_KEYS, "CUDA_VISIBLE_DEVICES"):
        monkeypatch.setenv(key, "old")
    monkeypatch.setenv("STOCKAGENT_FACTORIZED_PREP_THREADS", "3")
    monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {0, 1, 2, 3})
    assert configure_cpu_source_runtime()["effective_threads"] == 3
