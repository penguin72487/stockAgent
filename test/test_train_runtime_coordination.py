from __future__ import annotations

from datetime import timedelta
import json
import multiprocessing as mp
from pathlib import Path
import queue
import random
from types import SimpleNamespace

import numpy as np
import pytest
import torch

import train as train_entry


def test_startup_timing_buffers_until_output_path_is_known(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("STOCKAGENT_ROOT_LAUNCH_MONOTONIC_NS", raising=False)
    monkeypatch.delenv("STOCKAGENT_RUN_ID", raising=False)
    monkeypatch.setenv("RANK", "0")
    monkeypatch.setenv("WORLD_SIZE", "2")
    recorder = train_entry._StartupTimingRecorder()
    recorder.checkpoint("config", config="tw_public")
    path = tmp_path / "startup_timing.jsonl"

    recorder.bind(path)
    recorder.checkpoint("panel", rows=123)

    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [record["stage"] for record in records] == ["config", "panel"]
    assert records[0]["world_size"] == 2
    assert records[1]["rows"] == 123
    assert records[1]["cumulative_s"] >= records[0]["cumulative_s"]


def test_auto_multi_gpu_strategy_tracks_visible_device_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 1)
    assert train_entry._resolve_multi_gpu_strategy("auto") == "none"
    monkeypatch.setattr(torch.cuda, "device_count", lambda: 4)
    assert train_entry._resolve_multi_gpu_strategy("auto") == "distributed_data_parallel"
    assert train_entry._resolve_multi_gpu_strategy("none") == "none"


def test_process_thread_budget_prefers_config_then_inherited_then_affinity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(train_entry, "_available_cpu_count", lambda: 20)

    assert train_entry._resolve_process_thread_count(
        24,
        inherited_names=("OMP_NUM_THREADS",),
        local_world_size=4,
        environ={"OMP_NUM_THREADS": "9"},
    ) == 6
    assert train_entry._resolve_process_thread_count(
        None,
        inherited_names=("OMP_NUM_THREADS",),
        local_world_size=4,
        environ={"OMP_NUM_THREADS": "7"},
    ) == 7
    assert train_entry._resolve_process_thread_count(
        None,
        inherited_names=("OMP_NUM_THREADS",),
        local_world_size=4,
        environ={},
    ) == 5
    assert train_entry._resolve_process_thread_count(
        None,
        inherited_names=("TORCHINDUCTOR_COMPILE_THREADS",),
        local_world_size=4,
        environ={},
        fallback=3,
    ) == 3


def test_local_world_size_uses_local_not_global_rank_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORLD_SIZE", "16")
    monkeypatch.setenv("LOCAL_WORLD_SIZE", "4")
    assert train_entry._local_world_size("distributed_data_parallel") == 4
    assert train_entry._local_world_size("none") == 1


def test_rank_local_seed_is_reproducible_and_decorrelated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)

    def sample(rank: int) -> tuple[float, np.ndarray, torch.Tensor]:
        train_entry._set_global_seed(train_entry._rank_seed(901, rank))
        return random.random(), np.random.random(4), torch.rand(4)

    rank0 = sample(0)
    rank1 = sample(1)
    rank1_repeat = sample(1)

    assert rank0[0] != rank1[0]
    assert not np.array_equal(rank0[1], rank1[1])
    assert not torch.equal(rank0[2], rank1[2])
    assert rank1[0] == rank1_repeat[0]
    np.testing.assert_array_equal(rank1[1], rank1_repeat[1])
    assert torch.equal(rank1[2], rank1_repeat[2])


def test_panel_rank0_failure_is_reported_before_worker_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(train_entry, "_build_panel_kwargs", lambda config: {})
    monkeypatch.setattr(train_entry, "_distributed_ready", lambda: True)
    monkeypatch.setattr(train_entry, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(train_entry, "_distributed_rank", lambda: 1)

    def fake_all_gather(statuses, local_status) -> None:
        statuses[0] = {
            "rank": 0,
            "phase": "rank0_build",
            "ok": False,
            "error": "RuntimeError: rank0 exploded",
        }
        statuses[1] = dict(local_status)

    monkeypatch.setattr(torch.distributed, "all_gather_object", fake_all_gather)
    build_calls = 0

    def build_panel(*args, **kwargs):
        nonlocal build_calls
        build_calls += 1
        return object()

    config = SimpleNamespace(data=SimpleNamespace(parquet_root="unused"))
    with pytest.raises(RuntimeError, match="rank0 exploded"):
        train_entry._build_panel_rank_coordinated(
            build_panel,
            config,
            "distributed_data_parallel",
        )
    assert build_calls == 0


def test_panel_worker_failure_is_reported_to_rank0_without_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(train_entry, "_build_panel_kwargs", lambda config: {})
    monkeypatch.setattr(train_entry, "_distributed_ready", lambda: True)
    monkeypatch.setattr(train_entry, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(train_entry, "_distributed_rank", lambda: 0)
    phases: list[str] = []

    def fake_all_gather(statuses, local_status) -> None:
        phase = str(local_status["phase"])
        phases.append(phase)
        statuses[0] = dict(local_status)
        statuses[1] = (
            {"rank": 1, "phase": phase, "ok": True, "error": None}
            if phase == "rank0_build"
            else {
                "rank": 1,
                "phase": phase,
                "ok": False,
                "error": "OSError: cache read failed",
            }
        )

    monkeypatch.setattr(torch.distributed, "all_gather_object", fake_all_gather)
    config = SimpleNamespace(data=SimpleNamespace(parquet_root="unused"))
    with pytest.raises(RuntimeError, match="cache read failed"):
        train_entry._build_panel_rank_coordinated(
            lambda *args, **kwargs: object(),
            config,
            "distributed_data_parallel",
        )
    assert phases == ["rank0_build", "worker_cache_load"]


def _gloo_rank0_panel_failure_worker(
    rank: int,
    world_size: int,
    init_file: str,
    result_queue: mp.Queue,
) -> None:
    import torch.distributed as dist
    import train as child_train

    dist.init_process_group(
        "gloo",
        init_method=f"file://{init_file}",
        rank=rank,
        world_size=world_size,
        timeout=timedelta(seconds=15),
    )
    child_train._build_panel_kwargs = lambda config: {}
    config = SimpleNamespace(data=SimpleNamespace(parquet_root="unused"))

    def build_panel(*args, **kwargs):
        if rank == 0:
            raise RuntimeError("intentional rank0 panel failure")
        return object()

    try:
        child_train._build_panel_rank_coordinated(
            build_panel,
            config,
            "distributed_data_parallel",
        )
    except Exception as exc:
        result_queue.put((rank, type(exc).__name__, str(exc)))
    else:
        result_queue.put((rank, "no_error", ""))
    finally:
        dist.destroy_process_group()


@pytest.mark.skipif(
    not torch.distributed.is_available() or not torch.distributed.is_gloo_available(),
    reason="Gloo distributed backend is unavailable",
)
def test_gloo_panel_rank0_failure_terminates_all_ranks_without_deadlock(
    tmp_path: Path,
) -> None:
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    init_file = str(tmp_path / "gloo_panel_init")
    processes = [
        ctx.Process(
            target=_gloo_rank0_panel_failure_worker,
            args=(rank, 2, init_file, result_queue),
        )
        for rank in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=25)

    try:
        results = [result_queue.get(timeout=5) for _ in range(2)]
    except queue.Empty as exc:
        raise AssertionError("distributed panel workers did not report completion") from exc
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

    assert all(not process.is_alive() for process in processes)
    assert all(process.exitcode == 0 for process in processes)
    assert {rank for rank, _, _ in results} == {0, 1}
    assert all(kind == "RuntimeError" for _, kind, _ in results)
    assert all("intentional rank0 panel failure" in message for _, _, message in results)
