from __future__ import annotations

import ast
from datetime import timedelta
import inspect
import multiprocessing as mp
import os
from pathlib import Path
import queue

import pytest
import torch

from stockagent.training import trainer


@pytest.mark.parametrize("exists", [False, True])
@pytest.mark.parametrize("restarting", [False, True])
def test_restart_archive_always_rendezvous_even_without_a_file(
    tmp_path, monkeypatch, exists, restarting,
):
    path = tmp_path / "checkpoint_best.pt"
    if exists:
        path.write_bytes(b"retained checkpoint evidence")
    phases = []
    monkeypatch.setattr(trainer, "_distributed_should_write", lambda: True)
    monkeypatch.setattr(
        trainer, "_raise_if_distributed_phase_failed",
        lambda phase, error: phases.append((phase, error)),
    )
    archived = trainer._archive_fold_best_checkpoint_for_restart(
        path, fold_id=2, restarting=restarting,
    )
    assert phases == [("archive_unresumable_fold_2_checkpoint", None)]
    if exists and restarting:
        assert archived is not None
        assert archived.read_bytes() == b"retained checkpoint evidence"
        assert not path.exists()
    else:
        assert archived is None
        assert path.exists() == exists


def test_delayed_worker_never_inspects_or_archives_the_mutable_file(monkeypatch):
    class ForbiddenWorkerPath:
        def exists(self):
            pytest.fail("worker must not decide collective order from file existence")

    phases = []
    monkeypatch.setattr(trainer, "_distributed_should_write", lambda: False)
    monkeypatch.setattr(
        trainer, "_raise_if_distributed_phase_failed",
        lambda phase, error: phases.append((phase, error)),
    )
    result = trainer._archive_fold_best_checkpoint_for_restart(
        ForbiddenWorkerPath(), fold_id=2, restarting=True,
    )
    assert result is None
    assert phases == [("archive_unresumable_fold_2_checkpoint", None)]


def test_trainer_calls_restart_rendezvous_outside_file_existence_branches():
    tree = ast.parse(inspect.getsource(trainer._run_training_impl))
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_archive_fold_best_checkpoint_for_restart"
    ]
    assert len(calls) == 1
    current = calls[0]
    while current in parents:
        current = parents[current]
        if isinstance(current, ast.If):
            assert "exists" not in ast.unparse(current.test)
            assert "restarting_fold" not in ast.unparse(current.test)


@pytest.mark.parametrize("remote", [
    None,
    {"rank": 1, "phase": "other_phase", "ok": True, "error": None},
    {"rank": 0, "phase": "expected", "ok": True, "error": None},
])
def test_phase_guard_rejects_missing_or_misaligned_status(monkeypatch, remote):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer, "_distributed_rank", lambda: 0)

    def gather(statuses, local):
        assert local["phase"] == "expected"
        statuses[:] = [local, remote]

    monkeypatch.setattr(trainer.dist, "all_gather_object", gather)
    with pytest.raises(RuntimeError, match="phase order mismatch.*expected"):
        trainer._raise_if_distributed_phase_failed("expected", None)


def _restart_worker(rank, init_file, checkpoint_path, archive_finished, results, scenario, backend):
    import torch.distributed as dist

    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    device = torch.device("cpu")
    if backend == "nccl":
        torch.cuda.set_device(rank)
        device = torch.device("cuda", rank)
    dist.init_process_group(
        backend, init_method=f"file://{init_file}", rank=rank,
        world_size=2, timeout=timedelta(seconds=10),
    )
    path = Path(checkpoint_path)
    original_archive = trainer._archive_restart_artifact

    def controlled_archive(path, *, reason):
        try:
            if scenario == "archive_error":
                raise PermissionError("synthetic archive denied")
            return original_archive(path, reason=reason)
        finally:
            # Force the worker to arrive only AFTER the writer has moved the
            # shared path. No scheduler luck or sleep is needed for this race.
            archive_finished.set()

    try:
        if scenario == "phase_mismatch":
            trainer._raise_if_distributed_phase_failed(f"rank_{rank}_phase", None)
        else:
            if rank == 0:
                trainer._archive_restart_artifact = controlled_archive
            else:
                if not archive_finished.wait(timeout=10):
                    raise RuntimeError("writer did not finish archive attempt")
                if scenario == "delayed_worker":
                    assert not path.exists()
            trainer._archive_fold_best_checkpoint_for_restart(
                path, fold_id=2, restarting=True,
            )
            # The following curve phase and tensor collective must still line
            # up, matching the reported failure's phase transition.
            trainer._raise_if_distributed_phase_failed("resume_epoch_curve_trim", None)
            value = torch.tensor([float(rank + 1)], device=device)
            dist.all_reduce(value)
            assert value.item() == 3.0
        results.put((rank, "ok", ""))
    except Exception as exc:
        results.put((rank, type(exc).__name__, str(exc)))
    finally:
        trainer._archive_restart_artifact = original_archive
        dist.destroy_process_group()


@pytest.mark.skipif(
    not torch.distributed.is_available() or not torch.distributed.is_gloo_available(),
    reason="Gloo backend unavailable",
)
@pytest.mark.parametrize("scenario", ["delayed_worker", "archive_error", "phase_mismatch"])
@pytest.mark.parametrize("backend", [
    "gloo",
    pytest.param("nccl", marks=pytest.mark.skipif(
        os.environ.get("STOCKAGENT_TEST_NCCL") != "1",
        reason="NCCL communication probe requires explicit STOCKAGENT_TEST_NCCL=1",
    )),
])
def test_real_two_process_restart_coordination(tmp_path, monkeypatch, scenario, backend):
    # Communication only: no train.py, model, optimizer, or strategy data. GPU
    # probes are opt-in so the ordinary regression suite does not occupy GPUs.
    if backend == "gloo":
        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    elif not torch.distributed.is_nccl_available() or torch.cuda.device_count() < 2:
        pytest.skip("NCCL probe requires two visible CUDA GPUs")
    path = tmp_path / "checkpoint_best.pt"
    path.write_bytes(b"original checkpoint evidence")
    ctx = mp.get_context("spawn")
    archive_finished, results = ctx.Event(), ctx.Queue()
    processes = [
        ctx.Process(
            target=_restart_worker,
            args=(rank, str(tmp_path / "distributed_init"), str(path), archive_finished, results, scenario, backend),
        ) for rank in range(2)
    ]
    observed = []
    try:
        for process in processes:
            process.start()
        observed = [results.get(timeout=20) for _ in processes]
        for process in processes:
            process.join(timeout=5)
        assert all(process.exitcode == 0 for process in processes)
    except queue.Empty as exc:
        raise AssertionError("restart ranks did not finish within the bounded test") from exc
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        results.close()
        results.join_thread()
    assert {rank for rank, _, _ in observed} == {0, 1}
    if scenario == "delayed_worker":
        assert all(kind == "ok" for _, kind, _ in observed), observed
        archived = list(tmp_path.glob("checkpoint_best.unresumable_without_optimizer_state.*.pt"))
        assert len(archived) == 1
        assert archived[0].read_bytes() == b"original checkpoint evidence"
        assert not path.exists()
    else:
        expected = "synthetic archive denied" if scenario == "archive_error" else "phase order mismatch"
        assert all(kind == "RuntimeError" and expected in message for _, kind, message in observed), observed
        assert path.read_bytes() == b"original checkpoint evidence"
