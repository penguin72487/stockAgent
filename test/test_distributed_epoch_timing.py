from datetime import timedelta

import torch
import pytest

from stockagent.training import trainer


@pytest.mark.parametrize("src", [0, 1])
@pytest.mark.parametrize("diagnostics", [{}, {"loss_sharpe": -1.25, "vjp_passes": 4.0}])
def test_eval_timing_broadcast_preserves_scalars_and_structured_diagnostics(
    monkeypatch, src, diagnostics
):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)
    rank = {"value": src}
    monkeypatch.setattr(trainer, "_distributed_rank", lambda: rank["value"])
    sent = {}

    def broadcast(payload, *, src):
        if rank["value"] == src:
            sent["scalars"] = payload.clone()
        else:
            payload.copy_(sent["scalars"])

    def broadcast_objects(payload, *, src, device):
        assert device == torch.device("cpu")
        if rank["value"] == src:
            sent["diagnostics"] = dict(payload[0])
        else:
            payload[0] = dict(sent["diagnostics"])

    monkeypatch.setattr(trainer.dist, "broadcast", broadcast)
    monkeypatch.setattr(trainer.dist, "broadcast_object_list", broadcast_objects)
    source = trainer.TimingBreakdown(
        total_s=1.25,
        resident_input_cache_hits=7,
        resident_input_cache_bytes=24 * 1024**3,
        deferred_validation_checks=11,
        batches=5,
        gradient_norm_first_zero_batch=-1,
        multiobjective_diagnostics=dict(diagnostics),
    )
    # Process-local CUDA event handles must never enter a collective payload.
    source.cuda_events.append(object())
    result, elapsed, loss = trainer._broadcast_epoch_eval_timing(
        source, elapsed_s=2.5, loss_s=0.125, src=src, device=torch.device("cpu")
    )
    rank["value"] = 1 - src
    received, remote_elapsed, remote_loss = trainer._broadcast_epoch_eval_timing(
        trainer.TimingBreakdown(), elapsed_s=0.0, loss_s=0.0,
        src=src, device=torch.device("cpu"),
    )
    assert (elapsed, loss) == (remote_elapsed, remote_loss) == (2.5, 0.125)
    for name in source.__dataclass_fields__:
        if name != "cuda_events":
            assert getattr(result, name) == getattr(received, name) == getattr(source, name)
            assert type(getattr(received, name)) is type(getattr(source, name))
    assert result.cuda_events == received.cuda_events == []
    assert len(source.cuda_events) == 1
    assert ("diagnostics" in sent) == bool(diagnostics)
    assert received.multiobjective_diagnostics is not source.multiobjective_diagnostics


def test_eval_timing_single_process_keeps_structured_metadata(monkeypatch):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: False)
    timing = trainer.TimingBreakdown(multiobjective_diagnostics={"loss_mdd": 0.1})
    resolved, elapsed, loss = trainer._broadcast_epoch_eval_timing(
        timing, elapsed_s=2, loss_s=1, src=0, device=torch.device("cpu")
    )
    assert resolved is timing
    assert (elapsed, loss) == (2.0, 1.0)


def _eval_timing_nccl_worker(rank, rendezvous):
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group(
        "nccl", rank=rank, world_size=2,
        init_method=rendezvous, timeout=timedelta(seconds=60),
    )
    try:
        for src in (0, 1):
            for diagnostics in ({}, {"loss_sharpe": -1.25, "vjp_passes": 4.0}):
                timing = trainer.TimingBreakdown(
                    total_s=1.25 if rank == src else 0.0,
                    resident_input_cache_bytes=24 * 1024**3 if rank == src else 0,
                    deferred_validation_checks=11 if rank == src else 0,
                    batches=5 if rank == src else 0,
                    multiobjective_diagnostics=dict(diagnostics) if rank == src else {},
                )
                timing.cuda_events.append(object())
                resolved, elapsed, loss = trainer._broadcast_epoch_eval_timing(
                    timing, elapsed_s=2.5 if rank == src else 0.0,
                    loss_s=0.125 if rank == src else 0.0,
                    src=src, device=torch.device("cuda", rank),
                )
                assert (elapsed, loss) == (2.5, 0.125)
                assert resolved.total_s == 1.25
                assert resolved.resident_input_cache_bytes == 24 * 1024**3
                assert type(resolved.resident_input_cache_bytes) is int
                assert resolved.deferred_validation_checks == 11
                assert resolved.batches == 5
                assert resolved.gradient_norm_first_zero_batch == -1
                assert resolved.multiobjective_diagnostics == diagnostics
                assert resolved.cuda_events == []
                assert len(timing.cuda_events) == 1
        torch.distributed.barrier()
    finally:
        torch.distributed.destroy_process_group()


@pytest.mark.skipif(torch.cuda.device_count() < 2, reason="requires two CUDA devices")
def test_eval_timing_roundtrip_on_two_cuda_ranks(tmp_path):
    torch.multiprocessing.spawn(
        _eval_timing_nccl_worker,
        args=((tmp_path / "eval-timing-nccl").as_uri(),),
        nprocs=2,
        join=True,
    )


def test_epoch_timing_single_process(monkeypatch):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: False)
    assert trainer._max_rank_epoch_timing(
        epoch_wall_s=3.0, train_total_s=2.0, device=torch.device("cpu")
    ) == {
        "epoch_wall_s_max_rank": 3.0,
        "train_total_s_max_rank": 2.0,
        "timing_rank_count": 1,
    }


def test_epoch_timing_reduces_maximum_not_mean(monkeypatch):
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)

    def remote_max(payload, *, op):
        assert op == trainer.dist.ReduceOp.MAX
        payload.copy_(torch.maximum(payload, payload.new_tensor([4.0, 1.0])))

    monkeypatch.setattr(trainer.dist, "all_reduce", remote_max)
    assert trainer._max_rank_epoch_timing(
        epoch_wall_s=3.0, train_total_s=2.0, device=torch.device("cpu")
    ) == {
        "epoch_wall_s_max_rank": 4.0,
        "train_total_s_max_rank": 2.0,
        "timing_rank_count": 2,
    }


def test_allocator_telemetry_is_opt_in_and_never_initializes_cuda(monkeypatch):
    monkeypatch.delenv("STOCKAGENT_CUDA_ALLOCATOR_TELEMETRY", raising=False)
    def forbidden():
        raise AssertionError("default reporting touched CUDA")
    monkeypatch.setattr(torch.cuda, "is_initialized", forbidden)
    assert trainer._cuda_allocator_curve_payload() == {}
    monkeypatch.setenv("STOCKAGENT_CUDA_ALLOCATOR_TELEMETRY", "1")
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: False)
    assert trainer._cuda_allocator_curve_payload() == {}


def test_allocator_telemetry_keeps_live_reserved_and_peak_separate(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_CUDA_ALLOCATOR_TELEMETRY", "1")
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 1)
    monkeypatch.setattr(trainer.dist, "is_initialized", lambda: False)
    monkeypatch.setattr(torch.cuda, "memory_stats", lambda device: {
        "allocated_bytes.all.current": 5,
        "reserved_bytes.all.current": 11,
        "allocated_bytes.all.peak": 7,
        "reserved_bytes.all.peak": 13,
        "num_alloc_retries": 2,
    })
    payload = trainer._cuda_allocator_curve_payload()
    assert payload["cuda_allocator_device"] == 1
    assert payload["cuda_allocator_process_rank"] == 0
    assert payload["cuda_allocator_allocated_bytes"] == 5
    assert payload["cuda_allocator_reserved_bytes"] == 11
    assert payload["cuda_allocator_peak_allocated_bytes"] == 7
    assert payload["cuda_allocator_peak_reserved_bytes"] == 13
    assert payload["cuda_allocator_num_alloc_retries"] == 2


def test_allocator_telemetry_distinguishes_pinned_owned_and_active(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_CUDA_ALLOCATOR_TELEMETRY", "1")
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 0)
    monkeypatch.setattr(trainer.dist, "is_initialized", lambda: False)
    monkeypatch.setattr(torch.cuda, "memory_stats", lambda device: {})
    monkeypatch.setattr(torch.cuda.memory, "host_memory_stats", lambda: {
        "allocated_bytes.current": 11, "active_bytes.current": 5,
        "allocated_bytes.peak": 17, "active_bytes.peak": 9,
        "num_host_alloc": 4, "num_host_free": 1,
    })
    payload = trainer._cuda_allocator_curve_payload()
    assert payload["host_pinned_allocator_owned_bytes"] == 11
    assert payload["host_pinned_allocator_active_bytes"] == 5
    assert payload["host_pinned_allocator_peak_owned_bytes"] == 17
    assert payload["host_pinned_allocator_peak_active_bytes"] == 9
    assert payload["host_pinned_allocator_num_host_free"] == 1


def test_allocator_telemetry_accepts_unavailable_host_stats(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_CUDA_ALLOCATOR_TELEMETRY", "1")
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 0)
    monkeypatch.setattr(trainer.dist, "is_initialized", lambda: False)
    monkeypatch.setattr(torch.cuda, "memory_stats", lambda device: {})
    monkeypatch.setattr(torch.cuda.memory, "host_memory_stats", None)
    assert not any(k.startswith("host_pinned_allocator_")
                   for k in trainer._cuda_allocator_curve_payload())


def test_epoch_cache_trim_disabled_and_cpu_do_not_initialize_cuda(monkeypatch):
    monkeypatch.delenv("STOCKAGENT_CUDA_EPOCH_CACHE_TRIM_FRACTION", raising=False)
    def forbidden():
        raise AssertionError("trim touched uninitialized CUDA")
    monkeypatch.setattr(torch.cuda, "is_initialized", forbidden)
    assert trainer._trim_cuda_epoch_cache(torch.device('cuda')) == {}
    monkeypatch.setenv("STOCKAGENT_CUDA_EPOCH_CACHE_TRIM_FRACTION", '.8')
    assert trainer._trim_cuda_epoch_cache(torch.device('cpu')) == {}


@pytest.mark.parametrize('value', ['nan', 'inf', '-.1', '1'])
def test_epoch_cache_trim_rejects_invalid_threshold(monkeypatch, value):
    monkeypatch.setenv("STOCKAGENT_CUDA_EPOCH_CACHE_TRIM_FRACTION", value)
    with pytest.raises(ValueError, match='finite in'):
        trainer._trim_cuda_epoch_cache(torch.device('cuda'))


def test_epoch_cache_trim_only_reclaims_when_reserved_memory_exceeds_threshold(monkeypatch):
    gib = 1024**3
    monkeypatch.setenv("STOCKAGENT_CUDA_EPOCH_CACHE_TRIM_FRACTION", '.8')
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(torch.cuda, "get_device_properties", lambda d: type('P', (), {'total_memory': 32*gib})())
    state = {'reserved': 24*gib, 'calls': 0}
    monkeypatch.setattr(torch.cuda, "memory_reserved", lambda d: state['reserved'])
    monkeypatch.setattr(torch.cuda, "memory_allocated", lambda d: 20*gib)
    def empty():
        state['calls'] += 1
        state['reserved'] = 20*gib
    monkeypatch.setattr(torch.cuda, "empty_cache", empty)
    trainer._trim_cuda_epoch_cache(torch.device('cuda'))
    assert state['calls'] == 0
    state['reserved'] = 29*gib
    result = trainer._trim_cuda_epoch_cache(torch.device('cuda'))
    assert state['calls'] == 1 and result['cuda_epoch_cache_trim_reclaimed_bytes'] == 9*gib
