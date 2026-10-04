"""Optional host/GPU caching must not alter inputs or diverge DDP control flow."""
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import time
from unittest.mock import patch
import weakref

import pytest
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from torch.nn.parallel import DistributedDataParallel

from stockagent.training import trainer
from stockagent.training.windowed import WindowedSplitTensors


def _split():
    features = torch.arange(12 * 4 * 3, dtype=torch.float32).reshape(12, 4, 3) / 100
    mask = torch.ones(12, 4, dtype=torch.bool)
    mask[3, 1] = False
    return WindowedSplitTensors(
        features=features, valid_indices=torch.arange(2, 12),
        future_log_returns=torch.arange(48, dtype=torch.float32).reshape(12, 4) / 10000,
        tradable_mask=mask, can_buy_mask=mask.clone(), can_sell_mask=mask.clone(),
        benchmark=torch.arange(12, dtype=torch.float32) / 1000,
        lookback=3, sample_mask=torch.tensor([True] * 9 + [False]),
        volume_notional=torch.full((12, 4), 1e6),
    )


def _cache(split, device=torch.device("cuda", 0), **kwargs):
    return trainer._cache_train_windowed_with_rank_consensus(
        name="cache regression", phase="cache_regression", split=split,
        device=device, enabled=kwargs.pop("enabled", True),
        target_fraction=.9, safety_margin_gb=0, **kwargs,
    )


def _assert_bytes_equal(a, b):
    assert a.dtype == b.dtype and a.shape == b.shape
    assert torch.equal(a.detach().cpu().contiguous().reshape(-1).view(torch.uint8),
                       b.detach().cpu().contiguous().reshape(-1).view(torch.uint8))


def _assert_batches_equal(actual, expected):
    assert actual.keys() == expected.keys()
    for key in actual:
        if isinstance(actual[key], torch.Tensor):
            _assert_bytes_equal(actual[key], expected[key])
        else:
            assert actual[key] == expected[key]


@pytest.mark.parametrize("resident_rank", [False, True])
def test_mixed_residency_returns_original_host_without_quantization(monkeypatch, resident_rank):
    host = _split()
    host.features[0, 0] = torch.tensor([float("nan"), -0., float("inf")])
    original = host.features.clone()
    reductions = iter([0, -10, 1])
    cached_refs = []

    def attempt(**kwargs):
        if not resident_rank:
            return host
        cached = replace(host, features=host.features.clone())
        cached_refs.append(weakref.ref(cached.features))
        return cached

    monkeypatch.setattr(trainer, "_maybe_cache_windowed_split_on_device", attempt)
    monkeypatch.setattr(trainer, "_distributed_min_int", lambda value, device: next(reductions))
    monkeypatch.setattr(trainer, "_tensor_on_requested_device", lambda t, d: t is not host.features)

    def release(device):
        assert all(ref() is None for ref in cached_refs), "rollback must release the discarded GPU copy"

    monkeypatch.setattr(trainer, "_release_cuda_memory", release)
    result = _cache(host)
    assert result is host
    _assert_bytes_equal(result.features, original)
    assert result.features.dtype == torch.float32
    assert result.sample_mask is host.sample_mask
    assert result.future_log_returns is host.future_log_returns
    assert result.tradable_mask is host.tradable_mask


def test_matching_residency_keeps_cache(monkeypatch):
    host = _split()
    cached = replace(host, features=host.features.clone())
    monkeypatch.setattr(trainer, "_maybe_cache_windowed_split_on_device", lambda **kw: cached)
    monkeypatch.setattr(trainer, "_tensor_on_requested_device", lambda t, d: True)
    assert _cache(host) is cached


def test_disabled_cache_preserves_original_split():
    host = _split()
    result = _cache(host, enabled=False)
    assert result.features is host.features
    assert result.future_log_returns is host.future_log_returns
    _assert_batches_equal(result.batch_by_rows(0, 4, torch.device("cpu"), False),
                          host.batch_by_rows(0, 4, torch.device("cpu"), False))


def test_explicit_amp_storage_cannot_silently_fallback(monkeypatch):
    host = _split()
    monkeypatch.setattr(trainer, "_maybe_cache_windowed_split_on_device", lambda **kw: host)
    with pytest.raises(RuntimeError, match="could not be honored"):
        _cache(host, feature_dtype=torch.bfloat16)


def test_dtype_disagreement_is_not_hidden_as_residency(monkeypatch):
    host = _split()
    reductions = iter([0, -1, 0])
    monkeypatch.setattr(trainer, "_maybe_cache_windowed_split_on_device", lambda **kw: host)
    monkeypatch.setattr(trainer, "_distributed_min_int", lambda value, device: next(reductions))
    with pytest.raises(RuntimeError, match="no identical original host"):
        _cache(host)


def test_unexpected_cache_error_is_not_silently_ignored(monkeypatch):
    def failed_attempt(**kwargs):
        raise ValueError("invalid execution tensor")

    monkeypatch.setattr(trainer, "_maybe_cache_windowed_split_on_device", failed_attempt)
    with pytest.raises(RuntimeError, match="invalid execution tensor"):
        _cache(_split())


def _ddp_cache_worker(rank, init_path, results_path):
    torch.set_num_threads(1)
    torch.manual_seed(123)
    torch.cuda.set_device(rank)
    device = torch.device("cuda", rank)
    dist.init_process_group("nccl", init_method=f"file://{init_path}",
                            rank=rank, world_size=2, timeout=timedelta(seconds=45))
    cases = []
    try:
        ddp = DistributedDataParallel(torch.nn.Linear(3, 1).to(device), device_ids=[rank])
        original_cache = trainer._maybe_cache_windowed_split_on_device
        for scenario in ("both_gpu", "rank0_budget", "rank1_oom", "both_gpu_again",
                         "strict_amp_budget", "rank1_error", "both_gpu_after_errors"):
            host = _split()

            def controlled_attempt(**kwargs):
                if scenario == "rank1_error" and rank == 1:
                    raise ValueError("synthetic invalid execution tensor")
                if scenario in {"rank0_budget", "strict_amp_budget"} and rank == 0:
                    # Exercise the real budget guard, without allocating all VRAM.
                    with patch.object(torch.cuda, "mem_get_info", return_value=(0, 32 * 1024**3)):
                        return original_cache(**kwargs)
                if scenario == "rank1_oom" and rank == 1:
                    original_to = torch.Tensor.to

                    def controlled_to(tensor, *args, **to_kwargs):
                        if tensor is host.features:
                            raise RuntimeError("CUDA out of memory: injected feature cache copy")
                        return original_to(tensor, *args, **to_kwargs)

                    with patch.object(torch.Tensor, "to", controlled_to):
                        return original_cache(**kwargs)
                return original_cache(**kwargs)

            with patch.object(trainer, "_maybe_cache_windowed_split_on_device", controlled_attempt):
                if scenario in {"strict_amp_budget", "rank1_error"}:
                    message = "could not be honored" if scenario == "strict_amp_budget" else "invalid execution tensor"
                    try:
                        _cache(host, device, feature_dtype=torch.bfloat16 if scenario == "strict_amp_budget" else None)
                    except RuntimeError as exc:
                        assert message in str(exc)
                    else:
                        raise AssertionError("every rank must reject an explicit storage/error contract failure")
                    cases.append({"scenario": scenario, "coordinated_error": True})
                    continue
                cached = _cache(host, device)
            expected_residency = "cpu" if scenario in {"rank0_budget", "rank1_oom"} else "cuda"
            assert cached.features.device.type == expected_residency
            assert cached.features.dtype == torch.float32
            actual = cached.batch_by_rows(0, 4, device, non_blocking=False)
            reference = host.batch_by_rows(0, 4, device, non_blocking=False)
            _assert_batches_equal(actual, reference)
            # Exercise the real DDP backward collective with identical model state.
            gradients = []
            outputs = []
            for batch in (actual, reference):
                ddp.zero_grad(set_to_none=True)
                output = ddp(batch["x"]).mean()
                output.square().backward()
                outputs.append(output.detach().clone())
                gradients.append([parameter.grad.detach().clone() for parameter in ddp.parameters()])
            _assert_bytes_equal(outputs[0], outputs[1])
            for actual_grad, reference_grad in zip(*gradients):
                _assert_bytes_equal(actual_grad, reference_grad)
            cases.append({"scenario": scenario, "features_device": expected_residency,
                          "feature_dtype": "float32", "batch_bytes_equal": True,
                          "ddp_forward_and_gradients_equal": True})
            del cached, actual, reference, gradients, outputs, output, batch
            trainer._release_cuda_memory(device)
        Path(results_path, f"rank{rank}.json").write_text(json.dumps(cases, indent=2))
        dist.barrier()
    finally:
        dist.destroy_process_group()


@pytest.mark.skipif(not torch.cuda.is_available() or torch.cuda.device_count() < 2,
                    reason="requires two CUDA devices and NCCL")
def test_two_gpu_multi_group_cache_consensus_and_exact_ddp_gradients(tmp_path):
    context = mp.spawn(_ddp_cache_worker, args=(str(tmp_path / "rendezvous"), str(tmp_path)),
                       nprocs=2, join=False)
    deadline = time.monotonic() + 150
    try:
        while not context.join(timeout=1):
            if time.monotonic() > deadline:
                pytest.fail("DDP cache regression did not complete its bounded collective workflow")
    finally:
        for process in context.processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
    results = [json.loads((tmp_path / f"rank{rank}.json").read_text()) for rank in range(2)]
    assert results[0] == results[1]
    assert len(results[0]) == 7
