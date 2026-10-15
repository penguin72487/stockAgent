"""Annual segment lengths share preparation without changing account scans."""
import os

import pytest
import torch

from stockagent.backtest import simulator


def _factory():
    return simulator._prepare_runner_factory(
        long_only=False, gross_budget=1.0, min_trade_weight=0.01,
        portfolio_activation="pre_normalized", side_masks_require_tradable=True,
    )


def _inputs(rows, device="cpu"):
    generator = torch.Generator(device=device).manual_seed(1040 + rows)
    weights = torch.randn(rows, 23, generator=generator, device=device) * 0.03
    masks = [torch.rand(rows, 23, generator=generator, device=device) > 0.15 for _ in range(3)]
    return weights, masks


def test_prepare_key_does_not_specialize_calendar_length(monkeypatch):
    def key(rows):
        weights, masks = _inputs(rows)
        return simulator._prepare_compile_key(weights, *masks, False, 1.0, 0.01,
                                               "pre_normalized", True)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE_DYNAMIC", "0")
    reference = key(128)
    assert all(key(rows) == reference for rows in (1, 2, 19, 43, 85, 245))
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE_DYNAMIC", "1")
    assert key(128) == reference  # this setting belongs to the recurrent scan


def _check_segments(compiled, device):
    for rows in (1, 2, 3, 8, 19, 43, 57, 64, 85, 116, 128, 245):
        weights, masks = _inputs(rows, device)
        eager_weights = weights.detach().clone().requires_grad_()
        compiled_weights = weights.detach().clone().requires_grad_()
        expected = _factory()(eager_weights, *masks)
        actual = compiled(compiled_weights, *masks)
        for target, value in zip(expected, actual):
            if target.dtype == torch.bool:
                assert torch.equal(target, value)
            else:
                torch.testing.assert_close(value, target, rtol=3e-6, atol=2e-7)
        expected[0].square().sum().backward()
        actual[0].square().sum().backward()
        torch.testing.assert_close(compiled_weights.grad, eager_weights.grad, rtol=3e-6, atol=2e-7)


def test_dynamic_prepare_more_than_eight_lengths_output_and_gradient():
    graphs = []
    def backend(graph, examples):
        graphs.append(graph)
        return graph.forward
    compiled = torch.compile(_factory(), backend=backend, fullgraph=True, dynamic=True)
    _check_segments(compiled, "cpu")
    warmed = len(graphs)
    _check_segments(compiled, "cpu")
    assert len(graphs) == warmed and warmed <= 2  # singleton is a separate ABI


def test_resolver_compiles_once_for_annual_rows_without_global_scan_mode(monkeypatch):
    from types import SimpleNamespace
    calls = []
    monkeypatch.setattr(simulator, "_compile_prep_enabled", lambda: True)
    monkeypatch.setattr(simulator, "_compile_enabled", lambda: True)
    monkeypatch.setattr(simulator, "_torch_dynamo_is_compiling", lambda: False)
    monkeypatch.setattr(simulator, "_configure_inductor_cudagraphs", lambda: None)
    monkeypatch.setattr(simulator, "_mark_static_shape", lambda *args: None)
    monkeypatch.setattr(simulator, "_PREP_COMPILED_CACHE", {})
    monkeypatch.setattr(simulator, "_PREP_COMPILE_FAILED", set())
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE_DYNAMIC", "0")
    def compile_runner(fn, **kwargs):
        calls.append(kwargs)
        return fn
    monkeypatch.setattr(torch, "compile", compile_runner)
    reference = None
    for rows in (128, 85, 43, 64, 19, 1):
        weights = SimpleNamespace(device=torch.device("cuda:0"), dtype=torch.float32,
                                  size=lambda index: (rows, 23)[index])
        masks = [torch.ones(rows,23,dtype=torch.bool)] * 3
        runner = simulator._resolve_prepare_runner(weights, *masks, long_only=False,
                    gross_budget=1.0, min_trade_weight=0.01,
                    portfolio_activation="pre_normalized", side_masks_require_tradable=True)
        if reference is None: reference = runner
        assert runner is reference
    assert len(calls) == 1 and calls[0]["fullgraph"] and calls[0]["dynamic"] is True
    assert simulator._compile_dynamic_enabled() is False


@pytest.mark.skipif(os.environ.get("STOCKAGENT_TEST_CUDA_PREP") != "1", reason="opt-in CUDA compiler proof")
def test_dynamic_prepare_cuda_segments_no_recompile_after_warmup():
    assert torch.cuda.is_available()
    from train import _configure_cpu_parallelism
    _configure_cpu_parallelism(cpu_threads=8, compile_threads=16, local_world_size=2)
    device = torch.device("cuda", int(os.environ.get("LOCAL_RANK", "0")))
    torch.cuda.set_device(device)
    from torch._dynamo.utils import counters
    compiled = torch.compile(_factory(), fullgraph=True, dynamic=True,
                             options={"triton.cudagraphs": False})
    _check_segments(compiled, device)
    warmed = counters["stats"]["unique_graphs"]
    _check_segments(compiled, device)
    assert counters["stats"]["unique_graphs"] == warmed
