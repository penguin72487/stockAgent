"""Storage/recomputation policy, not a different wide-panel model."""
from copy import deepcopy
import os
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.models.factorized_input import _use_encoder_checkpoint
from stockagent.training.checkpoint_contract import (
    _configuration_fingerprint_snapshot, _training_checkpoint_contract,
)


def _direct_model():
    return SimpleNamespace(
        candle_encoder=SimpleNamespace(feature_svd_components=0,
            causal_feature_window_rms_normalization=False),
        temporal_basis_feature_encoder=None,
        _input_basis_enabled=lambda: False,
        factorized_encoder_checkpoint=False,
        lookback=32, d_model=32,
    )


def test_portfolio_graph_scope_preserves_encoder_checkpoint_and_native_fingerprint():
    from stockagent.models.factorized_input import _partition_compile_options
    original = load_config(os.environ.get("STOCKAGENT_FRONTIER_TEST_BASELINE_CONFIG",
        "configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml"))
    selected = deepcopy(original)
    selected.training.financial_transformer.factorized_portfolio_cuda_graphs = True
    assert _configuration_fingerprint_snapshot(original) != _configuration_fingerprint_snapshot(selected)
    model = _direct_model()
    model.factorized_portfolio_cuda_graphs = True
    assert _partition_compile_options(model)["triton.cudagraphs"] is False
    assert _partition_compile_options(model, stage="head")["triton.cudagraphs"] is True
    assert original.training.factorized_encoder_checkpoint is True


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
@pytest.mark.filterwarnings("ignore:The CUDA Graph is empty.*:UserWarning:torch.cuda.graphs")
def test_portfolio_graph_multiple_forwards_preserve_outputs_and_full_gradients(monkeypatch):
    import torch._functorch.config as aot_config
    from stockagent.models.financial_transformer import FinancialTransformerModel
    from stockagent.models.factorized_input import _partition_compile_options
    monkeypatch.setattr(torch.backends.cuda.matmul, "allow_tf32", True)
    torch.manual_seed(198)
    baseline = FinancialTransformerModel(lookback=2, num_features=10, num_symbols=3, d_model=8,
        attention_mode="market_token", temporal_layers=1, temporal_heads=2,
        cross_heads=2, latent_layers=1, num_latent_factors=2, num_market_tokens=1,
        market_layers=1, head_hidden_dim=8, head_layers=1, dropout=0., input_dropout=0.,
        candle_dropout=0., temporal_pooling="last", temporal_query_mode="last_only",
        feature_bottleneck_dim=0, return_aux=False).cuda().train()
    candidate = deepcopy(baseline)
    candidate.factorized_portfolio_cuda_graphs = True
    compiled = torch.compile(candidate._forward_stock_embeddings, dynamic=False,
        options=_partition_compile_options(candidate, stage="head"))
    for _ in range(2):
        source = [torch.randn(2, 3, 8, device="cuda", requires_grad=True) for _ in range(2)]
        observed_inputs = [x.detach().clone().requires_grad_() for x in source]
        mask = torch.ones((2, 3), device="cuda", dtype=torch.bool)
        reference = [baseline._forward_stock_embeddings(x, mask) for x in source]
        with aot_config.patch(backward_pass_autocast="off"):
            observed = [compiled(x, mask).clone() for x in observed_inputs]
        coefficients = [torch.randn_like(x) for x in reference]
        for a, b in zip(observed, reference, strict=True):
            torch.testing.assert_close(a, b, rtol=1e-4, atol=1e-5)
        sum((a * b).sum() for a, b in zip(reference, coefficients, strict=True)).backward()
        sum((a * b).sum() for a, b in zip(observed, coefficients, strict=True)).backward()
        for a, b in zip(observed_inputs, source, strict=True):
            torch.testing.assert_close(a.grad, b.grad, rtol=1e-3, atol=1e-5)
        for (_, a), (_, b) in zip(candidate.named_parameters(), baseline.named_parameters(), strict=True):
            if b.grad is None:
                assert a.grad is None
            else:
                torch.testing.assert_close(a.grad, b.grad, rtol=1e-3, atol=1e-5)
        baseline.zero_grad(set_to_none=True)
        candidate.zero_grad(set_to_none=True)
    # PyTorch initializes its allocator pool with an intentionally empty
    # capture; the numerical test must also prove actual model graph nodes.
    from torch._inductor.cudagraph_trees import get_container
    manager = get_container(torch.cuda.current_device()).tree_manager
    assert manager is not None and sum(len(nodes) for nodes in manager.roots.values()) > 0
    from stockagent.training.trainer import _cuda_graph_capture_snapshot
    observed = _cuda_graph_capture_snapshot(SimpleNamespace(training=SimpleNamespace(
        financial_transformer=SimpleNamespace(factorized_portfolio_cuda_graphs=True))))
    assert observed["cuda_graph_runtime_inspection_ok"]
    assert observed["cuda_graph_root_count_local"] > 0


def test_activation_policy_preserves_formal_resume_contract():
    original = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v3.yaml")
    candidate = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_no_basis_saved_activations_v1.yaml")
    assert original.training.factorized_encoder_checkpoint is True
    assert candidate.training.factorized_encoder_checkpoint is False
    left = _configuration_fingerprint_snapshot(original)
    right = _configuration_fingerprint_snapshot(candidate)
    left.pop("runner"); right.pop("runner")
    assert left == right
    assert _training_checkpoint_contract(original) == _training_checkpoint_contract(candidate)


def test_selected_runtime_candidate_keeps_the_formal_training_assumptions():
    original = load_config("configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v3.yaml")
    candidate = load_config("configs/deployments/tw_day_trade_factorized_values_20261006_no_basis_runtime_optimized_v1.yaml")
    assert _training_checkpoint_contract(original) == _training_checkpoint_contract(candidate)
    assert candidate.trading == original.trading
    assert candidate.walk_forward == original.walk_forward
    assert candidate.runner.output_dir == original.runner.output_dir
    assert candidate.runner.resume and not candidate.runner.isolate_train_folds
    assert candidate.environment.cpu_threads == 8
    assert candidate.environment.amp_dtype == original.environment.amp_dtype == "bf16"
    assert candidate.environment.use_tensor_cores == original.environment.use_tensor_cores
    assert candidate.training.batch_size_train == original.training.batch_size_train == 32
    assert candidate.training.eval_backtest_chunk_rows == original.training.eval_backtest_chunk_rows == 32
    assert not candidate.training.factorized_encoder_checkpoint
    assert not candidate.training.day_trade_sparse_events
    assert candidate.training.financial_transformer == original.training.financial_transformer


def test_saved_activations_memory_guard_is_conservative(monkeypatch):
    model = _direct_model()
    slab = SimpleNamespace(shape=(47, 2757, 14726))
    expected = (2 * 47 * 2757 * 14726 * 4
                + 16 * 16 * 32 * 2757 * 32 * 4 + 1536 * 1024**2)
    monkeypatch.setattr(torch.cuda, "memory_reserved", lambda device: 0)
    monkeypatch.setattr(torch.cuda, "memory_allocated", lambda device: 0)
    monkeypatch.setattr(torch.cuda, "mem_get_info", lambda device: (expected - 1, 32 * 1024**3))
    assert _use_encoder_checkpoint(model, slab, torch.device("cuda"))
    monkeypatch.setattr(torch.cuda, "mem_get_info", lambda device: (expected, 32 * 1024**3))
    assert not _use_encoder_checkpoint(model, slab, torch.device("cuda"))
    monkeypatch.setattr(torch.cuda, "mem_get_info", lambda device: (expected - 1024, 32 * 1024**3))
    monkeypatch.setattr(torch.cuda, "memory_reserved", lambda device: 2048)
    monkeypatch.setattr(torch.cuda, "memory_allocated", lambda device: 1024)
    assert not _use_encoder_checkpoint(model, slab, torch.device("cuda"))


def test_explicit_retention_trial_is_not_silently_reverted(monkeypatch):
    model = _direct_model()
    model.factorized_encoder_force_retention = True
    slab = SimpleNamespace(shape=(159, 2757, 14726))
    monkeypatch.setattr(torch.cuda, "mem_get_info", lambda device: pytest.fail("explicit trial used auto guard"))
    assert not _use_encoder_checkpoint(model, slab, torch.device("cuda"))
    with pytest.raises(ValueError, match="no-basis CUDA"):
        _use_encoder_checkpoint(model, slab, torch.device("cpu"))


@pytest.mark.parametrize("unsupported", ["default", "cpu", "basis", "input_basis", "svd", "window_rms"])
def test_unsupported_policy_keeps_checkpoint_without_memory_query(monkeypatch, unsupported):
    model = _direct_model()
    device = torch.device("cpu" if unsupported == "cpu" else "cuda")
    if unsupported == "default":
        del model.factorized_encoder_checkpoint
    elif unsupported == "basis":
        model.temporal_basis_feature_encoder = object()
    elif unsupported == "input_basis":
        model._input_basis_enabled = lambda: True
    elif unsupported == "svd":
        model.candle_encoder.feature_svd_components = 32
    elif unsupported == "window_rms":
        model.candle_encoder.causal_feature_window_rms_normalization = True
    monkeypatch.setattr(torch.cuda, "mem_get_info", lambda device: pytest.fail("unsupported policy queried VRAM"))
    assert _use_encoder_checkpoint(model, SimpleNamespace(shape=(47, 2757, 14726)), device)


@pytest.mark.parametrize("packet_cache", [True, False])
def test_activation_retention_preserves_outputs_gradients_and_rng(tmp_path, monkeypatch, packet_cache):
    import stockagent.models.factorized_input as policy
    from stockagent.data.factorized_panel import (
        FactorizedPanelFeatures, FactorizedPanelSlab, write_array_block,
    )
    from stockagent.models.financial_transformer import FinancialTransformerModel

    rng = np.random.default_rng(5)
    base = rng.normal(size=(8, 3, 2)).astype(np.float32)
    individual = rng.normal(size=(8, 3, 6)).astype(np.float32)
    shared = rng.normal(size=(8, 2)).astype(np.float32)
    np.save(tmp_path / "common.npy", shared, allow_pickle=False)
    proof = write_array_block(tmp_path / "block.npy.zst", individual)
    source = FactorizedPanelFeatures(tmp_path, {
        "common": {"path": "common.npy"}, "blocks": [{**proof, "start": 0}],
        "individual_channels": [str(i) for i in range(6)],
        "common_channels": ["common1", "common2"], "stream_chunk_bytes": 3 * 4 * 10 * 4,
    }, base, cache_bytes=individual.nbytes)
    model = FinancialTransformerModel(lookback=2, num_features=10, num_symbols=3, d_model=8,
        attention_mode="market_token", temporal_layers=1, temporal_heads=2,
        cross_heads=2, latent_layers=1, num_latent_factors=2, num_market_tokens=1,
        market_layers=1, head_hidden_dim=8, head_layers=1, dropout=0., input_dropout=0.,
        candle_dropout=0., temporal_pooling="last", temporal_query_mode="last_only",
        feature_bottleneck_dim=0, return_aux=False).cpu().train()
    candidate = deepcopy(model)
    candidate.factorized_encoder_packet_cache = packet_cache
    # CPU oracle forces each scheduling branch; production retention is CUDA-only.
    mask = torch.ones((3, 3), dtype=torch.bool)
    slab = FactorizedPanelSlab(source, np.arange(2, 6))
    monkeypatch.setattr(policy, "_use_encoder_checkpoint", lambda *args: True)
    expected = model.forward_from_panel_slab(slab, mask)
    expected.square().sum().backward()
    after_reference = torch.get_rng_state().clone()
    monkeypatch.setattr(policy, "_use_encoder_checkpoint", lambda *args: False)
    actual = candidate.forward_from_panel_slab(slab, mask)
    actual.square().sum().backward()
    assert torch.equal(after_reference, torch.get_rng_state())
    assert torch.equal(expected, actual)
    for left, right in zip(model.parameters(), candidate.parameters(), strict=True):
        assert (left.grad is None) == (right.grad is None)
        if left.grad is not None:
            assert torch.equal(left.grad, right.grad)
