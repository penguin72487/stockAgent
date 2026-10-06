"""Storage/recomputation policy, not a different wide-panel model."""
from copy import deepcopy
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


def test_activation_retention_preserves_outputs_gradients_and_rng(tmp_path, monkeypatch):
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
