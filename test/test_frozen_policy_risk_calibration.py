"""Identity policy transfer, conditional risk credit, and immutable encoder reuse."""
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.data.factorized_panel import FactorizedPanelSlab
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.models.factorized_input import _frozen_policy_risk_output
from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config
from stockagent.training.trainer import (
    _PretrainedInitialization, _transfer_pretrained_feature_identity,
    _own_cudagraph_parameter_gradients,
)
from test_factorized_sparse_projection import source_fixture


def model(mode="off", device="cpu", *, features=7, symbols=4, compile=False):
    torch.manual_seed(23)
    return FinancialTransformerModel(lookback=4, num_features=features, num_symbols=symbols,
        d_model=8, temporal_layers=1, temporal_heads=2, temporal_ffn_mult=1,
        attention_mode="market_token", cross_heads=2, market_layers=1,
        num_market_tokens=1, head_hidden_dim=8, head_layers=1,
        dropout=0., input_dropout=0., candle_dropout=0.,
        portfolio_output_mode="score_entmax_scale_separated_cash", center_long_short_logits=False,
        portfolio_risk_calibration=mode, return_aux=False, factorized_input_compile=compile).to(device)


@pytest.mark.parametrize("mode", ["constant", "linear"])
@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_old_policy_identity_risk_gradient_and_mask(mode, device):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("requires CUDA")
    old = model(device=device)
    candidate = model(mode, device)
    candidate.load_state_dict(old.state_dict(), strict=False)
    x = torch.randn(3, 4, 4, 7, device=device)
    mask = torch.tensor([[True, True, True, False], [False]*4, [True]*4], device=device)
    with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
        reference = old(x, mask, return_aux=False)
        actual = candidate(x, mask, return_aux=False)
    assert torch.equal(reference, actual)
    for name, p in candidate.named_parameters():
        p.requires_grad_(name.startswith("risk_calibration_head."))
    with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
        weights = candidate(x, mask, return_aux=False)
    # Exact first-order log utility credit for fixed fractional signed legs.
    payoff = reference.detach().sign() * .02
    loss = -torch.log1p((weights * payoff).sum(1)).mean()
    loss.backward()
    grads = [p.grad for p in candidate.risk_calibration_head.parameters()]
    assert all(g is not None and torch.isfinite(g).all() for g in grads)
    assert sum(g.abs().sum().item() for g in grads) > 0
    assert all(p.grad is None for n, p in candidate.named_parameters()
               if not n.startswith("risk_calibration_head."))
    z = torch.randn(4, 4, 8, device=device)
    w = torch.tensor([[.2, -.1, .3, 0.], [0., 0., 0., 0.],
                      [.5, -.5, 0., 0.], [.02, -.03, 0., .1]], device=device)
    valid = w != 0
    for delta in [-100., -.4, -.0001, .0001, .4, 100.]:
        with torch.no_grad():
            head = candidate.risk_calibration_head
            head.weight.zero_()
            (head.weight if mode == "constant" else head.bias).fill_(delta)
            with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
                calibrated, _, _ = candidate._calibrate_portfolio_risk(w, z, valid)
        assert torch.isfinite(calibrated).all()
        assert (calibrated.abs().sum(1) <= 1.000001).all()
        assert torch.equal(calibrated.sign(), w.sign())
        assert (calibrated[~valid] == 0).all()
        # A scalar risk correction cannot redistribute nonzero stock legs.
        ratio = calibrated[0, :3] / w[0, :3]
        torch.testing.assert_close(ratio, ratio[:1].expand_as(ratio), rtol=1e-6, atol=1e-7)
        if abs(delta) == .0001:
            # Tiny risk updates must remain observable inside BF16 training.
            # Otherwise a correct head gradient could change no output at all.
            assert calibrated.dtype == torch.float32
            assert torch.any(calibrated[0] != w[0])
            expected_gross = torch.sigmoid(torch.logit(w[0].abs().sum()) + delta)
            torch.testing.assert_close(calibrated[0].abs().sum(), expected_gross,
                                       rtol=1e-6, atol=1e-7)


@pytest.mark.parametrize("mode", ["constant", "linear"])
def test_exact_transfer_allows_only_zero_identity_head(mode):
    old, target = model(), model(mode)
    init = _PretrainedInitialization(checkpoint_path=Path("old.pt"),
        checkpoint={"model_state_dict": old.state_dict()},
        source_feature_names=[f"f{i}" for i in range(7)], provenance={})
    report = _transfer_pretrained_feature_identity(target, init,
        target_feature_names=init.source_feature_names, require_exact_backbone=True,
        trainable_parameter_prefixes=["risk_calibration_head."])
    assert report["incompatible_source_tensor_count"] == 0
    assert report["identity_initialized_risk_head_tensors"]
    for name, value in old.state_dict().items():
        assert torch.equal(value, target.state_dict()[name])
    with torch.no_grad():
        target.risk_calibration_head.weight.add_(.1)
    with pytest.raises(RuntimeError, match="not exactly compatible"):
        _transfer_pretrained_feature_identity(target, init,
            target_feature_names=init.source_feature_names, require_exact_backbone=True,
            trainable_parameter_prefixes=["risk_calibration_head."])
    broken = deepcopy(init.checkpoint)
    broken["model_state_dict"].pop("score_head.0.proj.weight")
    init = _PretrainedInitialization(checkpoint_path=Path("broken.pt"), checkpoint=broken,
        source_feature_names=init.source_feature_names, provenance={})
    with pytest.raises(RuntimeError, match="not exactly compatible"):
        _transfer_pretrained_feature_identity(model(mode), init,
            target_feature_names=init.source_feature_names, require_exact_backbone=True,
            trainable_parameter_prefixes=["risk_calibration_head."])


@pytest.mark.parametrize("device,compile", [("cpu", False), ("cuda", False), ("cuda", True)])
@pytest.mark.filterwarnings("ignore:TensorFloat32 tensor cores.*:UserWarning")
def test_cached_encoder_matches_head_updates_and_invalidates_changes(tmp_path, device, compile):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("requires CUDA")
    source = source_fixture(tmp_path)
    candidate = model("linear", device, features=28, compile=compile)
    for name, p in candidate.named_parameters():
        p.requires_grad_(name.startswith(("score_head.", "risk_calibration_head.")))
    reference = deepcopy(candidate)
    candidate.factorized_frozen_encoder_cache_bytes = 4096
    slab = FactorizedPanelSlab(source, np.arange(6))
    mask = torch.ones(3, 4, dtype=torch.bool, device=device)
    optimizers = [torch.optim.SGD(m.parameters(), lr=.02) for m in [reference, candidate]]
    for _ in range(3):
        outputs = []
        for m, optimizer in zip([reference, candidate], optimizers):
            optimizer.zero_grad(set_to_none=True)
            with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
                w = m.forward_from_panel_slab(slab, mask, return_aux=False)
            outputs.append(w.detach().clone())
            (-w[:, 0].mean()+.2*w[:, 1].mean()).backward()
            optimizer.step()
        torch.testing.assert_close(outputs[0], outputs[1], rtol=2e-5, atol=2e-6)
        for (n, p), (n2, q) in zip(reference.named_parameters(), candidate.named_parameters()):
            assert n == n2
            if p.grad is not None:
                torch.testing.assert_close(p.grad, q.grad, rtol=3e-4, atol=2e-6)
    cache = candidate.__dict__["_frozen_encoder_cache"]
    assert cache["hits"] == 2 and cache["misses"] == 1
    assert cache["bytes"] <= 4096
    # Validation mode must not discard the training generation every epoch.
    candidate.eval()
    with torch.no_grad():
        candidate.forward_from_panel_slab(slab, mask, return_aux=False)
    candidate.train()
    with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
        candidate.forward_from_panel_slab(slab, mask, return_aux=False)
    assert cache["hits"] == 3
    mask2 = mask.clone(); mask2[0, -1] = False
    candidate.forward_from_panel_slab(slab, mask2, return_aux=False)
    assert cache["misses"] == 3
    with torch.no_grad():
        candidate.candle_encoder.candle_query.add_(.1)
    candidate.forward_from_panel_slab(slab, mask, return_aux=False)
    assert cache["misses"] == 4 and len(cache["entries"]) == 1
    candidate.candle_encoder.candle_query.requires_grad_(True)
    with pytest.raises(ValueError, match="only score/risk"):
        candidate.forward_from_panel_slab(slab, mask, return_aux=False)


def test_cache_budget_and_source_row_identity(tmp_path):
    source = source_fixture(tmp_path)
    m = model(features=28)
    for n, p in m.named_parameters():
        p.requires_grad_(n.startswith("score_head."))
    m.factorized_frozen_encoder_cache_bytes = 400
    mask = torch.ones(3, 4, dtype=torch.bool)
    for rows in [np.arange(6), np.arange(1, 7), np.arange(6)]:
        m.forward_from_panel_slab(FactorizedPanelSlab(source, rows), mask, return_aux=False)
    cache = m.__dict__["_frozen_encoder_cache"]
    assert cache["misses"] == 3 and cache["hits"] == 0
    assert cache["evictions"] == 2 and cache["bytes"] <= 400
    source.content_fingerprint = {}
    with pytest.raises(ValueError, match="immutable source"):
        m.forward_from_panel_slab(FactorizedPanelSlab(source, np.arange(6)), mask, return_aux=False)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA graphs")
@pytest.mark.filterwarnings("ignore:TensorFloat32 tensor cores.*:UserWarning")
@pytest.mark.filterwarnings("ignore:The CUDA Graph is empty.*:UserWarning")
def test_risk_head_cuda_graphs_keep_multiple_pending_forwards_owned(tmp_path):
    # CUDAGraphTreeManager intentionally captures an empty graph to own its
    # memory pool; pytest's error filter otherwise rejects that initialization.
    source = source_fixture(tmp_path)
    candidate = model("linear", "cuda", features=28, compile=True)
    candidate.temporal_blocks_fp32 = True
    candidate.factorized_cuda_graphs = True
    candidate.factorized_portfolio_cuda_graphs = True
    candidate.factorized_frozen_encoder_cache_bytes = 8192
    for name, p in candidate.named_parameters():
        p.requires_grad_(name.startswith("risk_calibration_head."))
    with torch.no_grad():
        candidate.risk_calibration_head.weight.fill_(.01)
    reference = deepcopy(candidate)
    # Keep the admitted compiled arithmetic fixed while isolating ownership
    # across pending forwards. BF16 eager/compiled schedules can round apart.
    reference.factorized_frozen_encoder_cache_bytes = 0
    slabs = [FactorizedPanelSlab(source, np.arange(i, i+6)) for i in range(3)]
    mask = torch.ones(3, 4, dtype=torch.bool, device="cuda")
    optimizers = [torch.optim.SGD(m.parameters(), lr=.01) for m in (reference, candidate)]
    for _ in range(2):
        torch.compiler.cudagraph_mark_step_begin()
        outputs = []
        for m, optimizer in zip((reference, candidate), optimizers):
            optimizer.zero_grad(set_to_none=True)
            with torch.autocast("cuda", dtype=torch.bfloat16):
                pending = [m.forward_from_panel_slab(s, mask, return_aux=False) for s in slabs]
            # Retain all batch graphs until one trajectory backward. Reusing a
            # producer's saved workspace would corrupt this sum's gradients.
            loss = sum((i+1)*w[:, 0].mean()-.3*w[:, 1].mean()
                       for i, w in enumerate(pending))
            outputs.append(torch.stack(pending).detach().clone())
            loss.backward()
            # Match the canonical executor: leaf .grad can initially be a
            # replay-owned backward output. Own it before another graph runs.
            _own_cudagraph_parameter_gradients(m)
            optimizer.step()
        torch.testing.assert_close(outputs[0], outputs[1], rtol=3e-5, atol=3e-6)
        for p, q in zip(reference.risk_calibration_head.parameters(),
                        candidate.risk_calibration_head.parameters()):
            torch.testing.assert_close(p.grad, q.grad, rtol=3e-4, atol=3e-6)
            torch.testing.assert_close(p, q, rtol=3e-5, atol=3e-6)
        assert sum(p.grad.abs().sum().item()
                   for p in candidate.risk_calibration_head.parameters()) > 0
    assert candidate._frozen_encoder_cache['hits'] == 3


def test_frozen_policy_cache_skips_scores_and_tracks_temperature_and_mutation(tmp_path):
    source = source_fixture(tmp_path)
    candidate = model("linear", features=28)
    for name, p in candidate.named_parameters():
        p.requires_grad_(name.startswith('risk_calibration_head.'))
    reference = deepcopy(candidate)
    candidate.factorized_frozen_encoder_cache_bytes = 4096
    slab = FactorizedPanelSlab(source, np.arange(6))
    mask = torch.ones(3, 4, dtype=torch.bool)
    score_calls = []
    handle = candidate.score_head.register_forward_hook(lambda *a: score_calls.append(1))
    optimizers = [torch.optim.SGD(m.parameters(), lr=.02) for m in (reference, candidate)]
    for _ in range(3):
        outputs = []
        for m, optimizer in zip((reference, candidate), optimizers):
            optimizer.zero_grad(set_to_none=True)
            w = m.forward_from_panel_slab(slab, mask, return_aux=False)
            outputs.append(w.detach().clone())
            (-w[:, 0].mean()+.2*w[:, 1].mean()).backward()
            optimizer.step()
        torch.testing.assert_close(outputs[0], outputs[1])
        for p, q in zip(reference.risk_calibration_head.parameters(),
                        candidate.risk_calibration_head.parameters()):
            torch.testing.assert_close(p.grad, q.grad)
    cache = candidate._frozen_encoder_cache
    assert cache['hits'] == 2 and cache['misses'] == 1
    assert len(score_calls) == 1
    value, _ = next(iter(cache['entries'].values()))
    assert isinstance(value, tuple) and len(value) == 2
    assert value[0].shape == (3, 4) and value[1].shape == (3, 24)
    candidate.forward_from_panel_slab(slab, mask, return_aux=False, temperature=.5)
    assert cache['misses'] == 2 and len(score_calls) == 2
    candidate.forward_from_panel_slab(slab, mask, return_aux=False)
    assert cache['hits'] == 3
    with torch.no_grad():
        next(candidate.score_head.parameters()).add_(.01)
    candidate.forward_from_panel_slab(slab, mask, return_aux=False)
    assert cache['misses'] == 3 and len(cache['entries']) == 1
    assert len(score_calls) == 3
    handle.remove()


@pytest.mark.parametrize('mode', ['constant', 'linear'])
def test_fused_risk_readout_matches_independent_analytic_vjp(mode):
    m = model(mode)
    features = (torch.ones(4, 1) if mode == 'constant' else
        torch.linspace(-.4, .6, 4*24).reshape(4, 24))
    w = torch.tensor([[.2, -.1, 0., .3], [0., 0., 0., 0.],
                      [.5, -.5, 0., 0.], [.02, -.03, 0., .1]])
    mask = w != 0
    cotangent = torch.tensor([[.7, -.3, 2., .5], [.1, .2, .3, .4],
                              [.4, -.2, .1, .3], [-.5, .8, .2, .3]])
    with torch.no_grad():
        m.risk_calibration_head.weight.fill_(.03)
        if m.risk_calibration_head.bias is not None:
            m.risk_calibration_head.bias.fill_(-.02)
    actual = _frozen_policy_risk_output(m, (w, features), mask)
    (actual*cotangent).sum().backward()
    # Independent odds derivative: d w_new / d delta = w_new*(1-g_new).
    # Compute the oracle in FP64 without differentiating the implementation.
    f64 = features.double()
    delta = f64 @ m.risk_calibration_head.weight.detach().double().T
    if m.risk_calibration_head.bias is not None:
        delta = delta+m.risk_calibration_head.bias.detach().double()
    gross = w.double().abs().sum(1, keepdim=True)
    expected = w.double()/(gross+(1-gross)*torch.exp(-delta))
    derivative = (expected*cotangent.double()).sum(1, keepdim=True)*(1-expected.abs().sum(1, keepdim=True))
    torch.testing.assert_close(m.risk_calibration_head.weight.grad.double(),
        derivative.T @ f64, rtol=1e-5, atol=1e-7)
    if m.risk_calibration_head.bias is not None:
        torch.testing.assert_close(m.risk_calibration_head.bias.grad.double(),
            derivative.sum(0), rtol=1e-5, atol=1e-7)
    assert w.grad is None and features.grad is None


def test_new_model_contracts_do_not_change_disabled_fingerprint():
    old = {"portfolio_output_mode": "score_entmax_scale_separated_cash"}
    disabled = {**old, "portfolio_risk_calibration": "off", "factorized_frozen_encoder_cache_bytes": 0}
    assert _project_temporal_basis_model_config(old) == _project_temporal_basis_model_config(disabled)
    for change in [{"portfolio_risk_calibration": "linear"},
                   {"factorized_frozen_encoder_cache_bytes": 4096}]:
        assert _project_temporal_basis_model_config({**old, **change}) != _project_temporal_basis_model_config(old)
    cfg = load_config("configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml")
    assert cfg.training.financial_transformer.portfolio_risk_calibration == "off"
