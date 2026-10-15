import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import (
    FactorizedDateView, FactorizedLiveWindow, attach_factorized_features,
)
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.training.runtime import call_model
from test_factorized_panel import example


def make_model():
    model = FinancialTransformerModel(lookback=4, num_features=10, num_symbols=3,
        d_model=8, attention_mode="market_token", temporal_layers=1,
        temporal_heads=2, cross_heads=2, num_market_tokens=1, market_layers=1,
        head_hidden_dim=8, head_layers=1, dropout=0., input_dropout=0.,
        candle_dropout=0., causal_feature_rms_normalization=True,
        sanitize_inputs=False).eval()
    model.candle_encoder.set_causal_feature_rms_normalizer(
        torch.arange(1, 11, dtype=torch.float32), torch.arange(10) % 3 != 0)
    return model


def assert_tree_bits(a, b):
    if isinstance(a, torch.Tensor):
        assert a.dtype == b.dtype and a.shape == b.shape
        assert torch.equal(a.contiguous().view(torch.uint8), b.contiguous().view(torch.uint8))
    elif isinstance(a, dict):
        assert a.keys() == b.keys()
        for key in a:
            assert_tree_bits(a[key], b[key])
    elif isinstance(a, (tuple, list)):
        assert len(a) == len(b)
        for x, y in zip(a, b, strict=True):
            assert_tree_bits(x, y)
    else:
        assert a == b


@pytest.mark.parametrize("device", ["cpu", "cuda"])
@pytest.mark.parametrize("overlay", [False, True])
def test_live_transport_complete_outputs_preserve_bits_and_source(tmp_path, device, overlay):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, dense = example(tmp_path)
    panel.features[4, 0, 0] = dense[4, 0, 0] = -0.
    panel.features[5, 1, 1] = dense[5, 1, 1] = float("nan")
    attach_factorized_features(panel, path)
    view = FactorizedDateView(panel.features, [1, 3, 4, 5, 6])
    window = FactorizedLiveWindow(view, 1, 5)
    expected = np.nan_to_num(dense[[3, 4, 5, 6]], nan=0., posinf=0., neginf=0.)
    original = view[1:5].copy()
    if overlay:
        values = np.array([-.02, -0., .025], dtype=np.float32)
        window = window.with_last_feature(1, values)
        expected[-1, :, 1] = values
        values[:] = 1.  # The request owns its copied quote overlay.
    target = torch.device(device)
    actual = window.materialize(target).cpu().numpy()[0]
    assert actual.tobytes() == expected.tobytes()
    assert window[-1].tobytes() == expected[-1].tobytes()
    with pytest.raises(IndexError):
        window[0]
    model = make_model().to(target)
    mask = torch.tensor([[True, False, True]], device=target)
    with torch.inference_mode(), torch.autocast(device, dtype=torch.bfloat16,
                                              enabled=device == "cuda"):
        control = call_model(model, torch.from_numpy(expected).unsqueeze(0).to(target),
                             mask, return_aux=True)
        candidate = call_model(model, window, mask, return_aux=True)
        assert_tree_bits(control, candidate)
        assert_tree_bits(candidate, call_model(model, window, mask, return_aux=True))
    assert view[1:5].tobytes() == original.tobytes()


def test_owned_candle_requires_explicit_inference_and_plain_rms():
    model = make_model()
    x = torch.ones(1, 4, 3, 10)
    with pytest.raises(RuntimeError, match="plain FP32 inference"):
        model.candle_encoder(x, owned_inference=True)
    with torch.inference_mode():
        control, _ = model.candle_encoder(x.clone())
        candidate, _ = model.candle_encoder(x.clone(), owned_inference=True)
        assert_tree_bits(control, candidate)


def test_live_window_rejects_training_before_any_source_read(tmp_path):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    window = FactorizedLiveWindow(panel.features, 1, 5)
    with pytest.raises(RuntimeError, match="eval and inference"):
        call_model(make_model().train(), window, torch.ones(1, 3, dtype=torch.bool))
    with pytest.raises(ValueError, match="outside the source calendar"):
        FactorizedLiveWindow(panel.features, -1, 3)
    with pytest.raises(ValueError, match="symbol-aligned"):
        window.with_last_feature(0, [float("nan")]*3)


def test_only_matching_plain_factorized_model_admits_transport():
    from dataclasses import replace
    from stockagent.config import load_config
    from stockagent.live.signal_engine import _factorized_live_stream_admitted
    config = load_config("configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_live_v1.yaml")
    assert _factorized_live_stream_admitted(config)
    for delta in ({"causal_feature_rms_normalization": False},
                  {"causal_feature_window_rms_normalization": True},
                  {"temporal_basis_families": ["haar"]},
                  {"candle_dropout": .1}, {"feature_svd_components": 2}):
        other = replace(config, training=replace(config.training,
            financial_transformer=replace(config.training.financial_transformer, **delta)))
        assert not _factorized_live_stream_admitted(other)


def test_live_attachment_respects_existing_cache_budget(tmp_path):
    from stockagent.data.factorized_panel import attach_factorized_live_features
    panel, path, _ = example(tmp_path)
    import json
    manifest = json.loads(path.read_text())
    manifest["paper_inference"] = {"contract": "factorized_paper_projection_lag1_v1",
        "status": "verified", "raw_source_publication": False}
    path.write_text(json.dumps(manifest))
    attached = attach_factorized_live_features(panel, path, host_cache_bytes=4096)
    assert attached.features.source._cache.budget_bytes == 4096


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_opening_adapter_preserves_complete_window_and_missing_price_sentinel(tmp_path, device):
    from stockagent.live.quote_provider import PriceSnapshot
    from stockagent.live.signal_engine import (
        DAY_TRADE_OPEN_GAP_FEATURE, _day_trade_live_model_window,
    )
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, dense = example(tmp_path)
    attach_factorized_features(panel, path)
    panel.feature_names[1] = DAY_TRADE_OPEN_GAP_FEATURE
    base = dense[-4:].copy()
    before = base.copy()
    lazy = FactorizedLiveWindow(panel.features, 5, 9)
    snapshot = PriceSnapshot(prices=np.array([1.05, np.nan, 1.04]),
        open_prices=np.array([1.05, np.nan, 1.04]), source="shioaji",
        timestamp="2025-01-10 09:00:00")
    kwargs = dict(panel_idx=8, lookback=4, price_snapshot=snapshot,
        resolved_asof="2025-01-10 09:00:00", source_timezone="Asia/Taipei")
    control = _day_trade_live_model_window(panel, base_model_window=base, **kwargs)
    candidate = _day_trade_live_model_window(panel, base_model_window=lazy, **kwargs)
    assert candidate[1:4] == control[1:4]
    assert candidate[4].tobytes() == control[4].tobytes()
    assert candidate[4].tolist() == [True, False, True]
    assert candidate[0].materialize(torch.device(device)).cpu().numpy()[0].tobytes() == control[0].tobytes()
    assert control[0][-1, 1, 1] == 0.  # Unknown price is not an invented open.
    assert base.tobytes() == before.tobytes()
    assert panel.features[-4:].tobytes() == before.tobytes()


@pytest.mark.parametrize("attribute,value", [
    ("causal_feature_rms_normalization", False),
    ("causal_feature_compression", "log1p"),
    ("causal_feature_window_rms_normalization", True),
    ("feature_svd_components", 2), ("feature_bottleneck_dim", 2),
])
def test_live_model_guard_rejects_other_feature_transforms_before_io(tmp_path, attribute, value):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    window = FactorizedLiveWindow(panel.features, 1, 5)
    model = make_model()
    setattr(model.candle_encoder, attribute, value)
    with torch.inference_mode(), pytest.raises(ValueError, match="admitted no-basis"):
        call_model(model, window, torch.ones(1, 3, dtype=torch.bool))
    assert panel.features._cache.misses == 0
