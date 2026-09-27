from io import BytesIO
import json
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.models.cross_sectional_all_futures import CrossSectionalAllFuturesModel
from stockagent.models.factory import build_model
from stockagent.training import trainer


def _case():
    config = load_config("configs/markets/tw_futures_v8_general.yaml")
    config.training.financial_transformer.futures_feature_rms_normalization = True
    config.training.financial_transformer.causal_feature_min_active_dates = 2
    dates = np.arange("2020-01-01", "2020-01-09", dtype="datetime64[D]")
    features = np.zeros((8, 2, 22), dtype=np.float32)
    features[:, :, 0] = 1000  # categorical product ID is outside the RMS ABI
    features[:2, :, 1:18] = 1e6  # lookback context is not a futures decision
    features[3, :, 1:18] = 1e6  # a filtered training date must stay excluded
    features[2, 0, 1] = 2
    features[2, 1, 1] = 1e6  # unavailable candidate
    features[4, :, 1] = 4
    features[4, :, 2] = 7  # fewer than two observed training dates
    features[5:, :, 1:18] = 1e9  # validation/test and future-only columns
    mask = np.ones((8, 2), dtype=np.bool_)
    mask[2, 1] = False
    daily = SimpleNamespace(candidate_features=features, candidate_mask=mask, dates=dates)
    panel = SimpleNamespace(stock_context_futures_portfolio_daily=daily)
    dataset = SimpleNamespace(valid_indices=np.array([2, 4]), execution_mode="tw_stock_context_futures_portfolio")
    return config, panel, dataset


def _fit(tmp_path, config, panel, dataset):
    return trainer._fit_group_futures_feature_rms(
        config=config, panel=panel, train_ds=dataset, train_years=[2020],
        group_folds=[SimpleNamespace(fold_id=1)], output_path=tmp_path,
    )


def test_futures_rms_uses_only_selected_training_decisions_and_candidate_mask(tmp_path):
    config, panel, dataset = _case()
    fitted = _fit(tmp_path, config, panel, dataset)
    assert fitted is not None
    scale, active, metadata = fitted
    assert scale.shape == (17,)
    assert scale[0] == pytest.approx(np.sqrt(12))
    assert active.tolist() == [True] + [False] * 16
    torch.testing.assert_close(scale[1:], torch.ones(16))
    assert metadata["candidate_cell_count"] == 3
    assert metadata["train_row_count"] == 2
    assert metadata["feature_date_start"] == "2020-01-03"
    assert metadata["feature_date_end"] == "2020-01-05"
    for path in [tmp_path / "train_2020/futures_feature_rms_normalization.json",
                 tmp_path / "fold_01/futures_feature_rms_normalization.json"]:
        assert json.loads(path.read_text()) == metadata
    panel.stock_context_futures_portfolio_daily.candidate_features[5:] *= 100
    after = _fit(tmp_path, config, panel, dataset)
    torch.testing.assert_close(scale, after[0], rtol=0, atol=0)
    torch.testing.assert_close(active, after[1], rtol=0, atol=0)
    assert metadata == after[2]


def test_futures_rms_cache_and_disabled_mode_use_canonical_fit_gate(tmp_path, monkeypatch):
    config, panel, dataset = _case()
    first = _fit(tmp_path, config, panel, dataset)
    def forbidden(*_args, **_kwargs):
        raise AssertionError("the exact cached transform must not refit")
    monkeypatch.setattr(trainer, "_fit_masked_training_feature_rms", forbidden)
    second = _fit(tmp_path, config, panel, dataset)
    torch.testing.assert_close(first[0], second[0], rtol=0, atol=0)
    config.training.financial_transformer.futures_feature_rms_normalization = False
    assert _fit(tmp_path, config, SimpleNamespace(), SimpleNamespace()) is None


@pytest.mark.parametrize("model_name", ["financial_transformer", "cross_sectional_all_futures"])
def test_factory_fitted_buffers_survive_checkpoint_and_reapply_after_transfer(tmp_path, model_name):
    config, panel, dataset = _case()
    fitted = _fit(tmp_path, config, panel, dataset)
    config.training.model_name = model_name
    config.training.transformer_base_portfolio.futures_feature_rms_normalization = True
    for model_config in [config.training.financial_transformer,
                         config.training.transformer_base_portfolio]:
        model_config.temporal_basis_families = ["haar", "dct"]
        model_config.temporal_basis_disabled_families = []
        model_config.temporal_basis_components_by_family = {}
    model = build_model(config=config, lookback=32, num_features=3, num_symbols=2,
                        feature_names=["a", "b", "c"])
    assert model.futures_feature_rms_normalization
    # A source checkpoint's scale must not own the new fold's transform.
    model.set_futures_feature_rms_normalizer(torch.full((17,), 999.0), torch.ones(17, dtype=torch.bool))
    trainer._apply_futures_feature_rms_to_model(model, fitted)
    torch.testing.assert_close(model.futures_feature_rms_scale, fitted[0])
    torch.testing.assert_close(model.futures_feature_active_mask, fitted[1])
    assert model.futures_feature_rms_metadata == fitted[2]
    archive = BytesIO()
    torch.save(model.state_dict(), archive)
    archive.seek(0)
    restored = torch.load(archive, weights_only=True)
    torch.testing.assert_close(restored["futures_feature_rms_scale"], fitted[0])
    torch.testing.assert_close(restored["futures_feature_active_mask"], fitted[1])


def test_exact_resume_keeps_checkpoint_rms_buffers_and_forward_output():
    torch.manual_seed(2718)

    def make_model():
        return CrossSectionalAllFuturesModel(
            lookback=1, num_features=3, num_symbols=2, d_model=8,
            attention_mode="temporal_only", use_latent_factors=False,
            use_market_tokens=False, temporal_layers=1, temporal_heads=2,
            temporal_ffn_mult=2, temporal_pooling="last",
            temporal_query_mode="last_only", portfolio_mode="long_short",
            portfolio_output_mode="projection_l1",
            projection_l1_scale_by_active_count=True,
            center_long_short_logits=False, futures_feature_rms_normalization=True,
            return_aux=False, execution_mode="tw_stock_context_futures_portfolio",
        ).eval()

    model = make_model()
    checkpoint_scale = torch.arange(1, 18, dtype=torch.float32)
    checkpoint_active = torch.arange(17) % 3 != 0
    model.set_futures_feature_rms_normalizer(checkpoint_scale, checkpoint_active)
    inputs = torch.randn(1, 1, 2, 3)
    stock_mask = torch.ones(1, 2, dtype=torch.bool)
    features = torch.randn(1, 1936, 18)
    features[..., 0] = 1
    candidate_mask = torch.zeros(1, 1936, dtype=torch.bool)
    candidate_mask[:, [2, 9, 20]] = True
    context = {"candidate_features": features, "candidate_mask": candidate_mask}
    with torch.no_grad():
        expected = model(inputs, stock_mask, portfolio_context=context)
    assert torch.count_nonzero(expected).item() > 0
    archive = BytesIO()
    torch.save({"model_state_dict": model.state_dict()}, archive)
    archive.seek(0)

    resumed = make_model()
    # The new process can fit a different transform before restoring its exact
    # checkpoint. Resume must retain the checkpoint-owned persistent buffers.
    resumed.set_futures_feature_rms_normalizer(
        torch.full((17,), 999.0), ~checkpoint_active,
    )
    checkpoint = torch.load(archive, weights_only=True)
    trainer._load_state_dict(resumed, checkpoint["model_state_dict"])
    torch.testing.assert_close(resumed.futures_feature_rms_scale, checkpoint_scale, rtol=0, atol=0)
    torch.testing.assert_close(resumed.futures_feature_active_mask, checkpoint_active, rtol=0, atol=0)
    with torch.no_grad():
        actual = resumed(inputs, stock_mask, portfolio_context=context)
    torch.testing.assert_close(actual, expected, rtol=0, atol=0)


def test_futures_rms_rank_zero_payload_is_broadcast(tmp_path, monkeypatch):
    config, panel, dataset = _case()
    published = []
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer, "_distributed_should_write", lambda: True)
    monkeypatch.setattr(trainer, "_run_rank0_store_synchronized_phase", lambda _name, fn: fn())
    monkeypatch.setattr(trainer.dist, "broadcast_object_list", lambda objects, src: published.append(objects[0]))
    fitted = _fit(tmp_path, config, panel, dataset)
    assert len(published) == 1 and published[0]["metadata"] == fitted[2]
    monkeypatch.setattr(trainer, "_run_rank0_store_synchronized_phase", lambda _name, _fn: None)
    monkeypatch.setattr(trainer, "_distributed_should_write", lambda: False)
    monkeypatch.setattr(trainer.dist, "broadcast_object_list", lambda objects, src: objects.__setitem__(0, published[0]))
    received = _fit(tmp_path, config, panel, dataset)
    torch.testing.assert_close(fitted[0], received[0], rtol=0, atol=0)
    assert received[2] == fitted[2]
