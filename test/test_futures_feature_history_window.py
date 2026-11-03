"""Feature prehistory must not create account sessions or future leakage."""

import pytest
import numpy as np
import torch
from dataclasses import replace

from stockagent.training.trainer import (
    _feature_slab_from_metadata,
    _pad_windowed_training_split,
    _windowed_base_compatible,
)
from stockagent.training.windowed import WindowedSplitTensors


def _split(indices):
    # Three historical feature sessions precede four account sessions.
    dates, offset, symbols = 4, 3, 2
    features = torch.arange((dates + offset) * symbols, dtype=torch.float32).reshape(-1, symbols, 1)
    mask = torch.ones((dates, symbols), dtype=torch.bool)
    return WindowedSplitTensors(
        features=features, valid_indices=torch.tensor(indices),
        future_log_returns=torch.arange(dates * symbols, dtype=torch.float32).reshape(dates, symbols),
        tradable_mask=mask, can_buy_mask=mask, can_sell_mask=mask,
        benchmark=torch.arange(dates, dtype=torch.float32), lookback=3,
        execution_mode="tw_day_trade", feature_row_offset=offset,
        day_trade_eligible_mask=mask, day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
    )


@pytest.mark.parametrize("indices", [[0, 1, 2, 3], [0, 2, 3]])
def test_feature_history_keeps_first_decision_and_account_axis(indices):
    split = _split(indices)
    x, returns, *_ = split.materialize_windows()
    for row, date_idx in enumerate(indices):
        assert torch.equal(x[row], split.features[date_idx:date_idx + 3])
        assert torch.equal(returns[row], split.future_log_returns[date_idx])
    # Day zero sees only three prior days; its own and later features are not read.
    assert torch.equal(x[0], split.features[:3])
    assert split.session_advance_mask.shape == (4,)


def test_feature_offset_survives_split_transforms_and_fast_slab():
    split = _split([0, 1, 2, 3])
    cached = split.to_device_cache(torch.device("cpu"))
    padded = _pad_windowed_training_split(split, 3)
    subset = split.subset_symbols(torch.tensor([1]))
    assert cached.feature_row_offset == padded.feature_row_offset == subset.feature_row_offset == 3
    assert _windowed_base_compatible(split, cached)
    assert torch.equal(cached.materialize_windows()[0], split.materialize_windows()[0])
    assert torch.equal(subset.materialize_windows()[0], split.materialize_windows()[0][:, :, 1:2])
    metadata = {"date_start": torch.tensor([0]), "date_indices": torch.tensor([0, 1])}
    slab = _feature_slab_from_metadata(split, metadata, torch.device("cpu"), False)
    assert torch.equal(slab, split.features[:4])


def test_feature_offset_rejects_bad_account_axis():
    split = _split([0, 1, 2, 3])
    with pytest.raises(ValueError, match="incompatible calendar"):
        WindowedSplitTensors(features=split.features, valid_indices=split.valid_indices,
            future_log_returns=split.future_log_returns, tradable_mask=split.tradable_mask,
            can_buy_mask=split.can_buy_mask, can_sell_mask=split.can_sell_mask,
            benchmark=split.benchmark, lookback=3, feature_row_offset=2)


@pytest.mark.parametrize("indices", [[-1], [4]])
def test_feature_offset_rejects_out_of_range_decisions(indices):
    with pytest.raises(ValueError, match="account calendar or causal window"):
        _split(indices)


def test_real_futures_account_dataset_uses_feature_only_precontext(tmp_path, monkeypatch):
    from stockagent.data import tw_futures_account_attachment as attachment
    from stockagent.data.futures_feature_history import bind_futures_feature_history
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.futures_account_bridge import PreparedFuturesAccountSource
    from stockagent.training.windowed import dataset_to_windowed_tensors
    from test_tw_futures_account_attachment import _fixture

    panel, config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, config)
    prior = np.full((2, *panel.features.shape[1:]), 17., dtype=np.float32)
    historical = replace(panel,
        dates=np.concatenate((np.asarray(["2025-12-30", "2025-12-31"], dtype="datetime64[D]"), panel.dates)),
        features=np.concatenate((prior, panel.features)),
        alive_mask=np.concatenate((np.ones(prior.shape[:2], dtype=bool), panel.alive_mask)))
    history = bind_futures_feature_history(historical, panel, lookback=2, feature_lag=1)
    panel = replace(panel, feature_history=history)
    source = PreparedFuturesAccountSource.from_daily(
        panel.stock_context_futures_portfolio_daily, panel.dates, prepare_execution=True)
    dataset = CrossSectionalDataset(panel, np.arange(2), lookback=2,
        execution_mode="tw_stock_context_futures_portfolio", futures_account_source=source)
    assert dataset.valid_indices.tolist() == [0, 1]
    assert dataset.feature_row_offset == 2
    np.testing.assert_array_equal(dataset[0]["x"].numpy(), prior)
    split = dataset_to_windowed_tensors(dataset)
    assert split.feature_row_offset == 2
    torch.testing.assert_close(split.materialize_windows()[0][0], dataset[0]["x"])
    assert split.overnight_log_returns.shape[0] == 2
    assert len(source.calendar.dates) == 3  # Includes exactly the real account-only event.
    with pytest.raises(ValueError, match="bound dated futures account"):
        CrossSectionalDataset(panel, np.arange(2), lookback=2,
            execution_mode="tw_stock_context_futures_portfolio")
    bad_panel = replace(panel, features=panel.features + 1)
    with pytest.raises(ValueError, match="exact tail"):
        history.verify(bad_panel)


def test_feature_history_loader_and_checkpoint_bind_full_history(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from downloader.artifact_io import sha256_file
    from stockagent.config import load_config
    from stockagent.data import tw_futures_decision_clock
    from stockagent.data.tw_futures_decision_clock import CLOCK_DATASET, decision_clock_contract
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest, validate_checkpoint_manifest
    from test_tw_futures_account_attachment import _fixture
    from test_tw_stock_context_futures_portfolio import _stock_panel
    from train import _attach_configured_stock_context_futures_panel

    _, short_config = _fixture(tmp_path, monkeypatch)
    clock = Path(short_config.trading.tw_futures_portfolio_entry_clock_path)
    clock.write_bytes(b"feature-history clock")
    clock.with_name("manifest.json").write_text(json.dumps({
        "dataset": CLOCK_DATASET, "status": "complete", "start_date": "2026-01-02",
        "end_date": "2026-01-04", "clock": decision_clock_contract("08:45"),
        "entries_sha256": sha256_file(clock),
    }))
    short_config.trading.tw_futures_account_calendar_contract = "dated_futures_account_calendar_v1"
    short_config.trading.tw_futures_feature_history_contract = "futures_feature_only_history_v2"
    short_config.training = SimpleNamespace(lookback=2)
    # The fixture's feature axis includes only real prior and decision days;
    # its account-only event is attached by the existing account adapter.
    panel = replace(_stock_panel(rows=4), dates=np.asarray(
        ["2025-12-30", "2025-12-31", "2026-01-02", "2026-01-04"], dtype="datetime64[D]"))
    attached = _attach_configured_stock_context_futures_panel(panel, short_config)
    assert attached.feature_history.prefix_rows == 2
    assert attached.dates.tolist() == panel.dates[2:].tolist()

    config = load_config("configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml")
    trading = replace(config.trading, tw_futures_account_calendar_contract="dated_futures_account_calendar_v1",
        tw_futures_feature_history_contract="futures_feature_only_history_v2",
        tw_futures_portfolio_capital_basis="initial_margin", tw_futures_portfolio_holding_policy="carry",
        tw_futures_portfolio_integer_contracts=True, tw_futures_portfolio_decision_time="08:45",
        tw_futures_portfolio_entry_clock_path=str(clock))
    config = replace(config, trading=trading)
    current = build_checkpoint_manifest(attached, config)
    contract = current["contracts"]["data"]
    assert contract["futures_feature_history_contract"] == "futures_feature_only_history_v2"
    assert contract["panel_arrays"]["futures_feature_history"]["prefix_rows"] == 2

    modified = attached.feature_history.features.copy()
    modified[0, 0, 0] += 1
    changed = replace(attached, feature_history=replace(attached.feature_history, features=modified))
    newer = build_checkpoint_manifest(changed, config)
    assert newer["fingerprints"]["data"] != current["fingerprints"]["data"]
    with pytest.raises(RuntimeError, match="data"):
        validate_checkpoint_manifest({"experiment_manifest": current}, newer,
                                     checkpoint_path=tmp_path / "old.pt", scope="resume")
    changed_alive = attached.feature_history.alive_mask.copy()
    changed_alive[0, 0] = ~changed_alive[0, 0]
    changed = replace(attached, feature_history=replace(attached.feature_history, alive_mask=changed_alive))
    assert build_checkpoint_manifest(changed, config)["fingerprints"]["data"] != current["fingerprints"]["data"]
    with pytest.raises(ValueError, match="unversioned checkpoint"):
        build_checkpoint_manifest(attached, replace(config, trading=replace(
            trading, tw_futures_feature_history_contract=None)))


def test_factorized_feature_history_keeps_lazy_source_and_exact_tail(tmp_path):
    from stockagent.data.factorized_panel import attach_factorized_features
    from stockagent.data.futures_feature_history import bind_futures_feature_history
    from stockagent.data.panel import slice_panel_end, slice_panel_start
    from test_factorized_panel import example

    panel, manifest, dense = example(tmp_path)
    panel = attach_factorized_features(panel, manifest)
    prefix = slice_panel_end(panel, panel.dates[6])
    decision = slice_panel_start(prefix, prefix.dates[3])
    history = bind_futures_feature_history(prefix, decision, lookback=3, feature_lag=1)
    assert history.prefix_rows == 3
    assert history.features._cache is decision.features._cache
    assert history.fingerprint()["features"] == prefix.features.content_fingerprint
    slab = history.features.as_torch().slab(0, 3).stock_chunk(0, len(panel.symbols))
    np.testing.assert_array_equal(slab.numpy(), dense[:3])


def test_feature_history_rms_fits_actual_prior_windows_and_binds_alive_mask(tmp_path):
    from types import SimpleNamespace
    from stockagent.config import load_config
    from stockagent.data.futures_feature_history import bind_futures_feature_history
    from stockagent.training.trainer import _fit_group_causal_feature_rms, _split_valid_indices

    dates = np.asarray(["2010-12-29", "2010-12-30", "2010-12-31",
                        "2011-01-03", "2011-01-04"], dtype="datetime64[D]")
    values = np.asarray([[2, 3], [2, 3], [2, 3], [2, 0], [1e6, 1e6]], dtype=np.float32)[:, None, :]
    source = SimpleNamespace(dates=dates, features=values, alive_mask=np.ones((5, 1), dtype=bool),
                             symbols=["A"], feature_names=["stable", "prior_only"])
    panel = SimpleNamespace(dates=dates[3:], features=values[3:], alive_mask=source.alive_mask[3:],
                            symbols=source.symbols, feature_names=source.feature_names)
    panel.feature_history = bind_futures_feature_history(source, panel, lookback=3, feature_lag=1)
    indices = np.asarray([0, 1], dtype=np.int64)
    assert _split_valid_indices(panel, indices, 3, "tw_stock_context_futures_portfolio", "panel_history").tolist() == [0, 1]
    assert not _split_valid_indices(panel, indices, 3, "tw_stock_context_futures_portfolio", "split_only").size
    train_ds = SimpleNamespace(valid_indices=indices, lookback=3,
                               execution_mode="tw_stock_context_futures_portfolio", feature_row_offset=3)
    config = load_config("configs/experiment_baseline.yaml")
    config.training.model_name = "financial_transformer"
    model = config.training.financial_transformer
    model.causal_feature_rms_normalization = True
    model.causal_feature_min_active_dates = 1
    model.causal_feature_scale_epsilon = 1e-6
    model.categorical_feature_names = []

    def fit():
        return _fit_group_causal_feature_rms(config=config, panel=panel, train_ds=train_ds,
            train_years=[2011], group_folds=[], output_path=tmp_path)

    scale, active, metadata = fit()
    torch.testing.assert_close(scale, torch.tensor([2., np.sqrt(27 / 4)], dtype=torch.float32))
    assert active.tolist() == [True, True]
    assert (metadata["feature_date_start"], metadata["feature_date_end"]) == ("2010-12-29", "2011-01-03")
    assert metadata["feature_row_offset"] == 3
    changed_alive = panel.feature_history.alive_mask.copy()
    changed_alive[0, 0] = False
    panel.feature_history = replace(panel.feature_history, alive_mask=changed_alive)
    changed_scale, _, changed_metadata = fit()
    torch.testing.assert_close(changed_scale, torch.tensor([2., np.sqrt(6.)], dtype=torch.float32))
    assert changed_metadata["normalizer_fingerprint"] != metadata["normalizer_fingerprint"]


def test_feature_history_training_projections_use_same_causal_rows(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from stockagent.config import load_config
    from stockagent.data.futures_feature_history import bind_futures_feature_history
    from stockagent.training import trainer

    dates = np.asarray(["2010-12-29", "2010-12-30", "2010-12-31",
                        "2011-01-03", "2011-01-04"], dtype="datetime64[D]")
    values = np.asarray([[1, 2], [2, 1], [3, 4], [4, 2], [1e6, 1e6]], dtype=np.float32)[:, None, :]
    source = SimpleNamespace(dates=dates, features=values, alive_mask=np.ones((5, 1), dtype=bool),
                             symbols=["A"], feature_names=["first", "second"])
    panel = SimpleNamespace(dates=dates[3:], features=values[3:], alive_mask=source.alive_mask[3:],
                            symbols=source.symbols, feature_names=source.feature_names)
    panel.feature_history = bind_futures_feature_history(source, panel, lookback=3, feature_lag=1)
    train_ds = SimpleNamespace(valid_indices=np.asarray([0, 1]), lookback=3,
        features_t=torch.from_numpy(values), execution_mode="tw_stock_context_futures_portfolio",
        feature_row_offset=3)
    config = load_config("configs/markets/tw_public_multi_basis.yaml")
    config.training.lookback = 3
    config.training.temporal_basis_covariance_cpu_threads = 1
    model = config.training.financial_transformer
    model.temporal_basis_families = ["pca_klt"]
    model.temporal_basis_components = 2
    model.temporal_basis_components_by_family = {}
    model.causal_feature_rms_normalization = True
    model.categorical_feature_names = []
    model.feature_svd_components = 1
    model.feature_svd_analysis_components = 2
    model.feature_svd_oversampling = 0
    model.feature_svd_power_iterations = 0

    seen = []
    original = trainer.fit_training_only_pca_klt
    def record_indices(features, targets, **kwargs):
        seen.append(np.asarray(targets).tolist())
        return original(features, targets, **kwargs)
    monkeypatch.setattr(trainer, "fit_training_only_pca_klt", record_indices)
    common = dict(config=config, panel=panel, train_ds=train_ds, train_years=[2011],
                  group_folds=[], output_path=tmp_path)
    _, temporal_metadata = trainer._fit_group_temporal_basis(**common)
    assert seen == [[3, 4]]
    assert temporal_metadata["training_covariance"]["validation_rows_used"] == 0
    rms = trainer._fit_group_causal_feature_rms(**common)
    directions, metadata = trainer._fit_group_feature_svd(**common, device=torch.device("cpu"),
                                                          causal_feature_rms=rms)
    assert directions.shape == (1, 2)
    assert (metadata["feature_date_start"], metadata["feature_date_end"]) == ("2010-12-29", "2011-01-03")
    assert metadata["alive_training_cells"] == 4
    assert metadata["validation_rows_used"] == metadata["test_rows_used"] == 0
    assert metadata["feature_history"]["contract"] == "futures_feature_only_history_v2"


def test_feature_history_config_requires_explicit_dated_account_and_history(tmp_path):
    import yaml
    from stockagent.config import _load_raw_config, load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot

    raw = _load_raw_config("configs/markets/tw_stock_futures_carry_0845_20261021_v1.yaml")
    raw["data"]["panel_start_date"] = "2010-11-01"
    raw["walk_forward"]["split_start_year"] = 2011
    raw["walk_forward"]["expected_first_year"] = 2011
    raw["walk_forward"]["lookback_context"] = "panel_history"
    raw["walk_forward"]["year_boundary_mode"] = "calendar"
    raw["trading"]["tw_futures_account_calendar_contract"] = "dated_futures_account_calendar_v1"
    raw["trading"]["tw_futures_feature_history_contract"] = "futures_feature_only_history_v2"
    path = tmp_path / "config.yaml"

    def load():
        path.write_text(yaml.safe_dump(raw))
        return load_config(path)

    active = load()
    assert active.trading.tw_futures_feature_history_contract == "futures_feature_only_history_v2"
    assert _configuration_fingerprint_snapshot(active)["trading"]["tw_futures_feature_history_contract"] == "futures_feature_only_history_v2"
    disabled = replace(active, trading=replace(active.trading, tw_futures_feature_history_contract=None))
    assert "tw_futures_feature_history_contract" not in _configuration_fingerprint_snapshot(disabled)["trading"]
    for group, key, value in (
        ("trading", "tw_futures_account_calendar_contract", None),
        ("walk_forward", "lookback_context", "split_only"),
        ("walk_forward", "split_start_year", None),
        ("trading", "tw_futures_feature_history_contract", "unknown"),
    ):
        previous = raw[group][key]
        raw[group][key] = value
        with pytest.raises(ValueError, match="feature history"):
            load()
        raw[group][key] = previous
