"""Year-only formal inference retains feature context and exact accounting."""
from dataclasses import fields, replace
from datetime import date
import hashlib
import json

import numpy as np
import pytest
import torch

from stockagent.data.inference_scope import (
    require_complete_year_split, scoped_source_digest, year_decision_mask,
)
from stockagent.data.tw_day_trade_carry_source import build_prepared_day_trade_carry_source
from stockagent.data.tw_day_trade_execution import load_tw_day_trade_execution_tape
from stockagent.data.walkforward import WalkForwardFold
from stockagent.training.dataset import CrossSectionalDataset
import stockagent.training.trainer as trainer
from test_day_trade_carry_source import _physical_source_inputs
from test_day_trade_inference_source import inference_config, physical_panel


def test_year_mask_preserves_observed_calendar_and_rejects_partial_owned_account():
    days = np.array(["2025-12-31", "2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    np.testing.assert_array_equal(year_decision_mask(days, 2026), [False, True, True])
    scope = require_complete_year_split(days, 2026, [1, 2], [2026])
    assert scope["materialized_sessions"] == 2
    assert scope["reference_calendar_sessions"] == 3
    assert scope["unrequested_sessions_are_zero_trades"] is False
    for rows, years in (([2], [2026]), ([0, 1, 2], [2026]), ([1, 2], [2025, 2026]),
                        ([2, 1], [2026]), ([1.0, 2.0], [2026])):
        with pytest.raises(ValueError, match="complete fresh owned"):
            require_complete_year_split(days, 2026, rows, years)
    assert scoped_source_digest("original", 2026) != scoped_source_digest("original", 2025)


@pytest.mark.parametrize("days,year", [(["2026-01-02", "2026-01-02"], 2026),
    (["2026-01-05", "2026-01-02"], 2026), (["NaT"], 2026),
    (["2026-01-02"], 2025), (["2026-01-02"], True)])
def test_year_scope_rejects_unknown_duplicate_unordered_or_unobserved_dates(days, year):
    with pytest.raises(ValueError):
        year_decision_mask(days, year)


def test_dataset_allows_prior_feature_context_but_not_prior_decisions():
    panel = physical_panel()
    panel.dates[:3] = np.array(["2023-12-27", "2023-12-28", "2023-12-29"], dtype="datetime64[D]")
    panel.inference_year = 2024
    # Only decision coordinates are bounded. Lookback still sees the original
    # preceding feature rows and does not warm up again inside January.
    dataset = CrossSectionalDataset(panel, [3, 4, 5], lookback=3)
    assert list(dataset.valid_indices) == [3, 4, 5]
    np.testing.assert_array_equal(dataset.features_t.numpy(), panel.features)
    for indices in ([0, 3], [-1], [6], [[3]], [3.5], [True]):
        with pytest.raises(ValueError, match="decision"):
            CrossSectionalDataset(panel, indices, lookback=1)


def test_scoped_source_matches_requested_year_without_loading_old_sessions(tmp_path):
    panel, public, minute = _physical_source_inputs(tmp_path)
    kwargs = dict(panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid")
    full = build_prepared_day_trade_carry_source(**kwargs)
    scoped = build_prepared_day_trade_carry_source(**kwargs, materialization_year=2020)
    assert len(scoped) == len(full) == 2  # stable checkpoint/global row axis
    assert scoped.release_id != full.release_id
    assert scoped.audit_receipt["materialization_scope"]["materialized_sessions"] == 1
    assert len(scoped.audit_receipt["minute_partition_scope"]["outside_materialization_scope_not_read"]) == 1
    for representation in ("session_at", "compact_session_at", "packed_session_loader"):
        with pytest.raises(ValueError, match="outside.*inference|outside.*materialization"):
            getattr(scoped, representation)(0)
    for item in fields(full.session_at(1)):
        actual, expected = getattr(scoped.session_at(1), item.name), getattr(full.session_at(1), item.name)
        if isinstance(expected, torch.Tensor):
            torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)
        else:
            assert actual == expected
    # Cache hits retain exactly the same scope; not a full-history cache alias.
    again = build_prepared_day_trade_carry_source(**kwargs, materialization_year=2020)
    assert again.release_id == scoped.release_id
    with pytest.raises(ValueError):
        again.session_at(0)


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("policy", ["scheduled", "full_session_volume_100pct"])
def test_execution_tape_leaves_unrequested_history_unknown(tmp_path, cached, policy):
    panel, _public, minute = _physical_source_inputs(tmp_path)
    if policy == "full_session_volume_100pct":
        from stockagent.data.tw_day_trade_execution import DAY_TRADE_MINUTE_SOURCE_SCHEMA_VERSION
        path = minute / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["schema_version"] = DAY_TRADE_MINUTE_SOURCE_SCHEMA_VERSION
        manifest["dates"] = [row["trade_date"] for row in manifest["partitions"]]
        path.write_text(json.dumps(manifest))
    kwargs = dict(panel_dates=panel.dates, panel_symbols=panel.symbols,
        official_open_prices=panel.open_prices, official_close_prices=panel.close_prices,
        daily_volume_shares=panel.daily_volumes, allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        cache_dir=tmp_path / "tape" if cached else None, policy=policy)
    full = load_tw_day_trade_execution_tape(minute, **kwargs)
    scoped = load_tw_day_trade_execution_tape(minute, **kwargs, materialization_year=2020)
    assert np.isnan(scoped[0]).all()
    np.testing.assert_array_equal(scoped[1], full[1])
    again = load_tw_day_trade_execution_tape(minute, **kwargs, materialization_year=2020)
    np.testing.assert_array_equal(again, scoped)


def test_year_inference_skips_old_validation_but_keeps_real_forward_and_artifacts(tmp_path, monkeypatch):
    panel = physical_panel()
    panel.dates[:3] = np.array(["2023-12-27", "2023-12-28", "2023-12-29"], dtype="datetime64[D]")
    original = panel.day_trade_carry_source
    source = replace(original, session_days=(), sessions=tuple(
        replace(session, day=date.fromisoformat(str(day)).toordinal())
        for session, day in zip(original.sessions, panel.dates, strict=True)))
    panel.day_trade_carry_source = source
    config = inference_config()
    fold = WalkForwardFold(fold_id=1, train_indices=np.array([0]),
        val_indices=np.array([1, 2]), test_indices=np.array([3, 4, 5]),
        train_years=[2022], val_years=[2023], test_years=[2024])
    model = trainer.build_model(config=config, lookback=1, num_features=2,
        num_symbols=panel.num_symbols, feature_names=panel.feature_names)
    with torch.no_grad():
        for parameter in model.parameters():
            parameter.zero_()
        list(model.parameters())[-1].fill_(0.21)
    fold_dir = trainer._fold_dir(tmp_path, 1)
    fold_dir.mkdir()
    checkpoint_path = trainer._best_checkpoint_path(fold_dir)
    torch.save({"model_state_dict": model.state_dict(), "best_val_loss": 0.0,
        "fold_id": 1, "train_years": [2022], "val_years": [2023], "test_years": [2024],
        "experiment_manifest": trainer._checkpoint_manifest(panel, config)}, checkpoint_path)
    before_hash = hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()
    baseline = trainer.run_inference(panel, [fold], config, tmp_path)
    with np.load(fold_dir / "test_backtest.npz", allow_pickle=False) as archive:
        expected = {name: archive[name].copy() for name in archive.files}
    assert np.any(expected["shares_history"] != 0), "parity must include real simulated positions"
    old_validation = baseline[0].val_metrics.copy()
    loaded_rows = []
    def scoped_session(row):
        assert row >= 3, "outside-year physical source must never be decoded"
        loaded_rows.append(row)
        return source.session_at(row)
    panel.day_trade_carry_source = replace(source, sessions=(),
        session_days=source.session_days, session_loader=scoped_session)
    actual = trainer.run_inference(panel, [fold], config, tmp_path, inference_year=2024)
    assert loaded_rows and set(loaded_rows) <= {3, 4, 5}
    assert actual[0].val_metrics == old_validation
    assert hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() == before_hash
    with np.load(fold_dir / "test_backtest.npz", allow_pickle=False) as archive:
        assert set(archive.files) == set(expected)
        for name, value in expected.items():
            np.testing.assert_array_equal(archive[name], value, err_msg=name)
    scope = json.loads((fold_dir / "inference_scope.json").read_text())
    assert scope["validation_recomputed"] is False
    assert scope["validation_years_retained"] == [2023]
    marker = json.loads((fold_dir / "fold_complete.json").read_text())
    assert marker["test_rows"] == 3
    assert marker["inference_scope"]["receipt_sha256"] == hashlib.sha256(
        (fold_dir / "inference_scope.json").read_bytes()).hexdigest()
    assert (fold_dir / "equity_curve.png").stat().st_size > 0
    assert (fold_dir / "annual_performance.png").stat().st_size > 0
