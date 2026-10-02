"""Checkpoint projection must retain the exact executor-only FIFO source."""
from dataclasses import fields, replace
from datetime import date
import hashlib
import json

import numpy as np
import pytest
import torch

from stockagent.training.checkpoint_contract import subset_panel_symbols
from stockagent.training.day_trade_carry_bridge import (
    PackedDayTradeCarrySession, PreparedDayTradeCarryBatch, PreparedDayTradeCarrySource,
)
from stockagent.backtest.tw_day_trade_carry import compact_day_trade_carry_session
from stockagent.training.day_trade_carry_artifact import DayTradeCarryArtifactContext
from stockagent.config import load_config
from stockagent.data.walkforward import WalkForwardFold
from stockagent.data.tw_day_trade_carry_source import _PhysicalSessionCache
import stockagent.training.trainer as trainer
from test_checkpoint_manifest import _day_trade_minute_panel
from test_day_trade_carry_training import Policy, fixture


def physical_panel():
    panel = _day_trade_minute_panel()
    panel.can_short_open_open_mask = panel.tradable_mask.copy()
    panel.corporate_action_avoidance_mask = np.zeros_like(panel.tradable_mask)
    panel.unresolved_corporate_action_mask = np.zeros_like(panel.tradable_mask)
    _, runtime, _ = fixture(rows=panel.num_dates - 1, symbols=panel.num_symbols)
    source = runtime.day_trade_carry_source
    panel.day_trade_carry_source = replace(
        source,
        universe=tuple(panel.symbols),
        session_days=(),
        sessions=tuple(
            replace(session, day=date.fromisoformat(str(day)).toordinal())
            for session, day in zip(source.sessions, panel.dates, strict=True)
        ),
    )
    # The compressed executor tape is separate from features, but has the
    # identical stock axis and must not disappear during checkpoint alignment.
    panel.day_trade_minute_execution[:] = np.arange(panel.num_symbols)[None, :, None]
    return panel


def test_checkpoint_subset_keeps_lazy_pinned_carry_source_and_minute_tape():
    panel = physical_panel()
    original = panel.day_trade_carry_source
    aligned = subset_panel_symbols(panel, ["0050", "1101"])

    assert aligned.day_trade_carry_source is not None
    assert aligned.day_trade_carry_source.universe == ("0050", "1101")
    assert aligned.day_trade_carry_source.release_id == original.release_id
    assert aligned.day_trade_carry_source.session_days == original.session_days
    assert aligned.day_trade_carry_source.sessions == ()
    np.testing.assert_array_equal(
        aligned.day_trade_minute_execution, panel.day_trade_minute_execution[:, [2, 0]]
    )
    assert original.universe == tuple(panel.symbols)
    for field in fields(original.session_at(0)):
        value = getattr(original.session_at(0), field.name)
        projected = getattr(aligned.day_trade_carry_source.session_at(0), field.name)
        if isinstance(value, torch.Tensor):
            torch.testing.assert_close(projected, value[[2, 0]], rtol=0, atol=0, equal_nan=True)


def test_checkpoint_projection_cannot_fabricate_missing_physical_source():
    panel = physical_panel()
    with pytest.raises(ValueError, match="physical.*missing|missing.*physical"):
        subset_panel_symbols(panel, ["0050", "MISSING"], allow_missing_masked=True)


def test_checkpoint_projection_rejects_misaligned_source_before_loading():
    panel = physical_panel()
    panel.day_trade_carry_source = replace(
        panel.day_trade_carry_source, universe=("2330", "0050", "1101")
    )
    with pytest.raises(ValueError, match="physical.*universe"):
        subset_panel_symbols(panel, ["0050", "1101"])


def test_checkpoint_projection_rejects_misaligned_calendar_before_loading():
    panel = physical_panel()
    panel.day_trade_carry_source = replace(
        panel.day_trade_carry_source, session_days=(),
        sessions=tuple(replace(s, day=s.day + 1) for s in panel.day_trade_carry_source.sessions),
    )
    with pytest.raises(ValueError, match="physical.*calendar"):
        subset_panel_symbols(panel, ["0050", "1101"])


def test_projection_does_not_decode_sessions_until_used():
    panel = physical_panel()
    original = panel.day_trade_carry_source
    calls = []

    def loader(row):
        calls.append(row)
        return original.session_at(row)

    panel.day_trade_carry_source = PreparedDayTradeCarrySource(
        sessions=(), universe=original.universe, release_id=original.release_id,
        session_days=original.session_days, session_loader=loader,
    )
    aligned = subset_panel_symbols(panel, ["0050", "1101"])
    assert calls == []
    assert aligned.day_trade_carry_source is not None
    aligned.day_trade_carry_source.session_at(1)
    assert calls == [1]


def test_sparse_projection_preserves_csr_order_and_integer_identities():
    source = physical_panel().day_trade_carry_source
    compact = tuple(compact_day_trade_carry_session(s) for s in source.sessions)
    lazy = PreparedDayTradeCarrySource(
        sessions=(), universe=source.universe, release_id=source.release_id,
        session_days=source.session_days, session_loader=source.session_at,
        compact_session_loader=lambda row: compact[row],
    )
    projected = lazy.project_universe(("0050", "1101"))
    actual = projected.compact_session_at(0)
    expected = compact_day_trade_carry_session(projected.session_at(0))
    for field in fields(actual):
        observed = getattr(actual, field.name)
        target = getattr(expected, field.name)
        if isinstance(observed, torch.Tensor):
            torch.testing.assert_close(observed, target, rtol=0, atol=0, equal_nan=True)
    assert actual.exit_symbol_indices.dtype == torch.int64
    # Also exercise the generic row API; it must not cast CSR identities to FP64.
    eager_sparse = replace(source, sessions=compact)
    rows = eager_sparse.rows([0], [2, 0], torch.device("cpu"))
    assert rows[0].official_open.shape == (2,)
    assert rows[0].uses_sparse_events
    assert rows[0].exit_symbol_indices.dtype == torch.int64
    rows[0].validate_shape(2, torch.device("cpu"))


def test_packed_projection_retains_transport_and_matches_dense_without_decoding():
    source = physical_panel().day_trade_carry_source
    dense = source.session_at(0)
    retained = torch.isfinite(dense.exit_prices) | (dense.exit_capacity != 0)
    flat = torch.nonzero(retained.flatten(), as_tuple=False).flatten()
    payload = {}
    for field in fields(PackedDayTradeCarrySession):
        if field.name in {"day", "exit_flat", "exit_price", "exit_capacity"}:
            continue
        value = getattr(dense, field.name, None)
        if value is None and field.name not in {"terminal_liquidation_price", "entry_path", "stop_hits"}:
            value = dense.official_open.new_full(
                (len(source.universe),), 1.0 if field.name == "share_ratio" else 0.0
            )
        payload[field.name] = value
    packed = PackedDayTradeCarrySession(
        day=dense.day, exit_flat=flat,
        exit_price=dense.exit_prices.flatten()[flat],
        exit_capacity=dense.exit_capacity.flatten()[flat], **payload,
    )
    calls = []

    def reject_dense(row):
        raise AssertionError("packed projection must not decode dense exits")

    def load_packed(row):
        calls.append(row)
        return packed

    lazy = PreparedDayTradeCarrySource(
        sessions=(), universe=source.universe, release_id=source.release_id,
        session_days=source.session_days, session_loader=reject_dense,
        packed_session_loader=load_packed,
    )
    projected = lazy.project_universe(("0050", "1101"))
    assert calls == []
    actual_packed = projected.packed_session_loader(0)
    assert calls == [0]
    assert bool((actual_packed.exit_flat[1:] > actual_packed.exit_flat[:-1]).all())
    rebuilt = PreparedDayTradeCarryBatch.from_packed_sessions(
        (actual_packed,), 1, event_compression=True,
    ).sessions(torch.device("cpu"))[0]
    expected = source.project_universe(("0050", "1101")).session_at(0)
    for name in ("official_open", "exit_prices", "exit_capacity", "marks"):
        torch.testing.assert_close(
            getattr(rebuilt, name), getattr(expected, name), rtol=0, atol=0, equal_nan=True,
        )


def inference_config():
    config = load_config("configs/experiment_baseline.yaml")
    config.environment.device = "cpu"
    config.environment.amp_dtype = "tf32"  # CPU path is ordinary FP32, not AMP.
    config.training.model_name = "mlp"
    config.training.lookback = 1
    config.training.enable_torch_compile = False
    config.training.backtest_compile = False
    config.training.inference_backtest_compile = False
    config.training.backtest_autotune = False
    config.training.inference_backtest_autotune = False
    config.training.strict_no_fallback = False
    config.training.table_output_format = "csv"
    config.training.save_daily_weights_table = True
    config.training.mlp.hidden_dim = 8
    config.training.mlp.embedding_dim = 4
    config.training.mlp.dropout = 0.0
    config.trading.execution_mode = "tw_day_trade"
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    config.trading.volume_participation_equity = 10_000_000.0
    config.trading.max_turnover_ratio = 0.0
    config.trading.max_volume_participation = 0.5
    config.trading.reporting_leverage = 1.0
    config.trading.tw_commission_rebate_timing = "daily_close"
    config.walk_forward.lookback_context = "panel_history"
    return config


def test_full_owned_prefix_reuses_exact_endpoint_without_source_decode():
    split, runtime, _ = fixture(rows=3)
    tensor, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        Policy(), None, split, device=torch.device("cpu"), amp_dtype=None,
        non_blocking=False, long_only=False, buy_fee_rate=0.001425,
        sell_fee_rate=0.002925, max_turnover_ratio=0.0, gross_leverage=1.0,
        min_trade_weight=0.0, model_chunk_rows=2, backtest_chunk_rows=2,
        portfolio_activation="pre_normalized", max_volume_participation=0.5,
        volume_participation_equity=10_000_000.0, execution_runtime=runtime,
    )
    recorded = tensor.to_numpy()
    original = runtime.day_trade_carry_source

    def unexpected_decode(row):
        raise AssertionError("full owned prefix must not re-decode the source")

    source = replace(original, sessions=(), session_loader=unexpected_decode)
    current = replace(runtime, day_trade_carry_source=source)
    actual = trainer._replay_physical_carry_split_prefix(
        recorded, split, len(split), runtime=current, config=inference_config(),
    )
    for name in ("minute_nav", "shares_history", "strategy_returns", "requested_weights_history"):
        np.testing.assert_array_equal(getattr(actual, name), getattr(recorded, name))
        assert not np.shares_memory(getattr(actual, name), getattr(recorded, name))
    assert actual.day_trade_carry_state.inventory.cohorts.data_ptr() != recorded.day_trade_carry_state.inventory.cohorts.data_ptr()
    wrong = replace(recorded, day_trade_carry_state=replace(
        recorded.day_trade_carry_state, last_session_day=original.day_at(0),
    ))
    with pytest.raises(ValueError, match="exact recorded endpoint"):
        trainer._replay_physical_carry_split_prefix(
            wrong, split, len(split), runtime=current, config=inference_config(),
        )


def test_real_neural_inference_writes_physical_archive_reports_and_stitched_account(tmp_path):
    panel = physical_panel()
    source = panel.day_trade_carry_source
    cache = _PhysicalSessionCache(64 * 1024**2)
    panel.day_trade_carry_source = replace(
        source, sessions=(),
        session_loader=lambda row: cache.get_or_load("dense", row, lambda: source.session_at(row)),
        runtime_cache_info=cache.snapshot,
    )
    config = inference_config()
    trained = subset_panel_symbols(panel, ["0050", "1101"])
    fold = WalkForwardFold(
        fold_id=1, train_indices=np.array([0]), val_indices=np.array([1, 2]),
        test_indices=np.array([3, 4, 5]), train_years=[2022],
        val_years=[2023], test_years=[2024],
    )
    model = trainer.build_model(
        config=config, lookback=1, num_features=2, num_symbols=2,
        feature_names=trained.feature_names,
    )
    with torch.no_grad():
        for parameter in model.parameters():
            parameter.zero_()
        list(model.parameters())[-1].fill_(0.21)
    fold_dir = trainer._fold_dir(tmp_path, 1)
    fold_dir.mkdir(parents=True)
    checkpoint_path = trainer._best_checkpoint_path(fold_dir)
    torch.save({
        "model_state_dict": model.state_dict(), "best_val_loss": 0.0,
        "fold_id": fold.fold_id, "train_years": fold.train_years,
        "val_years": fold.val_years, "test_years": fold.test_years,
        "experiment_manifest": trainer._checkpoint_manifest(trained, config),
    }, checkpoint_path)
    checkpoint_hash = hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()
    results = trainer.run_inference(panel, [fold], config, tmp_path)
    assert len(results) == 1
    assert hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() == checkpoint_hash
    assert not trainer._model_path(fold_dir).exists()  # inference never rewrites weights
    context = DayTradeCarryArtifactContext(
        tuple(trained.symbols), trained.day_trade_carry_source.release_id,
        10_000_000.0, 10_000_000.0,
    )
    for name in ("test_backtest.npz", "deployment_test_backtest.npz"):
        # The root stitched account expands the checkpoint's requested actions
        # into the full-panel universe; its source context must reflect that
        # account, not relabel it as the smaller reset-state fold diagnostic.
        artifact_context = (
            context if name == "test_backtest.npz"
            else replace(context, universe=tuple(panel.symbols))
        )
        result, dates = trainer._load_backtest_artifact(
            fold_dir / name, day_trade_carry_context=artifact_context,
        )
        assert result.settlement_ledger_unit == "currency"
        assert result.minute_nav.shape == (3, 270)
        assert result.day_trade_carry_state.last_session_day == date(2024, 1, 7).toordinal()
        assert dates.tolist() == panel.dates[3:].tolist()
        if name == "deployment_test_backtest.npz":
            np.testing.assert_array_equal(result.requested_weights_history[:, 1], 0.0)
            np.testing.assert_array_equal(result.shares_history[:, 1], 0.0)
        with np.load(fold_dir / name, allow_pickle=False) as archive:
            assert int(archive["artifact_schema_version"]) == 8
    for name in ("save_timing.json", "plot_timing.json", "mode_artifact_contract.json",
                 "fold_complete.json", "settlement_audit_summary.json"):
        assert (fold_dir / name).is_file(), name
    timing = json.loads((fold_dir / "save_timing.json").read_text())
    assert timing["model_checkpoint_written"] == 0
    assert timing["backtest_npz_s"] >= 0
    cache_receipt = json.loads((fold_dir / "physical_source_cache.json").read_text())
    assert cache_receipt["release_id"] == source.release_id
    assert cache_receipt["scope"] == "rank_local_shared_source_lifetime_before_fold_reporting"
    assert cache_receipt["representations"]["dense"]["loads"] > 0
    assert 0 < cache_receipt["peak_retained_tensor_bytes"] <= cache_receipt["maximum_bytes"]
    assert json.loads(trainer._test_symbols_path(fold_dir).read_text()) == ["0050", "1101"]
