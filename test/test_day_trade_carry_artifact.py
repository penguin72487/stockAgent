from dataclasses import replace
from datetime import date
import csv
import json

import numpy as np
import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_day_trade_carry import _compact_detached_carry_state
from stockagent.training.day_trade_carry_artifact import DayTradeCarryArtifactContext
from stockagent.training.trainer import (
    _atomic_numpy_archive_save, _load_backtest_artifact, _save_backtest_artifact,
    _prefix_backtest_result, _save_settlement_audit_artifacts,
    _slice_backtest_rows,
)
from test_tw_day_trade_carry import DAY, assert_state, canonical_kwargs, session, v


CONTEXT = DayTradeCarryArtifactContext(
    ("2330",), "test-exact-public-minute-actions-release", 10_000_000., 10_000_000.)


def simulate(weights, sessions, initial=None):
    weights = v(*weights).reshape(-1, 1)
    return run_backtest_torch(weights, torch.zeros_like(weights), torch.ones_like(weights),
        torch.zeros(len(sessions)), **canonical_kwargs(weights, sessions),
        initial_day_trade_carry_state=initial).to_numpy()


def dates(sessions):
    return np.array([date.fromordinal(s.day) for s in sessions], dtype="datetime64[D]")


@pytest.mark.parametrize("compression", ["none", "compressed"])
@pytest.mark.parametrize("sign", [1., -1.])
def test_canonical_roundtrip_retains_claims_fifo_and_exact_continuation(tmp_path, compression, sign):
    action = replace(session(DAY + 1, 2000, volume=0), action_mask=v(1), share_ratio=v(.5),
        cash_per_old_share=v(1), payment_day=v(DAY + 4))
    sessions = [session(), action, session(DAY + 4, 2000, exits=True)]
    full = simulate([sign * .21] * 3, sessions)
    prefix = simulate([sign * .21] * 2, sessions[:2])
    target = tmp_path / "test_backtest.npz"
    _save_backtest_artifact(target, prefix, dates(sessions[:2]), compression=compression,
        day_trade_carry_context=CONTEXT)
    loaded, loaded_dates = _load_backtest_artifact(target, day_trade_carry_context=CONTEXT)
    np.testing.assert_array_equal(loaded_dates, dates(sessions[:2]))
    assert_state(loaded.day_trade_carry_state, prefix.day_trade_carry_state)
    assert loaded.day_trade_carry_state.inventory.claims[..., 0].sum() == sign * 2000
    for field in ("minute_nav", "strategy_returns", "weights_history", "shares_history"):
        np.testing.assert_array_equal(getattr(loaded, field), getattr(prefix, field))
    tail = simulate([sign * .21], sessions[2:], loaded.day_trade_carry_state)
    for field in ("minute_nav", "strategy_returns", "weights_history", "shares_history"):
        np.testing.assert_array_equal(np.concatenate([getattr(loaded, field), getattr(tail, field)]),
            getattr(full, field))
    # A chunk boundary may discard already consumed FIFO rows.  The canonical
    # active inventory and every cash claim must still match exactly.
    assert_state(
        _compact_detached_carry_state(full.day_trade_carry_state),
        _compact_detached_carry_state(tail.day_trade_carry_state),
    )
    context = replace(CONTEXT, initial_nav=loaded.day_trade_carry_state.last_nav.item())
    _save_backtest_artifact(tmp_path / "tail.npz", tail, dates(sessions[2:]), day_trade_carry_context=context)
    restored, _ = _load_backtest_artifact(tmp_path / "tail.npz", day_trade_carry_context=context)
    assert_state(restored.day_trade_carry_state, tail.day_trade_carry_state)
    with np.load(target, allow_pickle=False) as data:
        assert data["artifact_schema_version"] == 8
        assert "payable_queue_sessions" not in data.files
        assert data["carry_inventory_cohorts"].dtype == np.float64


def test_physical_fifo_audit_never_fabricates_tplus_cash_queues(tmp_path):
    action = replace(
        session(DAY + 1, 2000, volume=0),
        action_mask=v(1),
        share_ratio=v(1),
        cash_per_old_share=v(10),
        payment_day=v(DAY + 5),
    )
    sessions = [session(), action]
    result = simulate([0.21, 0.21], sessions)
    base = tmp_path / "settlement_audit"

    _save_settlement_audit_artifacts(
        base, result, dates(sessions), table_output_format="csv"
    )

    with base.with_suffix(".csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[-1]["audit_kind"] == "physical_fifo_margin_carry"
    assert "payable_t_plus_1" not in rows[-1]
    assert float(rows[-1]["close_nav"]) == pytest.approx(
        result.minute_nav[-1, -1]
    )
    summary = json.loads(
        base.with_name("settlement_audit_summary.json").read_text(encoding="utf-8")
    )
    assert summary["cash_settlement_queues_applicable"] is False
    assert summary["outstanding_cash_claim_count"] == 1
    assert summary["corporate_action_receivable"] == pytest.approx(20_000)
    assert summary["final_nav"] == pytest.approx(result.minute_nav[-1, -1])


def test_financial_default_and_post_default_segment_survive_archive(tmp_path):
    sessions = [session(volume=20_000), session(DAY + 1, 3000), session(DAY + 2)]
    result = simulate([-.9, .9, .9], sessions)
    assert not result.day_trade_carry_state.alive
    path = tmp_path / "default.npz"
    _save_backtest_artifact(path, result, dates(sessions), day_trade_carry_context=CONTEXT)
    loaded, _ = _load_backtest_artifact(path, day_trade_carry_context=CONTEXT)
    assert_state(loaded.day_trade_carry_state, result.day_trade_carry_state)
    tail_session = session(DAY + 3)
    tail = simulate([.9], [tail_session], loaded.day_trade_carry_state)
    dead_context = replace(CONTEXT, initial_nav=0.)
    _save_backtest_artifact(path, tail, dates([tail_session]), day_trade_carry_context=dead_context)
    restored, _ = _load_backtest_artifact(path, day_trade_carry_context=dead_context)
    np.testing.assert_array_equal(restored.minute_nav, 0.)


@pytest.mark.parametrize("change,match", [
    ({"universe": ("0050",)}, "universe/release"),
    ({"release_id": "different-release"}, "universe/release"),
    ({"initial_nav": 1_000_000.}, "initial NAV"),
    ({"initial_capital": 1_000_000.}, "capital"),
])
def test_load_requires_callers_exact_context(tmp_path, change, match):
    result = simulate([.21], [session()])
    path = tmp_path / "result.npz"
    _save_backtest_artifact(path, result, dates([session()]), day_trade_carry_context=CONTEXT)
    with pytest.raises(ValueError, match="pinned source context"):
        _load_backtest_artifact(path)
    with pytest.raises(ValueError, match=match):
        _load_backtest_artifact(path, day_trade_carry_context=replace(CONTEXT, **change))


@pytest.mark.parametrize("name,transform,match", [
    ("carry_inventory_cohorts", lambda a: a.astype(np.float32), "float64"),
    ("carry_inventory_claims", lambda a: a.astype(np.float32), "float64"),
    ("carry_abi", lambda a: np.asarray("tw_day_trade_physical_fifo_sessions_v2"), "incompatible"),
    ("artifact_schema_version", lambda a: np.asarray(7), "legacy archive"),
    ("carry_last_session_day", lambda a: a + 1, "terminal session"),
    ("minute_nav", lambda a: a[:, :269], "shape"),
    ("strategy_returns", lambda a: a + .01, "daily return"),
    ("strategy_returns", lambda a: a.astype(np.float32), "float64"),
    ("shares_history", lambda a: a + 1, "terminal physical shares"),
    ("minute_nav", lambda a: np.full_like(a, np.nan), "finite"),
    ("settlement_default", lambda a: ~a, "default state"),
])
def test_corrupt_archive_never_silently_coerces_or_discards_state(tmp_path, name, transform, match):
    result = simulate([.21], [session()])
    path = tmp_path / "result.npz"
    _save_backtest_artifact(path, result, dates([session()]), day_trade_carry_context=CONTEXT)
    with np.load(path, allow_pickle=False) as data:
        payload = {key: np.array(data[key], copy=True) for key in data.files}
    payload[name] = transform(payload[name])
    np.savez(path, **payload)
    with pytest.raises((ValueError, RuntimeError), match=match):
        _load_backtest_artifact(path, day_trade_carry_context=CONTEXT)


def test_prefix_cannot_keep_future_inventory_or_drop_other_state(tmp_path):
    sessions = [session(), session(DAY + 1)]
    result = simulate([.21, .21], sessions)
    with pytest.raises(ValueError, match="terminal session"):
        _save_backtest_artifact(tmp_path / "prefix.npz", result, dates(sessions[:1]),
            day_trade_carry_context=CONTEXT)
    with pytest.raises(ValueError, match="cannot discard"):
        _save_backtest_artifact(tmp_path / "mixed.npz", replace(result, final_cash=np.asarray(10.)),
            dates(sessions), day_trade_carry_context=CONTEXT)
    with pytest.raises(ValueError, match="exact data release"):
        _save_backtest_artifact(tmp_path / "latest.npz", result, dates(sessions),
            day_trade_carry_context=replace(CONTEXT, release_id="latest"))
    assert not list(tmp_path.iterdir())


def test_canonical_slices_preserve_curves_and_require_exact_boundary_inventory(tmp_path):
    sessions = [session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)]
    result = simulate([.21, -.21, .1], sessions)
    full = _prefix_backtest_result(result, 3)
    assert_state(full.day_trade_carry_state, result.day_trade_carry_state)
    np.testing.assert_array_equal(full.minute_nav, result.minute_nav)
    assert full.day_trade_carry_state.inventory.cohorts.data_ptr() != result.day_trade_carry_state.inventory.cohorts.data_ptr()
    prefix = _prefix_backtest_result(result, 2)
    assert prefix.day_trade_carry_state is None  # cannot infer FIFO from shares
    np.testing.assert_array_equal(prefix.minute_nav, result.minute_nav[:2])
    with pytest.raises(ValueError, match="both inventory"):
        _save_backtest_artifact(tmp_path / "prefix.npz", prefix, dates(sessions[:2]),
            day_trade_carry_context=CONTEXT)
    exact_prefix = simulate([.21, -.21], sessions[:2])
    prefix = _prefix_backtest_result(result, 2,
        day_trade_carry_terminal_state=exact_prefix.day_trade_carry_state)
    path = tmp_path / "prefix.npz"
    _save_backtest_artifact(path, prefix, dates(sessions[:2]), day_trade_carry_context=CONTEXT)
    restored, _ = _load_backtest_artifact(path, day_trade_carry_context=CONTEXT)
    assert_state(restored.day_trade_carry_state, exact_prefix.day_trade_carry_state)
    with pytest.raises(ValueError, match="endpoint"):
        _prefix_backtest_result(result, 2, day_trade_carry_terminal_state=result.day_trade_carry_state)
    tail = _slice_backtest_rows(result, 2, 3, preserve_terminal_state=True)
    context = replace(CONTEXT, initial_nav=exact_prefix.day_trade_carry_state.last_nav.item())
    _save_backtest_artifact(tmp_path / "tail.npz", tail, dates(sessions[2:]), day_trade_carry_context=context)
    assert_state(tail.day_trade_carry_state, result.day_trade_carry_state)


@pytest.mark.parametrize("failure", ["write", "readback"])
def test_atomic_npz_failure_preserves_existing_archive(tmp_path, monkeypatch, failure):
    path = tmp_path / "result.npz"
    original = {"existing": np.arange(10.)}
    _atomic_numpy_archive_save(path, original, compression="none")
    original_bytes = path.read_bytes()
    def fail(*args, **kwargs):
        raise OSError("injected interrupted archive")
    if failure == "write":
        monkeypatch.setattr(np, "savez", fail)
    with pytest.raises(OSError, match="injected"):
        _atomic_numpy_archive_save(path, {"new": np.arange(3.)}, compression="none",
            validate=fail if failure == "readback" else None)
    assert path.read_bytes() == original_bytes
    assert list(tmp_path.iterdir()) == [path]
