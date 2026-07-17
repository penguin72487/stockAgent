from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from torch import nn

from stockagent.backtest.simulator import BacktestResult
from stockagent.backtest.tw_integer_execution import TaiwanIntegerState
from stockagent.training.trainer import (
    FoldResult,
    _load_backtest_artifact,
    _prefix_backtest_result,
    _save_backtest_artifact,
    _save_fold_output_artifacts,
    _save_settlement_audit_artifacts,
    _slice_backtest_rows,
)


def _integer_result() -> BacktestResult:
    payables = np.asarray(
        [[0.0, 12.345678901234], [12.345678901234, 0.0]], dtype=np.float64
    )
    receivables = np.asarray(
        [[0.0, 0.0], [0.0, 7.654321098765]], dtype=np.float64
    )
    cash = np.asarray(
        [100_000_000.12500001, 99_999_999.87500001], dtype=np.float64
    )
    state = TaiwanIntegerState(
        mode="tw_cash",
        settled_cash=float(cash[-1]),
        holdings=np.asarray([3, 4], dtype=np.int64),
        payable_queue=payables[-1].copy(),
        receivable_queue=receivables[-1].copy(),
        last_nav=100_000_019.7654321,
        alive=True,
        last_weights=np.asarray([0.3, 0.7], dtype=np.float64),
        short_sale_collateral=np.zeros(2, dtype=np.float64),
        short_margin_collateral=np.zeros(2, dtype=np.float64),
    )
    return BacktestResult(
        strategy_returns=np.asarray([1.0e-10, -2.0e-10], dtype=np.float64),
        benchmark_returns=np.asarray([3.0e-10, 4.0e-10], dtype=np.float64),
        turnovers=np.asarray([0.1234567890123, 0.9876543210987], dtype=np.float64),
        weights_history=np.asarray([[0.4, 0.6], [0.3, 0.7]], dtype=np.float64),
        execution_mode="tw_cash",
        settlement_ledger_unit="currency",
        requested_weights_history=np.asarray(
            [[0.45, 0.55], [0.25, 0.75]], dtype=np.float64
        ),
        cash_history=cash,
        payables_history=payables,
        receivables_history=receivables,
        settlement_default=np.asarray([False, False], dtype=np.bool_),
        final_weights=np.asarray([0.3, 0.7], dtype=np.float64),
        final_cash=np.asarray(cash[-1], dtype=np.float64),
        final_payables=payables[-1].copy(),
        final_receivables=receivables[-1].copy(),
        final_alive=np.asarray(True, dtype=np.bool_),
        shares_history=np.asarray([[2, 3], [3, 4]], dtype=np.int64),
        short_sale_collateral_history=np.zeros((2, 2), dtype=np.float64),
        short_margin_collateral_history=np.zeros((2, 2), dtype=np.float64),
        final_short_sale_collateral=np.zeros(2, dtype=np.float64),
        final_short_margin_collateral=np.zeros(2, dtype=np.float64),
        final_integer_state=state,
    )


def _continuous_result() -> BacktestResult:
    return BacktestResult(
        strategy_returns=np.asarray([-0.01, 0.02], dtype=np.float32),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        turnovers=np.asarray([0.2, 0.1], dtype=np.float32),
        weights_history=np.asarray([[0.5, 0.5], [0.4, 0.6]], dtype=np.float32),
        execution_mode="tw_cash",
        settlement_ledger_unit="nav_ratio",
        requested_weights_history=np.asarray(
            [[0.55, 0.45], [0.35, 0.65]], dtype=np.float32
        ),
        cash_history=np.asarray([0.5, 0.4], dtype=np.float32),
        payables_history=np.asarray([[0.0, 0.5], [0.5, 0.0]], dtype=np.float32),
        receivables_history=np.zeros((2, 2), dtype=np.float32),
        settlement_default=np.zeros(2, dtype=np.bool_),
        equity_scale_history=np.asarray(
            [np.exp(-0.01), np.exp(0.01)], dtype=np.float32
        ),
        final_weights=np.asarray([0.4, 0.6], dtype=np.float32),
        final_cash=np.asarray(0.4, dtype=np.float32),
        final_payables=np.asarray([0.5, 0.0], dtype=np.float32),
        final_receivables=np.asarray([0.0, 0.0], dtype=np.float32),
        final_alive=np.asarray(True, dtype=np.bool_),
        final_equity_scale=np.asarray(np.exp(0.01), dtype=np.float32),
    )


def test_integer_backtest_npz_round_trip_preserves_precision_units_and_terminal_state(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    path = tmp_path / "integer.npz"

    _save_backtest_artifact(path, result, dates)
    loaded, loaded_dates = _load_backtest_artifact(path)

    assert loaded.execution_mode == "tw_cash"
    assert loaded.settlement_ledger_unit == "currency"
    assert loaded.cash_history is not None
    assert loaded.cash_history.dtype == np.float64
    np.testing.assert_array_equal(loaded.cash_history, result.cash_history)
    np.testing.assert_array_equal(loaded.payables_history, result.payables_history)
    np.testing.assert_array_equal(loaded.receivables_history, result.receivables_history)
    np.testing.assert_array_equal(
        loaded.requested_weights_history, result.requested_weights_history
    )
    np.testing.assert_array_equal(loaded_dates, dates)
    assert loaded.final_integer_state is not None
    assert loaded.final_integer_state.last_nav == result.final_integer_state.last_nav  # type: ignore[union-attr]
    np.testing.assert_array_equal(
        loaded.final_integer_state.holdings,
        result.final_integer_state.holdings,  # type: ignore[union-attr]
    )
    np.testing.assert_array_equal(
        loaded.final_integer_state.last_weights,
        result.final_integer_state.last_weights,  # type: ignore[union-attr]
    )
    with np.load(path, allow_pickle=False) as archive:
        assert int(archive["artifact_schema_version"].item()) == 4
        assert archive["settlement_ledger_unit"].item() == "currency"
        assert int(archive["payable_queue_sessions"].item()) == 2
        assert int(archive["receivable_queue_sessions"].item()) == 2
        np.testing.assert_array_equal(
            archive["integer_state_last_weights"], result.final_weights
        )


def test_schema_four_round_trip_preserves_longer_dividend_receivable_queue(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    receivables = np.asarray(
        [[0.0, 0.0, 100.0, 0.0], [0.0, 7.0, 0.0, 100.0]],
        dtype=np.float64,
    )
    result.receivables_history = receivables
    result.final_receivables = receivables[-1].copy()
    state = result.final_integer_state
    assert state is not None
    result.final_integer_state = TaiwanIntegerState(
        mode=state.mode,
        settled_cash=state.settled_cash,
        holdings=state.holdings.copy(),
        payable_queue=state.payable_queue.copy(),
        receivable_queue=receivables[-1].copy(),
        last_nav=state.last_nav,
        alive=state.alive,
        last_weights=state.last_weights.copy(),
        short_sale_collateral=state.short_sale_collateral.copy(),
        short_margin_collateral=state.short_margin_collateral.copy(),
    )
    path = tmp_path / "long-dividend-queue.npz"
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")

    _save_backtest_artifact(path, result, dates)
    loaded, _ = _load_backtest_artifact(path)

    assert loaded.payables_history is not None
    assert loaded.receivables_history is not None
    assert loaded.payables_history.shape == (2, 2)
    assert loaded.receivables_history.shape == (2, 4)
    np.testing.assert_array_equal(loaded.receivables_history, receivables)
    assert loaded.final_integer_state is not None
    assert loaded.final_integer_state.payable_queue.shape == (2,)
    assert loaded.final_integer_state.receivable_queue.shape == (4,)
    with np.load(path, allow_pickle=False) as archive:
        assert int(archive["artifact_schema_version"].item()) == 4
        assert int(archive["payable_queue_sessions"].item()) == 2
        assert int(archive["receivable_queue_sessions"].item()) == 4


def test_schema_two_integer_artifact_without_state_weights_upgrades_losslessly(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    current_path = tmp_path / "current.npz"
    legacy_path = tmp_path / "legacy-schema-two.npz"
    _save_backtest_artifact(current_path, result, dates)
    with np.load(current_path, allow_pickle=False) as archive:
        legacy_payload = {
            key: np.asarray(archive[key]).copy()
            for key in archive.files
            if key
            not in {
                "integer_state_last_weights",
                "integer_state_short_sale_collateral",
                "integer_state_short_margin_collateral",
                "short_sale_collateral_history",
                "short_margin_collateral_history",
                "final_short_sale_collateral",
                "final_short_margin_collateral",
            }
        }
    legacy_payload["artifact_schema_version"] = np.asarray(2, dtype=np.int64)
    np.savez(legacy_path, **legacy_payload)

    loaded, _ = _load_backtest_artifact(legacy_path)

    assert loaded.final_integer_state is not None
    np.testing.assert_array_equal(
        loaded.final_integer_state.last_weights, loaded.final_weights
    )


def test_schema_three_round_trip_preserves_margin_short_collateral(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    holdings = np.asarray([3, -4], dtype=np.int64)
    sale_history = np.asarray([[0.0, 20.0], [0.0, 40.0]], dtype=np.float64)
    margin_history = np.asarray([[0.0, 18.0], [0.0, 36.0]], dtype=np.float64)
    final_weights = np.asarray([0.3, -0.7], dtype=np.float64)
    state = result.final_integer_state
    assert state is not None
    result.final_integer_state = TaiwanIntegerState(
        mode=state.mode,
        settled_cash=state.settled_cash,
        holdings=holdings,
        payable_queue=state.payable_queue.copy(),
        receivable_queue=state.receivable_queue.copy(),
        last_nav=state.last_nav,
        alive=state.alive,
        last_weights=final_weights.copy(),
        short_sale_collateral=sale_history[-1].copy(),
        short_margin_collateral=margin_history[-1].copy(),
    )
    result.shares_history = np.asarray([[2, -2], [3, -4]], dtype=np.int64)
    result.weights_history = np.asarray(
        [[0.4, -0.4], final_weights], dtype=np.float64
    )
    result.final_weights = final_weights
    result.short_sale_collateral_history = sale_history
    result.short_margin_collateral_history = margin_history
    result.final_short_sale_collateral = sale_history[-1].copy()
    result.final_short_margin_collateral = margin_history[-1].copy()
    path = tmp_path / "margin-short.npz"

    _save_backtest_artifact(
        path,
        result,
        np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
    )
    loaded, _ = _load_backtest_artifact(path)

    np.testing.assert_array_equal(
        loaded.short_sale_collateral_history, sale_history
    )
    np.testing.assert_array_equal(
        loaded.short_margin_collateral_history, margin_history
    )
    np.testing.assert_array_equal(
        loaded.final_short_sale_collateral, sale_history[-1]
    )
    np.testing.assert_array_equal(
        loaded.final_short_margin_collateral, margin_history[-1]
    )
    assert loaded.final_integer_state is not None
    np.testing.assert_array_equal(
        loaded.final_integer_state.short_sale_collateral, sale_history[-1]
    )
    np.testing.assert_array_equal(
        loaded.final_integer_state.short_margin_collateral, margin_history[-1]
    )


def test_schema_three_normalizes_omitted_tw_cash_collateral_to_zero(
    tmp_path: Path,
) -> None:
    result = _continuous_result()
    path = tmp_path / "long-only-placeholder.npz"

    _save_backtest_artifact(
        path,
        result,
        np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
    )
    loaded, _ = _load_backtest_artifact(path)

    np.testing.assert_array_equal(
        loaded.short_sale_collateral_history, np.zeros((2, 2), dtype=np.float32)
    )
    np.testing.assert_array_equal(
        loaded.short_margin_collateral_history, np.zeros((2, 2), dtype=np.float32)
    )
    np.testing.assert_array_equal(
        loaded.final_short_sale_collateral, np.zeros(2, dtype=np.float32)
    )
    np.testing.assert_array_equal(
        loaded.final_short_margin_collateral, np.zeros(2, dtype=np.float32)
    )


def test_schema_three_rejects_zero_inference_for_negative_tw_cash_positions(
    tmp_path: Path,
) -> None:
    result = _continuous_result()
    result.weights_history[0, 0] = -0.5

    with pytest.raises(ValueError, match="negative realised positions"):
        _save_backtest_artifact(
            tmp_path / "invalid-short-placeholder.npz",
            result,
            np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
        )


def test_settlement_audit_keeps_each_due_session_column_and_declares_units(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    result.short_sale_collateral_history = np.asarray(
        [[1.0, 2.0], [3.0, 4.0]], dtype=np.float64
    )
    result.short_margin_collateral_history = np.asarray(
        [[5.0, 6.0], [7.0, 8.0]], dtype=np.float64
    )
    result.final_short_sale_collateral = np.asarray([3.0, 4.0])
    result.final_short_margin_collateral = np.asarray([7.0, 8.0])
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    base = tmp_path / "settlement_audit"

    _save_settlement_audit_artifacts(
        base,
        result,
        dates,
        table_output_format="csv",
    )

    with base.with_suffix(".csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["execution_mode"] == "tw_cash"
    assert rows[0]["settlement_ledger_unit"] == "currency"
    assert rows[0]["payable_t_plus_1"] == "0.0"
    assert float(rows[0]["payable_t_plus_2"]) == pytest.approx(12.345678901234)
    assert float(rows[1]["payable_t_plus_1"]) == pytest.approx(12.345678901234)
    assert rows[1]["payable_t_plus_2"] == "0.0"
    assert float(rows[0]["short_sale_collateral_total"]) == pytest.approx(3.0)
    assert float(rows[1]["short_sale_collateral_total"]) == pytest.approx(7.0)
    assert float(rows[0]["short_margin_collateral_total"]) == pytest.approx(11.0)
    assert float(rows[1]["short_margin_collateral_total"]) == pytest.approx(15.0)
    summary = json.loads(
        (tmp_path / "settlement_audit_summary.json").read_text(encoding="utf-8")
    )
    assert summary["execution_mode"] == "tw_cash"
    assert summary["settlement_ledger_unit"] == "currency"
    assert summary["payable_queue_sessions"] == 2
    assert summary["receivable_queue_sessions"] == 2
    assert summary["final_short_sale_collateral_total"] == pytest.approx(7.0)
    assert summary["final_short_margin_collateral_total"] == pytest.approx(15.0)


def test_settlement_audit_keeps_longer_receivable_claim_queue(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    result.receivables_history = np.asarray(
        [[0.0, 0.0, 100.0, 0.0], [0.0, 7.0, 0.0, 100.0]],
        dtype=np.float64,
    )
    result.final_receivables = result.receivables_history[-1].copy()
    base = tmp_path / "settlement_audit_long_claim"

    _save_settlement_audit_artifacts(
        base,
        result,
        np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
        table_output_format="csv",
    )

    with base.with_suffix(".csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert "payable_t_plus_3" not in rows[0]
    assert float(rows[0]["receivable_t_plus_3"]) == pytest.approx(100.0)
    assert float(rows[1]["receivable_t_plus_4"]) == pytest.approx(100.0)
    summary = json.loads(
        (tmp_path / "settlement_audit_long_claim_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["payable_queue_sessions"] == 2
    assert summary["receivable_queue_sessions"] == 4


def test_settlement_audit_rejects_aggregate_claim_history_that_loses_due_offset(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    result.payables_history = np.asarray([12.0, 0.0], dtype=np.float64)
    result.receivables_history = np.asarray([0.0, 7.0], dtype=np.float64)

    with pytest.raises(ValueError, match="due-session"):
        _save_settlement_audit_artifacts(
            tmp_path / "audit",
            result,
            np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
            table_output_format="csv",
        )


def test_prefix_drops_wrong_terminal_state_but_full_copy_is_independent() -> None:
    result = _integer_result()

    partial = _prefix_backtest_result(result, 1)
    assert partial.settlement_ledger_unit == "currency"
    assert partial.cash_history is not None and partial.cash_history.shape == (1,)
    assert partial.payables_history is not None and partial.payables_history.shape == (1, 2)
    assert partial.final_weights is None
    assert partial.final_cash is None
    assert partial.final_payables is None
    assert partial.final_receivables is None
    assert partial.final_alive is None
    assert partial.final_integer_state is None

    full = _prefix_backtest_result(result, 2)
    assert full.final_integer_state is not None
    assert full.final_integer_state is not result.final_integer_state
    assert not np.shares_memory(
        full.final_integer_state.holdings,
        result.final_integer_state.holdings,  # type: ignore[union-attr]
    )
    assert not np.shares_memory(
        full.final_integer_state.last_weights,
        result.final_integer_state.last_weights,  # type: ignore[union-attr]
    )
    np.testing.assert_array_equal(full.final_weights, result.final_weights)
    np.testing.assert_array_equal(
        full.final_short_sale_collateral, result.final_short_sale_collateral
    )
    np.testing.assert_array_equal(
        full.final_short_margin_collateral, result.final_short_margin_collateral
    )
    assert full.final_integer_state.short_sale_collateral is not None
    assert not np.shares_memory(
        full.final_integer_state.short_sale_collateral,
        result.final_integer_state.short_sale_collateral,  # type: ignore[union-attr]
    )


def test_slice_copies_short_collateral_and_only_keeps_true_terminal_state() -> None:
    result = _integer_result()

    partial = _slice_backtest_rows(
        result, 0, 1, preserve_terminal_state=True
    )
    assert partial.short_sale_collateral_history is not None
    assert partial.short_sale_collateral_history.shape == (1, 2)
    assert partial.short_margin_collateral_history is not None
    assert partial.final_short_sale_collateral is None
    assert partial.final_short_margin_collateral is None
    assert partial.final_integer_state is None

    terminal = _slice_backtest_rows(
        result, 1, 2, preserve_terminal_state=True
    )
    np.testing.assert_array_equal(
        terminal.final_short_sale_collateral,
        result.final_short_sale_collateral,
    )
    np.testing.assert_array_equal(
        terminal.final_short_margin_collateral,
        result.final_short_margin_collateral,
    )
    assert terminal.final_integer_state is not None
    assert not np.shares_memory(
        terminal.final_integer_state.short_margin_collateral,
        result.final_integer_state.short_margin_collateral,  # type: ignore[union-attr]
    )


def test_fold_output_uses_integer_oracle_as_canonical_taiwan_artifact(
    tmp_path: Path,
) -> None:
    continuous = _continuous_result()
    integer = _integer_result()
    fold_result = FoldResult(
        fold_id=1,
        train_years=[2023],
        val_years=[2024],
        test_years=[2025],
        best_val_loss=0.0,
        val_ic={},
        val_metrics={},
        test_ic={},
        test_metrics={"cumulative_return": -123.0},
    )
    config = SimpleNamespace(
        trading=SimpleNamespace(execution_mode="tw_cash"),
        training=SimpleNamespace(
            table_output_format="csv",
            save_daily_weights_table=False,
            save_integer_share_daily_weights_table=False,
            save_integer_share_holdings_table=False,
            backtest_artifact_compression="none",
        ),
    )
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")

    _save_fold_output_artifacts(
        fold_dir=tmp_path,
        fold_result=fold_result,
        model=nn.Linear(2, 1),
        test_backtest=continuous,
        test_dates=dates,
        symbols=["A", "B"],
        config=config,  # type: ignore[arg-type]
        test_integer_backtest=integer,
        print_report=False,
        write_plots=False,
    )

    canonical, _ = _load_backtest_artifact(tmp_path / "test_backtest.npz")
    surrogate, _ = _load_backtest_artifact(
        tmp_path / "test_backtest_continuous_surrogate.npz"
    )
    assert canonical.settlement_ledger_unit == "currency"
    assert canonical.final_integer_state is not None
    np.testing.assert_array_equal(canonical.strategy_returns, integer.strategy_returns)
    assert surrogate.settlement_ledger_unit == "nav_ratio"
    np.testing.assert_array_equal(
        surrogate.strategy_returns, continuous.strategy_returns
    )
    np.testing.assert_array_equal(
        surrogate.equity_scale_history, continuous.equity_scale_history
    )
    np.testing.assert_array_equal(
        surrogate.final_equity_scale, continuous.final_equity_scale
    )
    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["test_continuous_surrogate_metrics"] == {
        "cumulative_return": -123.0
    }
    assert metrics["test_metrics"] == metrics["test_integer_metrics"]


def test_taiwan_artifact_requires_explicit_ledger_units(tmp_path: Path) -> None:
    result = _continuous_result()
    result.settlement_ledger_unit = None
    with pytest.raises(ValueError, match="settlement_ledger_unit"):
        _save_backtest_artifact(
            tmp_path / "invalid.npz",
            result,
            np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
        )


def test_integer_artifact_rejects_inconsistent_or_partial_terminal_state(
    tmp_path: Path,
) -> None:
    result = _integer_result()
    result.final_cash = np.asarray(
        float(result.final_integer_state.settled_cash) + 1.0,  # type: ignore[union-attr]
        dtype=np.float64,
    )
    dates = np.asarray(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    with pytest.raises(ValueError, match="disagree"):
        _save_backtest_artifact(tmp_path / "inconsistent.npz", result, dates)

    np.savez(
        tmp_path / "partial.npz",
        artifact_schema_version=np.asarray(2, dtype=np.int64),
        execution_mode=np.asarray("tw_cash", dtype="U32"),
        settlement_ledger_unit=np.asarray("currency", dtype="U16"),
        strategy_returns=np.zeros(1, dtype=np.float64),
        benchmark_returns=np.zeros(1, dtype=np.float64),
        turnovers=np.zeros(1, dtype=np.float64),
        weights_history=np.zeros((1, 1), dtype=np.float64),
        dates=np.asarray(["2026-01-02"], dtype="datetime64[D]"),
        cash_history=np.ones(1, dtype=np.float64),
        payables_history=np.zeros((1, 2), dtype=np.float64),
        receivables_history=np.zeros((1, 2), dtype=np.float64),
        settlement_default=np.zeros(1, dtype=np.bool_),
        integer_state_mode=np.asarray("tw_cash", dtype="U32"),
    )
    with pytest.raises(ValueError, match="incomplete integer terminal state"):
        _load_backtest_artifact(tmp_path / "partial.npz")
