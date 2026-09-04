from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

import stockagent.backtest.tw_futures_portfolio as futures_portfolio_module
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_futures_portfolio import (
    TW_FUTURES_PORTFOLIO_DEFAULT_FUNDING,
    TW_FUTURES_PORTFOLIO_DEFAULT_NONPOSITIVE_EQUITY,
    TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE,
    TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE,
    run_tw_futures_portfolio_integer_surrogate_torch,
    run_tw_futures_portfolio_integer_torch,
)
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
)
from stockagent.data.tw_stock_context_futures_portfolio import (
    TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION,
    TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_LEGACY_CONTRACT_VERSION,
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CURRENT_OPEN_CONTRACT_VERSION,
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION,
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION,
    TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS,
    TaiwanStockContextFuturesPortfolioDaily,
    attach_stock_context_futures_portfolio_daily,
    fixed_futures_slot_symbols,
)
from stockagent.models.cross_sectional_all_futures import (
    CrossSectionalAllFuturesModel,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.checkpoint_contract import (
    build_checkpoint_manifest,
    validate_checkpoint_manifest,
)
from stockagent.training.windowed import dataset_to_windowed_tensors
from stockagent.training.trainer import (
    _split_recurrent_symbol_count,
    _split_uses_recurrent_futures_equity_scale,
)


def _stock_panel(rows: int = 3, symbols: int = 2) -> PanelData:
    dates = np.asarray(
        [np.datetime64("2026-01-02") + np.timedelta64(idx, "D") for idx in range(rows)],
        dtype="datetime64[D]",
    )
    shape = (rows, symbols)
    mask = np.ones(shape, dtype=bool)
    prices = np.full(shape, 100.0, dtype=np.float32)
    return PanelData(
        dates=dates,
        symbols=[f"S{idx}" for idx in range(symbols)],
        feature_names=["f0", "f1", "f2"],
        features=np.zeros((*shape, 3), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=prices.copy(),
        daily_volumes=np.ones(shape, dtype=np.float32),
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        force_short_cover_mask=np.zeros(shape, dtype=bool),
        force_exit_mask=np.zeros(shape, dtype=bool),
        open_prices=prices.copy(),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _integer_execution(
    simple_returns: torch.Tensor,
    *,
    group_indices: torch.Tensor | None = None,
    executable: bool = True,
) -> torch.Tensor:
    rows, slots = simple_returns.shape
    execution = torch.zeros((rows, slots, 11), dtype=torch.float32)
    execution[..., 0] = torch.log1p(simple_returns)
    execution[..., 1] = float(executable)
    execution[..., 3] = 100_000.0
    execution[..., 4] = 100_000.0 * (1.0 + simple_returns)
    execution[..., 5:8] = 0.0
    execution[..., 8] = 100.0 if executable else 0.0
    execution[..., 9] = (
        torch.arange(slots, dtype=torch.float32)[None, :]
        if group_indices is None
        else group_indices.to(dtype=torch.float32)[None, :]
    )
    execution[..., 10] = 0.0
    return execution


def test_attach_uses_only_prior_futures_features_and_separate_action_axis(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    session_dates = [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)]
    for idx, session_date in enumerate(session_dates):
        row: dict[str, object] = {
            "date": session_date,
            "product": "TX",
            "symbol": "TAIFEX_SLOT_0001",
            "tenor_rank": 1,
            "open": 100.0 + idx,
            "close": 100.5 + idx,
            "volume": 10 if idx != 1 else 0,
            "holding_log_return": 0.01 * (idx + 1),
            "executable": idx != 1,
            "must_liquidate": idx == 2,
            "can_hold_overnight": idx < 2,
            "same_contract_as_previous_session": idx > 0,
            "contract_multiplier": 200.0,
            "sinopac_network_fee_group": "large",
            "underlying_symbol": "S0",
            "contract": "202601",
            "physical_contract": "TX202601",
            "asset_class": "index_future",
            "previous_volume": 10.0,
            "previous_settlement": 99.0 + idx if idx < 2 else None,
        }
        for feature_idx, name in enumerate(FUTURES_MODEL_FEATURE_COLUMNS):
            row[name] = (
                7 if name == "taifex_product_id" else 100.0 * idx + feature_idx
            )
        rows.append(row)
    data_path = tmp_path / "continuous_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows), data_path)
    manifest = {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "feature_contract_version": TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        "fixed_model_output_slots": TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        "outputs": {"continuous_daily": {"sha256": _sha256(data_path)}},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
        integer_contracts=True,
        integer_fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    )
    daily = panel.stock_context_futures_portfolio_daily
    assert daily is not None
    assert panel.num_symbols == 2
    assert len(daily.symbols) == TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT
    assert not daily.candidate_mask[0].any()
    assert daily.candidate_mask[1, 0]
    assert daily.candidate_mask[2, 0]
    # Date 1 receives date 0's completed feature, never date 1's outcome.
    settlement_feature = FUTURES_MODEL_FEATURE_COLUMNS.index(
        "taifex_settlement_logret_1d"
    )
    assert daily.candidate_features[1, 0, settlement_feature] == pytest.approx(1.0)
    assert daily.candidate_features[2, 0, settlement_feature] == pytest.approx(101.0)
    assert daily.candidate_features.shape[-1] == len(
        TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
    )
    # The structural routing key is also lagged with the futures token. It is
    # consumed only as an integer gather index, never as a continuous feature.
    underlying_index = len(FUTURES_MODEL_FEATURE_COLUMNS)
    assert daily.candidate_features[1, 0, underlying_index] == pytest.approx(0.0)
    denomination_start = len(TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS)
    assert len(TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS) == 3
    assert daily.integer_execution is not None
    expected_contract_cash = (
        daily.integer_execution[1, 0, 3]
        + 2.0 * daily.integer_execution[1, 0, 5]
        + 2.0 * daily.integer_execution[1, 0, 6]
    )
    assert daily.candidate_features[
        1, 0, denomination_start + 2
    ] == pytest.approx(expected_contract_cash)
    # Zero current volume/executability is executor-only and cannot erase the
    # already known same-physical-contract policy token.
    assert not daily.executable_mask[1, 0]
    assert daily.candidate_mask[1, 0]
    assert daily.must_liquidate_mask[2, 0]

    legacy_panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
    )
    legacy_daily = legacy_panel.stock_context_futures_portfolio_daily
    assert legacy_daily is not None
    assert legacy_daily.candidate_features.shape[-1] == len(
        TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS
    )
    assert (
        legacy_daily.contract_version
        == TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_LEGACY_CONTRACT_VERSION
    )

    current_open_panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
        integer_contracts=True,
        current_open_feature=True,
        integer_fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    )
    current_open_daily = current_open_panel.stock_context_futures_portfolio_daily
    assert current_open_daily is not None
    assert current_open_daily.contract_version == (
        TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CURRENT_OPEN_CONTRACT_VERSION
    )
    assert current_open_daily.candidate_features.shape[-1] == len(
        TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS
    )
    assert current_open_daily.candidate_features[1, 0, -1] == pytest.approx(
        np.log(101.0 / 100.0)
    )
    assert current_open_daily.candidate_mask[1, 0]
    # Missing current previous-settlement makes the same-print feature
    # unknowable, so the candidate fails closed without reallocating elsewhere.
    assert not current_open_daily.candidate_mask[2, 0]


def test_carry_valuation_guard_quarantines_complete_physical_contract(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    session_dates = [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)]
    for date_index, session_date in enumerate(session_dates):
        for slot, product, asset_class, physical_contract, simple_return in (
            (1, "TX", "index_future", "TX202601", 0.01),
            # One unsupported +50% adjacent-OPEN valuation makes this entire
            # physical stock-future contract non-reconstructable.
            (
                2,
                "ABF",
                "stock_future",
                "ABF202601",
                0.50 if date_index == 1 else 0.01,
            ),
        ):
            row: dict[str, object] = {
                "date": session_date,
                "product": product,
                "symbol": f"TAIFEX_SLOT_{slot:04d}",
                "tenor_rank": 1,
                "open": 100.0,
                "close": 100.0,
                "volume": 100,
                "holding_log_return": float(np.log1p(simple_return)),
                "executable": True,
                "must_liquidate": date_index == 2,
                "can_hold_overnight": date_index < 2,
                "same_contract_as_previous_session": date_index > 0,
                "contract_multiplier": 200.0,
                "sinopac_network_fee_group": (
                    "large" if asset_class == "index_future" else "stock"
                ),
                "underlying_symbol": "S0",
                "contract": "202601",
                "physical_contract": physical_contract,
                "asset_class": asset_class,
                "previous_volume": 100.0,
                "previous_settlement": 99.0,
            }
            for feature_index, name in enumerate(FUTURES_MODEL_FEATURE_COLUMNS):
                row[name] = 7 if name == "taifex_product_id" else feature_index
            rows.append(row)
    data_path = tmp_path / "continuous_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows), data_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
                "feature_contract_version": TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
                "fixed_model_output_slots": TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
                "outputs": {"continuous_daily": {"sha256": _sha256(data_path)}},
            }
        ),
        encoding="utf-8",
    )
    panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
        integer_contracts=True,
        current_open_feature=True,
        carry_valuation_max_abs_simple_return=0.25,
        integer_fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    )
    daily = panel.stock_context_futures_portfolio_daily
    assert daily is not None
    assert daily.contract_version == (
        TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION
    )
    assert daily.carry_valuation_quarantine_mask is not None
    assert daily.carry_valuation_quarantine_mask[:, 1].all()
    assert not daily.candidate_mask[:, 1].any()
    assert not daily.executable_mask[:, 1].any()
    assert daily.integer_execution is not None
    assert not np.isfinite(daily.integer_execution[:, 1, 3]).any()
    # The index future is unaffected, proving that this is physical-contract
    # fail-close rather than a day-level market filter.
    assert daily.candidate_mask[1:, 0].all()


def test_expiry_row_uses_official_final_settlement_for_exact_integer_pnl(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    for idx, session_date in enumerate(
        [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)]
    ):
        open_price = 100.0 + idx
        close_price = open_price + 1.0
        row: dict[str, object] = {
            "date": session_date,
            "product": "TX",
            "symbol": "TAIFEX_SLOT_0001",
            "tenor_rank": 1,
            "open": open_price,
            "close": close_price,
            # The daily settlement/close is intentionally different from the
            # separately receipted final settlement on the expiry row.
            "settlement": close_price,
            "volume": 100,
            "holding_log_return": float(np.log(close_price / open_price)),
            "executable": True,
            "must_liquidate": idx == 2,
            "can_hold_overnight": idx < 2,
            "same_contract_as_previous_session": idx > 0,
            "liquidation_reason": (
                "last_trade_date" if idx == 2 else "carry_same_contract"
            ),
            "contract_multiplier": 200.0,
            "sinopac_network_fee_group": "large",
            "underlying_symbol": "S0",
            "contract": "202601",
            "physical_contract": "TX202601",
            "asset_class": "index_future",
            "previous_volume": 100.0,
            "previous_settlement": 99.0,
        }
        for feature_idx, name in enumerate(FUTURES_MODEL_FEATURE_COLUMNS):
            row[name] = 7 if name == "taifex_product_id" else feature_idx
        rows.append(row)

    data_path = tmp_path / "continuous_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows), data_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
                "feature_contract_version": TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
                "fixed_model_output_slots": TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
                "outputs": {"continuous_daily": {"sha256": _sha256(data_path)}},
            }
        ),
        encoding="utf-8",
    )
    final_dir = tmp_path / "final_settlement_v1"
    final_dir.mkdir()
    final_path = final_dir / "futures_final_settlement_history.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "settlement_date": date(2026, 1, 4),
                    "product": "TX",
                    "contract": "202601",
                    "final_settlement_price": 110.0,
                }
            ]
        ),
        final_path,
    )
    (final_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION,
                "outputs": {
                    "futures_final_settlement_history": {
                        "sha256": _sha256(final_path)
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
        integer_contracts=True,
        current_open_feature=True,
        carry_valuation_max_abs_simple_return=0.25,
        expiry_settlement_valuation=True,
        final_settlement_path=final_path,
        integer_fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    )
    daily = panel.stock_context_futures_portfolio_daily
    assert daily is not None
    assert daily.expiry_settlement_valuation is True
    assert daily.contract_version == (
        TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION
    )
    assert daily.holding_log_returns[2, 0] == pytest.approx(
        np.log(110.0 / 102.0), abs=1.0e-7
    )
    assert daily.integer_execution is not None
    assert daily.integer_execution[2, 0, 3] == pytest.approx(102.0 * 200.0)
    assert daily.integer_execution[2, 0, 4] == pytest.approx(110.0 * 200.0)
    assert daily.expiry_settlement_quarantine_mask is not None
    assert not daily.expiry_settlement_quarantine_mask.any()
    assert daily.expiry_settlement_quarantined_physical_contracts == 0
    # The expiry switch is intentionally narrow: pre-expiry rows retain the
    # archive's ordinary same-contract holding label.
    assert daily.holding_log_returns[1, 0] == pytest.approx(
        np.log(102.0 / 101.0), abs=1.0e-7
    )

    # A missing official expiry price cannot be replaced by the daily close.
    # Quarantine the complete physical contract from its first row so no held
    # quantity can arrive at an unpriceable terminal state.
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "settlement_date": date(2026, 1, 4),
                    "product": "TE",
                    "contract": "202601",
                    "final_settlement_price": 999.0,
                }
            ]
        ),
        final_path,
    )
    (final_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION,
                "outputs": {
                    "futures_final_settlement_history": {
                        "sha256": _sha256(final_path)
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    quarantined_panel = attach_stock_context_futures_portfolio_daily(
        _stock_panel(),
        data_path,
        fee_per_side_twd_by_group={
            "large": 60.0,
            "standard": 24.0,
            "stock": 40.0,
            "micro": 16.0,
        },
        integer_contracts=True,
        current_open_feature=True,
        carry_valuation_max_abs_simple_return=0.25,
        expiry_settlement_valuation=True,
        final_settlement_path=final_path,
        integer_fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    )
    quarantined = quarantined_panel.stock_context_futures_portfolio_daily
    assert quarantined is not None
    assert quarantined.expiry_settlement_quarantine_mask is not None
    assert quarantined.expiry_settlement_quarantine_mask[:, 0].all()
    assert quarantined.expiry_settlement_quarantined_physical_contracts == 1
    assert not quarantined.candidate_mask[:, 0].any()
    assert not quarantined.executable_mask[:, 0].any()
    assert quarantined.integer_execution is not None
    assert not np.isfinite(quarantined.integer_execution[:, 0, 3]).any()


def test_dataset_keeps_stock_attention_axis_and_packs_futures_execution() -> None:
    panel = _stock_panel(rows=4, symbols=3)
    futures_shape = (4, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)
    candidate_mask = np.zeros(futures_shape, dtype=bool)
    candidate_mask[1:, 0] = True
    executable = np.zeros(futures_shape, dtype=bool)
    executable[1:, 0] = True
    returns = np.full(futures_shape, np.nan, dtype=np.float32)
    returns[1:, 0] = 0.0
    fee = np.full(futures_shape, np.nan, dtype=np.float32)
    fee[1:, 0] = 0.001
    panel.stock_context_futures_portfolio_daily = (
        TaiwanStockContextFuturesPortfolioDaily(
            dates=panel.dates,
            symbols=fixed_futures_slot_symbols(),
            candidate_features=np.zeros(
                (
                    *futures_shape,
                    len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
                ),
                dtype=np.float32,
            ),
            candidate_mask=candidate_mask,
            holding_log_returns=returns,
            executable_mask=executable,
            must_liquidate_mask=np.zeros(futures_shape, dtype=bool),
            can_hold_overnight_mask=executable.copy(),
            fee_rate_per_open_notional=fee,
            open_prices=np.ones(futures_shape, dtype=np.float32),
            close_prices=np.ones(futures_shape, dtype=np.float32),
            volumes=np.ones(futures_shape, dtype=np.float32),
            benchmark_log_returns=np.zeros(4, dtype=np.float32),
            source_path="synthetic",
            manifest_path="synthetic",
        )
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(4, dtype=np.int64),
        lookback=1,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    assert tuple(dataset.tradable_mask_t.shape) == (4, 3)
    assert tuple(dataset.derivative_candidate_mask_t.shape) == futures_shape
    assert tuple(dataset.overnight_log_returns_t.shape) == (*futures_shape, 4)
    final_row = int(dataset.valid_indices[-1])
    assert bool(
        (dataset.overnight_log_returns_t[final_row, :, 2] > 0.5).all().item()
    )
    windowed = dataset_to_windowed_tensors(dataset)
    assert windowed.execution_mode == "tw_stock_context_futures_portfolio"
    assert tuple(windowed.derivative_candidate_features.shape) == (
        4,
        TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
    )
    assert _split_recurrent_symbol_count(windowed) == 1936
    assert not _split_uses_recurrent_futures_equity_scale(windowed)
    assert _split_uses_recurrent_futures_equity_scale(
        SimpleNamespace(
            execution_mode="tw_stock_context_futures_portfolio",
            overnight_log_returns=torch.zeros((2, 3, 11)),
        )
    )


def test_model_masks_before_projection_and_can_retain_cash() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=2,
        num_features=3,
        num_symbols=4,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        projection_l1_scale_by_active_count=True,
        center_long_short_logits=False,
        return_aux=False,
        execution_mode="tw_stock_context_futures_portfolio",
    ).eval()
    with torch.no_grad():
        model.futures_action_head.weight.zero_()
        model.futures_action_head.bias.fill_(0.5)
    candidate_features = torch.zeros((1, 1936, 18), dtype=torch.float32)
    candidate_mask = torch.zeros((1, 1936), dtype=torch.bool)
    candidate_mask[:, [2, 9]] = True
    with torch.no_grad():
        weights = model(
            torch.zeros((1, 2, 4, 3), dtype=torch.float32),
            torch.ones((1, 4), dtype=torch.bool),
            portfolio_context={
                "candidate_features": candidate_features,
                "candidate_mask": candidate_mask,
            },
        )
    assert tuple(weights.shape) == (1, 1936)
    assert torch.count_nonzero(weights.masked_select(~candidate_mask)) == 0
    assert weights.abs().sum().item() == pytest.approx(0.5, abs=1e-6)
    with pytest.raises(ValueError, match="requires causal futures context"):
        model(
            torch.zeros((1, 2, 4, 3), dtype=torch.float32),
            torch.ones((1, 4), dtype=torch.bool),
        )


def test_denomination_aware_output_groups_contracts_without_top_k_or_redistribution() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=1,
        num_features=3,
        num_symbols=2,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        projection_l1_scale_by_active_count=True,
        center_long_short_logits=False,
        futures_denomination_aware_output=True,
        futures_denomination_reference_capital=1_000_000.0,
        return_aux=False,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    # Group 0 contains a standard and mini contract. Group 1 is below one
    # contract and must remain cash. Groups 2 and 3 are both viable: there is
    # deliberately no fixed-K selection between them.
    raw = torch.tensor(
        [[0.20, 0.35, 0.05, -0.11, 0.21]],
        dtype=torch.float32,
        requires_grad=True,
    )
    mask = torch.ones_like(raw, dtype=torch.bool)
    denomination = torch.tensor(
        [[
            [0.0, 0.0, 400_000.0],
            [0.0, 1.0, 20_000.0],
            [1.0, 0.0, 100_000.0],
            [2.0, 0.0, 100_000.0],
            [3.0, 0.0, 100_000.0],
        ]],
        dtype=torch.float32,
    )
    projected, aux = model._denomination_aware_group_projection(
        raw, mask, denomination
    )
    # 55% group request becomes 27 mini denominations = 54%, shared back over
    # the two candidate slots so the exact executor receives one group target.
    assert projected[0, :2].tolist() == pytest.approx([0.27, 0.27], abs=1.0e-7)
    assert projected[0, 2].item() == pytest.approx(0.0, abs=1.0e-7)
    assert projected[0, 3].item() == pytest.approx(-0.10, abs=1.0e-7)
    assert projected[0, 4].item() == pytest.approx(0.20, abs=1.0e-7)
    assert projected.abs().sum().item() < raw.abs().sum().item()
    assert int(aux["futures_valid_denomination_group"].sum().item()) == 4
    projected.sum().backward()
    assert raw.grad is not None
    assert torch.isfinite(raw.grad).all()
    # Even the sub-contract group has an STE direction toward its first unit.
    assert raw.grad[0, 2].item() == pytest.approx(1.0, abs=1.0e-7)


def test_current_futures_open_model_requires_new_abi_and_backpropagates() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=1,
        num_features=3,
        num_symbols=2,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        center_long_short_logits=False,
        futures_denomination_aware_output=True,
        projection_l1_scale_by_active_count=True,
        futures_current_open_feature=True,
        futures_denomination_reference_capital=1_000_000.0,
        return_aux=False,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    features = torch.zeros(
        (
            1,
            TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
            len(TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS),
        ),
        dtype=torch.float32,
    )
    features[..., len(FUTURES_MODEL_FEATURE_COLUMNS)] = -1.0
    denomination_start = len(TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS)
    features[0, 2, denomination_start : denomination_start + 3] = torch.tensor(
        [2.0, 0.0, 100_000.0]
    )
    features[0, 9, denomination_start : denomination_start + 3] = torch.tensor(
        [9.0, 0.0, 100_000.0]
    )
    features[0, 2, -1] = 0.01
    features[0, 9, -1] = -0.02
    candidate_mask = torch.zeros((1, 1936), dtype=torch.bool)
    candidate_mask[:, [2, 9]] = True
    inputs = torch.zeros((1, 1, 2, 3), dtype=torch.float32)
    stock_mask = torch.ones((1, 2), dtype=torch.bool)

    weights = model(
        inputs,
        stock_mask,
        portfolio_context={
            "candidate_features": features,
            "candidate_mask": candidate_mask,
        },
    )
    # A slot-specific objective avoids cancellation between the positive and
    # negative OPEN-gap examples while proving that the new input participates
    # in the differentiable backward world.
    weights[0, 2].backward()
    assert model.futures_current_open_encoder is not None
    open_grad = model.futures_current_open_encoder[0].weight.grad
    assert open_grad is not None
    assert torch.isfinite(open_grad).all()
    assert torch.count_nonzero(open_grad) > 0

    with pytest.raises(ValueError, match="current OPEN gap context"):
        model(
            inputs,
            stock_mask,
            portfolio_context={
                "candidate_features": features[..., :-1],
                "candidate_mask": candidate_mask,
            },
        )


def test_model_routes_each_futures_token_to_its_underlying_stock() -> None:
    model = CrossSectionalAllFuturesModel(
        lookback=2,
        num_features=3,
        num_symbols=4,
        d_model=8,
        attention_mode="temporal_only",
        use_latent_factors=False,
        use_market_tokens=False,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
        center_long_short_logits=False,
        return_aux=True,
        return_aux_details=True,
        execution_mode="tw_stock_context_futures_portfolio",
    ).eval()
    features = torch.zeros(
        (
            1,
            TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
            len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
        ),
        dtype=torch.float32,
    )
    underlying_index = len(FUTURES_MODEL_FEATURE_COLUMNS)
    features[..., underlying_index] = -1.0
    features[0, 2, underlying_index] = 0.0
    features[0, 9, underlying_index] = 1.0
    candidate_mask = torch.zeros((1, 1936), dtype=torch.bool)
    candidate_mask[:, [2, 9]] = True
    stock_embeddings = torch.randn((1, 4, 8), dtype=torch.float32)

    with torch.no_grad():
        output = model._portfolio_outputs_from_stock_embeddings(
            stock_embeddings,
            torch.ones((1, 4), dtype=torch.bool),
            {},
            portfolio_context={
                "candidate_features": features,
                "candidate_mask": candidate_mask,
            },
        )

    link_mask = output["aux"]["futures_underlying_link_mask"]
    link_gate = output["aux"]["futures_underlying_gate"]
    assert link_mask[0, 2]
    assert link_mask[0, 9]
    assert int(link_mask.sum().item()) == 2
    assert 0.0 < link_gate[0, 2, 0].item() < 1.0
    assert 0.0 < link_gate[0, 9, 0].item() < 1.0


def test_executor_carries_until_mandatory_contract_close_without_redistribution() -> None:
    actions = torch.zeros((3, 1936), dtype=torch.float32, requires_grad=True)
    with torch.no_grad():
        actions[:, 0] = torch.tensor([0.4, -0.8, 0.2])
    execution = torch.zeros((3, 1936, 4), dtype=torch.float32)
    execution[:, 0, 0] = 0.0
    execution[[0, 2], 0, 1] = 1.0
    execution[2, 0, 2] = 1.0
    execution[:, 0, 3] = 0.0
    # Stock masks intentionally have a different width. The dedicated futures
    # executor must consume only its packed 1,936-slot sidecar.
    stock_mask = torch.ones((3, 2), dtype=torch.bool)
    result = run_backtest_torch(
        actions,
        torch.zeros((3, 2), dtype=torch.float32),
        stock_mask,
        torch.zeros(3, dtype=torch.float32),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=stock_mask,
        can_sell_mask=stock_mask,
        force_exit_mask=torch.zeros_like(stock_mask),
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
    )
    assert result.weights_history[:, 0].tolist() == pytest.approx([0.4, 0.4, 0.2])
    assert result.final_weights is not None
    assert result.final_weights[0].item() == pytest.approx(0.0)
    # Row 1's unavailable request stays in its own slot; it is neither closed
    # nor redistributed. Row 2 includes rebalance plus mandatory close.
    assert result.turnovers.tolist() == pytest.approx([0.4, 0.0, 0.4])
    (-result.strategy_returns.mean()).backward()
    assert actions.grad is not None


def test_integer_carry_jointly_packs_standard_and_mini_and_closes_at_expiry() -> None:
    actions = torch.zeros((1, 1936), dtype=torch.float32, requires_grad=True)
    with torch.no_grad():
        actions[0, 0] = 0.55
    execution = torch.full((1, 1936, 11), float("nan"), dtype=torch.float32)
    execution[..., 1:3] = 0.0
    execution[..., 9] = torch.arange(1936, dtype=torch.float32)[None, :]
    execution[..., 10] = 0.0
    # One group contains a TWD 400k standard and a TWD 20k mini.  Both earn
    # 10%; the exact TWD 550k sleeve fits 1 + 7 contracts after current and
    # reserved close fees, and the expiry row then charges the actual close.
    execution[0, :2, 0] = torch.log(torch.tensor(1.1))
    execution[0, :2, 1] = 1.0
    execution[0, :2, 2] = 1.0
    execution[0, :2, 3] = torch.tensor([400_000.0, 20_000.0])
    execution[0, :2, 4] = torch.tensor([440_000.0, 22_000.0])
    execution[0, :2, 5] = 40.0
    execution[0, :2, 6:8] = 0.0
    execution[0, :2, 8] = 100.0
    execution[0, :2, 9] = 0.0
    execution[0, :2, 10] = torch.tensor([0.0, 1.0])
    result = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    assert result.contract_quantities_history is not None
    assert result.contract_quantities_history.dtype == torch.int64
    assert result.contract_quantities_history[0, :2].tolist() == [1, 7]
    assert result.final_weights[:2].tolist() == pytest.approx([0.0, 0.0])
    # 540k gross notional earns 54k; eight opens and eight closes each cost 40.
    assert torch.expm1(result.strategy_returns[0]).item() == pytest.approx(
        0.05336, abs=1e-6
    )
    (-result.strategy_returns.mean()).backward()
    assert actions.grad is not None

    wrapped = run_backtest_torch(
        actions.detach(),
        torch.zeros((1, 2), dtype=torch.float32),
        torch.ones((1, 2), dtype=torch.bool),
        torch.zeros(1, dtype=torch.float32),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
    )
    assert wrapped.settlement_ledger_unit == "contract_quantity"
    assert wrapped.weights_history[0, :2].tolist() == pytest.approx([1.0, 7.0])
    assert torch.equal(
        wrapped.weights_history,
        torch.round(wrapped.weights_history),
    )


def test_integer_account_reserves_locked_positions_before_new_targets() -> None:
    execution = torch.zeros((1, 2, 11), dtype=torch.float32)
    execution[..., 3:5] = 100_000.0
    execution[..., 9] = torch.tensor([[0.0, 1.0]])
    # Slot 0 is a blocked carried position. Slot 1 can trade, but its requested
    # 90% sleeve must contract to the residual 50% account capacity.
    execution[0, 1, 1] = 1.0
    execution[0, 1, 8] = 100.0
    result = run_tw_futures_portfolio_integer_torch(
        torch.tensor([[0.0, 0.90]], dtype=torch.float32),
        execution,
        initial_capital=1_000_000.0,
        initial_quantities=torch.tensor([5.0, 0.0]),
    )
    assert result.contract_quantities_history is not None
    assert result.contract_quantities_history[0].tolist() == [5, 5]
    assert bool(result.final_alive)
    assert result.default_reason_history is not None
    assert result.default_reason_history.tolist() == [0]


def test_integer_account_defaults_only_when_minimum_locked_cash_is_impossible() -> None:
    execution = torch.zeros((1, 1, 11), dtype=torch.float32)
    execution[..., 3:5] = 100_000.0
    result = run_tw_futures_portfolio_integer_torch(
        torch.zeros((1, 1), dtype=torch.float32),
        execution,
        initial_capital=1_000_000.0,
        initial_quantities=torch.tensor([11.0]),
    )
    assert not bool(result.final_alive)
    assert result.default_reason_history is not None
    assert result.default_reason_history.tolist() == [
        TW_FUTURES_PORTFOLIO_DEFAULT_FUNDING
    ]


def test_flat_action_head_portfolio_is_an_exact_cash_account() -> None:
    execution = _integer_execution(
        torch.tensor(
            [[-0.20, 0.15], [0.25, -0.10], [-0.05, 0.08]],
            dtype=torch.float32,
        )
    )
    result = run_tw_futures_portfolio_integer_torch(
        torch.zeros((3, 2), dtype=torch.float32),
        execution,
        initial_capital=10_000_000.0,
    )
    torch.testing.assert_close(result.strategy_returns, torch.zeros(3))
    assert result.equity_scale_history is not None
    torch.testing.assert_close(result.equity_scale_history, torch.ones(3))
    assert result.default_history is not None
    assert not bool(result.default_history.any())
    assert bool(result.final_alive)


def test_integer_surrogate_has_gradient_inside_hard_quantization_plateau() -> None:
    actions = torch.tensor(
        [[0.20, 0.10], [0.25, 0.05]],
        dtype=torch.float32,
        requires_grad=True,
    )
    execution = _integer_execution(
        torch.tensor([[0.010, -0.005], [0.020, 0.004]], dtype=torch.float32)
    )

    def objective(value: torch.Tensor) -> torch.Tensor:
        result = run_tw_futures_portfolio_integer_surrogate_torch(
            value,
            execution,
            initial_capital=1_000_000.0,
            return_weights_history=False,
        )
        return -result.strategy_returns.mean()

    loss = objective(actions)
    loss.backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    epsilon = 1.0e-4
    plus = actions.detach().clone()
    minus = actions.detach().clone()
    plus[1, 0] += epsilon
    minus[1, 0] -= epsilon
    finite_difference = ((objective(plus) - objective(minus)) / (2.0 * epsilon)).item()
    # The forward path is a hard whole-contract floor, so a small perturbation
    # inside one plateau leaves its value unchanged. The explicit
    # straight-through backward remains non-zero and supplies the optimizer
    # with a direction toward the next executable contract.
    assert finite_difference == pytest.approx(0.0, abs=1.0e-7)
    assert abs(actions.grad[1, 0].item()) > 1.0e-6


def test_integer_surrogate_groups_standard_and_mini_and_masks_capacity_gradient() -> None:
    same_group = torch.tensor([0, 0], dtype=torch.int64)
    actions = torch.tensor([[0.20, 0.30]], requires_grad=True)
    execution = _integer_execution(
        torch.tensor([[0.01, 0.01]], dtype=torch.float32),
        group_indices=same_group,
    )
    grouped = run_tw_futures_portfolio_integer_surrogate_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    (-grouped.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert actions.grad[0, 0].item() == pytest.approx(
        actions.grad[0, 1].item(), abs=1.0e-8
    )
    assert actions.grad.abs().sum().item() > 0.0

    blocked_actions = actions.detach().clone().requires_grad_(True)
    blocked_execution = _integer_execution(
        torch.tensor([[0.01, 0.01]], dtype=torch.float32),
        group_indices=same_group,
        executable=False,
    )
    blocked = run_tw_futures_portfolio_integer_surrogate_torch(
        blocked_actions,
        blocked_execution,
        initial_capital=1_000_000.0,
    )
    (-blocked.strategy_returns.sum()).backward()
    assert blocked_actions.grad is not None
    assert torch.count_nonzero(blocked_actions.grad) == 0


def test_integer_training_surrogate_carries_continuous_state_without_account_death() -> None:
    actions = torch.tensor(
        [[0.05, 0.0], [0.20, 0.0]],
        dtype=torch.float32,
        requires_grad=True,
    )
    execution = _integer_execution(
        torch.tensor([[0.01, 0.0], [0.02, 0.0]], dtype=torch.float32)
    )
    first = run_backtest_torch(
        actions[:1],
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution[:1],
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=True,
    )
    assert first.final_alive is not None and bool(first.final_alive)
    assert first.final_weights is not None
    assert first.final_equity_scale is not None
    assert first.settlement_ledger_unit == "notional_weight_training_surrogate"
    # One contract is 10% of capital. A 5% request has zero executable forward
    # exposure but retains a straight-through gradient.
    assert first.weights_history[0, 0].item() == pytest.approx(0.0, abs=1.0e-7)

    second = run_backtest_torch(
        actions[1:],
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution[1:],
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=True,
        initial_weights=first.final_weights,
        initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
    )
    assert second.final_alive is not None and bool(second.final_alive)
    (-first.strategy_returns.sum() - second.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    assert torch.count_nonzero(actions.grad) == 2


def test_integer_training_uses_exact_forward_and_surrogate_only_for_backward() -> None:
    actions = torch.tensor([[0.05, 0.20]], dtype=torch.float32, requires_grad=True)
    execution = _integer_execution(
        torch.tensor([[0.03, -0.01]], dtype=torch.float32)
    )
    trained = run_backtest_torch(
        actions,
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=False,
    )
    audited = run_backtest_torch(
        actions.detach(),
        torch.zeros((1, 1)),
        torch.ones((1, 1), dtype=torch.bool),
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        overnight_returns=execution,
        execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1_000_000.0,
        futures_portfolio_training_surrogate_only=False,
    )
    assert trained.settlement_ledger_unit == "contract_quantity"
    assert torch.equal(trained.weights_history, audited.weights_history)
    assert torch.equal(trained.weights_history, torch.round(trained.weights_history))
    assert trained.strategy_returns.tolist() == pytest.approx(
        audited.strategy_returns.tolist(), abs=1.0e-8
    )
    (-trained.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    # Slot 0 has an exact zero-contract forward at 5%, yet receives a stable
    # backward direction from the grouped quantization relaxation.
    assert trained.weights_history[0, 0].item() == 0.0
    assert abs(actions.grad[0, 0].item()) > 0.0


def test_integer_exact_ruin_keeps_forward_absorbing_but_backward_recoverable() -> None:
    actions = torch.tensor([[-0.50], [0.20]], dtype=torch.float32, requires_grad=True)
    # A short five-contract position loses more than the fully collateralized
    # account on row 0. Exact forward must die and row 1 must stay flat.
    execution = _integer_execution(
        torch.tensor([[3.0], [0.10]], dtype=torch.float32)
    )
    result = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
        recoverable_backward=True,
    )
    assert result.final_alive is not None and not bool(result.final_alive)
    assert result.default_history is not None
    assert result.default_history.tolist() == [True, False]
    assert result.default_reason_history is not None
    assert result.default_reason_history.tolist() == [
        TW_FUTURES_PORTFOLIO_DEFAULT_NONPOSITIVE_EQUITY,
        0,
    ]
    assert result.strategy_returns[0].item() < -15.0
    assert result.strategy_returns[1].item() == 0.0
    (-result.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    # Row 0 learns away from insolvency and row 1 still learns although exact
    # state entered the batch dead. Neither value changed in forward.
    assert torch.count_nonzero(actions.grad) == 2


def test_integer_dead_initial_account_has_exact_zero_forward_and_shadow_gradient() -> None:
    actions = torch.tensor([[0.20]], dtype=torch.float32, requires_grad=True)
    execution = _integer_execution(torch.tensor([[0.10]], dtype=torch.float32))
    result = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
        initial_alive=torch.tensor(False),
        initial_equity_scale=torch.tensor(0.0),
        recoverable_backward=True,
    )
    assert result.strategy_returns.tolist() == [0.0]
    assert result.final_alive is not None and not bool(result.final_alive)
    assert result.default_history is not None
    assert result.default_history.tolist() == [False]
    (-result.strategy_returns.sum()).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    assert torch.count_nonzero(actions.grad) == 1


def test_integer_executor_chunking_carries_quantities_and_equity_together() -> None:
    actions = torch.tensor([[0.30, 0.10], [0.35, 0.05]], dtype=torch.float32)
    execution = _integer_execution(
        torch.tensor([[0.10, -0.02], [0.03, 0.01]], dtype=torch.float32)
    )
    full = run_tw_futures_portfolio_integer_torch(
        actions,
        execution,
        initial_capital=1_000_000.0,
    )
    first = run_tw_futures_portfolio_integer_torch(
        actions[:1],
        execution[:1],
        initial_capital=1_000_000.0,
    )
    second = run_tw_futures_portfolio_integer_torch(
        actions[1:],
        execution[1:],
        initial_capital=1_000_000.0,
        initial_quantities=first.final_weights,
        initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
    )
    assert second.strategy_returns[0].item() == pytest.approx(
        full.strategy_returns[1].item(), abs=1.0e-7
    )
    assert torch.equal(second.final_weights, full.final_weights)
    assert second.final_equity_scale is not None
    assert full.final_equity_scale is not None
    assert second.final_equity_scale.item() == pytest.approx(
        full.final_equity_scale.item(), abs=1.0e-7
    )


def test_integer_compiled_block_boundary_preserves_shadow_state_and_gradient() -> None:
    raw_actions = torch.tensor(
        [
            [0.30, 0.10],
            [0.35, 0.05],
            [-0.20, 0.25],
            [0.15, -0.30],
        ],
        dtype=torch.float32,
    )
    execution = _integer_execution(
        torch.tensor(
            [
                [0.10, -0.02],
                [0.03, 0.01],
                [-0.04, 0.02],
                [0.01, -0.03],
            ],
            dtype=torch.float32,
        )
    )

    full_actions = raw_actions.clone().requires_grad_(True)
    full = futures_portfolio_module._run_tw_futures_portfolio_integer_torch_impl(
        full_actions,
        execution,
        initial_capital=1_000_000.0,
        recoverable_backward=True,
    )
    full_loss = -full.strategy_returns.mean()
    full_gradient = torch.autograd.grad(full_loss, full_actions)[0]

    blocked_actions = raw_actions.clone().requires_grad_(True)
    first = futures_portfolio_module._run_tw_futures_portfolio_integer_torch_impl(
        blocked_actions[:2],
        execution[:2],
        initial_capital=1_000_000.0,
        recoverable_backward=True,
    )
    assert first._surrogate_final_weights is not None
    assert first._surrogate_final_equity_scale is not None
    assert first._surrogate_final_alive is not None
    second = futures_portfolio_module._run_tw_futures_portfolio_integer_torch_impl(
        blocked_actions[2:],
        execution[2:],
        initial_capital=1_000_000.0,
        initial_quantities=first.final_weights,
        initial_equity_scale=first.final_equity_scale,
        initial_alive=first.final_alive,
        recoverable_backward=True,
        _initial_surrogate_weights=first._surrogate_final_weights,
        _initial_surrogate_equity_scale=first._surrogate_final_equity_scale,
        _initial_surrogate_alive=first._surrogate_final_alive,
    )
    blocked_returns = torch.cat(
        (first.strategy_returns, second.strategy_returns)
    )
    blocked_gradient = torch.autograd.grad(
        -blocked_returns.mean(), blocked_actions
    )[0]

    torch.testing.assert_close(blocked_returns, full.strategy_returns)
    torch.testing.assert_close(blocked_gradient, full_gradient)
    assert torch.equal(second.final_weights, full.final_weights)
    torch.testing.assert_close(
        second.final_equity_scale,
        full.final_equity_scale,
    )


def test_integer_compiled_tail_padding_is_an_inert_fixed_size_suffix() -> None:
    actions = torch.tensor(
        [[0.30, -0.10], [0.20, 0.05], [-0.15, 0.25]],
        dtype=torch.float32,
        requires_grad=True,
    )
    execution = _integer_execution(
        torch.tensor(
            [[0.01, -0.02], [0.03, 0.01], [-0.01, 0.02]],
            dtype=torch.float32,
        )
    )
    advance = torch.tensor([True, False, True])

    padded_actions, padded_execution, padded_advance = (
        futures_portfolio_module._pad_integer_compile_tail(
            actions,
            execution,
            advance,
            block_rows=4,
        )
    )

    assert tuple(padded_actions.shape) == (4, 2)
    assert tuple(padded_execution.shape) == (4, 2, 11)
    assert tuple(padded_advance.shape) == (4,)
    torch.testing.assert_close(padded_actions[:3], actions)
    torch.testing.assert_close(padded_execution[:3], execution)
    assert torch.equal(padded_advance[:3], advance)
    assert torch.count_nonzero(padded_actions[3]).item() == 0
    assert torch.count_nonzero(padded_execution[3]).item() == 0
    assert padded_advance[3].item() is False

    padded_actions.sum().backward()
    torch.testing.assert_close(actions.grad, torch.ones_like(actions))


def test_integer_compiled_block_rows_has_one_environment_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolve = getattr(
        futures_portfolio_module,
        "resolve_tw_futures_portfolio_integer_compiled_block_rows",
    )
    monkeypatch.setenv(
        "STOCKAGENT_TW_FUTURES_PORTFOLIO_COMPILE_BLOCK_ROWS",
        "64",
    )
    assert resolve() == 64
    monkeypatch.setenv(
        "STOCKAGENT_TW_FUTURES_PORTFOLIO_COMPILE_BLOCK_ROWS",
        "invalid",
    )
    assert resolve() == 0


def test_integer_zero_turnover_objective_can_skip_unused_series_exactly() -> None:
    actions = torch.tensor(
        [[0.30, -0.10], [0.20, 0.05], [-0.15, 0.25]],
        dtype=torch.float32,
    )
    execution = _integer_execution(
        torch.tensor(
            [[0.01, -0.02], [0.03, 0.01], [-0.01, 0.02]],
            dtype=torch.float32,
        )
    )

    complete_actions = actions.clone().requires_grad_(True)
    complete = run_tw_futures_portfolio_integer_torch(
        complete_actions,
        execution,
        initial_capital=1_000_000.0,
        return_turnovers=True,
        recoverable_backward=True,
    )
    complete_loss = -complete.strategy_returns.mean()
    complete_gradient = torch.autograd.grad(
        complete_loss,
        complete_actions,
    )[0]

    skipped_actions = actions.clone().requires_grad_(True)
    skipped = run_tw_futures_portfolio_integer_torch(
        skipped_actions,
        execution,
        initial_capital=1_000_000.0,
        return_turnovers=False,
        recoverable_backward=True,
    )
    skipped_loss = -skipped.strategy_returns.mean()
    skipped_gradient = torch.autograd.grad(skipped_loss, skipped_actions)[0]

    torch.testing.assert_close(skipped.strategy_returns, complete.strategy_returns)
    torch.testing.assert_close(skipped_loss, complete_loss)
    torch.testing.assert_close(skipped_gradient, complete_gradient)
    assert torch.count_nonzero(skipped.turnovers).item() == 0
    assert torch.count_nonzero(complete.turnovers).item() > 0
    assert torch.equal(skipped.final_weights, complete.final_weights)
    torch.testing.assert_close(
        skipped.final_equity_scale,
        complete.final_equity_scale,
    )


def test_formal_config_preserves_full_contract_and_1000_epochs() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml"
    )
    assert config.trading.execution_mode == "tw_stock_context_futures_portfolio"
    assert config.training.model_name == "cross_sectional_all_futures"
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == 0.0
    assert config.training.batch_size_train == 96
    assert config.training.eval_backtest_chunk_rows == 64
    assert config.training.eval_backtest_chunk_rows_auto is False
    assert config.training.eval_backtest_compile is True
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_multi_basis_projection_l1_"
        "v3_full1000_batch96_chunk64"
    )
    assert len(config.data.feature_include) >= 90
    assert config.data.feature_exclude == ["next_session_open_gap_logret"]
    assert "twpub_pe_log" in config.data.feature_include
    assert "twpub_margin_balance_log" in config.data.feature_include
    assert "twpub_foreign_net_buy_flow" in config.data.feature_include
    assert "twpub_material_event_count_log" in config.data.feature_include
    assert "twpub_cbc_m2_yoy" in config.data.feature_include
    assert "twpub_taifex_tx_open_interest_log" in config.data.feature_include
    assert not config.training.train_symbol_compaction
    assert not config.training.distributed_symbol_sharded_ledger
    assert config.training.transformer_base_portfolio.portfolio_output_mode == (
        "projection_l1"
    )
    assert config.trading.portfolio_activation == "pre_normalized"
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    assert manifest["contracts"]["model"]["model_name"] == (
        "cross_sectional_all_futures"
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["fixed_model_output_slots"] == 1936
    assert futures_contract["holding"] == (
        "same_physical_contract_cross_session_until_own_expiry"
    )
    assert manifest["contracts"]["model"]["contract_version"] == 2
    assert futures_contract["cross_domain_contract_version"] == 2
    assert futures_contract["candidate_feature_columns"] == list(
        TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS
    )


def test_integer_0900_carry_config_is_fresh_full_feature_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0900_integer_full_features_"
        "multi_basis_projection_l1_cash_capital10m.yaml"
    )
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.trading.tw_futures_portfolio_integer_initial_capital == pytest.approx(
        10_000_000.0
    )
    assert config.trading.tw_futures_portfolio_integer_fee_per_contract_per_side_twd == pytest.approx(
        40.0
    )
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert config.data.day_trade_open_feature is True
    assert config.data.feature_exclude == []
    assert len(config.data.feature_include) >= 90
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.compile_loss is False
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert (
        config.training.transformer_base_portfolio.futures_denomination_aware_output
        is True
    )
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_0900_integer_denomination_exact_"
        "ste_full_features_cash_capital10m_v6"
    )
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["integer_training_surrogate"] == (
        TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE
    )
    assert futures_contract["integer_training_forward"] == "exact_integer_account_v2"
    assert futures_contract["denomination_aware_model_output"] is True
    assert futures_contract["candidate_feature_columns"] == list(
        TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
    )
    assert manifest["contracts"]["model"]["contract_version"] == 3


def test_integer_0845_futures_open_config_uses_stock_tminus1_and_new_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0845_integer_futures_open_"
        "full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    assert config.data.day_trade_open_feature is True
    assert config.data.feature_exclude == []
    assert config.data.feature_shift_next_session == [
        "next_session_open_gap_logret"
    ]
    assert config.data.tw_futures_current_open_feature is True
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.transformer_base_portfolio.futures_denomination_aware_output
    assert config.training.transformer_base_portfolio.futures_current_open_feature
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_0845_integer_futures_open_exact_"
        "ste_stock_tminus1_cash_capital10m_v1"
    )
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["cross_domain_contract_version"] == 4
    assert futures_contract["candidate_feature_columns"] == list(
        TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS
    )
    assert futures_contract["cash_stock_information_clock"] == (
        "completed_cash_stock_sessions_through_t_minus_1"
    )
    assert futures_contract["entry_price_clock"] == (
        "08:45_daily_session_open_same_print_research_proxy"
    )
    assert manifest["contracts"]["model"]["contract_version"] == 4
    legacy_0900 = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0900_integer_full_features_"
        "multi_basis_projection_l1_cash_capital10m.yaml"
    )
    legacy_manifest = build_checkpoint_manifest(
        _stock_panel(), legacy_0900, include_data_content=False
    )
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch"):
        validate_checkpoint_manifest(
            {"experiment_manifest": legacy_manifest},
            manifest,
            checkpoint_path=Path("legacy-0900-v3.pt"),
            scope="model",
        )


def test_integer_0845_recoverable_config_is_fresh_guarded_baseline() -> None:
    v1 = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0845_integer_futures_open_"
        "full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0845_integer_futures_open_"
        "recoverable_full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.futures_portfolio_recoverable_backward is True
    assert config.data.feature_exclude == []
    assert len(config.data.feature_include) >= 90
    assert config.data.tw_futures_carry_valuation_max_abs_simple_return == pytest.approx(
        0.25
    )
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_0845_integer_futures_open_exact_"
        "ste_recoverable_guarded_stock_tminus1_cash_capital10m_v2"
    )
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["cross_domain_contract_version"] == (
        TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION
    )
    assert futures_contract["integer_training_surrogate"] == (
        TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE
    )
    assert futures_contract["integer_recoverable_backward"] is True
    assert futures_contract["carry_valuation_max_abs_simple_return"] == pytest.approx(
        0.25
    )
    v1_manifest = build_checkpoint_manifest(
        _stock_panel(), v1, include_data_content=False
    )
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch"):
        validate_checkpoint_manifest(
            {"experiment_manifest": v1_manifest},
            manifest,
            checkpoint_path=Path("legacy-0845-v1.pt"),
            scope="resume",
        )


def test_integer_0845_carry_to_expiry_22_basis_config_is_fresh_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_full_features_multi_basis_projection_l1_cash_"
        "capital10m.yaml"
    )
    basis = config.training.transformer_base_portfolio
    assert config.trading.execution_mode == "tw_stock_context_futures_portfolio"
    assert config.training.model_name == "cross_sectional_all_futures"
    assert len(config.data.feature_include) == 99
    assert config.data.feature_exclude == []
    assert config.data.feature_shift_next_session == [
        "next_session_open_gap_logret"
    ]
    assert config.data.tw_futures_current_open_feature is True
    assert config.data.tw_futures_expiry_settlement_valuation is True
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.trading.tw_futures_portfolio_integer_initial_capital == pytest.approx(
        10_000_000.0
    )
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert config.training.epochs == 1000
    assert config.training.lookback == 32
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 32
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.futures_portfolio_recoverable_backward is True
    assert config.training.pretrained_initialization_root is None
    assert config.training.pretrained_initialization_validation_guard is False
    assert len(basis.temporal_basis_families) == 22
    assert sum(basis.temporal_basis_components_by_family.values()) == 524
    assert basis.temporal_basis_input == "raw_features"
    assert basis.portfolio_output_mode == "projection_l1"
    assert basis.projection_l1_scale_by_active_count is True
    assert config.training.learning_rate == pytest.approx(1.0e-4)
    assert config.training.lr_scheduler == "warmup_cosine"
    assert config.training.lr_scheduler_warmup_steps == 256
    assert config.training.lr_scheduler_eta_min == pytest.approx(1.0e-6)
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.early_stopping_min_delta == pytest.approx(1.0e-4)
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_full_features_cash_capital10m_v1"
    )

    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["cross_domain_contract_version"] == (
        TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION
    )
    assert futures_contract["holding"] == (
        "same_physical_contract_cross_session_until_own_expiry"
    )
    assert futures_contract["expiry_settlement_valuation"] is True
    assert futures_contract["expiry_exit_price_source"] == (
        "receipt_backed_official_taifex_final_settlement"
    )
    assert futures_contract["final_settlement_path"].endswith(
        "data_tw_futures/final_settlement_v1/"
        "futures_final_settlement_history.parquet"
    )
    assert futures_contract["missing_final_settlement_policy"] == (
        "quarantine_entire_physical_contract_no_redistribution"
    )


def test_integer_0845_carry_to_expiry_guard_matches_day_trade_training_stage() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_pretrained_guard_full_features_multi_basis_"
        "projection_l1_cash_capital10m.yaml"
    )
    assert config.training.epochs == 1000
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.futures_portfolio_recoverable_backward is True
    assert config.training.pretrained_initialization_root.endswith(
        "tw_stock_context_all_futures_carry_multi_basis_projection_l1_"
        "v3_full1000_batch96_chunk64"
    )
    assert config.training.pretrained_initialization_fold_policy == (
        "matching_train_and_validation_years"
    )
    assert config.training.pretrained_initialization_feature_adapter == (
        "transformer_feature_projection_by_name"
    )
    assert config.training.pretrained_initialization_require_exact_backbone is False
    assert config.training.pretrained_initialization_validation_guard is True
    assert config.training.pretrained_initialization_trainable_parameter_prefixes == [
        "temporal_basis_feature_encoder.",
        "futures_",
    ]
    assert config.training.batch_size_train == 256
    assert config.training.batch_size_eval == 64
    assert config.training.eval_model_chunk_rows == 64
    assert config.training.eval_backtest_chunk_rows == 64
    assert len(config.data.feature_include) == 99
    assert sum(
        config.training.transformer_base_portfolio.temporal_basis_components_by_family.values()
    ) == 524
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.data.tw_futures_expiry_settlement_valuation is True
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_pretrained_guard_cash_capital10m_v2"
    )


def test_integer_0845_funding_safe_trajectory_v3_is_a_fresh_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_pretrained_guard_funding_safe_trajectory_full_features_"
        "multi_basis_projection_l1_cash_capital10m.yaml"
    )
    basis = config.training.transformer_base_portfolio
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 256
    assert config.training.batch_size_eval == 64
    assert config.training.futures_portfolio_optimizer_step_per_trajectory is True
    assert config.training.lr_scheduler_warmup_steps == 256
    assert basis.futures_denomination_aware_output is True
    assert basis.futures_denomination_hard_projection is False
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_pretrained_guard_funding_safe_trajectory_"
        "cash_capital10m_v3"
    )
    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    assert manifest["contracts"]["model"]["contract_version"] == 5
    assert manifest["contracts"]["training"]["optimizer"]["step_cadence"] == (
        "full_chronological_trajectory"
    )
    futures_contract = manifest["contracts"]["trading"][
        "taiwan_stock_context_futures_portfolio"
    ]
    assert futures_contract["integer_training_forward"] == "exact_integer_account_v2"
    assert futures_contract["denomination_hard_projection_owner"] == (
        "exact_integer_executor_dynamic_equity"
    )


def test_integer_0845_stable_full_finetune_v4_isolated_training_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_stable_full_finetune_full_features_multi_basis_"
        "projection_l1_cash_capital10m_v4.yaml"
    )
    basis = config.training.transformer_base_portfolio
    assert config.training.epochs == 1000
    assert config.training.early_stopping_no_improve_ratio == pytest.approx(0.1)
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.futures_portfolio_recoverable_backward is True
    assert config.training.futures_portfolio_optimizer_step_per_trajectory is True
    assert config.training.lr_scheduler_warmup_steps == 32
    assert (
        config.training.pretrained_initialization_require_improvement_over_flat_cash
        is True
    )
    assert config.training.pretrained_initialization_trainable_parameter_prefixes == []
    assert len(config.data.feature_include) == 99
    assert sum(basis.temporal_basis_components_by_family.values()) == 524
    assert basis.futures_denomination_aware_output is True
    assert basis.futures_denomination_hard_projection is False
    assert config.trading.tw_futures_portfolio_integer_contracts is True
    assert config.data.tw_futures_expiry_settlement_valuation is True
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_stable_full_finetune_cash_capital10m_v4"
    )

    manifest = build_checkpoint_manifest(
        _stock_panel(), config, include_data_content=False
    )
    training_contract = manifest["contracts"]["training"]
    assert training_contract["optimizer"]["step_cadence"] == (
        "full_chronological_trajectory"
    )
    assert training_contract["scheduler"]["warmup_steps"] == 32
    assert training_contract["fold_continuation"][
        "pretrained_initialization_require_improvement_over_flat_cash"
    ] is True


def test_integer_0845_pretrained_guard_uses_fold_matched_source_and_exact_loss() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_0845_integer_futures_open_"
        "pretrained_guard_full_features_multi_basis_projection_l1_"
        "cash_capital10m.yaml"
    )
    assert config.training.epochs == 1000
    assert config.training.model_name == "cross_sectional_all_futures"
    assert config.training.futures_portfolio_training_surrogate_only is False
    assert config.training.futures_portfolio_recoverable_backward is True
    assert config.training.pretrained_initialization_root.endswith(
        "tw_stock_context_all_futures_carry_multi_basis_projection_l1_"
        "v3_full1000_batch96_chunk64"
    )
    assert config.training.pretrained_initialization_fold_policy == (
        "matching_train_and_validation_years"
    )
    assert config.training.pretrained_initialization_feature_adapter == (
        "transformer_feature_projection_by_name"
    )
    assert config.training.pretrained_initialization_require_exact_backbone is True
    assert config.training.pretrained_initialization_validation_guard is True
    assert config.training.warm_start_from_previous_fold is False
    assert config.training.pretrained_initialization_trainable_parameter_prefixes == [
        "feature_proj.",
        "futures_underlying_",
        "futures_denomination_encoder.",
        "futures_current_open_encoder.",
        "futures_action_head.",
    ]
    assert config.data.feature_exclude == []
    assert config.data.feature_shift_next_session == [
        "next_session_open_gap_logret"
    ]
    assert config.data.tw_futures_current_open_feature is True
    assert config.trading.tw_futures_portfolio_integer_initial_capital == pytest.approx(
        10_000_000.0
    )
    assert config.training.transformer_base_portfolio.projection_l1_scale_by_active_count
    assert config.runner.output_dir.endswith(
        "tw_stock_context_all_futures_carry_0845_integer_futures_open_exact_"
        "recoverable_pretrained_guard_stock_tminus1_cash_capital10m_v3"
    )
