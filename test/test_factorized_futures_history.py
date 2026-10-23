"""Older stock context must not backdate cash eligibility or future observations."""
from dataclasses import asdict, replace
from datetime import date

import numpy as np
import polars as pl
import pytest

from scripts.prepare_tw_day_trade_factorized_panel import (
    causal_state_blocks,
    stock_context_preparation_contract,
    stock_context_source_sessions,
)
from stockagent.config import load_config
from stockagent.data.tw_public_release_schedule import RULES
from stockagent.data.walkforward import build_expanding_year_folds, year_boundary_offset_sessions


CONFIG = "configs/markets/tw_stock_futures_carry_0845_history2011_20261021_v5.yaml"


def test_history2011_preserves_actor_account_and_neutral_cash_open():
    base = load_config("configs/markets/tw_stock_futures_carry_0845_openref_20261021_v4.yaml")
    config = load_config(CONFIG)
    context = stock_context_preparation_contract(config)
    assert context["futures_context_only"] and context["feature_lag"] == 1
    assert config.training.financial_transformer == base.training.financial_transformer
    assert config.training.lookback == base.training.lookback == 32
    assert config.training.batch_size_train == base.training.batch_size_train == 128
    assert config.data.factorized_transfer_mode == "compact_cuda_cached"
    assert config.training.pretrained_initialization_root is None
    assert not config.training.warm_start_from_previous_fold
    assert config.walk_forward.split_start_year == 2011
    assert config.trading.tw_futures_portfolio_holding_policy == "carry"
    assert config.trading.tw_futures_portfolio_integer_contracts
    assert config.trading.tw_futures_portfolio_valuation_research_contract is None
    assert not config.data.use_tw_public_rules


@pytest.mark.parametrize("invalid", ["cash_rules", "cash_open", "resume"])
def test_feature_only_extension_rejects_wrong_eligibility_or_clock(invalid):
    config = load_config(CONFIG)
    if invalid == "cash_rules":
        config = replace(config, data=replace(config.data, use_tw_public_rules=True))
    elif invalid == "cash_open":
        config = replace(config, data=replace(config.data, feature_zero_fill=[]))
    else:
        config = replace(config, runner=replace(config.runner, resume=True))
    with pytest.raises(ValueError):
        stock_context_preparation_contract(config)


def test_observed_calendar_extends_futures_without_changing_day_trade_calendar():
    config = load_config(CONFIG)
    context = stock_context_preparation_contract(config)
    observed = [date(2010, 12, 30), date(2011, 1, 3), date(2013, 1, 2), date(2014, 1, 6)]
    assert stock_context_source_sessions(observed, end=date(2013, 1, 2), context=context) == observed[:3]
    cash = dict(context, futures_context_only=False)
    assert stock_context_source_sessions(observed, end=date(2014, 1, 6), context=cash) == observed[2:]


def test_longer_training_history_keeps_validation_and_test_ownership():
    config = load_config(CONFIG)
    dates = np.arange('2011-01-03', '2026-09-25', dtype='datetime64[D]')
    dates = dates[np.is_busday(dates)]  # A calendar fixture, not historical observations.
    options = dict(min_train_years=1, val_years=1, require_future_test_year=True,
        year_boundary_offset_sessions=year_boundary_offset_sessions(config))
    new = build_expanding_year_folds(dates, split_start_year=2011, **options)[-1]
    recent = dates[dates >= np.datetime64('2020-03-23')]
    old = build_expanding_year_folds(recent, split_start_year=2020, **options)[-1]
    assert new.fold_id == 14 and old.fold_id == 5
    assert new.train_years[0] == 2011
    np.testing.assert_array_equal(dates[new.val_indices], recent[old.val_indices])
    np.testing.assert_array_equal(dates[new.test_indices], recent[old.test_indices])
    assert dates[new.train_indices[-1]] < dates[new.val_indices[0]]


def test_first_2011_feature_carries_released_history_without_future_leakage():
    # The canonical lag-1 ABI stores decision t's ready facts in row t-1.
    # Retaining the observed 2010 session makes the first 2011 decision usable.
    days = [date(2010, 12, 30), date(2011, 1, 3), date(2011, 1, 4)]
    definition = {"feature": "reported", "rule": asdict(RULES["quarter"])}
    events = pl.DataFrame({"date": [date(2010, 12, 30), date(2011, 1, 4)],
        "symbol": ["2330", "2330"], "feature": ["reported", "reported"],
        "value": [17., 999.], "period": ["2010Q3", "2010Q4"]})
    lifecycle = np.full((3, 1), date(2000, 1, 1).toordinal(), dtype=np.int32)
    options = dict(model_channel_policy="value_only", max_block_bytes=1024)
    values = np.concatenate([v for _, v, _ in causal_state_blocks(
        days, ["2330"], [definition], events, lifecycle, **options)])
    changed = events.with_columns(pl.when(pl.col("date") == days[2])
        .then(123456.).otherwise(pl.col("value")).alias("value"))
    other = np.concatenate([v for _, v, _ in causal_state_blocks(
        days, ["2330"], [definition], changed, lifecycle, **options)])
    assert values[0, 0, 0] == other[0, 0, 0] == 17.
    assert values[1, 0, 0] == 999. and other[1, 0, 0] == 123456.
    assert values[2, 0, 0] == other[2, 0, 0] == 0.  # No next decision.
