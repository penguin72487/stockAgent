"""Annual train accounts reuse the exact ledger and one fixed-policy update."""
from dataclasses import replace
from datetime import date
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.training.day_trade_carry_bridge import bind_physical_carry_loss
from test_day_trade_carry_training import fixture, Policy, train_epoch
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from stockagent.config import load_config
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
from stockagent.training.trainer import _validate_annual_day_trade_training_boundaries


def annual_fixture():
    split, runtime, loss_fn = fixture()
    days = [date(2023, 12, 29), date(2024, 1, 2), date(2024, 12, 31),
            date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)]
    sessions = tuple(replace(s, day=d.toordinal(), terminal_liquidation_price=s.official_open * 1.01)
                     for s, d in zip(runtime.day_trade_carry_source.sessions, days))
    return split, replace(runtime, day_trade_training_annual_episodes=True,
                          day_trade_carry_source=replace(runtime.day_trade_carry_source, sessions=sessions, session_days=())), loss_fn


def independent_year_reference(split, runtime, loss_fn, model):
    total = 0
    for start, end in [(0, 2), (2, 5)]:
        batch = split.batch_by_rows(start, end, torch.device("cpu"), False)
        bound = bind_physical_carry_loss(loss_fn, source=runtime.day_trade_carry_source,
            split=split, start=start, end=end, device=torch.device("cpu"), previous=None)
        value = bound(model(batch["x"], batch["tradable_mask"]), batch["future_log_returns"],
            batch["tradable_mask"], benchmark_returns=batch["benchmark"],
            day_trade_eligible_mask=batch["day_trade_eligible_mask"],
            day_trade_can_buy_open_mask=batch["day_trade_can_buy_open_mask"],
            day_trade_can_sell_open_mask=batch["day_trade_can_sell_open_mask"],
            can_short_open_mask=batch["can_short_open_mask"], aux_outputs={}, **_LOSS_OPTIONS)
        total = total + value * ((end - start) / len(split))
    return total


@pytest.mark.parametrize("batch_size", [2, 3, 5])
def test_annual_accounts_match_independent_year_losses(batch_size):
    split, runtime, loss_fn = annual_fixture()
    model = Policy()
    expected = independent_year_reference(split, runtime, loss_fn, model)
    # With all five dates in one batch, the reference also shares precisely the
    # same within-year gradient horizon; smaller batches intentionally truncate.
    expected.backward()
    expected_grad = model.action.grad.clone()
    model.zero_grad()
    actual, timing = train_epoch(split, runtime, loss_fn, model,
        batch_size=batch_size, optimizer_step_per_trajectory=True)
    torch.testing.assert_close(actual.double(), expected.double(), rtol=1e-6, atol=1e-8)
    assert timing.optimizer_steps == 1
    if batch_size == 5:
        torch.testing.assert_close(model.action.grad, expected_grad, rtol=1e-5, atol=1e-8)


def test_annual_boundary_refuses_to_discard_carried_inventory():
    split, runtime, loss_fn = annual_fixture()
    source = runtime.day_trade_carry_source
    runtime = replace(runtime, day_trade_carry_source=replace(source,
        sessions=tuple(replace(s, terminal_liquidation_price=None) for s in source.sessions)))
    with pytest.raises(ValueError, match="cannot discard holdings"):
        train_epoch(split, runtime, loss_fn, Policy(), batch_size=3,
                    optimizer_step_per_trajectory=True)


def test_annual_account_refuses_lookback_shifted_first_year():
    split, runtime, loss_fn = fixture()
    days = [
        date(2024, 12, 27),
        date(2024, 12, 30),
        date(2024, 12, 31),
        date(2025, 1, 2),
        date(2025, 1, 3),
        date(2025, 1, 6),
    ]
    source = runtime.day_trade_carry_source
    sessions = tuple(
        replace(session, day=day.toordinal())
        for session, day in zip(source.sessions, days, strict=True)
    )
    runtime = replace(
        runtime,
        day_trade_training_annual_episodes=True,
        day_trade_carry_source=replace(source, sessions=sessions, session_days=()),
    )

    with pytest.raises(ValueError, match="lookback rows may provide feature context"):
        train_epoch(
            split,
            runtime,
            loss_fn,
            Policy(),
            batch_size=2,
            optimizer_step_per_trajectory=True,
        )


def test_annual_accounts_compose_with_sub_lot_recovery_and_daily_cache():
    split, runtime, loss_fn = annual_fixture()
    source = runtime.day_trade_carry_source
    assert source is not None
    recovery_loss = partial(loss_fn, day_trade_sub_lot_recovery=True)
    model = Policy()

    first, first_timing = train_epoch(
        split,
        runtime,
        recovery_loss,
        model,
        batch_size=2,
        optimizer_step_per_trajectory=True,
    )
    second, second_timing = train_epoch(
        split,
        runtime,
        recovery_loss,
        model,
        batch_size=3,
        optimizer_step_per_trajectory=True,
    )

    assert torch.isfinite(first) and torch.isfinite(second)
    assert first_timing.optimizer_steps == second_timing.optimizer_steps == 1
    assert source._sub_lot_recovery_cache_stats == {"hits": 5, "misses": 5}


@pytest.mark.parametrize("flag", ["day_trade_training_annual_episodes", "day_trade_sub_lot_recovery"])
def test_training_experiments_have_distinct_checkpoint_contracts(flag, tmp_path):
    root = Path("configs/deployments/tw_day_trade_v8_ofat_resume_corrected_control.yaml").resolve()
    baseline = load_config(root)
    path = tmp_path / "candidate.yaml"
    path.write_text(f"base_config: {root}\ntraining:\n  {flag}: true\n")
    candidate = load_config(path)
    before, after = _training_checkpoint_contract(baseline), _training_checkpoint_contract(candidate)
    assert flag not in before["fold_continuation"]
    assert flag in after["fold_continuation"]
    after["fold_continuation"].pop(flag)
    assert before == after
    path.write_text(path.read_text() + "trading:\n  tw_day_trade_terminal_liquidation_unlimited_capacity: false\n")
    with pytest.raises(ValueError):
        load_config(path)


def test_corrected_annual_config_uses_prior_year_only_as_lookback_context():
    config = load_config(
        Path(
            "configs/deployments/"
            "tw_day_trade_v8_combined_annual_log_cash_sub_lot_"
            "first_session_fold10_v4.yaml"
        ).resolve()
    )
    contract = _training_checkpoint_contract(config)

    assert config.walk_forward.lookback_context == "panel_history"
    assert config.walk_forward.split_start_year == 2015
    assert config.runner.start_fold == 10
    assert contract["fold_continuation"]["day_trade_training_annual_episodes"] == (
        "calendar_year_first_session_fresh_capital_v2"
    )


def test_corrected_all_fold_config_starts_at_fold_one_with_causal_pretraining():
    config = load_config(
        Path(
            "configs/deployments/"
            "tw_day_trade_v8_combined_annual_log_cash_sub_lot_"
            "first_session_all_folds_v5.yaml"
        ).resolve()
    )

    assert config.runner.start_fold == 1
    assert config.training.warm_start_from_previous_fold is False
    assert config.training.pretrained_initialization_fold_policy == (
        "matching_validation_and_causal_train_superset"
    )
    assert config.runner.output_dir.endswith("first_session_all_folds_v5")


def test_annual_preflight_rejects_first_year_consumed_by_lookback():
    config = load_config(
        Path(
            "configs/deployments/"
            "tw_day_trade_v8_combined_annual_log_cash_sub_lot_fold11_v3.yaml"
        ).resolve()
    )
    dates = np.concatenate(
        (
            np.arange("2014-01-01", "2014-03-01", dtype="datetime64[D]"),
            np.arange("2015-01-01", "2015-03-01", dtype="datetime64[D]"),
        )
    )
    panel = SimpleNamespace(dates=dates)
    fold = SimpleNamespace(
        fold_id=1,
        train_years=[2014, 2015],
        train_indices=np.arange(dates.size, dtype=np.int64),
    )

    with pytest.raises(ValueError, match="year=2014.*do not shift"):
        _validate_annual_day_trade_training_boundaries(panel, [fold], config)


@pytest.mark.parametrize("compile_loss", ["true", "null"])
def test_sub_lot_recovery_requires_explicit_eager_loss(compile_loss, tmp_path):
    root = Path("configs/deployments/tw_day_trade_v8_ofat_resume_corrected_control.yaml").resolve()
    path = tmp_path / "candidate.yaml"
    path.write_text(f"base_config: {root}\ntraining:\n  day_trade_sub_lot_recovery: true\n  compile_loss: {compile_loss}\n")
    with pytest.raises(ValueError, match="compile_loss=false"):
        load_config(path)


def test_sub_lot_source_labels_are_reused_across_epochs():
    split, runtime, loss_fn = fixture()
    source = runtime.day_trade_carry_source
    assert source is not None
    sessions = tuple(
        replace(
            session,
            terminal_liquidation_price=session.official_open * 1.01,
        )
        for session in source.sessions
    )
    source = replace(source, sessions=sessions)
    runtime = replace(runtime, day_trade_carry_source=source)
    recovery_loss = partial(loss_fn, day_trade_sub_lot_recovery=True)
    model = Policy()

    first, _ = train_epoch(
        split,
        runtime,
        recovery_loss,
        model,
        batch_size=2,
        optimizer_step_per_trajectory=True,
    )
    first_stats = dict(source._sub_lot_recovery_cache_stats)
    second, _ = train_epoch(
        split,
        runtime,
        recovery_loss,
        model,
        # Change the batch partition to prove the cache is keyed by physical
        # session day rather than by the first epoch's batch boundaries.
        batch_size=3,
        optimizer_step_per_trajectory=True,
    )
    second_stats = dict(source._sub_lot_recovery_cache_stats)

    assert torch.isfinite(first) and torch.isfinite(second)
    assert first_stats == {"hits": 0, "misses": 5}
    assert second_stats == {"hits": 5, "misses": 5}
