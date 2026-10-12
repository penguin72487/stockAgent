"""TW annual accounts share the canonical shifted ownership contract."""
from dataclasses import replace
from datetime import date, timedelta
import os
from pathlib import Path

import numpy as np
import pytest
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel

from stockagent.config import load_config
from stockagent.data.walkforward import build_expanding_year_folds, year_period_contract
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
from stockagent.training.day_trade_carry_bridge import bind_physical_carry_loss
from stockagent.training.trainer import (
    _pad_windowed_training_split, _validate_annual_day_trade_training_boundaries,
)
from test_day_trade_carry_training import fixture, Policy, train_epoch
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from types import SimpleNamespace


def shifted_fixture(device="cpu"):
    split, runtime, loss_fn = fixture(device=device)
    days = [date(2023, 12, 29), date(2024, 1, 2), date(2024, 12, 31),
            date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)]
    source = runtime.day_trade_carry_source
    sessions = tuple(replace(s, day=d.toordinal(), terminal_liquidation_price=s.official_open * 1.01)
                     for s, d in zip(source.sessions, days, strict=True))
    contract = year_period_contract(np.asarray(days, dtype="datetime64[D]"), 1)
    return split, replace(runtime, day_trade_training_annual_episodes=True,
        day_trade_training_period_contract=contract,
        day_trade_carry_source=replace(source, sessions=sessions, session_days=())), loss_fn


def independent_shifted_reference(split, runtime, loss_fn, model, device):
    total = 0
    # Deliberately NOT calendar cuts: Jan 2 belongs to the previous period.
    for start, end in [(0, 1), (1, 3), (3, 5)]:
        batch = split.batch_by_rows(start, end, device, False)
        bound = bind_physical_carry_loss(loss_fn, source=runtime.day_trade_carry_source,
            split=split, start=start, end=end, device=device, previous=None)
        value = bound(model(batch["x"], batch["tradable_mask"]), batch["future_log_returns"],
            batch["tradable_mask"], benchmark_returns=batch["benchmark"],
            day_trade_eligible_mask=batch["day_trade_eligible_mask"],
            day_trade_can_buy_open_mask=batch["day_trade_can_buy_open_mask"],
            day_trade_can_sell_open_mask=batch["day_trade_can_sell_open_mask"],
            can_short_open_mask=batch["can_short_open_mask"], aux_outputs={}, **_LOSS_OPTIONS)
        total = total + value * ((end - start) / len(split))
    return total


@pytest.mark.parametrize("batch_size", [1, 2, 3, 5])
def test_shifted_accounts_match_independent_owned_periods(batch_size):
    split, runtime, loss_fn = shifted_fixture()
    model = Policy()
    expected = independent_shifted_reference(split, runtime, loss_fn, model, torch.device("cpu"))
    expected.backward()
    expected_gradient = model.action.grad.clone()
    model.zero_grad()
    actual, timing = train_epoch(split, runtime, loss_fn, model, batch_size=batch_size,
                                optimizer_step_per_trajectory=True)
    torch.testing.assert_close(actual.double(), expected.double(), rtol=1e-6, atol=1e-8)
    assert timing.optimizer_steps == 1
    if batch_size == 5:
        torch.testing.assert_close(model.action.grad, expected_gradient, rtol=1e-5, atol=1e-8)


def test_shifted_first_owned_session_cannot_be_silently_omitted():
    split, runtime, loss_fn = shifted_fixture()
    split._valid_indices_cpu = split._valid_indices_cpu[2:]
    with pytest.raises(ValueError, match="annual training episode begins after"):
        bind_physical_carry_loss(loss_fn, source=runtime.day_trade_carry_source,
            split=split, start=0, end=1, device=torch.device("cpu"), previous=None,
            reset_calendar_year=True, reset_period_contract=runtime.day_trade_training_period_contract)


def test_shifted_calendar_validator_and_checkpoint_share_boundary(tmp_path):
    base = Path("configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml").resolve()
    config = load_config(base)
    before = _training_checkpoint_contract(config)
    config.walk_forward.year_boundary_mode = "lookback_shifted"
    config.walk_forward.split_start_year = 2014
    after = _training_checkpoint_contract(config)
    assert before["fold_continuation"]["day_trade_training_annual_episodes"] == "calendar_year_first_session_fresh_capital_v2"
    assert after["fold_continuation"]["day_trade_training_annual_episodes"] == "observed_session_shifted_fresh_capital_v1"
    for lookback in (32, 256):
        config.training.lookback = lookback
        dates = np.arange("2014-01-06", "2026-10-02", dtype="datetime64[D]")
        dates = dates[np.is_busday(dates)]
        folds = build_expanding_year_folds(dates, 1, split_start_year=2014,
                                           year_boundary_offset_sessions=lookback)
        report = _validate_annual_day_trade_training_boundaries(SimpleNamespace(dates=dates), [folds[9]], config)
        assert report["year_boundary_offset_sessions"] == lookback
        assert report["groups"][0]["years"][0]["first_session"] == str(dates[lookback])
        assert report["contract"] == after["fold_continuation"]["day_trade_training_annual_episodes"]


@pytest.mark.skipif(int(os.environ.get("WORLD_SIZE", "1")) != 2,
                    reason="requires explicit two-GPU torchrun")
def test_shifted_annual_full_batch_two_gpu_ddp_gradient():
    """Tiny semantic DDP acceptance, not a full-feature VRAM/speed claim."""
    import stockagent.training.trainer as trainer
    expected_code = os.environ.get("STOCKAGENT_EXPECTED_CODE_ROOT")
    if expected_code:
        assert Path(trainer.__file__).resolve().is_relative_to(Path(expected_code).resolve())
    rank = int(os.environ["LOCAL_RANK"])
    print(f"rank={rank} canonical_trainer={trainer.__file__}", flush=True)
    torch.cuda.set_device(rank)
    dist.init_process_group("nccl", timeout=timedelta(seconds=120))
    try:
        device = torch.device("cuda", rank)
        split, runtime, loss_fn = shifted_fixture(device=device)
        policy = Policy().to(device)
        reference = Policy().to(device)
        expected = independent_shifted_reference(split, runtime, loss_fn, reference, device)
        expected.backward()
        split = _pad_windowed_training_split(split, 6)
        ddp = DistributedDataParallel(policy, device_ids=[rank])
        actual, timing = train_epoch(split, runtime, loss_fn, ddp, device="cuda", ddp=True,
            batch_size=6, optimizer_step_per_trajectory=True)
        failure = None
        try:
            torch.testing.assert_close(actual.double(), expected.double(), rtol=1e-6, atol=1e-8)
            torch.testing.assert_close(policy.action.grad, reference.action.grad, rtol=1e-5, atol=1e-8)
            assert timing.optimizer_steps == 1
        except AssertionError as exc:
            failure = f"rank={rank}: {exc}"
        failures = [None] * 2
        dist.all_gather_object(failures, failure)
        assert not any(failures), str(failures)
    finally:
        dist.destroy_process_group()
