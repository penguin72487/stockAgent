"""Independent accounts, uneven date weights and real two-rank updates."""
from dataclasses import replace
from datetime import timedelta
import multiprocessing as mp
from types import SimpleNamespace

import pytest
import torch
import torch.distributed as dist
from torch.amp import GradScaler

from stockagent.config import TrainingConfig, load_config
from stockagent.training import trainer
from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
from stockagent.training.episode_parallel import plan_annual_episode_ownership, validate_annual_episode_receipt
from test_day_trade_carry_training import Policy
from test_day_trade_training_episodes import annual_fixture, independent_year_reference
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from test_day_trade_shifted_annual import shifted_fixture, independent_shifted_reference


class PolicyWithUnused(Policy):
    def __init__(self):
        super().__init__()
        self.unused = torch.nn.Parameter(torch.tensor(.123))


class CountingSGD(torch.optim.SGD):
    def __init__(self, parameters):
        super().__init__(parameters, lr=.001, weight_decay=.01)
        self.calls = 0

    def step(self, *args, **kwargs):
        self.calls += 1
        return super().step(*args, **kwargs)


class Scheduler:
    calls = 0

    def step(self):
        self.calls += 1


def test_whole_account_plan_has_exact_disjoint_coverage_and_no_padding():
    split, runtime, _ = annual_fixture()
    padded = trainer._pad_windowed_training_split(split, 6)
    plan = plan_annual_episode_ownership(padded, runtime.day_trade_carry_source,
        period_contract=None, world_size=2, global_batch_rows=6)
    assert sorted(plan.rows_by_rank) == [2, 3]
    assert plan.total_valid_rows == 5
    assert sorted(period for periods in plan.periods_by_rank for period in periods) == [2024, 2025]
    assert sorted(row for spans in plan.spans_by_rank for start, stop in spans
                  for row in range(start, stop)) == list(range(5))
    assert all(len(periods) == 1 for periods in plan.periods_by_rank)
    checked = validate_annual_episode_receipt(plan.receipt(), train_rows=5,
                                              global_batch_rows=6, world_size=2)
    assert checked["batches_by_rank"] == [1, 1]
    assert checked["padded_slots"] == 6


@pytest.mark.parametrize("damage", ["duplicate", "missing", "weight", "span", "rank", "cadence"])
def test_annual_admission_rejects_false_coverage_or_update_receipts(damage):
    from copy import deepcopy
    split, runtime, _ = annual_fixture()
    plan = plan_annual_episode_ownership(trainer._pad_windowed_training_split(split, 6),
        runtime.day_trade_carry_source, period_contract=None, world_size=2, global_batch_rows=6)
    import json
    receipt = deepcopy(json.loads(json.dumps(plan.receipt())))
    if damage == "duplicate":
        receipt["accounts_by_rank"][1][0][0] = receipt["accounts_by_rank"][0][0][0]
        receipt["periods_by_rank"][1][0] = receipt["periods_by_rank"][0][0]
    elif damage == "missing":
        receipt["total_valid_rows"] = 4
    elif damage == "weight":
        receipt["rows_by_rank"][0] += 1
    elif damage == "span":
        receipt["spans_by_rank"][0][0][1] -= 1
    elif damage == "rank":
        receipt["spans_by_rank"].pop()
    else:
        receipt["update_cadence"] = "per_batch"
    with pytest.raises(ValueError):
        validate_annual_episode_receipt(receipt, train_rows=5, global_batch_rows=6, world_size=2)


def test_partial_account_and_internal_mask_hole_rejected():
    split, runtime, _ = annual_fixture()
    late = replace(split, valid_indices=split.valid_indices[1:])
    with pytest.raises(ValueError, match="first configured physical session"):
        plan_annual_episode_ownership(late, runtime.day_trade_carry_source,
            period_contract=None, world_size=2, global_batch_rows=6)
    hole = replace(split, sample_mask=torch.tensor([True, False, True, True, True]))
    with pytest.raises(ValueError, match="inert padding"):
        plan_annual_episode_ownership(hole, runtime.day_trade_carry_source,
            period_contract=None, world_size=2, global_batch_rows=6)


def test_shifted_ownership_uses_configured_periods_including_january_context():
    split, runtime, _ = shifted_fixture()
    plan = plan_annual_episode_ownership(split, runtime.day_trade_carry_source,
        period_contract=runtime.day_trade_training_period_contract,
        world_size=2, global_batch_rows=6)
    accounts = dict(zip((period for periods in plan.periods_by_rank for period in periods),
                        (span for spans in plan.spans_by_rank for span in spans), strict=True))
    assert accounts == {2023: (0, 1), 2024: (1, 3), 2025: (3, 5)}


def _worker(rank, rendezvous, results, shifted):
    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{rendezvous}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=40))
    try:
        split, runtime, loss_fn = shifted_fixture() if shifted else annual_fixture()
        split = trainer._pad_windowed_training_split(split, 6)
        policy = PolicyWithUnused()
        config = SimpleNamespace(training=TrainingConfig(non_blocking_transfer=False,
            ddp_broadcast_buffers=False, ddp_trajectory_gradient_sync="trajectory",
            day_trade_annual_episode_sharding=True))
        model = trainer._wrap_distributed_data_parallel_model(policy, config=config,
            device=torch.device("cpu"))
        optimizer, scheduler = CountingSGD(model.parameters()), Scheduler()
        def forbidden(*args, **kwargs):
            raise AssertionError("local episode actions or metadata invoked a collective")
        trainer._all_gather_autograd = forbidden
        trainer._all_gather_no_grad = forbidden
        # Different accounts need different numbers of forwards. Calling DDP
        # forward here would reintroduce hidden buffer/parameter rendezvous.
        model.forward = forbidden
        options = dict(_LOSS_OPTIONS, batch_size=6, device=torch.device("cpu"),
            amp_dtype=None, non_blocking=False, grad_clip_norm=.05,
            execution_runtime=runtime, max_volume_participation=.5,
            volume_participation_equity=10_000_000., optimizer_step_per_trajectory=True,
            ddp_trajectory_gradient_sync="trajectory", day_trade_annual_episode_sharding=True,
            lr_scheduler=scheduler, lr_scheduler_interval="step")
        with pytest.raises(ValueError, match="world size differs"):
            trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split, optimizer,
                GradScaler("cpu", enabled=False), day_trade_annual_episode_world_size=4, **options)
        value, timing = trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split,
            optimizer, GradScaler("cpu", enabled=False), **options)
        assert timing.optimizer_steps == optimizer.calls == scheduler.calls == 1
        assert policy.unused.grad is None and policy.unused.item() == pytest.approx(.123)
        assert timing.gradient_norm_observations == 1 and timing.gradient_norm_zero_batches == 0
        # Independent accounts can use a parameter on one rank only. Verify
        # zero contribution without activating global unused weight decay.
        partial = torch.nn.ParameterList([torch.nn.Parameter(torch.tensor(0.)) for _ in range(3)])
        partial[0].grad = torch.tensor(float(2 + rank))
        partial[1].grad = torch.tensor(4.) if rank == 0 else None
        trainer._synchronize_trajectory_gradients(partial, allow_partial_presence=True)
        assert partial[0].grad.item() == 2.5 and partial[1].grad.item() == 2.
        assert partial[2].grad is None
        results.put((rank, {"loss": value.item(), "parameter": policy.action.detach().tolist(),
                            "gradient": policy.action.grad.tolist(),
                            "norm": timing.gradient_norm_before_clip_sum,
                            "batches": timing.batches}))
    except BaseException as exc:
        results.put((rank, f"{type(exc).__name__}: {exc}"))
    finally:
        dist.destroy_process_group()


@pytest.mark.parametrize("shifted", [False, True])
def test_two_ranks_match_independent_account_gradient_clip_and_single_update(tmp_path, monkeypatch, shifted):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    split, runtime, loss_fn = shifted_fixture() if shifted else annual_fixture()
    reference = PolicyWithUnused()
    optimizer = CountingSGD(reference.parameters())
    expected = (independent_shifted_reference(split, runtime, loss_fn, reference, torch.device("cpu"))
                if shifted else independent_year_reference(split, runtime, loss_fn, reference))
    expected.backward()
    norm = torch.nn.utils.clip_grad_norm_(reference.parameters(), .05).item()
    gradient = reference.action.grad.detach().clone()
    optimizer.step()
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_worker, args=(rank, str(tmp_path / "init"), results, shifted))
               for rank in range(2)]
    for worker in workers:
        worker.start()
    observed = [results.get(timeout=65) for _ in workers]
    for worker in workers:
        worker.join(timeout=15)
        if worker.is_alive():
            worker.terminate()
        assert worker.exitcode == 0
    for _, result in observed:
        assert isinstance(result, dict), result
        assert result["loss"] == pytest.approx(expected.item(), rel=2e-6, abs=1e-7)
        torch.testing.assert_close(torch.tensor(result["gradient"]), gradient, rtol=2e-5, atol=1e-7)
        torch.testing.assert_close(torch.tensor(result["parameter"]), reference.action.detach(), rtol=2e-6, atol=1e-7)
        assert result["norm"] == pytest.approx(norm, rel=2e-5, abs=1e-7)
    if shifted:
        assert sorted(result["batches"] for _, result in observed) == [1, 2]


def test_disabled_flag_preserves_legacy_fingerprint_and_enabled_mode_changes_it():
    config = load_config("configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml")
    assert "day_trade_annual_episode_sharding" not in _configuration_fingerprint_snapshot(config)["training"]
    changed = replace(config, training=replace(config.training,
        day_trade_annual_episode_sharding=True, ddp_broadcast_buffers=False,
        ddp_trajectory_gradient_sync="trajectory"))
    assert _training_checkpoint_contract(changed) != _training_checkpoint_contract(config)
    other_world = replace(changed, training=replace(changed.training, day_trade_annual_episode_world_size=4))
    assert _training_checkpoint_contract(changed) != _training_checkpoint_contract(other_world)
