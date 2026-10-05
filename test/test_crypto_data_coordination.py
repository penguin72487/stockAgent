"""Small error-channel tests; no market data, models, or training jobs."""

from __future__ import annotations

import ast
from datetime import timedelta
import inspect
import multiprocessing as mp
from types import SimpleNamespace

import pytest
import torch

from stockagent.backtest.crypto_perpetual import CryptoPerpetualDataError
from stockagent.backtest.futures_data_validity import FuturesCarryDataError
from stockagent.training import trainer


def _split():
    return SimpleNamespace(execution_mode="crypto_perpetual", overnight_log_returns=None)


def test_crypto_invalid_chunk_clears_all_accumulated_gradients():
    parameter = torch.nn.Parameter(torch.tensor(1.0))
    parameter.grad = torch.tensor(7.0)
    optimizer = torch.optim.AdamW([parameter])
    with pytest.raises(CryptoPerpetualDataError, match="split row 12.*missing mark"):
        with trainer._carry_loss_data_guard(_split(), optimizer, torch.device("cpu"), 12):
            raise CryptoPerpetualDataError("missing mark")
    assert parameter.grad is None
    assert parameter.item() == 1.0
    assert not optimizer.state


def test_valid_crypto_chunk_retains_accumulated_gradients():
    parameter = torch.nn.Parameter(torch.tensor(1.0))
    parameter.grad = torch.tensor(7.0)
    optimizer = torch.optim.AdamW([parameter])
    with trainer._carry_loss_data_guard(_split(), optimizer, torch.device("cpu"), 0):
        pass
    assert parameter.grad.item() == 7.0


@pytest.mark.parametrize("error", [
    CryptoPerpetualDataError("held valuation missing"),
    CryptoPerpetualDataError("held valuation missing", evidence={"row": 2, "symbol_index": 1}),
    FuturesCarryDataError({"row": 3, "reason": "held_settlement_missing"}),
])
def test_capture_restores_product_specific_error(error):
    errors = []
    with trainer._capture_carry_data_error(errors):
        raise error
    assert len(errors) == 1
    with pytest.raises(type(error)) as restored:
        trainer._raise_carry_data_error(errors[0])
    assert str(restored.value) == str(error)


def test_capture_does_not_hide_unrelated_programming_failure():
    errors = []
    with pytest.raises(RuntimeError, match="programming failure"):
        with trainer._capture_carry_data_error(errors):
            raise RuntimeError("programming failure")
    assert errors == []


def test_epoch_eval_crypto_error_rendezvous_precedes_scalar_broadcast():
    tree = ast.parse(inspect.getsource(trainer._run_training_impl))
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    sync_calls = [node for node in ast.walk(tree)
                  if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                  and node.func.id == "_synchronize_carry_data_error"]
    assert len(sync_calls) == 1
    node = sync_calls[0]
    owning_if = None
    while node in parents:
        node = parents[node]
        if isinstance(node, ast.If) and "crypto_perpetual" in ast.unparse(node.test):
            owning_if = node
            break
    assert owning_if is not None
    crypto_runtime = SimpleNamespace(mode="crypto_perpetual")
    noncarry_config = SimpleNamespace(trading=SimpleNamespace(
        tw_stock_futures_day_trade_residual_policy="fail",
    ))
    assert eval(compile(ast.Expression(owning_if.test), "<condition>", "eval"), {
        "config": noncarry_config, "execution_runtime": crypto_runtime,
    })
    # All per-epoch scalar broadcasts must remain later in this same block.
    sibling_block = parents[owning_if]
    siblings = next(values for _, values in ast.iter_fields(sibling_block)
                    if isinstance(values, list) and owning_if in values)
    later = siblings[siblings.index(owning_if) + 1:]
    assert any(isinstance(item, ast.Call) and isinstance(item.func, ast.Name)
               and item.func.id == "_broadcast_epoch_eval_tensor"
               for sibling in later for item in ast.walk(sibling))


def _one_rank_fails(rank, init_file, results):
    import torch.distributed as dist

    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=10))
    try:
        parameter = torch.nn.Parameter(torch.tensor(1.0))
        parameter.grad = torch.tensor(7.0)
        optimizer = torch.optim.AdamW([parameter])
        evidence = {"row": 2, "symbol_index": 1, "date": "2026-09-17", "symbol": "TESTUSDT"}
        with pytest.raises(CryptoPerpetualDataError, match="rank 1 missing mark") as caught:
            with trainer._carry_loss_data_guard(
                _split(), optimizer, torch.device("cpu"), 4, distributed=True,
            ):
                if rank == 1:
                    raise CryptoPerpetualDataError("rank 1 missing mark", evidence=evidence)
        assert all(caught.value.evidence[key] == value for key, value in evidence.items())
        assert caught.value.evidence["split_row"] == 6
        assert parameter.grad is None and parameter.item() == 1.0 and not optimizer.state

        # Independent val/test workers must rendezvous even on the healthy rank.
        errors = []
        with trainer._capture_carry_data_error(errors):
            if rank == 0:
                raise CryptoPerpetualDataError("rank 0 validation missing mark", evidence=evidence)
        error = trainer._synchronize_carry_data_error(
            errors[0] if errors else None, torch.device("cpu"), distributed=True,
        )
        with pytest.raises(CryptoPerpetualDataError, match="rank 0 validation missing mark") as caught:
            trainer._raise_carry_data_error(error)
        assert caught.value.evidence == evidence
        sentinel = torch.tensor(rank + 1)
        dist.all_reduce(sentinel)
        assert sentinel.item() == 3
        results.put((rank, "ok"))
    except BaseException as exc:
        results.put((rank, f"{type(exc).__name__}: {exc}"))
    finally:
        dist.destroy_process_group()


def test_two_ranks_stop_before_backward_or_eval_broadcast(tmp_path, monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_one_rank_fails,
               args=(rank, str(tmp_path / "rendezvous"), results)) for rank in range(2)]
    try:
        for worker in workers:
            worker.start()
        messages = [results.get(timeout=30) for _ in workers]
        assert sorted(messages) == [(0, "ok"), (1, "ok")]
        for worker in workers:
            worker.join(timeout=5)
            assert worker.exitcode == 0
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=5)
        results.close()
