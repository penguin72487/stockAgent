from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_tw_day_trade_batch_sizes.py"
SPEC = importlib.util.spec_from_file_location("benchmark_tw_day_trade_batch_sizes", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_default_python_preserves_private_venv_package_visibility(tmp_path):
    private = tmp_path / "kernel-env"
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", "--system-site-packages", str(private)], check=True)
    python = private / "bin/python"
    site = Path(subprocess.check_output([str(python), "-c",
        "import sysconfig;print(sysconfig.get_paths()['purelib'])"], text=True).strip())
    (site / "frontier_private_marker.py").write_text("VALUE = 'private-only'\n")
    script = """import argparse, subprocess
from scripts.benchmark_tw_day_trade_batch_sizes import main
def capture(parser, *args, **kwargs):
    action = next(a for a in parser._actions if a.dest == 'python')
    output = subprocess.check_output([str(action.default), '-c',
        'import frontier_private_marker;print(frontier_private_marker.VALUE)'], text=True)
    assert output.strip() == 'private-only'
    raise SystemExit(0)
argparse.ArgumentParser.parse_args = capture
main()
"""
    env = dict(os.environ, PYTHONPATH=str(SCRIPT.parents[1]))
    subprocess.run([str(python), "-c", script], env=env, check=True)


def _epoch(epoch: int, *, wall: float, train: float, grad: float = 2.0) -> dict:
    return {
        "epoch": epoch,
        "epoch_wall_s": wall,
        "train_total_s": train,
        "train_loss": -0.1,
        "val_mean": -0.2,
        "test_mean": -0.3,
        "train_batches": 3,
        "train_zero_grad_batches": 0,
        "train_grad_norm_before_clip_mean": grad,
        "dynamo_unique_graphs_epoch_delta": 0,
        "bt_compile_failures": 0,
        "bt_prep_compile_failures": 0,
        "bt_runtime_fallback_calls": 0,
        "bt_eager_runner_calls": 0,
        "bt_prep_compile_nonhit": 0,
        "bt_compile_nonhit": 0,
    }


def _memory(*, peak: float = 0.5, headroom: float = 16.0) -> dict:
    return {
        "ok": True,
        "per_gpu": {},
        "max_peak_fraction": peak,
        "min_headroom_gib": headroom,
    }


def test_frozen_project_root_owns_config_resolution(tmp_path, monkeypatch) -> None:
    @dataclass
    class Config:
        source: Path

    config_path = tmp_path / "experiment.yaml"
    seen = []
    module = SimpleNamespace(
        __file__=str(tmp_path / "stockagent/config.py"),
        load_config=lambda path: seen.append(path) or Config(source=path),
    )
    monkeypatch.setattr(benchmark.sys, "path", benchmark.sys.path.copy())
    monkeypatch.setattr(benchmark.importlib, "import_module", lambda name: module)
    assert benchmark._resolved_source_config(config_path, tmp_path) == {"source": str(config_path)}
    assert benchmark.sys.path[0] == str(tmp_path)
    assert seen == [config_path]


def test_frozen_project_root_rejects_an_already_loaded_foreign_library(tmp_path, monkeypatch) -> None:
    module = SimpleNamespace(
        __file__=str(tmp_path / "other/stockagent/config.py"),
        load_config=lambda _: pytest.fail("must reject before reading the config"),
    )
    monkeypatch.setattr(benchmark.sys, "path", benchmark.sys.path.copy())
    monkeypatch.setattr(benchmark.importlib, "import_module", lambda name: module)
    with pytest.raises(ValueError, match="config library does not match --project-root"):
        benchmark._resolved_source_config(tmp_path / "experiment.yaml", tmp_path)


def test_ddp_score_accepts_canonical_trajectory_max_rank_names() -> None:
    row = _epoch(3, wall=1.0, train=0.5)
    row.update(epoch_max_rank_s=4.0, train_max_rank_s=3.0)
    score = benchmark._score_curve(
        [row], skip_epochs=2, minimum_steady_epochs=1, train_rows=90,
        global_batch_size=32, memory=_memory(), max_peak_fraction=0.98,
        min_headroom_gib=0.5, world_size=2,
    )
    assert score["ok"]
    assert score["timing_scope"] == "maximum_rank"
    assert score["median_epoch_wall_s"] == 4.0
    assert score["epoch_wall_field"] == "epoch_max_rank_s"


def test_ddp_score_rejects_rank_local_time_and_missing_graph_telemetry() -> None:
    row = _epoch(3, wall=4.0, train=3.0)
    row.pop("dynamo_unique_graphs_epoch_delta")
    score = benchmark._score_curve(
        [row], skip_epochs=2, minimum_steady_epochs=1, train_rows=90,
        global_batch_size=32, memory=_memory(), max_peak_fraction=0.98,
        min_headroom_gib=0.5, world_size=2,
    )
    assert not score["ok"]
    assert any("maximum-rank" in reason for reason in score["reasons"])
    assert any("graph telemetry" in reason for reason in score["reasons"])


def test_parse_batch_sizes_requires_power_of_two_globally_and_per_rank() -> None:
    assert benchmark._parse_batch_sizes("512,64,128,128", world_size=2) == [64, 128, 512]
    with pytest.raises(ValueError, match="power of two"):
        benchmark._parse_batch_sizes("96", world_size=2)
    with pytest.raises(ValueError, match="divisible"):
        benchmark._parse_batch_sizes("64", world_size=3)


def test_parse_batch_sizes_can_search_between_power_of_two_frontiers() -> None:
    assert benchmark._parse_batch_sizes(
        "192,160,160",
        world_size=2,
        require_power_of_two=False,
    ) == [160, 192]
    with pytest.raises(ValueError, match="positive"):
        benchmark._parse_batch_sizes(
            "0",
            world_size=2,
            require_power_of_two=False,
        )


def test_source_contract_fails_closed_on_another_executor() -> None:
    config = {
        "data": {},
        "trading": {"execution_mode": "tw_day_trade", "frequency": "daily"},
        "training": {"model_name": "financial_transformer", "loss_type": "log_utility"},
    }
    assert benchmark._validate_source_contract(config)["trading.execution_mode"] == "tw_day_trade"
    config["trading"]["execution_mode"] = "tw_minute"
    with pytest.raises(ValueError, match="not the requested daily day-trade contract"):
        benchmark._validate_source_contract(config)


def test_source_contract_accepts_only_strict_minute_executable_pairing() -> None:
    config = {
        "data": {
            "day_trade_minute_execution_root": "minute",
            "day_trade_minute_execution_allow_daily_proxy": False,
        },
        "trading": {
            "execution_mode": "tw_day_trade",
            "frequency": "daily",
            "tw_day_trade_unlimited_margin_conversion": False,
            "tw_short_capacity_limit_enabled": False,
        },
        "training": {
            "model_name": "executable_portfolio_transformer",
            "loss_type": "log_utility",
        },
    }

    contract = benchmark._validate_source_contract(config)
    assert contract["objective"] == "strict_minute"

    config["data"]["day_trade_minute_execution_allow_daily_proxy"] = True
    with pytest.raises(ValueError, match="allow_daily_proxy=false"):
        benchmark._validate_source_contract(config)


def test_source_contract_accepts_explicit_naive_projection_l1() -> None:
    config = {
        "data": {},
        "trading": {"execution_mode": "naive", "frequency": "daily"},
        "training": {
            "model_name": "financial_transformer",
            "loss_type": "log_utility",
            "financial_transformer": {"portfolio_output_mode": "projection_l1"},
        },
    }
    contract = benchmark._validate_source_contract(
        config, expected_execution_mode="naive"
    )
    assert contract["training.portfolio_output_mode"] == "projection_l1"


def test_futures_benchmark_keeps_exact_account_contract() -> None:
    config = {
        "data": {},
        "trading": {"execution_mode": "tw_stock_context_futures_portfolio", "frequency": "daily",
                    "tw_futures_portfolio_integer_contracts": True,
                    "tw_futures_portfolio_capital_basis": "initial_margin"},
        "training": {"model_name": "financial_transformer", "loss_type": "log_utility",
                     "futures_portfolio_training_surrogate_only": False,
                     "futures_portfolio_optimizer_step_per_trajectory": True},
    }
    contract = benchmark._validate_source_contract(
        config, expected_execution_mode="tw_stock_context_futures_portfolio")
    assert contract["capital_basis"] == "initial_margin"
    assert contract["optimizer_step_per_trajectory"] is True
    config["training"]["futures_portfolio_training_surrogate_only"] = True
    with pytest.raises(ValueError, match="exact training-forward"):
        benchmark._validate_source_contract(config, expected_execution_mode="tw_stock_context_futures_portfolio")


def test_score_curve_uses_real_rows_and_complete_epoch_median() -> None:
    rows = [
        _epoch(1, wall=99.0, train=90.0),
        _epoch(2, wall=10.0, train=8.0),
        _epoch(3, wall=4.0, train=3.0),
        _epoch(4, wall=5.0, train=4.0),
        _epoch(5, wall=6.0, train=5.0),
    ]
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is True
    assert score["train_batches"] == 3
    assert score["median_epoch_wall_s"] == pytest.approx(5.0)
    assert score["complete_epoch_real_rows_per_s"] == pytest.approx(289 / 5)
    assert score["padding_fraction"] == pytest.approx((384 - 289) / 384)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda rows: rows[0].update(train_zero_grad_batches=1), "zero-gradient"),
        (lambda rows: rows[0].update(bt_runtime_fallback_calls=1), "fallback"),
        (lambda rows: rows[0].update(dynamo_unique_graphs_epoch_delta=1), "Dynamo"),
        (lambda rows: rows[0].update(train_loss=float("nan")), "invalid train_loss"),
    ],
)
def test_score_curve_rejects_invalid_training(mutate, reason: str) -> None:
    rows = [_epoch(3, wall=4.0, train=3.0), _epoch(4, wall=4.0, train=3.0), _epoch(5, wall=4.0, train=3.0)]
    mutate(rows)
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is False
    assert any(reason in item for item in score["reasons"])


def test_naive_score_allows_canonical_eager_eval_but_rejects_runtime_fallback() -> None:
    rows = [
        _epoch(3, wall=4.0, train=3.0),
        _epoch(4, wall=4.0, train=3.0),
        _epoch(5, wall=4.0, train=3.0),
    ]
    for row in rows:
        row["bt_eager_runner_calls"] = 1
        row["bt_prep_compile_nonhit"] = 1
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
        strict_compiled_backtest=False,
    )
    assert score["ok"] is True
    rows[0]["bt_runtime_fallback_calls"] = 1
    rejected = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
        strict_compiled_backtest=False,
    )
    assert rejected["ok"] is False


def test_score_uses_slowest_rank_and_rejects_incomplete_rank_timing() -> None:
    rows = [_epoch(i, wall=4.0, train=3.0) for i in (3, 4, 5)]
    for row in rows:
        row.update(epoch_wall_s_max_rank=5.0, train_total_s_max_rank=3.5)
    kwargs = dict(
        skip_epochs=2, minimum_steady_epochs=3, train_rows=289,
        global_batch_size=128, memory=_memory(), max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    score = benchmark._score_curve(rows, **kwargs)
    assert score["ok"]
    assert score["timing_scope"] == "maximum_rank"
    assert score["complete_epoch_real_rows_per_s"] == pytest.approx(289 / 5)
    assert score["median_train_wall_s"] == 3.5
    del rows[1]["train_total_s_max_rank"]
    assert not benchmark._score_curve(rows, **kwargs)["ok"]


def test_annual_score_requires_verified_account_coverage_instead_of_legacy_batch_count():
    from stockagent.training.episode_parallel import AnnualEpisodePlan
    plan = AnnualEpisodePlan((((0, 3),), ((3, 5),)), ((2024,), (2025,)),
                             (3, 2), 5, 3, (((2024, 0, 3),), ((2025, 3, 5),)))
    rows = [_epoch(i, wall=4., train=3.) for i in (3, 4, 5)]
    for row in rows:
        row.update(train_batches=1, epoch_wall_s_max_rank=4., train_total_s_max_rank=3.,
                   dynamo_unique_graphs_epoch_delta=0)
    kwargs = dict(skip_epochs=2, minimum_steady_epochs=3, train_rows=5,
                  global_batch_size=6, world_size=2, memory=_memory(),
                  max_peak_fraction=.9, min_headroom_gib=3.)
    score = benchmark._score_curve(rows, annual_ownership=plan.receipt(), **kwargs)
    assert score["ok"] and score["annual_ownership_validation"]["ok"]
    assert score["padded_slots"] == 6
    bad = plan.receipt()
    bad["rows_by_rank"] = (2, 3)
    assert not benchmark._score_curve(rows, annual_ownership=bad, **kwargs)["ok"]


def test_score_curve_rejects_unsafe_vram_and_winner_uses_complete_epoch_rate() -> None:
    rows = [_epoch(3, wall=4.0, train=3.0), _epoch(4, wall=4.0, train=3.0), _epoch(5, wall=4.0, train=3.0)]
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(peak=0.95, headroom=1.0),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is False
    winner = benchmark._select_winner(
        [
            {"ok": True, "batch_size": 128, "complete_epoch_real_rows_per_s": 100.0},
            {"ok": True, "batch_size": 256, "complete_epoch_real_rows_per_s": 120.0},
            {"ok": False, "batch_size": 512, "complete_epoch_real_rows_per_s": 999.0},
        ]
    )
    assert winner["batch_size"] == 256


def test_host_memory_sampler_retains_shared_scope_and_minimum_headroom(tmp_path):
    path = tmp_path/"memory.csv"
    path.write_text("index,host_memory_used_bytes,host_memory_limit_bytes\n"
                    "0,1073741824,4294967296\n"
                    "1,2147483648,4294967296\n"
                    "0,,max\n")
    result = benchmark._host_memory_summary(path)
    assert result == {"observed": True, "scope": "shared_cgroup_including_file_cache",
                      "peak_used_gib": 2., "min_headroom_gib": 2.}
    assert benchmark._host_memory_summary(tmp_path/"absent.csv")["observed"] is False


def test_dynamo_cache_fallback_warnings_are_rejections_even_with_exit_zero():
    assert "hit config.recompile_limit" in benchmark._FAILURE_PATTERNS
    assert "hit config.cache_size_limit" in benchmark._FAILURE_PATTERNS


def _resource_rejection_log():
    prefix = "[rank1]:E1012 21:35:24.147000 885948 site-packages/torch/_inductor/select_algorithm.py:3924] [3/0] "
    return (prefix + "Runtime error during autotuning: \n" + prefix
        + "No valid triton configs. OutOfMemoryError: out of resource: triton_mm Required: 163840 "
        + "Hardware limit:101376 Reducing block sizes or `num_stages` may help.. \n"
        + prefix + "Ignoring this choice.\n")


def test_discarded_autotune_shared_memory_choices_are_retained_without_fatal_oom():
    failures, candidates = benchmark._failure_log_evidence(_resource_rejection_log())
    assert failures == []
    assert candidates == [{"kernel": "triton_mm", "required_bytes": 163840,
        "hardware_limit_bytes": 101376}]


@pytest.mark.parametrize("fatal", ["CUDA out of memory", "torch.OutOfMemoryError: allocation failed",
    "CUDA error: an illegal memory access was encountered", "hit config.recompile_limit"])
def test_real_failure_is_preserved_beside_a_discarded_autotune_choice(fatal):
    failures, candidates = benchmark._failure_log_evidence(_resource_rejection_log() + fatal)
    assert candidates and failures and any(pattern in fatal for pattern in failures)


def test_unpaired_or_other_rank_autotune_resource_failure_is_rejected():
    log = _resource_rejection_log()
    failures, candidates = benchmark._failure_log_evidence(log.replace("Ignoring this choice.", "Not ignored."))
    assert failures == ["OutOfMemoryError"] and not candidates
    failures, candidates = benchmark._failure_log_evidence(log.replace(
        "[rank1]:E1012 21:35:24.147000 885948 site-packages/torch/_inductor/select_algorithm.py:3924] [3/0] Ignoring",
        "[rank0]:E1012 21:35:24.147000 885947 site-packages/torch/_inductor/select_algorithm.py:3924] [3/0] Ignoring"))
    assert failures == ["OutOfMemoryError"] and not candidates


def test_new_candidate_uses_role_separated_cache_without_changing_original_config(tmp_path, monkeypatch):
    from copy import deepcopy
    import yaml
    monkeypatch.setattr(benchmark, 'ROOT', tmp_path)
    source = {'data': {'day_trade_minute_execution_cache_dir': str(tmp_path / 'artifacts/markets/formal/runtime-cache/physical-source'),
                       'parquet_root': '/exact/fixed-source/stocks'},
              'training': {'optimizer': 'adamw'}, 'trading': {'execution_mode': 'tw_day_trade'}}
    before = deepcopy(source)
    target = tmp_path / 'candidate.yaml'
    benchmark._write_candidate_config(source, path=target, output_dir=tmp_path / 'artifacts/benchmarks/new/training',
        batch_size=128, batch_size_eval=16, epochs=3, start_fold=11)
    actual = yaml.safe_load(target.read_text())
    assert actual['data']['day_trade_minute_execution_cache_dir'] == str(tmp_path / 'artifacts/cache/formal/runtime-cache/physical-source')
    assert actual['data']['parquet_root'] == source['data']['parquet_root'] and actual['trading'] == source['trading']
    assert actual['training']['optimizer'] == 'adamw' and source == before
    assert not (tmp_path / 'artifacts/markets').exists()
