import ast
from pathlib import Path
import fcntl
import json
from types import SimpleNamespace

import numpy as np
import pytest

import train
from stockagent.config import load_config
from stockagent.data import tw_day_trade_carry_source
from stockagent.data.tw_day_trade_execution import discover_day_trade_minute_execution_source


def config_with_source(root):
    config = load_config("configs/markets/tw.yaml")
    config.trading.execution_mode = "tw_day_trade"
    config.data.day_trade_minute_execution_root = str(root)
    return config


def test_physical_source_preflight_matches_the_actual_builder_call():
    """Adding a builder argument must update the early admission contract."""
    # Read one source generation rather than asking inspect/linecache for two
    # imported function offsets; another authorized task may edit train.py
    # while this broad regression process is already running.
    module = ast.parse(Path(train.__file__).read_text(encoding="utf-8"))
    preflight = next(node for node in module.body if isinstance(node, ast.FunctionDef)
                     and node.name == "_preflight_day_trade_carry_abi")
    assignment = next(node for node in ast.walk(preflight) if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == "required"
                              for target in node.targets))
    required = ast.literal_eval(assignment.value)
    main = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    calls = [node for node in ast.walk(main) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name)
             and node.func.id == "build_prepared_day_trade_carry_source"]
    assert len(calls) == 1
    assert required == {keyword.arg for keyword in calls[0].keywords}
    config_assignment = next(node for node in ast.walk(preflight) if isinstance(node, ast.Assign)
                             and any(isinstance(target, ast.Name) and target.id == "required_config"
                                     for target in node.targets))
    config_fields = ast.literal_eval(config_assignment.value)
    actual_fields = {(node.value.attr, node.attr) for node in ast.walk(calls[0])
                     if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
                     and isinstance(node.value.value, ast.Name) and node.value.value.id == "config"}
    assert {(section, field) for section, fields in config_fields.items() for field in fields} == actual_fields


def test_physical_source_preflight_rejects_mixed_release_without_defaulting_cost(tmp_path, monkeypatch):
    config = config_with_source(tmp_path / "minutes")
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    config.trading.tw_day_trade_short_limit_up_penalty_rate = 0.07

    # The older frozen source accepts no research-cost keyword; a generic
    # **kwargs sink is not evidence that it implements that financial rule.
    def older_builder(**kwargs):
        raise AssertionError("the source builder must not execute during preflight")

    monkeypatch.setattr(tw_day_trade_carry_source, "build_prepared_day_trade_carry_source", older_builder)
    with pytest.raises(RuntimeError, match="physical source ABI mismatch") as error:
        train._preflight_day_trade_carry_abi(config)
    assert "short_limit_up_penalty_rate" in str(error.value)
    assert config.trading.tw_day_trade_short_limit_up_penalty_rate == 0.07


def test_physical_source_preflight_requires_the_loaded_config_cost_field(tmp_path):
    config = config_with_source(tmp_path / "minutes")
    config.trading = SimpleNamespace(
        execution_mode="tw_day_trade", tw_day_trade_unlimited_margin_conversion=True,
    )
    with pytest.raises(RuntimeError, match="trading.tw_day_trade_short_limit_up_penalty_rate"):
        train._preflight_day_trade_carry_abi(config)


@pytest.mark.parametrize("execution_mode, minute_root, physical", [
    ("naive", "minutes", True),
    ("tw_day_trade", None, True),
    ("tw_day_trade", "minutes", False),
])
def test_other_execution_paths_do_not_require_the_physical_source_abi(
    tmp_path, monkeypatch, execution_mode, minute_root, physical,
):
    config = config_with_source(tmp_path / "minutes")
    config.trading.execution_mode = execution_mode
    config.data.day_trade_minute_execution_root = minute_root
    config.trading.tw_day_trade_unlimited_margin_conversion = physical
    monkeypatch.setattr(tw_day_trade_carry_source, "build_prepared_day_trade_carry_source", None)
    train._preflight_day_trade_carry_abi(config)


def test_physical_source_preflight_accepts_the_current_canonical_abi(tmp_path):
    config = config_with_source(tmp_path / "minutes")
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    train._preflight_day_trade_carry_abi(config)


@pytest.mark.parametrize("damage", ["missing", "file", "empty", "invalid_date"])
@pytest.mark.parametrize("strategy", ["none", "distributed_data_parallel"])
def test_unavailable_execution_source_fails_before_panel_allocation(tmp_path, damage, strategy):
    root = tmp_path / "minutes"
    if damage == "file":
        root.write_bytes(b"file")
    elif damage != "missing":
        root.mkdir()
        if damage == "invalid_date":
            partition = root / "trade_date=invalid" / "data.parquet"
            partition.parent.mkdir()
            partition.write_bytes(b"unread")
    called = []

    def builder(*args, **kwargs):
        called.append((args, kwargs))
        raise AssertionError("unavailable source must fail before panel construction")

    with pytest.raises(ValueError if damage == "invalid_date" else FileNotFoundError):
        train._build_panel_rank_coordinated(builder, config_with_source(root), strategy)
    assert called == []


def test_metadata_preflight_does_not_read_or_claim_partition_contents(tmp_path, monkeypatch):
    root = tmp_path / "minutes"
    for day in ["2026-09-29", "2026-09-28", "invalid"]:
        path = root / f"trade_date={day}" / "data.parquet"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"not a parquet payload")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("availability discovery must not decode partition contents")

    monkeypatch.setattr(Path, "read_bytes", forbidden)
    paths, first = discover_day_trade_minute_execution_source(root)
    assert len(paths) == 3
    assert first == np.datetime64("2026-09-28", "D")
    sentinel = object()
    assert train._build_panel_rank_coordinated(
        lambda *_args, **_kwargs: sentinel, config_with_source(root), "none",
    ) is sentinel


def test_other_execution_modes_do_not_require_day_trade_minute_source(tmp_path):
    config = config_with_source(tmp_path / "missing")
    config.trading.execution_mode = "naive"
    sentinel = object()
    assert train._build_panel_rank_coordinated(
        lambda *_args, **_kwargs: sentinel, config, "none",
    ) is sentinel


@pytest.mark.parametrize("mode", ["train", "resume", "eval"])
def test_inference_physical_source_binding_cannot_change_training(tmp_path, mode):
    config = config_with_source(tmp_path / "minutes")
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    config.data.day_trade_physical_public_feature_path = "original/features/daily.parquet"
    config.data.day_trade_inference_public_feature_path = "current/features/daily.parquet"
    with pytest.raises(ValueError, match="training/resume is forbidden"):
        train._day_trade_physical_source_path(config, mode=mode)
    assert train._day_trade_physical_source_path(config, mode="infer") == "current/features/daily.parquet"
    assert config.data.day_trade_physical_public_feature_path == "original/features/daily.parquet"


@pytest.mark.parametrize("frozen", [None, "original/features/daily.parquet"])
def test_default_physical_source_binding_is_unchanged(tmp_path, frozen):
    config = config_with_source(tmp_path / "minutes")
    config.data.day_trade_physical_public_feature_path = frozen
    for mode in ("train", "infer"):
        assert train._day_trade_physical_source_path(config, mode=mode) == (
            frozen or config.data.tw_public_feature_path
        )


@pytest.mark.parametrize("execution_mode,physical", [("naive", True), ("tw_day_trade", False)])
def test_inference_binding_requires_physical_fifo(tmp_path, execution_mode, physical):
    config = config_with_source(tmp_path / "minutes")
    config.trading.execution_mode = execution_mode
    config.trading.tw_day_trade_unlimited_margin_conversion = physical
    config.data.day_trade_inference_public_feature_path = "current/features/daily.parquet"
    with pytest.raises(ValueError, match="physical-FIFO inference only"):
        train._day_trade_physical_source_path(config, mode="infer")


@pytest.mark.parametrize("abort", [False, True])
def test_inference_panel_and_physical_reads_hold_and_release_same_producer_lease(tmp_path, abort):
    root = tmp_path / "current"
    (root / "features").mkdir(parents=True)
    (root / "tw_corporate_action_reference.parquet").touch()
    lock = tmp_path / ".locks" / "tw-public-refresh.lock"
    lock.parent.mkdir()
    config = config_with_source(tmp_path / "minutes")
    config.data.parquet_root = str(root / "stocks")
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    config.data.day_trade_inference_public_feature_path = str(root / "features/daily.parquet")

    def read_inputs():
        with train._panel_input_read_guard(config, mode="infer") as path:
            assert path == config.data.day_trade_inference_public_feature_path
            with lock.open("a+") as writer:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(writer.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            if abort:
                raise ValueError("strict source validation failed")

    if abort:
        with pytest.raises(ValueError, match="strict source validation"):
            read_inputs()
    else:
        read_inputs()
    with lock.open("a+") as writer:
        fcntl.flock(writer.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
        with pytest.raises(TWPublicSourceUpdatePending):
            with train._panel_input_read_guard(config, mode="infer"):
                raise AssertionError("must defer while the canonical source producer is active")


def test_inference_physical_binding_config_is_normalized_and_scoped(tmp_path):
    path = tmp_path / "config.json"
    candidate = Path("configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_candidate_v1.yaml").resolve()
    path.write_text(json.dumps({"base_config": str(candidate), "data": {
        "day_trade_inference_public_feature_path": " current/features/daily.parquet "}}))
    assert load_config(path).data.day_trade_inference_public_feature_path == "current/features/daily.parquet"
    baseline = Path("configs/experiment_baseline.yaml").resolve()
    path.write_text(json.dumps({"base_config": str(baseline), "data": {
        "day_trade_inference_public_feature_path": "current"}}))
    with pytest.raises(ValueError, match="physical-FIFO"):
        load_config(path)
