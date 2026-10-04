from pathlib import Path

import numpy as np
import pytest

import train
from stockagent.config import load_config
from stockagent.data.tw_day_trade_execution import discover_day_trade_minute_execution_source


def config_with_source(root):
    config = load_config("configs/markets/tw.yaml")
    config.trading.execution_mode = "tw_day_trade"
    config.data.day_trade_minute_execution_root = str(root)
    return config


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
