from datetime import datetime
from dataclasses import asdict
import os
from pathlib import Path

import polars as pl
import pytest
import yaml

from stockagent import config as config_module
from stockagent.live import market_status as status


def test_parquet_date_reuses_verified_scalar_and_separates_grain(tmp_path, monkeypatch):
    path = tmp_path / "2330_features.parquet"
    pl.DataFrame({"date": [datetime(2026, 10, 5, 13, 30)]}).write_parquet(path)
    calls = []
    reader = status._max_date_from_parquet_uncached

    def count(*args, **kwargs):
        calls.append(kwargs["date_only"])
        return reader(*args, **kwargs)

    monkeypatch.setattr(status, "_max_date_from_parquet_uncached", count)
    assert status._max_date_from_parquet(path) == "2026-10-05"
    assert status._max_date_from_parquet(path) == "2026-10-05"
    assert status._max_date_from_parquet(path, date_only=False) == "2026-10-05 13:30:00"
    assert status._max_date_from_parquet(path, date_only=False) == "2026-10-05 13:30:00"
    assert calls == [True, False]


def test_atomic_replacement_invalidates_same_size_and_mtime(tmp_path, monkeypatch):
    path = tmp_path / "dates.parquet"
    path.write_text("2026-10-05")
    monkeypatch.setattr(status, "_max_date_from_parquet_uncached", lambda p, **kw: p.read_text())
    assert status._max_date_from_parquet(path) == "2026-10-05"
    before = path.stat()
    replacement = tmp_path / "replacement.parquet"
    replacement.write_text("2026-10-06")
    os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
    os.replace(replacement, path)
    assert path.stat().st_size == before.st_size
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert status._max_date_from_parquet(path) == "2026-10-06"


def test_in_place_correction_and_removal_do_not_return_old_date(tmp_path, monkeypatch):
    path = tmp_path / "dates.parquet"
    path.write_text("2026-10-05")
    monkeypatch.setattr(status, "_max_date_from_parquet_uncached", lambda p, **kw: p.read_text())
    assert status._max_date_from_parquet(path) == "2026-10-05"
    before = path.stat()
    path.write_text("2026-10-06")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert status._max_date_from_parquet(path) == "2026-10-06"
    path.unlink()
    assert status._max_date_from_parquet(path) is None


def test_concurrently_replaced_source_is_retried_not_cached(tmp_path, monkeypatch):
    path = tmp_path / "dates.parquet"
    path.write_text("2026-10-05")
    calls = 0

    def reader(p, **kw):
        nonlocal calls
        calls += 1
        value = p.read_text()
        if calls == 1:
            p.write_text("2026-10-06")
        return value

    monkeypatch.setattr(status, "_max_date_from_parquet_uncached", reader)
    assert status._max_date_from_parquet(path) == "2026-10-06"
    assert status._max_date_from_parquet(path) == "2026-10-06"
    assert calls == 2


def test_transient_failure_is_not_a_negative_cache_entry(tmp_path, monkeypatch):
    path = tmp_path / "dates.parquet"
    path.write_text("repairable")
    calls = 0
    ready = False

    def reader(p, **kw):
        nonlocal calls
        calls += 1
        return "2026-10-06" if ready else None

    monkeypatch.setattr(status, "_max_date_from_parquet_uncached", reader)
    assert status._max_date_from_parquet(path) is None
    ready = True
    assert status._max_date_from_parquet(path) == "2026-10-06"
    assert calls == 3


def _inherited_config(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump({
        "experiment_name": "data-locator-test", "data": {
            "parquet_root": "stocks", "benchmark_name": "2330"},
        "environment": {"device": "cuda", "use_tensor_cores": True, "amp_dtype": "bf16"},
        "walk_forward": {"min_train_years": 1, "val_years": 1, "require_future_test_year": False},
        "trading": {"frequency": "daily", "buy_fee_rate": 0.000855,
                    "sell_fee_rate": 0.003855, "long_only": False, "gross_leverage": 2.5},
        "training": {"non_blocking_transfer": True, "model_name": "transformer_base_portfolio"},
        "evaluation": {},
    }))
    middle = tmp_path / "middle.yaml"
    middle.write_text("base_config: base.yaml\n")
    child = tmp_path / "child.yaml"
    child.write_text("base_config: middle.yaml\n")
    return base, middle, child


def test_canonical_loader_source_collection_preserves_entire_config(tmp_path):
    base, middle, child = _inherited_config(tmp_path)
    sources = {}
    observed = config_module.load_config(child, source_signatures=sources)
    assert asdict(observed) == asdict(config_module.load_config(child))
    assert set(sources) == {base, middle, child}
    assert all(status._file_identity(path) == signature for path, signature in sources.items())


@pytest.mark.parametrize("name", ["tw_day_trade_100m_fold11", "tw_day_trade_multi_basis_fold11",
                                "tw_day_trade_multi_basis_22_fold11", "tw_day_trade_v8_annual_log_cash_fold10"])
def test_source_collection_preserves_every_deployed_model_and_execution_field(name):
    path = Path(__file__).resolve().parents[1] / "configs" / "deployments" / f"{name}.yaml"
    sources = {}
    observed = config_module.load_config(path, source_signatures=sources)
    assert asdict(observed) == asdict(config_module.load_config(path))
    assert all(status._file_identity(path) == signature for path, signature in sources.items())


def test_data_locator_reuses_only_scalars_and_invalidates_parent_changes(tmp_path, monkeypatch):
    base, middle, child = _inherited_config(tmp_path)
    loader = config_module.load_config
    calls = []

    def count(*args, **kwargs):
        calls.append(args[0])
        return loader(*args, **kwargs)

    monkeypatch.setattr(config_module, "load_config", count)
    assert status._validated_data_locator(child) == ("stocks", "2330")
    assert status._validated_data_locator(child) == ("stocks", "2330")
    assert len(calls) == 1
    raw = yaml.safe_load(base.read_text())
    raw["data"] = {"parquet_root": "new_stocks", "benchmark_name": "0050"}
    base.write_text(yaml.safe_dump(raw))
    assert status._validated_data_locator(child) == ("new_stocks", "0050")
    assert len(calls) == 2
    base.unlink()
    with pytest.raises(OSError):
        status._validated_data_locator(child)
    assert str(child) not in status._DATA_LOCATOR_CACHE


def test_invalid_parent_edit_cannot_reuse_previous_valid_location(tmp_path):
    base, middle, child = _inherited_config(tmp_path)
    assert status._validated_data_locator(child) == ("stocks", "2330")
    raw = yaml.safe_load(base.read_text())
    raw["training"]["non_blocking_transfer"] = "false"
    base.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="non_blocking_transfer"):
        status._validated_data_locator(child)
    assert str(child) not in status._DATA_LOCATOR_CACHE


def test_source_collector_rejects_changed_config_during_load(tmp_path, monkeypatch):
    base, middle, child = _inherited_config(tmp_path)
    load_yaml = yaml.load
    calls = 0

    def mutate(handle, *args, **kwargs):
        nonlocal calls
        calls += 1
        result = load_yaml(handle, *args, **kwargs)
        if calls == 1:
            child.write_text("base_config: base.yaml\n")
        return result

    monkeypatch.setattr(config_module.yaml, "load", mutate)
    with pytest.raises(ValueError, match="changed during load"):
        config_module.load_config(child, source_signatures={})


def test_feature_inventory_is_immutable_and_invalidates_names_not_file_contents(tmp_path):
    root = tmp_path / "stocks"
    root.mkdir()
    first = root / "2330_features.parquet"
    first.write_text("old contents")
    paths = status._feature_files(root)
    assert paths == (first,)
    assert status._feature_files(root) is paths
    first.write_text("new feature contents, different file size")
    assert status._feature_files(root) is paths
    added = root / "0050_features.parquet"
    added.write_text("new symbol")
    assert set(status._feature_files(root)) == {first, added}
    first.unlink()
    assert status._feature_files(root) == (added,)
    renamed = root / "0051_features.parquet"
    added.rename(renamed)
    assert status._feature_files(root) == (renamed,)


def test_empty_inventory_is_invalidated_when_first_feature_appears(tmp_path):
    assert status._feature_files(tmp_path) == ()
    first = tmp_path / "2330_features.parquet"
    first.write_text("feature")
    assert status._feature_files(tmp_path) == (first,)
