from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import os
from pathlib import Path

import pytest
import yaml

from stockagent import config as config_module


@pytest.fixture(autouse=True)
def isolated_cache():
    config_module.clear_config_snapshot_cache()
    yield
    config_module.clear_config_snapshot_cache()


def _inherited(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(yaml.safe_dump({
        "experiment_name": "snapshot-test",
        "data": {"parquet_root": "stocks", "benchmark_name": "2330"},
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


def test_reuses_full_validation_but_never_mutable_caller_state(tmp_path, monkeypatch):
    _, _, child = _inherited(tmp_path)
    loader = config_module.load_config
    calls = []

    def count(*args, **kwargs):
        calls.append(args[0])
        return loader(*args, **kwargs)

    monkeypatch.setattr(config_module, "load_config", count)
    expected = asdict(loader(child))
    first = config_module.load_config_cached(child)
    first.environment.device = "cpu"
    first.data.feature_include.append("mutated")
    second = config_module.load_config_cached(child)
    assert asdict(second) == expected
    assert first is not second
    assert len(calls) == 1


def test_inherited_change_and_invalid_change_cannot_reuse_old_snapshot(tmp_path):
    base, _, child = _inherited(tmp_path)
    assert config_module.load_config_cached(child).data.benchmark_name == "2330"
    raw = yaml.safe_load(base.read_text())
    raw["data"]["benchmark_name"] = "0050"
    base.write_text(yaml.safe_dump(raw))
    assert config_module.load_config_cached(child).data.benchmark_name == "0050"
    raw["training"]["non_blocking_transfer"] = "false"
    base.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="non_blocking_transfer"):
        config_module.load_config_cached(child)
    assert not config_module._CONFIG_SNAPSHOT_CACHE


def test_same_size_mtime_replacement_invalidates_parent(tmp_path):
    base, _, child = _inherited(tmp_path)
    assert config_module.load_config_cached(child).data.benchmark_name == "2330"
    before = base.stat()
    replacement = tmp_path / "replacement.yaml"
    replacement.write_text(base.read_text().replace("2330", "0050"))
    os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
    replacement.replace(base)
    assert base.stat().st_size == before.st_size
    assert base.stat().st_mtime_ns == before.st_mtime_ns
    assert config_module.load_config_cached(child).data.benchmark_name == "0050"


def test_removed_parent_fails_and_restored_parent_recovers(tmp_path):
    base, _, child = _inherited(tmp_path)
    saved = base.read_bytes()
    config_module.load_config_cached(child)
    base.unlink()
    with pytest.raises(OSError):
        config_module.load_config_cached(child)
    assert not config_module._CONFIG_SNAPSHOT_CACHE
    base.write_bytes(saved)
    assert config_module.load_config_cached(child).data.benchmark_name == "2330"


@pytest.mark.parametrize("relative", [False, True])
def test_symlink_request_preserves_canonical_inheritance_and_retargeting(tmp_path, monkeypatch, relative):
    root = tmp_path / "target"
    root.mkdir()
    _, _, child = _inherited(root)
    other_root = tmp_path / "other"
    other_root.mkdir()
    other_base, _, other_child = _inherited(other_root)
    other_base.write_text(other_base.read_text().replace("2330", "0050"))
    # Absolute symlink inheritance uses the alias's parent by canonical design;
    # relative symlinks resolve first and use the target's parent.
    alias = tmp_path / "alias.yaml"
    _inherited(tmp_path)
    alias.symlink_to(child)
    monkeypatch.chdir(tmp_path)
    requested = Path("alias.yaml") if relative else alias
    assert asdict(config_module.load_config_cached(requested)) == asdict(config_module.load_config(requested))
    alias.unlink()
    alias.symlink_to(other_child)
    assert asdict(config_module.load_config_cached(requested)) == asdict(config_module.load_config(requested))
    if relative:
        assert config_module.load_config_cached(requested).data.benchmark_name == "0050"


def test_parent_symlink_retarget_is_checked_on_each_hit(tmp_path):
    base, middle, child = _inherited(tmp_path)
    alternate = tmp_path / "alternate.yaml"
    alternate.write_text(base.read_text().replace("2330", "0050"))
    middle.unlink()
    middle.symlink_to(base)
    assert config_module.load_config_cached(child).data.benchmark_name == "2330"
    middle.unlink()
    middle.symlink_to(alternate)
    assert config_module.load_config_cached(child).data.benchmark_name == "0050"


def test_source_changed_after_validation_is_not_cached(tmp_path, monkeypatch):
    base, _, child = _inherited(tmp_path)
    loader = config_module.load_config

    def racing(*args, **kwargs):
        config = loader(*args, **kwargs)
        base.write_text(base.read_text().replace("2330", "0050"))
        return config

    monkeypatch.setattr(config_module, "load_config", racing)
    with pytest.raises(ValueError, match="changed during load"):
        config_module.load_config_cached(child)
    assert not config_module._CONFIG_SNAPSHOT_CACHE


def test_source_changed_during_cached_copy_fails_closed(tmp_path, monkeypatch):
    base, _, child = _inherited(tmp_path)
    config_module.load_config_cached(child)
    copier = config_module.deepcopy

    def racing(value):
        result = copier(value)
        base.write_text(base.read_text().replace("2330", "0050"))
        return result

    monkeypatch.setattr(config_module, "deepcopy", racing)
    with pytest.raises(ValueError, match="changed during cached load"):
        config_module.load_config_cached(child)
    assert not config_module._CONFIG_SNAPSHOT_CACHE


def test_parallel_hits_have_independent_nested_objects(tmp_path):
    _, _, child = _inherited(tmp_path)
    expected = asdict(config_module.load_config_cached(child))
    with ThreadPoolExecutor(max_workers=6) as pool:
        configs = list(pool.map(lambda _: config_module.load_config_cached(child), range(12)))
    assert len({id(value) for value in configs}) == 12
    assert all(asdict(value) == expected for value in configs)
    configs[0].data.feature_include.append("one-caller-only")
    assert all(not value.data.feature_include for value in configs[1:])


def test_snapshot_count_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(config_module, "_CONFIG_SNAPSHOT_MAX_ENTRIES", 2)
    _, _, child = _inherited(tmp_path)
    for number in range(4):
        alias = tmp_path / f"config{number}.yaml"
        alias.write_text(f"base_config: {child.name}\n")
        config_module.load_config_cached(alias)
    assert len(config_module._CONFIG_SNAPSHOT_CACHE) == 2


@pytest.mark.parametrize("name", [
    "tw_day_trade_100m_fold11", "tw_day_trade_multi_basis_fold11",
    "tw_day_trade_multi_basis_22_fold11", "tw_day_trade_v8_annual_log_cash_fold10",
    "tw_day_trade_factorized_scale_separated_cash_fold11_live_v1", "tw_day_trade_unit_mgda_fold10_live_v1",
])
def test_deployment_snapshot_preserves_every_field(name):
    root = Path(__file__).resolve().parents[1]
    path = root / "configs" / "deployments" / f"{name}.yaml"
    assert asdict(config_module.load_config_cached(path)) == asdict(config_module.load_config(path))
    assert asdict(config_module.load_config_cached(path)) == asdict(config_module.load_config(path))
