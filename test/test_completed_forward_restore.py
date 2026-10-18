from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path

import pytest
import torch

from scripts import publish_data_releases as releases
from stockagent.data_sync import completed_forward_restore as restore
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.live import completed_inference_cache as cache
from test_completed_forward_publication import _argv, _fixture


def _published(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    assert releases.main(_argv(fixture)) == 0
    publication = json.loads(capsys.readouterr().out)["published"][0]
    source, identity, key, catalog, cold, _, retention = fixture
    binding = restore.ForwardColdBinding(
        json.loads(catalog.read_text())["publication_families"][0], cold, retention, tmp_path)
    preserved = {}
    for suffix in ("forward", "receipt.json"):
        path = source / f"{key}.{suffix}"
        preserved[suffix] = path.read_bytes()
        path.rename(tmp_path / f"original.{suffix}")
    return fixture, binding, Path(publication["fixed_pin"]["path"]), preserved


@contextmanager
def _owner(source, key):
    descriptor = os.open(source / f"{key}.lock", os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield descriptor
    finally:
        os.close(descriptor)


def _restore(fixture, binding, **kwargs):
    source, identity, key, *_ = fixture
    with _owner(source, key) as descriptor, reserve_capacity(
            source, key=key, maximum_growth_bytes=2 * 1024**2, capacity_bytes=128 * 1024**2):
        return restore.restore_completed_forward(
            source, key, identity, binding=binding, key_descriptor=descriptor,
            read=cache._read, validate_before_commit=kwargs.get("validate"),
            maximum_payload_bytes=kwargs.get("maximum_payload_bytes", cache.MAX_PAYLOAD_BYTES))


def test_fixed_cold_result_restores_original_flat_files_without_model_or_reencoding(tmp_path, monkeypatch, capsys):
    fixture, binding, pin, preserved = _published(tmp_path, monkeypatch, capsys)
    source, identity, key, *_ = fixture
    original_pin = pin.read_bytes(), pin.stat()
    original_read = cache._read
    seen = []

    def read(*args, **kwargs):
        from stockagent.data_sync.completed_forward_capacity import capacity_status
        assert capacity_status(source)["active_reservations"][0]["key"] == key
        seen.append(args[0])
        return original_read(*args, **kwargs)

    monkeypatch.setattr(cache, "_read", read)
    monkeypatch.setattr(torch, "save", lambda *_a, **_kw: pytest.fail("must not reencode"))
    value, receipt = _restore(fixture, binding)
    assert len(seen) == 1 and seen[0].name.startswith(".packed-stage-")
    assert receipt["gpu_forward_calls"] == 0 and not receipt["payload_reencoded"]
    assert receipt["consumer_commit"] and not receipt["managed_materialization_created"]
    assert value["scores"].view(torch.uint8).numpy().tobytes() == torch.tensor(
        [float("nan"), -0.0, 1.25]).view(torch.uint8).numpy().tobytes()
    assert not torch.cuda.is_initialized()
    assert not list(source.glob(".packed-stage-*"))
    assert (pin.read_bytes(), pin.stat()) == original_pin
    for suffix, content in preserved.items():
        assert (source / f"{key}.{suffix}").read_bytes() == content
    value["scores"].fill_(9)
    monkeypatch.setattr(cache, "_read", original_read)
    hit, proof = cache.cached_forward(source, identity, lambda: pytest.fail("must not compute"))
    assert proof["state"] == "disk_hit" and torch.isnan(hit["scores"][0])


def test_never_registered_key_is_only_normal_cold_miss(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, _, catalog, cold, _, retention = fixture
    packed.initialize_packed_layout(cold)
    binding = restore.ForwardColdBinding(
        json.loads(catalog.read_text())["publication_families"][0], cold, retention, tmp_path)
    before = {p.name: p.read_bytes() for p in source.iterdir()}
    assert _restore(fixture, binding) is None
    assert {p.name: p.read_bytes() for p in source.iterdir() if not p.name.startswith(".namespace-")} == before


def test_fixed_restore_does_not_follow_or_read_a_changed_moving_head(tmp_path, monkeypatch, capsys):
    fixture, binding, pin, _ = _published(tmp_path, monkeypatch, capsys)
    cold = fixture[4]
    fixed = packed.resolve_packed_pin(cold, pin)
    for head in (cold / "heads" / fixed.manifest["dataset"]).glob("*.json"):
        head.write_text("{unrelated moving head is not a fixed binding}")
    value, receipt = _restore(fixture, binding)
    assert receipt["snapshot_id"] == fixed.manifest["snapshot_id"]
    assert torch.isnan(value["scores"][0])


@pytest.mark.parametrize("damage", ["missing-pin", "corrupt-pin", "symlink-pin", "missing-object", "corrupt-object"])
def test_registered_cold_failure_never_becomes_gpu_miss_or_modifies_originals(tmp_path, monkeypatch, capsys, damage):
    fixture, binding, pin, preserved = _published(tmp_path, monkeypatch, capsys)
    source, _, key, _, cold, *_ = fixture
    fixed = packed.resolve_packed_pin(cold, pin)
    if damage == "missing-pin":
        pin.unlink()
    elif damage == "corrupt-pin":
        pin.write_text("{damaged pin; retain}")
    elif damage == "symlink-pin":
        real = tmp_path / "unrelated-pin"
        pin.rename(real)
        pin.symlink_to(real)
    else:
        obj = cold / fixed.manifest["archive"]["objects"][0]["relpath"]
        if damage == "missing-object":
            obj.unlink()
        else:
            obj.write_bytes(b"damaged; retain")
    with pytest.raises((SnapshotError, OSError, ValueError)):
        _restore(fixture, binding)
    assert not (source / f"{key}.receipt.json").exists()
    assert not (source / f"{key}.forward").exists()
    assert all((tmp_path / f"original.{suffix}").read_bytes() == content for suffix, content in preserved.items())


@pytest.mark.parametrize("race", ["input", "pin", "key-owner"])
def test_source_or_owner_race_retains_private_stage_and_never_commits(tmp_path, monkeypatch, capsys, race):
    fixture, binding, pin, _ = _published(tmp_path, monkeypatch, capsys)
    source, _, key, *_ = fixture
    original_stage = restore.stage_packed_root_files
    staged = []
    changed = False

    def stage(*args, **kwargs):
        nonlocal changed
        result = original_stage(*args, **kwargs)
        staged.extend(result["files"].values())
        changed = True
        if race == "pin":
            pin.write_bytes(pin.read_bytes() + b" ")
        elif race == "key-owner":
            owner = source / f"{key}.lock"
            owner.rename(source / f"{key}.old-lock")
            owner.touch(mode=0o600)
        return result

    def validate():
        if changed and race == "input":
            raise RuntimeError("input generation changed")

    monkeypatch.setattr(restore, "stage_packed_root_files", stage)
    with pytest.raises((RuntimeError, SnapshotError)):
        _restore(fixture, binding, validate=validate)
    assert len(staged) == 2 and all(p.exists() for p in staged)
    assert not (source / f"{key}.forward").exists()
    assert not (source / f"{key}.receipt.json").exists()


def test_cold_restore_requires_original_key_owner_before_any_cold_io(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, identity, key, catalog, cold, _, retention = fixture
    binding = restore.ForwardColdBinding(
        json.loads(catalog.read_text())["publication_families"][0], cold, retention, tmp_path)
    monkeypatch.setattr(restore, "_check_packed_read_authority", lambda *_a: pytest.fail("owner first"))
    with (source / f"{key}.lock").open("rb") as descriptor:
        (source / f"{key}.lock").unlink()
        with pytest.raises((SnapshotError, OSError)):
            restore.restore_completed_forward(
                source, key, identity, binding=binding, key_descriptor=descriptor.fileno(),
                read=cache._read, validate_before_commit=None, maximum_payload_bytes=cache.MAX_PAYLOAD_BYTES)


def test_other_cache_roots_do_not_load_production_catalog(tmp_path, monkeypatch):
    monkeypatch.setattr(restore, "_configuration", lambda *_a: pytest.fail("offline roots stay independent"))
    assert restore.production_binding(tmp_path) is None


def test_canonical_cache_miss_uses_fixed_cold_result_before_compute(tmp_path, monkeypatch, capsys):
    fixture, binding, _, preserved = _published(tmp_path, monkeypatch, capsys)
    source, identity, key, *_ = fixture
    monkeypatch.setattr(restore, "production_binding", lambda root: binding)
    value, receipt = cache.cached_forward(source, identity, lambda: pytest.fail("cold result exists; no model"))
    assert receipt["state"] == "cold_restored" and receipt["gpu_forward_calls"] == 0
    assert receipt["capacity"]["state"].startswith("released")
    assert torch.isnan(value["scores"][0])
    assert (source / f"{key}.forward").read_bytes() == preserved["forward"]


def test_valid_hot_hit_does_not_need_cold_binding_or_capacity(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, identity, *_ = fixture
    monkeypatch.setattr(restore, "production_binding", lambda *_a: pytest.fail("valid hit must not access cold config"))
    _, receipt = cache.cached_forward(source, identity, lambda: pytest.fail("valid hot hit"))
    assert receipt["state"] == "disk_hit" and receipt["gpu_forward_calls"] == 0


def test_registered_pin_failure_propagates_from_original_cache_without_compute(tmp_path, monkeypatch, capsys):
    fixture, binding, pin, _ = _published(tmp_path, monkeypatch, capsys)
    source, identity, key, *_ = fixture
    pin.unlink()
    monkeypatch.setattr(restore, "production_binding", lambda root: binding)
    with pytest.raises(SnapshotError, match="missing its fixed pin"):
        cache.cached_forward(source, identity, lambda: pytest.fail("registered failure is not a model miss"))
    assert not (source / f"{key}.forward").exists()


def test_original_cache_defers_on_physical_pressure_before_model_work(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace
    from stockagent.data_sync import completed_forward_capacity as capacity

    fixture, binding, _, _ = _published(tmp_path, monkeypatch, capsys)
    source, identity, *_ = fixture
    monkeypatch.setattr(restore, "production_binding", lambda root: binding)
    monkeypatch.setattr(capacity.os, "statvfs", lambda _root: SimpleNamespace(f_bavail=1, f_frsize=4096))
    with pytest.raises(capacity.CapacityDeferred) as error:
        cache.cached_forward(source, identity, lambda: pytest.fail("no admission, no compute"))
    assert error.value.details["reason"] == "filesystem_headroom_exhausted"
    assert not list(source.glob(".packed-stage-*"))


def test_never_enrolled_input_keeps_entire_compute_serialize_commit_under_growth_lease(tmp_path, monkeypatch, capsys):
    from stockagent.data_sync.completed_forward_capacity import capacity_status

    fixture, binding, _, _ = _published(tmp_path, monkeypatch, capsys)
    source = fixture[0]
    identity = {"new input": "never enrolled"}
    key = cache.identity_digest(identity)
    seen = []
    monkeypatch.setattr(restore, "production_binding", lambda root: binding)

    def observe(stage):
        assert capacity_status(source)["active_reservations"][0]["key"] == key
        seen.append(stage)

    def compute():
        observe("compute")
        return {"raw": torch.tensor([-0., -7., float("nan")], dtype=torch.float64)}

    original_save, original_write = torch.save, cache.atomic_write_bytes

    def save(*args, **kwargs):
        observe("serialize")
        return original_save(*args, **kwargs)

    def write(*args, **kwargs):
        observe("durable_commit")
        return original_write(*args, **kwargs)

    monkeypatch.setattr(torch, "save", save)
    monkeypatch.setattr(cache, "atomic_write_bytes", write)
    value, receipt = cache.cached_forward(source, identity, compute)
    assert receipt["state"] == "computed" and receipt["capacity"]["state"] == "released"
    assert seen == ["compute", "serialize", "durable_commit", "durable_commit"]
    assert torch.signbit(value["raw"][0])
    assert not capacity_status(source)["active_reservations"]


def test_concurrent_original_consumers_restore_once_then_share_verified_hot_result(tmp_path, monkeypatch, capsys):
    from concurrent.futures import ThreadPoolExecutor

    fixture, binding, _, _ = _published(tmp_path, monkeypatch, capsys)
    source, identity, *_ = fixture
    monkeypatch.setattr(restore, "production_binding", lambda root: binding)
    original_stage = restore.stage_packed_root_files
    restores = []

    def stage(*args, **kwargs):
        restores.append(True)
        return original_stage(*args, **kwargs)

    monkeypatch.setattr(restore, "stage_packed_root_files", stage)
    with ThreadPoolExecutor(max_workers=3) as workers:
        results = list(workers.map(lambda _index: cache.cached_forward(
            source, identity, lambda: pytest.fail("already preserved in cold")), range(3)))
    assert restores == [True]
    assert sorted(receipt["state"] for _, receipt in results) == ["cold_restored", "disk_hit", "disk_hit"]
    assert all(torch.isnan(value["scores"][0]) for value, _ in results)


def test_production_binding_uses_existing_catalog_and_retention_and_fences_changes(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, _, catalog, _, _, retention = fixture
    config_dir = tmp_path / "configs/data_sync"
    config_dir.mkdir(parents=True)
    catalog.rename(config_dir / "packed_datasets.json")
    retention.rename(config_dir / "packed_retention.json")
    monkeypatch.setattr(restore, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(restore, "CANONICAL_CACHE", Path("cache"))
    binding = restore.production_binding(source)
    assert binding is not None and binding.entry["publish"] is True
    path = config_dir / "packed_datasets.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SnapshotError, match="configuration changed"):
        binding.guard()
