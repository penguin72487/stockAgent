"""Catalog compaction preserves raw metadata bindings and every admission gate."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import pytest

from stockagent.data_sync import backup_stream as module
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    _validate_manifest, initialize_packed_layout, publish_packed_snapshot,
)
from stockagent.runtime_identity import identity_sha256


def descriptors_under(root):
    """Observe only this test's descriptors, not unrelated SQLite GC timing."""
    result = set()
    for name in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink("/proc/self/fd/" + name)
        except FileNotFoundError:
            continue
        if target == str(root) or target.startswith(str(root) + "/"):
            result.add(name)
    return result


@pytest.fixture
def retained(tmp_path):
    cold, source = tmp_path / "cold", tmp_path / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir()
    (source / "價格.txt").write_text("-2.5,null,股\n", encoding="utf-8")
    (source / "shared.txt").write_text("original raw value\n")
    first = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1)
    (source / "價格.txt").write_text("-3.5,null,股\n", encoding="utf-8")
    second = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"datasets": [{"dataset": "prices", "publish": True}]}))
    return cold, catalog, first, second


@pytest.mark.parametrize("value", [
    {}, {"files": [], "releases": []},
    {"z": None, "a": [True, False, -0.0, 1e-20, 10**25]},
    {'quoted"key': [{"中": '中文\n"\\', "b": {"y": [1, 2], "a": None}}]},
    {"records": ({"raw": "x" * 1000000},), "flag": True},
    {"nested": {"raw": [3, 2, 1]}, "empty": ()},
])
def test_streamed_hash_is_exact_original_json(value):
    assert module.catalog_identity(value) == identity_sha256(value)


def test_hash_omits_only_file_pins_without_mutating_input():
    original = {"files": [{"relative": "原始值", "signature": [1, 2], "bytes": 3}],
                "other": [{"signature": "preserved", "value": -3.5}], "missing": None}
    before = deepcopy(original)
    expected = {**original, "files": [{"relative": "原始值", "bytes": 3}]}
    assert module.catalog_identity(original, omit_file_signatures=True) == identity_sha256(expected)
    assert original == before


@pytest.mark.parametrize("value", [{"bad": [math.nan]}, {"bad": {"number": math.inf}}, {1: "bad key"}])
def test_hash_retains_original_invalid_number_and_catalog_key_rejections(value):
    with pytest.raises(ValueError):
        module.catalog_identity(value)


def test_complete_catalog_identity_matches_original_and_raw_manifest_hashes(retained, monkeypatch):
    cold, catalog, first, second = retained
    reads = []
    original_read = module.PinnedObjectSignatures.read_metadata

    def read(pinned, relative):
        path = pinned.root / relative
        if path.parent.parent == cold / "manifests":
            reads.append(path)
        return original_read(pinned, relative)

    monkeypatch.setattr(module.PinnedObjectSignatures, "read_metadata", read)
    result = module.capture_catalog(cold, catalog)
    assert sorted(reads) == sorted([first.manifest_path, second.manifest_path])
    content = {k: v for k, v in result.items() if k not in {"identity_sha256", "observed_at_utc"}}
    content["files"] = [{k: v for k, v in r.items() if k != "signature"} for r in result["files"]]
    assert result["identity_sha256"] == identity_sha256(content)
    assert len(result["releases"]) == 2 and not result["metadata_errors"]
    for release in result["releases"]:
        assert release["manifest_sha256"] == hashlib.sha256((cold / release["manifest_relative"]).read_bytes()).hexdigest()
    shared = set(result["releases"][0]["required_file_keys"]) & set(result["releases"][1]["required_file_keys"])
    assert shared
    for key in shared:
        one = next(k for k in result["releases"][0]["required_file_keys"] if k == key)
        two = next(k for k in result["releases"][1]["required_file_keys"] if k == key)
        assert one is two


def test_compact_bindings_share_only_exact_object_descriptors(retained):
    _, _, first, _ = retained
    pool = {}
    one = module._catalog_manifest(first.manifest, pool, retain_objects=True)
    two = module._catalog_manifest(deepcopy(first.manifest), pool, retain_objects=True)
    assert all(a is b for a, b in zip(one.references, two.references))
    modified = deepcopy(first.manifest)
    modified["archive"]["objects"][0]["bytes"] += 1
    other = module._catalog_manifest(modified, pool, retain_objects=True)
    assert one.references[1] != other.references[1]
    assert one.references[1] is not other.references[1]
    assert not module._catalog_manifest(first.manifest, pool, retain_objects=False).references


@pytest.mark.parametrize("field,value", [
    ("sha256", "bad"), ("relpath", "../escape"), ("kind", "unknown"),
    ("bytes", -1), ("bytes", True), ("bytes", 1.0),
])
def test_validation_cache_does_not_hide_changed_invalid_descriptor(retained, field, value):
    _, _, first, _ = retained
    cache = set()
    _validate_manifest(first.manifest, object_validation_cache=cache)
    assert cache
    changed = deepcopy(first.manifest)
    changed["archive"]["objects"][0][field] = value
    with pytest.raises(SnapshotError):
        _validate_manifest(changed, object_validation_cache=cache)


def test_validation_cache_still_rejects_duplicate_objects_and_bad_manifest_clock(retained):
    _, _, first, _ = retained
    cache = set()
    _validate_manifest(first.manifest, object_validation_cache=cache)
    repeated = deepcopy(first.manifest)
    repeated["archive"]["objects"].append(deepcopy(repeated["archive"]["objects"][0]))
    with pytest.raises(SnapshotError, match="repeats object"):
        _validate_manifest(repeated, object_validation_cache=cache)
    changed = deepcopy(first.manifest)
    changed["publisher"]["node_id"] = "other"
    with pytest.raises(SnapshotError, match="publisher HLC"):
        _validate_manifest(changed, object_validation_cache=cache)


def test_capture_rejects_cross_manifest_size_conflict(retained):
    cold, catalog, first, second = retained
    common = {r["sha256"] for r in second.manifest["archive"]["objects"]}
    changed = deepcopy(first.manifest)
    ref = next(r for r in changed["archive"]["objects"] if r["sha256"] in common)
    ref["bytes"] += 1
    first.manifest_path.write_text(json.dumps(changed))
    with pytest.raises(SnapshotError, match="disagree on an immutable object"):
        module.capture_catalog(cold, catalog)


def test_pinned_metadata_reader_rejects_leaf_and_parent_symlinks(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "metadata.json").write_text("{}")
    (root / "redirected").symlink_to(outside, target_is_directory=True)
    (root / "leaf.json").symlink_to(outside / "metadata.json")
    with module.PinnedObjectSignatures(root) as pinned:
        for name in ("redirected/metadata.json", "leaf.json", "../outside/metadata.json"):
            with pytest.raises((SnapshotError, OSError)):
                pinned.read_metadata(name)


def test_pinned_metadata_reader_detects_directory_rebinding(tmp_path):
    directory = tmp_path / "manifests"
    directory.mkdir()
    (directory / "m.json").write_text("{}")
    with module.PinnedObjectSignatures(tmp_path) as pinned:
        raw, observed = pinned.read_metadata("manifests/m.json")
        assert raw == b"{}" and observed[2] == 2
        directory.rename(tmp_path / "old-manifests")
        directory.mkdir()
        with pytest.raises(SnapshotError, match="directory changed"):
            pinned.recheck()


def test_pinned_metadata_reader_rejects_fifo_without_waiting(tmp_path):
    os.mkfifo(tmp_path / "metadata.json")
    with module.PinnedObjectSignatures(tmp_path) as pinned:
        with pytest.raises(SnapshotError, match="not a regular"):
            pinned.read_metadata("metadata.json")


def test_pinned_metadata_reader_rejects_replaced_open_leaf(tmp_path, monkeypatch):
    path = tmp_path / "metadata.json"
    path.write_text("{}")
    old_fstat = os.fstat
    count = 0
    with module.PinnedObjectSignatures(tmp_path) as pinned:
        def fstat(descriptor):
            nonlocal count
            count += 1
            if count == 2:
                replacement = tmp_path / "replacement.json"
                replacement.write_text("[]")
                replacement.replace(path)
            return old_fstat(descriptor)
        monkeypatch.setattr(os, "fstat", fstat)
        with pytest.raises(SnapshotError, match="metadata changed"):
            pinned.read_metadata("metadata.json")


def test_nested_catalog_readers_stay_bounded_under_real_fd_limit(tmp_path):
    """A wide catalog must not starve other activities in the same worker."""
    child = r'''
import json, os, resource, sys
from pathlib import Path
from stockagent.data_sync.packed_backup import PinnedObjectSignatures
root = Path(sys.argv[1])
for index in range(400):
    path = root / "manifests" / ("dataset-%04d" % index) / "v.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"raw":-2.5,"missing":null}')
for index in range(256):
    digest = "%02x" % index + "0" * 62
    path = root / "objects/blobs" / digest[:2] / (digest + ".blob")
    path.parent.mkdir(parents=True)
    path.write_bytes(b"original")
before = len(os.listdir("/proc/self/fd"))
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(256, hard), hard))
peak = before
with PinnedObjectSignatures(root) as metadata:
    for index in range(400):
        raw, observed = metadata.read_metadata("manifests/dataset-%04d/v.json" % index)
        assert raw == b'{"raw":-2.5,"missing":null}' and observed[2] == len(raw)
        peak = max(peak, len(os.listdir("/proc/self/fd")))
    with PinnedObjectSignatures(root) as objects:
        for index in range(256):
            digest = "%02x" % index + "0" * 62
            assert objects.signature("objects/blobs/" + digest[:2] + "/" + digest + ".blob")[2] == 8
            peak = max(peak, len(os.listdir("/proc/self/fd")))
        objects.recheck()
    metadata.recheck()
assert len(os.listdir("/proc/self/fd")) == before
assert peak <= before + 128, peak
print(json.dumps({"peak_fds":peak,"before":before,"readers":2,"directories":660}))
'''
    result = subprocess.run([sys.executable, "-c", child, str(tmp_path)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["peak_fds"] <= 140


def test_evicted_directory_identity_is_rechecked_on_reopen_and_final_scan(tmp_path):
    for name in ("first", "second", "third"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "m.json").write_bytes(b"{}")
    with module.PinnedObjectSignatures(tmp_path, max_open_directories=2) as pinned:
        for name in ("first", "second", "third"):
            pinned.read_metadata(name + "/m.json")
        assert ("first",) not in pinned._directories
        (tmp_path / "first").rename(tmp_path / "original-first")
        (tmp_path / "first").mkdir()
        (tmp_path / "first/m.json").write_bytes(b"{}")
        with pytest.raises(SnapshotError, match="directory changed"):
            pinned.read_metadata("first/m.json")
        with pytest.raises(SnapshotError, match="directory changed"):
            pinned.recheck()


def test_evicted_parent_replaced_by_symlink_is_rejected(tmp_path):
    for name in ("first", "second"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "m.json").write_bytes(b"{}")
    with module.PinnedObjectSignatures(tmp_path, max_open_directories=2) as pinned:
        pinned.read_metadata("first/m.json")
        pinned.read_metadata("second/m.json")
        (tmp_path / "first").rename(tmp_path / "original-first")
        (tmp_path / "first").symlink_to(tmp_path / "original-first", target_is_directory=True)
        with pytest.raises(OSError):
            pinned.read_metadata("first/m.json")
        with pytest.raises(SnapshotError, match="directory changed"):
            pinned.recheck()


def test_bounded_directory_cache_remains_safe_for_shared_export_threads(tmp_path):
    names = []
    for index in range(128):
        digest = f"{index:02x}" + "0" * 62
        name = f"objects/blobs/{digest[:2]}/{digest}.blob"
        path = tmp_path / name
        path.parent.mkdir(parents=True)
        path.write_bytes(b"original")
        names.append(name)
    expected = [module.signature(tmp_path / name) for name in names]
    assert not descriptors_under(tmp_path)
    with module.PinnedObjectSignatures(tmp_path, max_open_directories=4) as pinned:
        with ThreadPoolExecutor(max_workers=8) as workers:
            for _ in range(4):
                assert list(workers.map(pinned.signature, names)) == expected
        pinned.recheck()
        assert len(pinned._directories) <= 4
    assert not descriptors_under(tmp_path)


def test_leaf_open_wrapper_failure_closes_descriptor(tmp_path, monkeypatch):
    (tmp_path / "m.json").write_bytes(b"{}")
    assert not descriptors_under(tmp_path)
    with module.PinnedObjectSignatures(tmp_path) as pinned:
        def broken(*args, **kwargs):
            raise MemoryError("injected file wrapper failure")
        monkeypatch.setattr(os, "fdopen", broken)
        for _ in range(4):
            with pytest.raises(MemoryError):
                pinned.read_metadata("m.json")
        assert len(descriptors_under(tmp_path)) == 1
    assert not descriptors_under(tmp_path)
    pinned.close()  # Idempotent, and no silent reuse of a closed descriptor.
    with pytest.raises(ValueError, match="closed"):
        pinned.read_metadata("m.json")


@pytest.mark.parametrize("budget", [True, 0, 1, -1, 2.0, None])
def test_directory_budget_is_validated_before_opening_root(tmp_path, budget):
    with pytest.raises(ValueError, match="descriptor budget"):
        module.PinnedObjectSignatures(tmp_path, max_open_directories=budget)


def test_wide_full_catalog_retains_every_fixed_release_under_fd_limit(retained):
    cold, catalog, first, second = retained
    datasets = ["prices"]
    head = json.loads((cold / "heads/prices/penguin.json").read_bytes())
    for index in range(160):
        name = f"dataset-{index:04d}"
        datasets.append(name)
        (cold / "manifests" / name).mkdir()
        (cold / "heads" / name).mkdir()
        copied = None
        for original in (first, second):
            manifest = deepcopy(original.manifest)
            manifest["dataset"] = name
            raw = json.dumps(manifest).encode()
            relative = f"manifests/{name}/{manifest['snapshot_id']}.json"
            (cold / relative).write_bytes(raw)
            copied = {**head, "dataset": name, "manifest_relpath": relative,
                      "manifest_sha256": hashlib.sha256(raw).hexdigest()}
        (cold / f"heads/{name}/penguin.json").write_text(json.dumps(copied))
    catalog.write_text(json.dumps({"datasets": [{"dataset": name, "publish": True} for name in datasets]}))
    child = r'''
import json, resource, sys
from pathlib import Path
from stockagent.data_sync.backup_stream import capture_catalog, catalog_identity
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(256, hard), hard))
result = capture_catalog(Path(sys.argv[1]), Path(sys.argv[2]))
assert not result["metadata_errors"] and not result["missing_objects"], result["metadata_errors"]
assert len(result["releases"]) == 322
assert sum(bool(r["current_head"]) for r in result["releases"]) == 161
body = {k:v for k,v in result.items() if k not in ("identity_sha256", "observed_at_utc")}
assert catalog_identity(body, omit_file_signatures=True) == result["identity_sha256"]
print(json.dumps({"releases":len(result["releases"]),"identity_sha256":result["identity_sha256"]}))
'''
    result = subprocess.run([sys.executable, "-c", child, str(cold), str(catalog)],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["releases"] == 322
