"""The optional native walker preserves canonical values, errors and fallback."""
import gc
import hashlib
import json
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import weakref

import pytest

from scripts import profile_workflow_speeds as profile
from stockagent import workflow_metric_tree as backend


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    if not (shutil.which("g++") or shutil.which("c++")):
        pytest.skip("optional C++ compiler unavailable")
    root = tmp_path_factory.mktemp("metric-native")
    module, info = backend.load_backend(root, "cpp")
    assert info["selected"] == "cpp"
    assert info["module_sha256"] == hashlib.sha256(Path(info["module_path"]).read_bytes()).hexdigest()
    return root, module, info


@pytest.mark.parametrize("payload", [
    {"seconds": 1, "elapsed_ms": 2, "total_s": True, "wall_seconds": -0.0},
    {"a~/b": [0, {"elapsed_s": 0}, None, {"seconds": 1.25}]},
    {"ſecret": {"seconds": 99}, "authorızation": {"seconds": 99}, "safe": {"seconds": 2}},
    {"seconds": float("nan"), "wall_seconds": float("inf"), "total_ms": -1, "median_ms": False},
    [1, 2, {"seconds": 3}, [None, {"total_s": 4}]],
])
def test_native_order_units_values_and_unicode_redaction_equal(compiled, monkeypatch, payload):
    monkeypatch.setattr(profile, "_METRIC_TREE", compiled[1])
    expected = list(profile.reported_metrics(payload))
    assert profile._metric_rows(payload) == expected
    for actual, reference in zip(profile._metric_rows(payload), expected):
        if type(actual["reported_value"]) is float:
            assert struct.pack("!d", actual["reported_value"]) == struct.pack("!d", reference["reported_value"])


@pytest.mark.parametrize("cached", [False, True])
def test_native_borrows_large_integer_and_does_not_retain_values(compiled, monkeypatch, cached):
    monkeypatch.setattr(profile, "_METRIC_TREE", compiled[1])
    monkeypatch.setattr(profile, "_METRIC_DESCRIPTOR",
                        compiled[1].cache_descriptor(profile._metric_descriptor) if cached else None)
    value = 10**70
    payload = {"seconds": value}
    before = sys.getrefcount(value)
    for _ in range(2000):
        rows = profile._metric_rows(payload)
        assert rows[0]["reported_value"] is value
    del rows
    gc.collect()
    assert sys.getrefcount(value) == before
    for selected in (None, compiled[1]):
        monkeypatch.setattr(profile, "_METRIC_TREE", selected)
        with pytest.raises(OverflowError):
            profile._metric_rows({"seconds": 10**400})


@pytest.mark.parametrize("cached", [False, True])
def test_native_unsupported_container_and_deep_tree_use_full_python_fallback(compiled, monkeypatch, cached):
    class Custom(dict):
        def items(self):
            return {"seconds": 7}.items()

    monkeypatch.setattr(profile, "_METRIC_TREE", compiled[1])
    monkeypatch.setattr(profile, "_METRIC_DESCRIPTOR",
                        compiled[1].cache_descriptor(profile._metric_descriptor) if cached else None)
    assert profile._metric_rows(Custom({"ignored": 1}))[0]["reported_value"] == 7
    payload = {"elapsed_s": 1}
    for _ in range(80):
        payload = {"node": payload}
    assert profile._metric_rows(payload) == list(profile.reported_metrics(payload))
    cyclic = {}
    cyclic["self"] = cyclic
    with pytest.raises(RecursionError):
        profile._metric_rows(cyclic)


def test_native_long_key_does_not_enter_bounded_key_cache(compiled, monkeypatch):
    monkeypatch.setattr(profile, "_METRIC_TREE", compiled[1])
    profile._metric_key.cache_clear()
    key = "~/" * 150 + "_seconds"
    rows = profile._metric_rows({key: 0.0})
    assert rows[0]["pointer"] == "/" + key.replace("~", "~0").replace("/", "~1")
    assert profile._metric_key.cache_info().currsize == 0


def test_cached_descriptor_preserves_order_units_redaction_and_numeric_identity(compiled):
    cache = compiled[1].cache_descriptor(profile._metric_descriptor)
    value = {"a~/b": [{"elapsed_ms": -0.0}, {"seconds": 10**70}],
             "ſecret": {"seconds": 99}, "authorızation": {"seconds": 99},
             "safe": {"seconds": 1.25}, "total_s": True}
    expected = list(profile.reported_metrics(value))
    for _ in range(10):
        actual = compiled[1].rows(value, "", cache, profile.finite_seconds)
        assert actual == expected
        assert all(row["reported_value"] is ref["reported_value"] for row, ref in zip(actual, expected))
    assert cache.cache_info()["hits"] > cache.cache_info()["misses"]
    assert struct.pack("!d", actual[0]["reported_value"]) == struct.pack("!d", -0.0)


def test_native_cache_has_cardinality_and_unicode_length_bounds(compiled):
    calls = []

    def descriptor(key):
        calls.append(key)
        return profile._metric_descriptor(key)

    cache = compiled[1].cache_descriptor(descriptor)
    for index in range(5000):
        cache(str(index))
        assert cache.cache_info()["size"] <= 4096
    info = cache.cache_info()
    for key in ("x" * 257 + "_seconds", "😀" * 257 + "_seconds"):
        assert cache(key) == profile._metric_descriptor(key)
        assert cache(key) == profile._metric_descriptor(key)
    assert cache.cache_info()["size"] == info["size"]
    assert cache.cache_info()["misses"] == info["misses"] + 4
    # The bound is characters, matching Python len(), not UTF-8 bytes.
    key = "😀" * 250
    cache(key)
    cache(key)
    assert calls.count(key) == 1


def test_descriptor_instances_do_not_share_callback_results(compiled):
    first = compiled[1].cache_descriptor(lambda key: (True, True, key))
    second = compiled[1].cache_descriptor(lambda key: (True, False, key))
    value = {"seconds": 2}
    assert compiled[1].rows(value, "", first, profile.finite_seconds)[0]["reported_value"] == 2
    assert compiled[1].rows(value, "", second, profile.finite_seconds) == []
    with pytest.raises(TypeError, match="callable"):
        compiled[1].cache_descriptor(None)


def test_descriptor_errors_are_not_cached_and_mutable_metadata_is_not_retained(compiled):
    calls = 0

    def transient(key):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("canonical classifier failure")
        return True, True, key

    cache = compiled[1].cache_descriptor(transient)
    with pytest.raises(ValueError, match="canonical classifier failure"):
        compiled[1].rows({"seconds": 2}, "", cache, profile.finite_seconds)
    assert cache.cache_info()["size"] == 0
    assert compiled[1].rows({"seconds": 2}, "", cache, profile.finite_seconds)[0]["reported_value"] == 2
    assert calls == 2
    mutable = compiled[1].cache_descriptor(lambda key: ([], [], key))
    assert mutable("seconds") == ([], [], "seconds")
    assert mutable.cache_info()["size"] == 0
    invalid = compiled[1].cache_descriptor(lambda key: (True, True))
    with pytest.raises(TypeError, match="descriptor ABI"):
        compiled[1].rows({"seconds": 2}, "", invalid, profile.finite_seconds)
    assert invalid.cache_info()["size"] == 0


def test_descriptor_callback_reference_cycles_are_collected(compiled):
    class Callback:
        def __call__(self, key):
            return True, True, key

    callback = Callback()
    reference = weakref.ref(callback)
    callback.cache = compiled[1].cache_descriptor(callback)
    assert callback.cache("seconds") == (True, True, "seconds")
    del callback
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize("cached", [False, True])
def test_deferred_paths_keep_prefix_offsets_unicode_and_sibling_order(compiled, cached):
    descriptor = profile._metric_descriptor
    if cached:
        descriptor = compiled[1].cache_descriptor(descriptor)
    value = {"a~/字": [{"empty": [{"unused": 1}]}, {"elapsed_ms": -0.0}],
             "sibling": {"seconds": 10**70}, "secret": {"seconds": 99},
             "last": [[{}, {"wall_seconds": 3}], {"seconds": 4}]}
    expected = list(profile.reported_metrics(value, pointer="/already~0escaped"))
    for _ in range(20):
        actual = compiled[1].rows(value, "/already~0escaped", descriptor, profile.finite_seconds)
        assert actual == expected
        assert all(row["reported_value"] is ref["reported_value"] for row, ref in zip(actual, expected))
    assert [row["pointer"] for row in actual] == [
        "/already~0escaped/a~0~1字/1/elapsed_ms", "/already~0escaped/sibling/seconds",
        "/already~0escaped/last/0/1/wall_seconds", "/already~0escaped/last/1/seconds"]


def test_negative_branches_still_visit_keys_and_propagate_descriptor_errors(compiled):
    calls = []

    def descriptor(key):
        calls.append(key)
        if key == "explode":
            raise ValueError("negative child must still be visited")
        return profile._metric_descriptor(key)

    with pytest.raises(ValueError, match="negative child"):
        compiled[1].rows({"unused": [{"leaf": 1}, {"explode": []}]}, "", descriptor, profile.finite_seconds)
    assert calls == ["unused", "leaf", "explode"]
    for child in ({}, [], {"ignored": 1}):
        with pytest.raises(TypeError):
            compiled[1].rows({"unused": child}, "", lambda key: (True, False, 1), profile.finite_seconds)


def test_deferred_ancestor_metadata_lifetime_and_reference_release(compiled):
    seen = []

    class Escaped(str):
        pass

    def descriptor(key):
        escaped = Escaped(key)
        seen.append(weakref.ref(escaped))
        return True, key == "seconds", escaped

    value = {"outer": [{"inner": [{"seconds": 2}]}]}
    assert compiled[1].rows(value, "", descriptor, profile.finite_seconds)[0]["pointer"] == "/outer/0/inner/0/seconds"
    gc.collect()
    assert all(reference() is None for reference in seen)


def test_backend_reconfiguration_replaces_cache_and_python_clears_it(compiled, monkeypatch):
    monkeypatch.setattr(profile, "load_backend", lambda root, mode:
                        (None, {"selected": "python"}) if mode == "python" else
                        (compiled[1], {"selected": "cpp"}))
    monkeypatch.setattr(profile, "_METRIC_TREE", None)
    monkeypatch.setattr(profile, "_METRIC_DESCRIPTOR", None)
    info = profile.configure_metric_backend("cpp")
    first = profile._METRIC_DESCRIPTOR
    assert info["descriptor_cache"]["scope"] == "key semantics only"
    assert profile._metric_rows({"seconds": 1})[0]["reported_value"] == 1
    assert first.cache_info()["size"] == 1
    profile.configure_metric_backend("cpp")
    assert profile._METRIC_DESCRIPTOR is not first
    assert profile._METRIC_DESCRIPTOR.cache_info()["size"] == 0
    profile.configure_metric_backend("python")
    assert profile._METRIC_DESCRIPTOR is None and profile._METRIC_TREE is None
    assert profile._metric_rows({"seconds": 1})[0]["reported_value"] == 1


def test_auto_and_python_do_not_compile_when_cache_absent(tmp_path, monkeypatch):
    def unexpected_compiler(name):
        raise AssertionError("auto and python must not dispatch a compiler")

    monkeypatch.setattr(backend.shutil, "which", unexpected_compiler)
    for mode in ("auto", "python"):
        module, info = backend.load_backend(tmp_path, mode)
        assert module is None and info["selected"] == "python"
    assert not (tmp_path / "artifacts").exists()


def test_source_and_module_identity_gate_precedes_load(compiled, tmp_path, monkeypatch):
    source_cache = Path(compiled[2]["module_path"]).parent
    target = tmp_path / "artifacts/benchmarks/workflow_speed_metric_backend" / source_cache.name
    shutil.copytree(source_cache, target)
    receipt = json.loads((target / "build_receipt.json").read_bytes())
    receipt["module_sha256"] = "0" * 64
    (target / "build_receipt.json").write_text(json.dumps(receipt))
    module, info = backend.load_backend(tmp_path, "auto")
    assert module is None and info["reason"] == "source_bound_cache_rejected"
    with pytest.raises(RuntimeError, match="identity verification"):
        backend.load_backend(tmp_path, "cpp")


def test_verified_cache_reuses_module_without_compiler(compiled, monkeypatch):
    monkeypatch.setattr(backend.shutil, "which", lambda name: None)
    module, info = backend.load_backend(compiled[0], "auto")
    assert module is not None and info["selected"] == "cpp"
    assert not info.get("compiled_this_call", False)


def test_native_readback_binds_frozen_source_binary_receipt_and_runtime(compiled, tmp_path):
    output = tmp_path / "observation"
    output.mkdir()
    store = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=4 << 20)
    metadata = dict(compiled[2])
    for key in ("source_path", "module_path", "build_receipt"):
        assert store.read(Path(metadata[key])) is not None
    (output / "speed_samples.jsonl").write_text("")
    profile.write_csv(output / "speed_summary.csv", [])
    profile.atomic_write_json(output / "inventory.json", {"rows": []})
    profile.atomic_write_json(output / "environment.json", {"python": sys.version})
    profile.atomic_write_json(output / "evidence_manifest.json", {"files": list(store.sources.values()), "issues": [], "traversal_gaps": []})
    report = {"speed_samples": 0, "summary_groups": 0, "sample_counts": {}, "inventory_counts": {},
              "metric_backend": metadata,
              "outputs": {path.name: {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
                          for path in output.iterdir() if path.is_file()}}
    profile.atomic_write_json(output / "collection.json", report)
    assert profile.verify_collection(output)["native_metric_backend_verified"] == 1
    report["metric_backend"]["module_sha256"] = "0" * 64
    profile.atomic_write_json(output / "collection.json", report)
    with pytest.raises(ValueError, match="frozen identity"):
        profile.verify_collection(output)
    report["metric_backend"]["module_sha256"] = compiled[2]["module_sha256"]
    profile.atomic_write_json(output / "environment.json", {"python": "different-runtime"})
    path = output / "environment.json"
    report["outputs"][path.name] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}
    profile.atomic_write_json(output / "collection.json", report)
    with pytest.raises(ValueError, match="build runtime"):
        profile.verify_collection(output)


def test_missing_optional_source_falls_back_but_explicit_cpp_fails(tmp_path, monkeypatch):
    original = Path.read_bytes
    source = Path(backend.__file__).with_suffix(".cpp")

    def read(path):
        if path == source:
            raise FileNotFoundError(path)
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read)
    module, info = backend.load_backend(tmp_path, "auto")
    assert module is None and info["reason"] == "native_source_unavailable"
    with pytest.raises(FileNotFoundError):
        backend.load_backend(tmp_path, "cpp")


@pytest.mark.parametrize("timeout", [False, True])
def test_failed_or_timed_out_build_retains_source_and_diagnostic(tmp_path, monkeypatch, timeout):
    monkeypatch.setattr(backend.shutil, "which", lambda name: "/isolated-test-compiler")

    def fail(command, **kwargs):
        if timeout:
            raise subprocess.TimeoutExpired(command, 120)
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="compiler failed\n")

    monkeypatch.setattr(backend.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="retained at"):
        backend.load_backend(tmp_path, "cpp")
    failures = list((tmp_path / "artifacts/benchmarks/workflow_speed_metric_backend").glob("failed-*"))
    assert len(failures) == 1
    assert (failures[0] / "workflow_metric_tree.cpp").read_bytes() == Path(backend.__file__).with_suffix(".cpp").read_bytes()
    assert (failures[0] / "compile.log").read_text().strip()
    receipt = json.loads((failures[0] / "failed_build_receipt.json").read_bytes())
    assert receipt["command"][0] == "/isolated-test-compiler"
    assert receipt["identity"]["source_sha256"] == hashlib.sha256(Path(backend.__file__).with_suffix(".cpp").read_bytes()).hexdigest()
