from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.live import completed_inference_cache as cache
from stockagent.live import signal_engine
from stockagent.config import DataConfig, load_config


def _dependency_fixture(tmp_path):
    package = tmp_path / "app"
    package.mkdir()
    sources = {
        "__init__.py": "from . import initialization\n",
        "initialization.py": "INITIALIZED = True\n",
        "entry.py": "from app import child\ndef lazy():\n    from .branch import helper\n",
        "child.py": "from .branch import helper\n",
        "branch/__init__.py": "from . import initialization\n",
        "branch/initialization.py": "INITIALIZED = True\n",
        "branch/helper.py": "from .. import child\nVALUE = 1\n",
        # This file cannot even be parsed. Directory-wide collection would
        # incorrectly make a separate unfinished subsystem an admission gate.
        "news.py": "an unrelated unfinished news implementation ???\n",
    }
    for name, source in sources.items():
        path = package / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(source, encoding="utf-8")
    roots = (package / "entry.py",)
    return package, lambda: cache._implementation_identity_from_roots(tmp_path, roots)


def test_inference_identity_follows_transitive_lazy_relative_and_package_imports(tmp_path):
    package, identity = _dependency_fixture(tmp_path)
    paths = {Path(row["path"]).relative_to(package).as_posix() for row in identity()}
    assert paths == {"__init__.py", "initialization.py", "entry.py", "child.py",
                     "branch/__init__.py", "branch/initialization.py", "branch/helper.py"}


def test_unconsumed_news_edits_additions_and_removal_do_not_invalidate_loaded_owner(tmp_path, monkeypatch):
    import json

    package, identity = _dependency_fixture(tmp_path)
    before = identity()
    monkeypatch.setattr(cache, "implementation_identity", identity)
    monkeypatch.setattr(cache, "_admitted_implementation_json", lambda: json.dumps(before))
    (package / "news.py").write_text("still unrelated and not valid Python !!!\n")
    (package / "news_quality.py").write_text("unfinished ???\n")
    assert cache.loaded_implementation_identity() == before
    (package / "news.py").unlink()
    assert cache.loaded_implementation_identity() == before


@pytest.mark.parametrize("dependency", ["child.py", "branch/helper.py", "branch/initialization.py"])
def test_real_dependency_change_requires_fresh_owner_even_with_restored_mtime(tmp_path, monkeypatch, dependency):
    import json

    package, identity = _dependency_fixture(tmp_path)
    before = identity()
    monkeypatch.setattr(cache, "implementation_identity", identity)
    monkeypatch.setattr(cache, "_admitted_implementation_json", lambda: json.dumps(before))
    target = package / dependency
    original = target.stat()
    target.write_text(target.read_text() + "CHANGED = True\n")
    os.utime(target, ns=(original.st_atime_ns, original.st_mtime_ns))
    with pytest.raises(cache.LoadedImplementationChanged):
        cache.loaded_implementation_identity()
    assert cache._admitted_implementation_json() == json.dumps(before)


def test_dependency_membership_is_not_cached_after_adding_or_removing_imported_child(tmp_path):
    package, identity = _dependency_fixture(tmp_path)
    before = identity()
    (package / "entry.py").write_text("try:\n    from app import optional\nexcept ImportError:\n    pass\n")
    absent = identity()
    (package / "optional.py").write_text("VALUE = 7\n")
    added = identity()
    assert before != absent != added
    assert str(package / "optional.py") in {row["path"] for row in added}
    (package / "optional.py").unlink()
    assert identity() == absent


def test_cached_graph_detects_later_namespace_package_without_initializer(tmp_path):
    entry = tmp_path / "entry.py"
    entry.write_text("try:\n    from later.branch import child\nexcept ImportError:\n    pass\n")
    roots = (entry,)
    before = cache._implementation_identity_from_roots(tmp_path, roots)
    branch = tmp_path / "later/branch"
    branch.mkdir(parents=True)
    (branch / "child.py").write_text("VALUE = 1\n")
    after = cache._implementation_identity_from_roots(tmp_path, roots)
    assert before != after
    assert str(branch / "child.py") in {row["path"] for row in after}


def test_cached_identity_rows_are_not_mutable_by_consumers(tmp_path):
    _package, identity = _dependency_fixture(tmp_path)
    before = identity()
    changed = identity()
    changed[0]["generation"][0] = -1
    changed[0]["sha256"] = "not the admitted hash"
    changed.pop()
    assert identity() == before


def test_dependency_syntax_cache_reuses_only_unchanged_consumed_sources(tmp_path, monkeypatch):
    package, identity = _dependency_fixture(tmp_path)
    parse = cache.ast.parse
    calls = []
    def counted(*args, **kwargs):
        calls.append(kwargs["filename"])
        return parse(*args, **kwargs)
    monkeypatch.setattr(cache.ast, "parse", counted)
    identity()
    assert len(calls) == 7
    identity()
    assert len(calls) == 7
    (package / "news.py").write_text("not parsed ???\n")
    identity()
    assert len(calls) == 7
    (package / "branch/helper.py").write_text("from .. import child\nVALUE = 2\n")
    identity()
    assert len(calls) == 8


def test_inference_identity_refuses_code_racing_dependency_discovery(tmp_path, monkeypatch):
    package, identity = _dependency_fixture(tmp_path)
    original = cache._repository_imports
    def racing(path, sha):
        imports = original(path, sha)
        if path.name == "helper.py":
            path.write_text("VALUE = 2\n")
        return imports
    monkeypatch.setattr(cache, "_repository_imports", racing)
    with pytest.raises(cache.LoadedImplementationChanged):
        identity()


@pytest.mark.parametrize("source", [
    "import importlib\nimportlib.import_module('app.child')\n",
    "__import__('app.child')\n",
    "from importlib import import_module as load\nload('app.child')\n",
])
def test_literal_dynamic_loader_retains_declared_dependency_identity(tmp_path, source):
    package, _identity = _dependency_fixture(tmp_path)
    path = package / "entry.py"
    path.write_text(source)
    paths = {row["path"] for row in cache._implementation_identity_from_roots(tmp_path, (path,))}
    assert str(package / "child.py") in paths
    assert str(package / "branch/helper.py") in paths


def test_canonical_inference_closure_excludes_news_and_retains_financial_dependencies():
    repo = Path(cache.__file__).resolve().parents[2]
    paths = {Path(row["path"]).relative_to(repo).as_posix()
             for row in cache.implementation_identity()}
    assert not any("news" in path for path in paths)
    assert {"stockagent/live/signal_engine.py", "stockagent/config.py",
            "stockagent/models/factory.py", "stockagent/models/financial_transformer.py",
            "stockagent/data/panel.py", "stockagent/data/tw_price_rules.py",
            "stockagent/data/factorized_panel.py", "stockagent/backtest/simulator.py",
            "stockagent/training/runtime.py", "stockagent/training/inference_contract.py",
            "stockagent/data_sync/immutable_cache_files.py"} <= paths


def _bits(tensor):
    return tensor.contiguous().view(torch.uint8).numpy().tobytes()


@pytest.mark.parametrize("dtype", [torch.float32, torch.float64, torch.bfloat16])
def test_lossless_outputs_are_owned_cpu_tensors_and_survive_reload(tmp_path, dtype):
    tensor = torch.tensor([0.0, -0.0, float("nan"), float("inf"), -2.5], dtype=dtype)
    original = {"weights": tensor, "aux": (tensor[1:3], None, [True, "raw"])}
    value, first = cache.cached_forward(tmp_path, {"input": "a"}, lambda: original)
    saved = _bits(value["weights"])
    tensor.fill_(1)
    loaded, second = cache.cached_forward(tmp_path, {"input": "a"},
        lambda: pytest.fail("cache hit must not compute"))
    assert _bits(loaded["weights"]) == saved
    assert _bits(loaded["aux"][0]) == _bits(value["aux"][0])
    assert loaded["weights"].dtype == dtype
    assert loaded["weights"].device.type == "cpu"
    assert first["state"] == "computed" and second["state"] == "disk_hit"
    assert second["gpu_forward_calls"] == 0
    assert all(path.stat().st_mode & 0o077 == 0 for path in tmp_path.iterdir())
    loaded["weights"].fill_(0)
    reloaded, _ = cache.cached_forward(tmp_path, {"input": "a"}, lambda: pytest.fail("compute"))
    assert _bits(reloaded["weights"]) == saved


def test_safe_none_result_and_tuple_identity_can_be_reused(tmp_path):
    cache.cached_forward(tmp_path, {"shape": (1, 2)}, lambda: None)
    value, receipt = cache.cached_forward(tmp_path, {"shape": (1, 2)}, lambda: pytest.fail("compute"))
    assert value is None and receipt["state"] == "disk_hit"


@pytest.mark.parametrize("kind", ["tuple", "dict", "tensor"])
def test_completed_signal_projection_keeps_consumed_values_and_source_inventory(tmp_path, kind):
    weights = torch.tensor([[0., -0., float("nan"), 1.]], dtype=torch.float64)
    aux = {key: weights.clone() for key in signal_engine._COMPLETED_SIGNAL_AUX_KEYS}
    # An unconsumed diagnostic must never reach the CPU copy/serializer. The
    # ordinary model output remains intact; this is a service projection only.
    aux["temporal_basis_input_features"] = object()
    raw_scores = weights.to(torch.bfloat16)
    output = ((weights, raw_scores, aux) if kind == "tuple" else
              {"weights": weights, **aux} if kind == "dict" else weights)
    original_weights, original_aux = signal_engine.extract_weights_and_aux(output)
    expected_score, expected_source = signal_engine._raw_score_tensor_from_model_output(
        output, original_aux)
    projection = signal_engine._completed_signal_projection(output)
    projected_weights, projected_aux = signal_engine.extract_weights_and_aux(projection["output"])
    assert projected_weights is original_weights  # No staging copy before the cache owns it.
    if original_aux is not None:
        assert projection["source_aux_fields"] == sorted(original_aux)
        assert "temporal_basis_input_features" not in projected_aux
        assert "temporal_basis_input_features" in original_aux
        for key in signal_engine._COMPLETED_SIGNAL_AUX_KEYS:
            assert projected_aux[key] is original_aux[key]
    else:
        assert projected_aux is None and projection["source_aux_fields"] == []
    loaded, _ = cache.cached_forward(tmp_path, {"output_profile": projection["contract"]},
                                    lambda: projection)
    assert _bits(signal_engine.extract_weights_and_aux(loaded["output"])[0]) == _bits(weights)
    score, source = signal_engine._raw_score_tensor_from_model_output(
        loaded["output"], signal_engine.extract_weights_and_aux(loaded["output"])[1])
    assert source == expected_source
    assert (score is None and expected_score is None) or _bits(score) == _bits(expected_score)


def test_completed_signal_projection_retains_tuple_fallback_score_without_aux():
    weights, scores = torch.tensor([[1.]]), torch.tensor([[-0.]], dtype=torch.bfloat16)
    projection = signal_engine._completed_signal_projection((weights, scores))
    projected = projection["output"]
    projected_weights, aux = signal_engine.extract_weights_and_aux(projected)
    raw, source = signal_engine._raw_score_tensor_from_model_output(projected, aux)
    assert projected_weights is weights and raw is scores and source == "scores"
    assert aux is None and projection["source_aux_fields"] == []


def test_every_live_aux_value_consumer_is_in_completed_signal_projection():
    """A new live consumer requires an intentional profile/version review."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(signal_engine))
    consumed = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (isinstance(node.func, ast.Attribute) and node.func.attr == "get"
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "aux"
                and node.args and isinstance(node.args[0], ast.Constant)):
            consumed.add(node.args[0].value)
        if (isinstance(node.func, ast.Name) and node.func.id == "_aux_scalar_by_symbol"
                and len(node.args) > 1 and isinstance(node.args[1], ast.Constant)):
            consumed.add(node.args[1].value)
    score_reader = ast.parse(inspect.getsource(signal_engine._raw_score_tensor_from_model_output))
    for node in ast.walk(score_reader):
        if (isinstance(node, ast.For) and isinstance(node.target, ast.Name)
                and node.target.id == "key" and isinstance(node.iter, ast.Tuple)):
            consumed.update(item.value for item in node.iter.elts if isinstance(item, ast.Constant))
    assert consumed <= signal_engine._COMPLETED_SIGNAL_AUX_KEYS


def test_different_inputs_still_compute_but_identical_outputs_share_payload(tmp_path, monkeypatch):
    import json

    calls = []
    def compute():
        calls.append(True)
        return {"weights": torch.tensor([0., -0., float("nan"), 2.], dtype=torch.float64)}
    first_identity, second_identity = {"input": "old"}, {"input": "new"}
    _, first = cache.cached_forward(tmp_path, first_identity, compute)
    written = []
    original = cache.atomic_write_bytes
    def record(path, *args, **kwargs):
        written.append(path.suffix)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(cache, "atomic_write_bytes", record)
    _, second = cache.cached_forward(tmp_path, second_identity, compute)
    a, b = (tmp_path / f"{receipt['key']}.forward" for receipt in (first, second))
    assert calls == [True, True]  # Storage equality does not waive changed-input compute.
    assert second["state"] == "computed" and second["storage_build"]["reused_payload"]
    assert second["storage_build"]["avoided_payload_bytes"] == a.stat().st_size
    assert os.path.samefile(a, b) and a.stat().st_mode & 0o222 == 0
    assert ".forward" not in written
    record = json.loads(b.with_suffix(".receipt.json").read_bytes())
    assert record["identity"] == second_identity and record["key"] != first["key"]
    a.unlink()  # Each key owns a link; retiring its donor cannot break it.
    loaded, _ = cache.cached_forward(tmp_path, second_identity, lambda: pytest.fail("compute"))
    before = _bits(loaded["weights"])
    loaded["weights"].fill_(7.)
    loaded, _ = cache.cached_forward(tmp_path, second_identity, lambda: pytest.fail("compute"))
    assert _bits(loaded["weights"]) == before


@pytest.mark.parametrize("damage", ["writable", "busy", "receipt", "payload"])
def test_unsupported_forward_donor_is_a_miss_not_a_changed_authority(tmp_path, damage):
    from contextlib import nullcontext
    import json

    _, first = cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([3.]))
    source = tmp_path / f"{first['key']}.forward"
    lease = nullcontext()
    if damage == "writable":
        source.chmod(0o600)
    elif damage == "busy":
        lease = cache._singleflight(source.with_suffix(".lock"))
    elif damage == "receipt":
        receipt_path = source.with_suffix(".receipt.json")
        receipt = json.loads(receipt_path.read_bytes())
        receipt["identity"] = {"wrong": "input"}
        receipt_path.write_text(json.dumps(receipt))
    else:
        content = bytearray(source.read_bytes())
        content[-1] ^= 1
        source.write_bytes(content)
    before = source.read_bytes()
    with lease:
        result, second = cache.cached_forward(tmp_path, {"input": "b"}, lambda: torch.tensor([3.]))
    assert result.tolist() == [3.] and not second["storage_build"]["reused_payload"]
    assert not os.path.samefile(source, tmp_path / f"{second['key']}.forward")
    assert source.read_bytes() == before


def test_incomplete_payload_receipt_commit_is_repaired(tmp_path):
    identity = {"input": "crash"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([1.]))
    (tmp_path / f"{cache.identity_digest(identity)}.receipt.json").unlink()
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([2.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [2.]


def test_inputs_are_revalidated_after_content_lookup_before_receipt_commit(tmp_path, monkeypatch):
    changed = []
    def donor_lookup(*args, **kwargs):
        changed.append(True)
        return False
    def validate():
        if changed:
            raise RuntimeError("inputs changed during storage lookup")
    monkeypatch.setattr(cache, "_reuse_payload", donor_lookup)
    with pytest.raises(RuntimeError, match="during storage"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([1.]),
                             validate_before_commit=validate)
    assert not list(tmp_path.glob("*.receipt.json"))
    assert not list(tmp_path.glob("*.forward"))


@pytest.mark.parametrize("damage", [b"[]", b'{"payload_bytes":0}', b""])
def test_corrupt_receipt_is_repaired_not_accepted(tmp_path, damage):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    (tmp_path / f"{cache.identity_digest(identity)}.receipt.json").write_bytes(damage)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [4.]


def test_receipt_symlink_does_not_disclose_or_overwrite_target(tmp_path):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    receipt = tmp_path / f"{cache.identity_digest(identity)}.receipt.json"
    receipt.unlink()
    target = tmp_path / "unrelated"
    target.write_text("preserve")
    receipt.symlink_to(target)
    _, result = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert result["repaired_invalid_cache"] and target.read_text() == "preserve"


def test_shared_writable_mapping_is_rejected(tmp_path, monkeypatch):
    import mmap

    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    monkeypatch.setattr(torch, "load", lambda *a, **kw: pytest.fail("unsafe map"))
    with torch.serialization.set_default_mmap_options(mmap.MAP_SHARED):
        value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [4.]


def test_fresh_process_hit_never_initializes_or_queries_cuda(tmp_path):
    cache.cached_forward(tmp_path, {"input": "persisted"}, lambda: torch.tensor([2.0]))
    program = """
import sys
from pathlib import Path
import torch
from stockagent.live.completed_inference_cache import cached_forward
def forbidden(*a, **kw):
    raise AssertionError('GPU or model compute attempted on a cache hit')
torch.cuda.is_available = forbidden
torch.cuda.init = forbidden
torch.cuda.synchronize = forbidden
torch.cuda.empty_cache = forbidden
assert not torch.cuda.is_initialized()
value, receipt = cached_forward(Path(sys.argv[1]), {'input': 'persisted'}, forbidden)
assert value.tolist() == [2.0] and receipt['gpu_forward_calls'] == 0
assert not torch.cuda.is_initialized()
"""
    result = subprocess.run([sys.executable, "-c", program, str(tmp_path)],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("damage", [b"", b"[]\ninvalid", b'{"key":"wrong"}\ninvalid'])
def test_corrupt_cache_is_rebuilt_not_returned(tmp_path, damage):
    identity = {"input": "a"}
    path = tmp_path / f"{cache.identity_digest(identity)}.forward"
    path.write_bytes(damage)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3]))
    assert value.tolist() == [3]
    assert receipt["repaired_invalid_cache"] is True
    _, receipt = cache.cached_forward(tmp_path, identity, lambda: pytest.fail("not rebuilt"))
    assert receipt["state"] == "disk_hit"


def test_payload_tampering_and_symlink_cannot_be_loaded(tmp_path):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3]))
    path = tmp_path / f"{cache.identity_digest(identity)}.forward"
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    _, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4]))
    assert receipt["repaired_invalid_cache"]
    target = tmp_path / "unrelated"
    target.write_text("preserve this")
    path.unlink()
    path.symlink_to(target)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([5]))
    assert value.tolist() == [5] and receipt["repaired_invalid_cache"]
    assert target.read_text() == "preserve this" and not path.is_symlink()


def test_concurrent_same_identity_computes_exactly_once(tmp_path):
    barrier = threading.Barrier(6)
    calls = []

    def compute():
        calls.append(True)
        time.sleep(0.08)
        return torch.tensor([7.0])

    def request():
        barrier.wait(timeout=5)
        return cache.cached_forward(tmp_path, {"input": "same"}, compute)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: request(), range(6)))
    assert calls == [True]
    assert sum(receipt["state"] == "computed" for _, receipt in results) == 1
    assert all(value.tolist() == [7.0] for value, _ in results)


def test_failed_compute_or_changed_identity_is_not_published(tmp_path):
    def changed():
        raise RuntimeError("changed before commit")

    with pytest.raises(RuntimeError, match="changed"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([1]),
                             validate_before_commit=changed)
    assert not list(tmp_path.glob("*.forward"))
    cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([2]))
    # Revalidate hits as well as newly computed outputs.
    with pytest.raises(RuntimeError, match="changed"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: pytest.fail("compute"),
                             validate_before_commit=changed)


def test_bounded_wait_and_payload_reject_without_deadlock(tmp_path, monkeypatch):
    path = tmp_path / "same.lock"
    with cache._singleflight(path):
        with pytest.raises(TimeoutError):
            with cache._singleflight(path, timeout_seconds=0.01):
                pytest.fail("lock was not exclusive")
    monkeypatch.setattr(cache, "MAX_PAYLOAD_BYTES", 32)
    with pytest.raises(ValueError, match="bounded"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.ones(100))
    assert not list(tmp_path.glob("*.forward"))


def test_input_identities_preserve_shape_dtype_order_and_empty_arrays():
    array = np.array([[0.0, -0.0], [1.0, 2.0]], dtype=np.float32)
    assert cache.array_identity(array) != cache.array_identity(array[:, ::-1])
    assert cache.array_identity(array) != cache.array_identity(array.astype(np.float64))
    assert cache.array_identity(array) != cache.array_identity(array.reshape(-1))
    assert cache.array_identity(np.empty((0, 3), dtype=np.float32))["shape"] == [0, 3]
    with pytest.raises(ValueError, match="Python objects"):
        cache.array_identity(np.array([object()], dtype=object))


def test_same_day_in_place_source_repair_invalidates_generation(tmp_path):
    root = tmp_path / "data" / "stock"
    root.mkdir(parents=True)
    member = root / "2330_features.parquet"
    member.write_bytes(b"before")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    first = cache.source_fingerprint(config)
    before = member.stat()
    member.write_bytes(b"repair")
    os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert cache.source_fingerprint(config) != first
    member.rename(root / "2317_features.parquet")
    assert cache.source_fingerprint(config) != first


def test_source_identity_ignores_unconsumed_provider_tables_and_directory_churn(tmp_path):
    root = tmp_path / "data" / "stocks"
    root.mkdir(parents=True)
    (root / "2330_features.parquet").write_bytes(b"published")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    fingerprint = lambda: cache.source_fingerprint(config, rule_data_dir=root.parent)
    before = fingerprint()
    raw = root.parent / "tpex_api_mopsfin_t187ap13_o.parquet"
    raw.write_bytes(b"new unconsumed provider snapshot")
    (root / "status.json").write_text("heartbeat")
    assert fingerprint() == before
    raw.write_bytes(b"next snapshot")
    assert fingerprint() == before
    (root / "2317_features.parquet").write_bytes(b"new consumed member")
    assert fingerprint() != before


def test_fallback_eligibility_cache_identity_detects_repair_with_restored_mtime(tmp_path, monkeypatch):
    root = tmp_path / "stocks"
    root.mkdir()
    paths = [tmp_path / "twse_day_trade_eligibility.parquet",
             tmp_path / "tpex_day_trade_eligibility.parquet", root / "symbols.csv"]
    for path in paths:
        path.write_bytes(b"before")
    panel = SimpleNamespace(dates=np.array([np.datetime64("2020-01-01")]),
                            num_dates=1, symbols=["2330"], day_trade_eligible_mask=None)
    identities = []

    def rules(*args):
        identities.append(args[-1])
        return np.ones(1, dtype=bool), np.ones(1, dtype=bool), {"source": "checked"}

    monkeypatch.setattr(signal_engine, "_cached_session_day_trade_rules", rules)
    kwargs = dict(session_date="2020-01-02", parquet_root=root, rule_data_dir=tmp_path)
    signal_engine._day_trade_model_eligibility(panel, **kwargs)
    before = paths[0].stat()
    paths[0].write_bytes(b"repair")
    os.utime(paths[0], ns=(before.st_atime_ns, before.st_mtime_ns))
    signal_engine._day_trade_model_eligibility(panel, **kwargs)
    assert identities[0] != identities[1]


@pytest.mark.parametrize("kind", ["feature", "receipt", "eligibility", "action", "entitlement"])
def test_source_identity_tracks_consumed_external_rules_and_receipts(tmp_path, kind):
    root = tmp_path / "data" / "stocks"
    root.mkdir(parents=True)
    external = root.parent / "features" / "public.parquet"
    external.parent.mkdir()
    external.write_bytes(b"features")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index",
        use_tw_public_features=True, tw_public_feature_path=str(external)))
    fingerprint = lambda: cache.source_fingerprint(config, rule_data_dir=root.parent)
    before = fingerprint()
    member = {"feature": external, "receipt": external.with_suffix(".summary.json"),
              "eligibility": root.parent / "twse_day_trade_eligibility.parquet",
              "action": root.parent / "tw_corporate_action_reference.parquet",
              "entitlement": root.parent / "tw_corporate_action_entitlements.summary.json"}[kind]
    member.write_bytes(b"repaired published input")
    assert fingerprint() != before


def test_factorized_identity_uses_verified_content_without_dense_materialization():
    class Window:
        _stockagent_factorized_live_window = True
        features = SimpleNamespace(content_fingerprint={"sha256": "verified"})
        start, stop, shape = 0, 3, (3, 2, 4)
        overlay = None

        def __array__(self, *args):
            pytest.fail("must not materialize the full factorized panel")

    window = Window()
    first = cache.window_identity(window)
    window.overlay = (2, np.ones(2, dtype=np.float32))
    assert cache.window_identity(window) != first


def _toy_forward(tmp_path, monkeypatch):
    config_path, checkpoint = tmp_path / "config.yaml", tmp_path / "checkpoint.pt"
    config_path.write_text("runtime: stable")
    checkpoint.write_bytes(b"model-identity")
    monkeypatch.setenv("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT", str(tmp_path / "cache"))
    calls = []
    monkeypatch.setattr(signal_engine, "resolve_device", lambda _: torch.device("cpu"))
    monkeypatch.setattr(signal_engine, "_cached_live_model", lambda **_: (torch.nn.Identity(), False))

    def forward(model, x, mask, **kwargs):
        calls.append(mask.clone())
        weights = mask.to(torch.float32) / 10
        return weights, {"raw_scores": weights.clone(), "aux": torch.tensor([1.0], dtype=torch.bfloat16)}

    monkeypatch.setattr(signal_engine, "call_model", forward)
    config = load_config("configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_live_v1.yaml")
    config.training.lookback = 3
    config.environment.device = "cpu"
    kwargs = dict(config=config,
        config_path=config_path, checkpoint=checkpoint,
        checkpoint_key=signal_engine._checkpoint_cache_key(checkpoint), state_dict={},
        panel=SimpleNamespace(symbols=["2330", "2317"], feature_names=["a", "b", "c", "d"],
            dates=["2026-10-09", "2026-10-12", "2026-10-13"], num_symbols=2),
        x_np=np.ones((3, 2, 4), dtype=np.float32), mask_np=np.array([True, False]),
        runtime_device=torch.device("cpu"), amp_dtype=None, non_blocking=False,
        include_unconstrained=False, completed_cache=True, panel_idx=2,
        source_fingerprint="accepted-close")
    return kwargs, calls


def test_canonical_forward_hit_skips_model_and_optional_raw_has_separate_mask(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    first, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "computed" and len(calls) == 1
    result, raw, _, receipt = signal_engine._live_forward_outputs(
        **{**kwargs, "include_unconstrained": True})
    assert _bits(result[0]) == _bits(first[0]) and len(calls) == 2
    assert raw[0].tolist() == [[pytest.approx(0.1), pytest.approx(0.1)]]
    assert receipt["state"] == "disk_hit" and receipt["unconstrained"]["state"] == "computed"
    monkeypatch.setattr(signal_engine, "resolve_device", lambda _: pytest.fail("GPU readiness queried"))
    monkeypatch.setattr(signal_engine, "_cached_live_model", lambda **_: pytest.fail("model loaded"))
    _, _, _, receipt = signal_engine._live_forward_outputs(**{**kwargs, "include_unconstrained": True})
    assert receipt["state"] == receipt["unconstrained"]["state"] == "disk_hit"


@pytest.mark.parametrize("change", ["mask", "input", "config", "inherited_config"])
def test_changed_forward_identity_recomputes(tmp_path, monkeypatch, change):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    signal_engine._live_forward_outputs(**kwargs)
    if change == "mask":
        kwargs["mask_np"] = np.ones(2, dtype=bool)
    elif change == "input":
        kwargs["x_np"][0, 0, 0] = 2
    elif change == "config":
        kwargs["config_path"].write_text("runtime: updated")
    else:
        kwargs["config"].training.seed += 1
    _, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "computed" and len(calls) == 2


def test_source_metadata_revision_rebuilds_summary_but_does_not_duplicate_identical_forward(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    signal_engine._live_forward_outputs(**kwargs)
    kwargs["source_fingerprint"] = "newly-accepted-source"
    _, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "disk_hit" and len(calls) == 1
    assert receipt["accepted_source_generation"] == "newly-accepted-source"


def test_output_projection_version_is_part_of_the_canonical_forward_key(tmp_path, monkeypatch):
    import json

    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    _, _, _, first = signal_engine._live_forward_outputs(**kwargs)
    root = tmp_path / "cache"
    identity = json.loads((root / f"{first['key']}.receipt.json").read_text())["identity"]
    assert identity["output_profile"] == signal_engine._COMPLETED_SIGNAL_PROJECTION
    assert first["output_projection"]["source_aux_fields"] == []
    monkeypatch.setattr(signal_engine, "_COMPLETED_SIGNAL_PROJECTION", "next_explicit_signal_abi")
    _, _, _, second = signal_engine._live_forward_outputs(**kwargs)
    assert first["key"] != second["key"] and len(calls) == 2
    assert (root / f"{first['key']}.forward").exists()  # No in-place legacy migration.


def test_candle_policy_has_its_own_key_and_only_completed_sessions_request_omission(tmp_path, monkeypatch):
    kwargs, _ = _toy_forward(tmp_path, monkeypatch)
    original, choices = signal_engine.call_model, []
    def observe(*args, **options):
        choices.append(options["materialize_candle_aux"])
        return original(*args, **options)
    monkeypatch.setattr(signal_engine, "call_model", observe)
    _, _, _, first = signal_engine._live_forward_outputs(**kwargs)
    monkeypatch.setattr(signal_engine, "_COMPLETED_CANDLE_AUX_POLICY", "next_explicit_diagnostic_policy")
    _, _, _, second = signal_engine._live_forward_outputs(**kwargs)
    assert first["key"] != second["key"]
    signal_engine._live_forward_outputs(**{**kwargs, "completed_cache": False})
    assert choices == [False, False, None]


def test_deployment_racing_forward_cannot_publish_old_result_under_new_identity(tmp_path, monkeypatch):
    kwargs, _ = _toy_forward(tmp_path, monkeypatch)
    original = signal_engine.call_model

    def replace(*args, **values):
        result = original(*args, **values)
        kwargs["config_path"].write_text("changed while computing")
        return result

    monkeypatch.setattr(signal_engine, "call_model", replace)
    with pytest.raises(RuntimeError, match="identity changed"):
        signal_engine._live_forward_outputs(**kwargs)
    assert not list((tmp_path / "cache").glob("*.forward"))


def test_live_forward_bypasses_persistent_cache(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    kwargs["completed_cache"] = False
    for _ in range(2):
        output, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
        assert receipt is None
        assert output[1]["aux"].dtype == torch.bfloat16  # Ordinary/live model ABI is unchanged.
    assert len(calls) == 2 and not (tmp_path / "cache").exists()


def test_model_cache_key_includes_resolved_inherited_configuration(tmp_path, monkeypatch):
    kwargs, _ = _toy_forward(tmp_path, monkeypatch)
    args = {"checkpoint_key": kwargs["checkpoint_key"], "runtime_device": kwargs["runtime_device"],
            "panel": kwargs["panel"], "lookback": 3, "config": kwargs["config"]}
    first = signal_engine._model_cache_key(**args)
    kwargs["config"].training.financial_transformer.causal_feature_rms_normalization = False
    assert signal_engine._model_cache_key(**args) != first


def test_forward_inventory_counts_full_tensor_outputs_without_value_reads():
    from scripts.benchmark_live_inference import forward_tensor_inventory

    output = (torch.empty((2, 3), device="meta"), None,
              {"bf16": torch.empty((7,), dtype=torch.bfloat16, device="meta")})
    rows = forward_tensor_inventory(output)
    assert sum(row["bytes"] for row in rows) == 24 + 14
    assert [row["path"] for row in rows] == ["output[0]", "output[2].bf16"]
