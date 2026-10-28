"""CPU preparation recovery: complete inventories, narrow receipts and ownership.

Fixtures model the original builders' output contracts. They do not establish
historical market coverage, a full production rebuild, CUDA or exact resume.
"""
import fcntl
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

import pytest

from stockagent.training_rescue_worker import (
    CONTRACT, PREPARED_BUILDERS, PREPARED_FILE_SET, atomic_json, digest,
    file_hash, preparation_references, prepare_step, validate_spec, prepared_file_set,
)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def setup_spec(tmp_path, kind="tw_futures_financial_v1"):
    source = tmp_path / "source"
    builder = source / PREPARED_BUILDERS[kind]
    builder.parent.mkdir(parents=True)
    builder.write_text("# fixed CPU builder fixture\n")
    root = tmp_path / "artifacts/data_preparation/rebuilt"
    spec = {"contract": CONTRACT, "schema_version": 1, "run_id": "prepared-fixture",
        "source_root": str(source), "runtime_prefix": sys.prefix,
        "output_root": str(tmp_path / "artifacts/markets/formal"),
        "config_file": str(source / "config.yaml"), "world_size": 1, "global_batch": 4,
        "environment": {}, "data_sources": [], "rebuild_steps": [
            [PREPARED_BUILDERS[kind], "--output", str(root)]]}
    spec["train_argv"] = ["train.py", "--config", spec["config_file"],
                          "--output-dir", spec["output_root"], "--resume"]
    return spec, root, builder


def oracle_fixture(tmp_path, kind="tw_futures_financial_v1"):
    spec, root, builder = setup_spec(tmp_path, kind)
    old = tmp_path / "old-prepared"
    originals = {"release/daily/continuous_daily.parquet": struct.pack("<ddd", -12.5, 0.0, 3.0),
                 "release/rules/rules.parquet": b"fixed exact financial rules"}
    if kind == "tw_futures_financial_v1":
        originals.update({"accounting_checkpoint.json": b'{"inputs":{"source":"fixed"}}',
            "product_scope.csv": b'product,status\nTX,admitted\n',
            "lifetime_scope.parquet": b'fixed complete lifetimes',
            "release/daily/manifest.json": b'{"rows":3,"source":"fixed"}',
            "release/rules/manifest.json": b'{"rules":"fixed"}'})
        summary = {"daily": str(old / "release/daily/continuous_daily.parquet"),
            "rules": str(old / "release/rules/rules.parquet"), "rows": 3,
            "source_sha256": "1" * 64, "point_in_time_verified": False}
        originals["release/build_summary.json"] = json.dumps(summary).encode()
        originals["build_acceptance.json"] = json.dumps({**summary,
            "elapsed_s": 60.5, "created_at_utc": "2026-10-25T10:00:00+00:00"}).encode()
    else:
        originals["0900/manifest.json"] = b'{"clock":"09:00","rows":3,"source_coverage_complete":false}'
    rows = [{"relative": name, "bytes": len(raw), "sha256": sha(raw),
             **({"original_json": raw.decode()} if name.endswith(".json") else {})}
            for name, raw in originals.items()]
    oracle = {"contract": PREPARED_FILE_SET, "artifact_kind": kind, "original_root": str(old),
              "scope": "published_financial_release_and_controls_v1" if kind == "tw_futures_financial_v1" else "all_clock_outputs_v1",
              "builder_sha256": file_hash(builder), "files": rows}
    reference = tmp_path / "controller/original-file-set.json"
    atomic_json(reference, oracle)
    spec["prepared_reconciliation"] = [{"step_index": 0, "kind": "file_set_v1",
        "reference_path": str(reference), "reference_sha256": file_hash(reference), "rebuilt_root": str(root)}]
    path = reference.with_name("spec.json")
    atomic_json(path, spec)
    return spec, path, root, oracle, originals


def write_rebuilt(root, oracle, originals):
    for name, raw in originals.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if name in {"build_acceptance.json", "release/build_summary.json"}:
            body = json.loads(raw)
            for key in ("daily", "rules"):
                body[key] = str(root / Path(body[key]).relative_to(oracle["original_root"]))
            if name == "build_acceptance.json":
                body.update(elapsed_s=0.25, created_at_utc="2026-10-26T00:00:00+00:00")
            raw = json.dumps(body, sort_keys=True, indent=2).encode()
        elif name.endswith(".json"):
            raw = json.dumps(json.loads(raw), sort_keys=True, indent=2).encode()
        target.write_bytes(raw)


@pytest.mark.parametrize("kind", list(PREPARED_BUILDERS))
def test_complete_cpu_file_set_recovery_and_repeat_readback(tmp_path, kind):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path, kind)
    assert validate_spec(spec) == spec
    assert preparation_references(spec, require_complete=True)[0]
    assert not prepare_step(path, 0)["skip_builder"]
    write_rebuilt(root, oracle, originals)
    assert prepare_step(path, 0, finish=True)["skip_builder"]
    assert {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()} == originals
    marker = json.loads((path.parent / "rebuild/0.json").read_bytes())
    proof = marker["proofs"][0]
    assert proof["files"] == len(originals) and not proof["changed_semantics"]
    assert not proof["fresh_gpu_acceptance_claim"]
    # Completed views are checked again. A cached complete marker is not a proof.
    assert prepare_step(path, 0)["skip_builder"]
    (root / "release/daily/continuous_daily.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SHA/size"):
        prepare_step(path, 0)


@pytest.mark.parametrize("damage", ["data", "missing", "extra", "file_symlink", "directory_symlink",
                                     "json_value", "json_type", "path_escape", "timing_nan"])
def test_no_receipt_is_restored_until_every_original_is_verified(tmp_path, damage):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    prepare_step(path, 0)
    write_rebuilt(root, oracle, originals)
    receipt = root / "build_acceptance.json"
    summary_before = receipt.read_bytes()
    data = root / "release/daily/continuous_daily.parquet"
    if damage == "data":
        data.write_bytes(b"incorrect values")
    elif damage == "missing":
        data.unlink()
    elif damage == "extra":
        (root / "release/unknown.bin").write_bytes(b"protect")
    elif damage == "file_symlink":
        data.unlink(); data.symlink_to(path)
    elif damage == "directory_symlink":
        (root / "release/unaccounted").symlink_to(path.parent, target_is_directory=True)
    else:
        target = root / "release/build_summary.json"
        body = json.loads(target.read_bytes())
        if damage == "json_value":
            body["source_sha256"] = "2" * 64
        elif damage == "json_type":
            body["point_in_time_verified"] = 0  # False == 0 must not pass.
        elif damage == "path_escape":
            body["daily"] = str(root.parent / "outside.parquet")
        else:
            target = receipt
            body = json.loads(receipt.read_bytes())
            body["elapsed_s"] = float("nan")
        target.write_text(json.dumps(body))
        if target == receipt:
            summary_before = target.read_bytes()
    with pytest.raises(ValueError):
        prepare_step(path, 0, finish=True)
    assert receipt.read_bytes() == summary_before
    assert json.loads((path.parent / "rebuild/0.json").read_bytes())["state"] == "building"


@pytest.mark.parametrize("damage", ["oracle_sha", "builder", "empty", "duplicate", "unsafe_name",
                                     "wrong_original_json", "unknown_kind", "bool_size"])
def test_invalid_original_oracle_rejects_before_builder_or_partial_move(tmp_path, damage):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    ref = spec["prepared_reconciliation"][0]
    if damage == "builder":
        (Path(spec["source_root"]) / spec["rebuild_steps"][0][0]).write_text("# changed builder\n")
    elif damage != "oracle_sha":
        if damage == "empty":
            oracle["files"] = []
        elif damage == "duplicate":
            oracle["files"].append(dict(oracle["files"][0]))
        elif damage == "unsafe_name":
            oracle["files"][0]["relative"] = "../outside"
        elif damage == "wrong_original_json":
            oracle["files"][-1]["original_json"] = "{}"
        elif damage == "unknown_kind":
            oracle["artifact_kind"] = "any-python"
        else:
            oracle["files"][0]["bytes"] = True
        atomic_json(Path(ref["reference_path"]), oracle)
        ref["reference_sha256"] = file_hash(Path(ref["reference_path"]))
    if damage == "oracle_sha":
        ref["reference_sha256"] = "0" * 64
    atomic_json(path, spec)
    with pytest.raises(ValueError):
        prepare_step(path, 0)
    assert not root.exists()
    assert not (path.parent / "rebuild/0.json").exists()


@pytest.mark.parametrize("damage", ["both_options", "missing_value", "unapproved_builder", "namespace_root",
                                     "nested_outputs", "source_overlap", "bad_ref_index", "outside_manifest",
                                     "reference_inside_output", "source_root_count"])
def test_preparation_path_and_schema_boundaries(tmp_path, damage):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    step = spec["rebuild_steps"][0]
    if damage == "both_options":
        step.extend(["--output-root", str(root)])
    elif damage == "missing_value":
        step.pop()
    elif damage == "unapproved_builder":
        step[0] = "scripts/arbitrary.py"
    elif damage == "namespace_root":
        step[-1] = str(root.parent)
    elif damage == "nested_outputs":
        spec["rebuild_steps"].append([step[0], "--output", str(root / "nested")])
    elif damage == "source_overlap":
        spec["data_sources"] = [{"dataset": "originals", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                                  "materialized_root": str(root.parent)}]
    elif damage == "bad_ref_index":
        spec["prepared_reconciliation"][0]["step_index"] = True
    elif damage == "outside_manifest":
        spec["prepared_reconciliation"] = [{"step_index": 0, "reference_path": str(path.parent / "original.json"),
                                            "rebuilt_manifest": str(root.parent / "elsewhere.json")}]
    elif damage == "reference_inside_output":
        spec["prepared_reconciliation"][0]["reference_path"] = str(root / "reference.json")
    else:
        spec["rebuild_source_roots"] = []
    with pytest.raises(ValueError):
        validate_spec(spec)


def test_prepared_step_has_one_kernel_lock_owner(tmp_path):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    prepare_step(path, 0)
    lock = path.parent / "rebuild/0.lock"
    with lock.open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            prepare_step(path, 0)
    assert not root.exists()


@pytest.mark.parametrize("equals", [False, True])
@pytest.mark.parametrize("finish", [False, True])
def test_live_output_writer_blocks_retry_without_moving_its_partial(tmp_path, equals, finish):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    prepare_step(path, 0)
    write_rebuilt(root, oracle, originals)
    args = ["--output=" + str(root)] if equals else ["--output", str(root)]
    writer = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", *args])
    try:
        with pytest.raises(ValueError, match="still running"):
            prepare_step(path, 0, finish=finish)
        assert (root / "build_acceptance.json").exists()
        assert not list(root.parent.glob(root.name + ".rescue-incomplete-*"))
    finally:
        writer.terminate(); writer.wait(timeout=10)


def test_incomplete_plan_is_rejected_before_cold_source_io(tmp_path):
    from stockagent.training_rescue import Rescue
    spec, root, builder = setup_spec(tmp_path)
    spec["data_sources"] = [{"dataset": "source", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                             "materialized_root": str(tmp_path / "source-materialization")}]
    # The uninitialized owner/SSH have no usable IO methods. Validation must fail first.
    owner = object.__new__(Rescue)
    with pytest.raises(ValueError, match="before source transfer"):
        owner.restore_inputs(object(), spec, tmp_path / "incoming")


def controller_fixture(tmp_path, monkeypatch):
    from stockagent.training_rescue import Rescue, load_policy
    repository = Path(__file__).resolve().parents[1]
    policy = load_policy(repository / "configs/training_rescue.json")
    policy.update(state_root=str(tmp_path / "controller-state"), cold_root=str(tmp_path / "cold"))
    owner = Rescue(policy)
    monkeypatch.setattr(owner, "guard", lambda: owner.cold.mkdir(parents=True, exist_ok=True))
    return owner


@pytest.mark.parametrize("damage", ["relative", "source_overlap", "output_overlap", "duplicate",
                                     "source_cache_overlap", "subtree_escape", "parent_alias"])
def test_source_aliases_cannot_redirect_recovery_owners(tmp_path, damage):
    spec, root, builder = setup_spec(tmp_path)
    source_binding(spec, tmp_path)
    row = spec["data_sources"][0]
    alias = tmp_path / "original/raw"
    row["links"] = [str(alias)]
    if damage == "relative":
        row["links"] = ["relative/raw"]
    elif damage == "source_overlap":
        row["links"] = [spec["source_root"]]
    elif damage == "output_overlap":
        row["links"] = [str(root.parent)]
    elif damage == "duplicate":
        row["links"] *= 2
    elif damage == "source_cache_overlap":
        row["links"] = [str(Path(row["materialized_root"]) / "alias")]
    elif damage == "subtree_escape":
        row["links"] = []
        row["subtree_links"] = {str(alias): "../outside"}
    else:
        target = tmp_path / "elsewhere"
        target.mkdir()
        alias.parent.symlink_to(target)
    with pytest.raises(ValueError):
        validate_spec(spec)


@pytest.mark.parametrize("snapshot", ["fixed", "other-fixed-version"])
def test_one_current_cache_link_cannot_admit_duplicate_or_conflicting_sources(tmp_path, snapshot):
    spec, root, builder = setup_spec(tmp_path)
    source_binding(spec, tmp_path)
    spec["data_sources"].append({**spec["data_sources"][0], "snapshot_id": snapshot})
    with pytest.raises(ValueError, match="duplicate/conflicting"):
        validate_spec(spec)


def test_input_preflight_cli_reads_pinned_controller_without_writes(tmp_path):
    import sys
    from stockagent.training_rescue import CACHE_CONTROLLER
    spec, root, builder = setup_spec(tmp_path)
    spec["rebuild_steps"] = []
    spec["source_root"] = str(Path(__file__).resolve().parents[1])
    source_binding(spec, tmp_path)
    alias = tmp_path / "legacy/raw"
    spec["data_sources"][0]["links"] = [str(alias)]
    path = tmp_path / "control/spec.json"
    atomic_json(path, spec)
    worker = Path(__file__).resolve().parents[1] / "stockagent/training_rescue_worker.py"
    command = [sys.executable, str(worker), "input-preflight", "--spec", str(path), "--index", "0",
               "--path", str(CACHE_CONTROLLER), "--sha256", file_hash(CACHE_CONTROLLER)]
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout)
    assert proof["spec_sha256"] == digest(spec) and proof["preflight_only"]
    assert not alias.parent.exists() and not Path(spec["data_sources"][0]["materialized_root"]).exists()
    assert not (path.parent / "source-inputs").exists()
    alias.parent.mkdir()
    alias.symlink_to(tmp_path / "unknown")
    rejected = subprocess.run(command, capture_output=True, text=True)
    assert rejected.returncode != 0 and alias.readlink() == tmp_path / "unknown"
    assert not (path.parent / "source-inputs").exists()


def test_pinned_cache_resource_materializes_real_archive_and_original_subtree(tmp_path, monkeypatch):
    from stockagent.training_rescue import CACHE_CONTROLLER
    import stockagent.training_rescue_worker as worker
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
    raw, cold = tmp_path / "archive-input", tmp_path / "packed"
    (raw / "repair/inputs").mkdir(parents=True)
    values = raw / "repair/inputs/raw.csv"
    values.write_bytes(b"-2,0,3\n")
    initialize_packed_layout(cold, node_id="fixture")
    release = publish_packed_snapshot(cold, "originals", raw, pack_buckets=2)
    spec, root, builder = setup_spec(tmp_path)
    spec["rebuild_steps"] = []
    source_binding(spec, tmp_path)
    row = spec["data_sources"][0]
    alias = tmp_path / "original/delta"
    row.update(snapshot_id=release.manifest["snapshot_id"], manifest_sha256=release.manifest_sha256,
               subtree_links={str(alias): "repair/inputs"})
    path = tmp_path / "controller/spec.json"
    atomic_json(path, spec)
    original_loader = worker.importlib.util.spec_from_file_location
    def load_with_fixture_cold(*args, **kwargs):
        selected = original_loader(*args, **kwargs)
        original_exec = selected.loader.exec_module
        def install(module):
            original_exec(module)
            resolve, use = module.resolve_packed_snapshot_id, module.use_materialized_snapshot
            def fixed_root(root):
                assert root in {Path("/srv/stockagent-packed"), cold}
                return cold
            module.resolve_packed_snapshot_id = lambda root, *a, **k: resolve(fixed_root(root), *a, **k)
            module.use_materialized_snapshot = lambda root, *a, **k: use(fixed_root(root), *a, **k)
        selected.loader.exec_module = install
        return selected
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(worker.importlib.util, "spec_from_file_location", load_with_fixture_cold)
    sha = file_hash(CACHE_CONTROLLER)
    original_builder_sha = file_hash(builder)
    with pytest.raises(ValueError, match="pinned SHA"):
        worker.materialize_input(path, 0, CACHE_CONTROLLER, "f" * 64)
    assert not alias.parent.exists()
    first = worker.materialize_input(path, 0, CACHE_CONTROLLER, sha)
    member = alias / "raw.csv"
    inode = member.stat().st_ino
    assert member.read_bytes() == values.read_bytes() and first["original_source_paths_ready"]
    assert first["lease"]["verification"] == "full" and first["lease"]["schema_version"] == 2
    second = worker.materialize_input(path, 0, CACHE_CONTROLLER, sha)
    assert member.stat().st_ino == inode and second["lease"]["verification"] == "full"
    assert file_hash(builder) == original_builder_sha
    assert json.loads((path.parent / "source-inputs/0.json").read_bytes()) == second


@pytest.mark.parametrize("fail_second_preflight", [True, False])
def test_every_input_alias_preflight_precedes_large_source_transfer(tmp_path, monkeypatch, fail_second_preflight):
    from types import SimpleNamespace
    import stockagent.data_sync.packed_snapshots as packed
    spec, root, builder = setup_spec(tmp_path)
    spec["rebuild_steps"] = []
    source_binding(spec, tmp_path)
    spec["data_sources"].append({**spec["data_sources"][0], "dataset": "other-originals"})
    owner = controller_fixture(tmp_path, monkeypatch)
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b"{}")
    def resolved(_, dataset, snapshot):
        return SimpleNamespace(manifest_sha256="a" * 64, manifest_path=manifest,
            manifest={"archive": {"objects": [], "inventory": {"relpath": "inventory", "sha256": "b" * 64,
                                                                  "bytes": 1}}})
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", resolved)
    events = []
    class Collision(Exception):
        pass
    class Target:
        def upload(self, path, data):
            self.controller_sha = sha(data)
            return {"sha256": self.controller_sha, "bytes": len(data)}
        def adapter(self, action, *arguments, **kwargs):
            index = int(arguments[arguments.index("--index") + 1])
            events.append((action, index))
            if fail_second_preflight and action == "input-preflight" and index == 1:
                raise Collision
            return {"spec_sha256": digest(spec), "preflight_only": action == "input-preflight",
                "manifest_sha256": "a" * 64, "cache_controller_sha256": self.controller_sha,
                "source_index": index, "dataset": spec["data_sources"][index]["dataset"], "snapshot_id": "fixed",
                "original_source_paths_ready": action == "materialize-input"}
    # The fixture manifest path must remain under the controller's fixed cold
    # namespace when deriving its remote destination; no real D paths are read.
    manifest_relative = Path("/srv/stockagent-packed/manifests/fixture.json")
    class ManifestPath(type(manifest)):
        def relative_to(self, root):
            return manifest_relative.relative_to(root)
    original_resolve = resolved
    def fixture_resolve(*arguments):
        value = original_resolve(*arguments)
        value.manifest_path = ManifestPath(manifest)
        return value
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", fixture_resolve)
    monkeypatch.setattr(owner, "push_cold_source", lambda *args: events.append(("transfer", args[2]["sha256"])))
    if fail_second_preflight:
        with pytest.raises(Collision):
            owner.restore_inputs(Target(), spec, tmp_path / "incoming")
        assert events == [("input-preflight", 0), ("input-preflight", 1)]
    else:
        owner.restore_inputs(Target(), spec, tmp_path / "incoming")
        assert events[:2] == [("input-preflight", 0), ("input-preflight", 1)]
        assert [event for event in events if event[0] == "materialize-input"] == [
            ("materialize-input", 0), ("materialize-input", 1)]


def source_binding(spec, tmp_path):
    spec["data_sources"] = [{"dataset": "originals", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                             "materialized_root": str(tmp_path / "raw-originals")}]


def preserve_reference(owner, spec):
    ref = spec["prepared_reconciliation"][0]
    raw = Path(ref["reference_path"]).read_bytes()
    target = owner.object_path(ref["reference_sha256"])
    target.parent.mkdir(parents=True)
    target.write_bytes(raw)
    return target, raw


def test_original_reference_cannot_live_inside_another_step_output(tmp_path):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    other = root.parent / "other"
    spec["rebuild_steps"].append([spec["rebuild_steps"][0][0], "--output", str(other)])
    spec["prepared_reconciliation"][0]["reference_path"] = str(other / "original.json")
    with pytest.raises(ValueError, match="outside rebuilt output"):
        validate_spec(spec)


def test_conflicting_original_reference_bindings_are_rejected(tmp_path):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    spec["prepared_reconciliation"][0].update(kind="factorized_manifest_v1",
        rebuilt_manifest=str(root / "manifest.json"))
    spec["prepared_reconciliation"].append({**spec["prepared_reconciliation"][0], "reference_sha256": "f" * 64})
    with pytest.raises(ValueError, match="conflicting SHA"):
        validate_spec(spec)


@pytest.mark.parametrize("damage", ["missing", "corrupt", "empty", "oversized", "symlink"])
@pytest.mark.parametrize("action", ["restore", "verify"])
def test_missing_or_corrupt_original_reference_rejects_before_raw_io(tmp_path, monkeypatch, damage, action):
    from stockagent.training_rescue import RescueError
    import stockagent.data_sync.packed_snapshots as packed
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    obj, raw = preserve_reference(owner, spec)
    if damage == "missing":
        obj.unlink()
    elif damage == "corrupt":
        obj.write_bytes(b"x" * len(raw))
    elif damage == "empty":
        obj.write_bytes(b"")
    elif damage == "oversized":
        with obj.open("wb") as stream:
            stream.truncate(32 * 1024**2 + 1)
    else:
        obj.unlink(); obj.symlink_to(path)
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", lambda *_: pytest.fail("no raw reads"))
    monkeypatch.setattr(owner, "push_object", lambda *_: pytest.fail("no destination contact"))
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    with pytest.raises((OSError, ValueError, RescueError)):
        if action == "restore":
            owner.restore_inputs(object(), spec, tmp_path / "incoming")
        else:
            owner.verify_sources(spec["run_id"])
    assert not owner.journal(spec["run_id"]).get("input_verification")
    assert not (owner.cold / "source-verification").exists()


def test_pinned_reference_uses_real_transfer_before_raw_and_deduplicates_paths(tmp_path, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    obj, raw = preserve_reference(owner, spec)
    ref = spec["prepared_reconciliation"][0]
    destination = tmp_path / "new-worker/original-file-set.json"
    ref.update(kind="factorized_manifest_v1", reference_path=str(destination),
               rebuilt_manifest=str(root / "manifest.json"))
    # Two dependent manifests may share one original oracle; transfer it once.
    spec["prepared_reconciliation"].append({**ref, "rebuilt_manifest": str(root / "other.json")})
    calls = []
    class ByteSSH:
        def command(self, argv):
            assert argv[0:3] == ["python3", "/opt/stockagent-training-rescue/worker.py", "receive"]
            calls.append(argv)
            worker = Path(__file__).resolve().parents[1] / "stockagent/training_rescue_worker.py"
            return [sys.executable, str(worker), *argv[2:]]
    class ReachedRaw(Exception):
        pass
    def raw_started(*_):
        assert destination.read_bytes() == raw and file_hash(destination) == ref["reference_sha256"]
        raise ReachedRaw
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", raw_started)
    for _ in range(2):  # Existing exact target bytes are acknowledged idempotently.
        with pytest.raises(ReachedRaw):
            owner.restore_inputs(ByteSSH(), spec, tmp_path / "incoming")
    assert len(calls) == 2 and obj.read_bytes() == raw


def test_original_prepared_alias_is_installed_under_step_lock_after_full_readback(tmp_path, monkeypatch):
    from stockagent.training_rescue import CACHE_CONTROLLER
    from stockagent.training_rescue_worker import prepared_link_preflight
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    alias = Path(oracle["original_root"])
    spec["prepared_links"] = [{"step_index": 0, "path": str(alias)}]
    atomic_json(path, spec)
    monkeypatch.setattr(sys, "path", list(sys.path))
    resource = {"controller": CACHE_CONTROLLER, "controller_sha": file_hash(CACHE_CONTROLLER)}
    preflight = prepared_link_preflight(path, **resource)
    assert preflight["preflight_only"] and not alias.exists() and not root.exists()
    assert not prepare_step(path, 0, **resource)["skip_builder"]
    assert not alias.exists()  # Admission/partial outputs are not usable views.
    write_rebuilt(root, oracle, originals)
    result = prepare_step(path, 0, finish=True, **resource)
    assert result["prepared_link_targets"] == {str(alias): str(root)}
    assert alias.is_symlink() and alias.readlink() == root
    assert {name: (alias / name).read_bytes() for name in originals} == originals
    inode = (alias / "release/daily/continuous_daily.parquet").stat().st_ino
    assert prepare_step(path, 0, **resource)["skip_builder"]
    assert (alias / "release/daily/continuous_daily.parquet").stat().st_ino == inode
    (root / "release/daily/continuous_daily.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SHA/size"):
        prepare_step(path, 0, **resource)
    assert alias.readlink() == root


@pytest.mark.parametrize("damage", ["regular", "foreign_link", "unknown_step", "source_overlap", "duplicate"])
def test_prepared_compatibility_conflicts_reject_before_owned_build_metadata(tmp_path, monkeypatch, damage):
    from stockagent.training_rescue import CACHE_CONTROLLER
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    alias = Path(oracle["original_root"])
    spec["prepared_links"] = [{"step_index": 0, "path": str(alias)}]
    if damage == "regular":
        alias.mkdir()
        (alias / "unknown").write_bytes(b"preserve")
    elif damage == "foreign_link":
        alias.symlink_to(tmp_path / "foreign")
    elif damage == "unknown_step":
        spec["prepared_links"][0]["step_index"] = 10
    elif damage == "source_overlap":
        spec["prepared_links"][0]["path"] = spec["source_root"]
    else:
        spec["prepared_links"].append(dict(spec["prepared_links"][0]))
    atomic_json(path, spec)
    monkeypatch.setattr(sys, "path", list(sys.path))
    with pytest.raises((ValueError, RuntimeError)):
        prepare_step(path, 0, controller=CACHE_CONTROLLER, controller_sha=file_hash(CACHE_CONTROLLER))
    assert not (path.parent / "rebuild").exists()
    if damage == "regular":
        assert (alias / "unknown").read_bytes() == b"preserve"


def test_all_original_reference_objects_preflight_before_first_target_write(tmp_path, monkeypatch):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    preserve_reference(owner, spec)
    ref = spec["prepared_reconciliation"][0]
    ref.update(kind="factorized_manifest_v1", rebuilt_manifest=str(root / "manifest.json"))
    spec["prepared_reconciliation"].append({**ref, "reference_sha256": "f" * 64,
        "reference_path": str(tmp_path / "missing-original.json"),
        "rebuilt_manifest": str(root / "second.json")})
    monkeypatch.setattr(owner, "push_object", lambda *_: pytest.fail("no partially delivered controls"))
    with pytest.raises(FileNotFoundError):
        owner.restore_inputs(object(), spec, tmp_path / "incoming")


def reference_batch_fixture(tmp_path, monkeypatch, count, *, repeated=False):
    import stockagent.data_sync.windows_cold_io as native
    spec, _, _ = setup_spec(tmp_path)
    spec.update(rebuild_steps=[], prepared_reconciliation=[], input_controls=[])
    owner = controller_fixture(tmp_path, monkeypatch)
    for i in range(count):
        raw = b"same exact metadata" if repeated else ("fixed-operand-" + str(i)).encode()
        checksum = sha(raw); obj = owner.object_path(checksum)
        obj.parent.mkdir(parents=True, exist_ok=True); obj.write_bytes(raw)
        spec['input_controls'].append({'path': str(tmp_path / ('operand-' + str(i) + '.json')),
                                       'sha256': checksum, 'bytes': len(raw)})
    monkeypatch.setattr(native, 'windows_path', lambda p: 'D:\\fixture')
    monkeypatch.setattr(owner, 'object_hash', lambda _: pytest.fail('no per-object native startup or fallback'))
    return owner, spec, native


@pytest.mark.parametrize('count,repeated', [(1, False), (17, False), (3, True)])
def test_original_metadata_full_hashes_batch_by_exact_CAS_object(tmp_path, monkeypatch, count, repeated):
    owner, spec, native = reference_batch_fixture(tmp_path, monkeypatch, count, repeated=repeated)
    calls = []
    def full_batch(paths):
        calls.append(list(paths))
        return {p: file_hash(p) for p in paths}
    monkeypatch.setattr(native, 'hash_many', full_batch)
    proof = owner.prepared_reference_objects(spec)
    assert len(proof) == count
    assert sum(map(len, calls)) == (1 if repeated else count)
    assert all(0 < len(batch) <= 16 and len(set(batch)) == len(batch) for batch in calls)
    assert [row['sha256'] for row in proof] == [row['sha256'] for row in sorted(spec['input_controls'], key=lambda r: r['path'])]


@pytest.mark.parametrize('damage', ['wrong_sha', 'missing', 'extra', 'generation', 'failure'])
def test_batched_original_metadata_never_accepts_partial_or_changed_native_proof(tmp_path, monkeypatch, damage):
    from stockagent.training_rescue import RescueError
    owner, spec, native = reference_batch_fixture(tmp_path, monkeypatch, 2)
    def bad_batch(paths):
        result = {p: file_hash(p) for p in paths}
        if damage == 'wrong_sha': result[paths[0]] = 'f' * 64
        elif damage == 'missing': result.pop(paths[0])
        elif damage == 'extra': result[tmp_path / 'foreign'] = 'f' * 64
        elif damage == 'generation': paths[0].write_bytes(paths[0].read_bytes())
        else: raise RuntimeError('native full readback failed; do not fall back')
        return result
    monkeypatch.setattr(native, 'hash_many', bad_batch)
    with pytest.raises(RuntimeError if damage == 'failure' else RescueError):
        owner.prepared_reference_objects(spec)


def test_pinned_original_reference_is_part_of_source_readiness(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from stockagent.training_rescue import input_source_readback_ready
    import stockagent.data_sync.packed_snapshots as packed
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    obj, raw = preserve_reference(owner, spec)
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    manifest_sha = spec["data_sources"][0]["manifest_sha256"]
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", lambda *_:
        SimpleNamespace(manifest_sha256=manifest_sha, manifest={"source": {"files": 1}}))
    monkeypatch.setattr(packed, "_load_inventory", lambda *_: pytest.fail("no duplicate full inventory load"))
    def full_readback(*args, **kwargs):
        assert kwargs == {"reconstruct_all": True, "d_primary_native_blob_reads": True,
                          "d_primary_native_pack_reads": True, "d_primary_native_chunk_reads": True}
        return {"independently_reconstructed_files": 1}
    monkeypatch.setattr(packed, "verify_packed_snapshot", full_readback)
    old_proof = {"input_verification": {"spec_sha256": digest(spec), "level": "all_original_file_SHA_decoded"}}
    assert not input_source_readback_ready(spec, old_proof)
    report = owner.verify_sources(spec["run_id"])
    assert report["prepared_reference_count"] == 1
    state = owner.journal(spec["run_id"])
    assert input_source_readback_ready(spec, state)
    proof = json.loads(Path(report["receipt"]).read_bytes())
    ref = spec["prepared_reconciliation"][0]
    assert proof["prepared_references"] == [{"reference_path": ref["reference_path"],
        "sha256": ref["reference_sha256"], "bytes": len(raw)}]
    state["input_verification"]["prepared_reference_sha256"][ref["reference_path"]] = "f" * 64
    assert not input_source_readback_ready(spec, state)


def test_original_reference_cli_is_readonly_and_complete(tmp_path):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    root.mkdir(parents=True)
    for name, raw in originals.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    before = {p: (p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
              for p in root.rglob("*")}
    worker = Path(__file__).resolve().parents[1] / "stockagent/training_rescue_worker.py"
    result = subprocess.run([sys.executable, str(worker), "prepared-reference", "--path", str(root),
        "--source-root", spec["source_root"], "--artifact-kind", "tw_futures_financial_v1"],
        check=True, capture_output=True)
    body = json.loads(result.stdout)
    assert body["contract"] == PREPARED_FILE_SET
    assert {row["relative"]: row["sha256"] for row in body["files"]} == {
        name: sha(raw) for name, raw in originals.items()}
    assert {p: (p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
            for p in root.rglob("*")} == before


def test_reference_census_rejects_inflight_mutation(tmp_path, monkeypatch):
    import stockagent.training_rescue_worker as worker
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    write_rebuilt(root, oracle, originals)
    original_hash = worker.file_hash
    def mutating_hash(path):
        result = original_hash(path)
        if path.name == "continuous_daily.parquet":
            path.write_bytes(path.read_bytes() + b"new-generation")
        return result
    monkeypatch.setattr(worker, "file_hash", mutating_hash)
    with pytest.raises(ValueError, match="changed during inventory"):
        prepared_file_set(root, artifact_kind="tw_futures_financial_v1", source_root=Path(spec["source_root"]))


@pytest.mark.parametrize("finish", [False, True])
def test_pending_writer_before_first_mkdir_is_not_an_idle_step(tmp_path, finish):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    prepare_step(path, 0)
    assert not root.exists()
    writer = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", "--output", str(root)])
    try:
        with pytest.raises(ValueError, match="still running"):
            prepare_step(path, 0, finish=finish)
        assert not root.exists()
    finally:
        writer.terminate(); writer.wait(timeout=10)
