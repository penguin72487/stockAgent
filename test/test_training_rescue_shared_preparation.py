"""Shared CPU work owns its entire builder lifetime and re-verifies every reuse.

Real child processes and byte-exact tiny published fixtures exercise ownership,
interruption and no-overwrite behavior. This is not a full market/GPU benchmark.
"""
import base64
import copy
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import stockagent.training_rescue_worker as worker
from test_training_rescue_preparation import (
    controller_fixture, oracle_fixture, preserve_reference, source_binding,
)


@pytest.fixture
def shared(tmp_path, monkeypatch):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path)
    spec["prepared_build_policy"] = worker.PREPARED_BUILD_POLICY
    source = Path(spec["source_root"])
    builder = source / spec["rebuild_steps"][0][0]
    payload = {name: base64.b64encode(raw).decode() for name, raw in originals.items()}
    builder.write_text(
        "import argparse,base64,json,os,pathlib,sys,time\n"
        "source=pathlib.Path(__file__).parent.parent\n"
        "p=argparse.ArgumentParser();p.add_argument('--output');a=p.parse_args()\n"
        "calls=source/'calls';calls.write_text(str(int(calls.read_text())+1) if calls.exists() else '1')\n"
        "(source/'child.pid').write_text(str(os.getpid()))\n"
        "while (source/'pause').exists() and not (source/'release').exists():time.sleep(.02)\n"
        "root=pathlib.Path(a.output);root.mkdir(parents=True,exist_ok=True)\n"
        "if (source/'fail').exists():\n"
        " (root/'partial.bin').write_bytes(b'keep owned failure');sys.exit(3)\n"
        f"for name,encoded in {payload!r}.items():\n"
        " target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(base64.b64decode(encoded))\n"
        "(source/'child.finished').write_text('done')\n"
        "print('original fixed CPU fixture complete',flush=True)\n"
    )
    (source / "scripts/runtime_env.sh").write_text('exec_fintech_python() { exec "$PYTHON_BIN" "$@"; }\n')
    oracle["builder_sha256"] = worker.file_hash(builder)
    ref = spec["prepared_reconciliation"][0]
    worker.atomic_json(Path(ref["reference_path"]), oracle)
    ref["reference_sha256"] = worker.file_hash(Path(ref["reference_path"]))
    worker.atomic_json(path, spec)
    controls = tmp_path / "shared-build-control"
    monkeypatch.setattr(worker, "PREPARED_BUILD_ROOT", controls)
    return spec, path, root, source, controls, originals


def second_run(shared, tmp_path):
    spec = copy.deepcopy(shared[0])
    spec["run_id"] = "second-clock"
    spec["output_root"] = str(tmp_path / "artifacts/markets/second")
    spec["train_argv"][spec["train_argv"].index("--output-dir") + 1] = spec["output_root"]
    path = tmp_path / "second-controller/spec.json"
    worker.atomic_json(path, spec)
    return spec, path


def test_pinned_data_cwd_executes_the_frozen_builder_and_resolves_relative_sources(shared, tmp_path):
    spec, path, root, source, _, originals = shared
    working = tmp_path / "data workspace 'quoted'"
    working.mkdir()
    (working / "original.raw").write_bytes(b"exact original data")
    # A mutable lookalike in the data workspace must never be selected.
    (working / "scripts").mkdir()
    (working / spec["rebuild_steps"][0][0]).write_text("raise RuntimeError('wrong code root')\n")
    builder = source / spec["rebuild_steps"][0][0]
    builder.write_text(builder.read_text() +
        "assert pathlib.Path('original.raw').read_bytes()==b'exact original data'\n"
        "(source/'actual-cwd').write_text(str(pathlib.Path.cwd()))\n")
    ref = spec["prepared_reconciliation"][0]
    oracle = json.loads(Path(ref["reference_path"]).read_bytes())
    oracle["builder_sha256"] = worker.file_hash(builder)
    worker.atomic_json(Path(ref["reference_path"]), oracle)
    ref["reference_sha256"] = worker.file_hash(Path(ref["reference_path"]))
    spec["rebuild_working_directories"] = [str(working)]
    worker.atomic_json(path, spec)
    first = worker.prepare_run(path, 0)
    assert first["builder_executed"] and (source / "actual-cwd").read_text() == str(working)
    assert worker.prepared_build_identity(spec, 0)["working_directories"] == [str(working)]
    other, other_path = second_run(shared, tmp_path)
    assert worker.prepare_run(other_path, 0)["builder_executed"] is False
    assert (source / "calls").read_text() == "1"
    assert {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()} == originals


def test_explicit_default_cwd_preserves_legacy_artifact_identity(shared):
    spec, path, _, source, _, _ = shared
    before = worker.prepared_build_identity(spec, 0)
    worker.prepare_run(path, 0)
    spec["rebuild_working_directories"] = [str(source)]
    assert worker.prepared_build_identity(spec, 0) == before
    # A different run marker owns the same unchanged artifact.
    other_path = path.parent / "explicit-default/spec.json"
    worker.atomic_json(other_path, spec)
    assert worker.prepare_run(other_path, 0)["builder_executed"] is False


def test_only_later_step_cwd_change_preserves_completed_prefix_identity(shared, tmp_path):
    spec = copy.deepcopy(shared[0])
    later = tmp_path / "artifacts/data_preparation/later"
    step = [*spec["rebuild_steps"][0]];step[-1] = str(later)
    spec["rebuild_steps"].append(step)
    ref = dict(spec["prepared_reconciliation"][0], step_index=1, rebuilt_root=str(later))
    spec["prepared_reconciliation"].append(ref)
    before = [worker.prepared_build_identity(spec, i) for i in range(2)]
    spec["rebuild_working_directories"] = [spec["source_root"], str(tmp_path)]
    worker.validate_spec(spec)
    assert worker.prepared_build_identity(spec, 0) == before[0]
    assert worker.prepared_build_identity(spec, 1) != before[1]


@pytest.mark.parametrize("damage", ["missing", "file", "symlink", "relative", "root", "nul", "count", "string", "no_policy", "generated"])
def test_invalid_preparation_cwd_cannot_quarantine_or_start_a_builder(shared, tmp_path, damage):
    spec, path, root, source, controls, _ = shared
    working = tmp_path / "working"
    working.mkdir()
    if damage == "missing":
        working = tmp_path / "absent"
    elif damage == "file":
        working = tmp_path / "regular-file"; working.write_text("keep")
    elif damage == "symlink":
        link = tmp_path / "redirect"; link.symlink_to(working); working = link
    elif damage == "generated":
        working = root / "data"
    spec["rebuild_working_directories"] = [str(working)]
    if damage == "relative": spec["rebuild_working_directories"] = ["relative"]
    elif damage == "root": spec["rebuild_working_directories"] = ["/"]
    elif damage == "nul": spec["rebuild_working_directories"] = [str(working) + "\x00"]
    elif damage == "count": spec["rebuild_working_directories"] = []
    elif damage == "string": spec["rebuild_working_directories"] = str(working)
    elif damage == "no_policy": spec.pop("prepared_build_policy")
    worker.atomic_json(path, spec)
    with pytest.raises(ValueError):
        worker.prepare_run(path, 0)
    assert not root.exists() and not controls.exists()
    assert not (path.parent / "rebuild/0.json").exists() and not (source / "calls").exists()


def test_changed_cwd_cannot_reuse_an_old_complete_claim(shared, tmp_path):
    _, path, root, source, _, _ = shared
    worker.prepare_run(path, 0)
    other, other_path = second_run(shared, tmp_path)
    working = tmp_path / "other-data"; working.mkdir()
    other["rebuild_working_directories"] = [str(working)]
    worker.atomic_json(other_path, other)
    before = {p: worker.signature(p) for p in root.rglob("*")}
    with pytest.raises(ValueError, match="another input recipe"):
        worker.prepare_run(other_path, 0)
    assert before == {p: worker.signature(p) for p in before}
    assert (source / "calls").read_text() == "1"


def test_distinct_training_runs_share_exact_members_without_second_builder(shared, tmp_path):
    spec, path, root, source, controls, originals = shared
    first = worker.prepare_run(path, 0)
    before = {p: worker.signature(p) for p in root.rglob("*")}
    other, other_path = second_run(shared, tmp_path)
    assert worker.digest(spec) != worker.digest(other)
    assert worker.prepared_build_identity(spec, 0) == worker.prepared_build_identity(other, 0)
    again = worker.prepare_run(other_path, 0)
    assert first["builder_executed"] and not again["builder_executed"]
    assert again["fresh_full_original_readback"] and (source / "calls").read_text() == "1"
    assert {p: worker.signature(p) for p in root.rglob("*")} == before
    assert {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()} == originals
    claim = json.loads((controls / (worker.digest(str(root)) + ".json")).read_bytes())
    assert claim["state"] == "complete" and worker.file_hash(Path(claim["log_path"])) == claim["log_sha256"]
    assert worker.prepare_run(path, 0)["builder_executed"] is False


@pytest.mark.parametrize("damage", ["member", "recipe", "claim_symlink", "lock_symlink", "missing_root"])
def test_complete_shared_claim_is_not_cached_readiness_or_replacement_permission(shared, tmp_path, damage):
    spec, path, root, source, controls, originals = shared
    worker.prepare_run(path, 0)
    other, other_path = second_run(shared, tmp_path)
    data = root / "release/daily/continuous_daily.parquet"
    if damage == "member":
        data.write_bytes(b"preserve corrupted evidence")
    elif damage == "recipe":
        other["environment"] = {"POLARS_MAX_THREADS": "1"}
        worker.atomic_json(other_path, other)
    elif damage.endswith("symlink"):
        name = worker.digest(str(root)) + (".json" if damage == "claim_symlink" else ".lock")
        original = controls / name
        target = tmp_path / ("foreign-" + name)
        original.rename(target); original.symlink_to(target)
    else:
        root.rename(tmp_path / "detached-original")
        assert worker.prepare_run(other_path, 0)["builder_executed"]
        assert (source / "calls").read_text() == "2"
        return
    before = data.read_bytes()
    with pytest.raises((ValueError, OSError)):
        worker.prepare_run(other_path, 0)
    assert data.read_bytes() == before and (source / "calls").read_text() == "1"
    assert not list(root.parent.glob(root.name + ".rescue-incomplete-*"))


def test_unowned_tree_is_never_adopted_even_with_exact_published_bytes(shared):
    _, path, root, source, _, originals = shared
    for name, raw in originals.items():
        p = root / name;p.parent.mkdir(parents=True, exist_ok=True);p.write_bytes(raw)
    before = {p: worker.signature(p) for p in root.rglob("*")}
    with pytest.raises(ValueError, match="unowned"):
        worker.prepare_run(path, 0)
    assert before == {p: worker.signature(p) for p in root.rglob("*")}
    assert not (source / "calls").exists()


def legacy_prepared(shared, tmp_path):
    """A real prior begin/finish marker, kept separate from the new owner."""
    spec, _, root, _, _, originals = shared
    old = copy.deepcopy(spec); old.pop("prepared_build_policy")
    old["run_id"] = "original-cpu-job"
    prior = tmp_path / "prior-owner/spec.json"
    worker.atomic_json(prior, old)
    (prior.parent / "cpu-rebuild.lock").touch(mode=0o600)
    worker.prepare_step(prior, 0)
    for name, raw in originals.items():
        path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    worker.prepare_step(prior, 0, finish=True)
    marker = prior.parent / "rebuild/0.json"
    return prior, marker, worker.digest(old), worker.file_hash(marker)


def test_explicit_finished_legacy_transfer_reuses_outputs_and_normal_shared_path(shared, tmp_path):
    _, path, root, source, _, _ = shared
    prior, marker, spec_sha, marker_sha = legacy_prepared(shared, tmp_path)
    before = {p: worker.signature(p) for p in root.rglob("*")}
    old_controls = {p: worker.signature(p) for p in (prior, marker)}
    proof = worker.adopt_prepared_run(path, 0, prior_spec=prior,
        prior_spec_sha256=spec_sha, prior_marker_sha256=marker_sha)
    assert proof["builder_executed"] is False and proof["prepared_files_rewritten"] == 0
    assert proof["fresh_full_original_readback"] and proof["prior_owner"]["prior_marker_sha256"] == marker_sha
    assert before == {p: worker.signature(p) for p in root.rglob("*")}
    assert old_controls == {p: worker.signature(p) for p in old_controls}
    other, other_path = second_run(shared, tmp_path)
    assert worker.prepare_run(other_path, 0)["builder_executed"] is False
    assert not (source / "calls").exists()
    assert before == {p: worker.signature(p) for p in root.rglob("*")}
    assert worker.adopt_prepared_run(path, 0, prior_spec=prior,
        prior_spec_sha256=spec_sha, prior_marker_sha256=marker_sha)["reused_shared_claim"]


@pytest.mark.parametrize("damage", ["missing_cpu_lock", "missing_step_lock", "cpu_lock_busy", "step_lock_busy",
    "wrong_spec", "wrong_marker", "unfinished", "source_changed", "member_changed", "new_owner"])
def test_explicit_transfer_needs_original_finished_owner_and_same_full_inputs(shared, tmp_path, damage):
    spec, path, root, _, controls, _ = shared
    prior, marker, spec_sha, marker_sha = legacy_prepared(shared, tmp_path)
    busy = None
    if damage.startswith("missing_") or damage.endswith("lock_busy"):
        lock = prior.parent / "cpu-rebuild.lock" if "cpu" in damage else marker.with_suffix(".lock")
        if damage.startswith("missing_"):
            lock.unlink()
        else:
            busy = lock.open("a+b"); fcntl.flock(busy, fcntl.LOCK_EX | fcntl.LOCK_NB)
    elif damage == "wrong_spec":
        spec_sha = "f" * 64
    elif damage == "wrong_marker":
        marker_sha = "f" * 64
    elif damage == "unfinished":
        body = json.loads(marker.read_bytes()); body["state"] = "building"; worker.atomic_json(marker, body)
        marker_sha = worker.file_hash(marker)
    elif damage == "source_changed":
        spec["environment"] = {"POLARS_MAX_THREADS": "2"}; worker.atomic_json(path, spec)
    elif damage == "member_changed":
        (root / "release/daily/continuous_daily.parquet").write_bytes(b"preserve bad original")
    else:
        claim = controls / (worker.digest(str(root)) + ".json")
        worker.atomic_json(claim, {"state": "complete", "artifact_sha256": "f" * 64})
    before = {p: worker.signature(p) for p in root.rglob("*")}
    try:
        with pytest.raises((ValueError, OSError)):
            worker.adopt_prepared_run(path, 0, prior_spec=prior,
                prior_spec_sha256=spec_sha, prior_marker_sha256=marker_sha)
    finally:
        if busy is not None: busy.close()
    assert before == {p: worker.signature(p) for p in root.rglob("*")}
    assert not (path.parent / "rebuild/0.json").exists()


def test_legacy_lock_replacement_cannot_publish_a_shared_complete_claim(shared, tmp_path, monkeypatch):
    _, path, root, _, controls, _ = shared
    prior, marker, spec_sha, marker_sha = legacy_prepared(shared, tmp_path)
    original = worker._reconcile_prepared_step
    def replace(spec, index):
        result = original(spec, index)
        lock = prior.parent / "cpu-rebuild.lock"
        lock.rename(lock.with_suffix(".original-preserved")); lock.touch()
        return result
    monkeypatch.setattr(worker, "_reconcile_prepared_step", replace)
    with pytest.raises(ValueError, match="changed"):
        worker.adopt_prepared_run(path, 0, prior_spec=prior,
            prior_spec_sha256=spec_sha, prior_marker_sha256=marker_sha)
    assert not (controls / (worker.digest(str(root)) + ".json")).exists()


def test_owned_failure_preserves_log_and_quarantines_only_its_exact_tree(shared):
    spec, path, root, source, controls, _ = shared
    (source / "fail").touch()
    with pytest.raises(ValueError, match="builder failed"):
        worker.prepare_run(path, 0)
    claim = json.loads((controls / (worker.digest(str(root)) + ".json")).read_bytes())
    assert claim["state"] == "failed" and Path(claim["log_path"]).exists()
    failed_log = worker.file_hash(Path(claim["log_path"]))
    (source / "fail").unlink()
    assert worker.prepare_run(path, 0)["builder_executed"]
    quarantined = list(root.parent.glob(root.name + ".rescue-incomplete-*"))
    assert len(quarantined) == 1 and (quarantined[0] / "partial.bin").read_bytes() == b"keep owned failure"
    assert worker.file_hash(Path(claim["log_path"])) == failed_log


@pytest.mark.parametrize("nested", [False, True])
def test_builder_inherits_shared_locks_after_its_controller_dies(shared, tmp_path, nested):
    _, path, root, source, controls, _ = shared
    (source / "pause").touch()
    code = ("import pathlib,sys;import stockagent.training_rescue_worker as w;"
            "w.PREPARED_BUILD_ROOT=pathlib.Path(sys.argv[2]);w.prepare_run(pathlib.Path(sys.argv[1]),0)")
    process = subprocess.Popen([sys.executable, "-c", code, str(path), str(controls)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    child = None
    try:
        deadline = time.monotonic() + 10
        while not (source / "child.pid").exists() and time.monotonic() < deadline:
            time.sleep(.02)
        child = int((source / "child.pid").read_text())
        process.kill();process.wait(timeout=10)
        other, other_path = second_run(shared, tmp_path)
        if nested:
            other["rebuild_steps"][0][-1] = str(root / "nested")
            other["prepared_reconciliation"][0]["rebuilt_root"] = str(root / "nested")
            worker.atomic_json(other_path, other)
        # Even an absent output directory cannot admit another writer.
        assert not root.exists()
        with pytest.raises(BlockingIOError):
            worker.prepare_run(other_path, 0)
        assert (source / "calls").read_text() == "1"
        (source / "release").touch()
        while not (source / "child.finished").exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert (source / "child.finished").exists()
        # No complete claim was written by the dead controller. Preserve the
        # interrupted output instead of silently declaring that run complete.
        if not nested:
            deadline = time.monotonic() + 5
            while True:
                try:
                    assert worker.prepare_run(other_path, 0)["builder_executed"]
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:raise
                    time.sleep(.02)
            assert len(list(root.parent.glob(root.name + ".rescue-incomplete-*"))) == 1
    finally:
        (source / "release").touch()
        if process.poll() is None:
            process.kill();process.wait(timeout=10)
        if child is not None:
            try:
                os.kill(child, signal.SIGTERM)
            except ProcessLookupError:
                pass
        process.stderr.close()


def test_shared_policy_cannot_use_split_begin_finish_or_legacy_defaults(shared):
    spec, path, _, source, _, _ = shared
    for finish in (False, True):
        with pytest.raises(ValueError, match="atomic prepare-run"):
            worker.prepare_step(path, 0, finish=finish)
    spec.pop("prepared_build_policy");worker.atomic_json(path, spec)
    with pytest.raises(ValueError, match="explicitly enrolled"):
        worker.prepare_run(path, 0)
    spec["prepared_build_policy"] = "invented-policy"
    with pytest.raises(ValueError, match="ownership policy"):
        worker.validate_spec(spec)
    assert not (source / "calls").exists()


def test_timeout_stops_only_the_owned_builder_and_keeps_its_failed_claim(shared, monkeypatch):
    _, path, root, source, controls, _ = shared
    (source / "pause").touch()
    original_popen = worker.subprocess.Popen
    def started_builder(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            pid_file = source / "child.pid"
            if pid_file.exists() and pid_file.read_text().strip() == str(process.pid):
                return process
            if process.poll() is not None:
                pytest.fail("fixture builder exited before its timeout barrier")
            time.sleep(.01)
        process.kill(); process.wait(timeout=10)
        pytest.fail("fixture builder did not reach its owned pause barrier")
    # Timeout behavior starts with a witnessed child, independent of the host's
    # Python startup latency. The production timeout and owned PGID are unchanged.
    monkeypatch.setattr(worker.subprocess, "Popen", started_builder)
    with pytest.raises(subprocess.TimeoutExpired):
        worker.prepare_run(path, 0, build_timeout=.25)
    pid = int((source / "child.pid").read_text())
    assert not worker._group_members(pid)
    claim = json.loads((controls / (worker.digest(str(root)) + ".json")).read_bytes())
    assert claim["state"] == "failed" and not root.exists()
    (source / "release").touch()
    assert worker.prepare_run(path, 0)["builder_executed"]


def test_replaced_global_lock_cannot_publish_a_complete_claim(shared, monkeypatch):
    _, path, root, _, controls, _ = shared
    original = worker._reconcile_prepared_step
    def replace_after_full_readback(spec, index):
        proof = original(spec, index)
        lock = controls / (worker.digest(str(root)) + ".lock")
        lock.rename(lock.with_suffix(".preserved-original"));lock.touch()
        return proof
    monkeypatch.setattr(worker, "_reconcile_prepared_step", replace_after_full_readback)
    with pytest.raises(ValueError, match="received file changed|lock inode"):
        worker.prepare_run(path, 0)
    claim = json.loads((controls / (worker.digest(str(root)) + ".json")).read_bytes())
    assert claim["state"] == "building"
    assert json.loads((path.parent / "rebuild/0.json").read_bytes())["state"] == "building"


@pytest.mark.parametrize("damage", [None, "member", "json_type", "original_sha", "original_changed"])
def test_factorized_original_reuse_is_full_readback_and_preserves_its_inode(tmp_path, monkeypatch, damage):
    import stockagent.data.factorized_panel as factorized
    root = tmp_path / "rebuilt";root.mkdir()
    member = root / "block.bin";member.write_bytes(b"exact immutable original values")
    manifest = {"common": {"path": member.name, "sha256": worker.file_hash(member)},
                "blocks": [], "raw_values": True, "build_wall_seconds": 5.0}
    reference = tmp_path / "original.json";worker.atomic_json(reference, manifest)
    rebuilt = root / "manifest.json";rebuilt.write_bytes(reference.read_bytes())
    expected_sha = worker.file_hash(reference)
    if damage == "member":member.write_bytes(b"wrong values")
    elif damage == "json_type":
        manifest["raw_values"] = 1;worker.atomic_json(rebuilt, manifest)
    elif damage == "original_sha":expected_sha = "0" * 64
    elif damage == "original_changed":
        verify = factorized.verify_factorized_members
        def changed(*args, **kwargs):
            verify(*args, **kwargs);reference.write_bytes(reference.read_bytes() + b" ")
        monkeypatch.setattr(factorized, "verify_factorized_members", changed)
    before = worker.signature(rebuilt)
    if damage is None:
        for _ in range(2):
            proof = worker.reconcile_prepared(reference, rebuilt, source_root=Path(__file__).resolve().parents[1],
                                              expected_original_sha=expected_sha)
            assert proof["sha256"] == expected_sha
    else:
        with pytest.raises(ValueError):
            worker.reconcile_prepared(reference, rebuilt, source_root=Path(__file__).resolve().parents[1],
                                      expected_original_sha=expected_sha)
    assert worker.signature(rebuilt) == before
    assert not (root / "training-rescue-rebuild-receipt.json").exists()


@pytest.mark.parametrize("damage", [None, "artifact", "spec", "index", "readback"])
def test_controller_uses_one_atomic_adapter_and_validates_its_exact_proof(shared, tmp_path, monkeypatch, damage):
    import stockagent.data_sync.packed_snapshots as packed
    from stockagent.training_rescue import RescueError
    spec = shared[0];source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    preserve_reference(owner, spec)
    manifest = Path("/srv/stockagent-packed/manifests/fixture.json")
    class ManifestPath(type(manifest)):
        def stat(self):return SimpleNamespace(st_size=2)
    resolved = SimpleNamespace(manifest_sha256="a" * 64, manifest_path=ManifestPath(manifest),
        manifest={"archive": {"objects": [], "inventory": {"relpath": "inventory", "sha256": "b" * 64, "bytes": 1}}})
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", lambda *a: resolved)
    monkeypatch.setattr(owner, "push_object", lambda *a: None)
    monkeypatch.setattr(owner, "push_cold_source", lambda *a: None)
    events = []
    class Target:
        def upload(self, path, data):
            self.sha = __import__("hashlib").sha256(data).hexdigest()
            return {"sha256": self.sha, "bytes": len(data)}
        def adapter(self, action, *args, **kwargs):
            events.append(action)
            if action == "prepare-run":
                proof = {"spec_sha256": worker.digest(spec), "step_index": 0, "skip_builder": True,
                    "fresh_full_original_readback": True, "artifact_sha256": worker.digest(worker.prepared_build_identity(spec, 0))}
                if damage == "artifact":proof["artifact_sha256"] = "0" * 64
                elif damage == "spec":proof["spec_sha256"] = "0" * 64
                elif damage == "index":proof["step_index"] = False
                elif damage == "readback":proof["fresh_full_original_readback"] = False
                return proof
            return {"spec_sha256": worker.digest(spec), "preflight_only": action == "input-preflight",
                "manifest_sha256": "a" * 64, "cache_controller_sha256": self.sha, "source_index": 0,
                "dataset": "originals", "snapshot_id": "fixed", "original_source_paths_ready": True}
        def run(self, *a, **kw):raise AssertionError("split unowned builder launch is forbidden")
    if damage is None:
        owner.restore_inputs(Target(), spec, tmp_path / "incoming")
    else:
        with pytest.raises(RescueError, match="build identity/readback"):
            owner.restore_inputs(Target(), spec, tmp_path / "incoming")
    assert events == ["input-preflight", "materialize-input", "prepare-run"]
