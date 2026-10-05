from __future__ import annotations

import json
import os
from pathlib import Path
import time
from types import SimpleNamespace

import pytest
import stockagent.data_sync.remote_legacy_return as returns
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, prepare_archive


def test_persistent_handoff_selects_only_the_exact_retained_cohort():
    from scripts.handoff_remote_legacy_archive_worker import legacy_systemd_owner
    assert legacy_systemd_owner(Path("/var/lib/stockagent-vast-legacy-return")) == "stockagent-legacy-return@main.service"
    assert legacy_systemd_owner(Path("/var/lib/stockagent-vast-legacy-return-partitions")) == "stockagent-legacy-return@partitions.service"
    with pytest.raises(SnapshotError):
        legacy_systemd_owner(Path("/var/lib/unregistered"))


def test_worker_handoff_requires_an_unowned_childless_boundary():
    from scripts.handoff_remote_legacy_archive_worker import safe_boundary
    safe = {"state": "S", "wchan": "locks_lock_inode_wait", "children": [],
            "common_fd_present": True, "holds_common_lock": False}
    assert safe_boundary(safe)
    assert safe_boundary({**safe, "state": "T"}, stopped=True)
    assert not safe_boundary(None)
    for change in ({"holds_common_lock": True}, {"children": [123]},
                   {"common_fd_present": False}, {"wchan": "p9_client_rpc"},
                   {"observation_complete": False}):
        assert not safe_boundary({**safe, **change})
    assert not safe_boundary(safe, stopped=True)


def test_handoff_transient_proc_fd_race_is_incomplete_not_worker_exit(tmp_path, monkeypatch):
    from scripts import handoff_remote_legacy_archive_worker as handoff
    proc = tmp_path / "42"
    (proc / "task/42").mkdir(parents=True)
    (proc / "fd").mkdir()
    (proc / "fdinfo").mkdir()
    (proc / "stat").write_text("42 (worker) S " + " ".join(["0"] * 18 + ["123"]))
    (proc / "cmdline").write_bytes(b"python\0scripts/return_remote_legacy_archives.py\0")
    (proc / "task/42/children").write_text("")
    (proc / "wchan").write_text("locks_lock_inode_wait")
    common = tmp_path / "ingress.lock"
    common.touch()
    (proc / "fd/7").symlink_to(common)
    monkeypatch.setattr(handoff, "COMMON", common)
    monkeypatch.setattr(handoff, "Path", lambda value: tmp_path if value == "/proc" else Path(value))
    incomplete = handoff.observation(42, 123, Path("/var/lib/stockagent-vast-legacy-return"))
    assert incomplete == {"observation_complete": False}
    assert not handoff.safe_boundary(incomplete)
    (proc / "fdinfo/7").write_text("pos:\t0\n")
    assert handoff.safe_boundary(handoff.observation(42, 123, Path("/var/lib/stockagent-vast-legacy-return")))


@pytest.fixture
def example(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    source = root / "ablations/example"
    source.mkdir(parents=True)
    (source / "unique.bin").write_bytes(b"original result")
    (source / "empty").mkdir()
    old = time.time() - 8 * 86400
    os.utime(source / "unique.bin", (old, old))
    relative = "ablations/example"
    dataset = returns.dataset_name(relative)
    spec = LegacyArchiveSpec(dataset, relative, 7, tmp_path / "stage")
    archive = prepare_archive(spec, root)
    metadata = {"transport_role": "legacy-quarantine-archive", "deployable": "false",
                "source_relative_root": relative, "legacy_manifest_sha256": "a" * 64}
    resolved = SimpleNamespace(manifest_sha256="b" * 64, manifest={"metadata": metadata})
    monkeypatch.setattr(returns, "resolve_packed_snapshot_id", lambda *a, **kw: resolved)
    monkeypatch.setattr(returns, "_load_inventory", lambda *a: [])
    monkeypatch.setattr(returns, "_validate_inventory", lambda *a: None)
    monkeypatch.setattr(returns, "_pinned_snapshot_ids", lambda *a: set())
    monkeypatch.setattr(returns, "artifact_process_references", lambda *a: [])
    monkeypatch.setattr(returns, "active_configuration_references", lambda *a: [])
    monkeypatch.setattr(returns, "process_references", lambda *a: [])
    monkeypatch.setattr(returns, "artifact_service_references", lambda sources, *a: {str(s): [] for s in sources})
    import scripts.configure_artifact_ingress_syncthing as config
    import scripts.manage_packed_edge as edge
    monkeypatch.setattr(config, "credentials", lambda: ("http://local", "not-real-secret"))
    monkeypatch.setattr(edge, "_convergence", lambda *a: {"ok": True})
    ack = {"contract": "d_verified_remote_legacy_return_v1", "authority_node_id": "penguin",
           "origin_node_id": "vastai1T", "relative_root": relative, "dataset": dataset,
           "snapshot_id": "exact-release", "manifest_sha256": "b" * 64,
           "legacy_manifest_sha256": "a" * 64, "cold_verified": True,
           "verified_at_epoch": time.time(), "archive_manifest": archive}
    ack["identity_sha256"] = returns.identity(ack)
    return root, source, tmp_path / "state", ack


def test_exact_d_return_retirement_retains_unique_other_data(example, tmp_path):
    root, source, state, ack = example
    unrelated = root / "unrelated"
    unrelated.mkdir()
    (unrelated / "keep").write_bytes(b"different source")
    result = returns.retire(root, tmp_path, state, ack, apply=True)
    assert result["deleted"] and not source.exists() and result["cold_deleted"] is False
    assert (unrelated / "keep").read_bytes() == b"different source"


def test_conflicted_configuration_never_permits_retirement(example, tmp_path, monkeypatch):
    root, source, state, ack = example
    def conflicted(*args):
        raise SyntaxError("unresolved merge")
    monkeypatch.setattr(returns, "artifact_service_references", conflicted)
    result = returns.retire(root, tmp_path, state, ack, apply=True)
    assert result["deleted"] is False and source.exists()
    assert "consumer-configuration-unreadable" in result["blockers"]


@pytest.mark.parametrize("change", ["same-size-bytes", "unknown-file", "empty-directory", "mtime", "mode"])
def test_changed_original_cannot_be_deleted(example, tmp_path, change):
    root, source, state, ack = example
    file = source / "unique.bin"
    if change == "same-size-bytes":
        info = file.stat()
        file.write_bytes(b"X" * info.st_size)
        os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif change == "unknown-file":
        (source / "new").write_bytes(b"unique")
    elif change == "empty-directory":
        (source / "empty").rmdir()
    elif change == "mtime":
        os.utime(file, ns=(file.stat().st_atime_ns, file.stat().st_mtime_ns - 1))
    else:
        file.chmod(0o600)
    with pytest.raises(SnapshotError):
        returns.retire(root, tmp_path, state, ack, apply=True)
    assert source.exists()


def test_shared_inode_is_not_automatically_unlinked(example, tmp_path):
    root, source, state, ack = example
    os.link(source / "unique.bin", tmp_path / "alias")
    result = returns.retire(root, tmp_path, state, ack, apply=True)
    assert result["deleted"] is False and source.exists()
    assert "shared-inode-requires-separate-audit" in result["blockers"]


def test_stale_or_forged_ack_is_rejected(example, tmp_path):
    root, source, state, ack = example
    ack["verified_at_epoch"] = time.time() - 301
    ack["identity_sha256"] = returns.identity({k: v for k, v in ack.items() if k != "identity_sha256"})
    with pytest.raises(SnapshotError, match="stale"):
        returns.retire(root, tmp_path, state, ack, apply=True)
    assert source.exists()


def test_transport_race_after_rename_preserves_quarantine(example, tmp_path, monkeypatch):
    root, source, state, ack = example
    import scripts.manage_packed_edge as edge
    calls = 0
    def convergence(*args):
        nonlocal calls
        calls += 1
        return {"ok": calls < 3}
    monkeypatch.setattr(edge, "_convergence", convergence)
    with pytest.raises(SnapshotError, match="retained"):
        returns.retire(root, tmp_path, state, ack, apply=True)
    assert not source.exists()
    assert next(state.glob("quarantine-*"), None) is not None
    assert next(state.glob("quarantine-*")) .joinpath("unique.bin").read_bytes() == b"original result"


def test_complete_inventory_includes_non_directory_and_hidden_files(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    (root / "markets/model").mkdir(parents=True)
    (root / "ablations").mkdir()
    (root / "markets/model/.receipt").write_bytes(b"proof")
    (root / "markets/summary.csv").write_bytes(b"a,b")
    monkeypatch.setattr(returns, "artifact_process_references", lambda *a: [])
    result = returns.inventory_scopes(root)
    assert len(result["items"]) == 2
    model = next(r for r in result["items"] if r["relative_root"] == "markets/model")
    assert model["files"] == 1 and model["rows"][0]["path"] == ".receipt"
    assert next(r for r in result["items"] if r["relative_root"] == "markets/summary.csv")["state"] == "non-directory-protected"


def test_active_config_blocks_source_with_no_open_fd(example, tmp_path, monkeypatch):
    root, source, state, ack = example
    monkeypatch.setattr(returns, "active_configuration_references", lambda *a: ["pid=42:active-config:data"])
    result = returns.retire(root, tmp_path, state, ack, apply=True)
    assert not result["deleted"] and source.exists()
    assert "active-process-reference" in result["blockers"]


def test_partition_retains_parent_files_and_exact_child_inventory(tmp_path, monkeypatch, capsys):
    from scripts import return_remote_legacy_archives as cli
    root = tmp_path / "artifacts"
    child = root / "markets/large/child"
    child.mkdir(parents=True)
    (root / "ablations").mkdir()
    (child / "old-result").write_bytes(b"original")
    (child.parent / "unique-parent-receipt.json").write_text('{"keep":true}')
    monkeypatch.setattr(returns, "artifact_process_references", lambda *a: [])
    retained = returns.inventory_scopes(root)
    path = tmp_path / "full.json"
    path.write_text(json.dumps(retained))
    state = tmp_path / "partitioned"
    monkeypatch.setattr(cli.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *a: None)
    monkeypatch.setattr(cli.sys, "argv", ["return", "partition", "--state-root", str(state),
                                         "--from-inventory", str(path), "--partition-root", "markets/large"])
    previous_umask = os.umask(0o022)
    try:
        assert cli.main() == 0
    finally:
        os.umask(previous_umask)
    result = json.loads((state / "inventory.json").read_text())
    selected = next(r for r in result["items"] if r["relative_root"] == "markets/large/child")
    assert selected["fingerprint"] == returns.metadata_tree(child)["fingerprint"]
    assert selected["files"] == 1
    parent = next(r for r in result["items"] if r["relative_root"].endswith("unique-parent-receipt.json"))
    assert parent["state"] == "non-directory-protected"
    assert (child.parent / "unique-parent-receipt.json").exists()
    assert result["all_scopes_inventoried"] is False
    capsys.readouterr()


def test_reclaim_priority_uses_allocated_bytes_and_keeps_shared_roots_last():
    from scripts import return_remote_legacy_archives as cli
    rows = [
        {"relative_root": "markets/shared", "logical_bytes": 1000, "reclaimable_allocated_file_bytes": 1000, "files": 1},
        {"relative_root": "markets/sparse", "logical_bytes": 900, "reclaimable_allocated_file_bytes": 10, "files": 1},
        {"relative_root": "markets/allocated", "logical_bytes": 2000, "reclaimable_allocated_file_bytes": 200, "files": 1},
    ]
    original = {r["relative_root"]: {"rows": [{"kind": "file", "signature": [0, 0, 0, 0, 0, 0, 2 if r["relative_root"].endswith("shared") else 1]}]} for r in rows}
    ordered = cli.ordered_items(rows, "reclaim-first", original)
    assert [r["relative_root"] for r in ordered] == ["markets/allocated", "markets/sparse", "markets/shared"]
    assert [r["relative_root"] for r in cli.ordered_items(rows, "smallest", original)] == ["markets/sparse", "markets/shared", "markets/allocated"]


def test_retained_cohort_rejects_a_second_progress_owner_and_releases_on_error(tmp_path):
    from scripts import return_remote_legacy_archives as cli
    with pytest.raises(ValueError):
        with cli.cohort_owner(tmp_path):
            with pytest.raises(SnapshotError, match="already has an owner"):
                with cli.cohort_owner(tmp_path):
                    pytest.fail("second owner was admitted")
            raise ValueError("simulate failed owner")
    with cli.cohort_owner(tmp_path):
        pass


def test_failed_phase_still_records_elapsed_time(tmp_path):
    from scripts import return_remote_legacy_archives as cli
    row = {"relative_root": "ablations/example"}
    spec = LegacyArchiveSpec("legacy-example", "ablations/example", 7, tmp_path / "encoded")
    with pytest.raises(ValueError):
        with cli.phase(SimpleNamespace(state_root=tmp_path), row, spec, "failed-phase"):
            raise ValueError("simulate scan timeout")
    recorded = json.loads((tmp_path / "item-status/legacy-example.json").read_text())
    assert recorded["state"] == "failed-phase" and recorded["phase_seconds"]["failed-phase"] >= 0


def test_verified_but_unretired_cohort_item_is_resumed(tmp_path, monkeypatch):
    from scripts import return_remote_legacy_archives as cli
    row = {"relative_root": "ablations/waiting", "files": 1, "logical_bytes": 10,
           "newest_mtime_ns": 1, "fingerprint": "original", "rows": []}
    (tmp_path / "inventory.json").write_text(json.dumps({"items": [row]}))
    prior = {k: v for k, v in row.items() if k != "rows"}
    prior.update(state="cold-verified-needs-audit", cold_verified=True, error="old failed transport audit")
    (tmp_path / "progress.json").write_text(json.dumps({"state": "old", "items": [prior]}))
    spec = LegacyArchiveSpec(cli.dataset_name(row["relative_root"]), row["relative_root"], 7, tmp_path / "encoded")
    monkeypatch.setattr(cli, "load_legacy_specs", lambda *a: {spec.dataset: spec})
    monkeypatch.setattr(cli, "_check_d_primary_mount", lambda *a: None)
    monkeypatch.setattr(cli.fcntl, "flock", lambda *a: None)
    seen = []
    def complete(args, policy, item, selected):
        seen.append(item["relative_root"])
        item["state"] = "remote-source-retired"
    monkeypatch.setattr(cli, "archive_one", complete)
    args = SimpleNamespace(state_root=tmp_path, sync_root=tmp_path / "packed", apply=True,
                           reuse_progress=None, include_root=[], max_items=None, order="reclaim-first", command="archive")
    policy = {"minimum_stable_hours": 12, "maximum_item_bytes": 100}
    assert cli.run_cohort(args, policy) == 0
    assert seen == [row["relative_root"]]
    result = json.loads((tmp_path / "progress.json").read_text())
    assert result["items"][0]["state"] == "remote-source-retired"
    # This mock replaces the entire staging/owned-publication operation. Its
    # lock phase is exercised by the real operation's concurrency tests.
    assert result["items"][0]["complete_workflow_seconds"] >= 0
    attempts = [json.loads(path.read_text()) for path in (tmp_path / "attempts").glob("*.json")]
    assert len(attempts) == 2
    assert any(record.get("error") == "old failed transport audit" for record in attempts)
    assert any(record["state"] == "remote-source-retired" and "error" not in record for record in attempts)
