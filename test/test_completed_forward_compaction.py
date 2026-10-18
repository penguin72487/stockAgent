from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
from contextlib import ExitStack

import pytest
import torch

from stockagent.data_sync import completed_forward_compaction as compaction
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.live import completed_inference_cache as cache


def _cli_policy(repo, roots=None, **overrides):
    policy = repo / "forward-policy.json"
    policy.write_text(json.dumps({
        "schema_version": 1, "contract": compaction.CONTRACT,
        "cache_roots": roots or ["artifacts/cache/completed_live_inference"], **overrides,
    }))
    return policy


def test_forward_cli_uses_native_owner_and_candidate_only_is_explicit(tmp_path, monkeypatch):
    from scripts import deduplicate_artifacts as cli
    root = tmp_path / "artifacts/cache/completed_live_inference"
    root.mkdir(parents=True)
    policy, calls = _cli_policy(tmp_path), []
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    def compact(path, receipt_dir, **kwargs):
        calls.append((path, receipt_dir, kwargs))
        return {"state": "compacted"}
    monkeypatch.setattr(compaction, "compact_completed_forward_cache", compact)
    for candidate_only in (False, True):
        monkeypatch.setattr("sys.argv", ["deduplicate_artifacts.py",
                            "--completed-forward-cache-policy", str(policy),
                            "--report-dir", str(tmp_path / "reports"), "--apply",
                            *(["--sharing-candidates-only"] if candidate_only else [])])
        assert cli.main() == 0
        path, receipts, options = calls[-1]
        assert path == root
        assert receipts == tmp_path / "reports/completed-forward/completed_live_inference"
        assert options["apply"] and options["full_audit"] is not candidate_only
        assert callable(options["consumer_pin_probe"])
    assert len(calls) == 2


def test_forward_cli_cannot_select_another_service_or_generic_artifact_root(tmp_path, monkeypatch):
    from scripts import deduplicate_artifacts as cli
    (tmp_path / "artifacts/cache/other-cache").mkdir(parents=True)
    policy = _cli_policy(tmp_path, ["artifacts/cache/other-cache"])
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    monkeypatch.setattr("sys.argv", ["deduplicate_artifacts.py",
                        "--completed-forward-cache-policy", str(policy), "--apply"])
    with pytest.raises(ValueError, match="only the canonical"):
        cli.main()


@pytest.mark.parametrize("flag", ["--sharing-candidates-only", "--expected-plan-fingerprint"])
def test_service_compaction_flags_cannot_enable_generic_artifact_mutation(monkeypatch, flag):
    from scripts import deduplicate_artifacts as cli
    monkeypatch.setattr("sys.argv", ["deduplicate_artifacts.py", flag,
                        *(["a" * 64] if flag == "--expected-plan-fingerprint" else []), "--apply"])
    monkeypatch.setattr(cli, "find_duplicate_groups", lambda *a, **kw: pytest.fail("generic dedup"))
    with pytest.raises(ValueError, match="explicit service-cache policy"):
        cli.main()


def test_service_policy_types_are_mutually_exclusive():
    from scripts import deduplicate_artifacts as cli
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["--service-cache-policy", "a.json",
                                       "--completed-forward-cache-policy", "b.json"])


@pytest.mark.parametrize("defect", ["contract", "writable", "redirected"])
def test_forward_policy_rejects_wrong_contract_or_untrusted_local_file(tmp_path, defect):
    from stockagent.data_sync.physical_cache_compaction import policy_roots
    (tmp_path / "artifacts/cache/completed_live_inference").mkdir(parents=True)
    path = _cli_policy(tmp_path, **({"contract": "unknown"} if defect == "contract" else {}))
    if defect == "writable":
        path.chmod(0o666)
    elif defect == "redirected":
        alias = tmp_path / "alias.json"
        alias.symlink_to(path)
        path = alias
    with pytest.raises(ValueError):
        policy_roots(path, tmp_path, contract=compaction.CONTRACT)


def test_forward_policy_is_opt_in_under_the_existing_daily_maintenance_lock():
    repo = Path(__file__).parents[1]
    policy = json.loads((repo / "configs/data_sync/completed_forward_compaction.json").read_bytes())
    assert policy == {"schema_version": 1, "contract": compaction.CONTRACT,
                      "cache_roots": ["artifacts/cache/completed_live_inference"]}
    wrapper = (repo / "scripts/run_artifact_dedup.sh").read_text()
    assert 'exec 9>/run/lock/stockagent-artifact-dedup.lock' in wrapper
    assert "/etc/stockagent/completed-forward-compaction.json" in wrapper
    assert '--completed-forward-cache-policy "$completed_forward_cache_policy" --sharing-candidates-only --apply' in wrapper


def _entry(root, label, value=7):
    identity = {"input": label}
    key = cache.identity_digest(identity)
    _, receipt = cache.cached_forward(root, identity, lambda: {
        "weights": torch.tensor([0., -0., float("nan"), float(value)], dtype=torch.float64)})
    path = root / f"{key}.forward"
    # Model the old writer's separate, writable payloads, even though the new
    # producer would already share readonly same-byte results.
    raw = path.read_bytes()
    path.unlink()
    path.write_bytes(raw)
    path.chmod(0o600)
    return key, path, receipt


def _run(root, receipts, **options):
    return compaction.compact_completed_forward_cache(
        root, receipts, reference_probe=options.pop("reference_probe", lambda *a, **kw: []), **options)


def _bytes(root):
    return {p.name: p.read_bytes() for p in root.iterdir() if p.suffix in {".forward", ".json"}}


def test_audit_apply_preserves_all_keys_receipts_tensor_bits_and_is_idempotent(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    a, ap, _ = _entry(root, "a")
    b, bp, _ = _entry(root, "b")
    before = _bytes(root)
    audit = _run(root, receipts)
    assert audit["state"] == "audited" and audit["duplicate_groups"] == 1
    assert not os.path.samefile(ap, bp) and ap.stat().st_mode & 0o222
    result = _run(root, receipts, apply=True, expected_fingerprint=audit["plan_fingerprint"])
    assert result["state"] == "compacted" and result["replaced_paths"] == 1
    assert result["retired_allocated_bytes"] == result["candidate_allocated_bytes"] > 0
    assert result["logical_paths_removed"] == result["cold_objects_removed"] == 0
    assert _bytes(root) == before and os.path.samefile(ap, bp)
    for key, label in ((a, "a"), (b, "b")):
        loaded, hit = cache.cached_forward(root, {"input": label}, lambda: pytest.fail("model"))
        assert hit["key"] == key and hit["state"] == "disk_hit"
        assert loaded["weights"].view(torch.int64).tolist()[:2] == [0, -(2**63)]
        loaded["weights"].fill_(1)
        assert _bytes(root) == before
    again = _run(root, receipts, apply=True)
    assert again["state"] == "compacted" and again["duplicate_groups"] == 0
    assert again["retired_allocated_bytes"] == 0


@pytest.mark.parametrize("apply", [False, True])
def test_incomplete_process_scan_retains_payloads_and_records_failure(tmp_path, apply):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _, first, _ = _entry(root, "a")
    _, second, _ = _entry(root, "b")
    before = _bytes(root)
    inodes = first.stat().st_ino, second.stat().st_ino

    def incomplete(*args, **kwargs):
        raise SnapshotError("incomplete process-reference scan: injected permission error")

    result = _run(root, receipts, apply=apply, reference_probe=incomplete)
    assert result["state"] == "failed" and result["error_type"] == "SnapshotError"
    assert "incomplete process-reference scan" in result["error"]
    assert result["replaced_paths"] == result["logical_paths_removed"] == 0
    assert _bytes(root) == before
    assert (first.stat().st_ino, second.stat().st_ino) == inodes
    assert json.loads((receipts / "latest.json").read_bytes())["state"] == "failed"


def test_capacity_plan_cannot_report_a_clean_scope_when_process_scan_is_incomplete(tmp_path):
    root = tmp_path / "cache"
    _entry(root, "a")
    before = _bytes(root)

    def incomplete(*args, **kwargs):
        raise SnapshotError("incomplete process-reference scan: injected permission error")

    with pytest.raises(SnapshotError, match="incomplete process-reference scan"):
        compaction.plan_completed_forward_working_set(root, reference_probe=incomplete)
    assert _bytes(root) == before


def test_native_changed_key_commit_reuses_newly_sealed_legacy_payload(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _, ap, _ = _entry(root, "a")
    _entry(root, "b")
    assert _run(root, receipts, apply=True)["state"] == "compacted"
    _, result = cache.cached_forward(root, {"input": "c"}, lambda: {
        "weights": torch.tensor([0., -0., float("nan"), 7.], dtype=torch.float64)})
    assert result["storage_build"]["reused_payload"]
    assert os.path.samefile(ap, root / f"{result['key']}.forward")


@pytest.mark.parametrize("defect", ["missing_receipt", "identity", "unsupported", "symlink", "mode", "duplicate_json"])
def test_unverified_or_unknown_keys_are_preserved_not_repaired_or_deleted(tmp_path, defect):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a")
    _, bp, _ = _entry(root, "b")
    receipt_path = bp.with_suffix(".receipt.json")
    if defect == "missing_receipt":
        receipt_path.unlink()
    elif defect == "symlink":
        outside = tmp_path / "outside"
        bp.rename(outside)
        bp.symlink_to(outside)
    elif defect == "mode":
        bp.chmod(0o644)
    elif defect == "duplicate_json":
        receipt_path.write_text('{"key":"a","key":"b"}')
    else:
        record = json.loads(receipt_path.read_bytes())
        record["identity" if defect == "identity" else "contract"] = {}
        receipt_path.write_text(json.dumps(record))
    before = _bytes(root)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "compacted" and result["replaced_paths"] == 0
    assert result["retained_keys"][0]["reason"] == "unverified_commit"
    assert _bytes(root) == before


def test_corrupt_committed_payload_rejects_before_any_changes(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _, ap, _ = _entry(root, "a")
    _entry(root, "b")
    raw = bytearray(ap.read_bytes())
    raw[len(raw) // 2] ^= 1
    ap.write_bytes(raw)
    before = _bytes(root)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "failed" and "SHA mismatch" in result["error"]
    assert result["replaced_paths"] == 0 and _bytes(root) == before


def test_external_alias_is_not_sealed_or_replaced(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _, ap, _ = _entry(root, "a")
    _entry(root, "b")
    external = tmp_path / "external"
    os.link(ap, external)
    result = _run(root, receipts, apply=True)
    assert result["external_link_inodes_retained"] == 1 and result["replaced_paths"] == 0
    assert external.stat().st_mode & 0o777 == 0o600


def test_busy_writer_is_retained_without_blocking_other_keys(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    a, _, _ = _entry(root, "a")
    _entry(root, "b")
    with (root / f"{a}.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run(root, receipts, apply=True)
    assert result["state"] == "compacted" and result["replaced_paths"] == 0
    assert result["retained_keys"][0]["reason"] == "writer_busy"


@pytest.mark.parametrize("phase", ["before", "retired"])
def test_process_and_retired_mmap_references_are_not_counted_as_reclaimed(tmp_path, phase):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b")
    def probe(targets, **kwargs):
        return ["pid=123:maps:forward"] if bool(targets) == (phase == "before") else []
    result = _run(root, receipts, apply=True, reference_probe=probe)
    assert result["retired_allocated_bytes"] == 0
    if phase == "before":
        assert result["state"] == "partially_compacted" and result["replaced_paths"] == 0
    else:
        assert result["state"] == "compacted" and result["replaced_paths"] == 1
        assert result["retained_open_inode_references"]


def test_dry_run_fingerprint_and_interruption_do_not_lose_payloads(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b")
    before = _bytes(root)
    failed = _run(root, receipts, apply=True, expected_fingerprint="0" * 64)
    assert failed["state"] == "failed" and failed["replaced_paths"] == 0
    original = os.replace
    def interrupted(source, destination):
        if ".forward-share-" in str(source):
            raise KeyboardInterrupt
        return original(source, destination)
    monkeypatch.setattr(os, "replace", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _run(root, receipts, apply=True)
    result = json.loads((receipts / "latest.json").read_bytes())
    assert result["state"] == "interrupted" and result["retired_allocated_bytes"] == 0
    assert _bytes(root) == before and not list(root.glob("*.tmp"))


def test_unknown_names_and_bounded_inventory_fail_closed(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b")
    (root / "unknown.forward").write_bytes(b"unique")
    monkeypatch.setattr(compaction, "MAX_KEYS", 2)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "failed" and "bounded key count" in result["error"]
    assert (root / "unknown.forward").read_bytes() == b"unique"


def test_contract_stays_in_sync_with_native_producer_and_no_model_import_in_decoder():
    assert compaction.SUPPORTED_FORWARD_CONTRACT == cache.CONTRACT
    source = Path(compaction.__file__).read_text()
    assert "import torch" not in source
    assert hashlib.sha256(source.encode()).hexdigest()


def test_common_primitive_rejects_a_mixed_content_class_before_chmod_or_replacement(tmp_path):
    from stockagent.data_sync.immutable_cache_compaction import share_locked_payload_group

    root = tmp_path / "cache"
    _entry(root, "a", 7), _entry(root, "b", 8)
    files = compaction._inventory(root)[0]
    before = {p.name: (p.read_bytes(), p.stat().st_mode) for p in root.glob("*.forward")}
    with ExitStack() as handles, pytest.raises(ValueError, match="equivalence class"):
        share_locked_payload_group(root, [[files[0]], [files[1]]], touched=set(),
                                   report={"replaced_paths": 0}, retired_handles=handles,
                                   temporary_tag="forward-share")
    assert {p.name: (p.read_bytes(), p.stat().st_mode) for p in root.glob("*.forward")} == before


def test_candidate_only_maintenance_avoids_rehashing_unique_payloads_but_is_not_integrity_acceptance(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b", 8)
    monkeypatch.setattr(compaction, "stable_payload_hash", lambda *a, **kw: pytest.fail("no duplicate candidates"))
    result = _run(root, receipts, full_audit=False, apply=True)
    assert result["state"] == "compacted" and result["duplicate_groups"] == 0
    assert result["hash_scope"] == "sharing_candidates_only"
    assert result["verified_payload_bytes"] == 0 and result["metadata_only_payload_inodes"] == 2


def test_candidate_only_mode_still_hashes_every_duplicate_before_sharing(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _, ap, _ = _entry(root, "a")
    _entry(root, "b")
    ap.write_bytes(ap.read_bytes()[:-1] + b"x")
    before = _bytes(root)
    result = _run(root, receipts, full_audit=False, apply=True)
    assert result["state"] == "failed" and "SHA mismatch" in result["error"]
    assert result["replaced_paths"] == 0 and _bytes(root) == before


def test_new_independent_key_during_audit_does_not_block_the_existing_exact_scope(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b")
    original = compaction.stable_payload_hash
    added = []
    def with_new_key(path, expected):
        digest = original(path, expected)
        if not added:
            added.append(_entry(root, "new", 9)[0])
        return digest
    monkeypatch.setattr(compaction, "stable_payload_hash", with_new_key)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "compacted" and result["replaced_paths"] == 1
    assert result["new_keys_retained"] == [added[0] + ".forward"]
    assert result["stable_committed_keys"] == 2


def test_changed_link_generation_during_audit_is_retained_not_a_false_content_failure(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _entry(root, "a"), _entry(root, "b")
    original = compaction.stable_payload_hash
    added = []
    def with_new_link(path, expected):
        digest = original(path, expected)
        if not added:
            added.append(path.name)
            os.link(path, tmp_path / "external")
        return digest
    monkeypatch.setattr(compaction, "stable_payload_hash", with_new_link)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "partially_compacted" and result["replaced_paths"] == 0
    assert result["changed_keys_retained"] == added
    assert (tmp_path / "external").stat().st_mode & 0o222
