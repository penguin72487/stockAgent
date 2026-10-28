from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import pytest

from stockagent.data_sync import artifact_retirement as retirement
from stockagent.data_sync import closed_source as closed
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_retention import RetentionConfig
from test_closed_source import cohort, adopt, original_owner

REAL_USAGE = retirement._closed_raw_usage


@pytest.fixture
def options(cohort, monkeypatch):
    source, cold, cache, _, _, config_path = cohort
    adopt(cohort)
    policy = source.parent / "retirement-policy.json"
    policy.write_text(json.dumps({"schema_version": 1, "authority_node_id": "test-node",
        "required_peer_names": [], "closed_source_raw_datasets": [closed.DATASET]}))
    monkeypatch.setattr(retirement, "verify_cold_resilience",
                        lambda *_: {"cold_primary_verified": True, "backup_verified": False})
    monkeypatch.setattr(retirement, "_closed_raw_usage", lambda *_a, **_k: {
        "process_references": [], "service_references": {}, "configuration_references": {},
        "additional_pins": [], "active_leases": []})
    with original_owner(source) as fd:
        yield {"source": source, "config": RetentionConfig.load(config_path, repo_root=source.parent),
            "policy_path": policy, "state_root": source.parent / "retirement-state",
            "source_lock_fd": fd, "repo_root": source.parent,
            "peer_probe": lambda: {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat(), "peers": []}}


def apply(options, plan):
    return retirement.apply_closed_source_raw_retirement(expected_fingerprint=plan["plan_fingerprint"],
        _ingress_owner=options["source"].parent / "original-ingress.lock", **options)


def test_exact_raw_retirement_keeps_all_service_data_and_recovers_after_unlink(options):
    source, cfg = options["source"], options["config"]
    baseline = {name: (source / name).read_bytes() for name in closed.SERVICE_VIEWS}
    plan = retirement.plan_closed_source_raw_retirement(**options)
    assert not plan["blockers"] and len(plan["members"]) == 2
    assert not (options["state_root"] / "source-raw-retirements").exists()
    result = apply(options, plan)
    assert result["deleted_names"] == 2
    assert result["reclaimed_allocated_file_bytes"] == plan["reclaimable_allocated_bytes"]
    assert result["post_unlink_original_recovery"]["independently_reconstructed_files"] == 10
    assert set(result["unlinked_member_evidence"]) == set(plan["members"])
    for name, proof in result["unlinked_member_evidence"].items():
        assert (proof["device"], proof["inode"]) == tuple(plan["source_identities"][name]["signature"][:2])
        assert proof["links_before"] == 1 and proof["links_after"] == 0 and proof["descriptor_closed"]
    assert result["cold_objects_deleted"] == result["service_views_deleted"] == 0
    assert not result["NAS_backup_verified"]
    assert all((source / name).read_bytes() == value for name, value in baseline.items())
    assert all(not (source / name).exists() for name in plan["members"])
    assert closed.resolve_closed_source(source, cfg.sync_root, cfg.materialized_root)[1]["raw_retirement_complete"]
    assert list((options["state_root"] / "source-raw-retirements").glob("*.quarantine-*")) == []
    again = retirement.plan_closed_source_raw_retirement(**options)
    assert apply(options, again)["action"] == "already-retired"


@pytest.mark.parametrize("gate", ["process_references", "service_references", "configuration_references",
                                  "additional_pins", "active_leases"])
def test_each_current_usage_gate_preserves_both_originals(options, monkeypatch, gate):
    monkeypatch.setattr(retirement, "_closed_raw_usage", lambda *_a, **_k: {gate: ["still in use"]})
    plan = retirement.plan_closed_source_raw_retirement(**options)
    assert gate in plan["blockers"]
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(options, plan)
    assert all((options["source"] / name).exists() for name in plan["members"])
    assert not (options["state_root"] / "source-raw-retirements").exists()


def test_stale_or_failed_transport_cannot_delete(options):
    options["peer_probe"] = lambda: {"ok": False, "checked_at": datetime.now(timezone.utc).isoformat(), "peers": []}
    plan = retirement.plan_closed_source_raw_retirement(**options)
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(options, plan)
    assert all((options["source"] / name).exists() for name in plan["members"])


def test_corrupt_cold_object_rejects_before_any_source_mutation(options):
    source, cfg = options["source"], options["config"]
    resolved = closed.resolve_closed_source(source, cfg.sync_root, cfg.materialized_root)[0]
    obj = cfg.sync_root / resolved.manifest["archive"]["objects"][0]["relpath"]
    raw = obj.read_bytes()
    obj.write_bytes(b"!" + raw[1:])
    with pytest.raises(SnapshotError):
        retirement.plan_closed_source_raw_retirement(**options)
    assert len(list((source / "raw").glob("*.zip"))) == 2


def test_changed_dry_run_or_policy_cannot_delete(options):
    plan = retirement.plan_closed_source_raw_retirement(**options)
    options["policy_path"].write_text(options["policy_path"].read_text() + "\n")
    with pytest.raises(SnapshotError, match="plan changed"):
        apply(options, plan)
    assert all((options["source"] / name).exists() for name in plan["members"])


def test_unreviewed_external_hardlink_preserves_names(options):
    name = next((options["source"] / "raw").glob("*.zip"))
    (name.parent.parent.parent / "external-raw-alias").hardlink_to(name)
    plan = retirement.plan_closed_source_raw_retirement(**options)
    assert "raw-inode-has-unreviewed-hardlinks" in plan["blockers"]


def test_new_use_after_quarantine_retains_both_exact_raw_files(options, monkeypatch):
    def usage(*_a, **kwargs):
        return {"process_references": ["new reader"] if kwargs.get("quarantine") else []}
    monkeypatch.setattr(retirement, "_closed_raw_usage", usage)
    plan = retirement.plan_closed_source_raw_retirement(**options)
    with pytest.raises(SnapshotError, match="post-quarantine use"):
        apply(options, plan)
    state = json.loads((options["state_root"] / "source-raw-retirements" / f"{closed.DATASET}.json").read_text())
    assert state["state"] == "retiring" and state["unlinked_members"] == []
    quarantine = Path(state["quarantine"])
    for name, row in plan["members"].items():
        assert hashlib.sha256((quarantine / Path(name).name).read_bytes()).hexdigest() == row["sha256"]
    assert not (options["source"] / closed.RETIREMENT_RECEIPT).exists()
    again = retirement.plan_closed_source_raw_retirement(**options)
    assert "unfinished-retirement-quarantine-requires-audit" in again["blockers"]


def test_absent_raw_without_receipt_is_not_retirement_acceptance(options):
    source, cfg = options["source"], options["config"]
    for path in (source / "raw").glob("*.zip"):
        path.unlink()
    status = closed.resolve_closed_source(source, cfg.sync_root, cfg.materialized_root)[1]
    assert status["raw_workspace_empty"] and not status["raw_retirement_complete"]
    assert status["raw_absence_without_retirement_receipt"]
    plan = retirement.plan_closed_source_raw_retirement(**options)
    assert "raw-name-set-differs-without-complete-retirement-receipt" in plan["blockers"]


def test_unknown_local_retirement_receipt_is_rejected(options):
    (options["source"] / closed.RETIREMENT_RECEIPT).write_text("{}")
    with pytest.raises(SnapshotError, match="retirement receipt differs"):
        retirement.plan_closed_source_raw_retirement(**options)


@pytest.mark.parametrize("extra", ["none", "pin", "lease", "bad-lease"])
def test_real_pin_and_lease_owner_preserves_additional_use(options, monkeypatch, extra):
    from stockagent.data_sync import artifact_maintenance, materialized_cache, remote_legacy_return
    from stockagent.data_sync import packed_snapshots as packed
    monkeypatch.setattr(artifact_maintenance, "_ancestor_process_references", lambda _a, _b, refs: refs)
    monkeypatch.setattr(materialized_cache, "process_references_many", lambda *_a, **_k: [])
    monkeypatch.setattr(retirement, "artifact_service_references", lambda paths, _r: {str(p): [] for p in paths})
    monkeypatch.setattr(remote_legacy_return, "active_configuration_references_many",
                        lambda paths, _r: {str(p): [] for p in paths})
    source, cfg = options["source"], options["config"]
    resolved = closed.resolve_closed_source(source, cfg.sync_root, cfg.materialized_root)[0]
    members = {name: row for name, row in closed._files(cfg.sync_root, resolved).items()
               if closed._RAW.fullmatch(name)}
    if extra == "pin":
        packed.write_packed_pin(cfg.materialized_root / "pins" / "research.pin.json", resolved)
    elif extra in {"lease", "bad-lease"}:
        path = materialized_cache._lease_path(cfg.materialized_root, closed.DATASET, resolved.manifest["snapshot_id"])
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"schema_version": 1, "dataset": closed.DATASET,
            "snapshot_id": resolved.manifest["snapshot_id"], "state": "hot",
            "target": str(materialized_cache._target_path(cfg.materialized_root, closed.DATASET, path.stem)),
            "expires_ns": time.time_ns() + 60 * 10**9} if extra == "lease" else {"expires_ns": 0}))
    if extra == "bad-lease":
        with pytest.raises(SnapshotError, match="unknown managed-use lease"):
            REAL_USAGE(source, members, cfg.materialized_root, resolved, options["repo_root"])
    else:
        result = REAL_USAGE(source, members, cfg.materialized_root, resolved, options["repo_root"])
        assert bool(result["additional_pins"]) == (extra == "pin")
        assert bool(result["active_leases"]) == (extra == "lease")


def test_existing_ingress_owner_prevents_competing_raw_retirement(options):
    owner = options["source"].parent / "original-ingress.lock"
    plan = retirement.plan_closed_source_raw_retirement(**options)
    with packed_owner(owner):
        with pytest.raises(SnapshotError, match="owner busy"):
            apply(options, plan)
    assert all((options["source"] / name).exists() for name in plan["members"])


def packed_owner(path):
    from stockagent.data_sync.packed_snapshots import _try_publication_lock
    return _try_publication_lock(path)


@pytest.mark.parametrize("damage", ["missing-proof", "recovery-count", "recovery-release", "link", "allocated", "time"])
def test_incomplete_post_unlink_receipt_never_reports_retirement_complete(options, damage):
    apply(options, retirement.plan_closed_source_raw_retirement(**options))
    path = options["source"] / closed.RETIREMENT_RECEIPT
    proof = json.loads(path.read_text())
    if damage == "missing-proof":
        proof.pop("unlinked_member_evidence")
    elif damage == "recovery-count":
        proof["post_unlink_original_recovery"]["independently_reconstructed_files"] -= 1
    elif damage == "recovery-release":
        proof["post_unlink_original_recovery"]["manifest_sha256"] = "0" * 64
    elif damage == "link":
        next(iter(proof["unlinked_member_evidence"].values()))["links_after"] = 1
    elif damage == "allocated":
        proof["reclaimed_allocated_file_bytes"] += 512
    else:
        proof["retired_at_utc"] = "2026-10-26T00:00:00"
    path.write_text(json.dumps(proof))
    with pytest.raises(SnapshotError, match="retirement (lacks|timestamp)"):
        closed.local_closed_source_status(options["source"])


def test_transport_failure_after_quarantine_preserves_every_original(options):
    calls = []
    def probe():
        calls.append(None)
        return {"ok": len(calls) < 3, "checked_at": datetime.now(timezone.utc).isoformat(), "peers": []}
    options["peer_probe"] = probe
    plan = retirement.plan_closed_source_raw_retirement(**options)
    with pytest.raises(SnapshotError):
        apply(options, plan)
    state = json.loads((options["state_root"] / "source-raw-retirements" / f"{closed.DATASET}.json").read_text())
    assert state["unlinked_members"] == [] and state["state"] == "retiring"
    for name, row in plan["members"].items():
        assert hashlib.sha256((Path(state["quarantine"]) / Path(name).name).read_bytes()).hexdigest() == row["sha256"]


@pytest.mark.parametrize("cohort", ["data_cftc_legacy"], indirect=True)
def test_layout_uses_actual_retirement_receipt_without_reopening_payload(options, monkeypatch):
    from stockagent.data_sync.dataset_version_layout import inventory_version_layout
    from stockagent.data_sync import packed_snapshots as packed
    source, repo = options["source"], options["repo_root"]
    policy = Path(__file__).resolve().parents[1] / "configs/columnar_storage.json"
    (repo / "configs").mkdir()
    (repo / "configs/columnar_storage.json").write_bytes(policy.read_bytes())
    for name in json.loads(policy.read_text())["source_versioning"]["roots"]:
        (repo / name).mkdir(exist_ok=True)
    catalog = [{"source": "data_cftc_legacy", "dataset": closed.DATASET, "publish": True}]
    before = inventory_version_layout(repo, catalog)
    row = next(row for row in before["roots"] if row["root"] == source.name)
    assert not row["native_workspace_migrated"]
    assert row["rollout_state"] == "closed_cftc_raw_retirement_pending"
    result = apply(options, retirement.plan_closed_source_raw_retirement(**options))
    monkeypatch.setattr(closed, "_hash", lambda *_: pytest.fail("inventory reopened source payload"))
    monkeypatch.setattr(packed, "_open_inventory", lambda *_: pytest.fail("inventory reopened cold payload"))
    after = inventory_version_layout(repo, catalog)
    row = next(row for row in after["roots"] if row["root"] == source.name)
    assert after["state"] == "accepted" and not after["errors"]
    assert row["native_workspace_migrated"] and row["rollout_state"] == "closed_cftc_raw_retired_receipt"
    assert row["native_acceptance"]["verified_at_utc"] == result["retired_at_utc"]
    assert not row["native_acceptance"]["payload_reverified_this_inventory"]
    assert sum(row["native_workspace_migrated"] for row in after["roots"]) == 1
    (source / closed.RETIREMENT_RECEIPT).write_text("{}")
    invalid = inventory_version_layout(repo, catalog)
    row = next(row for row in invalid["roots"] if row["root"] == source.name)
    assert not row["native_workspace_migrated"] and invalid["errors"]
