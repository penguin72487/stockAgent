from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import retire_enrolled_artifacts as scheduled
from stockagent.data_sync.cold_artifacts import ColdArtifactSpec
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_retention import RetentionConfig


def _retention_config(tmp_path: Path) -> RetentionConfig:
    return RetentionConfig(
        sync_root=tmp_path / "sync",
        archive_root=tmp_path / "archive",
        materialized_root=tmp_path / "materialized",
        backup_config=tmp_path / "backup.json",
        state_dir=tmp_path / "retention",
        folder_id="packed",
        required_peer_names=("old-cold-object-peer",),
    )


def _write_config(tmp_path: Path, *, relative_root: str, maximum_file_bytes=None):
    policy = tmp_path / "policy.json"
    registry = tmp_path / "registry.json"
    dataset = "artifact-complete-run"
    policy.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "authority_node_id": "penguin",
                "required_peer_names": [],
                "scheduled_datasets": [dataset],
            }
        )
    )
    registry.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifacts": [
                    {
                        "dataset": dataset,
                        "relative_root": relative_root,
                        "maximum_file_bytes": maximum_file_bytes,
                        "loose_file_threshold_bytes": 8388608,
                        "pack_buckets": 32,
                        "min_stable_hours": 24,
                        "completion_contract": "training-lifecycle-v1",
                    }
                ],
            }
        )
    )
    return policy, registry


def test_scheduled_specs_allow_only_registered_full_market_runs(tmp_path: Path) -> None:
    policy, registry = _write_config(tmp_path, relative_root="markets/complete-run")
    specs = scheduled.scheduled_specs(policy, registry, "penguin")
    assert [spec.dataset for spec in specs] == ["artifact-complete-run"]


def test_scheduled_legacy_specs_require_explicit_non_deployable_catalog(
    tmp_path: Path,
) -> None:
    policy = tmp_path / "policy.json"
    catalog = tmp_path / "legacy.json"
    policy.write_text(json.dumps({
        "schema_version": 1,
        "authority_node_id": "penguin",
        "scheduled_legacy_datasets": ["legacy-artifact-markets-crypto"],
    }))
    catalog.write_text(json.dumps({
        "schema_version": 1,
        "authority_node_id": "penguin",
        "archives": [{
            "dataset": "legacy-artifact-markets-crypto",
            "relative_root": "markets/crypto",
            "minimum_stable_days": 7,
            "archive_only": True,
            "compression": "gzip-1-csv-over-8m",
            "stage_root": str(tmp_path / "stage"),
        }],
    }))
    assert [spec.dataset for spec in scheduled.scheduled_legacy_specs(
        policy, catalog, "penguin"
    )] == ["legacy-artifact-markets-crypto"]
    policy.write_text(json.dumps({
        "schema_version": 1,
        "authority_node_id": "penguin",
        "scheduled_legacy_datasets": ["unregistered"],
    }))
    with pytest.raises(SnapshotError, match="not allowlisted"):
        scheduled.scheduled_legacy_specs(policy, catalog, "penguin")


@pytest.mark.parametrize(
    ("relative_root", "maximum_file_bytes"),
    [("ablations/complete-run", None), ("markets/partial-run", 100)],
)
def test_scheduled_specs_reject_unsafe_scope_or_partial_release(
    tmp_path: Path, relative_root: str, maximum_file_bytes: int | None
) -> None:
    policy, registry = _write_config(
        tmp_path, relative_root=relative_root, maximum_file_bytes=maximum_file_bytes
    )
    with pytest.raises(SnapshotError, match="scheduled artifact"):
        scheduled.scheduled_specs(policy, registry, "penguin")


def test_active_lease_short_circuits_only_before_expiry() -> None:
    day_ns = 86_400_000_000_000
    state = {
        "schema_version": 1,
        "state": "hot-enrolled",
        "last_used_ns": 1_000_000_000_000_000_000,
    }
    deferred = scheduled._active_lease_deferred_row(
        "registered-run", state, now_ns=state["last_used_ns"] + 6 * day_ns
    )
    assert deferred is not None
    assert deferred["blockers"] == ["seven-day-use-lease-active"]
    assert deferred["verification"] == "not_checked_active_lease"
    assert deferred["snapshot_id"] is None
    assert scheduled._active_lease_deferred_row(
        "registered-run", state, now_ns=state["last_used_ns"] + 7 * day_ns
    ) is None
    assert scheduled._active_lease_deferred_row(
        "registered-run", {**state, "schema_version": 0}, now_ns=state["last_used_ns"]
    ) is None
    assert scheduled._active_lease_deferred_row(
        "registered-run", state, now_ns=state["last_used_ns"] - 6_000_000_000
    ) is None


def test_scheduled_active_leases_avoid_expensive_cold_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact = ColdArtifactSpec(
        dataset="artifact-complete-run",
        relative_root="markets/complete-run",
        maximum_file_bytes=None,
        loose_file_threshold_bytes=8388608,
        pack_buckets=32,
        min_stable_hours=24,
        completion_contract="training-lifecycle-v1",
    )
    legacy = SimpleNamespace(
        dataset="legacy-artifact-markets-crypto", relative_root="markets/crypto"
    )
    state_root = tmp_path / "state"
    legacy_root = tmp_path / "legacy-state"
    state_bytes = {}
    for root, spec, key in (
        (state_root, artifact, "artifact_relative_root"),
        (legacy_root, legacy, "relative_root"),
    ):
        path = root / "retirements" / f"{spec.dataset}.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({
            "schema_version": 1,
            "dataset": spec.dataset,
            key: spec.relative_root,
            "state": "hot-enrolled",
            "last_used_ns": time.time_ns(),
        }))
        state_bytes[path] = path.read_bytes()
    monkeypatch.setattr(
        scheduled.RetentionConfig,
        "load",
        lambda *args, **kwargs: _retention_config(tmp_path),
    )
    monkeypatch.setattr(scheduled, "scheduled_specs", lambda *args: [artifact])
    monkeypatch.setattr(scheduled, "scheduled_legacy_specs", lambda *args: [legacy])
    monkeypatch.setattr(scheduled, "load_retirement_peer_names", lambda *args, **kwargs: ())
    def local_syncthing(cfg):
        assert cfg.required_peer_names == ()
        return {"ok": True}

    monkeypatch.setattr(scheduled, "_syncthing", local_syncthing)
    monkeypatch.setattr(
        scheduled,
        "plan_artifact_retirement",
        lambda *args, **kwargs: pytest.fail("active lease must not hash packed payload"),
    )
    monkeypatch.setattr(
        scheduled,
        "plan_legacy_retirement",
        lambda *args, **kwargs: pytest.fail("active lease must not hash legacy payload"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retire_enrolled_artifacts.py", "--apply", "--state-root", str(state_root),
            "--legacy-state-root", str(legacy_root),
        ],
    )
    assert scheduled.main() == 0
    rows = json.loads(capsys.readouterr().out)["rows"]
    assert len(rows) == 2
    assert all(row["action"] == "deferred" for row in rows)
    assert all(row["verification"] == "not_checked_active_lease" for row in rows)
    assert {path: path.read_bytes() for path in state_bytes} == state_bytes


@pytest.mark.parametrize(
    ("blockers", "expected_action"),
    [
        (["seven-day-use-lease-active"], "deferred"),
        (["artifact-has-process-references"], "renewed-in-use"),
    ],
)
def test_apply_preserves_lease_and_renews_live_references(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    blockers: list[str],
    expected_action: str,
) -> None:
    spec = ColdArtifactSpec(
        dataset="artifact-complete-run",
        relative_root="markets/complete-run",
        maximum_file_bytes=None,
        loose_file_threshold_bytes=8388608,
        pack_buckets=32,
        min_stable_hours=24,
        completion_contract="training-lifecycle-v1",
    )
    state_root = tmp_path / "state"
    state_path = state_root / "retirements" / f"{spec.dataset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "dataset": spec.dataset,
                "artifact_relative_root": spec.relative_root,
                "state": "hot-enrolled",
            }
        )
    )
    monkeypatch.setattr(
        scheduled.RetentionConfig,
        "load",
        lambda *args, **kwargs: _retention_config(tmp_path),
    )
    monkeypatch.setattr(scheduled, "scheduled_specs", lambda *args: [spec])
    monkeypatch.setattr(scheduled, "load_retirement_peer_names", lambda *args, **kwargs: ())
    monkeypatch.setattr(scheduled, "_syncthing", lambda *args: {"ok": True})
    monkeypatch.setattr(scheduled, "_bridge_inactive", lambda *args: True)
    monkeypatch.setattr(
        scheduled,
        "plan_artifact_retirement",
        lambda *args, **kwargs: {
            "apply_ready": False,
            "blockers": blockers,
            "lease_expires_at": "2026-10-02T00:00:00Z",
            "snapshot_id": "artifact-release",
            "plan_fingerprint": "exact-fingerprint",
        },
    )
    monkeypatch.setattr(
        scheduled,
        "apply_artifact_retirement",
        lambda *args, **kwargs: {"action": "renewed-in-use", "deleted": False},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retire_enrolled_artifacts.py",
            "--apply",
            "--state-root",
            str(state_root),
            "--legacy-state-root",
            str(tmp_path / "legacy-state"),
        ],
    )
    assert scheduled.main() == 0
    assert json.loads(capsys.readouterr().out)["rows"][0]["action"] == expected_action


def test_one_legacy_proof_failure_does_not_hide_other_dataset_plans(tmp_path, monkeypatch, capsys):
    specs = [SimpleNamespace(dataset=name, relative_root=f"markets/{name}")
             for name in ("mismatch", "valid")]
    legacy_state = tmp_path / "legacy-state"
    for spec in specs:
        path = legacy_state / "retirements" / f"{spec.dataset}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema_version": 1, "state": "hot-enrolled",
                                    "dataset": spec.dataset, "relative_root": spec.relative_root,
                                    "last_used_ns": time.time_ns() - 8 * 86_400_000_000_000}))
    monkeypatch.setattr(scheduled.RetentionConfig, "load", lambda *a, **kw: _retention_config(tmp_path))
    monkeypatch.setattr(scheduled, "scheduled_specs", lambda *a: [])
    monkeypatch.setattr(scheduled, "scheduled_legacy_specs", lambda *a: specs)
    monkeypatch.setattr(scheduled, "load_retirement_peer_names", lambda *a, **kw: ())
    monkeypatch.setattr(scheduled, "_syncthing", lambda *a: {"ok": True})
    monkeypatch.setattr(scheduled, "_bridge_inactive", lambda *a: True)

    def plan(spec, **options):
        if spec.dataset == "mismatch":
            raise SnapshotError("unique hot mirror differs")
        return {"apply_ready": False, "blockers": ["artifact-release-is-pinned"],
                "lease_expires_at": "expired", "snapshot_id": "valid-release"}

    monkeypatch.setattr(scheduled, "plan_legacy_retirement", plan)
    monkeypatch.setattr(scheduled, "apply_legacy_retirement",
                        lambda *a, **kw: pytest.fail("neither plan authorizes deletion"))
    monkeypatch.setattr(sys, "argv", ["retire_enrolled_artifacts.py", "--apply",
                                     "--legacy-state-root", str(legacy_state)])
    assert scheduled.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "partial_deferred"
    assert [row["dataset"] for row in result["rows"]] == ["mismatch", "valid"]
    assert result["rows"][0]["deleted"] is False
    assert result["rows"][1]["blockers"] == ["artifact-release-is-pinned"]


def test_manual_batch_bypasses_only_age_and_records_selected_result(tmp_path, monkeypatch, capsys):
    specs = [SimpleNamespace(dataset=name, relative_root=f"markets/{name}") for name in ("keep", "retire")]
    root = tmp_path / "state"
    for spec in specs:
        path = root / "retirements" / f"{spec.dataset}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema_version": 1, "state": "hot-enrolled",
                                   "dataset": spec.dataset, "relative_root": spec.relative_root,
                                   "last_used_ns": time.time_ns()}))
    monkeypatch.setattr(scheduled.RetentionConfig, "load", lambda *a, **kw: _retention_config(tmp_path))
    monkeypatch.setattr(scheduled, "scheduled_specs", lambda *a: [])
    monkeypatch.setattr(scheduled, "scheduled_legacy_specs", lambda *a: specs)
    monkeypatch.setattr(scheduled, "load_retirement_peer_names", lambda *a, **kw: ())
    monkeypatch.setattr(scheduled, "_syncthing", lambda *a: {"ok": True})
    monkeypatch.setattr(scheduled, "_wait_for_convergence", lambda cfg, timeout: {"ok": True})
    monkeypatch.setattr(scheduled, "_bridge_inactive", lambda *a: True)

    def plan(spec, **options):
        assert spec.dataset == "retire" and options["manual_immediate"] is True
        return {"apply_ready": True, "blockers": [], "lease_expires_at": "still-active",
                "snapshot_id": "exact-release", "plan_fingerprint": "manual-fingerprint"}

    def apply(spec, *, expected_fingerprint, **options):
        assert options["manual_immediate"] is True and expected_fingerprint == "manual-fingerprint"
        assert (tmp_path / "receipts/retire.plan.json").is_file()
        return {"action": "retired", "deleted": True, "reclaimed_allocated_file_bytes": 4096}

    monkeypatch.setattr(scheduled, "plan_legacy_retirement", plan)
    monkeypatch.setattr(scheduled, "apply_legacy_retirement", apply)
    monkeypatch.setattr(sys, "argv", ["retire_enrolled_artifacts.py", "--apply", "--manual-immediate",
                                     "--dataset", "retire", "--legacy-state-root", str(root),
                                     "--receipt-dir", str(tmp_path / "receipts")])
    assert scheduled.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["reclaimed_allocated_file_bytes"] == 4096
    assert len(result["rows"]) == 1 and result["manual_immediate"] is True
    assert json.loads((tmp_path / "receipts/summary.json").read_text()) == result
