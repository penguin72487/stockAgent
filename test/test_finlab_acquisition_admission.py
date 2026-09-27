"""FinLab core receipts must not manufacture surplus acquisition capacity."""
from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader import acquisition_policy as policy


def _snapshot(root, now, *, extra=None):
    rows = [
        {"id": "official:daily", "in_active_scope": True,
         "acquisition_progress": {"coverage_complete": True, "up_to_date": True,
                                  "state": "complete"}},
        {"id": "finlab:catalog-acquisition", "in_active_scope": True},
    ]
    if extra:
        rows.append(extra)
    path = root / "monitor.json"
    path.write_text(json.dumps({"generated_at_utc": now.isoformat(), "sources": rows,
                               "schema_version": 8, "read_only": True,
                               "summary": {"registered_items": len(rows)},
                               "integrity_checks": {"state": "pass", "violations": 0}}))
    return path


@pytest.mark.parametrize("age_minutes,status,complete,allowed", [
    (None, "ready", True, False),  # no local proof
    (61, "ready", True, False),
    (-2, "ready", True, False),
    (0, "blocked", False, False),
    (0, "quota_exhausted", True, False),
    (0, "authentication_failed", True, False),
    (0, "ready", True, True),
])
def test_other_provider_requires_current_consistent_finlab_core_receipt(
    tmp_path, monkeypatch, age_minutes, status, complete, allowed,
):
    now = datetime(2026, 9, 27, 1, tzinfo=UTC)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "_finmind_required", lambda *args: 0)
    if age_minutes is not None:
        folder = tmp_path / "data_finlab"
        folder.mkdir()
        (folder / "core_acquisition_status.json").write_text(json.dumps({
            "checked_at_utc": (now - timedelta(minutes=age_minutes)).isoformat(),
            "status": status, "required_complete": complete,
            "required_pending": 0 if complete else 1,
            "required_blocked": 0 if complete else 1,
            "catalog_sha256": "a" * 64,
        }))
    result = policy.evaluate_secondary_admission(snapshot_path=_snapshot(tmp_path, now), now=now)
    assert result["allowed"] is allowed


def test_current_finlab_caller_evidence_replaces_only_its_own_receipt(tmp_path, monkeypatch):
    now = datetime(2026, 9, 27, 1, tzinfo=UTC)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "_finmind_required", lambda *args: 0)
    path = _snapshot(tmp_path, now, extra={
        "id": "finlab:tw_tick:2330:alias", "in_active_scope": True,
        "registry_alias": True,
        "acquisition_progress": {"coverage_complete": False, "state": "not_downloaded"},
    })
    assert policy.evaluate_secondary_admission(
        snapshot_path=path, now=now, caller_provider="finlab", local_core_complete=True,
    )["allowed"]
    assert not policy.evaluate_secondary_admission(
        snapshot_path=path, now=now, caller_provider="finlab", local_core_complete=False,
    )["allowed"]
    path = _snapshot(tmp_path, now, extra={
        "id": "official:other", "in_active_scope": True,
        "acquisition_progress": {"coverage_complete": False, "up_to_date": False,
                                 "state": "authentication_failed"},
    })
    result = policy.evaluate_secondary_admission(
        snapshot_path=path, now=now, caller_provider="finlab", local_core_complete=True,
    )
    assert not result["allowed"]
    assert result["blocking_counts"] == {"authentication_failed": 1}


@pytest.mark.parametrize("pending,blocked", [(1, 0), (0, 1)])
def test_ready_label_with_nonzero_acquisition_debt_is_rejected(tmp_path, monkeypatch,
                                                            pending, blocked):
    now = datetime(2026, 9, 27, 1, tzinfo=UTC)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "_finmind_required", lambda *args: 0)
    folder = tmp_path / "data_finlab"
    folder.mkdir()
    (folder / "core_acquisition_status.json").write_text(json.dumps({
        "checked_at_utc": now.isoformat(), "status": "ready", "required_complete": True,
        "required_pending": pending, "required_blocked": blocked, "catalog_sha256": "a" * 64,
    }))
    assert not policy.evaluate_secondary_admission(
        snapshot_path=_snapshot(tmp_path, now), now=now,
    )["allowed"]


def test_failed_source_does_not_pass_from_inconsistent_complete_flags(tmp_path, monkeypatch):
    now = datetime(2026, 9, 27, 1, tzinfo=UTC)
    monkeypatch.setattr(policy, "ROOT", tmp_path)
    monkeypatch.setattr(policy, "_finmind_required", lambda *args: 0)
    path = _snapshot(tmp_path, now, extra={
        "id": "official:other", "in_active_scope": True,
        "acquisition_progress": {"coverage_complete": True, "up_to_date": True,
                                 "state": "authentication_failed"},
    })
    assert not policy.evaluate_secondary_admission(
        snapshot_path=path, now=now, caller_provider="finlab", local_core_complete=True,
    )["allowed"]
