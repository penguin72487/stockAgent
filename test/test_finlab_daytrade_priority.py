from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path

import pytest

from scripts import run_finlab_daytrade_priority as coordinator
from scripts import download_finlab_history as collector
from downloader.provider_priority import register
from stockagent.data.finlab_acquisition_contract import safe_stem
from stockagent.data.finlab_gap_priority import pending_daytrade_priority, pending_gap_requests


NOW = datetime(2026, 10, 28, 0, 0, tzinfo=UTC)


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False))


def receipt(root: Path, key: str, data: bytes, *, checked: datetime = NOW - timedelta(days=2)) -> dict:
    digest = hashlib.sha256(data).hexdigest()
    path = root / "datasets" / (digest + ".parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    result = {"dataset": key, "parquet_path": "datasets/" + path.name,
              "parquet_size_bytes": len(data), "sha256": digest,
              "source_checked_at_utc": checked.isoformat(), "source_check_mode": "upstream_incremental"}
    write_json(root / "receipts" / (safe_stem(key) + ".json"), result)
    return result


def test_selected_dependencies_are_verified_and_cached(tmp_path):
    dictionary = {"features": [
        {"feature": "a", "source": "FinLab", "dataset": "price:close"},
        {"feature": "b", "source": "other", "dataset": "ignored"},
        {"feature": "c", "source": "FinLab", "dataset": "price:close"},
        {"feature": "d", "source": "FinLab", "dataset": "revenue:monthly"},
    ]}
    dictionary_path = tmp_path / "feature_dictionary.json"
    write_json(dictionary_path, dictionary)
    parent = tmp_path / "factorized_manifest.json"
    write_json(parent, {"feature_dictionary_sha256": hashlib.sha256(dictionary_path.read_bytes()).hexdigest(),
                        "individual_channels": ["a", "b", "c"], "common_channels": ["d"]})
    cache = tmp_path / "cache/selected.json"
    parent_sha, keys = coordinator.selected_datasets(parent, cache)
    assert keys == ["price:close", "revenue:monthly"]
    assert coordinator.selected_datasets(parent, cache) == (parent_sha, keys)
    dictionary_path.write_text("{}")
    with pytest.raises(ValueError, match="SHA"):
        coordinator.selected_datasets(parent, cache)


def test_due_selection_uses_existing_finite_registry_without_duplicates(tmp_path):
    keys = ["price:close", "revenue:monthly"]
    write_json(tmp_path / "catalog/discovery.json", {"keys": keys})
    receipt(tmp_path, keys[0], b"old")
    receipt(tmp_path, keys[1], b"fresh", checked=NOW)
    assert coordinator.schedule_due(tmp_path, keys, now=NOW) == [keys[0]]
    assert coordinator.schedule_due(tmp_path, keys, now=NOW) == [keys[0]]
    plan = json.loads((tmp_path / "gap_priority_requests.json").read_bytes())
    assert len(plan["requests"]) == 1
    assert list(pending_gap_requests(tmp_path, now=NOW, available=keys)) == [keys[0]]
    assert pending_daytrade_priority(tmp_path, now=NOW, available=keys)
    receipt(tmp_path, keys[0], b"new", checked=NOW)
    assert not pending_daytrade_priority(tmp_path, now=NOW, available=keys)


def test_projection_checks_all_receipts_and_compares_source_heads(tmp_path):
    root, public, finlab = tmp_path / "projection", tmp_path / "public", tmp_path / "finlab"
    first, second = "price:close", "revenue:monthly"
    a = receipt(finlab, first, b"a")
    b = receipt(finlab, second, b"b")
    generation = root / "generations/g1"
    write_json(generation / "factorized_manifest.json", {
        "status": "complete", "paper_inference": {"status": "verified", "completed_session": "2026-10-27",
        "source_identity": {"parent": "parent", "finlab": {first: a["sha256"], second: b["sha256"]}}}})
    (root / "current").symlink_to(generation, target_is_directory=True)
    write_json(public / "features/tw_public_stock_daily.summary.json", {"requested_end_date": "2026-10-27"})
    check = lambda: coordinator.projection_needs_update(root, public, finlab, "parent", [first, second])
    assert check() == (False, "source_heads_current")
    receipt(finlab, first, b"new")
    assert check() == (True, "finlab_source_head_changed")
    write_json(public / "features/tw_public_stock_daily.summary.json", {"requested_end_date": "2026-10-28"})
    assert check() == (True, "completed_session_changed")
    write_json(public / "features/tw_public_stock_daily.summary.json", {"requested_end_date": "2026-10-27"})
    (finlab / b["parquet_path"]).unlink()
    assert check() == (False, "source_receipt_or_object_missing")


def test_projection_rejects_redirected_generation(tmp_path):
    root, outside = tmp_path / "projection", tmp_path / "outside"
    (root / "generations").mkdir(parents=True)
    write_json(outside / "factorized_manifest.json", {})
    (root / "current").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escapes"):
        coordinator.projection_needs_update(root, tmp_path, tmp_path, "parent", [])


def test_lock_reports_busy_without_interrupting_owner(tmp_path):
    with coordinator._lock(tmp_path / "owner.lock") as first:
        with coordinator._lock(tmp_path / "owner.lock") as second:
            assert first and not second
    with coordinator._lock(tmp_path / "owner.lock") as after:
        assert after


def test_registers_priority_before_waiting_for_account_owner(tmp_path, monkeypatch):
    finlab = tmp_path / "finlab"
    finlab.mkdir()
    monkeypatch.setattr(coordinator, "FINLAB", finlab)
    monkeypatch.setattr(coordinator, "STATE", tmp_path / "state")
    monkeypatch.setattr(coordinator, "selected_datasets", lambda *_: ("parent", ["price:close"]))
    calls = []
    monkeypatch.setattr(coordinator, "schedule_due", lambda *a, **kw: calls.append("registered") or ["price:close"])
    monkeypatch.setattr(coordinator.subprocess, "run", lambda *a, **kw: pytest.fail("second SDK owner"))
    with coordinator._lock(finlab / ".sync.lock") as owner:
        assert owner
        assert coordinator.main(["sync"]) == 75
    assert calls == ["registered"]


def test_opening_keeps_accepted_model_stable_while_raw_refresh_continues(tmp_path, monkeypatch):
    root = tmp_path / "projection"
    generation = root / "generations/g1"
    write_json(generation / "factorized_manifest.json", {"paper_inference": {"decision_session": "2026-10-28"}})
    (root / "current").symlink_to(generation, target_is_directory=True)
    monkeypatch.setattr(coordinator, "PROJECTION", root)
    monkeypatch.setattr(coordinator, "STATE", tmp_path / "state")
    monkeypatch.setattr(coordinator, "PUBLIC", tmp_path / "public")
    monkeypatch.setattr(coordinator, "selected_datasets", lambda *_: ("parent", ["price:close"]))
    monkeypatch.setattr(coordinator, "projection_needs_update", lambda *a, **kw: (True, "finlab_source_head_changed"))
    monkeypatch.setattr(coordinator, "_daytrade_reader_hold", lambda *a, **kw: {
        "session_date": "2026-10-28", "resume_after": "2026-10-28T13:35:00+08:00"})
    monkeypatch.setattr(coordinator.subprocess, "run", lambda *a, **kw: pytest.fail("changed opening input"))
    assert coordinator.main(["reconcile"]) == 75


def test_reader_hold_skips_repeated_source_scans_after_required_checks(tmp_path, monkeypatch):
    root, public = tmp_path / "projection", tmp_path / "public"
    generation = root / "generations/g1"
    write_json(generation / "factorized_manifest.json", {
        "status": "complete", "paper_inference": {"status": "verified", "decision_session": "2026-10-28",
        "completed_session": "2026-10-27", "source_identity": {"parent": "parent", "finlab": {"price:close": "sha"}}}})
    (root / "current").symlink_to(generation, target_is_directory=True)
    write_json(public / "features/tw_public_stock_daily.summary.json", {"requested_end_date": "2026-10-27"})
    monkeypatch.setattr(coordinator, "_daytrade_reader_hold", lambda *a, **kw: {"session_date": "2026-10-28"})
    monkeypatch.setattr(coordinator, "_local_receipt", lambda *a: pytest.fail("unnecessary source scan"))
    check = lambda: coordinator.projection_needs_update(
        root, public, tmp_path / "finlab", "parent", ["price:close"], protect_ready_readers=True)
    assert check() == (False, "daytrade_reader_priority")
    write_json(public / "features/tw_public_stock_daily.summary.json", {"requested_end_date": "2026-10-28"})
    assert check() == (True, "completed_session_changed")


def test_head_only_reader_hold_uses_verified_decision_session(tmp_path, monkeypatch):
    monkeypatch.setattr(coordinator, "active_opening_revision_freeze", lambda *a, **kw: {})
    local = coordinator.TAIPEI
    hold = lambda hour, minute, session="2026-10-28": coordinator._daytrade_reader_hold(
        tmp_path, decision_session=session, observed=datetime(2026, 10, 28, hour, minute, tzinfo=local))
    assert not hold(8, 18)
    assert hold(8, 19)["resume_after"] == "2026-10-28T13:35:00+08:00"
    assert hold(13, 34)
    assert not hold(13, 35)
    assert not hold(9, 0, "2026-10-29")


def test_optional_projection_is_preempted_when_live_reader_window_starts(monkeypatch):
    class Child:
        pid = 12345
        calls = 0

        def wait(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise coordinator.subprocess.TimeoutExpired("fake", timeout)
            return -15

    child = Child()
    monkeypatch.setattr(coordinator.subprocess, "Popen", lambda *a, **kw: child)
    monkeypatch.setattr(coordinator, "_daytrade_reader_hold", lambda *a, **kw: {
        "session_date": "2026-10-28", "resume_after": "2026-10-28T13:35:00+08:00"})
    signals = []
    monkeypatch.setattr(coordinator.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    assert coordinator._run_projection(["fake"], reason="finlab_source_head_changed",
                                       decision_session="2026-10-28") == 75
    assert signals == [(child.pid, coordinator.signal.SIGTERM)]


def test_missing_session_projection_is_not_blocked_by_reader_hold(monkeypatch):
    monkeypatch.setattr(coordinator, "_daytrade_reader_hold", lambda *a, **kw: pytest.fail("required projection was delayed"))
    monkeypatch.setattr(coordinator.subprocess, "run", lambda *a, **kw: type("Result", (), {"returncode": 0})())
    assert coordinator._run_projection(["fake"], reason="completed_session_changed", decision_session="") == 0


def test_history_owner_yields_at_a_key_boundary_for_verified_daytrade_request(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    key = "price:close"
    write_json(tmp_path / "catalog/discovery.json", {"keys": [key]})
    receipt(tmp_path, key, b"old", checked=now - timedelta(days=2))
    assert coordinator.schedule_due(tmp_path, [key], now=now) == [key]
    monkeypatch.setattr(collector, "credential_available", lambda: True)
    monkeypatch.setattr(collector, "sync_selection", lambda *a, **kw: [key])
    monkeypatch.setattr(collector, "record_core_acquisition_status", lambda *a, **kw:
                        {"required_complete": False, "required_pending": 1, "required_blocked": 0})
    monkeypatch.setattr(collector, "has_local_download", lambda *a, **kw: True)
    monkeypatch.setattr(collector, "fetch_one", lambda *a, **kw: pytest.fail("history started before handoff"))
    result = collector.sync_catalog([key], {}, tmp_path, limit=5, refresh_days=1,
                                    min_quota_remaining_mb=50, retry_unavailable=False)
    assert result["state"] == "priority_handoff"
    assert result["attempted"] == 0


def test_other_interactive_intent_does_not_imply_daytrade_handoff(tmp_path):
    key = "price:close"
    write_json(tmp_path / "catalog/discovery.json", {"keys": [key]})
    receipt(tmp_path, key, b"old")
    plan = {"contract": "provider_priority_query_v1", "provider": "finlab", "reason": "other research",
            "created_at_utc": NOW.isoformat(), "expires_at_utc": (NOW + timedelta(hours=1)).isoformat(),
            "fresh_after_utc": NOW.isoformat(), "items": [key]}
    register(tmp_path, plan, now=NOW)
    assert pending_gap_requests(tmp_path, now=NOW, available=[key])
    assert not pending_daytrade_priority(tmp_path, now=NOW, available=[key])
