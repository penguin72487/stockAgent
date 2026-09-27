"""Attempt telemetry must never stand in for a published data receipt."""

from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from scripts import compact_openbb_l1 as compactor
from test_openbb_l1_compaction import _args, _publish_tasks


INVOCATION = "a" * 32


@pytest.fixture(autouse=True)
def invocation(monkeypatch):
    monkeypatch.setenv("INVOCATION_ID", INVOCATION)


def _receipt(root):
    value = json.loads((root / "_state/l1_compaction_attempt_latest.json").read_text())
    assert value["schema_version"] == 1
    assert value["event"] == "openbb_l1_compaction_attempt"
    assert value["systemd_invocation_id"] == INVOCATION
    assert len(value["attempt_id"]) == 32
    assert datetime.fromisoformat(value["finished_at_utc"]) >= datetime.fromisoformat(
        value["started_at_utc"]
    )
    assert value["elapsed_seconds"] >= 0
    assert value["phase_elapsed_seconds"] >= 0
    assert "before_attempt" in value["stage_resources"]
    assert "after_attempt" in value["stage_resources"]
    assert len(json.dumps(value).encode()) < 32_768
    return value


def _published_marker(root):
    path = root / "_state/l1_compaction_latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"previous_published_proof":true}\n')
    return path, path.read_bytes()


def _terminal_event(capsys):
    lines = capsys.readouterr().out.splitlines()
    events = [json.loads(line) for line in lines if line.startswith('{"schema_version":')]
    assert len(events) == 1
    assert len(json.dumps(events[0]).encode()) < 32_768
    return events[0]


def test_initial_busy_records_without_database_or_publication(tmp_path, monkeypatch, capsys):
    published, before = _published_marker(tmp_path)
    monkeypatch.setattr(compactor, "_archive_compaction_allowed", lambda _: (False, "running"))
    monkeypatch.setattr(compactor, "_open_manifest", lambda _: pytest.fail("must not open DB"))
    assert compactor.run(_args(tmp_path) + ["--archive-idle-only"]) == 0
    receipt = _receipt(tmp_path)
    assert (receipt["state"], receipt["reason"], receipt["exit_code"]) == (
        "deferred", "archive_busy", 0
    )
    assert receipt["publication_stage"] == "not_attempted"
    assert receipt["stage_seconds"] == {}
    assert published.read_bytes() == before
    assert _terminal_event(capsys) == receipt


def test_mid_batch_defer_preserves_committed_progress_without_publishing(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ["A", "B", "C", "D"])
    published, before = _published_marker(tmp_path)
    allowed = iter([True, True, False])
    monkeypatch.setattr(compactor, "_archive_compaction_allowed", lambda _: (next(allowed), "running"))
    monkeypatch.setattr(compactor, "_publish_views", lambda *a, **k: pytest.fail("must defer"))
    assert compactor.run(_args(tmp_path) + ["--archive-idle-only"]) == 0
    receipt = _receipt(tmp_path)
    assert receipt["state"] == "deferred"
    assert receipt["reason"] == "archive_resumed"
    assert receipt["new_segments"] == 1
    assert receipt["publication_stage"] == "not_attempted"
    assert "segment_build" in receipt["stage_seconds"]
    assert "after_segment_build" in receipt["stage_resources"]
    assert "after_query_view_publish" not in receipt["stage_resources"]
    assert published.read_bytes() == before
    assert not (tmp_path / "openbb_l1.duckdb").exists()


def test_zero_batch_still_publishes_and_has_new_attempt_identity(tmp_path):
    _publish_tasks(tmp_path, ["A", "B"])
    assert compactor.run(_args(tmp_path)) == 0
    first = _receipt(tmp_path)
    assert first["new_segments"] == 1
    assert compactor.run(_args(tmp_path)) == 0
    second = _receipt(tmp_path)
    assert second["new_segments"] == 0
    assert second["attempt_id"] != first["attempt_id"]
    assert second["state"] == "completed"
    assert second["publication_stage"] == "status_published"
    canonical = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert canonical["started_at_utc"] == second["started_at_utc"]
    assert canonical["state"] == second["state"]


@pytest.mark.parametrize("failed", [0, 2])
def test_audit_only_does_not_publish(tmp_path, monkeypatch, failed):
    _publish_tasks(tmp_path, ["A"])
    published, before = _published_marker(tmp_path)
    monkeypatch.setattr(compactor, "_audit_segments", lambda *a, **k: [
        SimpleNamespace(status="failed") for _ in range(failed)
    ])
    monkeypatch.setattr(compactor, "_write_audit", lambda *a: None)
    assert compactor.run(_args(tmp_path) + ["--audit-only"]) == (2 if failed else 0)
    receipt = _receipt(tmp_path)
    assert receipt["mode"] == "audit"
    assert receipt["state"] == ("audit_failed" if failed else "audit_completed")
    assert receipt["failed_segments"] == failed
    assert receipt["publication_stage"] == "not_attempted"
    assert published.read_bytes() == before


@pytest.mark.parametrize("target,phase,publication", [
    ("_clean_duckdb_temp_files", "manifest_open", "not_attempted"),
    ("_open_manifest", "manifest_open", "not_attempted"),
    ("_mark_stale_segments", "stale_contract_audit", "not_attempted"),
    ("_load_unassigned_shards", "unassigned_source_load", "not_attempted"),
    ("_segment_batches", "batch_planning", "not_attempted"),
    ("_publish_views", "query_view_publish", "query_publish_started"),
    ("_quarantine_stale_outputs", "stale_output_quarantine", "query_published"),
    ("_write_status", "status_publish", "status_publish_started"),
])
def test_exception_preserves_original_and_exact_publication_boundary(
    tmp_path, monkeypatch, capsys, target, phase, publication,
):
    _publish_tasks(tmp_path, ["A", "B"])
    published, before = _published_marker(tmp_path)
    original = RuntimeError("private provider value must not enter telemetry")

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(compactor, target, fail)
    with pytest.raises(RuntimeError) as observed:
        compactor.run(_args(tmp_path))
    assert observed.value is original
    receipt = _receipt(tmp_path)
    assert receipt["state"] == "failed"
    assert receipt["reason"] == "exception"
    assert receipt["exit_code"] == 1
    assert receipt["phase"] == phase
    assert receipt["publication_stage"] == publication
    assert "private provider" not in json.dumps(receipt)
    assert published.read_bytes() == before
    assert _terminal_event(capsys) == receipt


@pytest.mark.parametrize("exception", [MemoryError("oom"), KeyboardInterrupt(), SystemExit(3)])
def test_fatal_build_failure_and_interruption_keep_partial_evidence(tmp_path, monkeypatch, exception):
    _publish_tasks(tmp_path, ["A", "B"])

    def fail(*args, **kwargs):
        raise exception

    monkeypatch.setattr(compactor, "compact_parquet_files", fail)
    with pytest.raises(type(exception)) as observed:
        compactor.run(_args(tmp_path))
    assert observed.value is exception
    receipt = _receipt(tmp_path)
    assert receipt["state"] == ("failed" if isinstance(exception, MemoryError) else "interrupted")
    assert receipt["phase"] == "segment_build"
    assert receipt["new_segments"] == 0
    assert receipt["failed_segments"] == 0
    assert "after_segment_build" not in receipt["stage_resources"]


def test_isolated_failed_and_backoff_segments_are_not_clean_completed(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ["A", "B"])

    def fail(*args, **kwargs):
        raise ValueError("bad source")

    monkeypatch.setattr(compactor, "compact_parquet_files", fail)
    assert compactor.run(_args(tmp_path)) == 0
    first = _receipt(tmp_path)
    assert first["state"] == "completed_with_failures"
    assert first["failed_segments"] == 1
    assert compactor.run(_args(tmp_path)) == 0
    second = _receipt(tmp_path)
    assert second["state"] == "completed_with_deferred_segments"
    assert second["deferred_failed_segments"] == 1


def test_lock_loser_emits_own_event_but_never_overwrites_latest(tmp_path, capsys):
    _publish_tasks(tmp_path, ["A"])
    latest = tmp_path / "_state/l1_compaction_attempt_latest.json"
    latest.write_text('{"active_owner":"other"}')
    before = latest.read_bytes()
    with compactor._exclusive_lock(tmp_path / "_state/openbb_l1_compaction.lock"):
        with pytest.raises(RuntimeError, match="another OpenBB L1 compactor"):
            compactor.run(_args(tmp_path))
        assert latest.read_bytes() == before
    event = _terminal_event(capsys)
    assert event["state"] == "failed"
    assert event["reason"] == "compactor_lock_busy"
    assert event["exit_code"] == 1
    assert event["publication_stage"] == "not_attempted"


@pytest.mark.parametrize("business_failure", [False, True])
def test_terminal_receipt_error_does_not_hide_original_failure(tmp_path, monkeypatch, capsys, business_failure):
    _publish_tasks(tmp_path, ["A", "B"])
    original = RuntimeError("original failure")
    real_write = compactor.atomic_write_json

    def write(path, payload):
        if payload["finished_at_utc"] is not None:
            raise OSError("disk full")
        return real_write(path, payload)

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(compactor, "atomic_write_json", write)
    if business_failure:
        monkeypatch.setattr(compactor, "_open_manifest", fail)
    with pytest.raises(RuntimeError) as observed:
        compactor.run(_args(tmp_path))
    if business_failure:
        assert observed.value is original
    else:
        assert isinstance(observed.value, compactor._AttemptReceiptError)
    persisted = json.loads((tmp_path / "_state/l1_compaction_attempt_latest.json").read_text())
    assert persisted["state"] == "running"
    assert persisted["finished_at_utc"] is None
    event = _terminal_event(capsys)
    assert event["state"] == "failed"
    assert event["receipt_write_failed"] is True
    assert event["reason"] == ("exception" if business_failure else "attempt_receipt_error")
    assert event["publication_stage"] == ("not_attempted" if business_failure else "status_published")


def test_start_receipt_failure_prevents_unobserved_work(tmp_path, monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(compactor, "atomic_write_json", fail)
    monkeypatch.setattr(compactor, "_open_manifest", lambda _: pytest.fail("no work without receipt"))
    with pytest.raises(compactor._AttemptReceiptError, match="cannot persist compaction start"):
        compactor.run(_args(tmp_path))
    event = _terminal_event(capsys)
    assert event["reason"] == "attempt_receipt_error"
    assert event["exit_code"] == 1


@pytest.mark.parametrize("identity", ["", "bad\nsecret", "A" * 32])
def test_invalid_invocation_is_not_exported(tmp_path, monkeypatch, identity):
    monkeypatch.setenv("INVOCATION_ID", identity)
    attempt = compactor._CompactionAttempt(compactor.parse_args(_args(tmp_path)))
    assert attempt.payload["systemd_invocation_id"] is None


@pytest.mark.parametrize("entered_lock", [False, True])
def test_secondary_sampling_oom_never_masks_original_error(tmp_path, monkeypatch, entered_lock):
    _publish_tasks(tmp_path, ["A"])
    original = ValueError("original failure")
    sample = compactor._stage_resources
    calls = 0

    def resource_sample():
        nonlocal calls
        calls += 1
        if calls > 1:
            raise MemoryError("sampling failed")
        return sample()

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(compactor, "_stage_resources", resource_sample)
    monkeypatch.setattr(compactor, "_open_manifest" if entered_lock else "_run_compaction", fail)
    with pytest.raises(ValueError) as observed:
        compactor.run(_args(tmp_path))
    assert observed.value is original


def test_running_receipt_is_only_start_snapshot(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ["A"])
    real_open = compactor._open_manifest

    def inspect_start(path):
        value = json.loads((tmp_path / "_state/l1_compaction_attempt_latest.json").read_text())
        assert value["state"] == "running"
        assert value["finished_at_utc"] is None
        assert value["exit_code"] is None
        assert value["elapsed_seconds"] == 0
        assert "after_attempt" not in value["stage_resources"]
        return real_open(path)

    monkeypatch.setattr(compactor, "_open_manifest", inspect_start)
    assert compactor.run(_args(tmp_path)) == 0


@pytest.mark.parametrize("code,expected", [(None, 0), (0, 0), (False, 0), (True, 1),
                                         (3, 3), (257, 1), (-1, 255), ("error", 1)])
def test_interruption_exit_code_matches_python_process_result(tmp_path, monkeypatch, code, expected):
    original = SystemExit(code)

    def fail(*args, **kwargs):
        raise original

    monkeypatch.setattr(compactor, "_open_manifest", fail)
    with pytest.raises(SystemExit) as observed:
        compactor.run(_args(tmp_path))
    assert observed.value is original
    receipt = _receipt(tmp_path)
    assert receipt["state"] == "interrupted"
    assert receipt["exit_code"] == expected
    assert receipt["exit_code_basis"] == "process_exit"


@pytest.mark.parametrize("outcome", ["completed", "deferred", "audit_completed", "failed"])
def test_real_producer_receipt_is_accepted_by_exact_invocation_consumer(tmp_path, monkeypatch, outcome):
    from scripts import audit_service_latency_coverage as monitor

    _publish_tasks(tmp_path, ["A", "B"])
    args = _args(tmp_path)
    if outcome == "deferred":
        args += ["--archive-idle-only"]
        monkeypatch.setattr(compactor, "_archive_compaction_allowed", lambda _: (False, "running"))
    elif outcome == "audit_completed":
        args += ["--audit-only"]
    elif outcome == "failed":
        def fail(*args, **kwargs):
            raise RuntimeError("injected manifest failure")
        monkeypatch.setattr(compactor, "_open_manifest", fail)
    if outcome == "failed":
        with pytest.raises(RuntimeError):
            compactor.run(args)
    else:
        assert compactor.run(args) == 0
    payload = _receipt(tmp_path)
    finished = datetime.fromisoformat(payload["finished_at_utc"])
    unit = "stockagent-openbb-l1-compaction.service"
    monkeypatch.setattr(monitor, "BUSINESS_RECEIPTS", {
        unit: tmp_path / "_state/l1_compaction_attempt_latest.json"
    })
    monkeypatch.setattr(monitor, "_openbb_attempt_journal", lambda *_: pytest.fail("no fallback needed"))
    receipt = monitor._business_receipt(
        unit, {
            "InvocationID": INVOCATION, "ActiveState": "inactive",
            "ExecMainStartTimestampMonotonic": int((999 - payload["elapsed_seconds"]) * 1e6),
            "ExecMainExitTimestampMonotonic": 999_000_000,
            "ExecMainStatus": payload["exit_code"],
            "Result": "exit-code" if outcome == "failed" else "success",
        }, {"observed_at": (finished + timedelta(seconds=1)).isoformat(), "monotonic": 1000.0},
    )
    assert receipt["matches_last_attempt"] is True
    assert receipt["state"] == outcome
    assert receipt["publication_stage"] == payload["publication_stage"]
    assert receipt["stage_resources"]["after_attempt"] == {
        key: payload["stage_resources"]["after_attempt"][key]
        for key in (*monitor.OPENBB_RESOURCE_COUNTERS, *monitor.OPENBB_RESOURCE_GAUGES)
    }
