from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

import scripts.retry_packed_syncthing_scans as retry
from stockagent.data_sync.desync_snapshots import SnapshotError


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    monkeypatch.setattr(sys, "argv", ["retry_packed_syncthing_scans.py"])
    root = tmp_path / "packed"
    pending = root / ".local-state/scan-pending"
    pending.mkdir(parents=True)
    monkeypatch.setattr(retry, "SYNC_ROOT", root)
    monkeypatch.setattr(retry, "RECEIPT_PATH", tmp_path / "latest.json")
    monkeypatch.setattr(retry, "_check_mount", lambda: None)
    monkeypatch.setattr(retry, "scan_folder_state", lambda: "idle")
    return pending, tmp_path / "latest.json"


@pytest.mark.parametrize("configured,budget", [(5, 5), (30, 30), (900, 30), (1800, 30)])
def test_backup_retry_uses_original_nonblocking_owner_and_bounded_network(
    tmp_path, monkeypatch, configured, budget,
):
    from stockagent.data_sync import backup_stream
    from scripts import configure_artifact_ingress_syncthing as api

    path = tmp_path / 'backup.json'
    original = {'scan_timeout_seconds': configured, 'retain_original_policy': True}
    path.write_text(json.dumps(original))
    before = path.read_bytes()
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)
    calls = []
    monkeypatch.setattr(api, 'request', lambda *args, **kwargs: {'state': 'idle'})

    class Queue:
        def __init__(self, config):
            assert config == {**original, 'scan_timeout_seconds': budget}

        def paired_transport(self):
            return 'test-api', 'test-key', 'test-folder'

        def notify_transport(self, *, wait_for_owner):
            calls.append(wait_for_owner)
            return {'state': 'deferred_existing_folder_scan', 'NAS_backup_verified': False}

    monkeypatch.setattr(backup_stream, 'BackupStream', Queue)
    result = retry.retry_backup_transport()
    assert calls == [False]
    assert result['state'] == 'deferred_existing_folder_scan'
    assert result['NAS_backup_verified'] is False
    assert path.read_bytes() == before, 'timer must not rewrite the enrolled backup policy'


@pytest.mark.parametrize('timeout', [True, 0, 1801, '900', None])
def test_backup_retry_rejects_invalid_config_before_owner(tmp_path, monkeypatch, timeout):
    from stockagent.data_sync import backup_stream

    path = tmp_path / 'backup.json'
    path.write_text(json.dumps({'scan_timeout_seconds': timeout}))
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)
    monkeypatch.setattr(backup_stream, 'BackupStream', lambda _: pytest.fail('invalid policy'))
    with pytest.raises(SnapshotError, match='timeout'):
        retry.retry_backup_transport()


@pytest.mark.parametrize('redirected', [False, True])
def test_backup_retry_requires_original_regular_config(tmp_path, monkeypatch, redirected):
    path = tmp_path / 'backup.json'
    if redirected:
        target = tmp_path / 'target.json'
        target.write_text('{}')
        path.symlink_to(target)
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)
    with pytest.raises(SnapshotError, match='missing or redirected'):
        retry.retry_backup_transport()


def test_backup_owner_busy_is_deferred_without_wait_or_new_writer(tmp_path, monkeypatch):
    from stockagent.data_sync import backup_stream
    from scripts import configure_artifact_ingress_syncthing as api

    path = tmp_path / 'backup.json'
    path.write_text('{}')
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)
    monkeypatch.setattr(api, 'request', lambda *args, **kwargs: {'state': 'idle'})

    class Queue:
        def __init__(self, _):
            pass

        def paired_transport(self):
            return 'test-api', 'test-key', 'test-folder'

        def notify_transport(self, *, wait_for_owner):
            assert wait_for_owner is False
            raise BlockingIOError('keep the original publication owner')

    monkeypatch.setattr(backup_stream, 'BackupStream', Queue)
    result = retry.retry_backup_transport()
    assert result == {'state': 'existing_source_owner_busy', 'NAS_backup_verified': False}


@pytest.mark.parametrize('status', [{'state': 'scanning'}, {'state': ''}, {}, None])
def test_busy_or_unknown_backup_folder_never_reads_or_writes_original_ledger(
    tmp_path, monkeypatch, status,
):
    from stockagent.data_sync import backup_stream
    from scripts import configure_artifact_ingress_syncthing as api

    path = tmp_path / 'backup.json'
    path.write_text('{"scan_timeout_seconds":900}')
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)
    calls = []

    class Queue:
        def __init__(self, _):
            pass

        def paired_transport(self):
            calls.append('validated_original_paired_scope')
            return 'test-api', 'test-key', 'test-folder'

        def notify_transport(self, **kwargs):
            pytest.fail('busy observation must not enter the ledger transaction')

    def request(base, key, endpoint, query=None, *, timeout):
        assert (base, key, endpoint, query, timeout) == (
            'test-api', 'test-key', '/rest/db/status', {'folder': 'test-folder'}, 30,
        )
        return status

    monkeypatch.setattr(backup_stream, 'BackupStream', Queue)
    monkeypatch.setattr(api, 'request', request)
    result = retry.retry_backup_transport()
    assert calls == ['validated_original_paired_scope']
    assert result['state'].startswith('deferred_')
    assert result['batch_scan_receipts'] == []
    assert result['ledger_read'] is result['ledger_written'] is result['NAS_backup_verified'] is False


def test_failed_backup_scope_validation_prevents_all_scan_requests(tmp_path, monkeypatch):
    from stockagent.data_sync import backup_stream
    from scripts import configure_artifact_ingress_syncthing as api

    path = tmp_path / 'backup.json'
    path.write_text('{}')
    monkeypatch.setattr(retry, 'BACKUP_CONFIG', path)

    class Queue:
        def __init__(self, _):
            pass

        def paired_transport(self):
            raise SnapshotError('paired scope changed')

    monkeypatch.setattr(backup_stream, 'BackupStream', Queue)
    monkeypatch.setattr(api, 'request', lambda *a, **kw: pytest.fail('unvalidated scope'))
    with pytest.raises(SnapshotError, match='scope changed'):
        retry.retry_backup_transport()


@pytest.mark.parametrize('packed_fails', [False, True])
def test_opted_in_backup_retry_runs_even_when_packed_folder_fails(
    tmp_path, monkeypatch, packed_fails,
):
    _, receipt = _setup(tmp_path, monkeypatch)
    calls = []

    def packed(*args, **kwargs):
        if packed_fails:
            raise SnapshotError('packed folder unavailable')
        return {'status': 'idle_no_pending'}

    def backup():
        calls.append('original_backup_notification')
        return {'state': 'deferred_existing_folder_scan', 'NAS_backup_verified': False}

    monkeypatch.setattr(retry, 'retry_one', packed)
    monkeypatch.setattr(retry, 'retry_backup_transport', backup)
    assert retry.main(['--backup-transport']) == int(packed_fails)
    result = json.loads(receipt.read_bytes())
    assert calls == ['original_backup_notification']
    assert result['backup_transport']['state'] == 'deferred_existing_folder_scan'
    assert result['backup_transport']['NAS_backup_verified'] is False


def test_backup_failure_keeps_packed_retry_receipt_and_has_no_false_ack(tmp_path, monkeypatch):
    _, receipt = _setup(tmp_path, monkeypatch)

    def fail():
        raise SnapshotError('untrusted API details should stay private')

    monkeypatch.setattr(retry, 'retry_backup_transport', fail)
    assert retry.main(['--backup-transport']) == 1
    result = json.loads(receipt.read_bytes())
    assert result['status'] == 'idle_no_pending'
    assert result['backup_transport'] == {
        'state': 'notification_retry_failed', 'error_type': 'SnapshotError',
        'NAS_backup_verified': False,
    }


def test_default_invocation_never_starts_backup_notification(tmp_path, monkeypatch):
    _, receipt = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(retry, 'retry_backup_transport', lambda: pytest.fail('not opted in'))
    assert retry.main([]) == 0
    assert 'backup_transport' not in json.loads(receipt.read_bytes())


@pytest.mark.parametrize('notification', [
    {'state': 'retryable_scan_error', 'error_type': 'TimeoutError'},
    {'state': 'retryable_scan_error'},
])
def test_original_notification_error_marks_service_failed_without_dropping_evidence(
    tmp_path, monkeypatch, notification,
):
    _, receipt = _setup(tmp_path, monkeypatch)
    result = {**notification, 'NAS_backup_verified': False}
    monkeypatch.setattr(retry, 'retry_backup_transport', lambda: result)
    assert retry.main(['--backup-transport']) == 1
    captured = json.loads(receipt.read_bytes())
    assert captured['status'] == 'idle_no_pending'
    assert captured['backup_transport'] == result


def test_active_folder_scan_defers_without_clearing_or_reposting(tmp_path, monkeypatch):
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    source = pending / 'dataset.json'
    raw = b'{"generation":"preserve-original"}\n'
    source.write_bytes(raw)
    monkeypatch.setattr(retry, 'scan_folder_state', lambda: 'scanning')
    monkeypatch.setattr(retry, 'scan_after_publish', lambda *a, **kw: pytest.fail('already scanning'))
    assert retry.main() == 0
    result = json.loads(receipt_path.read_bytes())
    assert result['status'] == 'deferred_existing_folder_scan'
    assert result['scan_requested'] is False
    assert result['pending_before'] == result['pending_after'] == 1
    assert result['peer_convergence'] == result['release_verification'] == 'not_checked'
    assert source.read_bytes() == raw


def test_failed_folder_observation_preserves_intent_without_speculative_scan(tmp_path, monkeypatch):
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    source = pending / 'dataset.json'
    source.write_bytes(b'preserve')
    def unavailable():
        raise SnapshotError('scan state unavailable')
    monkeypatch.setattr(retry, 'scan_folder_state', unavailable)
    monkeypatch.setattr(retry, 'scan_after_publish', lambda *a, **kw: pytest.fail('observation failed'))
    assert retry.main() == 1
    assert json.loads(receipt_path.read_bytes())['status'] == 'retry_failed'
    assert source.read_bytes() == b'preserve'


def test_idle_retry_does_not_scan_or_claim_convergence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, receipt_path = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(
        retry,
        "scan_after_publish",
        lambda *_args, **_kwargs: pytest.fail("no scan should be requested"),
    )

    assert retry.main() == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "idle_no_pending"
    assert receipt["pending_before"] == receipt["pending_after"] == 0
    assert receipt["requested_scan_policy"] == {"batch_object_paths": False}


def test_retries_only_one_existing_receipt_and_preserves_other_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    first = pending / "a-dataset.json"
    second = pending / "b-dataset.json"
    first.write_text("{}")
    second.write_text("{}")
    calls: list[tuple[Path, str, bool]] = []

    def scan(root: Path, dataset: str, *, retry_full: bool) -> bool:
        calls.append((root, dataset, retry_full))
        first.unlink()
        return True

    monkeypatch.setattr(retry, "scan_after_publish", scan)

    assert retry.main() == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert calls == [(retry.SYNC_ROOT, "a-dataset", True)]
    assert receipt["status"] == "scan_request_acknowledged"
    assert receipt["pending_before"] == 2
    assert receipt["pending_after"] == 1
    assert receipt["peer_convergence"] == "not_checked"
    assert receipt["release_verification"] == "not_checked"
    assert second.is_file()


def test_scan_failure_keeps_pending_and_records_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    source = pending / "dataset.json"
    source.write_text("{}")

    def fail(*_args, **_kwargs) -> bool:
        raise SnapshotError("Syncthing API unavailable")

    monkeypatch.setattr(retry, "scan_after_publish", fail)

    assert retry.main() == 1
    assert source.is_file()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "retry_failed"
    assert "Syncthing API unavailable" in receipt["error"]


def test_concurrent_retry_completed_same_pending_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    source = pending / "dataset.json"
    source.write_text("{}")

    def completed_elsewhere(*_args, **_kwargs) -> bool:
        source.unlink()
        raise FileNotFoundError(source)

    monkeypatch.setattr(retry, "scan_after_publish", completed_elsewhere)

    assert retry.main() == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["status"] == "concurrent_scan_completed"
    assert receipt["peer_convergence"] == "not_checked"


def test_missing_file_error_with_pending_receipt_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    source = pending / "dataset.json"
    source.write_text("{}")
    monkeypatch.setattr(
        retry,
        "scan_after_publish",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError("other file")),
    )

    assert retry.main() == 1
    assert source.is_file()
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "retry_failed"


def test_pending_symlink_is_rejected_before_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    target = tmp_path / "target.json"
    target.write_text("{}")
    (pending / "dataset.json").symlink_to(target)
    monkeypatch.setattr(
        retry,
        "scan_after_publish",
        lambda *_args, **_kwargs: pytest.fail("symlink must not be scanned"),
    )

    assert retry.main() == 1
    assert target.is_file()
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "retry_failed"


def test_mount_failure_prevents_pending_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    (pending / "dataset.json").write_text("{}")
    monkeypatch.setattr(
        retry,
        "_check_mount",
        lambda: (_ for _ in ()).throw(SnapshotError("D: unavailable")),
    )
    monkeypatch.setattr(
        retry,
        "scan_after_publish",
        lambda *_args, **_kwargs: pytest.fail("mount must be checked first"),
    )

    assert retry.main() == 1
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "retry_failed"


def test_explicit_dataset_drain_does_not_consume_other_publishers_notifications(tmp_path, monkeypatch):
    pending, _ = _setup(tmp_path, monkeypatch)
    (pending / "a-first.json").write_text("{}")
    target = pending / "bybit.json"
    target.write_text("{}")

    def scan(root, dataset, **kwargs):
        assert dataset == "bybit"
        target.unlink()
        return True

    monkeypatch.setattr(retry, "scan_after_publish", scan)
    result_path = tmp_path / "unique.json"
    assert retry.main(["--dataset", "bybit", "--receipt", str(result_path)]) == 0
    result = json.loads(result_path.read_text())
    assert result["dataset"] == "bybit"
    assert result["pending_after"] == 1
    assert (pending / "a-first.json").exists()


def test_explicit_dataset_already_drained_does_not_scan_other_pending(tmp_path, monkeypatch):
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    (pending / "other.json").write_text("{}")
    monkeypatch.setattr(retry, "scan_after_publish", lambda *_a, **_k: pytest.fail("not this publication"))
    assert retry.main(["--dataset", "bybit"]) == 0
    result = json.loads(receipt_path.read_text())
    assert result["dataset"] == "bybit"
    assert result["status"] == "idle_no_pending"
    assert result["pending_after"] == 1


def test_batch_cli_is_explicit_and_preserves_dataset_scope(tmp_path, monkeypatch):
    pending, _ = _setup(tmp_path, monkeypatch)
    target = pending / "bybit.json"
    target.write_text("{}")
    other = pending / "other.json"
    other.write_text("{}")
    calls = []

    def scan(root, dataset, **kwargs):
        calls.append((root, dataset, kwargs))
        target.unlink()
        return True

    monkeypatch.setattr(retry, "scan_after_publish", scan)
    result_path = tmp_path / "batch-result.json"
    assert retry.main([
        "--dataset", "bybit", "--receipt", str(result_path), "--batch-object-paths",
    ]) == 0
    assert calls == [(retry.SYNC_ROOT, "bybit", {"retry_full": True, "batch_object_paths": True})]
    result = json.loads(result_path.read_text())
    assert result["requested_scan_policy"] == {"batch_object_paths": True}
    assert result["status"] == "scan_request_acknowledged"
    assert result["peer_convergence"] == result["release_verification"] == "not_checked"
    assert other.is_file()


def test_batch_policy_is_not_scan_execution_when_no_pending(tmp_path, monkeypatch):
    _, receipt_path = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(retry, "scan_after_publish", lambda *a, **kw: pytest.fail("no pending"))
    assert retry.main(["--dataset", "bybit", "--batch-object-paths"]) == 0
    result = json.loads(receipt_path.read_text())
    assert result["requested_scan_policy"] == {"batch_object_paths": True}
    assert result["status"] == "idle_no_pending"
    assert result["peer_convergence"] == result["release_verification"] == "not_checked"


def test_batch_failure_records_requested_policy_and_keeps_pending(tmp_path, monkeypatch):
    pending, receipt_path = _setup(tmp_path, monkeypatch)
    target = pending / "bybit.json"
    target.write_text("{}")

    def failed(_root, _dataset, **kwargs):
        assert kwargs == {"retry_full": True, "batch_object_paths": True}
        raise SnapshotError("mock scan failed")

    monkeypatch.setattr(retry, "scan_after_publish", failed)
    assert retry.main(["--dataset", "bybit", "--batch-object-paths"]) == 1
    result = json.loads(receipt_path.read_text())
    assert result["requested_scan_policy"] == {"batch_object_paths": True}
    assert result["status"] == "retry_failed"
    assert target.is_file()


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_retry_one_rejects_non_boolean_batch_option(tmp_path, monkeypatch, value):
    _setup(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match="boolean"):
        retry.retry_one("bybit", batch_object_paths=value)
