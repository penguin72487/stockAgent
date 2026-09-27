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
    return pending, tmp_path / "latest.json"


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
