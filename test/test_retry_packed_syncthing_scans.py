from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.retry_packed_syncthing_scans as retry
from stockagent.data_sync.desync_snapshots import SnapshotError


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
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
