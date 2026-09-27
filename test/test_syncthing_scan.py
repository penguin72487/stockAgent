from __future__ import annotations

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace
import urllib.parse
import json

import pytest

import stockagent.data_sync.syncthing_scan as scan
from stockagent.data_sync.desync_snapshots import SnapshotError


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


@pytest.fixture
def queue_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    return packed


def test_queue_is_durable_without_network_and_merges_old_schema(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    pending.write_text(json.dumps({
        "schema_version": 1, "dataset": "dataset",
        "new_object_paths": ["objects/blobs/old.blob"],
    }))
    monkeypatch.setattr(scan, "_scan_pending", lambda *a, **kw: pytest.fail("network called"))
    monkeypatch.setattr(scan.subprocess, "run", lambda *a, **kw: pytest.fail("subprocess called"))
    assert scan.queue_after_publish(
        queue_root, "dataset", new_object_paths=("objects/blobs/new.blob",),
    ) is True
    receipt = json.loads(pending.read_text())
    assert receipt["new_object_paths"] == ["objects/blobs/new.blob", "objects/blobs/old.blob"]
    assert receipt["full_objects_scan"] is False
    assert len(receipt["generation"]) == 32


@pytest.mark.parametrize("same_paths", [False, True])
def test_queue_during_blocked_scan_is_prompt_and_new_generation_stays_pending(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch, same_paths: bool,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    first = "objects/blobs/old.blob"
    second = first if same_paths else "objects/blobs/new.blob"
    scan.queue_after_publish(queue_root, "dataset", new_object_paths=(first,))
    before = json.loads(pending.read_text())["generation"]
    started, release = threading.Event(), threading.Event()
    observed = []

    def blocked(_root, _dataset, paths, *, full_objects_scan):
        observed.append(paths)
        started.set()
        assert release.wait(timeout=5), "test did not release simulated network scan"

    monkeypatch.setattr(scan, "_scan_pending", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        scanning = pool.submit(scan.scan_after_publish, queue_root, "dataset", retry_full=True)
        try:
            assert started.wait(timeout=2)
            queuing = pool.submit(
                scan.queue_after_publish, queue_root, "dataset", new_object_paths=(second,),
            )
            # The network remains blocked; only the short queue lock is needed.
            assert queuing.result(timeout=1) is True
            assert json.loads(pending.read_text())["generation"] != before
        finally:
            release.set()
        assert scanning.result(timeout=2) is False
    assert pending.is_file()
    assert observed == [(first,)]
    assert scan.scan_after_publish(queue_root, "dataset", retry_full=True) is True
    assert observed[-1] == tuple(sorted({first, second}))
    assert not pending.exists()


def test_old_schema_replacement_during_scan_is_not_deleted(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    legacy = {
        "schema_version": 1, "dataset": "dataset",
        "new_object_paths": ["objects/blobs/old.blob"],
    }
    scan.atomic_write_json(pending, legacy)

    def replaced(*_args, **_kwargs):
        # Old publishers have no generation token; inode/ctime still fence ABA.
        scan.atomic_write_json(pending, legacy)

    monkeypatch.setattr(scan, "_scan_pending", replaced)
    assert scan.scan_after_publish(queue_root, "dataset", retry_full=True) is False
    assert pending.is_file()


def test_scan_failure_keeps_both_original_and_concurrent_queue(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"

    def fail_after_queue(*_args, **_kwargs):
        scan.queue_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/new.blob",))
        raise SnapshotError("network failed")

    monkeypatch.setattr(scan, "_scan_pending", fail_after_queue)
    with pytest.raises(SnapshotError, match="network failed"):
        scan.scan_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/old.blob",))
    assert json.loads(pending.read_text())["new_object_paths"] == [
        "objects/blobs/new.blob", "objects/blobs/old.blob",
    ]


@pytest.mark.parametrize("contents", ["{", "{}", json.dumps({
    "schema_version": 1, "dataset": "dataset", "new_object_paths": ["../outside"],
})])
def test_invalid_pending_queue_preserves_full_objects_retry(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch, contents: str,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    pending.write_text(contents)
    scan.queue_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/new.blob",))
    assert json.loads(pending.read_text())["full_objects_scan"] is True
    observed = []
    monkeypatch.setattr(scan, "_scan_pending", lambda *a, **kw: observed.append(kw))
    assert scan.scan_after_publish(queue_root, "dataset", retry_full=True)
    assert observed == [{"full_objects_scan": True}]


def test_queue_atomic_write_failure_never_reports_success(
    queue_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    scan.queue_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/old.blob",))
    before = pending.read_bytes()

    def failed(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(scan, "atomic_write_json", failed)
    with pytest.raises(OSError, match="disk full"):
        scan.queue_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/new.blob",))
    assert pending.read_bytes() == before


@pytest.mark.parametrize("paths", [("../outside",), ("/outside",), ("",), (None,)])
def test_queue_rejects_unsafe_paths(queue_root: Path, paths) -> None:
    with pytest.raises(SnapshotError, match="unsafe"):
        scan.queue_after_publish(queue_root, "dataset", new_object_paths=paths)


def test_queue_rejects_redirected_pending(queue_root: Path, tmp_path: Path) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    target = tmp_path / "target.json"
    target.write_text("keep")
    pending.symlink_to(target)
    with pytest.raises(SnapshotError, match="symlink"):
        scan.queue_after_publish(queue_root, "dataset")
    assert target.read_text() == "keep"


def test_d_primary_publish_scans_objects_before_manifest_and_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    assert scan.scan_after_publish(packed, "dataset") is False
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    config = tmp_path / "config.xml"
    config.write_text(
        '<configuration><folder id="stockagent-packed" path="'
        + str(packed)
        + '" paused="false"/><gui><address>127.0.0.1:8384</address>'
        "<apikey>test-key</apikey></gui></configuration>"
    )
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "CONFIGS", (config,))
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""),
    )
    scanned: list[str] = []

    def urlopen(request, *, timeout):
        assert timeout == 120
        assert request.get_header("X-api-key") == "test-key"
        scanned.append(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["sub"][
                0
            ]
        )
        return _Response()

    monkeypatch.setattr(scan.urllib.request, "urlopen", urlopen)
    assert scan.scan_after_publish(
        packed, "dataset", new_object_paths=("objects/blobs/aa/a.blob",)
    )
    assert scanned == ["objects/blobs/aa/a.blob", "manifests/dataset", "heads/dataset"]
    scanned.clear()
    assert scan.scan_after_publish(packed, "dataset", retry_full=True) is False
    pending = packed / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}")
    assert scan.scan_after_publish(packed, "dataset", retry_full=True)
    assert scanned == ["objects", "manifests/dataset", "heads/dataset"]
    assert not pending.exists()

    monkeypatch.setattr(
        scan.urllib.request,
        "urlopen",
        lambda request, *, timeout: (_ for _ in ()).throw(OSError("API unavailable")),
    )
    with pytest.raises(SnapshotError, match="API unavailable"):
        scan.scan_after_publish(
            packed, "dataset", new_object_paths=("objects/packs/a.zip",)
        )
    assert pending.is_file()


def test_d_primary_retry_uses_recorded_paths_and_merges_later_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    config = tmp_path / "config.xml"
    config.write_text(
        '<configuration><folder id="stockagent-packed" path="'
        + str(packed)
        + '" paused="false"/><gui><address>127.0.0.1:8384</address>'
        '<apikey>test-key</apikey></gui></configuration>'
    )
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "CONFIGS", (config,))
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""),
    )
    pending = packed / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    pending.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "dataset",
                "new_object_paths": ["objects/blobs/aa/old.blob"],
            }
        )
    )
    scanned: list[str] = []

    def urlopen(request, *, timeout):
        assert timeout == 120
        scanned.append(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["sub"][
                0
            ]
        )
        return _Response()

    monkeypatch.setattr(scan.urllib.request, "urlopen", urlopen)
    assert scan.scan_after_publish(
        packed, "dataset", new_object_paths=("objects/blobs/bb/new.blob",)
    )
    assert scanned == [
        "objects/blobs/aa/old.blob",
        "objects/blobs/bb/new.blob",
        "manifests/dataset",
        "heads/dataset",
    ]
    assert not pending.exists()
    pending.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "dataset",
                "new_object_paths": ["objects/blobs/cc/retry.blob"],
            }
        )
    )
    scanned.clear()
    assert scan.scan_after_publish(packed, "dataset", retry_full=True)
    assert scanned == [
        "objects/blobs/cc/retry.blob",
        "manifests/dataset",
        "heads/dataset",
    ]
    assert not pending.exists()


def test_d_primary_scan_fails_closed_when_mount_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=2, stderr="D missing"),
    )
    with pytest.raises(SnapshotError, match="D missing"):
        scan.scan_after_publish(packed, "dataset")


@pytest.fixture
def scan_api(queue_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = tmp_path / "batch-config.xml"
    config.write_text(
        f'<configuration><folder id="stockagent-packed" path="{queue_root}" '
        'paused="false"/><gui><address>127.0.0.1:8384</address>'
        '<apikey>test-only-secret</apikey></gui></configuration>'
    )
    monkeypatch.setattr(scan, "CONFIGS", (config,))
    monkeypatch.setattr(scan.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stderr=""))
    state = {"calls": [], "urls": [], "fail_at": None, "http_error": False, "callback": None}

    def urlopen(request, *, timeout):
        assert timeout == 120
        assert request.method == "POST"
        assert request.get_header("X-api-key") == "test-only-secret"
        parsed = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        assert set(parsed) == {"folder", "sub"}
        assert parsed["folder"] == ["stockagent-packed"]
        state["calls"].append(parsed["sub"])
        state["urls"].append(request.full_url)
        if state["callback"] is not None:
            state["callback"]()
        if state["fail_at"] == len(state["calls"]):
            if state["http_error"]:
                response = _Response()
                response.status = 500
                return response
            raise OSError("mock API unavailable")
        return _Response()

    monkeypatch.setattr(scan.urllib.request, "urlopen", urlopen)
    return state


def test_batch_candidate_exact_coverage_three_ordered_phases_and_request_cost(
    queue_root: Path, scan_api,
) -> None:
    # Frozen realistic shape: 18 payload objects + inventory + archived head.
    # This measures request count, not real network wall time or peer delivery.
    paths = tuple(f"objects/blobs/aa/{index:02d}.blob" for index in range(18)) + (
        "objects/inventories/aa/inventory.jsonl.gz",
        "head-history/heads/dataset/node/previous.json",
    )
    assert scan.scan_after_publish(queue_root, "dataset", new_object_paths=paths)
    legacy = list(scan_api["calls"])
    assert len(legacy) == 22
    scan_api["calls"].clear()
    scan_api["urls"].clear()
    assert scan.scan_after_publish(
        queue_root, "dataset", new_object_paths=paths, batch_object_paths=True,
    )
    assert scan_api["calls"] == [sorted(paths), ["manifests/dataset"], ["heads/dataset"]]
    assert [path for group in legacy for path in group] == [
        path for group in scan_api["calls"] for path in group
    ]
    assert len(scan_api["calls"]) == 3  # 19 fewer requests; no elapsed-time claim.


@pytest.mark.parametrize("count", [0, 1, 64, 65, 256, 257])
def test_batch_many_paths_are_bounded_and_keep_full_objects_fallback(
    queue_root: Path, scan_api, count: int,
) -> None:
    paths = tuple(f"objects/blobs/{index:04d}.blob" for index in range(count))
    assert scan.scan_after_publish(queue_root, "dataset", new_object_paths=paths, batch_object_paths=True)
    calls = scan_api["calls"]
    assert calls[-2:] == [["manifests/dataset"], ["heads/dataset"]]
    if count > 256:
        assert calls == [["objects"], ["manifests/dataset"], ["heads/dataset"]]
    else:
        assert [path for group in calls[:-2] for path in group] == sorted(paths)
        assert all(1 <= len(group) <= scan.MAX_BATCH_SUB_PATHS for group in calls[:-2])
        assert len(calls) == (count + 63) // 64 + 2


def test_batch_query_bytes_bound_and_encoding_preserve_exact_sub_values(
    queue_root: Path, scan_api,
) -> None:
    paths = tuple(f"objects/blobs/{index:03d}-" + "價" * 60 for index in range(65)) + (
        "objects/packs/a +%/b&next=123#?.zip",
    )
    assert scan.scan_after_publish(queue_root, "dataset", new_object_paths=paths, batch_object_paths=True)
    assert [path for group in scan_api["calls"][:-2] for path in group] == sorted(paths)
    assert len(scan_api["calls"]) > 3  # Encoded-byte bound, not just path count.
    assert all(
        len(urllib.parse.urlsplit(url).query.encode("ascii")) <= scan.MAX_BATCH_QUERY_BYTES
        for url in scan_api["urls"]
    )
    assert all("test-only-secret" not in url for url in scan_api["urls"])


@pytest.mark.parametrize("fail_at", [1, 2, 3])
@pytest.mark.parametrize("http_error", [False, True])
def test_batch_error_stops_later_phases_and_retains_pending(
    queue_root: Path, scan_api, fail_at: int, http_error: bool, capsys,
) -> None:
    paths = ("objects/blobs/a.blob", "objects/blobs/b.blob")
    scan_api.update(fail_at=fail_at, http_error=http_error)
    with pytest.raises(SnapshotError):
        scan.scan_after_publish(queue_root, "dataset", new_object_paths=paths, batch_object_paths=True)
    assert scan_api["calls"] == [list(paths), ["manifests/dataset"], ["heads/dataset"]][:fail_at]
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    assert pending.is_file()
    assert json.loads(pending.read_text())["new_object_paths"] == list(paths)
    assert "test-only-secret" not in str(capsys.readouterr())


def test_batch_failed_middle_object_chunk_never_scans_manifest_or_head(queue_root: Path, scan_api) -> None:
    paths = tuple(f"objects/blobs/{index:04d}.blob" for index in range(130))
    scan_api["fail_at"] = 2
    with pytest.raises(SnapshotError):
        scan.scan_after_publish(queue_root, "dataset", new_object_paths=paths, batch_object_paths=True)
    assert scan_api["calls"] == [list(paths[:64]), list(paths[64:128])]
    assert (queue_root / ".local-state/scan-pending/dataset.json").is_file()


def test_batch_retry_bad_receipt_retains_full_objects_fallback(queue_root: Path, scan_api) -> None:
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    pending.write_text("{}")
    assert scan.scan_after_publish(queue_root, "dataset", retry_full=True, batch_object_paths=True)
    assert scan_api["calls"] == [["objects"], ["manifests/dataset"], ["heads/dataset"]]
    assert not pending.exists()


def test_batch_scan_cas_keeps_newly_queued_generation(queue_root: Path, scan_api) -> None:
    paths = ("objects/blobs/a.blob", "objects/blobs/b.blob")

    def enqueue_once():
        scan_api["callback"] = None
        scan.queue_after_publish(queue_root, "dataset", new_object_paths=("objects/blobs/new.blob",))

    scan_api["callback"] = enqueue_once
    assert scan.scan_after_publish(
        queue_root, "dataset", new_object_paths=paths, batch_object_paths=True,
    ) is False
    pending = queue_root / ".local-state/scan-pending/dataset.json"
    assert json.loads(pending.read_text())["new_object_paths"] == [*paths, "objects/blobs/new.blob"]
    assert scan_api["calls"] == [list(paths), ["manifests/dataset"], ["heads/dataset"]]


def test_batch_oversized_single_query_fails_before_any_request(queue_root: Path, scan_api) -> None:
    with pytest.raises(SnapshotError, match="query limit"):
        scan.scan_after_publish(
            queue_root, "dataset", new_object_paths=("objects/" + "價" * 2000,), batch_object_paths=True,
        )
    assert not scan_api["calls"]
    assert (queue_root / ".local-state/scan-pending/dataset.json").is_file()


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_batch_option_requires_explicit_boolean(queue_root: Path, value) -> None:
    with pytest.raises(SnapshotError, match="boolean"):
        scan.scan_after_publish(queue_root, "dataset", batch_object_paths=value)
