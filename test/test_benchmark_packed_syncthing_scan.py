from __future__ import annotations

import fcntl
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import benchmark_packed_syncthing_scan as bench


def _write_json(path: Path, value: dict) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(value, sort_keys=True).encode()
    path.write_bytes(content)
    return content


@pytest.fixture
def released(tmp_path: Path, monkeypatch):
    root = tmp_path / "packed"
    root.mkdir()
    (root / ".stockagent-d-primary").write_text("{}")
    monkeypatch.setattr(bench, "SYNC_ROOT", root)
    monkeypatch.setattr(bench.scan, "CANONICAL_ROOT", root)
    objects = []
    for index in range(20):
        content = f"object-{index}".encode()
        digest = hashlib.sha256(content).hexdigest()
        relative = f"objects/blobs/{digest[:2]}/{digest}.blob"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        objects.append({"kind": "blob", "sha256": digest, "bytes": len(content), "relpath": relative})
    content = b"inventory fixture: not byte-verified by benchmark"
    digest = hashlib.sha256(content).hexdigest()
    inventory = {"sha256": digest, "bytes": len(content), "relpath": f"objects/inventories/{digest[:2]}/{digest}.jsonl.gz"}
    inventory_path = root / inventory["relpath"]
    inventory_path.parent.mkdir(parents=True)
    inventory_path.write_bytes(content)
    snapshot = "bybit-test-penguin"
    hlc = {"physical_ns": 1, "logical": 0, "node_id": "penguin"}
    manifest = {
        "schema_version": 1, "dataset": "bybit", "snapshot_id": snapshot,
        "publisher": {"node_id": "penguin"}, "hlc": hlc,
        "source": {"portable_fingerprint_sha256": "0" * 64},
        "archive": {"format": "stockagent-path-bucket-zip-v1", "objects": objects, "inventory": inventory},
    }
    manifest_relative = f"manifests/bybit/{snapshot}.json"
    manifest_bytes = _write_json(root / manifest_relative, manifest)
    head = {"schema_version": 1, "dataset": "bybit", "node_id": "penguin", "snapshot_id": snapshot,
            "hlc": hlc, "manifest_relpath": manifest_relative,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest()}
    head_bytes = _write_json(root / bench.HEAD_PATH, head)
    config = tmp_path / "config.xml"
    config.write_text(f'<configuration><folder id="stockagent-packed" path="{root}"/>'
                      '<gui><address>127.0.0.1:8384</address><apikey>SECRET-NEVER-RECORD</apikey></gui></configuration>')
    monkeypatch.setattr(bench.scan, "CONFIGS", (config,))
    monkeypatch.setattr(bench.scan.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stderr=""))
    state = {"root": root, "head_sha": hashlib.sha256(head_bytes).hexdigest(), "manifest": manifest,
             "manifest_path": root / manifest_relative, "head": head, "calls": [], "callback": None,
             "fail_at": None, "http_status": 200}

    class Response:
        def __init__(self, status):
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    def urlopen(request, *, timeout):
        assert 0 < timeout <= 90
        assert request.get_header("X-api-key") == "SECRET-NEVER-RECORD"
        state["calls"].append(request.full_url)
        if state["callback"]:
            state["callback"]()
        if state["fail_at"] == len(state["calls"]):
            raise OSError("SECRET-NEVER-RECORD")
        return Response(state["http_status"])

    monkeypatch.setattr(bench.scan.urllib.request, "urlopen", urlopen)
    return state


def test_default_dry_run_never_sends_api_or_reads_payload_bytes(released, tmp_path, monkeypatch):
    original = Path.read_bytes

    def metadata_only(path):
        if path.is_relative_to(released["root"] / "objects"):
            pytest.fail("payload bytes must not be read")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", metadata_only)
    receipt = tmp_path / "dry.json"
    result = bench.benchmark(head_sha256=released["head_sha"], receipt_path=receipt)
    assert result["state"] == "dry_run"
    assert result["expected_post_counts"] == {"single": 21, "batch": 3}
    assert len(result["release"]["paths"]) == 19
    assert result["trials"] == [] and released["calls"] == []
    assert result["release_verification"] == result["payload_byte_verification"] == "not_performed"
    assert not (released["root"] / ".local-state/locks/scan-bybit.lock").exists()


def test_execute_runs_exact_abba_canonical_requests_without_queue(released, tmp_path):
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "execute.json")
    assert result["state"] == "completed"
    assert [row["policy"] for row in result["trials"]] == ["single", "batch", "batch", "single"]
    assert [row["observed_post_count"] for row in result["trials"]] == [21, 3, 3, 21]
    paths = sorted(result["release"]["paths"])
    for trial in result["trials"]:
        assert trial["state"] == "completed"
        assert [row["phase"] for row in trial["requests"][-2:]] == ["manifest", "head"]
        assert [path for row in trial["requests"][:-2] for path in row["paths"]] == paths
        assert all(row["status"] == 200 and row["elapsed_seconds"] >= 0 for row in trial["requests"])
        assert trial["measured_request_seconds"] == pytest.approx(
            sum(row["elapsed_seconds"] for row in trial["requests"]), abs=0.000001,
        )
        assert trial["input_metadata_unchanged"] is True
    assert set(result["median_trial_seconds"]) == {"single", "batch"}
    assert set(result["median_request_ack_seconds"]) == {"single", "batch"}
    assert result["request_elapsed_scope"] == "urlopen_until_response_headers"
    assert result["code_sha256_before"] == result["code_sha256_after"]
    assert not (released["root"] / ".local-state/scan-pending/bybit.json").exists()
    assert "SECRET-NEVER-RECORD" not in json.dumps(result)
    assert '"headers":' not in json.dumps(result)


def test_pending_at_start_refuses_without_clearing(released, tmp_path):
    pending = released["root"] / ".local-state/scan-pending/bybit.json"
    pending.parent.mkdir(parents=True)
    pending.write_text("preserve")
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "pending.json")
    assert result["state"] == "contaminated"
    assert result["trials"] == [] and not released["calls"]
    assert pending.read_text() == "preserve"


def test_concurrent_pending_stops_next_request_and_keeps_queue(released, tmp_path):
    pending = released["root"] / ".local-state/scan-pending/bybit.json"

    def enqueue():
        released["callback"] = None
        pending.parent.mkdir(parents=True)
        pending.write_text("preserve concurrent writer")

    released["callback"] = enqueue
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "concurrent.json")
    assert result["state"] == "contaminated"
    assert len(result["trials"]) == 1 and len(released["calls"]) == 1
    assert pending.read_text() == "preserve concurrent writer"
    assert "median_trial_seconds" not in result
    assert "median_request_ack_seconds" not in result


@pytest.mark.parametrize("target", ["head", "manifest", "object", "code"])
def test_changed_inputs_or_implementation_contaminate_run(released, tmp_path, monkeypatch, target):
    def change_once():
        released["callback"] = None
        if target == "code":
            monkeypatch.setattr(bench, "_code_hashes", lambda: {"changed": "1" * 64})
        else:
            path = (released["root"] / bench.HEAD_PATH if target == "head" else
                    released["manifest_path"] if target == "manifest" else
                    released["root"] / released["manifest"]["archive"]["objects"][0]["relpath"])
            path.write_bytes(path.read_bytes() + b" ")

    released["callback"] = change_once
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / f"change-{target}.json")
    assert result["state"] == "contaminated"
    assert len(result["trials"]) == 1
    assert "median_trial_seconds" not in result


def test_wrong_head_pin_and_manifest_digest_are_not_accepted(released, tmp_path):
    result = bench.benchmark(head_sha256="f" * 64, execute=True, receipt_path=tmp_path / "bad-head.json")
    assert result["state"] == "refused" and not released["calls"]
    released["manifest_path"].write_bytes(released["manifest_path"].read_bytes() + b" ")
    result = bench.benchmark(head_sha256=released["head_sha"], receipt_path=tmp_path / "bad-manifest.json")
    assert result["state"] == "refused"


@pytest.mark.parametrize("problem", ["wrong_size", "symlink", "directory"])
def test_selected_payload_requires_safe_regular_size(released, tmp_path, problem):
    path = released["root"] / released["manifest"]["archive"]["objects"][0]["relpath"]
    if problem == "wrong_size":
        path.write_bytes(b"unexpected size")
    else:
        path.unlink()
        if problem == "symlink":
            target = tmp_path / "target"
            target.write_bytes(b"object-0")
            path.symlink_to(target)
        else:
            path.mkdir()
    result = bench.benchmark(head_sha256=released["head_sha"], receipt_path=tmp_path / "bad-object.json")
    assert result["state"] == "refused" and not released["calls"]


def test_busy_canonical_scan_lock_refuses_promptly(released, tmp_path):
    lock = released["root"] / ".local-state/locks/scan-bybit.lock"
    lock.parent.mkdir(parents=True)
    with lock.open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "busy.json")
    assert result["state"] == "refused" and not released["calls"]


def test_api_error_retains_safe_partial_receipt_and_no_median(released, tmp_path):
    released["fail_at"] = 2
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "failed.json")
    assert result["state"] == "failed"
    assert len(result["trials"]) == 1 and result["trials"][0]["observed_post_count"] == 2
    assert result["trials"][0]["requests"][-1]["error_type"] == "OSError"
    assert "median_trial_seconds" not in result
    assert "SECRET-NEVER-RECORD" not in json.dumps(result)


def test_non_200_status_never_produces_completed_trial(released, tmp_path):
    released["http_status"] = 503
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "http.json")
    assert result["state"] == "failed"
    assert result["trials"][0]["requests"][0]["status"] == 503


def test_canonical_request_plan_deviation_is_rejected(released, tmp_path, monkeypatch):
    def wrong_scan(*args, **kwargs):
        request = bench.scan.urllib.request.Request("http://127.0.0.1:8384/rest/db/scan?folder=stockagent-packed&sub=heads/bybit", method="POST")
        bench.scan.urllib.request.urlopen(request, timeout=120)

    monkeypatch.setattr(bench.scan, "_scan_pending", wrong_scan)
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, receipt_path=tmp_path / "wrong-plan.json")
    assert result["state"] == "failed" and not released["calls"]


def test_deadline_stops_trial_and_restores_alarm(released, tmp_path, monkeypatch):
    import time

    def stalled(*args, **kwargs):
        time.sleep(2)

    monkeypatch.setattr(bench.scan, "_scan_pending", stalled)
    before = bench.signal.getsignal(bench.signal.SIGALRM)
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, timeout_seconds=1,
                             receipt_path=tmp_path / "timeout.json")
    assert result["state"] == "failed"
    assert result["trials"][0]["error_type"] == "TimeoutError"
    assert result["trials"][0]["elapsed_seconds"] < 1.8
    assert bench.signal.getitimer(bench.signal.ITIMER_REAL)[0] == 0
    assert bench.signal.getsignal(bench.signal.SIGALRM) == before


def test_cli_is_dry_by_default_and_pin_is_required(released, tmp_path):
    with pytest.raises(SystemExit):
        bench.main(["--receipt", str(tmp_path / "required.json")])
    path = tmp_path / "cli.json"
    assert bench.main(["--head-sha256", released["head_sha"], "--receipt", str(path)]) == 0
    assert json.loads(path.read_text())["state"] == "dry_run"
    assert not released["calls"]


@pytest.mark.parametrize("kwargs", [{"object_limit": 19}, {"object_limit": True}, {"timeout_seconds": 0},
                                  {"timeout_seconds": float("nan")}, {"timeout_seconds": 121}])
def test_resource_bounds_are_checked_before_work(released, tmp_path, kwargs):
    with pytest.raises(ValueError):
        bench.benchmark(head_sha256=released["head_sha"], receipt_path=tmp_path / "bounds.json", **kwargs)
    assert not released["calls"]


def test_receipt_cannot_overwrite_existing_or_enter_cold(released, tmp_path):
    existing = tmp_path / "existing.json"
    existing.write_text("preserve")
    for path in (existing, released["root"] / "new-receipt.json"):
        with pytest.raises(ValueError):
            bench.benchmark(head_sha256=released["head_sha"], receipt_path=path)
    assert existing.read_text() == "preserve"


def test_nested_deadline_restores_only_remaining_outer_budget():
    import time

    previous = bench.signal.getsignal(bench.signal.SIGALRM)
    with bench._hard_deadline(1.0, terminal=True):
        before = bench.signal.getitimer(bench.signal.ITIMER_REAL)[0]
        with bench._hard_deadline(0.5):
            time.sleep(0.04)
        after = bench.signal.getitimer(bench.signal.ITIMER_REAL)[0]
        assert 0 < after <= before - 0.03
    assert bench.signal.getitimer(bench.signal.ITIMER_REAL)[0] == 0
    assert bench.signal.getsignal(bench.signal.SIGALRM) == previous


def test_earlier_outer_deadline_bypasses_inner_exception_handler():
    import time

    with pytest.raises(bench.BenchmarkBudgetExceeded):
        with bench._hard_deadline(0.04, terminal=True):
            try:
                with bench._hard_deadline(1.0):
                    time.sleep(0.2)
            except Exception:
                pytest.fail("outer deadline must not enter inner recovery")
    assert bench.signal.getitimer(bench.signal.ITIMER_REAL)[0] == 0


@pytest.mark.parametrize("stage", ["locked_initial_validation", "failure_validation", "locked_final_validation", "receipt_write"])
def test_global_budget_covers_all_lock_held_work_and_releases_lock(
    released, tmp_path, monkeypatch, stage,
):
    import time

    monkeypatch.setattr(bench, "_total_budget_seconds", lambda _: 0.15)
    original_check = bench._assert_frozen
    checks = 0

    def slow_check(*args):
        nonlocal checks
        checks += 1
        block_at = {"locked_initial_validation": 2, "failure_validation": 4, "locked_final_validation": 3}.get(stage)
        if checks == block_at:
            time.sleep(1)
        return original_check(*args)

    monkeypatch.setattr(bench, "_assert_frozen", slow_check)
    if stage == "failure_validation":
        def failed_scan(*args, **kwargs):
            raise bench.snapshots.SnapshotError("mock scan failed")
        monkeypatch.setattr(bench.scan, "_scan_pending", failed_scan)
    if stage == "locked_final_validation":
        monkeypatch.setattr(bench, "_run_trial", lambda *a, **kw: {
            "state": "completed", "policy": kw["policy"], "elapsed_seconds": 0.01,
            "measured_request_seconds": 0.005,
        })
    if stage == "receipt_write":
        original_write = bench.snapshots.atomic_write_json
        writes = 0

        def slow_write(*args):
            nonlocal writes
            writes += 1
            if writes == 1:
                time.sleep(1)
            return original_write(*args)

        monkeypatch.setattr(bench.snapshots, "atomic_write_json", slow_write)
    path = tmp_path / f"global-{stage}.json"
    started = time.monotonic()
    result = bench.benchmark(head_sha256=released["head_sha"], execute=True, timeout_seconds=1, receipt_path=path)
    assert time.monotonic() - started < 0.9
    assert result["state"] == "timed_out"
    assert result["global_budget_seconds"] == 0.15
    assert result["error_type"] == "BenchmarkBudgetExceeded"
    assert "median_trial_seconds" not in result and "median_request_ack_seconds" not in result
    assert json.loads(path.read_text())["state"] == "timed_out"
    # A distinct open file description must acquire the exact canonical lock.
    with (released["root"] / ".local-state/locks/scan-bybit.lock").open("a+b") as after:
        fcntl.flock(after, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert bench.signal.getitimer(bench.signal.ITIMER_REAL)[0] == 0
