from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import threading

import pytest

from stockagent.data_sync import completed_forward_capacity as capacity


KEY = hashlib.sha256(b"request-a").hexdigest()
OTHER_KEY = hashlib.sha256(b"request-b").hexdigest()
GROWTH = 1024**2
CAP = 64 * 1024**2


def _reserve(root, **overrides):
    options = dict(key=KEY, maximum_growth_bytes=GROWTH, capacity_bytes=CAP)
    options.update(overrides)
    return capacity.reserve_capacity(root, **options)


def _record_paths(root):
    return sorted(p for p in root.iterdir() if capacity._LEASE.fullmatch(p.name))


def test_physical_headroom_defers_before_any_model_or_payload_write(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(capacity.os, "statvfs", lambda _root: SimpleNamespace(f_bavail=1, f_frsize=4096))
    with pytest.raises(capacity.CapacityDeferred) as error:
        with _reserve(tmp_path, minimum_free_bytes=4096):
            pytest.fail("no allocation should be admitted")
    assert error.value.details["reason"] == "filesystem_headroom_exhausted"
    assert not _record_paths(tmp_path)


@pytest.mark.parametrize("value", [-1, True, 1.0, 2**63])
def test_physical_headroom_requires_an_integer_bound(tmp_path, value):
    with pytest.raises(ValueError, match="minimum_free_bytes"):
        with _reserve(tmp_path, minimum_free_bytes=value):
            pytest.fail("invalid headroom cannot reserve")


def _record(root, **overrides):
    info = root.stat()
    token = "0" * 32
    value = dict(contract=capacity.CONTRACT, key=KEY, token=token, capacity_bytes=CAP,
                 maximum_growth_bytes=GROWTH, control_bytes=capacity.CONTROL_HEADROOM_BYTES,
                 root_device=info.st_dev, root_inode=info.st_ino)
    value.update(overrides)
    path = root / f".{KEY}.capacity-{token}.json"
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return path


def test_reservation_is_live_until_commit_and_status_never_reaps(tmp_path):
    payload = tmp_path / f"{KEY}.forward"
    payload.write_bytes(b"existing opaque tensor bytes")
    before = payload.stat()
    with _reserve(tmp_path) as result:
        assert result["state"] == "reserved"
        observed = capacity.capacity_status(tmp_path)
        assert observed["active_reservations"][0]["key"] == KEY
        assert observed["reserved_growth_bytes"] == GROWTH + capacity.CONTROL_HEADROOM_BYTES
        assert observed["stale_reaped"] == observed["data_paths_removed"] == 0
        assert len(_record_paths(tmp_path)) == 1
    assert result["state"] == "released" and not _record_paths(tmp_path)
    assert payload.stat() == before
    assert payload.read_bytes() == b"existing opaque tensor bytes"
    assert not capacity.capacity_status(tmp_path)["active_reservations"]


def test_count_unique_allocations_including_directory_and_unknown_regular_bytes(tmp_path, monkeypatch):
    payload = tmp_path / "unknown-original"
    payload.write_bytes(b"\x01" * 16384)
    alias = tmp_path / "other-name"
    os.link(payload, alias)
    outside = tmp_path.parent / (tmp_path.name + "-external-alias")
    os.link(payload, outside)
    original = os.read

    def read(descriptor, count):
        assert (os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino) != (payload.stat().st_dev, payload.stat().st_ino)
        return original(descriptor, count)

    monkeypatch.setattr(os, "read", read)
    try:
        observed = capacity.capacity_status(tmp_path)
        expected = { (p.stat().st_dev, p.stat().st_ino): p.stat().st_blocks * 512 for p in (*tmp_path.iterdir(), tmp_path) }
        assert observed["namespace_allocated_bytes"] == sum(expected.values())
        assert observed["physical_inodes"] == len(expected) == 3
        assert observed["payload_bytes_read"] == observed["data_paths_removed"] == 0
    finally:
        outside.unlink()


def test_existing_bytes_and_reservations_refuse_before_compute(tmp_path):
    original = tmp_path / "original"
    original.write_bytes(b"\x01" * 4096)
    before = original.stat()
    observed = capacity.capacity_status(tmp_path)
    cap = observed["namespace_allocated_bytes"] + GROWTH + capacity.CONTROL_HEADROOM_BYTES
    with _reserve(tmp_path, capacity_bytes=cap):
        with pytest.raises(capacity.CapacityDeferred, match="capacity_exhausted") as error:
            with _reserve(tmp_path, key=OTHER_KEY, capacity_bytes=cap):
                pytest.fail("expensive model callback must not run")
        assert error.value.details["reserved_growth_bytes"] > 0
        assert error.value.details["allocation_admitted"] is False
        assert len(_record_paths(tmp_path)) == 1
    assert original.stat() == before
    with _reserve(tmp_path, key=OTHER_KEY, capacity_bytes=cap) as result:
        assert result["state"] == "reserved"


def test_exception_releases_lease_without_overwriting_original_error(tmp_path):
    with pytest.raises(RuntimeError, match="original compute failed"):
        with _reserve(tmp_path):
            raise RuntimeError("original compute failed")
    assert not _record_paths(tmp_path)
    with _reserve(tmp_path):
        pass


@pytest.mark.parametrize("exit_mode", ["normal", "crash"])
def test_independent_process_lease_and_crash_recovery(tmp_path, exit_mode):
    code = """
import json,os,sys
from pathlib import Path
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
with reserve_capacity(Path(sys.argv[1]),key=sys.argv[2],maximum_growth_bytes=1024**2,capacity_bytes=64*1024**2):
    print(json.dumps({'state':'lease-held'}),flush=True)
    sys.stdin.readline()
    if sys.argv[3]=='crash': os._exit(17)
"""
    child = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), KEY, exit_mode],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([child.stdout], [], [], 15)[0], "child never acquired its reservation"
        assert json.loads(child.stdout.readline())["state"] == "lease-held"
        assert child.poll() is None
        status = capacity.capacity_status(tmp_path)
        assert len(status["active_reservations"]) == 1 and not status["stale_reservations"]
        child.stdin.write("release\n")
        child.stdin.flush()
        assert child.wait(timeout=15) == (17 if exit_mode == "crash" else 0)
        status = capacity.capacity_status(tmp_path)
        assert not status["active_reservations"]
        assert len(status["stale_reservations"]) == int(exit_mode == "crash")
        # A status read cannot delete even a proven-dead control record.
        assert len(_record_paths(tmp_path)) == int(exit_mode == "crash")
        with _reserve(tmp_path, key=OTHER_KEY) as result:
            assert result["stale_reaped"] == int(exit_mode == "crash")
        assert not _record_paths(tmp_path)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=15)
        child.stdin.close()
        child.stdout.close()
        child.stderr.close()


def test_independent_threads_can_compute_without_holding_global_lock(tmp_path):
    barrier, errors = threading.Barrier(3), []
    released = False

    def worker(key):
        try:
            with _reserve(tmp_path, key=key):
                barrier.wait(timeout=10)
                barrier.wait(timeout=10)
        except Exception as error:
            errors.append(error)
            barrier.abort()

    threads = [threading.Thread(target=worker, args=(key,)) for key in (KEY, OTHER_KEY)]
    for thread in threads:
        thread.start()
    try:
        barrier.wait(timeout=10)
        assert len(capacity.capacity_status(tmp_path)["active_reservations"]) == 2
        barrier.wait(timeout=10)
        released = True
    finally:
        if not released:
            barrier.abort()
        for thread in threads:
            thread.join(timeout=10)
    assert not errors and all(not thread.is_alive() for thread in threads)
    assert not _record_paths(tmp_path)


def test_changing_capacity_policy_waits_for_existing_writers(tmp_path):
    with _reserve(tmp_path):
        with pytest.raises(capacity.CapacityDeferred, match="capacity_policy_changed_while_busy"):
            with _reserve(tmp_path, key=OTHER_KEY, capacity_bytes=CAP * 2):
                pytest.fail("mixed active policy generations cannot admit compute")
    with _reserve(tmp_path, capacity_bytes=CAP * 2):
        pass


@pytest.mark.parametrize("defect", ["wrong_contract", "wrong_root", "wrong_key", "wrong_token", "extra", "huge", "malformed", "duplicate", "mode", "shared"])
def test_invalid_reservation_is_retained_not_silently_reaped(tmp_path, defect):
    path = _record(tmp_path)
    value = json.loads(path.read_bytes())
    if defect == "wrong_contract": value["contract"] = "other"
    elif defect == "wrong_root": value["root_inode"] += 1
    elif defect == "wrong_key": value["key"] = OTHER_KEY
    elif defect == "wrong_token": value["token"] = "1" * 32
    elif defect == "extra": value["not_supported"] = True
    if defect in {"wrong_contract", "wrong_root", "wrong_key", "wrong_token", "extra"}:
        path.write_text(json.dumps(value))
    elif defect == "huge": path.write_bytes(b"x" * (capacity.MAX_RECORD_BYTES + 1))
    elif defect == "malformed": path.write_bytes(b"{")
    elif defect == "duplicate": path.write_text(json.dumps(value)[:-1] + ',"key":"' + KEY + '"}')
    elif defect == "mode": path.chmod(0o644)
    elif defect == "shared": os.link(path, tmp_path / "shared-record")
    before = path.read_bytes(), path.stat()
    with pytest.raises(capacity.CapacityDeferred):
        with _reserve(tmp_path):
            pytest.fail("unknown reservation cannot admit compute")
    assert (path.read_bytes(), path.stat()) == before


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_unknown_nonregular_entry_prevents_false_complete_capacity(tmp_path, kind):
    path = tmp_path / "unknown"
    if kind == "directory": path.mkdir()
    elif kind == "symlink": path.symlink_to("absent")
    else: os.mkfifo(path)
    before = path.lstat()
    with pytest.raises(capacity.CapacityDeferred, match="unmeasured_namespace_entry"):
        with _reserve(tmp_path):
            pytest.fail("unmeasured scope must not admit compute")
    assert path.lstat() == before


def test_namespace_lock_is_bounded_and_never_removed_when_busy(tmp_path):
    capacity.capacity_status(tmp_path)
    lock = tmp_path / capacity._LOCK
    with lock.open("rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = lock.stat()
        with pytest.raises(capacity.CapacityDeferred, match="namespace_lock_busy"):
            with _reserve(tmp_path, timeout_seconds=0):
                pytest.fail("busy namespace cannot admit")
        assert lock.stat() == before
    with _reserve(tmp_path, timeout_seconds=0):
        pass


@pytest.mark.parametrize("kind", ["symlink", "fifo", "shared", "nonempty", "writable"])
def test_untrusted_namespace_lock_cannot_be_used_or_deleted(tmp_path, kind):
    lock = tmp_path / capacity._LOCK
    if kind == "symlink": lock.symlink_to("missing")
    elif kind == "fifo": os.mkfifo(lock)
    else:
        lock.touch(mode=0o600)
        if kind == "shared": os.link(lock, tmp_path / "lock-alias")
        elif kind == "nonempty": lock.write_bytes(b"unknown")
        else: lock.chmod(0o666)
    before = lock.lstat()
    with pytest.raises((capacity.CapacityDeferred, OSError)):
        with _reserve(tmp_path, timeout_seconds=0):
            pytest.fail("untrusted lock cannot admit")
    assert lock.lstat() == before


@pytest.mark.parametrize("name", ["capacity_bytes", "maximum_growth_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, 2**63])
def test_invalid_byte_bounds_fail_before_creating_metadata(tmp_path, name, value):
    with pytest.raises(ValueError, match="positive integer byte count"):
        with _reserve(tmp_path, **{name: value}): pass
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("value", [-1, True, float("nan"), float("inf"), 61, "2"])
def test_invalid_timeouts_fail_before_creating_metadata(tmp_path, value):
    with pytest.raises(ValueError, match="lock timeout"):
        with _reserve(tmp_path, timeout_seconds=value): pass
    assert not list(tmp_path.iterdir())


def test_replaced_namespace_during_work_cannot_remove_replacement_record(tmp_path):
    root = tmp_path / "cache"
    root.mkdir()
    with _reserve(root) as result:
        path = _record_paths(root)[0]
        original = tmp_path / "original"
        root.rename(original)
        root.mkdir()
        replacement = root / path.name
        replacement.write_bytes(b"unique unknown data")
    assert result["state"] == "released_lease_cleanup_unconfirmed"
    assert replacement.read_bytes() == b"unique unknown data"
    assert (original / path.name).exists()


def test_release_failure_does_not_turn_a_valid_compute_into_another_compute(tmp_path, monkeypatch):
    original = capacity._namespace_lock
    calls = []

    @contextmanager
    def lock(root, **options):
        calls.append(root)
        if len(calls) == 2:
            raise capacity.CapacityDeferred("injected_release_busy")
        with original(root, **options):
            yield

    monkeypatch.setattr(capacity, "_namespace_lock", lock)
    with _reserve(tmp_path) as result:
        value = "computed_and_durably_committed"
    assert value == "computed_and_durably_committed"
    assert result["state"] == "released_lease_cleanup_unconfirmed"
    monkeypatch.setattr(capacity, "_namespace_lock", original)
    assert len(capacity.capacity_status(tmp_path)["stale_reservations"]) == 1
    with _reserve(tmp_path) as retried:
        assert retried["stale_reaped"] == 1


def test_inventory_bound_stops_streaming_before_unbounded_directory_enumeration(tmp_path, monkeypatch):
    for index in range(10): (tmp_path / str(index)).touch()
    monkeypatch.setattr(capacity, "MAX_NAMES", 5)
    with pytest.raises(capacity.CapacityDeferred, match="namespace_inventory_bound"):
        with _reserve(tmp_path): pytest.fail("unbounded namespace")


def test_unknown_stale_looking_names_are_counted_not_removed(tmp_path):
    opaque = tmp_path / f".{KEY}.capacity-not-a-token.json"
    opaque.write_bytes(b"unique bytes")
    before = opaque.stat()
    with _reserve(tmp_path): pass
    assert opaque.stat() == before and opaque.read_bytes() == b"unique bytes"


def test_reservation_covers_the_canonical_producer_through_every_durable_write(tmp_path, monkeypatch):
    import torch
    from stockagent.live import completed_inference_cache as cache

    identity = {"input": "complete-output-transaction"}
    key = cache.identity_digest(identity)
    real_write, stages = cache.atomic_write_bytes, []

    def require_reservation(stage):
        status = capacity.capacity_status(tmp_path)
        assert [row["key"] for row in status["active_reservations"]] == [key]
        stages.append(stage)

    def compute():
        require_reservation("compute")
        return {"values": torch.tensor([0., -0., float("nan"), -2.5], dtype=torch.float64)}

    def write(path, content, **options):
        require_reservation(path.suffix)
        assert options["durable"] is True  # Only ephemeral controls may skip fsync.
        real_write(path, content, **options)

    monkeypatch.setattr(cache, "atomic_write_bytes", write)
    with _reserve(tmp_path, key=key) as admission:
        value, receipt = cache.cached_forward(tmp_path, identity, compute)
        assert receipt["state"] == "computed" and admission["state"] == "reserved"
    assert admission["state"] == "released"
    assert stages == ["compute", ".forward", ".json"]
    monkeypatch.setattr(cache, "atomic_write_bytes", real_write)
    # The integration point belongs after the existing complete hit validation:
    # a valid hit still works when no further growth can be admitted.
    loaded, hit = cache.cached_forward(tmp_path, identity, lambda: pytest.fail("valid hit recomputed"))
    assert hit["state"] == "disk_hit" and hit["gpu_forward_calls"] == 0
    assert loaded["values"].view(torch.int64).tolist() == value["values"].view(torch.int64).tolist()
    assert not capacity.capacity_status(tmp_path)["active_reservations"]


def test_refused_admission_does_not_enter_the_canonical_producer(tmp_path):
    from stockagent.live import completed_inference_cache as cache

    with pytest.raises(capacity.CapacityDeferred, match="capacity_exhausted"):
        with _reserve(tmp_path, capacity_bytes=1):
            cache.cached_forward(tmp_path, {"must_not": "compute"}, lambda: pytest.fail("model call"))
    assert not list(tmp_path.glob("*.forward"))
    assert not list(tmp_path.glob("*.receipt.json"))


@pytest.mark.parametrize("mode", ["baseline", "capacity"])
def test_complete_transaction_benchmark_reuses_originals_and_retires_only_own_verified_copies(tmp_path, mode):
    import torch
    from stockagent.live import completed_inference_cache as cache
    from scripts.benchmark_completed_forward_usage import benchmark_capacity_transaction

    root, work = tmp_path / "source", tmp_path / "work"
    keys = []
    for index in range(2):
        _, receipt = cache.cached_forward(root, {"input": index}, lambda: {
            "values": torch.tensor([0., -0., float("nan"), -2.5], dtype=torch.float64)})
        keys.append(receipt["key"])

    def originals():
        result = {}
        for path in root.iterdir():
            raw, info = path.read_bytes(), path.stat()
            # Reads may advance atime (including under relatime). Preserve and
            # compare content, identity and mutation metadata, not access time.
            result[path.name] = (raw, info.st_dev, info.st_ino, info.st_mode,
                                 info.st_nlink, info.st_uid, info.st_gid,
                                 info.st_size, info.st_blocks,
                                 info.st_mtime_ns, info.st_ctime_ns)
        return result

    before = originals()
    result = benchmark_capacity_transaction(root, keys, work_root=work, mode=mode)
    assert result["state"] == "accepted" and result["private_working_copy_retired"]
    assert result["original_payloads_and_receipts_unchanged"]
    assert result["model_calls"] == result["source_or_ledger_writes"] == 0
    assert not result["cuda_initialized"] and not result["live_writer_adopted"]
    assert len(result["trials"]) == len(keys) and not list(work.iterdir())
    assert originals() == before


def test_unknown_private_benchmark_file_keeps_the_whole_working_copy(tmp_path, monkeypatch):
    import torch
    from stockagent.live import completed_inference_cache as cache
    from scripts.benchmark_completed_forward_usage import benchmark_capacity_transaction

    root, work = tmp_path / "source", tmp_path / "work"
    _, receipt = cache.cached_forward(root, {"input": 1}, lambda: torch.tensor([1.]))
    original = cache.cached_forward

    def with_unknown(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path != root:
            (path / "unique-unknown").write_bytes(b"not reproducibility-proven")
        return result

    monkeypatch.setattr(cache, "cached_forward", with_unknown)
    with pytest.raises(RuntimeError, match="unknown private content"):
        benchmark_capacity_transaction(root, [receipt["key"]], work_root=work, mode="capacity")
    cases = list(work.iterdir())
    assert len(cases) == 1 and (cases[0] / "unique-unknown").read_bytes() == b"not reproducibility-proven"
    assert (cases[0] / "FAILED.json").is_file()
    assert (cases[0] / f"{receipt['key']}.forward").is_file()
    assert (root / f"{receipt['key']}.forward").is_file()


def test_ephemeral_reservation_is_atomic_but_does_not_fsync(tmp_path, monkeypatch):
    original, writes = capacity.atomic_write_bytes, []

    def write(path, raw, **options):
        assert options == {"durable": False, "mode": 0o600}
        writes.append(path)
        original(path, raw, **options)

    monkeypatch.setattr(capacity, "atomic_write_bytes", write)
    with _reserve(tmp_path):
        assert len(capacity.capacity_status(tmp_path)["active_reservations"]) == 1
    assert len(writes) == 1 and not _record_paths(tmp_path)


def test_interruption_after_record_publication_cannot_reserve_credit_forever(tmp_path, monkeypatch):
    original = capacity.atomic_write_bytes

    def interrupted(path, raw, **options):
        original(path, raw, **options)
        raise InterruptedError("injected exit between atomic record and kernel lease")

    monkeypatch.setattr(capacity, "atomic_write_bytes", interrupted)
    with pytest.raises(InterruptedError, match="injected exit"):
        with _reserve(tmp_path): pytest.fail("transaction was not admitted")
    assert len(capacity.capacity_status(tmp_path)["stale_reservations"]) == 1
    monkeypatch.setattr(capacity, "atomic_write_bytes", original)
    with _reserve(tmp_path) as result:
        assert result["stale_reaped"] == 1
    assert not _record_paths(tmp_path)


@pytest.mark.parametrize("mode", ["staged-copy", "selected-root"])
def test_complete_cold_publication_benchmark_preserves_originals_and_retires_only_verified_work(tmp_path, mode):
    import torch
    from stockagent.live import completed_inference_cache as cache
    from scripts.benchmark_completed_forward_usage import benchmark_cold_publication

    root, work = tmp_path / "source", tmp_path / "work"
    _, receipt = cache.cached_forward(root, {"input": 1}, lambda: {
        "values": torch.tensor([0., -0., float("nan"), -2.5], dtype=torch.float64)})
    before = {p.name: (p.read_bytes(), capacity._generation(p.stat())) for p in root.iterdir()}
    result = benchmark_cold_publication(root, [receipt["key"]], work_root=work, mode=mode)
    assert result["state"] == "accepted" and result["private_working_copy_retired"]
    assert result["originals_unchanged"] and result["retired_private_files"]
    assert (result["staging_bytes_written"] > 0) == (mode == "staged-copy")
    assert result["model_calls"] == result["provider_calls"] == result["source_or_ledger_writes"] == 0
    assert not result["cuda_initialized"] and not result["production_cold_policy_enrolled"]
    assert not list(work.iterdir())
    assert {p.name: (p.read_bytes(), capacity._generation(p.stat())) for p in root.iterdir()} == before


@pytest.mark.parametrize("mode", ["materialized-copy", "flat-stage"])
def test_fixed_pin_consumer_restore_reuses_all_bits_and_never_computes(tmp_path, mode):
    import torch
    from stockagent.live import completed_inference_cache as cache
    from scripts.benchmark_completed_forward_usage import benchmark_cold_publication

    root, work = tmp_path / "source", tmp_path / "work"
    _, receipt = cache.cached_forward(root, {"input": "real-domain-ABI"}, lambda: {
        "signed": torch.tensor([0., -0., float("nan"), -2.5], dtype=torch.float64),
        "metadata": {"mask": torch.tensor([True, False]), "nested": (None, "raw", 3)}})
    before = {p.name: (p.read_bytes(), capacity._generation(p.stat())) for p in root.iterdir()}
    result = benchmark_cold_publication(root, [receipt["key"]], work_root=work,
                                       mode="selected-root", restore_mode=mode)
    assert result["contract"] == "completed_forward_fixed_cold_consumer_benchmark_v1"
    assert result["state"] == "accepted" and result["private_working_copy_retired"]
    assert result["consumer_tensor_bits_and_metadata_equal"] and result["consumer_gpu_forward_calls"] == 0
    assert result["restore_mode"] == mode
    assert result["restoration_payload_bytes_written"] == sum(
        before[receipt["key"] + suffix][1][2] for suffix in (".forward", ".receipt.json")) * (
            2 if mode == "materialized-copy" else 1)
    assert result["originals_unchanged"] and not result["cuda_initialized"]
    assert not list(work.iterdir())
    assert {p.name: (p.read_bytes(), capacity._generation(p.stat())) for p in root.iterdir()} == before


def test_unknown_cold_benchmark_content_preserves_the_whole_private_case(tmp_path):
    import torch
    from stockagent.live import completed_inference_cache as cache
    from stockagent.data_sync import packed_snapshots as packed
    from scripts.benchmark_completed_forward_usage import benchmark_cold_publication

    root, work = tmp_path / "source", tmp_path / "work"
    _, receipt = cache.cached_forward(root, {"input": 1}, lambda: torch.tensor([1.]))

    def with_unknown(cold, *args, **kwargs):
        result = packed.publish_packed_snapshot(cold, *args, **kwargs)
        (cold.parent / "unique-unknown").write_bytes(b"retain for diagnosis")
        return result

    with pytest.raises(RuntimeError, match="unknown private cold benchmark content"):
        benchmark_cold_publication(root, [receipt["key"]], work_root=work,
                                   mode="selected-root", publisher=with_unknown)
    cases = list(work.iterdir())
    assert len(cases) == 1 and (cases[0] / "FAILED.json").is_file()
    assert (cases[0] / "unique-unknown").read_bytes() == b"retain for diagnosis"
    assert len(list((cases[0] / "restored").rglob("*.forward"))) == 1
    assert (root / f"{receipt['key']}.forward").is_file()


@pytest.mark.parametrize("relation", ["same", "inside", "ancestor"])
def test_cold_benchmark_never_writes_into_an_authority_or_live_namespace(tmp_path, monkeypatch, relation):
    from scripts import benchmark_completed_forward_usage as benchmark

    root, protected = tmp_path / "source", tmp_path / "authority"
    root.mkdir()
    monkeypatch.setattr(benchmark, "_COLD_BENCHMARK_PROTECTED_ROOTS", (protected,))
    work = protected if relation == "same" else protected / "work" if relation == "inside" else tmp_path
    with pytest.raises(ValueError, match="private workspace"):
        benchmark.benchmark_cold_publication(root, [KEY], work_root=work, mode="selected-root")
    assert not protected.exists() and not list(tmp_path.glob("cold-publication-*"))
