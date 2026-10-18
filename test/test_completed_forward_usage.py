from __future__ import annotations

import hashlib
import json
import os

import pytest
import torch

from stockagent.data_sync import completed_forward_usage as usage
from stockagent.data_sync import completed_forward_compaction as compaction
from stockagent.live import completed_inference_cache as cache

NOW = 10**18


def _binding(label="a"):
    return {"key": hashlib.sha256(label.encode()).hexdigest(),
            "receipt_sha256": "1" * 64, "payload_sha256": "2" * 64, "payload_bytes": 42}


def _record(root, binding=None, now=NOW):
    return usage.note_successful_use(root, **(binding or _binding()), now_ns=now)


def _entry(root, label="a", value=1):
    identity = {"input": label}
    _, result = cache.cached_forward(root, identity, lambda: torch.tensor([value]))
    return identity, result["key"]


def _old_usage(root, key, now=NOW):
    path = root / f"{key}.receipt.json"
    raw = path.read_bytes()
    record = json.loads(raw)
    binding = {"key": key, "receipt_sha256": hashlib.sha256(raw).hexdigest(),
               "payload_sha256": record["payload_sha256"], "payload_bytes": record["payload_bytes"]}
    (root / f"{key}.usage.json").unlink(missing_ok=True)
    assert _record(root, binding, now)["state"] == "recorded"


def _plan(root, **kwargs):
    return compaction.plan_completed_forward_working_set(
        root, now_ns=NOW + 7200 * 10**9, reference_probe=lambda *a, **kw: [], **kwargs)


def test_one_sample_per_minute_is_not_a_hit_counter(tmp_path):
    assert _record(tmp_path)["state"] == "recorded"
    path = tmp_path / f"{_binding()['key']}.usage.json"
    before = path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns
    for offset in (1, 10**9, usage.SAMPLE_INTERVAL_NS - 1):
        assert _record(tmp_path, now=NOW + offset) == {
            "contract": usage.CONTRACT, "state": "sampled", "metadata_written": False,
            "sample_interval_seconds": 60}
    assert (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) == before
    assert _record(tmp_path, now=NOW + usage.SAMPLE_INTERVAL_NS)["state"] == "recorded"
    record, _ = usage.read_usage(path)
    assert record["recorded_samples"] == 2
    assert record["first_success_at_ns"] == NOW
    assert record["last_success_at_ns"] == NOW + usage.SAMPLE_INTERVAL_NS


def test_valid_changed_commit_resets_samples_and_reversed_clock_does_not_touch(tmp_path):
    _record(tmp_path)
    path = tmp_path / f"{_binding()['key']}.usage.json"
    before = path.read_bytes()
    assert _record(tmp_path, now=NOW - 1)["reason"] == "wall_clock_reversed"
    assert path.read_bytes() == before
    changed = {**_binding(), "receipt_sha256": "3" * 64}
    assert _record(tmp_path, changed, NOW + 1)["state"] == "recorded"
    record, _ = usage.read_usage(path)
    assert record["recorded_samples"] == 1 and record["first_success_at_ns"] == NOW + 1


@pytest.mark.parametrize("damage", ["symlink", "hardlink", "writable", "directory", "fifo",
                                     "oversized", "duplicate", "nan", "boolean", "unknown_contract", "nested"])
def test_untrusted_usage_is_bounded_retained_and_does_not_disclose_target(tmp_path, damage):
    _record(tmp_path)
    path = tmp_path / f"{_binding()['key']}.usage.json"
    raw = path.read_bytes()
    if damage in {"symlink", "directory", "fifo"}:
        path.unlink()
    if damage == "symlink":
        target = tmp_path / "unrelated"
        target.write_text("preserve")
        path.symlink_to(target)
    elif damage == "hardlink":
        os.link(path, tmp_path / "unrelated")
    elif damage == "writable":
        path.chmod(0o666)
    elif damage == "directory":
        path.mkdir()
    elif damage == "fifo":
        os.mkfifo(path)
    elif damage == "oversized":
        path.write_bytes(b"x" * (usage.MAX_USAGE_BYTES + 1))
    elif damage == "duplicate":
        path.write_bytes(raw[:-1] + b',"contract":"duplicate"}')
    elif damage == "nan":
        path.write_bytes(raw.replace(b'"payload_bytes":42', b'"payload_bytes":NaN'))
    elif damage == "boolean":
        path.write_bytes(raw.replace(b'"payload_bytes":42', b'"payload_bytes":true'))
    elif damage == "unknown_contract":
        path.write_bytes(raw.replace(usage.CONTRACT.encode(), b"unknown"))
    elif damage == "nested":
        path.write_bytes(b"[" * 1500 + b"]" * 1500)
    before = path.lstat()
    result = _record(tmp_path, now=NOW + usage.SAMPLE_INTERVAL_NS)
    assert result["state"] == "deferred"
    assert path.lstat() == before
    if damage == "symlink":
        assert target.read_text() == "preserve"


def test_telemetry_failure_keeps_valid_inference_and_failed_validation_does_not_renew(tmp_path, monkeypatch):
    identity, key = _entry(tmp_path)
    path = tmp_path / f"{key}.usage.json"
    before = path.read_bytes()
    def reject():
        raise RuntimeError("source changed")
    with pytest.raises(RuntimeError, match="source changed"):
        cache.cached_forward(tmp_path, identity, lambda: pytest.fail("GPU"), validate_before_commit=reject)
    assert path.read_bytes() == before
    path.unlink()
    def failed(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(usage, "atomic_write_bytes", failed)
    value, result = cache.cached_forward(tmp_path, identity, lambda: pytest.fail("GPU"))
    assert value.tolist() == [1] and result["state"] == "disk_hit"
    assert result["usage"]["state"] == "deferred" and not path.exists()


def test_failed_compute_or_commit_cannot_create_successful_use(tmp_path, monkeypatch):
    def failed(*args, **kwargs):
        raise OSError("commit failed")
    monkeypatch.setattr(cache, "atomic_write_bytes", failed)
    with pytest.raises(OSError, match="commit failed"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.ones(1))
    assert not list(tmp_path.glob("*.usage.json"))


def test_metadata_only_capacity_counts_aliases_once_and_never_deletes(tmp_path, monkeypatch):
    root = tmp_path / "cache"
    _, a = _entry(root, "a")
    _, b = _entry(root, "b")
    for key in (a, b):
        _old_usage(root, key)
    first = root / f"{a}.forward"
    assert os.path.samefile(first, root / f"{b}.forward")
    monkeypatch.setattr(compaction, "stable_payload_hash", lambda *a: pytest.fail("payload hash"))
    before = {p.name: p.read_bytes() for p in root.iterdir()}
    result = _plan(root, target_bytes=1)
    assert result["inventory_complete"] and not result["eviction_allowed"]
    assert result["committed_payload_allocated_bytes"] == first.stat().st_blocks * 512
    assert result["committed_logical_payload_bytes"] == first.stat().st_size * 2
    assert result["pressure_bytes"] == result["namespace_allocated_bytes"] - 1
    assert result["payload_bytes_hashed"] == result["logical_paths_removed"] == 0
    assert len(result["oldest_groups_requiring_recovery"]) == 1
    assert result["oldest_groups_requiring_recovery"][0]["keys"] == sorted([a, b])
    assert {p.name: p.read_bytes() for p in root.iterdir()} == before


def _consumer_snapshot(keys, complete=True):
    return {"contract": "completed_forward_current_consumers_v1", "keys": keys,
            "complete_for_scope": complete, "scope": "test_current_consumers"}


def test_capacity_uses_native_consumer_pins_and_retains_all_payload_aliases(tmp_path):
    root = tmp_path / "cache"
    _, a = _entry(root, "a")
    _, b = _entry(root, "b")
    for key in (a, b):
        _old_usage(root, key)
    result = _plan(root, consumer_pin_probe=lambda: _consumer_snapshot([a]))
    assert result["consumer_pins_complete_for_scope"]
    assert result["consumer_pin_snapshot_stable"]
    assert result["current_service_protected_keys"] == [a]
    assert result["oldest_groups_requiring_recovery"] == []
    assert all("current_service_consumer_pin" in row["retention_reasons"] for row in result["keys"])
    assert not result["consumer_pins_exhaustive"] and not result["eviction_allowed"]


@pytest.mark.parametrize("problem", ["incomplete", "raised", "unknown_contract", "bad_key", "changed"])
def test_capacity_fails_closed_for_unverifiable_or_concurrent_consumer_census(tmp_path, problem):
    root = tmp_path / "cache"
    _, key = _entry(root)
    _old_usage(root, key)
    calls = []
    def probe():
        calls.append(True)
        if problem == "raised":
            raise OSError("temporarily unreadable controls")
        result = _consumer_snapshot([], complete=problem != "incomplete")
        if problem == "unknown_contract":
            result["contract"] = "unsupported"
        elif problem == "bad_key":
            result["keys"] = ["invalid"]
        elif problem == "changed" and len(calls) > 1:
            result["keys"] = [key]
        return result
    result = _plan(root, consumer_pin_probe=probe)
    assert len(calls) == 2 and not result["consumer_pins_complete_for_scope"]
    assert result["oldest_groups_requiring_recovery"] == []
    assert "consumer_pin_inventory_incomplete_or_changed" in result["keys"][0]["retention_reasons"]
    if problem == "changed":
        assert not result["consumer_pin_snapshot_stable"]
        assert result["current_service_protected_keys"] == [key]


def test_missing_current_payload_is_not_presented_as_successful_cache_acceptance(tmp_path):
    root = tmp_path / "cache"
    _entry(root)
    result = _plan(root, consumer_pin_probe=lambda: _consumer_snapshot(["f" * 64]))
    assert result["consumer_pins_complete_for_scope"]  # Control census, NOT payload acceptance.
    assert result["consumer_keys_missing_verified_commit"] == ["f" * 64]
    assert not result["eviction_allowed"]


@pytest.mark.parametrize("reason", ["missing", "stale_commit", "busy", "recent", "future", "pin",
                                     "external_link", "process"])
def test_unknown_current_shared_or_active_keys_are_not_lru_candidates(tmp_path, reason):
    root = tmp_path / "cache"
    _, key = _entry(root)
    _old_usage(root, key)
    kwargs = {}
    if reason == "missing":
        (root / f"{key}.usage.json").unlink()
    elif reason == "stale_commit":
        receipt = root / f"{key}.receipt.json"
        receipt.write_bytes(receipt.read_bytes() + b"\n")
    elif reason == "recent":
        _old_usage(root, key, NOW + 7200 * 10**9 - usage.SAMPLE_INTERVAL_NS)
    elif reason == "future":
        _old_usage(root, key, NOW + 7200 * 10**9 + 1)
    elif reason == "pin":
        kwargs["protected_keys"] = [key]
    elif reason == "external_link":
        os.link(root / f"{key}.forward", tmp_path / "outside")
    if reason == "process":
        result = compaction.plan_completed_forward_working_set(
            root, now_ns=NOW + 7200 * 10**9, reference_probe=lambda *a, **kw: ["pid:123 mmap"])
    elif reason == "busy":
        with cache._singleflight(root / f"{key}.lock"):
            result = _plan(root)
    else:
        result = _plan(root, **kwargs)
    assert result["oldest_groups_requiring_recovery"] == []
    assert not result["eviction_allowed"] and (root / f"{key}.forward").exists()


def test_unknown_nested_namespace_and_orphan_payload_are_not_hidden(tmp_path):
    root = tmp_path / "cache"
    _entry(root)
    (root / "unexpected").mkdir()
    orphan = root / f"{'f' * 64}.forward"
    orphan.write_bytes(b"unique source")
    result = _plan(root)
    assert not result["inventory_complete"] and result["unmeasured_entries"][0]["path"] == "unexpected"
    assert result["unverified_keys_retained"][0]["path"] == orphan.name
    assert orphan.read_bytes() == b"unique source"


def test_capacity_cli_requires_explicit_forward_policy(monkeypatch):
    from scripts import deduplicate_artifacts as cli
    monkeypatch.setattr("sys.argv", ["deduplicate_artifacts.py", "--forward-capacity-bytes", "1", "--apply"])
    with pytest.raises(ValueError, match="explicit completed-forward"):
        cli.main()


def test_capacity_does_not_present_concurrent_namespace_change_as_complete(tmp_path):
    root = tmp_path / "cache"
    _entry(root)
    def concurrent(*args, **kwargs):
        (root / "new-unknown-payload").write_bytes(b"arrived")
        return []
    result = compaction.plan_completed_forward_working_set(root, reference_probe=concurrent)
    assert not result["inventory_complete"] and not result["metadata_snapshot_stable"]
    assert not result["eviction_allowed"]


def test_capacity_batches_process_path_probe_at_one_root_and_keeps_inode_alias_gate(tmp_path):
    root = tmp_path / "cache"
    _, key = _entry(root)
    seen = []
    def probe(paths, *, inode_keys):
        seen.append((paths, inode_keys))
        return []
    compaction.plan_completed_forward_working_set(root, reference_probe=probe)
    info = (root / f"{key}.forward").stat()
    assert seen == [([root.absolute()], {(info.st_dev, info.st_ino)})]


def test_usage_benchmark_checks_all_bytes_and_reduces_metadata_writes(tmp_path):
    from scripts.benchmark_completed_forward_usage import benchmark
    root = tmp_path / "cache"
    _, key = _entry(root)
    result = benchmark(root, [key], repeats=2)
    assert result["state"] == "accepted" and not result["cuda_initialized"]
    assert result["payload_and_receipt_generations_unchanged"]
    trials = result["trials"]
    assert [trial["mode"] for trial in trials] == ["none", "per_hit", "sampled", "sampled", "per_hit", "none"]
    assert all(trial["metadata_writes"] == 2 for trial in trials if trial["mode"] == "per_hit")
    assert all(trial["metadata_writes"] == 0 for trial in trials if trial["mode"] != "per_hit")


def test_complete_working_set_benchmark_retains_inode_gate_and_reduces_path_fanout(tmp_path, monkeypatch):
    from scripts import benchmark_completed_forward_usage as bench
    root = tmp_path / "cache"
    _entry(root, "a")
    _entry(root, "b")
    seen = []
    def probe(paths, *, inode_keys):
        seen.append((len(paths), inode_keys))
        return []
    monkeypatch.setattr(bench, "process_references_many", probe)
    result = bench.benchmark_working_set_probes(root)
    assert result["state"] == "accepted" and result["logical_paths_removed"] == 0
    assert [count for count, _ in seen] == [2, 1, 1, 2]
    assert all(keys == seen[0][1] for _, keys in seen)
