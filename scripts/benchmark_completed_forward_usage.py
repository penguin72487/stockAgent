#!/usr/bin/env python3
"""Measure complete CPU cache hits with no, per-hit and sampled usage writes.

Uses committed identities explicitly, not current-model admission. Forbids any
model compute, validates every returned tensor byte, and preserves original
payload/receipt generations. Usage modes may replace advisory metadata only;
capacity mode tests then retires its own verified private working copies.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import stat
import sys
import tempfile
import time
import uuid
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import torch

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import completed_forward_usage as usage
from stockagent.data_sync.immutable_cache_files import payload_info
from stockagent.data_sync.completed_forward_compaction import _inventory, plan_completed_forward_working_set
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.live import completed_inference_cache as cache

_COLD_BENCHMARK_PROTECTED_ROOTS = (
    Path("/srv/stockagent-packed"), Path("/srv/stockagent-packed-materialized"),
    Path("/srv/stockagent-live"), Path("/srv/stockagent-artifacts-hot"),
    Path("/srv/stockagent-d-volume"), Path("/mnt/d/stockagent-cold-primary"),
)


def _fingerprint(value):
    if isinstance(value, torch.Tensor):
        tensor = value.detach().cpu().contiguous()
        raw = tensor.reshape(-1).view(torch.uint8).numpy()
        return {"shape": list(tensor.shape), "dtype": str(tensor.dtype),
                "sha256": hashlib.sha256(memoryview(raw)).hexdigest()}
    if isinstance(value, dict):
        return {key: _fingerprint(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return {"container": type(value).__name__, "items": [_fingerprint(item) for item in value]}
    return {"type": type(value).__name__, "value": repr(value)}


def benchmark(root: Path, keys: list[str], *, repeats: int) -> dict:
    if repeats < 2 or not keys or len(keys) != len(set(keys)):
        raise ValueError("distinct committed keys and at least two repetitions required")
    records, before, expected = {}, {}, {}
    def forbidden():
        raise AssertionError("usage benchmark attempted model recomputation")
    for key in keys:
        if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
            raise ValueError("invalid cache key")
        path = root / f"{key}.forward"
        receipt = path.with_suffix(".receipt.json")
        with receipt.open("rb") as handle:
            raw = handle.read(1024**2 + 1)
        if len(raw) > 1024**2:
            raise ValueError("oversized receipt")
        record = json.loads(raw)
        if cache.identity_digest(record["identity"]) != key:
            raise ValueError("receipt identity/key mismatch")
        records[key] = record
        before[key] = {"payload": payload_info(path), "receipt": payload_info(receipt),
                       "receipt_sha256": hashlib.sha256(raw).hexdigest()}
        value, result = cache.cached_forward(root, record["identity"], forbidden)
        assert result["state"] == "disk_hit"
        expected[key] = _fingerprint(value)
    trials = []
    # Balanced repeated order; initial payload checksum/deserialization is not
    # silently omitted from any measured cache hit.
    for mode in ("none", "per_hit", "sampled", "sampled", "per_hit", "none"):
        count, durations = {"writes": 0, "bytes": 0}, []
        atomic = usage.atomic_write_bytes
        def measured_write(path, payload, **kwargs):
            atomic(path, payload, **kwargs)
            count["writes"] += 1
            count["bytes"] += len(payload)
        with ExitStack() as stack:
            stack.enter_context(patch.object(usage, "atomic_write_bytes", measured_write))
            if mode == "none":
                stack.enter_context(patch.object(cache, "note_successful_use", lambda *a, **kw: {}))
            elif mode == "per_hit":
                stack.enter_context(patch.object(usage, "SAMPLE_INTERVAL_NS", 0))
            started = time.perf_counter()
            for _ in range(repeats):
                for key in keys:
                    tick = time.perf_counter()
                    value, result = cache.cached_forward(root, records[key]["identity"], forbidden)
                    # Include the complete independent byte readback in total,
                    # but also retain the canonical call duration separately.
                    durations.append(time.perf_counter() - tick)
                    if result["state"] != "disk_hit" or _fingerprint(value) != expected[key]:
                        raise AssertionError("cache output changed")
            elapsed = time.perf_counter() - started
        trials.append({"mode": mode, "calls": len(durations), "complete_seconds": elapsed,
                       "mean_hit_seconds": statistics.mean(durations),
                       "metadata_writes": count["writes"], "metadata_bytes_written": count["bytes"],
                       "all_output_bytes_equal": True, "model_calls": 0})
    for key in keys:
        path = root / f"{key}.forward"
        receipt = path.with_suffix(".receipt.json")
        if (payload_info(path) != before[key]["payload"]
                or payload_info(receipt) != before[key]["receipt"]
                or hashlib.sha256(receipt.read_bytes()).hexdigest() != before[key]["receipt_sha256"]):
            raise AssertionError("benchmark mutated committed payload/receipt")
    return {"contract": "completed_forward_usage_benchmark_v1", "state": "accepted", "keys": keys,
            "repeats": repeats, "trials": trials, "payload_and_receipt_generations_unchanged": True,
            "scope": "complete_cpu_cache_hits_not_current_model_qualification_or_opening_slo",
            "provider_calls": 0, "model_calls": 0, "source_or_ledger_writes": 0,
            "cuda_initialized": torch.cuda.is_initialized()}


def benchmark_working_set_probes(root: Path) -> dict:
    """Complete metadata plans: old per-key path fanout versus canonical root.

    The inode set is retained in both arms, including aliases outside this
    namespace. Root-level references may retain MORE than the file-only arm;
    that is conservative, not permission to exclude a reader for speed.
    """
    root = root.absolute()
    files, _, _, _ = _inventory(root)
    baseline_paths = [root / item["path"] for item in files]
    if not baseline_paths:
        raise ValueError("committed payloads required for process-probe benchmark")
    trials, results = [], []
    for mode in ("per_key_paths", "one_root", "one_root", "per_key_paths"):
        def probe(paths, *, inode_keys):
            return process_references_many(baseline_paths if mode == "per_key_paths" else paths,
                                           inode_keys=inode_keys)
        started = time.perf_counter()
        result = plan_completed_forward_working_set(root, reference_probe=probe)
        elapsed = time.perf_counter() - started
        if not result["inventory_complete"] or not result["metadata_snapshot_stable"]:
            raise RuntimeError("cache namespace changed during benchmark; retain and retry")
        if result["eviction_allowed"] or result["payload_bytes_hashed"]:
            raise AssertionError("metadata benchmark performed payload/retirement work")
        results.append(result)
        trials.append({"mode": mode, "complete_plan_seconds": elapsed,
                       "process_target_paths": len(baseline_paths) if mode == "per_key_paths" else 1,
                       "committed_keys": len(result["keys"]),
                       "process_references": result["process_references"],
                       "namespace_allocated_bytes": result["namespace_allocated_bytes"],
                       "payload_bytes_hashed": 0})
    # Source bytes, known/unknown namespace and complete inode accounting stay
    # identical. Ref probes are inherently time-sensitive and are reported raw.
    for result in results[1:]:
        for name in ("namespace_allocated_bytes", "committed_logical_payload_bytes",
                     "committed_payload_allocated_bytes", "physical_regular_inodes"):
            if result[name] != results[0][name]:
                raise AssertionError("working-set footprint changed between trials")
    return {"contract": "completed_forward_process_probe_abba_v1", "state": "accepted",
            "scope": "complete_metadata_plan_not_eviction_or_payload_integrity", "trials": trials,
            "inode_alias_gate_preserved": True, "oldest_groups_remain_recovery_gated": True,
            "logical_paths_removed": 0, "provider_calls": 0, "model_calls": 0,
            "source_or_ledger_writes": 0, "cuda_initialized": torch.cuda.is_initialized()}


def benchmark_capacity_transaction(root: Path, keys: list[str], *, work_root: Path,
                                   mode: str, capacity_bytes: int = 8 * 1024**3) -> dict:
    """Replay COMPLETE accepted CPU outputs through the unchanged producer.

    This prototypes the miss boundary, not live adoption or model qualification.
    Preserve originals; retire only our own private copies after full parity.
    Failed or unknown working copies remain with their failure evidence.
    """
    from contextlib import nullcontext
    from stockagent.data_sync.completed_forward_capacity import reserve_capacity

    if (mode not in {"baseline", "capacity"} or not keys or len(keys) != len(set(keys))
            or any(not isinstance(key, str) or len(key) != 64
                   or any(c not in "0123456789abcdef" for c in key) for key in keys)):
        raise ValueError("explicit transaction mode and distinct canonical keys required")
    if torch.cuda.is_initialized():
        raise RuntimeError("capacity benchmark requires a CUDA-cold process")
    root, work_root = root.absolute(), work_root.absolute()
    if (root.resolve(strict=True) != root or work_root.resolve() != work_root
            or root == work_root or root in work_root.parents or work_root in root.parents):
        raise ValueError("private workspace must be real and outside the source cache")
    work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    working = Path(tempfile.mkdtemp(prefix=f"capacity-{mode}-", dir=work_root))
    before, sources, trials, commits = {}, {}, [], {}
    started, cpu = time.perf_counter(), time.process_time()
    code = cache.implementation_identity()

    def forbidden():
        raise AssertionError("benchmark must not call a model")

    try:
        for key in keys:
            path, receipt_path = root / f"{key}.forward", root / f"{key}.receipt.json"
            before[key] = {"payload": payload_info(path), "receipt": payload_info(receipt_path)}
            if before[key]["receipt"]["size"] > 1024**2:
                raise ValueError("oversized source receipt")
            with receipt_path.open("rb") as stream:
                raw = stream.read(1024**2 + 1)
            record = json.loads(raw)
            if (len(raw) > 1024**2 or record.get("contract") != cache.CONTRACT
                    or record.get("key") != key or cache.identity_digest(record["identity"]) != key):
                raise ValueError("source receipt is not a canonical committed key")
            if "code" in record["identity"] and record["identity"]["code"] != code:
                raise ValueError("source implementation identity is not current")
            sources[key] = {"path": str(path), "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                            "payload_sha256": record["payload_sha256"], "payload_bytes": record["payload_bytes"]}

            def validate():
                if before[key] != {"payload": payload_info(path), "receipt": payload_info(receipt_path)}:
                    raise RuntimeError("source cache generation changed")

            tick, spent = time.perf_counter(), time.process_time()
            with cache._singleflight(root / f"{key}.lock"):
                value = cache._read(path, key, record["identity"])
            if value is cache._CACHE_MISS:
                raise RuntimeError("original complete CPU result is missing")
            expected = _fingerprint(value)
            bound = cache.MAX_PAYLOAD_BYTES + 2 * 1024**2 + 8192
            context = (reserve_capacity(working, key=key, maximum_growth_bytes=bound,
                                        capacity_bytes=capacity_bytes) if mode == "capacity" else nullcontext(None))
            with context as admission:
                stored, result = cache.cached_forward(working, record["identity"], lambda cached_value=value: cached_value,
                                                       validate_before_commit=validate)
                if result["state"] != "computed" or _fingerprint(stored) != expected:
                    raise AssertionError("canonical store changed complete tensor bits")
            loaded, hit = cache.cached_forward(working, record["identity"], forbidden,
                                               validate_before_commit=validate)
            if hit["state"] != "disk_hit" or _fingerprint(loaded) != expected:
                raise AssertionError("complete warm-cache readback differs")
            if admission is not None and admission["state"] != "released":
                raise AssertionError("transaction reservation did not release")
            commits[key] = json.loads((working / f"{key}.receipt.json").read_bytes())
            trials.append({"key": key, "wall_seconds": time.perf_counter() - tick,
                           "cpu_seconds": time.process_time() - spent, "output_fingerprint": expected,
                           "admission": admission, "fixture_callback_calls": 1, "model_calls": 0,
                           "tensor_bytes_bitwise_equal": True, "complete_hit_verified": True})
            validate()
            del value, stored, loaded
        gc.collect()
        if str(working) in Path("/proc/self/maps").read_text():
            raise AssertionError("private output is still mapped; retain working copy")
        for key in keys:
            if before[key] != {"payload": payload_info(root / f"{key}.forward"),
                               "receipt": payload_info(root / f"{key}.receipt.json")}:
                raise RuntimeError("original cache changed before private-copy retirement")
        if code != cache.implementation_identity() or torch.cuda.is_initialized():
            raise RuntimeError("code generation changed or CUDA was initialized")
        allowed = {key + suffix for key in keys for suffix in (".forward", ".receipt.json", ".usage.json", ".lock")}
        allowed.add(".namespace-capacity.lock")
        names = sorted(working.iterdir())
        if (working.resolve() != working or working.parent != work_root
                or any(p.name not in allowed or not stat.S_ISREG(p.lstat().st_mode) for p in names)):
            raise RuntimeError("unknown private content prevents cleanup")
        allocations = {(p.stat().st_dev, p.stat().st_ino): p.stat().st_blocks * 512 for p in names}
        private_allocated = sum(allocations.values())
        # Exact files produced by THIS transaction only; never recursive.
        for path in names:
            path.unlink()
        working.rmdir()
        return {"contract": "completed_forward_capacity_transaction_benchmark_v1", "state": "accepted",
                "mode": mode, "trials": trials, "complete_wall_seconds": time.perf_counter() - started,
                "complete_cpu_seconds": time.process_time() - cpu, "sources": sources,
                "source_generations": before, "complete_code_identity": code,
                "private_commit_receipts": commits, "private_working_copy_retired": True,
                "private_allocated_bytes": private_allocated, "private_path": str(working),
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "original_payloads_and_receipts_unchanged": True, "model_calls": 0, "provider_calls": 0,
                "source_or_ledger_writes": 0, "cuda_initialized": False, "live_writer_adopted": False,
                "scope": "complete CPU output transaction prototype; not GPU/model or live-policy acceptance"}
    except BaseException as error:
        atomic_write_json(working / "FAILED.json", {"state": "failed", "mode": mode,
                          "error_type": type(error).__name__, "error": str(error), "originals_retained": True})
        raise


def benchmark_cold_publication(root: Path, keys: list[str], *, work_root: Path,
                               mode: str, publisher=None, restore_mode: str | None = None,
                               fetcher=None) -> dict:
    """Complete private cold publish/fixed-recovery, not a D rollout.

    Compare the original two-file staging copy with explicit source selection.
    Every source and recovered byte is checked; retire only the exact private
    copies produced here, after verification, never any original cache path.
    """
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.completed_forward_compaction import (
        MAX_RECEIPT_BYTES, SUPPORTED_FORWARD_CONTRACT, _digest,
    )
    from stockagent.data_sync.immutable_cache_files import stable_payload_hash
    from stockagent.data_sync.completed_forward_capacity import reserve_capacity

    if (mode not in {"staged-copy", "selected-root"} or not keys or len(keys) != len(set(keys))
            or any(not isinstance(key, str) or len(key) != 64
                   or any(c not in "0123456789abcdef" for c in key) for key in keys)):
        raise ValueError("explicit cold-publication mode and distinct committed keys required")
    if (restore_mode not in {None, "materialized-copy", "flat-stage"}
            or restore_mode is not None and mode != "selected-root"):
        raise ValueError("consumer restoration requires one complete selected-root release")
    root, work_root = root.absolute(), work_root.absolute()
    if (root.resolve(strict=True) != root or work_root.resolve() != work_root
            or root == work_root or root in work_root.parents or work_root in root.parents
            or any(work_root == protected or work_root in protected.parents
                   or protected in work_root.parents for protected in _COLD_BENCHMARK_PROTECTED_ROOTS)
            or torch.cuda.is_initialized()):
        raise ValueError("cold benchmark needs a real private workspace outside originals and no CUDA")
    work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    working = Path(tempfile.mkdtemp(prefix=f"cold-publication-{mode}-", dir=work_root))
    cold, materialized = working / "cold", working / "restored"
    consumer = working / "consumer"
    if restore_mode is not None:
        consumer.mkdir(mode=0o700)
    publish = publisher or packed.publish_packed_snapshot
    fetch = fetcher or packed.fetch_packed_snapshot
    before, expected_files, source_hashes, trials, staged_bytes, restore_written = {}, set(), {}, [], 0, 0
    tick, cpu = time.perf_counter(), time.process_time()

    def verify_originals():
        if any(payload_info(root / name) != info for name, info in before.items()):
            raise RuntimeError("original forward generation changed")

    try:
        for key in keys:
            names = [key + ".forward", key + ".receipt.json"]
            before.update({name: payload_info(root / name) for name in names})
            if before[names[1]]["size"] > MAX_RECEIPT_BYTES:
                raise ValueError("oversized committed receipt")
            record = json.loads((root / names[1]).read_bytes())
            if (record.get("contract") != SUPPORTED_FORWARD_CONTRACT or record.get("key") != key
                    or _digest(record["identity"]) != key or record.get("payload_bytes") != before[names[0]]["size"]
                    or not 0 < record["payload_bytes"] <= cache.MAX_PAYLOAD_BYTES):
                raise ValueError("source is not a supported completed forward")
            hashes = {name: stable_payload_hash(root / name, before[name]) for name in names}
            if hashes[names[0]] != record["payload_sha256"]:
                raise ValueError("original payload differs from its receipt")
            source_hashes.update(hashes)
            verify_originals()
            dataset = "forward-" + key[:24]
            source = root
            stage_written = 0
            if mode == "staged-copy":
                source = working / "source" / key
                source.mkdir(parents=True)
                for name in names:
                    shutil.copy2(root / name, source / name)
                    stage_written += before[name]["size"]
                    expected_files.add((source / name).relative_to(working).as_posix())
            published = publish(cold, dataset, source, node_id="benchmark-node",
                loose_file_threshold_bytes=1, pack_buckets=1,
                source_guard=verify_originals,
                **({"included_root_files": names} if mode == "selected-root" else {}))
            expected_files.update({
                "cold/.stignore", "cold/.stignore-edge", "cold/.local-state/node-id",
                "cold/.local-state/locks/publish-retention-global.lock",
                f"cold/.local-state/locks/publish-{dataset}.lock",
                published.manifest_path.relative_to(working).as_posix(),
                published.head_path.relative_to(working).as_posix(),
            })
            expected_files.update("cold/" + item["relpath"] for item in
                                  [published.manifest["archive"]["inventory"],
                                   *published.manifest["archive"]["objects"]])
            if restore_mode is not None:
                pin = working / "pins" / (key + ".pin.json")
                packed.write_packed_pin(pin, published)
                expected_files.add(pin.relative_to(working).as_posix())
                fixed = packed.resolve_packed_pin(cold, pin, expected_dataset=dataset)
            else:
                fixed = packed.resolve_packed_snapshot_id(cold, dataset, published.manifest["snapshot_id"])
            if fixed.manifest_sha256 != published.manifest_sha256:
                raise AssertionError("fixed release identity changed")
            recovered = None
            tensor_fingerprint = None
            if restore_mode is not None:
                source_value = cache._read(root / names[0], key, record["identity"])
                if source_value is cache._CACHE_MISS:
                    raise AssertionError("source is not an accepted complete CPU forward")
                expected = _fingerprint(source_value)
                def forbidden():
                    raise AssertionError("cold restore invoked model compute")
                with cache._singleflight(consumer / (key + ".lock")), reserve_capacity(
                    consumer, key=key, capacity_bytes=8 * 1024**3,
                    maximum_growth_bytes=2 * (cache.MAX_PAYLOAD_BYTES + MAX_RECEIPT_BYTES) + 512 * 1024,
                ) as admission:
                    if restore_mode == "materialized-copy":
                        recovered = fetch(cold, materialized, fixed)
                        prefix = ".consumer-copy-" + uuid.uuid4().hex + "-"
                        pending = {name: consumer / (prefix + name) for name in names}
                        for name in names:
                            with (recovered / name).open("rb") as reader:
                                packed._copy_member_and_verify(reader, pending[name],
                                    expected_size=before[name]["size"], expected_sha256=hashes[name],
                                    mode=before[name]["mode"], durable=True)
                    else:
                        staged = packed.stage_packed_root_files(cold, consumer, fixed,
                            expected_names=names, maximum_bytes=cache.MAX_PAYLOAD_BYTES + MAX_RECEIPT_BYTES)
                        pending = staged["files"]
                    staged_value = cache._read(pending[names[0]], key, record["identity"])
                    if staged_value is cache._CACHE_MISS or _fingerprint(staged_value) != expected:
                        raise AssertionError("complete staged CPU output differs")
                    verify_originals()
                    for name in names:  # original payload-before-receipt commit boundary
                        os.replace(pending[name], consumer / name)
                    packed._fsync_directory(consumer)
                if admission["state"] != "released":
                    raise AssertionError("cold restore capacity reservation did not release")
                # The original reader owns the same key lock itself. Release
                # the commit lease first; never recursively lock its inode.
                value, hit = cache.cached_forward(consumer, record["identity"], forbidden,
                                                  validate_before_commit=verify_originals)
                if hit["state"] != "disk_hit" or _fingerprint(value) != expected:
                    raise AssertionError("committed consumer did not reuse complete CPU output")
                tensor_fingerprint = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
                # The canonical CPU reader owns mmap-backed tensor storage.
                # Release our successful readbacks before inspecting retirement
                # eligibility; never waive the mapped-file preservation gate.
                del source_value, staged_value, value
                restore_written += sum(before[name]["size"] for name in names) * (2 if recovered else 1)
                expected_files.update((consumer / name).relative_to(working).as_posix() for name in names)
                expected_files.update({"consumer/.namespace-capacity.lock", "consumer/" + key + ".lock",
                                       "consumer/" + key + ".usage.json"})
                result_paths = {name: consumer / name for name in names}
            else:
                recovered = fetch(cold, materialized, fixed)
                result_paths = {name: recovered / name for name in names}
            if recovered is not None:
                if sorted(p.name for p in recovered.iterdir()) != sorted(names):
                    raise AssertionError("recovery includes unrelated source content")
                expected_files.update((recovered / name).relative_to(working).as_posix() for name in names)
                expected_files.update({
                    (recovered.parent / f".{recovered.name}.READY.json").relative_to(working).as_posix(),
                    f"restored/.locks/fetch-{dataset}-{recovered.name}.lock",
                })
            readback = {name: stable_payload_hash(path, payload_info(path)) for name, path in result_paths.items()}
            if readback != hashes:
                raise AssertionError("full cold reconstruction differs from original bytes")
            verify_originals()
            staged_bytes += stage_written
            trials.append({"key": key, "files": 2, "source_bytes": sum(before[n]["size"] for n in names),
                           "staging_bytes_written": stage_written, "reconstructed_sha256": readback,
                           "fixed_manifest_sha256": fixed.manifest_sha256, "fixed_snapshot_id": fixed.manifest["snapshot_id"],
                           **({"complete_tensor_and_metadata_sha256": tensor_fingerprint} if restore_mode else {}),
                           "whole_original_namespace_complete_claim": False})
        verify_originals()
        if restore_mode is not None:
            gc.collect()
        # A case owns only these canonical, declared files and their ancestors.
        # New/unknown names, links or references keep the whole case for review.
        directories = {"."}
        for name in expected_files:
            directories.update(p.as_posix() for p in Path(name).parents)
        observed, observed_dirs = {}, {"."}
        for parent, child_dirs, files in os.walk(working, followlinks=False):
            for name in child_dirs:
                path = Path(parent) / name
                if path.is_symlink():
                    raise RuntimeError("unknown private cold benchmark link prevents cleanup")
                observed_dirs.add(path.relative_to(working).as_posix())
            for name in files:
                path = Path(parent) / name
                observed[path.relative_to(working).as_posix()] = payload_info(path)
        # The original layout also creates empty pack/blob/staging directories.
        directories.update({"cold/objects/blobs", "cold/objects/packs", "cold/.local-state/staging"})
        if (set(observed) != expected_files or observed_dirs != directories
                or any(info["nlink"] != 1 for info in observed.values())
                or str(working) in Path("/proc/self/maps").read_text()):
            raise RuntimeError("unknown private cold benchmark content prevents cleanup")
        allocations = {(info["device"], info["inode"]): info["allocated"] for info in observed.values()}
        evidence = {name: {"bytes": info["size"], "sha256": stable_payload_hash(working / name, info)}
                    for name, info in observed.items()}
        verify_originals()
        for name, info in observed.items():
            if payload_info(working / name) != info:
                raise RuntimeError("private cold benchmark changed before retirement")
        for name in sorted(observed):
            (working / name).unlink()
        for name in sorted(directories - {"."}, key=lambda value: (len(Path(value).parts), value), reverse=True):
            (working / name).rmdir()
        working.rmdir()
        return {"contract": ("completed_forward_fixed_cold_consumer_benchmark_v1" if restore_mode
                             else "completed_forward_private_cold_publication_benchmark_v1"), "state": "accepted",
                "mode": mode, "trials": trials, "complete_wall_seconds": time.perf_counter() - tick,
                "complete_cpu_seconds": time.process_time() - cpu, "source_hashes": source_hashes,
                "staging_bytes_written": staged_bytes, "private_allocated_bytes": sum(allocations.values()),
                **({"restore_mode": restore_mode, "restoration_payload_bytes_written": restore_written,
                    "consumer_tensor_bits_and_metadata_equal": True, "consumer_gpu_forward_calls": 0}
                   if restore_mode else {}),
                "allocation_scope": "private_regular_file_inodes_only_excludes_directory_blocks",
                "retired_private_files": evidence, "private_working_copy_retired": True,
                "originals_unchanged": True, "model_calls": 0, "provider_calls": 0,
                "source_or_ledger_writes": 0, "cuda_initialized": torch.cuda.is_initialized(),
                "live_writer_adopted": False, "production_cold_policy_enrolled": False,
                "scope": ("complete explicit-key CPU source proof, selected canonical local publication, standard fixed pin, bounded reserved consumer restore/commit, original warm reader with forbidden compute, full tensor/metadata and file readback, exact private-copy retirement; not native D, live adoption or model qualification"
                          if restore_mode else "complete explicit-key source proof, optional staging, canonical local packed publish, fixed-release full fetch, byte readback and exact private-copy retirement; not D latency or model acceptance")}
    except BaseException as error:
        atomic_write_json(working / "FAILED.json", {"state": "failed", "mode": mode,
                          "error_type": type(error).__name__, "error": str(error), "originals_retained": True})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--key", action="append")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--working-set-probes", action="store_true")
    modes.add_argument("--capacity-transaction", choices=("baseline", "capacity"))
    modes.add_argument("--cold-publication", choices=("staged-copy", "selected-root"))
    modes.add_argument("--cold-restore", choices=("materialized-copy", "flat-stage"))
    parser.add_argument("--transaction-work-root", type=Path)
    parser.add_argument("--capacity-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if bool(args.capacity_transaction or args.cold_publication or args.cold_restore) != bool(args.transaction_work_root):
        parser.error("a capacity/cold transaction requires its explicit private work root")
    if args.capacity_transaction:
        result = benchmark_capacity_transaction(args.cache_root, args.key or [],
                    work_root=args.transaction_work_root, mode=args.capacity_transaction,
                    capacity_bytes=args.capacity_bytes)
    elif args.cold_publication:
        result = benchmark_cold_publication(args.cache_root, args.key or [],
                    work_root=args.transaction_work_root, mode=args.cold_publication)
    elif args.cold_restore:
        result = benchmark_cold_publication(args.cache_root, args.key or [],
                    work_root=args.transaction_work_root, mode="selected-root", restore_mode=args.cold_restore)
    elif args.working_set_probes:
        result = benchmark_working_set_probes(args.cache_root)
    else:
        result = benchmark(args.cache_root, args.key or [], repeats=args.repeats)
    atomic_write_json(args.output, result)
    print(json.dumps({name: result[name] for name in ("state", "trials", "cuda_initialized")}))


if __name__ == "__main__":
    main()
