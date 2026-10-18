"""Fresh-process ABBA over the complete retained-catalog capture workflow.

Uses a private synthetic retained-history fixture with real files, original
manifests, heads, no-follow object checks and canonical hashes. No provider call,
production registry write, source mutation or service control is performed.
This is not a native D/NAS throughput or complete migration acceptance.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import backup_stream
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.data_sync.packed_snapshots import (
    _canonical_json_bytes, fetch_packed_snapshot, initialize_packed_layout,
    publish_packed_snapshot, resolve_packed_snapshot_id,
)
from stockagent.runtime_identity import identity_sha256


def code_sha():
    paths = (Path(__file__), Path(backup_stream.__file__),
             Path("stockagent/data_sync/packed_backup.py"), Path("stockagent/data_sync/packed_snapshots.py"))
    return identity_sha256({str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})


def fixture(root: Path, files: int, versions: int, datasets: int = 1) -> dict:
    """A true canonical baseline plus retained metadata-only test vintages."""
    cold, source = root / "cold", root / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir(mode=0o700)
    for index in range(files):
        atomic_write_bytes(source / f"raw-{index:06d}.txt", f"{index},-2.5,NULL,股\n".encode())
    initial = publish_packed_snapshot(cold, "capture-fixture", source, loose_file_threshold_bytes=1)
    # All vintages refer to identical original files. Distinct test metadata is
    # explicit; these are not claimed as observed financial historical releases.
    for index in range(1, versions):
        manifest = deepcopy(initial.manifest)
        manifest["snapshot_id"] = f"fixture-{index:06d}"
        manifest["hlc"]["logical"] += index
        manifest.setdefault("metadata", {})["synthetic_catalog_revision"] = index
        raw = _canonical_json_bytes(manifest)
        path = cold / "manifests/capture-fixture" / (manifest["snapshot_id"] + ".json")
        atomic_write_bytes(path, raw)
    # Widen the namespace independently of history depth and source-file count.
    # Shared payloads remain unchanged; synthetic dataset aliases are explicit.
    dataset_names = ["capture-fixture"]
    original_head = json.loads((cold / "heads/capture-fixture/penguin.json").read_bytes())
    original_manifests = sorted((cold / "manifests/capture-fixture").glob("*.json"))
    for dataset_index in range(1, datasets):
        name = f"capture-fixture-{dataset_index:04d}"
        dataset_names.append(name)
        for original in original_manifests:
            manifest = json.loads(original.read_bytes())
            manifest["dataset"] = name
            raw = _canonical_json_bytes(manifest)
            relative = f"manifests/{name}/{original.name}"
            atomic_write_bytes(cold / relative, raw)
            if manifest["snapshot_id"] == original_head["snapshot_id"]:
                head = {**original_head, "dataset": name, "manifest_relpath": relative,
                        "manifest_sha256": hashlib.sha256(raw).hexdigest()}
                atomic_write_bytes(cold / f"heads/{name}/penguin.json", _canonical_json_bytes(head))
    catalog = root / "publication.json"
    atomic_write_json(catalog, {"datasets": [{"dataset": name, "publish": True} for name in dataset_names]}, durable=True)
    manifests = sorted((cold / "manifests").glob("*/*.json"))
    return {"files": files, "versions": versions, "datasets": datasets,
            "manifest_bytes": sum(p.stat().st_size for p in manifests),
            "manifest_input_sha256": identity_sha256([(p.relative_to(cold).as_posix(), hashlib.sha256(p.read_bytes()).hexdigest()) for p in manifests]),
            "baseline_snapshot_id": initial.manifest["snapshot_id"],
            "last_fixture_snapshot_id": f"fixture-{versions - 1:06d}" if versions > 1 else initial.manifest["snapshot_id"]}


def load_reference(path: Path):
    spec = importlib.util.spec_from_file_location("catalog_capture_preserved_reference", path)
    if spec is None or spec.loader is None:
        raise ValueError("preserved reference source cannot load")
    active = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = active
    spec.loader.exec_module(active)
    return active


def trial(root: Path, reference: Path, mode: str) -> dict:
    active = load_reference(reference) if mode == "reference" else backup_stream
    original_reader = active.PinnedObjectSignatures
    live_readers = set()
    cached_directory_high_water = 0

    class MeasuredReader(original_reader):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            live_readers.add(self)

        def _parent(self, *args, **kwargs):
            nonlocal cached_directory_high_water
            result = super()._parent(*args, **kwargs)
            cached_directory_high_water = max(cached_directory_high_water,
                                             sum(len(reader._directories) for reader in live_readers))
            return result

        def close(self):
            try:
                super().close()
            finally:
                live_readers.discard(self)

    initial_fds = len(os.listdir("/proc/self/fd"))
    started, cpu = time.perf_counter(), time.process_time()
    active.PinnedObjectSignatures = MeasuredReader
    try:
        result = active.capture_catalog(root / "cold", root / "publication.json")
    finally:
        active.PinnedObjectSignatures = original_reader
    elapsed, spent = time.perf_counter() - started, time.process_time() - cpu
    if live_readers or len(os.listdir("/proc/self/fd")) != initial_fds:
        raise ValueError("catalog reader leaked a directory descriptor")
    # Checks are outside the timing but inside the same process peak memory.
    if result["metadata_errors"] or result["missing_objects"]:
        raise ValueError("fixture capture has an unvalidated metadata/object gap")
    original_identity = {k: v for k, v in result.items() if k not in {"observed_at_utc", "identity_sha256"}}
    actual = backup_stream.catalog_identity(original_identity, omit_file_signatures=True)
    if actual != result["identity_sha256"]:
        raise ValueError("captured original canonical content identity differs")
    return {"mode": mode, "wall_seconds": elapsed, "cpu_seconds": spent,
        "fresh_process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "catalog_identity_sha256": actual, "releases": len(result["releases"]),
        "cached_directory_high_water": cached_directory_high_water,
        "fd_soft_limit": resource.getrlimit(resource.RLIMIT_NOFILE)[0],
        "files": len(result["files"]), "required_file_references": sum(len(r["required_file_keys"]) for r in result["releases"]),
        "source_writes": 0, "provider_calls": 0, "production_catalog_writes": 0}


def verify_originals(root: Path, description: dict) -> dict:
    """Independently restore both fixed ends, all bytes and original file hashes."""
    cold, source = root / "cold", root / "source"
    proofs = []
    for index, snapshot in enumerate((description["baseline_snapshot_id"], description["last_fixture_snapshot_id"])):
        release = resolve_packed_snapshot_id(cold, "capture-fixture", snapshot)
        target = fetch_packed_snapshot(cold, root / f"readback-{index}", release)
        files = sorted(p.name for p in target.iterdir() if p.is_file())
        for original in source.iterdir():
            restored = target / original.name
            if hashlib.sha256(original.read_bytes()).digest() != hashlib.sha256(restored.read_bytes()).digest():
                raise ValueError("original-file reconstruction differs")
        if len(files) != description["files"]:
            raise ValueError("original-file reconstruction set differs")
        proofs.append({"snapshot_id": snapshot, "manifest_sha256": release.manifest_sha256,
                       "files_verified": len(files), "materialized": True})
    return {"state": "verified_both_fixed_vintages_full_original_bytes", "versions": proofs}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--files", type=int, default=2048)
    parser.add_argument("--versions", type=int, default=256)
    parser.add_argument("--datasets", type=int, default=1)
    parser.add_argument("--trial", choices=("reference", "candidate"))
    parser.add_argument("--native-limits", action="store_true")
    args = parser.parse_args(argv)
    if args.trial:
        print(json.dumps(trial(args.root, args.reference, args.trial)))
        return 0
    base = Path("artifacts/benchmarks").resolve()
    if (args.root.exists() or not args.root.absolute().is_relative_to(base)
            or not args.reference.resolve().is_relative_to(base) or args.reference.is_symlink()
            or args.files < 1 or args.versions < 2 or not 1 <= args.datasets <= 1000):
        parser.error("fresh private benchmark root and preserved regular reference are required")
    args.root.mkdir(parents=True, mode=0o700)
    reference_sha = hashlib.sha256(args.reference.read_bytes()).hexdigest()
    pinned_code = code_sha()
    resource_limits = None
    owner = None
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        resource_limits, owner = match_native_limits(), owner_identity()
    # Canonical cold roots are data-only, never nested in a Git checkout.
    # Keep failed/successful engineering fixtures privately inspectable; none
    # of their names belongs to an authoritative source or cold namespace.
    work = Path(tempfile.mkdtemp(prefix="stockagent-catalog-capture-", dir="/var/tmp"))
    atomic_write_json(args.root / "fixture-intent.json", {"private_fixture_root": str(work),
        "authoritative_source": False, "production_cold": False}, durable=True)
    observed = fixture(work, args.files, args.versions, args.datasets)
    results = []
    for index, mode in enumerate(("reference", "candidate", "candidate", "reference")):
        if pinned_code != code_sha() or reference_sha != hashlib.sha256(args.reference.read_bytes()).hexdigest():
            raise ValueError("pinned benchmark implementation changed")
        run = subprocess.run([sys.executable, "-m", "scripts.benchmark_backup_catalog_capture",
            "--root", str(work), "--reference", str(args.reference), "--trial", mode],
            check=True, capture_output=True, text=True, timeout=1200)
        row = json.loads(run.stdout)
        results.append(row)
        atomic_write_json(args.root / f"trial-{index}-{mode}.json", row, durable=True)
        print(json.dumps({"trial": index, **row}), flush=True)
    stable = pinned_code == code_sha() and reference_sha == hashlib.sha256(args.reference.read_bytes()).hexdigest()
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_signature import limits
        stable = stable and resource_limits == limits() and owner == owner_identity()
    if not stable or len({r["catalog_identity_sha256"] for r in results}) != 1:
        raise ValueError("full capture differs or native owner/code/limits changed")
    proofs = verify_originals(work, observed)
    means = {mode: {key: statistics.mean(r[key] for r in results if r["mode"] == mode)
                   for key in ("wall_seconds", "cpu_seconds", "fresh_process_peak_rss_bytes")}
             for mode in ("reference", "candidate")}
    receipt = {"state": "accepted_complete_catalog_capture_fixture_abba", "observed_at_utc": datetime.now(UTC).isoformat(),
        "fixture": observed, "results": results, "means": means, "original_byte_readback": proofs,
        "code_sha256": pinned_code, "reference_sha256": reference_sha,
        "native_owner_and_limits_stable": stable, "native_resource_limits": resource_limits,
        "provider_calls": 0, "production_catalog_writes": 0, "production_source_writes": 0,
        "caveats": ["Synthetic retained metadata histories, real private filesystem and full fixed-vintage reconstruction.",
            "Measures full capture, not complete DuckLake register/export, native HDD, NAS or all-source migration."]}
    atomic_write_json(args.root / "acceptance.json", receipt, durable=True)
    print(json.dumps(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
